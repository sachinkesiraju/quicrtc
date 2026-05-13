// Package feed implements quicrtc's stream-per-GOP wire pattern.
//
// One unidirectional QUIC stream per GOP (group of pictures). The
// first frame on each stream is a keyframe; subsequent in-GOP frames
// are P-frames in decode order. When the next keyframe arrives, the
// previous stream is RESET (CancelWrite, not Close/FIN) so any
// in-flight bytes are dropped at the QUIC layer — the GOP they
// belonged to is now stale, the receiver will resync on the new key.
//
// Why this matters:
//
//   - In-stream FIFO order = decode order. Receiver needs no reorder
//     buffer; QUIC's per-stream byte ordering is the canonical order.
//
//   - Head-of-line blocking is bounded to one GOP. Loss recovery
//     within a GOP can stall that GOP's stream, but a fresh keyframe
//     opens a fresh stream and the receiver picks up immediately.
//
//   - Encoder runaway is structurally bounded. If the encoder
//     produces faster than the network drains, more keyframes arrive,
//     more old streams get reset, more flow-control credit is
//     released. The system finds a steady state without ad-hoc rate
//     limiting.
//
//   - ~30x less stream-open churn than per-frame streams (at GOP=1s,
//     fps=30).
//
// The pump is decoupled from QUIC via the StreamOpener interface so
// it can be tested with in-memory streams. The high-level server
// adapts webtransport.Session to StreamOpener; any future transport
// implementation can do the same.
package feed

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/track"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// SendStream is the minimal subset of webtransport.SendStream the
// pump needs. CancelWrite must reset the stream with the given error
// code such that any unsent bytes are not delivered.
type SendStream interface {
	io.Writer
	io.Closer
	SetWriteDeadline(time.Time) error
	CancelWrite(code uint32)
}

// StreamOpener opens new uni send streams. The high-level server
// implements this against a *webtransport.Session; tests use a
// fake.
type StreamOpener interface {
	// OpenSendStream blocks until a stream is open or ctx is done.
	OpenSendStream(ctx context.Context) (SendStream, error)
}

// Tunables for the pump. Keyframe stream-open and per-frame write
// deadlines are picked to be generous enough for typical real-world
// RTTs (mobile cellular: 50-200ms; transcontinental: 200-300ms)
// without being so loose that genuinely-stuck streams stall the
// pipeline. The original 150ms open deadline made every uni stream
// open fail under any RTT >= 150ms, killing video on high-RTT links.
//
// 1.5s open deadline accommodates 200-300ms RTT links with comfortable
// margin for reordering and SRTT growth. Per-frame write deadline
// scaled accordingly; if a frame can't be written in 1.5s the network
// is genuinely broken and the pump should give up and request a fresh
// keyframe.
const (
	DefaultOpenDeadlineKeyframe = 1500 * time.Millisecond
	DefaultWriteDeadline        = 1500 * time.Millisecond
)

// Config tunes pump behavior. Zero values pick the Default* above.
type Config struct {
	OpenDeadlineKeyframe time.Duration
	WriteDeadline        time.Duration

	// OnWriteBytes is called after each successful frame write with
	// the wire-byte count. Optional; useful for metrics.
	OnWriteBytes func(n int)

	// TrackName, if non-empty, causes the pump to write a
	// TypeStreamHeader frame as the first frame on each new uni
	// stream so the receiver can route AUs to the right track. An
	// empty TrackName preserves single-track wire compatibility
	// (the receiver falls back to the implicit "primary" track).
	TrackName string

	// Priority is the track-level priority hint (RFC 9218 numeric
	// scale: lower = more urgent). This value is currently
	// observability-only — quic-go does not yet expose
	// PRIORITY_UPDATE on the public API surface, so we cannot
	// influence the underlying QUIC stream scheduling. When upstream
	// support lands we'll plumb this through SetPriority on each
	// stream-open. For now: emitted via OnStreamOpened so observers
	// can verify intent matches arrival order.
	Priority uint8

	// OnStreamOpened is called after each successful uni stream
	// open with the track name + priority hint. Optional; useful for
	// metrics that want to verify priority ordering.
	OnStreamOpened func(trackName string, priority uint8)

	// Delivery selects which pump implementation handles this track.
	// Zero value is DeliveryStreamGOP — the safe default that matches
	// the legacy single-pump video path. Higher-level callers populate
	// this from track.DefaultDeliveryClass(kind); benchmarks set it
	// directly to A/B different pumps in the same binary.
	Delivery track.DeliveryClass

	// LowLatencyQueueDepth is the bounded queue depth used by the
	// StreamLowLatency pump as its drop-oldest-on-overflow threshold.
	// Zero picks DefaultLowLatencyQueueDepth (16).
	LowLatencyQueueDepth int

	// OnAUDropped is invoked when an AU is dropped (queue overflow
	// or write deadline). Optional; useful for metrics.
	OnAUDropped func(reason string)

	// DatagramSender is required when Delivery == DeliveryDatagramOrStream.
	// The high-level server wraps webtransport.Session.SendDatagram
	// into this interface so the pump stays testable.
	DatagramSender DatagramSender

	// TrackID is the session-scoped 1-byte handle for datagram tracks.
	// Used as the envelope's track-id byte so the receiver can demux
	// multiple datagram tracks on one connection. Assigned by the
	// publisher at Announce time. Unused for stream-based deliveries.
	TrackID byte

	// BidiOpener is required when Delivery == DeliveryBidiPerCall.
	// Each AU is treated as a logical call; one bidi stream is opened
	// per AU. Backed by webtransport.Session.OpenStreamSync on the
	// wire.
	BidiOpener BidiOpener

	// BidiCallTimeout caps the per-call wall-clock budget for
	// BidiPerCall calls. Zero picks DefaultBidiCallTimeout (30s).
	BidiCallTimeout time.Duration

	// MaxBidiResponse caps the bytes read off each BidiPerCall response
	// stream. <=0 picks DefaultMaxBidiResponse. Set lower for stricter
	// memory caps under high call rates or when responses are bounded
	// by the application's protocol (e.g., a tool-call schema).
	MaxBidiResponse int64

	// OnBidiResponse is invoked when a bidi call completes. seq matches
	// the AU's Seq for correlation. response holds the bytes received
	// before the peer FIN'd or the read errored; err is non-nil when
	// the call was cancelled, timed out, or the read failed midway.
	// Apps that only want successful responses should check err first.
	// When OnBidiResponse is nil, responses are drained but not
	// delivered (the stream still closes cleanly).
	OnBidiResponse func(seq uint32, response []byte, err error)
}

// DefaultLowLatencyQueueDepth bounds the StreamLowLatency pump's
// in-flight AU queue. Picked to absorb a small burst (e.g. an LLM
// emitting 10 tokens in <1ms) without queueing far beyond network
// drain capacity. Drop-oldest semantics on overflow.
const DefaultLowLatencyQueueDepth = 16

// Pump implements the per-DeliveryClass send dispatch. Construct one
// per session; call Run with the per-session Receiver.
type Pump struct {
	opener StreamOpener
	cfg    Config

	// bidiInFlight tracks concurrent in-flight BidiPerCall calls.
	// Exposed via BidiInFlight() for tests/observability.
	bidiInFlight int64
}

// BidiInFlight returns the current count of in-flight BidiPerCall
// calls. Useful for tests verifying that parallel calls actually run
// in parallel.
func (p *Pump) BidiInFlight() int64 {
	return atomic.LoadInt64(&p.bidiInFlight)
}

// New constructs a Pump bound to the given opener.
func New(opener StreamOpener, cfg Config) *Pump {
	if cfg.OpenDeadlineKeyframe == 0 {
		cfg.OpenDeadlineKeyframe = DefaultOpenDeadlineKeyframe
	}
	if cfg.WriteDeadline == 0 {
		cfg.WriteDeadline = DefaultWriteDeadline
	}
	return &Pump{opener: opener, cfg: cfg}
}

// Run blocks until the receiver's frame channel closes or ctx is
// done. The returned error is ctx.Err() on cancellation, nil on
// graceful close (channel drained).
//
// Run dispatches to the appropriate per-DeliveryClass implementation
// based on cfg.Delivery. Datagram/bidi paths fall back to StreamGOP
// when their required transport hooks (DatagramSender/BidiOpener) are
// not wired, so telemetry/tool-call tracks still deliver reliably on
// transports that don't support those primitives.
func (p *Pump) Run(ctx context.Context, recv *pubsub.Receiver) error {
	switch p.cfg.Delivery {
	case track.DeliveryStreamLowLatency:
		return p.runStreamLowLatency(ctx, recv)
	case track.DeliveryDatagramOrStream:
		if p.cfg.DatagramSender != nil {
			return p.runDatagramOrStream(ctx, recv)
		}
		return p.runStreamGOP(ctx, recv)
	case track.DeliveryBidiPerCall:
		if p.cfg.BidiOpener != nil {
			return p.runBidiPerCall(ctx, recv)
		}
		return p.runStreamGOP(ctx, recv)
	default:
		return p.runStreamGOP(ctx, recv)
	}
}

// runStreamGOP is the legacy per-GOP pump: one uni stream per
// keyframe, P-frames written in-stream, prev stream RESET on next
// keyframe. The video path; the structurally-correct choice when
// the AU stream has keyframe semantics.
func (p *Pump) runStreamGOP(ctx context.Context, recv *pubsub.Receiver) error {
	var current SendStream
	defer func() {
		if current != nil {
			_ = current.Close()
		}
	}()

	frames := recv.Frames()
	for {
		var au pubsub.AccessUnit
		select {
		case <-ctx.Done():
			return ctx.Err()
		case a, ok := <-frames:
			if !ok {
				return nil
			}
			au = a
		}

		if au.Keyframe {
			// Reset the previous GOP's stream so any in-flight bytes
			// are discarded — the receiver will resync on this new
			// keyframe regardless.
			if current != nil {
				current.CancelWrite(0)
			}
			openCtx, cancel := context.WithTimeout(ctx, p.cfg.OpenDeadlineKeyframe)
			s, err := p.opener.OpenSendStream(openCtx)
			cancel()
			if err != nil {
				current = nil
				recv.RequestKeyframe()
				continue
			}
			// Write the stream header (if multi-track) before any
			// feed frames so the receiver can demux this stream.
			if p.cfg.TrackName != "" {
				_ = s.SetWriteDeadline(time.Now().Add(p.cfg.WriteDeadline))
				if err := wire.WriteStreamHeader(s, p.cfg.TrackName); err != nil {
					s.CancelWrite(0)
					current = nil
					recv.RequestKeyframe()
					continue
				}
			}
			if p.cfg.OnStreamOpened != nil {
				p.cfg.OnStreamOpened(p.cfg.TrackName, p.cfg.Priority)
			}
			current = s
		}

		// P-frame without a current stream: the receiver just joined
		// mid-GOP and the very first AU dispatched to it was a P-frame
		// (e.g. its channel was full when a keyframe was published).
		// We have no decodable reference; ask for a fresh keyframe.
		if current == nil {
			recv.RequestKeyframe()
			continue
		}

		typ := wire.TypePFrame
		flags := byte(0)
		if au.Keyframe {
			typ = wire.TypeKeyframe
			flags |= wire.FlagKeyframe
		}
		if au.Discontinuity {
			flags |= wire.FlagDiscontinuity
		}

		_ = current.SetWriteDeadline(time.Now().Add(p.cfg.WriteDeadline))
		if err := writeFeedFramePooled(current, typ, au.PTSMicro, au.Seq, flags, au.Bytes); err != nil {
			current.CancelWrite(0)
			current = nil
			recv.RequestKeyframe()
			continue
		}
		if p.cfg.OnWriteBytes != nil {
			p.cfg.OnWriteBytes(wire.FeedHeaderLen + len(au.Bytes))
		}
	}
}

// lowLatencyBufPool reuses scratch buffers for the wire-frame encode.
// Tokens are typically 50–500 bytes; we size for that and let Go grow
// the slice if a larger AU arrives (Reset preserves capacity).
var lowLatencyBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 512)
		return &b
	},
}

// runStreamLowLatency opens ONE persistent uni stream for the track's
// lifetime and writes every AU into it as an independent frame. No
// keyframe/Pframe distinction on the wire (every AU is encoded as
// TypeKeyframe — the wire type is reused but semantically every
// low-latency AU is self-contained, so the existing receive path
// treats them identically). Backpressure is detected via a write
// deadline; on deadline-fire we drop the AU and the stream stays
// alive. App-layer bounded queue is the upstream throttle.
//
// Designed for LLM tokens, terminal output, and other small-AU
// workloads where per-AU stream-open cost (the legacy pump's
// behavior when every AU is marked Keyframe=true) adds jitter and
// allocation pressure that dominates measurable latency.
func (p *Pump) runStreamLowLatency(ctx context.Context, recv *pubsub.Receiver) error {
	// openStream opens the persistent uni stream and writes the
	// optional StreamHeader. Returns nil + non-nil error on failure;
	// callers retry on next AU rather than terminating the pump.
	openStream := func() (SendStream, error) {
		openCtx, cancel := context.WithTimeout(ctx, p.cfg.OpenDeadlineKeyframe)
		s, err := p.opener.OpenSendStream(openCtx)
		cancel()
		if err != nil {
			return nil, err
		}
		if p.cfg.TrackName != "" {
			_ = s.SetWriteDeadline(time.Now().Add(p.cfg.WriteDeadline))
			if err := wire.WriteStreamHeader(s, p.cfg.TrackName); err != nil {
				s.CancelWrite(0)
				return nil, err
			}
		}
		if p.cfg.OnStreamOpened != nil {
			p.cfg.OnStreamOpened(p.cfg.TrackName, p.cfg.Priority)
		}
		return s, nil
	}

	// Try to open up front. If it fails, defer the open until the
	// first AU arrives — that way a transient network blip during
	// track setup doesn't kill the pump permanently; we retry on
	// every AU until the open succeeds.
	stream, err := openStream()
	if err != nil {
		if p.cfg.OnAUDropped != nil {
			p.cfg.OnAUDropped("stream_open_failed")
		}
		stream = nil
	}
	defer func() {
		if stream != nil {
			_ = stream.Close()
		}
	}()

	// We don't need our own bounded queue here — the broadcaster
	// already enforces chanSize per-receiver and drops on overflow
	// with the kind-appropriate policy. The receive channel IS the
	// bounded queue for this pump.
	//
	// The write deadline is the only backpressure path: if the QUIC
	// stream can't accept bytes within the deadline, we drop the AU
	// and continue. We never CancelWrite the stream — unlike GOP,
	// there's no keyframe-driven resync to recover into.

	frames := recv.Frames()
	for {
		var au pubsub.AccessUnit
		select {
		case <-ctx.Done():
			return ctx.Err()
		case a, ok := <-frames:
			if !ok {
				return nil
			}
			au = a
		}

		// Lazy stream re-open: if the initial open failed (or a write
		// fault destroyed the stream below), retry on each AU. The
		// pump stays alive across transient open failures instead of
		// silently breaking the track.
		if stream == nil {
			s, err := openStream()
			if err != nil {
				if p.cfg.OnAUDropped != nil {
					p.cfg.OnAUDropped("stream_open_failed")
				}
				continue
			}
			stream = s
		}

		flags := wire.FlagKeyframe // every low-latency AU is self-contained
		if au.Discontinuity {
			flags |= wire.FlagDiscontinuity
		}

		_ = stream.SetWriteDeadline(time.Now().Add(p.cfg.WriteDeadline))
		if err := writeFeedFramePooled(stream, wire.TypeKeyframe, au.PTSMicro, au.Seq, flags, au.Bytes); err != nil {
			// Write failed — could be a transient deadline elapse or
			// the stream genuinely dying. Distinguish would require
			// inspecting err; for now treat it as "drop AU, reopen on
			// next AU." CancelWrite is a no-op on dead streams.
			stream.CancelWrite(0)
			stream = nil
			if p.cfg.OnAUDropped != nil {
				p.cfg.OnAUDropped("write_deadline")
			}
			continue
		}
		if p.cfg.OnWriteBytes != nil {
			p.cfg.OnWriteBytes(wire.FeedHeaderLen + len(au.Bytes))
		}
	}
}

// writeFeedFramePooled is a pooled-buffer variant of wire.WriteFeedFrame
// that avoids the per-call allocation in the hot path AND coalesces the
// header + payload into a single stream.Write so quic-go can pack into
// one QUIC packet without extra coalescing logic. Used by both
// runStreamGOP (typ = TypeKeyframe or TypePFrame) and runStreamLowLatency
// (typ = TypeKeyframe; every AU is self-contained).
func writeFeedFramePooled(w io.Writer, typ byte, pts uint64, seq uint32, flags byte, payload []byte) error {
	// Reject oversize payloads before encoding. The 3-byte length field
	// caps at MaxFeedPayload; silently truncating would desync the wire.
	if len(payload) > wire.MaxFeedPayload {
		return fmt.Errorf("%w: feed %d > %d", wire.ErrFrameTooLarge, len(payload), wire.MaxFeedPayload)
	}
	bufp := lowLatencyBufPool.Get().(*[]byte)
	buf := (*bufp)[:0]
	defer func() {
		*bufp = buf[:0]
		lowLatencyBufPool.Put(bufp)
	}()

	// Inline encode of the feed frame to one contiguous buffer:
	//   [1B type][3B BE length][8B BE pts][4B BE seq][1B flags][payload]
	// length is just len(payload) — the wire format puts the
	// fixed-size feed metadata between the length and the payload, so
	// this matches wire.WriteFeedFrame exactly.
	n := len(payload)
	buf = append(buf,
		typ,
		byte(n>>16), byte(n>>8), byte(n),
		byte(pts>>56), byte(pts>>48), byte(pts>>40), byte(pts>>32),
		byte(pts>>24), byte(pts>>16), byte(pts>>8), byte(pts),
		byte(seq>>24), byte(seq>>16), byte(seq>>8), byte(seq),
		flags,
	)
	buf = append(buf, payload...)
	_, err := w.Write(buf)
	return err
}
