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

	// OnWriteAttempt is called when the pump dequeues an AU and is
	// about to deliver it (before the write). Paired with
	// OnWriteBytes it lets an observer distinguish "nothing to send"
	// (no attempts) from "writes are stuck" (attempts without
	// successes) — the session's idle watchdog keys on exactly that
	// distinction. Optional.
	OnWriteAttempt func()

	// TrackName, if non-empty, causes the pump to write a
	// TypeStreamHeader frame as the first frame on each new uni
	// stream so the receiver can route AUs to the right track. An
	// empty TrackName preserves single-track wire compatibility
	// (the receiver falls back to the implicit "primary" track).
	TrackName string

	// Priority is the track-level priority hint (RFC 9218 numeric
	// scale: lower = more urgent). When Scheduler is set, this value
	// orders this pump's AU writes against other pumps sharing the
	// same Scheduler: higher-priority pumps drain first when both
	// have AUs ready. quic-go does not expose PRIORITY_UPDATE on its
	// public API, so byte-level interleaving inside the QUIC connection
	// is still kernel-of-truth FIFO; we only influence application-
	// level ordering at the Submit-to-write boundary. When upstream
	// support lands we will plumb this into SetPriority on each
	// stream-open and the Submit gate becomes redundant. Until then
	// the Scheduler path is the only way to give one track jump-ahead
	// behavior under contention. The hint is always emitted via
	// OnStreamOpened so observers can verify intent matches arrival
	// order even when no Scheduler is wired.
	Priority uint8

	// Scheduler, when non-nil, serializes this pump's AU writes
	// through a shared priority queue so cross-track ordering follows
	// Priority instead of goroutine scheduling. Multiple pumps in the
	// same session typically share one Scheduler.
	//
	// Precise semantic: each pump submits one AU at a time and waits
	// for the scheduler to run it before reading the next AU off its
	// receiver channel. When N pumps share a Scheduler and each has
	// at least one AU pending, the scheduler's worker picks the
	// highest-priority item. Under sustained contention this gives
	// "every available slot goes to the highest-priority pump that
	// has an item queued in the scheduler when the worker is free,"
	// NOT "all of high's items before any of low's." A pump that
	// could submit-ahead would give batched priority. The per-AU-wait
	// design intentionally trades that for back-pressure simplicity:
	// a slow downstream cannot fill the scheduler queue past one
	// item per pump.
	//
	// Optional; when nil, each pump writes directly without
	// coordination (the legacy path, lowest overhead, no cross-pump
	// priority influence).
	Scheduler *Scheduler

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

	// FallbackOpener is used by the DeliveryDatagramOrStream pump when
	// an AU cannot ride the datagram path (payload exceeds the QUIC
	// path MTU, or SendDatagram returns an error). When set, the pump
	// lazily opens a single persistent uni stream and writes the
	// affected AU there using the self-contained feed-frame format.
	// When unset, those AUs are dropped and surfaced via OnAUDropped
	// with reason "datagram_too_large" or "send_failed". The high-level
	// server wires the same StreamOpener it uses for stream-based tracks.
	FallbackOpener StreamOpener

	// OnFallback is invoked when an AU was routed through the fallback
	// stream rather than the datagram path. reason is the original
	// datagram-path failure ("datagram_too_large" or "send_failed").
	// Optional; useful for observability that wants to count fallback
	// activations without conflating them with hard drops.
	OnFallback func(reason string)

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

	// AllowLeadingPFrames permits the stream-per-GOP pump to open its
	// FIRST stream on a P-frame and write the leading P-frame run
	// without a keyframe. Set only for resumed sessions where the
	// subscriber declared a per-track continuation point
	// (HELLO.LastSeenSeq): the subscriber already holds the GOP head,
	// so the spliced mid-GOP P-frames are decodable continuation, not
	// garbage. Applies to the first stream only — after any keyframe
	// or stream failure, normal keyframe-first rules resume.
	AllowLeadingPFrames bool

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

	// Receiver pointer is set when Run starts so Stats() can read
	// QueueDepth without re-plumbing through every call site. Read
	// via atomic.Pointer.Load: only the goroutine running Run writes
	// it; observers may concurrently Load.
	recv atomic.Pointer[pubsub.Receiver]

	// oldestAUMicros is wall-clock UnixMicro of the head AU currently
	// being processed by the pump. Zero when idle. Updated when the
	// pump picks an AU off the channel; cleared when that AU is
	// successfully written or dropped.
	oldestAUMicros int64
}

// PumpStats reports the pump's current view of its outbound queue.
// Fields are best-effort snapshots: readers should not rely on
// strict per-AU consistency, only on order-of-magnitude correctness
// for observability.
type PumpStats struct {
	QueueDepth  int           // approximate AUs waiting in the receiver channel
	OldestAUAge time.Duration // wall-clock age of the oldest queued AU, or 0 if idle
}

// Stats returns a point-in-time snapshot of pump observability.
// Cheap (Receiver.QueueDepth is one len(chan); atomic loads
// otherwise); safe to call from any goroutine.
func (p *Pump) Stats() PumpStats {
	var depth int
	if r := p.recv.Load(); r != nil {
		depth = r.QueueDepth()
	}
	oldest := atomic.LoadInt64(&p.oldestAUMicros)
	var age time.Duration
	if oldest > 0 {
		age = time.Duration(time.Now().UnixMicro()-oldest) * time.Microsecond
		if age < 0 {
			age = 0
		}
	}
	return PumpStats{
		QueueDepth:  depth,
		OldestAUAge: age,
	}
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
	p.recv.Store(recv)
	defer p.recv.Store(nil)
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

// scheduledDo runs fn either inline (no Scheduler) or via the shared
// Scheduler's priority queue (Scheduler set). When scheduled, the
// caller blocks until fn returns. This serializes writes across all
// pumps sharing the Scheduler: lower-priority pumps yield to
// higher-priority ones at AU granularity.
//
// Done is allocated per call. The throughput cost is one channel
// send + receive per AU on top of the existing write, measurable
// but small relative to a stream.Write on a real network. Sessions
// that don't need priority gating leave Scheduler unset and pay
// nothing.
func (p *Pump) scheduledDo(fn func()) {
	if p.cfg.Scheduler == nil {
		fn()
		return
	}
	done := make(chan struct{})
	ok := p.cfg.Scheduler.Submit(WorkItem{
		Priority: p.cfg.Priority,
		// defer close so a panic inside fn still releases the wait
		// rather than leaking the pump goroutine.
		Do: func() {
			defer close(done)
			fn()
		},
	})
	if !ok {
		// Scheduler is closed (the session is shutting down). Run
		// inline so the pump can observe its own ctx-cancel and exit
		// instead of blocking forever on a done that will never fire.
		fn()
		return
	}
	<-done
}

// openGOPStream opens one uni stream and writes the multi-track
// header. Shared by the keyframe path and the resume-continuation
// path in runStreamGOP.
func (p *Pump) openGOPStream(ctx context.Context) (SendStream, error) {
	openCtx, cancel := context.WithTimeout(ctx, p.cfg.OpenDeadlineKeyframe)
	s, err := p.opener.OpenSendStream(openCtx)
	cancel()
	if err != nil {
		return nil, err
	}
	// Write the stream header (if multi-track) before any feed
	// frames so the receiver can demux this stream.
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

	// leadingAllowed permits the FIRST stream to start on a P-frame
	// (resume continuation — see Config.AllowLeadingPFrames). Cleared
	// once any stream is opened or any keyframe arrives: after that,
	// a missing stream means a real gap and only a keyframe resyncs.
	leadingAllowed := p.cfg.AllowLeadingPFrames

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
		if p.cfg.OnWriteAttempt != nil {
			p.cfg.OnWriteAttempt()
		}

		if au.Keyframe {
			leadingAllowed = false
			// Reset the previous GOP's stream so any in-flight bytes
			// are discarded — the receiver will resync on this new
			// keyframe regardless.
			if current != nil {
				current.CancelWrite(0)
			}
			s, err := p.openGOPStream(ctx)
			if err != nil {
				current = nil
				recv.RequestKeyframe()
				continue
			}
			current = s
		}

		// P-frame without a current stream. Two cases:
		//
		//   - Resume continuation (leadingAllowed): the subscriber
		//     declared LastSeenSeq, so it holds this GOP's head and
		//     these P-frames are decodable. Open the first stream and
		//     write them — this is what makes resume gap-free for
		//     video instead of stalling until the next keyframe.
		//
		//   - Mid-session (the common case): the receiver joined
		//     mid-GOP or a prior frame was dropped. No decodable
		//     reference; ask for a fresh keyframe.
		if current == nil {
			if !leadingAllowed {
				recv.RequestKeyframe()
				continue
			}
			leadingAllowed = false // one continuation stream only
			s, err := p.openGOPStream(ctx)
			if err != nil {
				recv.RequestKeyframe()
				continue
			}
			current = s
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

		writeErr := p.writeFrame(current, typ, au.PTSMicro, au.Seq, flags, au.Bytes)
		if writeErr != nil {
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
		if p.cfg.OnWriteAttempt != nil {
			p.cfg.OnWriteAttempt()
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

		writeErr := p.writeFrame(stream, wire.TypeKeyframe, au.PTSMicro, au.Seq, flags, au.Bytes)
		if writeErr != nil {
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

// pacingChunkBytes bounds how many bytes one scheduled write puts on the
// wire before yielding the shared scheduler worker back to other pumps.
// Only relevant when a Scheduler is set: a large bulk frame (a video
// keyframe is often hundreds of KiB) is written in chunks so a higher-
// priority pump's small AU — an LLM token, a tool call — runs between
// chunks instead of waiting out the whole frame. Chunk boundaries are
// invisible on the wire: the receiver reassembles by the frame's length
// prefix. 16 KiB keeps the yield granularity fine without adding many
// extra Write calls (a 256 KiB frame becomes 16 chunks).
const pacingChunkBytes = 16 * 1024

// appendFeedFrame encodes one feed frame onto dst and returns the grown
// slice:
//
//	[1B type][3B BE length][8B BE pts][4B BE seq][1B flags][payload]
//
// length is len(payload) — the fixed-size feed metadata sits between the
// length and the payload, matching wire.WriteFeedFrame exactly.
func appendFeedFrame(dst []byte, typ byte, pts uint64, seq uint32, flags byte, payload []byte) []byte {
	n := len(payload)
	dst = append(dst,
		typ,
		byte(n>>16), byte(n>>8), byte(n),
		byte(pts>>56), byte(pts>>48), byte(pts>>40), byte(pts>>32),
		byte(pts>>24), byte(pts>>16), byte(pts>>8), byte(pts),
		byte(seq>>24), byte(seq>>16), byte(seq>>8), byte(seq),
		flags,
	)
	return append(dst, payload...)
}

// writeFeedFramePooled encodes and writes one feed frame in a single
// stream.Write (header + payload coalesced so quic-go can pack one
// packet), using a pooled scratch buffer to avoid a per-call allocation.
// Used by the non-scheduler / small-frame path in writeFrame.
func writeFeedFramePooled(w io.Writer, typ byte, pts uint64, seq uint32, flags byte, payload []byte) error {
	// Reject oversize payloads before encoding. The 3-byte length field
	// caps at MaxFeedPayload; silently truncating would desync the wire.
	if len(payload) > wire.MaxFeedPayload {
		return fmt.Errorf("%w: feed %d > %d", wire.ErrFrameTooLarge, len(payload), wire.MaxFeedPayload)
	}
	bufp := lowLatencyBufPool.Get().(*[]byte)
	buf := appendFeedFrame((*bufp)[:0], typ, pts, seq, flags, payload)
	defer func() {
		*bufp = buf[:0]
		lowLatencyBufPool.Put(bufp)
	}()
	_, err := w.Write(buf)
	return err
}

// writeFrame writes one feed frame to w through the pump's scheduling
// discipline. Without a Scheduler — or for a small frame — it is a
// single coalesced, deadline-guarded write, identical to the legacy
// path. With a Scheduler AND a large frame it writes in pacingChunkBytes
// chunks, each submitted to the scheduler, so a higher-priority pump's AU
// preempts between chunks instead of waiting out the whole frame. The
// same pump writes its own chunks sequentially (scheduledDo blocks until
// each runs), so a frame's bytes stay contiguous and in order on its
// stream; only OTHER pumps' work — on OTHER streams — interleaves.
func (p *Pump) writeFrame(w SendStream, typ byte, pts uint64, seq uint32, flags byte, payload []byte) error {
	if len(payload) > wire.MaxFeedPayload {
		return fmt.Errorf("%w: feed %d > %d", wire.ErrFrameTooLarge, len(payload), wire.MaxFeedPayload)
	}
	if p.cfg.Scheduler == nil || wire.FeedHeaderLen+len(payload) <= pacingChunkBytes {
		var err error
		p.scheduledDo(func() {
			_ = w.SetWriteDeadline(time.Now().Add(p.cfg.WriteDeadline))
			err = writeFeedFramePooled(w, typ, pts, seq, flags, payload)
		})
		return err
	}
	// Large frame under a scheduler: chunked cooperative write.
	bufp := lowLatencyBufPool.Get().(*[]byte)
	buf := appendFeedFrame((*bufp)[:0], typ, pts, seq, flags, payload)
	defer func() {
		*bufp = buf[:0]
		lowLatencyBufPool.Put(bufp)
	}()
	var err error
	for off := 0; off < len(buf) && err == nil; off += pacingChunkBytes {
		end := min(off+pacingChunkBytes, len(buf))
		chunk := buf[off:end]
		p.scheduledDo(func() {
			_ = w.SetWriteDeadline(time.Now().Add(p.cfg.WriteDeadline))
			if _, werr := w.Write(chunk); werr != nil {
				err = werr
			}
		})
	}
	return err
}
