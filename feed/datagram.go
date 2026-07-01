package feed

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// DatagramSender is the transport hook the datagram pump writes to.
// The high-level server adapts webtransport.Session.SendDatagram into
// this interface so the pump remains testable with an in-memory fake.
type DatagramSender interface {
	// SendDatagram sends one datagram. May return an error for
	// "datagram too large" (quic-go DatagramTooLargeError) or for
	// transport failure. Implementations are expected to be
	// non-blocking; quic-go's datagram queue is bounded internally.
	SendDatagram(payload []byte) error
}

// ErrDatagramSenderNil indicates the pump was started without a
// DatagramSender — Config.DatagramSender must be set when
// Delivery=DeliveryDatagramOrStream.
var ErrDatagramSenderNil = errors.New("feed: DatagramSender required for DeliveryDatagramOrStream")

// datagramBufPool reuses encode scratch space.
var datagramBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, wire.MaxDatagramPayload+16)
		return &b
	},
}

// Fallback drop reasons surfaced via Config.OnAUDropped. Datagram path
// uses these consistently so callers can wire a single switch in their
// observability layer.
const (
	// DatagramTooLarge: payload exceeded MaxDatagramPayload AND no
	// fallback was configured (or the fallback open failed).
	dropReasonDatagramTooLarge = "datagram_too_large"
	// SendFailed: the transport rejected the datagram AND no fallback
	// was configured (or the fallback open failed).
	dropReasonSendFailed = "send_failed"
	// FallbackWriteFailed: fallback stream existed but the write
	// errored. The stream is torn down and the AU is dropped.
	dropReasonFallbackWriteFailed = "fallback_write_failed"
	// FallbackOpenFailed: fallback was configured but opening the uni
	// stream failed (network down, peer gone). AU is dropped.
	dropReasonFallbackOpenFailed = "fallback_open_failed"
)

// runDatagramOrStream is the datagram-first pump. Each AU is sent as
// one datagram with the 4-byte envelope. When the datagram path can't
// carry the AU (oversized payload or a transport-level send failure),
// the pump opens a persistent uni stream and writes the AU there
// instead. The fallback stream is lazy: it stays unopened until the
// first AU needs it, and it's reused across subsequent fallbacks so
// the steady-state cost of a stream of oversized AUs is one open, not
// one-per-AU.
//
// Backpressure on the datagram path is the QUIC datagram queue depth.
// Backpressure on the fallback path is the QUIC stream flow-control
// window. If both paths fail the AU is dropped and surfaced via
// OnAUDropped with the appropriate reason.
func (p *Pump) runDatagramOrStream(ctx context.Context, recv *pubsub.Receiver) error {
	if p.cfg.DatagramSender == nil {
		return ErrDatagramSenderNil
	}
	trackID := p.cfg.TrackID
	var seq uint16

	// fallbackStream holds the lazily-opened uni stream used for
	// oversize / send-failed AUs. nil means "not yet opened" OR "the
	// last fallback write tore it down and we should reopen on the
	// next fallback need." We never close it mid-run; the deferred
	// close handles shutdown.
	var fallbackStream SendStream
	defer func() {
		if fallbackStream != nil {
			_ = fallbackStream.Close()
		}
	}()

	// openFallback opens the fallback uni stream if a FallbackOpener
	// is configured. Writes the optional StreamHeader so the receiver
	// can demux to the right track. Returns nil + nil if no opener is
	// configured (caller drops the AU); returns nil + error on open or
	// header-write failure (caller surfaces dropReasonFallbackOpenFailed).
	openFallback := func() (SendStream, error) {
		if p.cfg.FallbackOpener == nil {
			return nil, nil
		}
		openCtx, cancel := context.WithTimeout(ctx, p.cfg.OpenDeadlineKeyframe)
		s, err := p.cfg.FallbackOpener.OpenSendStream(openCtx)
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

	// writeFallback writes one AU to the fallback uni stream using the
	// self-contained feed-frame format (matches runStreamLowLatency).
	// Returns false on write failure so the caller can tear down and
	// surface the right drop reason.
	writeFallback := func(s SendStream, au pubsub.AccessUnit) bool {
		flags := wire.FlagKeyframe // every datagram AU is self-contained
		if au.Discontinuity {
			flags |= wire.FlagDiscontinuity
		}
		var writeErr error
		p.scheduledDo(func() {
			_ = s.SetWriteDeadline(time.Now().Add(p.cfg.WriteDeadline))
			writeErr = writeFeedFramePooled(s, wire.TypeKeyframe, au.PTSMicro, au.Seq, flags, p.pubWall(au), au.Bytes)
		})
		if writeErr != nil {
			return false
		}
		if p.cfg.OnWriteBytes != nil {
			p.cfg.OnWriteBytes(wire.FeedHeaderLen + len(au.Bytes))
		}
		return true
	}

	// fallbackOrDrop is the shared spillover path used by both the
	// oversize and send-failed branches. Tries the fallback stream; on
	// open or write failure, surfaces the appropriate drop reason.
	// dropReason is the reason to surface when no fallback is wired or
	// the fallback open fails (callers pass dropReasonDatagramTooLarge
	// or dropReasonSendFailed).
	fallbackOrDrop := func(au pubsub.AccessUnit, dropReason string) {
		if p.cfg.FallbackOpener == nil {
			if p.cfg.OnAUDropped != nil {
				p.cfg.OnAUDropped(dropReason)
			}
			return
		}
		if fallbackStream == nil {
			s, err := openFallback()
			if err != nil {
				if p.cfg.OnAUDropped != nil {
					p.cfg.OnAUDropped(dropReasonFallbackOpenFailed)
				}
				return
			}
			fallbackStream = s
		}
		if !writeFallback(fallbackStream, au) {
			fallbackStream.CancelWrite(0)
			fallbackStream = nil
			if p.cfg.OnAUDropped != nil {
				p.cfg.OnAUDropped(dropReasonFallbackWriteFailed)
			}
			return
		}
		if p.cfg.OnFallback != nil {
			p.cfg.OnFallback(dropReason)
		}
	}

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

		// Encode envelope + payload into a pooled buffer.
		bufp := datagramBufPool.Get().(*[]byte)
		buf := (*bufp)[:0]

		encoded, err := wire.EncodeDatagram(buf, trackID, seq, au.Bytes)
		if err != nil {
			// Oversize. Route to fallback stream if configured.
			fallbackOrDrop(au, dropReasonDatagramTooLarge)
			*bufp = buf[:0]
			datagramBufPool.Put(bufp)
			seq++
			continue
		}

		var sendErr error
		p.scheduledDo(func() {
			sendErr = p.cfg.DatagramSender.SendDatagram(encoded)
		})
		if sendErr != nil {
			// quic-go DatagramTooLargeError or transport closed. Route
			// to fallback stream if configured.
			fallbackOrDrop(au, dropReasonSendFailed)
			*bufp = buf[:0]
			datagramBufPool.Put(bufp)
			seq++
			continue
		}

		if p.cfg.OnWriteBytes != nil {
			p.cfg.OnWriteBytes(len(encoded))
		}

		seq++
		*bufp = buf[:0]
		datagramBufPool.Put(bufp)
	}
}
