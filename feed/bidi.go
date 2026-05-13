package feed

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// BidiStream is the minimal subset of a QUIC bidi stream that
// BidiPerCall needs: write the request bytes, FIN the send side,
// read the response bytes, and cancel cleanly on timeout.
type BidiStream interface {
	io.Writer
	io.Reader

	// CloseWrite sends STREAM FIN on the send side, signaling to the
	// peer that the request is complete. Read side stays open until
	// the peer FINs back with the response.
	CloseWrite() error

	// CancelWrite + CancelRead are used on timeout / context cancel
	// to free up the stream resources on both sides.
	CancelWrite(code uint32)
	CancelRead(code uint32)
}

// BidiOpener opens a new bidi stream per call. Backed by
// webtransport.Session.OpenStreamSync on the wire.
type BidiOpener interface {
	OpenBidiStream(ctx context.Context) (BidiStream, error)
}

// ErrBidiOpenerNil indicates the pump was started without a BidiOpener.
var ErrBidiOpenerNil = errors.New("feed: BidiOpener required for DeliveryBidiPerCall")

// BidiCallTimeout caps how long a single bidi call may take from
// stream-open through response read. Configurable on Config; default
// chosen to be long enough for typical tool-call latency but short
// enough that a stuck call can't leak the stream indefinitely.
const DefaultBidiCallTimeout = 30 * time.Second

// runBidiPerCall opens one bidi stream per AU. Each AU is treated as
// a logical call: write the request bytes, FIN the send side, then
// (optionally) consume the response. Calls run concurrently — the
// pump spawns one goroutine per AU and returns immediately to read
// the next AU off the receive channel, so a slow call never blocks a
// fast call.
//
// This is the structural win over gRPC: each call gets its own QUIC
// stream, so under packet loss the streams have independent recovery
// — one stalled call does not HOL-block the others (which it would
// over TCP-multiplexed HTTP/2).
//
// Response bytes are delivered via the BidiCallResult callback if
// set; if not set, responses are discarded but the stream is still
// drained so the peer's FIN arrives cleanly.
func (p *Pump) runBidiPerCall(ctx context.Context, recv *pubsub.Receiver) error {
	if p.cfg.BidiOpener == nil {
		return ErrBidiOpenerNil
	}

	timeout := p.cfg.BidiCallTimeout
	if timeout == 0 {
		timeout = DefaultBidiCallTimeout
	}

	var wg sync.WaitGroup
	defer wg.Wait() // wait for all in-flight calls to settle before returning

	for au := range recv.Frames() {
		if err := ctx.Err(); err != nil {
			return err
		}

		wg.Add(1)
		atomic.AddInt64(&p.bidiInFlight, 1)
		go func(au pubsub.AccessUnit) {
			defer wg.Done()
			defer atomic.AddInt64(&p.bidiInFlight, -1)

			callCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			stream, err := p.cfg.BidiOpener.OpenBidiStream(callCtx)
			if err != nil {
				if p.cfg.OnAUDropped != nil {
					p.cfg.OnAUDropped("bidi_open_failed")
				}
				return
			}

			// Send request: feed-frame envelope so the receiver can
			// parse it the same way as stream-based AUs.
			flags := wire.FlagKeyframe
			if au.Discontinuity {
				flags |= wire.FlagDiscontinuity
			}
			if err := wire.WriteFeedFrame(stream, wire.TypeKeyframe, au.PTSMicro, au.Seq, flags, au.Bytes); err != nil {
				stream.CancelWrite(0)
				stream.CancelRead(0)
				if p.cfg.OnAUDropped != nil {
					p.cfg.OnAUDropped("bidi_write_failed")
				}
				return
			}
			// FIN the send side so the peer knows the request is
			// complete and can start producing the response.
			_ = stream.CloseWrite()

			if p.cfg.OnWriteBytes != nil {
				p.cfg.OnWriteBytes(wire.FeedHeaderLen + len(au.Bytes))
			}

			// Drain response. Either deliver to the application via
			// callback or just discard so the stream closes cleanly.
			// io.ReadAll returns the bytes it managed to read PLUS
			// any error; we surface both so the app can distinguish
			// partial-due-to-cancel from successful-end-of-stream.
			if p.cfg.OnBidiResponse != nil {
				resp, readErr := io.ReadAll(stream)
				p.cfg.OnBidiResponse(au.Seq, resp, readErr)
			} else {
				_, _ = io.Copy(io.Discard, stream)
			}
		}(au)
	}
	return nil
}
