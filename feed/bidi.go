package feed

import (
	"context"
	"errors"
	"fmt"
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

// DefaultMaxBidiResponse caps the bytes read off each bidi response
// stream. Bounds memory exposure from a hostile authenticated peer
// that responds with a large payload (io.ReadAll would otherwise
// buffer until OOM). Matches wire.MaxFeedPayload — anything that fits
// the wire fits this read.
const DefaultMaxBidiResponse = wire.MaxFeedPayload

// ErrBidiResponseTooLarge is surfaced via OnBidiResponse when the peer's
// response exceeded MaxBidiResponse and was truncated.
var ErrBidiResponseTooLarge = errors.New("feed: bidi response exceeded MaxBidiResponse")

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
	maxResp := p.cfg.MaxBidiResponse
	if maxResp <= 0 {
		maxResp = DefaultMaxBidiResponse
	}

	var wg sync.WaitGroup
	defer wg.Wait() // wait for all in-flight calls to settle before returning

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

			// Prepend a TypeStreamHeader so the receiver can route
			// this bidi stream to the right track queue. Mirrors the
			// uni-stream path in pump.go. Empty TrackName preserves
			// single-track wire compat (receivers route to "primary").
			if p.cfg.TrackName != "" {
				if err := wire.WriteStreamHeader(stream, p.cfg.TrackName); err != nil {
					stream.CancelWrite(0)
					stream.CancelRead(0)
					if p.cfg.OnAUDropped != nil {
						p.cfg.OnAUDropped("bidi_header_failed")
					}
					return
				}
			}

			// Send request: feed-frame envelope so the receiver can
			// parse it the same way as stream-based AUs.
			flags := wire.FlagKeyframe
			if au.Discontinuity {
				flags |= wire.FlagDiscontinuity
			}
			if pw := p.pubWall(au); pw != 0 {
				err = wire.WriteFeedFrameWall(stream, wire.TypeKeyframe, au.PTSMicro, au.Seq, flags, pw, au.Bytes)
			} else {
				err = wire.WriteFeedFrame(stream, wire.TypeKeyframe, au.PTSMicro, au.Seq, flags, au.Bytes)
			}
			if err != nil {
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

			// Response handling.
			//
			// Fire-and-forget (OnBidiResponse == nil): the caller does
			// not consume responses — a computer-use action whose result
			// comes back on a different track is the canonical case. We
			// MUST NOT block reading a response that may never arrive.
			// The read side has no deadline of its own, so a peer that
			// never FINs would pin this stream — and a concurrent-stream
			// slot — indefinitely; under a fire-every-AU workload that
			// slot leak is what starved OpenBidiStream and produced the
			// multi-second action tail. CloseWrite already delivered the
			// request, so the call is complete: cancel the read now.
			if p.cfg.OnBidiResponse == nil {
				stream.CancelRead(0)
				return
			}

			// Response wanted: read it, bounded by MaxBidiResponse so a
			// hostile peer can't OOM us, AND bounded in time by callCtx
			// so BidiCallTimeout actually caps open-through-read the way
			// this type's doc comment promises. The BidiStream interface
			// exposes no SetReadDeadline, so a watchdog cancels the read
			// when the call budget expires; readDone stops the watchdog
			// once the read returns on its own. Read at most maxResp+1
			// bytes so we can detect the exact-boundary overflow case.
			readDone := make(chan struct{})
			go func() {
				select {
				case <-callCtx.Done():
					stream.CancelRead(0)
				case <-readDone:
				}
			}()
			resp, readErr := io.ReadAll(io.LimitReader(stream, maxResp+1))
			close(readDone)
			if int64(len(resp)) > maxResp {
				resp = resp[:maxResp]
				stream.CancelRead(0)
				if readErr == nil {
					readErr = fmt.Errorf("%w: limit=%d", ErrBidiResponseTooLarge, maxResp)
				}
			}
			p.cfg.OnBidiResponse(au.Seq, resp, readErr)
		}(au)
	}
}
