package feed

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/track"
)

// hangingBidiStream simulates a peer that accepts the request (the
// CloseWrite FIN is fine) but NEVER sends a response and never FINs its
// write side — exactly the fire-and-forget shape of a computer-use
// action whose result comes back on a different track. A response Read
// blocks until CancelRead is called, then returns a reset error (this
// mirrors quic-go, where CancelRead wakes a blocked Read with a
// StreamError rather than letting it hang forever).
type hangingBidiStream struct {
	wake        chan struct{} // closed by CancelRead to unblock Read
	once        sync.Once
	closeWrite  atomic.Bool
	cancelRead  atomic.Bool
	cancelWrite atomic.Bool
}

var errStreamReset = errors.New("feed_test: stream reset")

func (s *hangingBidiStream) Write(p []byte) (int, error) { return len(p), nil }
func (s *hangingBidiStream) CloseWrite() error           { s.closeWrite.Store(true); return nil }
func (s *hangingBidiStream) CancelWrite(uint32)          { s.cancelWrite.Store(true) }
func (s *hangingBidiStream) CancelRead(uint32) {
	s.cancelRead.Store(true)
	s.once.Do(func() { close(s.wake) })
}
func (s *hangingBidiStream) Read([]byte) (int, error) {
	<-s.wake // block until CancelRead wakes us; the peer never sends
	return 0, errStreamReset
}

type hangingBidiOpener struct {
	mu      sync.Mutex
	streams []*hangingBidiStream
}

func (o *hangingBidiOpener) OpenBidiStream(context.Context) (BidiStream, error) {
	s := &hangingBidiStream{wake: make(chan struct{})}
	o.mu.Lock()
	o.streams = append(o.streams, s)
	o.mu.Unlock()
	return s, nil
}

func (o *hangingBidiOpener) first() *hangingBidiStream {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.streams) == 0 {
		return nil
	}
	return o.streams[0]
}

// TestBidiFireAndForgetDoesNotBlock proves the reliability fix for the
// computer-use action tail: when no OnBidiResponse is set (fire-and-
// forget), the pump cancels the read side immediately instead of pinning
// the stream — and a concurrent-stream slot — waiting for a response that
// never arrives. Before the fix, the response drain blocked until the
// peer FIN'd (which a one-way action never does), leaking the slot and
// starving OpenBidiStream under load. The BidiCallTimeout is set
// deliberately long here: the fix must NOT depend on it firing.
func TestBidiFireAndForgetDoesNotBlock(t *testing.T) {
	opener := &hangingBidiOpener{}
	bc := pubsub.NewBroadcaster(0)
	recv := bc.Subscribe()
	p := New(nil, Config{
		Delivery:        track.DeliveryBidiPerCall,
		BidiOpener:      opener,
		BidiCallTimeout: 30 * time.Second, // long on purpose
		// OnBidiResponse deliberately nil => fire-and-forget
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { _ = p.Run(ctx, recv); close(done) }()

	bc.Publish(pubsub.AccessUnit{Bytes: []byte("click 10,20"), Keyframe: true, Seq: 1})

	// The call must release its read side fast (CancelRead), NOT wait on
	// the 30s timeout for a response that will never arrive.
	deadline := time.After(2 * time.Second)
	for {
		if s := opener.first(); s != nil && s.cancelRead.Load() {
			if !s.closeWrite.Load() {
				t.Error("request send side was not FIN'd (CloseWrite)")
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("fire-and-forget bidi call did not release its read side promptly; it is blocked waiting for a response that never arrives")
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	<-done
}

// TestBidiResponseReadIsBoundedByTimeout proves the now-enforced
// contract that BidiCallTimeout bounds open-THROUGH-response-read. The
// peer never sends a response; with OnBidiResponse set, the call must
// still complete (with a non-nil error) within ~BidiCallTimeout instead
// of hanging forever (the read side previously had no deadline wired).
func TestBidiResponseReadIsBoundedByTimeout(t *testing.T) {
	opener := &hangingBidiOpener{}
	bc := pubsub.NewBroadcaster(0)
	recv := bc.Subscribe()

	gotErr := make(chan error, 1)
	p := New(nil, Config{
		Delivery:        track.DeliveryBidiPerCall,
		BidiOpener:      opener,
		BidiCallTimeout: 200 * time.Millisecond,
		OnBidiResponse: func(_ uint32, _ []byte, err error) {
			select {
			case gotErr <- err:
			default:
			}
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { _ = p.Run(ctx, recv); close(done) }()

	start := time.Now()
	bc.Publish(pubsub.AccessUnit{Bytes: []byte("x"), Keyframe: true, Seq: 1})

	select {
	case err := <-gotErr:
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("response read was not bounded by BidiCallTimeout; took %s", elapsed)
		}
		if err == nil {
			t.Error("expected a non-nil error for a response that never arrived")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("OnBidiResponse never fired; the response read hung past the call timeout")
	}

	cancel()
	<-done
}
