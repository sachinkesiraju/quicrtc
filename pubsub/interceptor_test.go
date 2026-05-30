package pubsub

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestInterceptorPassthrough(t *testing.T) {
	b := NewBroadcaster(10)
	r := b.Subscribe()

	var seen atomic.Int64
	remove := b.AddInterceptor(func(au AccessUnit) (AccessUnit, error) {
		seen.Add(1)
		return au, nil
	})
	defer remove()

	b.Publish(AccessUnit{Bytes: []byte("hello"), Keyframe: true, Seq: 1})

	got := <-r.Frames()
	if string(got.Bytes) != "hello" {
		t.Fatalf("payload mismatch: got %q", got.Bytes)
	}
	if seen.Load() != 1 {
		t.Fatalf("interceptor called %d times, want 1", seen.Load())
	}
}

func TestInterceptorMutate(t *testing.T) {
	b := NewBroadcaster(10)
	r := b.Subscribe()

	b.AddInterceptor(func(au AccessUnit) (AccessUnit, error) {
		au.Bytes = []byte("redacted")
		return au, nil
	})

	b.Publish(AccessUnit{Bytes: []byte("secret"), Keyframe: true, Seq: 1})

	got := <-r.Frames()
	if string(got.Bytes) != "redacted" {
		t.Fatalf("interceptor mutation lost: got %q", got.Bytes)
	}
}

func TestInterceptorDropAU(t *testing.T) {
	b := NewBroadcaster(10)
	r := b.Subscribe()

	b.AddInterceptor(func(au AccessUnit) (AccessUnit, error) {
		if string(au.Bytes) == "block" {
			return au, ErrDropAU
		}
		return au, nil
	})

	b.Publish(AccessUnit{Bytes: []byte("ok"), Keyframe: true, Seq: 1})
	b.Publish(AccessUnit{Bytes: []byte("block"), Keyframe: true, Seq: 2})
	b.Publish(AccessUnit{Bytes: []byte("ok2"), Keyframe: true, Seq: 3})

	got1 := <-r.Frames()
	got2 := <-r.Frames()
	if string(got1.Bytes) != "ok" || string(got2.Bytes) != "ok2" {
		t.Fatalf("blocked AU leaked: %q %q", got1.Bytes, got2.Bytes)
	}

	select {
	case extra := <-r.Frames():
		t.Fatalf("unexpected extra AU: %q", extra.Bytes)
	default:
	}
}

func TestInterceptorChainOrder(t *testing.T) {
	b := NewBroadcaster(10)
	r := b.Subscribe()

	b.AddInterceptor(func(au AccessUnit) (AccessUnit, error) {
		au.Bytes = append(au.Bytes, '1')
		return au, nil
	})
	b.AddInterceptor(func(au AccessUnit) (AccessUnit, error) {
		au.Bytes = append(au.Bytes, '2')
		return au, nil
	})

	b.Publish(AccessUnit{Bytes: []byte("a"), Keyframe: true, Seq: 1})

	got := <-r.Frames()
	if string(got.Bytes) != "a12" {
		t.Fatalf("chain order wrong: got %q want %q", got.Bytes, "a12")
	}
}

func TestInterceptorRemove(t *testing.T) {
	b := NewBroadcaster(10)
	r := b.Subscribe()

	var calls atomic.Int64
	remove := b.AddInterceptor(func(au AccessUnit) (AccessUnit, error) {
		calls.Add(1)
		return au, nil
	})

	b.Publish(AccessUnit{Bytes: []byte("a"), Keyframe: true, Seq: 1})
	remove()
	b.Publish(AccessUnit{Bytes: []byte("b"), Keyframe: true, Seq: 2})

	<-r.Frames()
	<-r.Frames()

	if calls.Load() != 1 {
		t.Fatalf("interceptor called %d times after remove, want 1", calls.Load())
	}

	remove()
}

func TestInterceptorSizeCapEnforcedAfterChain(t *testing.T) {
	b := NewBroadcasterWithBytes(10, 100)
	r := b.Subscribe()

	b.AddInterceptor(func(au AccessUnit) (AccessUnit, error) {
		au.Bytes = make([]byte, 200)
		return au, nil
	})

	b.Publish(AccessUnit{Bytes: []byte("small"), Keyframe: true, Seq: 1})

	select {
	case got := <-r.Frames():
		t.Fatalf("oversize AU leaked through: len=%d", len(got.Bytes))
	default:
	}
}

func TestInterceptorConcurrentRemoveDuringPublish(t *testing.T) {
	b := NewBroadcaster(100)
	r := b.Subscribe()

	var calls atomic.Int64
	remove := b.AddInterceptor(func(au AccessUnit) (AccessUnit, error) {
		calls.Add(1)
		return au, nil
	})

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				b.Publish(AccessUnit{Bytes: []byte("x"), Keyframe: true, Seq: 1})
				<-r.Frames()
			}
		}
	}()

	// Ensure the counting interceptor has fired at least once before we remove
	// it, so the concurrent add/remove below races against live publishes and
	// the calls>=1 assertion is deterministic rather than scheduler-dependent.
	for calls.Load() == 0 {
		runtime.Gosched()
	}

	for i := 0; i < 100; i++ {
		_ = b.AddInterceptor(func(au AccessUnit) (AccessUnit, error) { return au, nil })
	}
	remove()

	close(stop)
	wg.Wait()

	if calls.Load() < 1 {
		t.Fatalf("interceptor never called")
	}
}

func TestInterceptorNilNoop(t *testing.T) {
	b := NewBroadcaster(10)
	remove := b.AddInterceptor(nil)
	remove()

	r := b.Subscribe()
	b.Publish(AccessUnit{Bytes: []byte("x"), Keyframe: true, Seq: 1})
	got := <-r.Frames()
	if string(got.Bytes) != "x" {
		t.Fatalf("nil interceptor disrupted fanout")
	}
}

func TestInterceptorGenericError(t *testing.T) {
	b := NewBroadcaster(10)
	r := b.Subscribe()

	customErr := errors.New("custom drop reason")
	b.AddInterceptor(func(au AccessUnit) (AccessUnit, error) {
		return au, customErr
	})

	b.Publish(AccessUnit{Bytes: []byte("x"), Keyframe: true, Seq: 1})

	select {
	case got := <-r.Frames():
		t.Fatalf("AU leaked despite generic error: %q", got.Bytes)
	default:
	}
}
