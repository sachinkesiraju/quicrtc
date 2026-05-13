package pubsub

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func keyframe(seq uint32) AccessUnit {
	return AccessUnit{Bytes: []byte{byte(seq)}, Keyframe: true, Seq: seq, PTSMicro: uint64(seq) * 33333}
}

func pframe(seq uint32) AccessUnit {
	return AccessUnit{Bytes: []byte{byte(seq)}, Keyframe: false, Seq: seq, PTSMicro: uint64(seq) * 33333}
}

func drain(t *testing.T, r *Receiver, want int, timeout time.Duration) []AccessUnit {
	t.Helper()
	got := make([]AccessUnit, 0, want)
	deadline := time.After(timeout)
	for len(got) < want {
		select {
		case au, ok := <-r.Frames():
			if !ok {
				return got
			}
			got = append(got, au)
		case <-deadline:
			return got
		}
	}
	return got
}

func TestBroadcasterBasicFanout(t *testing.T) {
	b := NewBroadcaster(8)
	r1 := b.Subscribe()
	r2 := b.Subscribe()

	b.Publish(keyframe(0))
	b.Publish(pframe(1))
	b.Publish(pframe(2))

	for i, r := range []*Receiver{r1, r2} {
		got := drain(t, r, 3, time.Second)
		if len(got) != 3 {
			t.Fatalf("recv %d: got %d frames, want 3", i, len(got))
		}
		if !got[0].Keyframe || got[0].Seq != 0 {
			t.Fatalf("recv %d: first frame should be keyframe seq=0, got %+v", i, got[0])
		}
		if got[1].Seq != 1 || got[2].Seq != 2 {
			t.Fatalf("recv %d: out of order: %v %v", i, got[1].Seq, got[2].Seq)
		}
	}
}

func TestSubscribeAfterKeyframeReplaysCachedKey(t *testing.T) {
	b := NewBroadcaster(8)
	b.Publish(keyframe(0))
	b.Publish(pframe(1))

	r := b.Subscribe()
	got := drain(t, r, 1, time.Second)
	if len(got) != 1 || !got[0].Keyframe || got[0].Seq != 0 {
		t.Fatalf("late join should replay cached keyframe seq=0, got %+v", got)
	}
}

func TestSubscribeBeforeKeyframeFlagsNeedsKey(t *testing.T) {
	b := NewBroadcaster(8)
	r := b.Subscribe()
	if !r.NeedsKeyframe() {
		t.Fatal("fresh subscriber with no cached keyframe should need a keyframe")
	}
	// P-frames published before any key should be skipped.
	b.Publish(pframe(1))
	b.Publish(pframe(2))
	select {
	case au := <-r.Frames():
		t.Fatalf("should not have received pre-keyframe P-frame, got %+v", au)
	case <-time.After(50 * time.Millisecond):
	}
	// First keyframe clears the flag and is delivered.
	b.Publish(keyframe(3))
	got := drain(t, r, 1, time.Second)
	if len(got) != 1 || !got[0].Keyframe || got[0].Seq != 3 {
		t.Fatalf("expected first keyframe after subscribe, got %+v", got)
	}
	if r.NeedsKeyframe() {
		t.Fatal("flag should clear after keyframe delivery")
	}
}

func TestSlowSubscriberKeyframeEvictsOldest(t *testing.T) {
	b := NewBroadcaster(2) // tiny channel
	r := b.Subscribe()     // queue contains nothing initially (no cached key)

	// First keyframe occupies slot 0, P-frame fills slot 1.
	b.Publish(keyframe(0))
	b.Publish(pframe(1))
	// Channel now full. A new P-frame should be dropped (not evict).
	b.Publish(pframe(2))
	if !r.NeedsKeyframe() {
		t.Fatal("dropped P-frame should mark NeedsKeyframe")
	}
	// A new keyframe should evict the oldest queued frame to fit.
	b.Publish(keyframe(3))
	if r.NeedsKeyframe() {
		t.Fatal("keyframe enqueue should clear NeedsKeyframe")
	}

	got := drain(t, r, 2, time.Second)
	// We should have at most channel-cap (2) frames, ending with seq=3.
	if len(got) != 2 {
		t.Fatalf("expected 2 buffered frames after eviction, got %d (%+v)", len(got), got)
	}
	if got[len(got)-1].Seq != 3 || !got[len(got)-1].Keyframe {
		t.Fatalf("last frame should be keyframe seq=3, got %+v", got[len(got)-1])
	}
}

func TestPFrameSkippedAfterDropUntilNextKeyframe(t *testing.T) {
	b := NewBroadcaster(1)
	r := b.Subscribe()
	b.Publish(keyframe(0)) // fills the 1-slot
	b.Publish(pframe(1))   // dropped, flags NeedsKeyframe
	b.Publish(pframe(2))   // skipped at fanout (NeedsKeyframe is set)
	b.Publish(pframe(3))   // skipped

	// Drain whatever is queued (just the keyframe).
	got1 := drain(t, r, 1, 100*time.Millisecond)
	if len(got1) != 1 || got1[0].Seq != 0 {
		t.Fatalf("expected keyframe seq=0 first, got %+v", got1)
	}
	// No more frames should arrive until a keyframe is published.
	select {
	case au := <-r.Frames():
		t.Fatalf("unexpected frame while NeedsKeyframe: %+v", au)
	case <-time.After(50 * time.Millisecond):
	}
	b.Publish(keyframe(4))
	got2 := drain(t, r, 1, time.Second)
	if len(got2) != 1 || got2[0].Seq != 4 {
		t.Fatalf("expected keyframe seq=4, got %+v", got2)
	}
}

func TestUnsubscribeClosesChannel(t *testing.T) {
	b := NewBroadcaster(4)
	r := b.Subscribe()
	b.Unsubscribe(r)
	if _, ok := <-r.Frames(); ok {
		t.Fatal("channel should be closed after Unsubscribe")
	}
	// Idempotent.
	b.Unsubscribe(r)
}

func TestPublisherNotBlockedBySlowSubscriber(t *testing.T) {
	b := NewBroadcaster(1)
	slow := b.Subscribe() // never reads

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			if i%10 == 0 {
				b.Publish(keyframe(uint32(i)))
			} else {
				b.Publish(pframe(uint32(i)))
			}
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish blocked despite full subscriber channel")
	}
	// slow's channel should be at most chanSize-deep.
	if got := len(slow.ch); got > 1 {
		t.Fatalf("slow subscriber buffered %d > 1 frames", got)
	}
}

func TestConcurrentSubscribeUnsubscribeUnderPublish(t *testing.T) {
	b := NewBroadcaster(8)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var pubCount atomic.Uint64

	wg.Add(1)
	go func() {
		defer wg.Done()
		i := uint32(0)
		for {
			select {
			case <-stop:
				return
			default:
			}
			au := pframe(i)
			if i%30 == 0 {
				au = keyframe(i)
			}
			b.Publish(au)
			pubCount.Add(1)
			i++
		}
	}()

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				select {
				case <-stop:
					return
				default:
				}
				r := b.Subscribe()
				go func(r *Receiver) {
					for range r.Frames() {
					}
				}(r)
				time.Sleep(time.Millisecond)
				b.Unsubscribe(r)
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	if pubCount.Load() == 0 {
		t.Fatal("publisher made no progress")
	}
}

func TestLatestKeyframeReturnsCopyNotInternal(t *testing.T) {
	b := NewBroadcaster(4)
	au := keyframe(0)
	b.Publish(au)
	got := b.LatestKeyframe()
	if got == nil {
		t.Fatal("expected cached keyframe")
	}
	got.Seq = 999 // mutating the copy must not affect the cached one
	got2 := b.LatestKeyframe()
	if got2.Seq != 0 {
		t.Fatalf("internal cache leaked: %+v", got2)
	}
}

// TestMaxAUBytesDropsOversize verifies the per-AU byte cap drops
// AUs larger than the configured cap before they enter any
// receiver's queue. Prevents memory exhaustion from a buggy or
// malicious publisher.
func TestMaxAUBytesDropsOversize(t *testing.T) {
	b := NewBroadcasterWithBytes(4, 1024) // 1 KiB cap
	r := b.Subscribe()
	defer b.Unsubscribe(r)

	// Small AU (under cap): delivered.
	small := AccessUnit{Bytes: make([]byte, 256), Keyframe: true, Seq: 1}
	b.Publish(small)
	select {
	case got := <-r.Frames():
		if got.Seq != 1 {
			t.Fatalf("got seq %d, want 1", got.Seq)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("small AU not delivered")
	}

	// Oversized AU: dropped silently.
	large := AccessUnit{Bytes: make([]byte, 2048), Keyframe: true, Seq: 2}
	b.Publish(large)
	select {
	case got := <-r.Frames():
		t.Fatalf("oversized AU should have been dropped, got seq=%d", got.Seq)
	case <-time.After(100 * time.Millisecond):
		// Expected: nothing arrives.
	}
}

// TestMaxAUBytesZeroDisablesCap verifies that maxAUBytes <= 0
// disables the cap (any size AU passes through).
func TestMaxAUBytesZeroDisablesCap(t *testing.T) {
	b := NewBroadcasterWithBytes(4, 0)
	r := b.Subscribe()
	defer b.Unsubscribe(r)
	huge := AccessUnit{Bytes: make([]byte, 100<<20), Keyframe: true, Seq: 1}
	b.Publish(huge)
	select {
	case got := <-r.Frames():
		if got.Seq != 1 {
			t.Fatalf("got seq %d, want 1", got.Seq)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("huge AU should have been delivered when cap is disabled")
	}
}
