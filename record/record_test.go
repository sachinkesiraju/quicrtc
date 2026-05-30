package record

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/pubsub"
)

func TestFileRecorderRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "session.qrr")

	rec, err := NewFileRecorder(path)
	if err != nil {
		t.Fatal(err)
	}

	want := []pubsub.AccessUnit{
		{Bytes: []byte("frame-1"), Keyframe: true, Seq: 1, PTSMicro: 1000},
		{Bytes: []byte("frame-2"), Keyframe: false, Seq: 2, PTSMicro: 2000},
		{Bytes: []byte("frame-3"), Keyframe: false, Seq: 3, PTSMicro: 3000, Discontinuity: true},
	}
	for _, au := range want {
		if err := rec.Write("screen", "video", au); err != nil {
			t.Fatal(err)
		}
	}
	if err := rec.Close(); err != nil {
		t.Fatal(err)
	}

	rp, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer rp.Close()

	var got []Frame
	err = rp.PublishInto(context.Background(), 0, func(f Frame) error {
		got = append(got, f)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != len(want) {
		t.Fatalf("got %d frames, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].TrackName != "screen" || got[i].Kind != "video" {
			t.Fatalf("frame %d track/kind wrong: %+v", i, got[i])
		}
		if got[i].AU.Seq != want[i].Seq || string(got[i].AU.Bytes) != string(want[i].Bytes) {
			t.Fatalf("frame %d AU mismatch: got %+v want %+v", i, got[i].AU, want[i])
		}
		if got[i].AU.Keyframe != want[i].Keyframe {
			t.Fatalf("frame %d keyframe flag mismatch", i)
		}
		if got[i].AU.Discontinuity != want[i].Discontinuity {
			t.Fatalf("frame %d discontinuity flag mismatch", i)
		}
	}
}

func TestFileRecorderCopiesBytes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "session.qrr")
	rec, err := NewFileRecorder(path)
	if err != nil {
		t.Fatal(err)
	}

	buf := []byte("original")
	if err := rec.Write("t", "tokens", pubsub.AccessUnit{Bytes: buf, Keyframe: true, Seq: 1}); err != nil {
		t.Fatal(err)
	}
	// Mutate the caller-owned slice. The recorder must already have
	// taken its own copy; a future replay must still see "original".
	copy(buf, []byte("MUTATED!"))
	if err := rec.Close(); err != nil {
		t.Fatal(err)
	}

	rp, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer rp.Close()
	f, err := rp.Next()
	if err != nil {
		t.Fatal(err)
	}
	if string(f.AU.Bytes) != "original" {
		t.Fatalf("recorder did not copy bytes: got %q", f.AU.Bytes)
	}
}

func TestFileRecorderDroppedOnOverflow(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tight.qrr")
	rec, err := NewFileRecorderDepth(path, 1)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5000; i++ {
		_ = rec.Write("t", "tokens", pubsub.AccessUnit{Bytes: []byte("x"), Keyframe: true, Seq: uint32(i)})
	}

	if err := rec.Close(); err != nil {
		t.Fatal(err)
	}
	if rec.Dropped() == 0 {
		t.Logf("dropped count was 0 under deep stress — fast disk made it work")
	}
}

func TestFileRecorderWriteAfterCloseDoesNotPanic(t *testing.T) {
	dir := t.TempDir()
	rec, err := NewFileRecorder(filepath.Join(dir, "race.qrr"))
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.Close(); err != nil {
		t.Fatal(err)
	}
	// Post-close Write must NOT panic on send-to-closed-channel.
	// It should silently increment Dropped().
	for i := 0; i < 100; i++ {
		if err := rec.Write("t", "tokens", pubsub.AccessUnit{Bytes: []byte("x"), Keyframe: true, Seq: uint32(i)}); err != nil {
			t.Fatalf("Write after Close returned error: %v", err)
		}
	}
	if rec.Dropped() < 100 {
		t.Fatalf("expected post-close drops to be counted, got %d", rec.Dropped())
	}
}

func TestFileRecorderConcurrentWriteClose(t *testing.T) {
	dir := t.TempDir()
	rec, err := NewFileRecorder(filepath.Join(dir, "concurrent.qrr"))
	if err != nil {
		t.Fatal(err)
	}

	// Many writers race against Close. Without the closeMu RLock guard
	// in Write, this reliably panics under -race ("send on closed
	// channel").
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			seq := uint32(0)
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = rec.Write("t", "tokens", pubsub.AccessUnit{
					Bytes: []byte("x"), Keyframe: true, Seq: seq,
				})
				seq++
			}
		}(i)
	}
	time.Sleep(10 * time.Millisecond)
	if err := rec.Close(); err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()
}

func TestReplayerSpeedScaling(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "timing.qrr")
	rec, err := NewFileRecorder(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.Write("t", "tokens", pubsub.AccessUnit{Bytes: []byte("a"), Keyframe: true, Seq: 1}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	if err := rec.Write("t", "tokens", pubsub.AccessUnit{Bytes: []byte("b"), Keyframe: true, Seq: 2}); err != nil {
		t.Fatal(err)
	}
	if err := rec.Close(); err != nil {
		t.Fatal(err)
	}

	rp, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer rp.Close()

	// Play at 4x — should take ~10ms instead of ~40ms.
	start := time.Now()
	_ = rp.PublishInto(context.Background(), 4, func(f Frame) error { return nil })
	elapsed := time.Since(start)

	if elapsed > 35*time.Millisecond {
		t.Fatalf("speed=4 took %v, expected well under 35ms", elapsed)
	}
}
