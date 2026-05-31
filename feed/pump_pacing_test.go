package feed

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/track"
)

// slowStream is a SendStream whose Write takes time proportional to the
// byte count, so a large frame written in one shot ties up the caller far
// longer than a small one. Used to show that chunked cooperative writes
// let a high-priority pump preempt a large low-priority frame mid-write.
type slowStream struct {
	mu           sync.Mutex
	buf          bytes.Buffer
	perByteNanos int64
}

func (s *slowStream) Write(p []byte) (int, error) {
	if s.perByteNanos > 0 {
		time.Sleep(time.Duration(int64(len(p)) * s.perByteNanos))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}
func (s *slowStream) Close() error                     { return nil }
func (s *slowStream) SetWriteDeadline(time.Time) error { return nil }
func (s *slowStream) CancelWrite(uint32)               {}

type slowOpener struct{ perByteNanos int64 }

func (o *slowOpener) OpenSendStream(context.Context) (SendStream, error) {
	return &slowStream{perByteNanos: o.perByteNanos}, nil
}

// TestSchedulerPacingPreemptsLargeFrame proves the pacing fix that makes
// the priority scheduler actually bite: a low-priority pump writing a
// large (multi-chunk) frame must NOT hold the shared scheduler worker for
// the whole frame — a higher-priority pump's small frame, submitted while
// the large one is mid-write, must complete first. Without chunked
// cooperative writes the large frame is one indivisible Do() and the
// urgent frame waits the whole thing out.
func TestSchedulerPacingPreemptsLargeFrame(t *testing.T) {
	sched := NewScheduler()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)
	defer sched.Close()

	doneA := make(chan time.Time, 1) // low priority, large frame
	doneB := make(chan time.Time, 1) // high priority, small frame

	bcA := pubsub.NewBroadcaster(0)
	recvA := bcA.Subscribe()
	pA := New(&slowOpener{perByteNanos: 1500}, Config{
		Delivery:     track.DeliveryStreamLowLatency,
		Scheduler:    sched,
		Priority:     uint8(track.PriorityNormal), // bulk lane
		OnWriteBytes: func(int) { select { case doneA <- time.Now(): default: } },
	})

	bcB := pubsub.NewBroadcaster(0)
	recvB := bcB.Subscribe()
	pB := New(&slowOpener{perByteNanos: 0}, Config{
		Delivery:     track.DeliveryStreamLowLatency,
		Scheduler:    sched,
		Priority:     uint8(track.PriorityCritical), // jump-the-queue lane
		OnWriteBytes: func(int) { select { case doneB <- time.Now(): default: } },
	})

	go func() { _ = pA.Run(ctx, recvA) }()
	go func() { _ = pB.Run(ctx, recvB) }()

	// Large low-priority frame: 64 KiB => several pacing chunks, each
	// ~25ms on the slow stream.
	bcA.Publish(pubsub.AccessUnit{Bytes: make([]byte, 64*1024), Keyframe: true, Seq: 1})
	// Let A's first chunk get onto the worker before the urgent frame
	// arrives, so a pass genuinely reflects mid-frame preemption.
	time.Sleep(10 * time.Millisecond)
	bcB.Publish(pubsub.AccessUnit{Bytes: []byte("urgent token"), Keyframe: true, Seq: 1})

	var tA, tB time.Time
	select {
	case tB = <-doneB:
	case <-time.After(5 * time.Second):
		t.Fatal("high-priority pump never finished")
	}
	select {
	case tA = <-doneA:
	case <-time.After(5 * time.Second):
		t.Fatal("low-priority pump never finished")
	}

	if !tB.Before(tA) {
		t.Fatalf("high-priority small frame finished at %v, NOT before the low-priority large frame at %v — scheduler pacing/preemption is not working", tB, tA)
	}
}
