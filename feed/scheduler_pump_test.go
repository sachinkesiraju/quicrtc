package feed

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/track"
)

// slowSender simulates a transport whose SendDatagram blocks for
// blockFor. Records the per-pump send order so a test can assert
// the higher-priority pump's items drain first under contention.
type slowSender struct {
	name     string
	blockFor time.Duration
	order    *sendOrder
}

type sendOrder struct {
	mu   sync.Mutex
	seen []string
}

func (o *sendOrder) record(name string) {
	o.mu.Lock()
	o.seen = append(o.seen, name)
	o.mu.Unlock()
}

func (o *sendOrder) Names() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.seen...)
}

func (s *slowSender) SendDatagram(_ []byte) error {
	if s.blockFor > 0 {
		time.Sleep(s.blockFor)
	}
	s.order.record(s.name)
	return nil
}

// TestSchedulerPrioritizesAcrossPumps proves the scheduler wire-up
// actually changes write order under contention. The setup:
//
//  1. Pre-fill the scheduler with a long-blocking "warmup" item so
//     subsequent submissions queue behind it.
//  2. Submit one low-priority AU via the low pump.
//  3. Submit one high-priority AU via the high pump.
//  4. Release the warmup. The scheduler must now pick the high AU
//     before the low AU even though low was submitted first.
//
// This is the strict-preemption test: at the moment the scheduler is
// free to pick, both items are queued and priority must win. Without
// the Scheduler wire-up, the test would race on goroutine scheduling
// (no shared ordering point) and could see either order.
func TestSchedulerPrioritizesAcrossPumps(t *testing.T) {
	if testing.Short() {
		t.Skip("scheduler integration test in short mode")
	}

	order := &sendOrder{}
	senderLow := &slowSender{name: "low", order: order}
	senderHigh := &slowSender{name: "high", order: order}

	sched := NewScheduler()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)
	defer sched.Close()

	// Warmup item: holds the scheduler busy so the next two
	// submissions queue behind it. Released by signaling release.
	release := make(chan struct{})
	warmupStarted := make(chan struct{})
	sched.Submit(WorkItem{
		Priority: 0, // most urgent so it definitely runs first
		Do: func() {
			close(warmupStarted)
			<-release
		},
	})
	<-warmupStarted

	bLow := pubsub.NewBroadcaster(16)
	bHigh := pubsub.NewBroadcaster(16)
	rLow := bLow.Subscribe()
	rHigh := bHigh.Subscribe()

	pLow := New(nil, Config{
		Delivery:       track.DeliveryDatagramOrStream,
		DatagramSender: senderLow,
		Scheduler:      sched,
		Priority:       5,
	})
	pHigh := New(nil, Config{
		Delivery:       track.DeliveryDatagramOrStream,
		DatagramSender: senderHigh,
		Scheduler:      sched,
		Priority:       1,
	})

	doneLow := make(chan struct{})
	doneHigh := make(chan struct{})
	go func() {
		_ = pLow.Run(ctx, rLow)
		close(doneLow)
	}()
	go func() {
		_ = pHigh.Run(ctx, rHigh)
		close(doneHigh)
	}()

	// Publish low FIRST so its Submit lands on the scheduler before
	// the high pump's. If priority weren't honored, FIFO ordering
	// would drain low first.
	bLow.Publish(pubsub.AccessUnit{Bytes: []byte{0x01}, Keyframe: true, Seq: 1})

	// Wait until the low pump has reached Submit (i.e., its item is
	// in the scheduler's queue). Cheap proxy: the scheduler's
	// Submitted counter should tick from 1 (warmup) to 2.
	waitForSubmitted := func(n uint64) {
		deadline := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(deadline) {
			if sched.Stats().Submitted.Load() >= n {
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatalf("scheduler Submitted did not reach %d (got %d)", n, sched.Stats().Submitted.Load())
	}
	waitForSubmitted(2)

	// Now publish high. Its Submit lands AFTER low's in submission
	// order but with higher priority.
	bHigh.Publish(pubsub.AccessUnit{Bytes: []byte{0xff}, Keyframe: true, Seq: 1})
	waitForSubmitted(3)

	// Release the warmup. Scheduler is now free to pick between the
	// queued low and high items.
	close(release)

	// Wait for both sends to land.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(order.Names()) == 2 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-doneLow
	<-doneHigh
	bLow.Unsubscribe(rLow)
	bHigh.Unsubscribe(rHigh)

	got := order.Names()
	if len(got) != 2 {
		t.Fatalf("expected 2 sends, got %d (%v)", len(got), got)
	}
	if got[0] != "high" {
		t.Fatalf("scheduler picked %q first; high-priority pump should preempt low: %v", got[0], got)
	}
	t.Logf("scheduler order under contention: %v", got)
}

// TestScheduledDoDoesNotBlockAfterClose is the regression test for
// the goroutine-leak found in code review: if Submit landed after the
// scheduler was closed, scheduledDo blocked forever waiting on a done
// channel that no Do would ever close. The fix has Submit report
// closed-state via its return value; scheduledDo falls through to
// running fn inline so the pump can still observe its own ctx-cancel
// and exit cleanly.
func TestScheduledDoDoesNotBlockAfterClose(t *testing.T) {
	sched := NewScheduler()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched.Start(ctx)
	sched.Close()

	// Give the watcher a moment to flip s.closed.
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		// Touching Submit is the only way to observe closed-state.
		if !sched.Submit(WorkItem{Priority: 0, Do: func() {}}) {
			break
		}
		time.Sleep(time.Millisecond)
	}

	p := &Pump{cfg: Config{Scheduler: sched, Priority: 3}}
	var ran atomic.Bool
	done := make(chan struct{})
	go func() {
		p.scheduledDo(func() { ran.Store(true) })
		close(done)
	}()
	select {
	case <-done:
		if !ran.Load() {
			t.Fatal("fn did not run inline after Submit-failed fallback")
		}
	case <-time.After(time.Second):
		t.Fatal("scheduledDo blocked after scheduler Close (leak regression)")
	}
}

// TestSchedulerNotSetMeansFIFO confirms the legacy path: with no
// Scheduler, two pumps drain independently. The Priority hint is
// emitted via OnStreamOpened but does not alter write ordering.
// This test exercises that no-Scheduler path: it must not panic,
// must deliver all AUs, and must not invoke the scheduler internals.
func TestSchedulerNotSetMeansFIFO(t *testing.T) {
	order := &sendOrder{}
	senderA := &slowSender{name: "A", order: order}
	senderB := &slowSender{name: "B", order: order}

	bA := pubsub.NewBroadcaster(16)
	bB := pubsub.NewBroadcaster(16)
	rA := bA.Subscribe()
	rB := bB.Subscribe()

	var streamOpens atomic.Int64
	pA := New(nil, Config{
		Delivery:       track.DeliveryDatagramOrStream,
		DatagramSender: senderA,
		Priority:       1,
		OnStreamOpened: func(string, uint8) { streamOpens.Add(1) },
	})
	pB := New(nil, Config{
		Delivery:       track.DeliveryDatagramOrStream,
		DatagramSender: senderB,
		Priority:       5,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	doneA := make(chan struct{})
	doneB := make(chan struct{})
	go func() {
		_ = pA.Run(ctx, rA)
		close(doneA)
	}()
	go func() {
		_ = pB.Run(ctx, rB)
		close(doneB)
	}()

	for i := 0; i < 3; i++ {
		bA.Publish(pubsub.AccessUnit{Bytes: []byte{0xa, byte(i)}, Keyframe: true, Seq: uint32(i + 1)})
		bB.Publish(pubsub.AccessUnit{Bytes: []byte{0xb, byte(i)}, Keyframe: true, Seq: uint32(i + 1)})
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(order.Names()) == 6 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-doneA
	<-doneB
	bA.Unsubscribe(rA)
	bB.Unsubscribe(rB)

	got := order.Names()
	if len(got) != 6 {
		t.Fatalf("expected 6 sends, got %d (%v)", len(got), got)
	}
}
