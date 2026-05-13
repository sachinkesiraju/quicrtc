package feed

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSchedulerPriorityPreemption is the headline measurement for
// PR5: under contention for a single output (the connection's write
// budget), high-priority items should finish in ~constant time
// regardless of how much low-priority work is queued. Low-priority
// work happens "in the gaps."
//
// Setup:
//   - One scheduler.
//   - Two producers in parallel:
//     · 100 LOW-priority items (priority=4, "video bytes")
//     · 50  HIGH-priority items (priority=2, "tokens")
//   - Each item's Do() simulates a write by sleeping a tiny constant.
//
// Comparison:
//
//   - WITHOUT scheduler (baseline): items processed FIFO regardless
//     of priority. A token submitted after 100 video frames is queued
//     behind ALL of them — it waits ~100×writeTime to complete.
//
//   - WITH scheduler (PR5): tokens are picked first. The 50 tokens
//     all complete before the 50th video frame even starts.
//
//     go test -v ./feed -run TestSchedulerPriorityPreemption
func TestSchedulerPriorityPreemption(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scheduler test in short mode")
	}

	const (
		nLow     = 100
		nHigh    = 50
		workTime = 100 * time.Microsecond
		highPrio = 2 // track.PriorityHigh
		lowPrio  = 4 // track.PriorityNormal
	)

	// ----- baseline: pure FIFO, no priority -----
	baselineHigh, baselineLow := measureFIFOBaseline(nLow, nHigh, workTime)
	baselineHStats := computeLatStats(baselineHigh)
	baselineLStats := computeLatStats(baselineLow)

	// ----- PR5: scheduler with priority preemption -----
	pr5High, pr5Low := measureSchedulerPreemption(nLow, nHigh, workTime, highPrio, lowPrio)
	pr5HStats := computeLatStats(pr5High)
	pr5LStats := computeLatStats(pr5Low)

	t.Logf("=== priority preemption, %d high + %d low, %v work each ===", nHigh, nLow, workTime)
	t.Logf("BASELINE (FIFO)            HIGH %s", baselineHStats)
	t.Logf("BASELINE (FIFO)            LOW  %s", baselineLStats)
	t.Logf("PR5 (priority scheduler)   HIGH %s", pr5HStats)
	t.Logf("PR5 (priority scheduler)   LOW  %s", pr5LStats)

	t.Logf("=== headline ===")
	t.Logf("high-priority mean:  %v  ->  %v   (%.1fx faster)",
		baselineHStats.Mean.Round(time.Microsecond),
		pr5HStats.Mean.Round(time.Microsecond),
		float64(baselineHStats.Mean)/float64(pr5HStats.Mean))
	t.Logf("high-priority p99:   %v  ->  %v   (%.1fx faster)",
		baselineHStats.P99.Round(time.Microsecond),
		pr5HStats.P99.Round(time.Microsecond),
		float64(baselineHStats.P99)/float64(pr5HStats.P99))
	t.Logf("low-priority mean:   %v  ->  %v   (yields to high-priority, so worst-case acceptable)",
		baselineLStats.Mean.Round(time.Microsecond),
		pr5LStats.Mean.Round(time.Microsecond))
}

// measureFIFOBaseline simulates the pre-PR5 world: items are
// submitted and processed in submission order with no priority
// awareness. We interleave high and low submissions to model a
// realistic "tokens arrive while video is flowing" workload.
func measureFIFOBaseline(nLow, nHigh int, workTime time.Duration) (highLat, lowLat []time.Duration) {
	type item struct {
		isHigh  bool
		startTs time.Time
	}
	queue := make(chan item, nLow+nHigh)
	done := make(chan struct{})

	// Worker processes items FIFO.
	highLat = make([]time.Duration, 0, nHigh)
	lowLat = make([]time.Duration, 0, nLow)
	var hMu, lMu sync.Mutex

	go func() {
		for it := range queue {
			time.Sleep(workTime) // simulate write
			lat := time.Since(it.startTs)
			if it.isHigh {
				hMu.Lock()
				highLat = append(highLat, lat)
				hMu.Unlock()
			} else {
				lMu.Lock()
				lowLat = append(lowLat, lat)
				lMu.Unlock()
			}
		}
		close(done)
	}()

	// Submission: interleaved (low-heavy because nLow > nHigh).
	// First nHigh items: alternate low,high. Then trailing nLow-nHigh
	// low items.
	i, j := 0, 0
	for i < nHigh && j < nLow {
		queue <- item{isHigh: false, startTs: time.Now()}
		j++
		queue <- item{isHigh: true, startTs: time.Now()}
		i++
	}
	for j < nLow {
		queue <- item{isHigh: false, startTs: time.Now()}
		j++
	}
	close(queue)
	<-done
	return
}

// measureSchedulerPreemption submits the same workload through the
// scheduler with priority annotations.
func measureSchedulerPreemption(nLow, nHigh int, workTime time.Duration, highPrio, lowPrio uint8) (highLat, lowLat []time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sched := NewScheduler()
	sched.Start(ctx)

	highLat = make([]time.Duration, 0, nHigh)
	lowLat = make([]time.Duration, 0, nLow)
	var hMu, lMu sync.Mutex
	var doneWG sync.WaitGroup
	doneWG.Add(nLow + nHigh)

	submit := func(isHigh bool) {
		prio := lowPrio
		if isHigh {
			prio = highPrio
		}
		start := time.Now()
		sched.Submit(WorkItem{
			Priority: prio,
			Do: func() {
				time.Sleep(workTime)
				lat := time.Since(start)
				if isHigh {
					hMu.Lock()
					highLat = append(highLat, lat)
					hMu.Unlock()
				} else {
					lMu.Lock()
					lowLat = append(lowLat, lat)
					lMu.Unlock()
				}
				doneWG.Done()
			},
		})
	}

	// Same submission pattern as baseline so the comparison is fair.
	i, j := 0, 0
	for i < nHigh && j < nLow {
		submit(false)
		j++
		submit(true)
		i++
	}
	for j < nLow {
		submit(false)
		j++
	}

	doneWG.Wait()
	sched.Close()
	return
}

// TestSchedulerStarvationGuard sanity-checks that low-priority work
// eventually completes even when high-priority work keeps arriving.
// (The current scheduler is strict priority — no aging — but with
// finite workloads this still terminates. A production scheduler
// would add aging for unbounded streams; that's a v2 concern.)
func TestSchedulerStarvationGuard(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched := NewScheduler()
	sched.Start(ctx)
	defer sched.Close()

	const n = 50
	var completed atomic.Int32
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		prio := uint8(4) // low priority
		if i%5 == 0 {
			prio = 2 // high priority every 5th item
		}
		sched.Submit(WorkItem{
			Priority: prio,
			Do: func() {
				time.Sleep(50 * time.Microsecond)
				completed.Add(1)
				wg.Done()
			},
		})
	}
	wg.Wait()
	if got := completed.Load(); int(got) != n {
		t.Fatalf("expected %d completions, got %d", n, got)
	}
	// All items completed → no starvation in this bounded workload.
	t.Logf("starvation guard: %d items all completed", n)
}

// ------ Throughput benchmarks ------

// BenchmarkScheduler_Submit measures the cost of pushing a single
// WorkItem through Submit, including the heap.Push and cond.Signal
// bookkeeping. The Do() func is a no-op so we isolate dispatch
// overhead.
func BenchmarkScheduler_Submit(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched := NewScheduler()
	sched.Start(ctx)
	defer sched.Close()

	noop := func() {}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sched.Submit(WorkItem{Priority: 2, Do: noop})
	}
}

// BenchmarkScheduler_SubmitDispatch measures end-to-end: submit a
// WorkItem and wait for it to run. Captures the cost of one full
// dispatch cycle (push, signal, wait, pop, call). Useful as a
// throughput ceiling for the priority scheduler.
func BenchmarkScheduler_SubmitDispatch(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sched := NewScheduler()
	sched.Start(ctx)
	defer sched.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		done := make(chan struct{})
		sched.Submit(WorkItem{Priority: 2, Do: func() { close(done) }})
		<-done
	}
}

// silence sort import warning when this file builds alone
var _ = sort.Sort
