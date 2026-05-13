package feed

import (
	"container/heap"
	"context"
	"sync"
	"sync/atomic"
)

// Scheduler is an app-layer priority scheduler for cross-track write
// ordering. Sits ABOVE the pump layer: pumps push WorkItems via
// Submit; the scheduler's worker goroutine drains them by priority,
// invoking each item's Do() in turn.
//
// Why this exists: QUIC has no native stream-priority API (RFC 9000
// doesn't define one; RFC 9218 is HTTP-layer, not raw QUIC). For a
// mixed-workload session — tokens + video + telemetry sharing one
// connection — we need application-level scheduling to ensure user-
// facing tokens jump ahead of background video bytes during
// congestion.
//
// Priority ordering: LOWER priority value = higher urgency (matches
// RFC 9218 / track.Priority convention). Items with the same priority
// are processed in FIFO order via a monotonic sequence tag.
//
// The scheduler is single-worker by default: one goroutine drains
// the priority queue and calls Do() inline. This serializes writes
// across all registered pumps, which is exactly what we want for a
// shared transport — only one stream can hold the connection's write
// budget at a time, and we choose WHICH stream gets it next.
type Scheduler struct {
	mu       sync.Mutex
	cond     *sync.Cond
	items    workHeap
	seq      uint64
	closed   bool
	started  bool
	shutdown chan struct{} // closed by Close(); selected on by the ctx watcher
	stats    SchedulerStats
}

// SchedulerStats records counters useful for tests and observability.
type SchedulerStats struct {
	Submitted    atomic.Uint64
	Completed    atomic.Uint64
	HighPrioRuns atomic.Uint64 // items at priority <= PriorityHigh
	LowPrioRuns  atomic.Uint64 // items at priority >  PriorityHigh (i.e., normal+below)
}

// PriorityHigh is the priority threshold below which "HighPrioRuns"
// counts an item. Matches track.PriorityHigh (2). The scheduler does
// not import track to avoid a cycle.
const SchedulerPriorityHighThreshold uint8 = 2

// WorkItem is one unit of scheduled work. Do is invoked on the
// scheduler's worker goroutine when it's this item's turn. Do should
// be reasonably fast — long-running work blocks other items behind
// it. For pump workloads, Do is typically a single stream.Write().
type WorkItem struct {
	Priority uint8 // 0=most urgent, 7=background
	Do       func()
}

// NewScheduler returns a Scheduler ready to accept work. Call Start
// to launch the worker goroutine.
func NewScheduler() *Scheduler {
	s := &Scheduler{
		shutdown: make(chan struct{}),
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Start launches the single worker goroutine. The scheduler stops
// when ctx is cancelled OR Close is called. Returns immediately.
func (s *Scheduler) Start(ctx context.Context) {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.mu.Unlock()

	go s.run(ctx)
}

func (s *Scheduler) run(ctx context.Context) {
	// Watcher goroutine: marks the scheduler closed on EITHER ctx
	// cancel OR explicit Close(). Both paths route through the same
	// shutdown channel so neither leaks the other. Without the
	// shutdown channel, Close() without ctx-cancel would leak this
	// goroutine waiting forever on <-ctx.Done().
	go func() {
		select {
		case <-ctx.Done():
		case <-s.shutdown:
		}
		s.mu.Lock()
		s.closed = true
		s.cond.Broadcast()
		s.mu.Unlock()
	}()

	for {
		s.mu.Lock()
		for s.items.Len() == 0 && !s.closed {
			s.cond.Wait()
		}
		if s.closed && s.items.Len() == 0 {
			s.mu.Unlock()
			return
		}
		item := heap.Pop(&s.items).(workEntry)
		s.mu.Unlock()

		if item.priority <= SchedulerPriorityHighThreshold {
			s.stats.HighPrioRuns.Add(1)
		} else {
			s.stats.LowPrioRuns.Add(1)
		}
		item.do()
		s.stats.Completed.Add(1)
	}
}

// Submit enqueues a WorkItem. Returns immediately — Do() runs later
// on the worker goroutine. Non-blocking; if the scheduler is closed,
// Submit silently drops the item (and increments a metric in
// production code).
func (s *Scheduler) Submit(item WorkItem) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.stats.Submitted.Add(1)
	s.seq++
	heap.Push(&s.items, workEntry{
		priority: item.Priority,
		seq:      s.seq,
		do:       item.Do,
	})
	s.cond.Signal()
	s.mu.Unlock()
}

// Close stops the scheduler. Items still in the queue at the moment
// of close are drained before the worker exits. Idempotent —
// concurrent or repeated calls are safe.
func (s *Scheduler) Close() {
	s.mu.Lock()
	// Close-the-channel-once pattern: guarded by mu so concurrent
	// Close() calls don't double-close shutdown.
	select {
	case <-s.shutdown:
		// already closed
	default:
		close(s.shutdown)
	}
	s.mu.Unlock()
}

// Stats returns a copy of the scheduler's counters.
func (s *Scheduler) Stats() *SchedulerStats {
	return &s.stats
}

// ------ internal: priority queue ------

type workEntry struct {
	priority uint8
	seq      uint64 // FIFO tiebreaker for same-priority items
	do       func()
}

type workHeap []workEntry

func (h workHeap) Len() int { return len(h) }
func (h workHeap) Less(i, j int) bool {
	if h[i].priority != h[j].priority {
		return h[i].priority < h[j].priority // lower value = higher priority
	}
	return h[i].seq < h[j].seq
}
func (h workHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *workHeap) Push(x any)   { *h = append(*h, x.(workEntry)) }
func (h *workHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
