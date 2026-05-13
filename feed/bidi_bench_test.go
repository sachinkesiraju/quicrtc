package feed

import (
	"bytes"
	"context"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/track"
)

// fakeBidiStream simulates a bidi stream with controllable per-stream
// latency. Each stream "responds" after responseDelay (drawn from
// per-call config) with a fixed response payload. The "stalled" flag
// makes that one stream block until the test releases it — used to
// prove that parallel calls don't HOL-block under the BidiPerCall
// design (one stalled call doesn't affect the others).
type fakeBidiStream struct {
	id            int
	mu            sync.Mutex
	requestBuf    bytes.Buffer
	responseBuf   bytes.Buffer
	responseDelay time.Duration
	finned        bool
	cancelledW    bool
	cancelledR    bool
	stalled       *atomic.Bool // if set and true, response blocks
	released      chan struct{}
	finCh         chan struct{}
}

func newFakeBidiStream(id int, responseDelay time.Duration, response []byte, stalled *atomic.Bool, released chan struct{}) *fakeBidiStream {
	s := &fakeBidiStream{
		id:            id,
		responseDelay: responseDelay,
		stalled:       stalled,
		released:      released,
		finCh:         make(chan struct{}),
	}
	s.responseBuf.Write(response)
	return s
}

func (s *fakeBidiStream) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancelledW {
		return 0, io.ErrClosedPipe
	}
	return s.requestBuf.Write(p)
}
func (s *fakeBidiStream) CloseWrite() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.finned {
		s.finned = true
		close(s.finCh)
	}
	return nil
}
func (s *fakeBidiStream) CancelWrite(uint32) { s.mu.Lock(); s.cancelledW = true; s.mu.Unlock() }
func (s *fakeBidiStream) CancelRead(uint32)  { s.mu.Lock(); s.cancelledR = true; s.mu.Unlock() }

// Read blocks until the request is FIN'd, then waits responseDelay,
// then returns the response payload. If stalled is set, blocks until
// released. Each call returns one chunk; subsequent calls return EOF.
func (s *fakeBidiStream) Read(p []byte) (int, error) {
	<-s.finCh
	if s.stalled != nil && s.stalled.Load() {
		<-s.released
	}
	if s.responseDelay > 0 {
		time.Sleep(s.responseDelay)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancelledR {
		return 0, io.ErrClosedPipe
	}
	if s.responseBuf.Len() == 0 {
		return 0, io.EOF
	}
	n, _ := s.responseBuf.Read(p)
	return n, nil
}

// fakeBidiOpener implements BidiOpener with configurable per-call
// response delay. One of the streams may be marked "stalled" — used
// by the benchmark to prove the BidiPerCall design isolates parallel
// calls from each other under loss/slowdown.
type fakeBidiOpener struct {
	mu              sync.Mutex
	streams         []*fakeBidiStream
	defaultDelay    time.Duration
	stallStream     int // 0 = no stall, else this 1-indexed stream stalls
	stallActive     *atomic.Bool
	stallReleased   chan struct{}
	defaultResponse []byte
}

func newFakeBidiOpener(delay time.Duration, response []byte, stallStream int) *fakeBidiOpener {
	stalled := &atomic.Bool{}
	if stallStream > 0 {
		stalled.Store(true)
	}
	return &fakeBidiOpener{
		defaultDelay:    delay,
		stallStream:     stallStream,
		stallActive:     stalled,
		stallReleased:   make(chan struct{}),
		defaultResponse: response,
	}
}

func (o *fakeBidiOpener) OpenBidiStream(ctx context.Context) (BidiStream, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	id := len(o.streams) + 1
	var stalled *atomic.Bool
	if id == o.stallStream {
		stalled = o.stallActive
	}
	s := newFakeBidiStream(id, o.defaultDelay, o.defaultResponse, stalled, o.stallReleased)
	o.streams = append(o.streams, s)
	return s, nil
}

func (o *fakeBidiOpener) ReleaseStall() {
	o.stallActive.Store(false)
	close(o.stallReleased)
}

func (o *fakeBidiOpener) Streams() []*fakeBidiStream {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]*fakeBidiStream(nil), o.streams...)
}

// measureParallelCalls publishes N AUs concurrently and records each
// call's completion latency. If stallStream > 0, the n-th call (1-
// indexed) blocks until releaseAfter — simulating one stalled call
// among many.
//
// Returns: per-call latency vector (in publish order).
func measureParallelCalls(t testing.TB, deliveryClass track.DeliveryClass, n int, perCallDelay time.Duration, stallStream int, releaseAfter time.Duration) []time.Duration {
	if tb := t; tb != nil {
		tb.Helper()
	}
	bc := pubsub.NewBroadcaster(n + 16)
	r := bc.Subscribe()

	opener := newFakeBidiOpener(perCallDelay, []byte("ok"), stallStream)
	streamOp := &countingOpener{inner: &fakeOpener{}}

	// Per-call completion timing: OnBidiResponse fires when the
	// response has been fully drained, which is the natural
	// "call complete" signal.
	startTs := make([]time.Time, n)
	doneTs := make([]time.Time, n)
	var doneMu sync.Mutex
	var doneWG sync.WaitGroup
	doneWG.Add(n)
	cfg := Config{
		Delivery:        deliveryClass,
		WriteDeadline:   100 * time.Millisecond,
		BidiOpener:      opener,
		BidiCallTimeout: 30 * time.Second,
		OnBidiResponse: func(seq uint32, _ []byte, _ error) {
			doneMu.Lock()
			if int(seq) < n {
				doneTs[seq] = time.Now()
			}
			doneMu.Unlock()
			doneWG.Done()
		},
	}

	p := New(streamOp, cfg)
	pumpDone := make(chan error, 1)
	go func() { pumpDone <- p.Run(context.Background(), r) }()

	for i := 0; i < n; i++ {
		startTs[i] = time.Now()
		bc.Publish(pubsub.AccessUnit{
			Bytes:    []byte("request-payload"),
			Keyframe: true,
			Seq:      uint32(i),
			PTSMicro: uint64(i) * 1000,
		})
	}

	// If we're testing the stall scenario, release after the
	// configured delay so the stalled call eventually completes.
	if stallStream > 0 {
		go func() {
			time.Sleep(releaseAfter)
			opener.ReleaseStall()
		}()
	}

	doneWG.Wait()
	bc.Unsubscribe(r)
	<-pumpDone

	lat := make([]time.Duration, n)
	for i := 0; i < n; i++ {
		lat[i] = doneTs[i].Sub(startTs[i])
	}
	return lat
}

// TestBidiPerCallIsolation is the headline measurement for PR4: under
// a workload where ONE of N parallel calls is deliberately slow, the
// fast calls should still complete in fast-call time, not slow-call
// time. This is the QUIC-stream-per-call structural win — under TCP
// HOL, the slow call would stall everyone behind it.
//
// We compare two configurations:
//
//	BASELINE: legacy GOP path. With the legacy single-stream pump,
//	          calls are processed serially — one slow call blocks
//	          the entire queue. Tail latency ≈ slow-call time.
//
//	PR4:      BidiPerCall — each call gets its own bidi stream,
//	          processed in parallel. Tail latency ≈ slow-call time
//	          for that one call, BUT median latency ≈ fast-call time.
//
//	go test -v ./feed -run TestBidiPerCallIsolation
func TestBidiPerCallIsolation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping bidi-per-call isolation test in short mode")
	}

	const (
		nCalls      = 8
		fastDelay   = 5 * time.Millisecond
		slowDelay   = 200 * time.Millisecond
		stallStream = 1 // first call stalls
	)

	// Baseline: REAL serialized pump — processes calls one-at-a-time
	// using the same fakeBidiOpener the kind-aware path uses. Only
	// difference is dispatch strategy (sequential vs spawn-per-call).
	// The stalled call blocks everyone behind it, modeling TCP-HOL
	// behavior.
	baseline := measureParallelCallsBaseline(t, nCalls, fastDelay, stallStream, slowDelay)
	bStats := computeLatStats(baseline)

	// Kind-aware: BidiPerCall — each call gets its own stream.
	pr4 := measureParallelCalls(t, track.DeliveryBidiPerCall, nCalls, fastDelay, stallStream, slowDelay)
	pr4Stats := computeLatStats(pr4)

	t.Logf("=== parallel call latency, %d calls, 1 stalled %v, others %v ===", nCalls, slowDelay, fastDelay)
	t.Logf("BASELINE (serial)        %s", bStats)
	t.Logf("PR4 (BidiPerCall)        %s", pr4Stats)

	// Sort each result to extract fast-call median and slow-call tail.
	sortedBaseline := append([]time.Duration(nil), baseline...)
	sort.Slice(sortedBaseline, func(i, j int) bool { return sortedBaseline[i] < sortedBaseline[j] })
	sortedPR4 := append([]time.Duration(nil), pr4...)
	sort.Slice(sortedPR4, func(i, j int) bool { return sortedPR4[i] < sortedPR4[j] })

	t.Logf("=== headline ===")
	t.Logf("median (fast) call:  %v  ->  %v   (%.1fx faster)",
		sortedBaseline[len(sortedBaseline)/2].Round(time.Millisecond),
		sortedPR4[len(sortedPR4)/2].Round(time.Millisecond),
		float64(sortedBaseline[len(sortedBaseline)/2])/float64(sortedPR4[len(sortedPR4)/2]))
	t.Logf("max (stalled) call:  %v  ->  %v   (similar — both wait for the slow one)",
		sortedBaseline[len(sortedBaseline)-1].Round(time.Millisecond),
		sortedPR4[len(sortedPR4)-1].Round(time.Millisecond))
}

// measureParallelCallsBaseline runs a REAL serialized pump: processes
// calls one-at-a-time on a single goroutine, opening + draining each
// stream before moving to the next. This is the structural shape of
// a TCP-HOL-blocking transport under loss: gRPC over HTTP/2 can
// multiplex on idle networks, but under packet loss the TCP receive
// buffer stalls until retransmit, effectively serializing all streams.
//
// We exercise the same fakeBidiOpener + fakeBidiStream as PR4 so the
// only difference between baseline and PR4 is the dispatch strategy
// (serial-then-next vs spawn-per-call). The measured latency vector
// is comparable head-to-head.
func measureParallelCallsBaseline(t testing.TB, n int, perCallDelay time.Duration, stallStream int, releaseAfter time.Duration) []time.Duration {
	if t != nil {
		t.Helper()
	}
	opener := newFakeBidiOpener(perCallDelay, []byte("ok"), stallStream)

	// In a real publish-then-serial-process scenario, ALL n calls
	// are queued at "publish time" (t=0) but the serial worker
	// processes them one-at-a-time. Latency for call k is from
	// "publish at t=0" to "drain complete," NOT from "serial worker
	// finally reached call k" — that would erase the HOL-blocking
	// signal we want to measure.
	publishTime := time.Now()

	// Release the stall asynchronously so the baseline run terminates.
	if stallStream > 0 {
		go func() {
			time.Sleep(releaseAfter)
			opener.ReleaseStall()
		}()
	}

	lat := make([]time.Duration, n)
	for i := 0; i < n; i++ {
		// Sequentially: open, write, FIN, drain. Next call cannot
		// begin until this one's response has been fully consumed —
		// the serial-pump property.
		stream, err := opener.OpenBidiStream(context.Background())
		if err != nil {
			if t != nil {
				t.Fatalf("baseline open call %d: %v", i, err)
			}
			return nil
		}
		_, _ = stream.Write([]byte("request-payload"))
		_ = stream.CloseWrite()
		// Drain the response.
		buf := make([]byte, 64)
		for {
			_, err := stream.Read(buf)
			if err != nil {
				break
			}
		}

		// Latency from publish-time (when the user would have queued
		// this call), not from when the serial worker dequeued it.
		lat[i] = time.Since(publishTime)
	}
	return lat
}

// ------ Throughput benchmarks ------

// BenchmarkBidiPerCall_Idle exercises the dispatch hot path of
// runBidiPerCall without real network: a fake bidi opener returns
// instantly, no per-call delay, no stall. Measures the
// goroutine-spawn + write + read cost per AU plus the wg.Add/Done
// bookkeeping. Useful as a regression guard on the dispatch overhead.
func BenchmarkBidiPerCall_Idle(b *testing.B) {
	const nPerIter = 100
	payload := []byte("request-payload")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bc := pubsub.NewBroadcaster(nPerIter + 16)
		r := bc.Subscribe()
		opener := newFakeBidiOpener(0, []byte("ok"), 0)
		streamOp := &countingOpener{inner: &fakeOpener{}}

		var wg sync.WaitGroup
		wg.Add(nPerIter)
		cfg := Config{
			Delivery:        track.DeliveryBidiPerCall,
			WriteDeadline:   100 * time.Millisecond,
			BidiOpener:      opener,
			BidiCallTimeout: 30 * time.Second,
			OnBidiResponse: func(_ uint32, _ []byte, _ error) {
				wg.Done()
			},
		}
		p := New(streamOp, cfg)
		pumpDone := make(chan error, 1)
		go func() { pumpDone <- p.Run(context.Background(), r) }()

		for j := 0; j < nPerIter; j++ {
			bc.Publish(pubsub.AccessUnit{
				Bytes:    payload,
				Keyframe: true,
				Seq:      uint32(j),
				PTSMicro: uint64(j) * 1000,
			})
		}
		wg.Wait()
		bc.Unsubscribe(r)
		<-pumpDone
		b.ReportMetric(float64(nPerIter), "calls/op")
	}
}
