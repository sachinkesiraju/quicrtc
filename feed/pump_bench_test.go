package feed

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/track"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// countingOpener wraps fakeOpener so benchmarks can count stream
// opens cheaply. The legacy GOP pump opens one stream per keyframe;
// since the token workload marks every AU as a keyframe (every
// token is independent), the GOP pump opens one stream per AU.
// The StreamLowLatency pump opens exactly one stream for the entire
// run. This counter exposes the structural difference.
type countingOpener struct {
	inner *fakeOpener
	opens int64
}

func (c *countingOpener) OpenSendStream(ctx context.Context) (SendStream, error) {
	atomic.AddInt64(&c.opens, 1)
	return c.inner.OpenSendStream(ctx)
}

func (c *countingOpener) Opens() int64 {
	return atomic.LoadInt64(&c.opens)
}

// runTokenLikeWorkload pushes n token-sized AUs through the pump
// configured for the given delivery class. Returns total wall-clock
// time, stream open count, and the inter-AU latency vector recorded
// at the receiver side (the AU-arrived timestamps from the
// broadcaster channel, NOT from the on-the-wire fake stream — we
// want the latency the receiver actually perceives).
//
// Note: this is an in-memory micro-benchmark. There is no QUIC, no
// network. We're measuring the protocol-path overhead — the cost of
// stream-open churn, header encoding, allocations, scheduling. That
// cost is the structural prediction the design is making: with
// persistent stream + pooled buffers, per-AU overhead drops to
// near-zero and tail latency stabilizes.
func runTokenLikeWorkload(b *testing.B, deliveryClass track.DeliveryClass, n int, payloadSize int) (totalElapsed time.Duration, streamOpens int64, allocs uint64, drained int) {
	b.Helper()
	bc := pubsub.NewBroadcaster(n * 2) // generous to avoid drops in the benchmark
	r := bc.Subscribe()
	innerOp := &fakeOpener{}
	op := &countingOpener{inner: innerOp}

	p := New(op, Config{
		Delivery:      deliveryClass,
		WriteDeadline: 100 * time.Millisecond,
	})

	// Drain the receiver in a goroutine so the pump can flush. The
	// fake stream consumes writes instantaneously so there is no
	// real backpressure; this is purely measuring the pump's
	// CPU/allocation cost on its own.
	pumpDone := make(chan error, 1)
	go func() { pumpDone <- p.Run(context.Background(), r) }()

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i & 0xff)
	}

	var mStart, mEnd runtimeMemStats
	readMemStats(&mStart)
	start := time.Now()

	for i := 0; i < n; i++ {
		bc.Publish(pubsub.AccessUnit{
			Bytes:    payload,
			Keyframe: true, // every token is independent
			Seq:      uint32(i),
			PTSMicro: uint64(i) * 1000,
		})
	}
	// Close subscription so the pump's Run returns.
	bc.Unsubscribe(r)
	<-pumpDone

	totalElapsed = time.Since(start)
	readMemStats(&mEnd)
	streamOpens = op.Opens()
	allocs = mEnd.Mallocs - mStart.Mallocs
	// Count how many AUs were actually written to the fake stream
	// (vs dropped). For GOP at this scale none should drop; for
	// LowLatency same. This is a sanity check.
	for _, s := range innerOp.Streams() {
		drained += len(parseAllFeedFramesBench(s.Bytes()))
	}
	return
}

// parseAllFeedFramesBench is a cheaper variant of the test helper
// — it just counts frames without allocating wire.FeedFrame structs
// per frame. Sufficient for the benchmark sanity check.
func parseAllFeedFramesBench(b []byte) []struct{} {
	out := []struct{}{}
	i := 0
	for i < len(b) {
		if i+4 > len(b) {
			return out
		}
		length := int(b[i+1])<<16 | int(b[i+2])<<8 | int(b[i+3])
		// Skip header (17B) + payload
		i += 17 + length
		if i > len(b) {
			return out
		}
		out = append(out, struct{}{})
	}
	return out
}

// TestPooledEncodeMatchesWire is a guardrail against the inline
// pooled-buffer encoder in writeFeedFramePooled drifting from the
// canonical wire.WriteFeedFrame layout. Any field added to the wire
// frame header must land in both encoders or this test fails.
//
// Covers both TypeKeyframe and TypePFrame — the pooled encoder is
// shared between runStreamGOP (which emits both) and runStreamLowLatency
// (which emits only TypeKeyframe). Byte-parity for TypePFrame is the
// invariant that guards the GOP coalesce path; a hardcoded type byte
// would corrupt P-frames silently (every P-frame would set FlagKeyframe
// via IsKeyframe()).
func TestPooledEncodeMatchesWire(t *testing.T) {
	types := []struct {
		name string
		typ  byte
	}{
		{"keyframe", wire.TypeKeyframe},
		{"pframe", wire.TypePFrame},
		{"frame", wire.TypeFrame},
	}
	cases := []struct {
		name    string
		pts     uint64
		seq     uint32
		flags   byte
		payload []byte
	}{
		{"empty payload", 0, 0, 0, nil},
		{"typical token", 1_700_000_000_000_000, 42, 0x01, []byte("hello world")},
		{"large pts/seq", 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFF, 0xFF, []byte{1, 2, 3, 4}},
		{"1KiB payload", 12345, 1, 0x03, bytes.Repeat([]byte{0xAB}, 1024)},
		{"64KiB video keyframe", 99, 7, wire.FlagKeyframe, bytes.Repeat([]byte{0xCD}, 64<<10)},
	}
	for _, ty := range types {
		for _, tc := range cases {
			t.Run(ty.name+"/"+tc.name, func(t *testing.T) {
				var canonical bytes.Buffer
				if err := wire.WriteFeedFrame(&canonical, ty.typ, tc.pts, tc.seq, tc.flags, tc.payload); err != nil {
					t.Fatalf("canonical write: %v", err)
				}
				var inlined bytes.Buffer
				if err := writeFeedFramePooled(&inlined, ty.typ, tc.pts, tc.seq, tc.flags, 0, tc.payload); err != nil {
					t.Fatalf("inlined write: %v", err)
				}
				if !bytes.Equal(canonical.Bytes(), inlined.Bytes()) {
					t.Fatalf("encoder drift\ncanonical: %x\ninlined:   %x", canonical.Bytes(), inlined.Bytes())
				}
			})
		}
	}
}

// countingWriter records every Write call so tests can verify the
// pooled encoder coalesces header + payload into a single Write.
type countingWriter struct {
	writes int
	bytes  int
}

func (c *countingWriter) Write(p []byte) (int, error) {
	c.writes++
	c.bytes += len(p)
	return len(p), nil
}

// TestCoalescedWriteCount confirms the pooled encoder issues exactly
// one Write per AU, vs wire.WriteFeedFrame which does two (header
// then payload). One Write per AU gives quic-go the best chance of
// packing into a single QUIC packet without its own coalescing layer.
func TestCoalescedWriteCount(t *testing.T) {
	payload := bytes.Repeat([]byte{0x42}, 1024)

	var ref countingWriter
	if err := wire.WriteFeedFrame(&ref, wire.TypeKeyframe, 0, 0, 0, payload); err != nil {
		t.Fatalf("ref write: %v", err)
	}
	if ref.writes != 2 {
		t.Fatalf("wire.WriteFeedFrame: expected 2 writes, got %d", ref.writes)
	}

	var pooled countingWriter
	if err := writeFeedFramePooled(&pooled, wire.TypeKeyframe, 0, 0, 0, 0, payload); err != nil {
		t.Fatalf("pooled write: %v", err)
	}
	if pooled.writes != 1 {
		t.Fatalf("writeFeedFramePooled: expected 1 write, got %d", pooled.writes)
	}
	if ref.bytes != pooled.bytes {
		t.Fatalf("byte count mismatch: ref=%d pooled=%d", ref.bytes, pooled.bytes)
	}
}

// BenchmarkPooledFeedFrame_Token exercises the encoder with token-sized
// payloads (the LowLatency hot path).
func BenchmarkPooledFeedFrame_Token(b *testing.B) {
	payload := bytes.Repeat([]byte{0x42}, 64)
	var w countingWriter
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = writeFeedFramePooled(&w, wire.TypeKeyframe, uint64(i), uint32(i), 0, 0, payload)
	}
}

// BenchmarkPooledFeedFrame_Keyframe exercises the encoder with video-
// keyframe-sized payloads (the GOP hot path after #2 coalesce lands).
func BenchmarkPooledFeedFrame_Keyframe(b *testing.B) {
	payload := bytes.Repeat([]byte{0x42}, 64<<10)
	var w countingWriter
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = writeFeedFramePooled(&w, wire.TypeKeyframe, uint64(i), uint32(i), 0, 0, payload)
	}
}

// BenchmarkCanonicalFeedFrame_Keyframe is the baseline for the
// keyframe-sized comparison — measures wire.WriteFeedFrame's two-write
// path against the pooled coalesced variant.
func BenchmarkCanonicalFeedFrame_Keyframe(b *testing.B) {
	payload := bytes.Repeat([]byte{0x42}, 64<<10)
	var w countingWriter
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = wire.WriteFeedFrame(&w, wire.TypeKeyframe, uint64(i), uint32(i), 0, payload)
	}
}

// BenchmarkTokenPump_GOP exercises the legacy stream-per-keyframe
// pump with a token-sized workload. Every AU is a "keyframe" (token
// is independent), so the GOP pump opens N streams for N AUs and
// cancels each one before the next. This is the structural
// inefficiency we're measuring against.
func BenchmarkTokenPump_GOP(b *testing.B) {
	const (
		nAUs        = 1000
		payloadSize = 64
	)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		elapsed, opens, allocs, drained := runTokenLikeWorkload(b, track.DeliveryStreamGOP, nAUs, payloadSize)
		b.ReportMetric(float64(elapsed.Nanoseconds())/float64(nAUs), "ns/AU")
		b.ReportMetric(float64(opens), "stream-opens")
		b.ReportMetric(float64(allocs)/float64(nAUs), "allocs/AU")
		b.ReportMetric(float64(drained), "AUs-written")
		if drained != nAUs {
			b.Fatalf("GOP: expected %d AUs written, got %d", nAUs, drained)
		}
	}
}

// BenchmarkTokenPump_LowLatency exercises the kind-aware
// StreamLowLatency pump on the same token workload. One persistent
// stream, pooled header buffers, no GOP semantics. This is what
// PR2 is supposed to deliver.
func BenchmarkTokenPump_LowLatency(b *testing.B) {
	const (
		nAUs        = 1000
		payloadSize = 64
	)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		elapsed, opens, allocs, drained := runTokenLikeWorkload(b, track.DeliveryStreamLowLatency, nAUs, payloadSize)
		b.ReportMetric(float64(elapsed.Nanoseconds())/float64(nAUs), "ns/AU")
		b.ReportMetric(float64(opens), "stream-opens")
		b.ReportMetric(float64(allocs)/float64(nAUs), "allocs/AU")
		b.ReportMetric(float64(drained), "AUs-written")
		if drained != nAUs {
			b.Fatalf("LowLatency: expected %d AUs written, got %d", nAUs, drained)
		}
	}
}

// ---------- Latency-distribution benchmark ----------
//
// The bench above measures throughput + alloc cost. This one measures
// per-AU latency distribution: how long from Publish() to the AU
// landing on the stream (which the fake stream's write timestamp
// approximates). p50, p99, and stddev are the load-bearing metrics
// for "jitter" claims.

// timestampedStream records the time of each successful write. Used
// to measure per-AU latency from publish to wire.
type timestampedStream struct {
	mu        sync.Mutex
	writeTs   []time.Time
	cancelled bool
	closed    bool
}

func (s *timestampedStream) Write(p []byte) (int, error) {
	s.mu.Lock()
	s.writeTs = append(s.writeTs, time.Now())
	s.mu.Unlock()
	return len(p), nil
}
func (s *timestampedStream) Close() error                     { s.closed = true; return nil }
func (s *timestampedStream) SetWriteDeadline(time.Time) error { return nil }
func (s *timestampedStream) CancelWrite(uint32)               { s.cancelled = true }
func (s *timestampedStream) Writes() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.writeTs...)
}

type timestampedOpener struct {
	mu      sync.Mutex
	streams []*timestampedStream
}

func (o *timestampedOpener) OpenSendStream(ctx context.Context) (SendStream, error) {
	s := &timestampedStream{}
	o.mu.Lock()
	o.streams = append(o.streams, s)
	o.mu.Unlock()
	return s, nil
}

// measureInterArrivalJitter pushes N AUs through the pump and records
// the inter-arrival time between successive writes on the wire.
// "Jitter" is the stddev of these inter-arrival times — the smaller
// it is, the smoother the on-wire flow looks to a receiver.
//
// This is a single-goroutine measurement (publish loop records times,
// pump goroutine records times, but we compare TIME DELTAS WITHIN the
// pump goroutine, never cross-goroutine — avoids the clock-correlation
// bugs that arise from comparing publishTs to writeTs.)
func measureInterArrivalJitter(t testing.TB, delivery track.DeliveryClass, nAUs int, paceMicros int) (interArrival []time.Duration, opens int) {
	t.Helper()
	bc := pubsub.NewBroadcaster(nAUs + 16)
	r := bc.Subscribe()
	op := &timestampedOpener{}
	p := New(op, Config{
		Delivery:      delivery,
		WriteDeadline: 100 * time.Millisecond,
	})

	pumpDone := make(chan error, 1)
	go func() { pumpDone <- p.Run(context.Background(), r) }()

	payload := make([]byte, 64)
	for i := range payload {
		payload[i] = byte(i & 0xff)
	}

	for i := 0; i < nAUs; i++ {
		bc.Publish(pubsub.AccessUnit{
			Bytes:    payload,
			Keyframe: true,
			Seq:      uint32(i),
			PTSMicro: uint64(i) * 1000,
		})
		if paceMicros > 0 {
			time.Sleep(time.Duration(paceMicros) * time.Microsecond)
		}
	}
	bc.Unsubscribe(r)
	<-pumpDone

	// Collect all write timestamps across all streams. In GOP mode
	// streams are opened one-per-AU and writes happen sequentially;
	// in LowLatency mode all writes are on one stream. Either way,
	// the writes happen in pump-goroutine order, so concatenating
	// per-stream write lists in stream-open order gives the on-wire
	// arrival order.
	op.mu.Lock()
	streams := append([]*timestampedStream(nil), op.streams...)
	opens = len(streams)
	op.mu.Unlock()

	var allWrites []time.Time
	for _, s := range streams {
		allWrites = append(allWrites, s.Writes()...)
	}

	if len(allWrites) < 2 {
		return nil, opens
	}
	interArrival = make([]time.Duration, len(allWrites)-1)
	for i := 1; i < len(allWrites); i++ {
		interArrival[i-1] = allWrites[i].Sub(allWrites[i-1])
	}
	return interArrival, opens
}

type latStats struct {
	N      int
	Mean   time.Duration
	Stddev time.Duration
	P50    time.Duration
	P99    time.Duration
	P999   time.Duration
	Max    time.Duration
}

func computeLatStats(samples []time.Duration) latStats {
	if len(samples) == 0 {
		return latStats{}
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var sum int64
	for _, d := range sorted {
		sum += int64(d)
	}
	mean := time.Duration(sum / int64(len(sorted)))
	var sqSum float64
	for _, d := range sorted {
		diff := float64(d - mean)
		sqSum += diff * diff
	}
	stddev := time.Duration(0)
	if len(sorted) > 1 {
		// Variance = sqSum / (N-1); stddev = sqrt(variance).
		variance := sqSum / float64(len(sorted)-1)
		stddev = time.Duration(int64(math.Sqrt(variance)))
	}
	pct := func(p float64) time.Duration {
		idx := int(float64(len(sorted)-1) * p)
		return sorted[idx]
	}
	return latStats{
		N:      len(sorted),
		Mean:   mean,
		Stddev: stddev,
		P50:    pct(0.50),
		P99:    pct(0.99),
		P999:   pct(0.999),
		Max:    sorted[len(sorted)-1],
	}
}

func (s latStats) String() string {
	return fmt.Sprintf("n=%d mean=%v stddev=%v p50=%v p99=%v p999=%v max=%v",
		s.N, s.Mean.Round(time.Nanosecond), s.Stddev.Round(time.Nanosecond),
		s.P50.Round(time.Nanosecond), s.P99.Round(time.Nanosecond),
		s.P999.Round(time.Nanosecond), s.Max.Round(time.Nanosecond))
}

// TestTokenPumpJitterComparison is the headline measurement: same
// workload, two pump paths, side-by-side inter-arrival jitter.
// We measure on-wire smoothness, not absolute latency (which requires
// cross-goroutine timestamp correlation that's prone to drift).
//
// Inter-arrival jitter is the metric viewers actually perceive: low
// stddev = smooth, high stddev = stutter. For LLM token streams,
// jitter directly translates to "tokens appearing at a steady pace"
// vs "tokens arriving in bursts."
//
// Run with `go test -v ./feed -run TestTokenPumpJitterComparison`.
func TestTokenPumpJitterComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping jitter comparison in short mode")
	}

	const (
		nAUs       = 2000
		paceMicros = 1000 // ~1000 AU/s — high LLM token rate
	)

	// --- baseline: every track on the legacy stream-per-GOP path ---
	gopIA, gopOpens := measureInterArrivalJitter(t, track.DeliveryStreamGOP, nAUs, paceMicros)
	gopStats := computeLatStats(gopIA)

	// --- kind-aware: KindTokens routes through StreamLowLatency ---
	llIA, llOpens := measureInterArrivalJitter(t, track.DeliveryStreamLowLatency, nAUs, paceMicros)
	llStats := computeLatStats(llIA)

	t.Logf("GOP (baseline)     opens=%d  inter-arrival %s", gopOpens, gopStats)
	t.Logf("LowLatency (new)   opens=%d  inter-arrival %s", llOpens, llStats)

	// Headline derived numbers. The 1000µs ideal inter-arrival
	// matches the publish pace; the closer the measured value is to
	// 1000µs and the smaller the stddev, the smoother the stream.
	streamReduction := float64(gopOpens-llOpens) / float64(gopOpens) * 100
	var jitterImprovement float64
	if gopStats.Stddev > 0 {
		jitterImprovement = (1 - float64(llStats.Stddev)/float64(gopStats.Stddev)) * 100
	}
	var p99Improvement float64
	if gopStats.P99 > 0 {
		p99Improvement = (1 - float64(llStats.P99)/float64(gopStats.P99)) * 100
	}

	t.Logf("=== headline numbers ===")
	t.Logf("inter-arrival p50:  %v -> %v",
		gopStats.P50.Round(time.Nanosecond), llStats.P50.Round(time.Nanosecond))
	t.Logf("inter-arrival p99:  %v -> %v   (%.1f%% improvement)",
		gopStats.P99.Round(time.Nanosecond), llStats.P99.Round(time.Nanosecond), p99Improvement)
	t.Logf("jitter (stddev):    %v -> %v   (%.1f%% reduction)",
		gopStats.Stddev.Round(time.Nanosecond), llStats.Stddev.Round(time.Nanosecond), jitterImprovement)
	t.Logf("stream opens:       %d -> %d    (%.1f%% reduction)",
		gopOpens, llOpens, streamReduction)
}
