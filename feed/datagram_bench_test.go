package feed

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/track"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// inMemDatagramSender mimics webtransport.Session.SendDatagram with
// instant non-blocking semantics. It records send timestamps so the
// benchmark can measure inter-arrival latency on the wire side.
type inMemDatagramSender struct {
	mu        sync.Mutex
	sends     []time.Time
	bytes     int64
	dropCount int64
	maxSize   int // 0 = no limit
}

func (s *inMemDatagramSender) SendDatagram(payload []byte) error {
	if s.maxSize > 0 && len(payload) > s.maxSize {
		atomic.AddInt64(&s.dropCount, 1)
		return wire.ErrDatagramTooLarge
	}
	s.mu.Lock()
	s.sends = append(s.sends, time.Now())
	s.bytes += int64(len(payload))
	s.mu.Unlock()
	return nil
}

func (s *inMemDatagramSender) Sends() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.sends...)
}

func (s *inMemDatagramSender) Bytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bytes
}

// runTelemetryWorkload publishes N telemetry-sized AUs (typical
// metric payload) through the pump configured for the given delivery
// class. Returns wall-clock time, byte count, allocations, and
// inter-arrival jitter at the send side.
func runTelemetryWorkload(tb testing.TB, deliveryClass track.DeliveryClass, n int, payloadSize int) (elapsed time.Duration, bytesOnWire int64, allocs uint64, opens int64, sendIA []time.Duration) {
	if tb != nil {
		tb.Helper()
	}

	bc := pubsub.NewBroadcaster(n * 2)
	r := bc.Subscribe()

	cfg := Config{
		Delivery:      deliveryClass,
		WriteDeadline: 100 * time.Millisecond,
		TrackID:       42, // telemetry track ID
	}
	var sender *inMemDatagramSender
	var streamOpener *countingOpener
	if deliveryClass == track.DeliveryDatagramOrStream {
		sender = &inMemDatagramSender{}
		cfg.DatagramSender = sender
	}
	streamOpener = &countingOpener{inner: &fakeOpener{}}

	p := New(streamOpener, cfg)

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
			Keyframe: true, // telemetry AUs are independent
			Seq:      uint32(i),
			PTSMicro: uint64(i) * 1000,
		})
	}
	bc.Unsubscribe(r)
	<-pumpDone

	elapsed = time.Since(start)
	readMemStats(&mEnd)
	allocs = mEnd.Mallocs - mStart.Mallocs
	opens = streamOpener.Opens()

	if sender != nil {
		bytesOnWire = sender.Bytes()
		ts := sender.Sends()
		if len(ts) > 1 {
			sendIA = make([]time.Duration, len(ts)-1)
			for i := 1; i < len(ts); i++ {
				sendIA[i-1] = ts[i].Sub(ts[i-1])
			}
		}
	} else {
		// For stream-based deliveries, count bytes on the fake stream.
		for _, s := range streamOpener.inner.Streams() {
			bytesOnWire += int64(len(s.Bytes()))
		}
	}
	return
}

// BenchmarkTelemetry_StreamReliable runs telemetry AUs through the
// legacy GOP path (the only choice in baseline). Establishes how
// expensive "reliable telemetry sharing a stream" actually is.
func BenchmarkTelemetry_StreamReliable(b *testing.B) {
	const (
		n           = 1000
		payloadSize = 64
	)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		elapsed, bytes, allocs, opens, _ := runTelemetryWorkload(b, track.DeliveryStreamGOP, n, payloadSize)
		b.ReportMetric(float64(elapsed.Nanoseconds())/float64(n), "ns/AU")
		b.ReportMetric(float64(bytes)/float64(n), "B/AU-wire")
		b.ReportMetric(float64(opens), "stream-opens")
		b.ReportMetric(float64(allocs)/float64(n), "allocs/AU")
	}
}

// BenchmarkTelemetry_Datagram runs telemetry AUs through the
// DatagramOrStream path. Should show: (a) much smaller wire envelope
// (4B vs 17B), (b) one allocation per AU at most (pooled encoder),
// (c) zero stream opens.
func BenchmarkTelemetry_Datagram(b *testing.B) {
	const (
		n           = 1000
		payloadSize = 64
	)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		elapsed, bytes, allocs, opens, _ := runTelemetryWorkload(b, track.DeliveryDatagramOrStream, n, payloadSize)
		b.ReportMetric(float64(elapsed.Nanoseconds())/float64(n), "ns/AU")
		b.ReportMetric(float64(bytes)/float64(n), "B/AU-wire")
		b.ReportMetric(float64(opens), "stream-opens")
		b.ReportMetric(float64(allocs)/float64(n), "allocs/AU")
	}
}

// TestTelemetryPathComparison is the headline measurement for PR3:
// same telemetry workload through both paths, with all the metrics
// users care about (per-AU latency, wire bytes, allocs, stream opens).
//
//	go test -v ./feed -run TestTelemetryPathComparison
func TestTelemetryPathComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping telemetry comparison in short mode")
	}

	const (
		n           = 2000
		payloadSize = 64
	)

	// Baseline: telemetry over reliable stream.
	bElapsed, bBytes, bAllocs, bOpens, _ := runTelemetryWorkload(t, track.DeliveryStreamGOP, n, payloadSize)

	// Kind-aware: telemetry over datagrams.
	dElapsed, dBytes, dAllocs, dOpens, _ := runTelemetryWorkload(t, track.DeliveryDatagramOrStream, n, payloadSize)

	t.Logf("=== telemetry path comparison, %d AUs, %d-byte payload ===", n, payloadSize)
	t.Logf("BASELINE (reliable stream)  elapsed=%v  wire=%dB (%.1fB/AU)  stream-opens=%d  allocs=%d (%.1f/AU)",
		bElapsed.Round(time.Microsecond), bBytes, float64(bBytes)/float64(n), bOpens, bAllocs, float64(bAllocs)/float64(n))
	t.Logf("PR3 (datagram envelope)     elapsed=%v  wire=%dB (%.1fB/AU)  stream-opens=%d  allocs=%d (%.1f/AU)",
		dElapsed.Round(time.Microsecond), dBytes, float64(dBytes)/float64(n), dOpens, dAllocs, float64(dAllocs)/float64(n))

	t.Logf("=== headline ===")
	t.Logf("elapsed:        %v  ->  %v   (%.1f%% reduction)",
		bElapsed.Round(time.Microsecond), dElapsed.Round(time.Microsecond),
		(1-float64(dElapsed)/float64(bElapsed))*100)
	t.Logf("wire bytes/AU:  %.1f  ->  %.1f   (%.1f%% smaller envelope)",
		float64(bBytes)/float64(n), float64(dBytes)/float64(n),
		(1-float64(dBytes)/float64(bBytes))*100)
	t.Logf("allocs/AU:      %.1f  ->  %.1f",
		float64(bAllocs)/float64(n), float64(dAllocs)/float64(n))
	t.Logf("stream opens:   %d  ->  %d",
		bOpens, dOpens)
}
