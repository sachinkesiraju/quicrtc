package transport

import (
	"sync"
	"time"
)

// CongestionState exposes the underlying QUIC connection's current
// view of network conditions. Implementations wrap whatever the
// transport library makes available (today: quic-go's
// Connection.ConnectionState plus a small rolling rate counter for
// the bandwidth estimate that quic-go does not expose directly).
//
// Pure observability: no actuator on this interface. Publishers
// query the state and decide for themselves whether to back off. A
// future scheduler will consume it to make per-lane scheduling
// decisions.
type CongestionState interface {
	// EstimatedSendBandwidth returns a recent average bytes/sec
	// observed leaving this connection. Sliding window of 1 second
	// typical. Zero when no traffic has been sent.
	EstimatedSendBandwidth() uint64

	// SmoothedRTT returns the connection's smoothed round-trip time.
	// Returns 0 if the underlying connection hasn't reported one yet.
	SmoothedRTT() time.Duration

	// BytesInFlight is the count of bytes sent and not yet
	// acknowledged. Useful as a backpressure proxy: a fast climb
	// without a matching SmoothedRTT change suggests the peer is
	// slow to ack, not that the path is congested.
	BytesInFlight() uint64

	// SendBufferAvailable is the count of bytes the connection can
	// accept right now without blocking. Useful for application-level
	// pacing decisions ("do I have headroom to emit a 100 KiB AU?").
	SendBufferAvailable() uint64
}

// SendRateMeter is a sliding-window rate counter used by connection
// adapters to derive EstimatedSendBandwidth() without asking the
// underlying transport (which typically doesn't expose it). Add bytes
// after each successful send; Rate returns the trailing-window average.
//
// Safe for concurrent use: both Add and Rate advance the window head
// and are serialized by an internal mutex.
type SendRateMeter struct {
	mu           sync.Mutex
	windowMicros int64
	samples      []rateSample
	head         int
	totalBytes   uint64
}

type rateSample struct {
	tMicros int64
	bytes   uint32
}

// NewSendRateMeter returns a meter with the given sliding-window
// duration (typically 1 second). Zero or negative window picks 1s.
func NewSendRateMeter(window time.Duration) *SendRateMeter {
	if window <= 0 {
		window = time.Second
	}
	// Bound the sample buffer at 1024 entries; high-rate senders will
	// drop the oldest. At 1024 events per second the resolution is
	// ~1ms, fine enough for any congestion observation use.
	return &SendRateMeter{
		windowMicros: window.Microseconds(),
		samples:      make([]rateSample, 0, 1024),
	}
}

// Add records `n` bytes sent at `now`. Drops samples older than the
// configured window from the head.
func (m *SendRateMeter) Add(n int, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := now.UnixMicro()
	m.samples = append(m.samples, rateSample{tMicros: t, bytes: uint32(n)})
	m.totalBytes += uint64(n)
	cutoff := t - m.windowMicros
	for m.head < len(m.samples) && m.samples[m.head].tMicros < cutoff {
		m.totalBytes -= uint64(m.samples[m.head].bytes)
		m.head++
	}
	// Compact periodically so the slice doesn't grow without bound.
	if m.head > 1024 {
		m.samples = append(m.samples[:0], m.samples[m.head:]...)
		m.head = 0
	}
}

// Rate returns the trailing-window average bytes/sec at `now`. Returns
// 0 if no samples are in the window.
func (m *SendRateMeter) Rate(now time.Time) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	cutoff := now.UnixMicro() - m.windowMicros
	for m.head < len(m.samples) && m.samples[m.head].tMicros < cutoff {
		m.totalBytes -= uint64(m.samples[m.head].bytes)
		m.head++
	}
	if m.head >= len(m.samples) {
		return 0
	}
	span := now.UnixMicro() - m.samples[m.head].tMicros
	if span <= 0 {
		return 0
	}
	return m.totalBytes * 1_000_000 / uint64(span)
}
