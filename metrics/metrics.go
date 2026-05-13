// Package metrics defines the observability interface for quicrtc
// servers and clients. The interface is pluggable so integrators can
// wire it to Prometheus, OpenTelemetry, expvar, or any other metrics
// system without quicrtc taking a hard dependency.
//
// Two implementations ship in this package:
//
//   - NoopMetrics (the default when Config.Metrics is nil) — zero cost.
//   - InMemory (via NewInMemory) — atomic counters readable through
//     Snapshot, useful for tests and small single-binary deployments.
//
// Adapters for Prometheus / OpenTelemetry are intentionally out of
// scope here so this package adds zero external dependencies. Wire
// your own adapter by implementing the Metrics interface against
// your registry of choice:
//
//	type promAdapter struct{ reg *prometheus.Registry /* + counters */ }
//	func (p *promAdapter) SessionStarted() { p.sessionsTotal.Inc() }
//	// ... implement the other methods ...
//	srv := server.New(server.Config{Metrics: &promAdapter{...}, ...})
package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

// Metrics is the observability surface every quicrtc component
// emits to. All methods must be safe for concurrent use and must
// be cheap when called on a NoopMetrics — they're on the per-AU
// hot path.
type Metrics interface {
	// SessionStarted is called when a new session establishes.
	SessionStarted()
	// SessionEnded is called when a session terminates (any reason).
	// dur is the wallclock from start to end.
	SessionEnded(dur time.Duration)
	// SessionResumed is called when a session is resumed via SessionID.
	// dur is the resume RTT.
	SessionResumed(dur time.Duration)

	// TrackAdded / TrackRemoved track lifecycle. Per kind for
	// breakdown.
	TrackAdded(kind string)
	TrackRemoved(kind string)

	// AUSent / AURecv are called per AU successfully sent/received.
	// kind is the track kind (video/audio/tokens/...). bytes is the
	// AU payload size.
	AUSent(kind string, bytes int)
	AURecv(kind string, bytes int)

	// AUDropped is called when the broadcaster drops an AU due to
	// slow consumer. Most important per-modality reliability signal.
	AUDropped(kind string, reason string)

	// StreamOpened / StreamReset track per-stream churn (uni feed
	// streams plus stream-per-GOP resets).
	StreamOpened(kind string)
	StreamReset(kind string, reason string)

	// HandshakeDuration records the time from accept-stream to
	// SDP-write-completed for one session.
	HandshakeDuration(dur time.Duration)

	// AuthFailed is called when HELLO is rejected. reason is short
	// (e.g., "slug", "version", "role").
	AuthFailed(reason string)
}

// NoopMetrics is the zero-cost default. All methods are no-ops.
type NoopMetrics struct{}

func (NoopMetrics) SessionStarted()                 {}
func (NoopMetrics) SessionEnded(time.Duration)      {}
func (NoopMetrics) SessionResumed(time.Duration)    {}
func (NoopMetrics) TrackAdded(string)               {}
func (NoopMetrics) TrackRemoved(string)             {}
func (NoopMetrics) AUSent(string, int)              {}
func (NoopMetrics) AURecv(string, int)              {}
func (NoopMetrics) AUDropped(string, string)        {}
func (NoopMetrics) StreamOpened(string)             {}
func (NoopMetrics) StreamReset(string, string)      {}
func (NoopMetrics) HandshakeDuration(time.Duration) {}
func (NoopMetrics) AuthFailed(string)               {}

// InMemory is a process-local Metrics impl that exposes per-event
// counters atomically. Useful for tests and local development; for
// production, plug in a Prometheus or OTel-backed implementation.
type InMemory struct {
	mu sync.Mutex

	sessionsStarted atomic.Uint64
	sessionsEnded   atomic.Uint64
	sessionsResumed atomic.Uint64
	authFailures    atomic.Uint64

	// Per-kind atomic counters. Lazily allocated under mu.
	tracksByKind      map[string]*atomic.Uint64
	tracksRemovedKind map[string]*atomic.Uint64
	auSentByKind      map[string]*atomic.Uint64
	auRecvByKind      map[string]*atomic.Uint64
	bytesSentByKind   map[string]*atomic.Uint64
	bytesRecvByKind   map[string]*atomic.Uint64
	dropsByKind       map[string]*atomic.Uint64
	streamsOpenedKind map[string]*atomic.Uint64
	streamsResetKind  map[string]*atomic.Uint64
}

// NewInMemory returns a fresh InMemory metrics sink.
func NewInMemory() *InMemory {
	return &InMemory{
		tracksByKind:      map[string]*atomic.Uint64{},
		tracksRemovedKind: map[string]*atomic.Uint64{},
		auSentByKind:      map[string]*atomic.Uint64{},
		auRecvByKind:      map[string]*atomic.Uint64{},
		bytesSentByKind:   map[string]*atomic.Uint64{},
		bytesRecvByKind:   map[string]*atomic.Uint64{},
		dropsByKind:       map[string]*atomic.Uint64{},
		streamsOpenedKind: map[string]*atomic.Uint64{},
		streamsResetKind:  map[string]*atomic.Uint64{},
	}
}

func (m *InMemory) get(table map[string]*atomic.Uint64, key string) *atomic.Uint64 {
	m.mu.Lock()
	c, ok := table[key]
	if !ok {
		c = new(atomic.Uint64)
		table[key] = c
	}
	m.mu.Unlock()
	return c
}

func (m *InMemory) SessionStarted()              { m.sessionsStarted.Add(1) }
func (m *InMemory) SessionEnded(time.Duration)   { m.sessionsEnded.Add(1) }
func (m *InMemory) SessionResumed(time.Duration) { m.sessionsResumed.Add(1) }
func (m *InMemory) TrackAdded(k string)          { m.get(m.tracksByKind, k).Add(1) }
func (m *InMemory) TrackRemoved(k string)        { m.get(m.tracksRemovedKind, k).Add(1) }
func (m *InMemory) AUSent(k string, b int) {
	m.get(m.auSentByKind, k).Add(1)
	m.get(m.bytesSentByKind, k).Add(uint64(b))
}
func (m *InMemory) AURecv(k string, b int) {
	m.get(m.auRecvByKind, k).Add(1)
	m.get(m.bytesRecvByKind, k).Add(uint64(b))
}
func (m *InMemory) AUDropped(k, reason string)      { m.get(m.dropsByKind, k+":"+reason).Add(1) }
func (m *InMemory) StreamOpened(k string)           { m.get(m.streamsOpenedKind, k).Add(1) }
func (m *InMemory) StreamReset(k, reason string)    { m.get(m.streamsResetKind, k+":"+reason).Add(1) }
func (m *InMemory) HandshakeDuration(time.Duration) {}
func (m *InMemory) AuthFailed(string)               { m.authFailures.Add(1) }

// Snapshot returns a point-in-time read of all counters. Safe for
// metric scrape endpoints to expose.
type Snapshot struct {
	SessionsStarted uint64
	SessionsEnded   uint64
	SessionsResumed uint64
	AuthFailures    uint64
	TracksByKind    map[string]uint64
	AUSentByKind    map[string]uint64
	AURecvByKind    map[string]uint64
	BytesSentByKind map[string]uint64
	BytesRecvByKind map[string]uint64
	DropsByKind     map[string]uint64
	StreamsOpened   map[string]uint64
	StreamsReset    map[string]uint64
}

func snap(table map[string]*atomic.Uint64) map[string]uint64 {
	out := make(map[string]uint64, len(table))
	for k, c := range table {
		out[k] = c.Load()
	}
	return out
}

func (m *InMemory) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return Snapshot{
		SessionsStarted: m.sessionsStarted.Load(),
		SessionsEnded:   m.sessionsEnded.Load(),
		SessionsResumed: m.sessionsResumed.Load(),
		AuthFailures:    m.authFailures.Load(),
		TracksByKind:    snap(m.tracksByKind),
		AUSentByKind:    snap(m.auSentByKind),
		AURecvByKind:    snap(m.auRecvByKind),
		BytesSentByKind: snap(m.bytesSentByKind),
		BytesRecvByKind: snap(m.bytesRecvByKind),
		DropsByKind:     snap(m.dropsByKind),
		StreamsOpened:   snap(m.streamsOpenedKind),
		StreamsReset:    snap(m.streamsResetKind),
	}
}
