package client

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/sachinkesiraju/quicrtc/wire"
)

const (
	// kindStatsInterval is the period of the subscriber's KindStats
	// emit loop. ~1 Hz keeps control-stream overhead negligible while
	// giving the publisher fresh end-to-end latency per Kind.
	kindStatsInterval = time.Second

	// kindStatsMaxSamples caps the per-Kind latency window between
	// emits so a fast track can't grow the sample slice without bound
	// if the emitter stalls. Excess samples in a tick are dropped from
	// the percentile estimate (not from delivery).
	kindStatsMaxSamples = 4096
)

// kindAccumulator holds the running receive-side observability state
// for one track Kind between emitter ticks. latMs is reset each tick;
// lastSeq and dropped are cumulative for the session.
type kindAccumulator struct {
	latMs   []float64
	lastSeq uint32
	haveSeq bool
	dropped uint64
}

// kindStatsCollector accumulates per-Kind receive samples and turns
// them into wire.KindStats snapshots. Safe for concurrent observe
// calls from multiple feed-drain goroutines. Active only when the
// "kind-stats" feature was negotiated.
type kindStatsCollector struct {
	mu  sync.Mutex
	acc map[string]*kindAccumulator
}

func newKindStatsCollector() *kindStatsCollector {
	return &kindStatsCollector{acc: make(map[string]*kindAccumulator)}
}

// observe records one received AU under its track Kind. recvWallMicro
// is the local receive wall-clock (Unix micros); pubWallMicro is the
// publisher's stamp (0 when absent, in which case no latency sample is
// recorded). seq drives drop detection via gaps in the per-Kind
// monotonic sequence.
func (k *kindStatsCollector) observe(kind string, seq uint32, pubWallMicro, recvWallMicro uint64) {
	k.mu.Lock()
	defer k.mu.Unlock()
	a := k.acc[kind]
	if a == nil {
		a = &kindAccumulator{}
		k.acc[kind] = a
	}
	if a.haveSeq && seq > a.lastSeq+1 {
		a.dropped += uint64(seq - a.lastSeq - 1)
	}
	if !a.haveSeq || seq > a.lastSeq {
		a.lastSeq = seq
		a.haveSeq = true
	}
	if pubWallMicro != 0 && recvWallMicro >= pubWallMicro && len(a.latMs) < kindStatsMaxSamples {
		a.latMs = append(a.latMs, float64(recvWallMicro-pubWallMicro)/1000.0)
	}
}

// snapshot produces one KindStats per Kind that saw activity and
// resets the per-tick latency window. lastSeq and dropped are
// cumulative and preserved across ticks.
func (k *kindStatsCollector) snapshot() []wire.KindStats {
	k.mu.Lock()
	defer k.mu.Unlock()
	out := make([]wire.KindStats, 0, len(k.acc))
	for kind, a := range k.acc {
		ks := wire.KindStats{
			Kind:    kind,
			LastSeq: a.lastSeq,
			Dropped: a.dropped,
		}
		if len(a.latMs) > 0 {
			sort.Float64s(a.latMs)
			ks.RecvP50Ms = uint32(percentile(a.latMs, 0.50) + 0.5)
			ks.RecvP99Ms = uint32(percentile(a.latMs, 0.99) + 0.5)
			a.latMs = a.latMs[:0]
		}
		out = append(out, ks)
	}
	return out
}

// percentile returns the p-quantile (0..1) of an ascending slice using
// nearest-rank. sorted must be non-empty.
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 1 {
		return sorted[0]
	}
	idx := int(p*float64(len(sorted)-1) + 0.5)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// emitKindStats runs the ~1 Hz loop that marshals per-Kind snapshots
// and writes them as TypeKindStats control frames back to the
// publisher. Started only when "kind-stats" was negotiated; exits on
// session-context cancel, client close, or an unrecoverable ctl write.
func (c *Client) emitKindStats(ctx context.Context) {
	t := time.NewTicker(kindStatsInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closed:
			return
		case <-t.C:
			for _, ks := range c.kindStats.snapshot() {
				payload, err := ks.Marshal()
				if err != nil {
					continue
				}
				if err := c.writeCtl(wire.TypeKindStats, payload); err != nil {
					return
				}
			}
		}
	}
}
