// Package loadgen contains the shared harness used by every benchmark
// in benchmarks/...: latency-stats math, the QUIC and pion pair
// helpers, the UDP network-condition proxy, and the synthetic payload
// codec. It's import-path internal so the public package surface
// stays the same.
package loadgen

import (
	"fmt"
	"sort"
	"time"
)

// LatencyStats summarizes a vector of durations.
type LatencyStats struct {
	N    int
	Min  time.Duration
	Mean time.Duration
	P50  time.Duration
	P95  time.Duration
	P99  time.Duration
	Max  time.Duration
}

// ComputeStats returns LatencyStats for the given samples. Empty input
// returns the zero value.
func ComputeStats(samples []time.Duration) LatencyStats {
	if len(samples) == 0 {
		return LatencyStats{}
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	pct := func(p float64) time.Duration {
		idx := int(float64(len(sorted)-1) * p)
		return sorted[idx]
	}
	return LatencyStats{
		N:    len(sorted),
		Min:  sorted[0],
		Mean: sum / time.Duration(len(sorted)),
		P50:  pct(0.50),
		P95:  pct(0.95),
		P99:  pct(0.99),
		Max:  sorted[len(sorted)-1],
	}
}

func (s LatencyStats) String() string {
	return fmt.Sprintf(
		"n=%d  min=%6s  mean=%6s  p50=%6s  p95=%6s  p99=%6s  max=%6s",
		s.N,
		s.Min.Round(time.Microsecond),
		s.Mean.Round(time.Microsecond),
		s.P50.Round(time.Microsecond),
		s.P95.Round(time.Microsecond),
		s.P99.Round(time.Microsecond),
		s.Max.Round(time.Microsecond),
	)
}
