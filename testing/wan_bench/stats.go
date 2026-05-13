package main

// Shared statistical helpers for benchmark summarization.
//
// Reviewers asked for variance-aware reporting:
//   - sample count, min, max
//   - p25/p50/p75 (IQR), p95, p99
//   - mean, stddev
//   - bootstrap 95% CI for the mean and median (1000 resamples)
//
// Everything here works in float64 ms / µs space — callers convert.

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
)

// FullStats is the variance-aware summary of a sample vector.
type FullStats struct {
	N      int
	Min    float64
	Max    float64
	Mean   float64
	StdDev float64
	P25    float64
	P50    float64
	P75    float64
	P95    float64
	P99    float64
	IQR    float64

	// Bootstrap 95% CI [lo, hi] for mean and median.
	MeanCI95Lo   float64
	MeanCI95Hi   float64
	MedianCI95Lo float64
	MedianCI95Hi float64
}

// computeFullStats returns a FullStats over xs. Uses 1000 bootstrap
// resamples (seeded for reproducibility per call site via the supplied
// seed; pass 0 to use a stable default).
func computeFullStats(xs []float64) FullStats {
	if len(xs) == 0 {
		return FullStats{}
	}
	sorted := append([]float64(nil), xs...)
	sort.Float64s(sorted)

	pct := func(p float64) float64 {
		if len(sorted) == 1 {
			return sorted[0]
		}
		idx := int(float64(len(sorted)-1) * p)
		if idx < 0 {
			idx = 0
		}
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		return sorted[idx]
	}

	var sum float64
	for _, v := range xs {
		sum += v
	}
	mean := sum / float64(len(xs))

	var ssq float64
	for _, v := range xs {
		ssq += (v - mean) * (v - mean)
	}
	std := 0.0
	if len(xs) > 1 {
		std = math.Sqrt(ssq / float64(len(xs)-1))
	}

	meanLo, meanHi, medLo, medHi := bootstrapCI95(xs, 1000)

	return FullStats{
		N:            len(xs),
		Min:          sorted[0],
		Max:          sorted[len(sorted)-1],
		Mean:         mean,
		StdDev:       std,
		P25:          pct(0.25),
		P50:          pct(0.50),
		P75:          pct(0.75),
		P95:          pct(0.95),
		P99:          pct(0.99),
		IQR:          pct(0.75) - pct(0.25),
		MeanCI95Lo:   meanLo,
		MeanCI95Hi:   meanHi,
		MedianCI95Lo: medLo,
		MedianCI95Hi: medHi,
	}
}

// bootstrapCI95 returns the bootstrap 95% CI for the mean and median.
// Uses a fixed seed (1) per call so consecutive runs over the same
// data are reproducible; callers don't usually want jitter on top of
// already-noisy benchmark data.
func bootstrapCI95(xs []float64, B int) (meanLo, meanHi, medLo, medHi float64) {
	if len(xs) == 0 {
		return 0, 0, 0, 0
	}
	if len(xs) == 1 {
		return xs[0], xs[0], xs[0], xs[0]
	}
	rng := rand.New(rand.NewSource(1))
	means := make([]float64, B)
	medians := make([]float64, B)
	resample := make([]float64, len(xs))
	for b := 0; b < B; b++ {
		var sum float64
		for i := range resample {
			resample[i] = xs[rng.Intn(len(xs))]
			sum += resample[i]
		}
		means[b] = sum / float64(len(resample))
		// Median: copy and sort.
		cp := append([]float64(nil), resample...)
		sort.Float64s(cp)
		mid := len(cp) / 2
		if len(cp)%2 == 0 {
			medians[b] = 0.5 * (cp[mid-1] + cp[mid])
		} else {
			medians[b] = cp[mid]
		}
	}
	sort.Float64s(means)
	sort.Float64s(medians)
	loIdx := int(0.025 * float64(B))
	hiIdx := int(0.975 * float64(B))
	if hiIdx >= B {
		hiIdx = B - 1
	}
	return means[loIdx], means[hiIdx], medians[loIdx], medians[hiIdx]
}

// SummaryCSVHeader returns the canonical per-protocol summary header.
func SummaryCSVHeader() string {
	return "protocol,phase,n,min,p25,p50,p75,p95,p99,max,mean,stddev,iqr,mean_ci95_lo,mean_ci95_hi,median_ci95_lo,median_ci95_hi"
}

// SummaryCSVRow renders a FullStats as one CSV row, prefixed by the
// protocol+phase identifiers the caller cares about.
func SummaryCSVRow(protocol, phase string, s FullStats) string {
	return fmt.Sprintf("%s,%s,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f",
		protocol, phase, s.N,
		s.Min, s.P25, s.P50, s.P75, s.P95, s.P99, s.Max,
		s.Mean, s.StdDev, s.IQR,
		s.MeanCI95Lo, s.MeanCI95Hi, s.MedianCI95Lo, s.MedianCI95Hi,
	)
}

// FormatSummaryLine renders a FullStats as a single console line for
// the "===== summary =====" block. Units are caller-supplied (e.g. "ms").
func FormatSummaryLine(label string, s FullStats, unit string) string {
	return fmt.Sprintf("  %-30s n=%-4d min=%-7.2f p25=%-7.2f p50=%-7.2f p75=%-7.2f p95=%-7.2f p99=%-7.2f max=%-8.2f mean=%-7.2f sd=%-7.2f iqr=%-7.2f mean95=[%.2f,%.2f] med95=[%.2f,%.2f] %s",
		label, s.N, s.Min, s.P25, s.P50, s.P75, s.P95, s.P99, s.Max,
		s.Mean, s.StdDev, s.IQR,
		s.MeanCI95Lo, s.MeanCI95Hi, s.MedianCI95Lo, s.MedianCI95Hi, unit)
}
