package client

import "testing"

// TestKindStatsLatencyPercentiles feeds a known latency distribution
// and checks the emitted p50/p99 land in the right buckets.
func TestKindStatsLatencyPercentiles(t *testing.T) {
	c := newKindStatsCollector()
	// 100 samples: 99 at 10ms, 1 at 200ms. recvWall - pubWall in micros.
	const pub = 1_000_000
	for i := 0; i < 99; i++ {
		c.observe("video", uint32(i+1), pub, pub+10_000) // 10ms
	}
	c.observe("video", 100, pub, pub+200_000) // 200ms outlier

	snaps := c.snapshot()
	if len(snaps) != 1 {
		t.Fatalf("want 1 kind, got %d", len(snaps))
	}
	ks := snaps[0]
	if ks.Kind != "video" {
		t.Fatalf("kind: %q", ks.Kind)
	}
	if ks.RecvP50Ms != 10 {
		t.Fatalf("p50: got %d want 10", ks.RecvP50Ms)
	}
	if ks.RecvP99Ms != 10 {
		t.Fatalf("p99: got %d want 10 (outlier should be above p99 rank)", ks.RecvP99Ms)
	}
	if ks.LastSeq != 100 {
		t.Fatalf("lastSeq: got %d want 100", ks.LastSeq)
	}
}

// TestKindStatsDropDetection verifies gaps in Seq accumulate as drops
// and that lastSeq tracks the max.
func TestKindStatsDropDetection(t *testing.T) {
	c := newKindStatsCollector()
	c.observe("data", 1, 0, 0)
	c.observe("data", 2, 0, 0)
	c.observe("data", 5, 0, 0) // gap: 3,4 missing -> +2
	c.observe("data", 6, 0, 0)
	c.observe("data", 10, 0, 0) // gap: 7,8,9 missing -> +3

	snaps := c.snapshot()
	if len(snaps) != 1 {
		t.Fatalf("want 1 kind, got %d", len(snaps))
	}
	ks := snaps[0]
	if ks.Dropped != 5 {
		t.Fatalf("dropped: got %d want 5", ks.Dropped)
	}
	if ks.LastSeq != 10 {
		t.Fatalf("lastSeq: got %d want 10", ks.LastSeq)
	}
	// No pubWall samples -> percentiles stay zero.
	if ks.RecvP50Ms != 0 || ks.RecvP99Ms != 0 {
		t.Fatalf("expected zero percentiles without wall-clock, got p50=%d p99=%d", ks.RecvP50Ms, ks.RecvP99Ms)
	}
}

// TestKindStatsSnapshotResetsWindow ensures latency samples reset each
// snapshot while cumulative lastSeq/dropped persist.
func TestKindStatsSnapshotResetsWindow(t *testing.T) {
	c := newKindStatsCollector()
	const pub = 2_000_000
	c.observe("video", 1, pub, pub+5_000)
	first := c.snapshot()
	if first[0].RecvP50Ms != 5 {
		t.Fatalf("first p50: got %d want 5", first[0].RecvP50Ms)
	}
	// No new samples; percentiles reset to 0 but lastSeq persists.
	second := c.snapshot()
	if second[0].RecvP50Ms != 0 {
		t.Fatalf("second p50: got %d want 0 (window should reset)", second[0].RecvP50Ms)
	}
	if second[0].LastSeq != 1 {
		t.Fatalf("lastSeq should persist: got %d want 1", second[0].LastSeq)
	}
}

func TestPercentileNearestRank(t *testing.T) {
	s := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if got := percentile(s, 0.5); got != 6 {
		t.Fatalf("p50 nearest-rank: got %v want 6", got)
	}
	if got := percentile(s, 0.99); got != 10 {
		t.Fatalf("p99: got %v want 10", got)
	}
	if got := percentile([]float64{42}, 0.5); got != 42 {
		t.Fatalf("single: got %v want 42", got)
	}
}
