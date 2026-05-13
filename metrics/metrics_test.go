package metrics

import (
	"testing"
	"time"
)

func TestNoopMetricsSafe(t *testing.T) {
	// All Noop methods must be callable concurrently without panic.
	var m Metrics = NoopMetrics{}
	done := make(chan struct{}, 8)
	for i := 0; i < 8; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 1000; j++ {
				m.SessionStarted()
				m.SessionEnded(time.Millisecond)
				m.TrackAdded("video")
				m.AUSent("tokens", 64)
				m.AURecv("tokens", 64)
				m.AUDropped("video", "slow_consumer")
				m.StreamOpened("video")
				m.StreamReset("video", "keyframe")
				m.HandshakeDuration(time.Millisecond)
				m.AuthFailed("slug")
			}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
}

func TestInMemorySnapshot(t *testing.T) {
	m := NewInMemory()
	m.SessionStarted()
	m.SessionStarted()
	m.SessionEnded(50 * time.Millisecond)
	m.TrackAdded("video")
	m.TrackAdded("video")
	m.TrackAdded("tokens")
	m.AUSent("tokens", 64)
	m.AUSent("tokens", 64)
	m.AURecv("video", 50000)
	m.AUDropped("video", "slow_consumer")

	snap := m.Snapshot()
	if snap.SessionsStarted != 2 {
		t.Errorf("SessionsStarted = %d, want 2", snap.SessionsStarted)
	}
	if snap.SessionsEnded != 1 {
		t.Errorf("SessionsEnded = %d, want 1", snap.SessionsEnded)
	}
	if got := snap.TracksByKind["video"]; got != 2 {
		t.Errorf("video tracks = %d, want 2", got)
	}
	if got := snap.TracksByKind["tokens"]; got != 1 {
		t.Errorf("tokens tracks = %d, want 1", got)
	}
	if got := snap.AUSentByKind["tokens"]; got != 2 {
		t.Errorf("tokens AUs sent = %d, want 2", got)
	}
	if got := snap.BytesSentByKind["tokens"]; got != 128 {
		t.Errorf("tokens bytes sent = %d, want 128", got)
	}
	if got := snap.BytesRecvByKind["video"]; got != 50000 {
		t.Errorf("video bytes recv = %d, want 50000", got)
	}
	if got := snap.DropsByKind["video:slow_consumer"]; got != 1 {
		t.Errorf("video drops = %d, want 1", got)
	}
}
