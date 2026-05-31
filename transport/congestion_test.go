package transport

import (
	"testing"
	"time"
)

func TestSendRateMeterTrailingWindow(t *testing.T) {
	m := NewSendRateMeter(100 * time.Millisecond)
	now := time.Unix(0, 0)

	for i := 0; i < 10; i++ {
		m.Add(1000, now.Add(time.Duration(i)*10*time.Millisecond))
	}

	r := m.Rate(now.Add(100 * time.Millisecond))
	if r < 80_000 || r > 200_000 {
		t.Fatalf("rate outside expected band: got %d B/s", r)
	}
}

func TestSendRateMeterAgesOut(t *testing.T) {
	m := NewSendRateMeter(50 * time.Millisecond)
	now := time.Unix(0, 0)
	m.Add(1000, now)
	r := m.Rate(now.Add(200 * time.Millisecond))
	if r != 0 {
		t.Fatalf("expected rate to age to 0, got %d", r)
	}
}

func TestSendRateMeterEmpty(t *testing.T) {
	m := NewSendRateMeter(time.Second)
	if r := m.Rate(time.Now()); r != 0 {
		t.Fatalf("empty meter rate = %d, want 0", r)
	}
}
