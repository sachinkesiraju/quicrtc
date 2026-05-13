// Per-session rate limiting for inbound (PublishBack) traffic.
// Token-bucket on both AUs/sec and bytes/sec so a single misbehaving
// client cannot saturate a session.
package session

import (
	"sync"
	"time"
)

// RateLimit configures per-session caps on inbound AU rate and
// throughput. Zero values disable the corresponding limit.
type RateLimit struct {
	// MaxAUsPerSecond caps inbound AccessUnit rate. Excess AUs are
	// dropped at the demuxer (they never reach the application).
	// 0 = no rate cap.
	MaxAUsPerSecond int

	// MaxBytesPerSecond caps inbound throughput. Excess bytes are
	// dropped (the AU containing them is dropped). 0 = no byte cap.
	MaxBytesPerSecond int64

	// BurstAUs is the bucket size for AU rate (defaults to
	// MaxAUsPerSecond if 0).
	BurstAUs int

	// BurstBytes is the bucket size for byte rate (defaults to
	// MaxBytesPerSecond if 0).
	BurstBytes int64
}

// rateLimiter is a token-bucket pair (AUs and bytes). Cheap; safe
// for concurrent use.
type rateLimiter struct {
	mu sync.Mutex

	auTokens    float64
	auCap       float64
	auRate      float64 // tokens/sec
	bytesTokens float64
	bytesCap    float64
	bytesRate   float64

	last time.Time
}

func newRateLimiter(cfg RateLimit) *rateLimiter {
	if cfg.BurstAUs <= 0 {
		cfg.BurstAUs = cfg.MaxAUsPerSecond
	}
	if cfg.BurstBytes <= 0 {
		cfg.BurstBytes = cfg.MaxBytesPerSecond
	}
	return &rateLimiter{
		auTokens:    float64(cfg.BurstAUs),
		auCap:       float64(cfg.BurstAUs),
		auRate:      float64(cfg.MaxAUsPerSecond),
		bytesTokens: float64(cfg.BurstBytes),
		bytesCap:    float64(cfg.BurstBytes),
		bytesRate:   float64(cfg.MaxBytesPerSecond),
		last:        time.Now(),
	}
}

// allow returns true if an AU of the given size can proceed. False
// means the AU should be dropped (any limit hit). Both AU-rate and
// byte-rate buckets are checked; both must have tokens available.
func (l *rateLimiter) allow(auBytes int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(l.last).Seconds()
	l.last = now

	if l.auRate > 0 {
		l.auTokens += elapsed * l.auRate
		if l.auTokens > l.auCap {
			l.auTokens = l.auCap
		}
		if l.auTokens < 1 {
			return false
		}
	}
	if l.bytesRate > 0 {
		l.bytesTokens += elapsed * l.bytesRate
		if l.bytesTokens > l.bytesCap {
			l.bytesTokens = l.bytesCap
		}
		need := float64(auBytes)
		if l.bytesTokens < need {
			return false
		}
	}
	if l.auRate > 0 {
		l.auTokens--
	}
	if l.bytesRate > 0 {
		l.bytesTokens -= float64(auBytes)
	}
	return true
}

// noLimits returns true when both rates are unset — Allow is then
// always true and we can skip the lock entirely.
func (l *rateLimiter) noLimits() bool {
	return l.auRate <= 0 && l.bytesRate <= 0
}
