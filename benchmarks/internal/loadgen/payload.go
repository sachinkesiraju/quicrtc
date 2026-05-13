package loadgen

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"
)

// TokenStreamResult bundles the headline stats for a token-stream
// run: per-event end-to-end latency, inter-arrival jitter, and the
// total wall-clock span from first-receive to last-receive.
type TokenStreamResult struct {
	EndToEnd     LatencyStats
	InterArrival LatencyStats
	TotalTime    time.Duration
}

func (r TokenStreamResult) Summary() string {
	var b strings.Builder
	fmt.Fprintf(&b, "[end-to-end %s]  [inter-arrival %s]  total=%s",
		r.EndToEnd, r.InterArrival, r.TotalTime.Round(time.Microsecond))
	return b.String()
}

// MakeTokenPayload builds a 64-byte token payload: 8 bytes timestamp
// (UnixNano) followed by 0xab filler. The receiver decodes the
// timestamp via DecodeStamp.
func MakeTokenPayload(t time.Time) []byte {
	const size = 64
	out := make([]byte, size)
	binary.BigEndian.PutUint64(out[:8], uint64(t.UnixNano()))
	for i := 8; i < size; i++ {
		out[i] = 0xab
	}
	return out
}

// TimedPayload is a variable-size payload: 8-byte UnixNano timestamp
// prefix, 0xcd filler. Used by benchmarks where AU sizes vary per
// stream (video frames, tokens, actions, telemetry samples).
func TimedPayload(size int, t time.Time) []byte {
	if size < 8 {
		size = 8
	}
	b := make([]byte, size)
	binary.BigEndian.PutUint64(b[:8], uint64(t.UnixNano()))
	for i := 8; i < size; i++ {
		b[i] = 0xcd
	}
	return b
}

// DecodeStamp pulls the 8-byte UnixNano timestamp prefix from a
// MakeTokenPayload- or TimedPayload-encoded buffer.
func DecodeStamp(b []byte) time.Time {
	if len(b) < 8 {
		return time.Time{}
	}
	return time.Unix(0, int64(binary.BigEndian.Uint64(b[:8])))
}

// PercentReduction returns the percentage delta from before → after.
// Positive means after is smaller (improvement).
func PercentReduction(before, after time.Duration) float64 {
	if before == 0 {
		return 0
	}
	return (1 - float64(after)/float64(before)) * 100
}
