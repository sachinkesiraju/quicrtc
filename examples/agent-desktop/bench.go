// Headless bench mode: connect a Go client to each transport through
// the same emulated link the browser would use, collect per-lane
// delivery latency for a fixed window, and print the comparison
// table. This is the browserless, CI-able version of what the demo
// page shows live, and the source of the README numbers.
//
// Latency here is one-way publish→receive wall time. Both clients run
// in this process on the same clock as the publisher, so the numbers
// are directly comparable — no clock-sync caveat.
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"time"

	"golang.org/x/net/websocket"

	"github.com/sachinkesiraju/quicrtc/client"
	"github.com/sachinkesiraju/quicrtc/examples/internal/status"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// laneStats accumulates one lane's latency samples for one transport.
type laneStats struct {
	mu      sync.Mutex
	samples []float64 // ms
	lastAt  time.Time // arrival time of the previous AU (gap tracking)
	maxGap  float64   // ms, largest inter-arrival gap (screen freeze)
	count   int
}

func (l *laneStats) add(latMs float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if !l.lastAt.IsZero() {
		if gap := float64(now.Sub(l.lastAt)) / 1e6; gap > l.maxGap {
			l.maxGap = gap
		}
	}
	l.lastAt = now
	l.samples = append(l.samples, latMs)
	l.count++
}

func (l *laneStats) percentile(p float64) float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.samples) == 0 {
		return 0
	}
	s := make([]float64, len(l.samples))
	copy(s, l.samples)
	sort.Float64s(s)
	idx := int(p * float64(len(s)-1))
	return s[idx]
}

func (l *laneStats) gap() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.maxGap
}

func (l *laneStats) n() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.count
}

type transportStats struct {
	screen, tokens, tools, telemetry laneStats
}

func (t *transportStats) lane(lane int) *laneStats {
	switch lane {
	case laneScreen:
		return &t.screen
	case laneReasoning:
		return &t.tokens
	case laneToolCalls:
		return &t.tools
	case laneTelemetry:
		return &t.telemetry
	default:
		return nil
	}
}

func latMs(ptsMicro uint64) float64 {
	return float64(time.Now().UnixMicro()-int64(ptsMicro)) / 1e3
}

// runBench connects both clients, samples for dur (after a short
// warmup), prints the table, and returns.
func runBench(ctx context.Context, shareURL, wsURL string, dur time.Duration, st *status.Status) error {
	fmt.Printf("\nbench: sampling both transports for %v (plus 3s warmup)…\n", dur)

	benchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	qStats := &transportStats{}
	wStats := &transportStats{}
	warmupOver := time.Now().Add(3 * time.Second)
	record := func(t *transportStats, lane int, pts uint64) {
		if time.Now().Before(warmupOver) {
			return
		}
		if ls := t.lane(lane); ls != nil {
			ls.add(latMs(pts))
		}
	}

	// quicrtc client.
	qc, err := client.Dial(benchCtx, shareURL, client.Options{})
	if err != nil {
		return fmt.Errorf("quicrtc dial: %w", err)
	}
	defer qc.Close()
	for lane, name := range map[int]string{
		laneScreen: trackScreen, laneReasoning: trackReasoning, laneToolCalls: trackToolCalls,
	} {
		go func(lane int, name string) {
			for {
				au, err := qc.RecvOn(benchCtx, name)
				if err != nil {
					return
				}
				record(qStats, lane, au.PTSMicro)
			}
		}(lane, name)
	}
	go func() {
		for {
			raw, err := qc.ReceiveDatagram(benchCtx)
			if err != nil {
				return
			}
			_, _, payload, err := wire.DecodeDatagram(raw)
			if err != nil {
				continue
			}
			if pts, ok := telemetryTS(payload); ok {
				record(qStats, laneTelemetry, pts)
			}
		}
	}()

	// WebSocket baseline client.
	ws, err := websocket.Dial(wsURL, "", "http://127.0.0.1/")
	if err != nil {
		return fmt.Errorf("websocket dial: %w", err)
	}
	defer ws.Close()
	go func() {
		<-benchCtx.Done()
		_ = ws.Close()
	}()
	go func() {
		for {
			var msg []byte
			if err := websocket.Message.Receive(ws, &msg); err != nil {
				return
			}
			if len(msg) < wsEnvelopeLen {
				continue
			}
			lane := int(msg[0])
			pts := binary.BigEndian.Uint64(msg[5:13])
			record(wStats, lane, pts)
		}
	}()

	select {
	case <-ctx.Done():
	case <-time.After(3*time.Second + dur):
	}
	cancel()

	printBenchTable(qStats, wStats)
	return nil
}

// telemetryTS extracts "ts_us" from the telemetry JSON without a full
// parse dependency in the hot path.
func telemetryTS(body []byte) (uint64, bool) {
	const key = `"ts_us":`
	i := indexBytes(body, key)
	if i < 0 {
		return 0, false
	}
	i += len(key)
	var v uint64
	seen := false
	for ; i < len(body) && body[i] >= '0' && body[i] <= '9'; i++ {
		v = v*10 + uint64(body[i]-'0')
		seen = true
	}
	return v, seen
}

func indexBytes(b []byte, sub string) int {
	for i := 0; i+len(sub) <= len(b); i++ {
		if string(b[i:i+len(sub)]) == sub {
			return i
		}
	}
	return -1
}

func printBenchTable(q, w *transportStats) {
	fmt.Printf("\n── delivery latency, publish → receive (one-way, ms) ─────────────────────\n")
	fmt.Printf("%-22s %10s %10s %10s %10s %8s\n", "lane", "ws p50", "ws p99", "quicrtc p50", "quicrtc p99", "p99 ×")
	row := func(name string, wl, ql *laneStats) {
		ratio := "-"
		if qp := ql.percentile(0.99); qp > 0 {
			ratio = fmt.Sprintf("%.1f×", wl.percentile(0.99)/qp)
		}
		fmt.Printf("%-22s %10.1f %10.1f %10.1f %10.1f %8s\n",
			fmt.Sprintf("%s (n=%d/%d)", name, wl.n(), ql.n()),
			wl.percentile(0.50), wl.percentile(0.99),
			ql.percentile(0.50), ql.percentile(0.99), ratio)
	}
	row("reasoning tokens", &w.tokens, &q.tokens)
	row("tool calls", &w.tools, &q.tools)
	row("telemetry", &w.telemetry, &q.telemetry)
	row("screen frames", &w.screen, &q.screen)
	fmt.Printf("\nworst screen-frame gap (freeze): websocket %.0f ms, quicrtc %.0f ms\n",
		w.screen.gap(), q.screen.gap())
	fmt.Println("(same engine, same events, same timestamps, same emulated link)")
}
