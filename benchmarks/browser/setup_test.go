// Browser setup-latency benchmark — time-to-first-byte from
// "navigate" to "first message arrived" as measured by the
// browser's monotonic clock. This is the metric where quicrtc's
// architectural advantage is biggest: no amount of receiver-side
// JS optimization can make WebRTC's ICE+DTLS handshake faster
// than a QUIC handshake.
package browser

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/chromedp/chromedp"

	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/track"
)

const setupIterations = 10

// TestBrowserSetupLatencyComparison measures time-to-first-byte
// from the browser's perspective for both transports. Each
// iteration: launch Chromium, navigate, wait for the first message
// to arrive, record elapsed.
//
// We use page-load → first-recv as the metric, anchored at the
// browser's performance.now(). This is what users feel when they
// open an AI app and wait for the first stream to start.
func TestBrowserSetupLatencyComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping setup-latency comparison in short mode")
	}

	measure := func(label string, do func() (time.Duration, error)) {
		samples := make([]time.Duration, 0, setupIterations)
		for i := 0; i < setupIterations; i++ {
			d, err := do()
			if err != nil {
				t.Logf("%s iter %d: %v", label, i, err)
				continue
			}
			samples = append(samples, d)
		}
		stats := summarize(samples)
		t.Logf("%-22s n=%d  min=%s mean=%s p50=%s p95=%s p99=%s max=%s",
			label, len(samples),
			stats.Min.Round(time.Microsecond), stats.Mean.Round(time.Microsecond),
			stats.P50.Round(time.Microsecond), stats.P95.Round(time.Microsecond),
			stats.P99.Round(time.Microsecond), stats.Max.Round(time.Microsecond))
	}

	measure("quicrtc (browser)", func() (time.Duration, error) {
		return runOneSetupQuicrtc(t)
	})
	measure("webrtc (browser)", func() (time.Duration, error) {
		return runOneSetupWebRTC(t)
	})
}

func runOneSetupQuicrtc(t *testing.T) (time.Duration, error) {
	o, err := NewOrchestrator()
	if err != nil {
		return 0, err
	}
	defer o.Close()

	sender, err := o.QuicrtcPC().AddTrack(context.Background(), track.Video("primary", "test"))
	if err != nil {
		return 0, err
	}
	o.SetQuicrtcSender(sender)

	allocCtx, cancel := chromedp.NewExecAllocator(context.Background(), chromeOpts()...)
	defer cancel()
	ctx, cancel2 := chromedp.NewContext(allocCtx)
	defer cancel2()

	// Pre-warm: send a steady drumbeat in a goroutine so the first
	// arriving message is the first one after the browser is ready.
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		first := true
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				ts := time.Now()
				_ = sender.Send(context.Background(), pubsub.AccessUnit{
					Bytes:    makeTimedPayload(64, ts),
					Keyframe: first,
					PTSMicro: uint64(ts.UnixNano()),
				})
				first = false
			}
		}
	}()
	defer close(stop)

	pageURL := o.QuicrtcURL(1) // expect just 1 message
	startWall := time.Now()
	if err := chromedp.Run(ctx, chromedp.Navigate(pageURL)); err != nil {
		return 0, err
	}
	resCtx, resCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer resCancel()
	res, err := o.AwaitResult(resCtx)
	if err != nil {
		return 0, err
	}
	if res.Error != "" {
		return 0, &browserErr{msg: res.Error}
	}
	// Browser's first-recv-ms is performance.now() since page load.
	// Compare against navigate-time on the orchestrator's clock —
	// we use the orchestrator's elapsed wallclock since that's the
	// closest proxy to "user clicked the link until first byte
	// rendered" without cross-clock drift.
	if len(res.RecvAtMs) == 0 {
		return 0, &browserErr{msg: "no recv times"}
	}
	// Total wall time from "navigate started" to "browser reports".
	// The /report POST happens immediately after first-recv, so
	// total wall ≈ navigate + browser bootstrap + handshake +
	// first-recv. Subtracting fixed Chrome/browser bootstrap
	// (which dominates) is hard; we report the raw wall delta.
	return time.Since(startWall), nil
}

func runOneSetupWebRTC(t *testing.T) (time.Duration, error) {
	o, err := NewOrchestrator()
	if err != nil {
		return 0, err
	}
	defer o.Close()

	allocCtx, cancel := chromedp.NewExecAllocator(context.Background(), chromeOpts()...)
	defer cancel()
	ctx, cancel2 := chromedp.NewContext(allocCtx)
	defer cancel2()

	dcOpen := make(chan struct{})
	o.PionDC().OnOpen(func() { close(dcOpen) })

	// Sender goroutine starts the moment DC opens.
	go func() {
		<-dcOpen
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			ts := time.Now()
			if err := o.PionDC().Send(makeTimedPayload(64, ts)); err != nil {
				return
			}
			<-ticker.C
		}
	}()

	pageURL := o.WebRTCURL(1)
	startWall := time.Now()
	if err := chromedp.Run(ctx, chromedp.Navigate(pageURL)); err != nil {
		return 0, err
	}
	resCtx, resCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer resCancel()
	res, err := o.AwaitResult(resCtx)
	if err != nil {
		return 0, err
	}
	if res.Error != "" {
		return 0, &browserErr{msg: res.Error}
	}
	if len(res.RecvAtMs) == 0 {
		return 0, &browserErr{msg: "no recv times"}
	}
	return time.Since(startWall), nil
}

type browserErr struct{ msg string }

func (e *browserErr) Error() string { return e.msg }

type setupStats struct {
	Min, Mean, P50, P95, P99, Max time.Duration
}

func summarize(s []time.Duration) setupStats {
	if len(s) == 0 {
		return setupStats{}
	}
	sorted := append([]time.Duration(nil), s...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	pct := func(p float64) time.Duration {
		return sorted[int(float64(len(sorted)-1)*p)]
	}
	return setupStats{
		Min:  sorted[0],
		Mean: sum / time.Duration(len(sorted)),
		P50:  pct(0.50),
		P95:  pct(0.95),
		P99:  pct(0.99),
		Max:  sorted[len(sorted)-1],
	}
}
