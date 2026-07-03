// Browser e2e benchmark: uses real headless Chromium as the
// receiver, comparing native WebTransport (quicrtc) vs native
// WebRTC RTCPeerConnection. Measures end-to-end latency from
// the browser's perspective — what real users experience.
//
// Prerequisites: macOS with Google Chrome installed at the
// default location, OR set CHROME_PATH env var.
package browser

import (
	"context"
	"encoding/binary"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chromedp/cdproto/runtime"
	"github.com/chromedp/chromedp"

	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/track"
)

const (
	browserN       = 100
	browserPace    = 5 * time.Millisecond // 200 msg/s
	browserPayload = 64
)

// chromeOpts returns chromedp options targeting Chrome.
func chromeOpts() []chromedp.ExecAllocatorOption {
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("headless", true),
		chromedp.Flag("disable-web-security", true),
		chromedp.Flag("ignore-certificate-errors", true),
		chromedp.Flag("allow-insecure-localhost", true),
		chromedp.Flag("autoplay-policy", "no-user-gesture-required"),
		chromedp.Flag("origin-to-force-quic-on", "127.0.0.1"),
	)
	if path := os.Getenv("CHROME_PATH"); path != "" {
		opts = append(opts, chromedp.ExecPath(path))
	} else if _, err := os.Stat("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"); err == nil {
		opts = append(opts, chromedp.ExecPath("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"))
	} else if _, err := os.Stat("/usr/local/bin/google-chrome"); err == nil {
		opts = append(opts, chromedp.ExecPath("/usr/local/bin/google-chrome"))
	}
	return opts
}

func TestBrowserChat_QuicrtcNative(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping browser test in short mode")
	}
	o, err := NewOrchestrator()
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	sender, err := o.QuicrtcPC().AddTrack(context.Background(), track.Video("primary", "test"))
	if err != nil {
		t.Fatal(err)
	}
	o.SetQuicrtcSender(sender)

	allocCtx, cancel := chromedp.NewExecAllocator(context.Background(), chromeOpts()...)
	defer cancel()
	ctx, cancel2 := chromedp.NewContext(allocCtx, chromedp.WithLogf(t.Logf))
	defer cancel2()

	// Capture browser console messages for debugging.
	chromedp.ListenTarget(ctx, func(ev interface{}) {
		switch e := ev.(type) {
		case *runtime.EventConsoleAPICalled:
			args := make([]string, 0, len(e.Args))
			for _, a := range e.Args {
				if a.Value != nil {
					args = append(args, string(a.Value))
				} else {
					args = append(args, a.Description)
				}
			}
			t.Logf("[browser %s] %s", e.Type, strings.Join(args, " "))
		case *runtime.EventExceptionThrown:
			t.Logf("[browser exception] %s", e.ExceptionDetails.Error())
		}
	})

	// Navigate the browser to the receiver page in the background.
	pageURL := o.QuicrtcURL(browserN)
	t.Logf("navigating browser → %s", pageURL)
	navDone := make(chan error, 1)
	go func() {
		navDone <- chromedp.Run(ctx, chromedp.Navigate(pageURL), chromedp.WaitVisible("#status"))
	}()

	select {
	case err := <-navDone:
		if err != nil {
			t.Fatalf("browser nav: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("browser nav timeout")
	}
	time.Sleep(1000 * time.Millisecond)

	// Send messages.
	// Track time-to-first-message: send the first message NOW
	// (timestamped); subsequent sends are paced. The receiver's
	// first arrival timestamp lets us compute setup-to-first-msg
	// latency in browser-monotonic ms.
	firstSendTs := time.Now()
	sendIdx := 0
	sendDone := drivePacedSender(t, browserN, browserPace, func(ts time.Time) error {
		isKey := sendIdx == 0
		sendIdx++
		return sender.Send(context.Background(), pubsub.AccessUnit{
			Bytes:    makeTimedPayload(browserPayload, ts),
			Keyframe: isKey,
			PTSMicro: uint64(ts.UnixNano()),
		})
	})
	<-sendDone
	t.Logf("send-side: first message sent at %v (orchestrator wallclock)", firstSendTs)

	resCtx, resCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer resCancel()
	res, err := o.AwaitResult(resCtx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Error != "" {
		t.Fatalf("browser reported: %s", res.Error)
	}
	logBrowserResult(t, "quicrtc-native (browser)", res)
}

func TestBrowserChat_PionWebRTC(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping browser test in short mode")
	}
	o, err := NewOrchestrator()
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	allocCtx, cancel := chromedp.NewExecAllocator(context.Background(), chromeOpts()...)
	defer cancel()
	ctx, cancel2 := chromedp.NewContext(allocCtx)
	defer cancel2()

	// Wait for the browser DC to open before sending. We poll
	// the pion DC's ReadyState.
	dcOpen := make(chan struct{})
	o.PionDC().OnOpen(func() { close(dcOpen) })

	pageURL := o.WebRTCURL(browserN)
	t.Logf("navigating browser → %s", pageURL)
	go func() {
		_ = chromedp.Run(ctx, chromedp.Navigate(pageURL), chromedp.WaitVisible("#status"))
	}()

	select {
	case <-dcOpen:
	case <-time.After(15 * time.Second):
		t.Fatal("WebRTC DC did not open in 15s")
	}

	firstSendTs := time.Now()
	sendDone := drivePacedSender(t, browserN, browserPace, func(ts time.Time) error {
		return o.PionDC().Send(makeTimedPayload(browserPayload, ts))
	})
	<-sendDone
	t.Logf("send-side: first message sent at %v (orchestrator wallclock)", firstSendTs)

	resCtx, resCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer resCancel()
	res, err := o.AwaitResult(resCtx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Error != "" {
		t.Fatalf("browser reported: %s", res.Error)
	}
	logBrowserResult(t, "webrtc (browser)", res)
}

// ----- helpers -----

func makeTimedPayload(size int, ts time.Time) []byte {
	if size < 8 {
		size = 8
	}
	b := make([]byte, size)
	binary.BigEndian.PutUint64(b[:8], uint64(ts.UnixNano()))
	return b
}

func drivePacedSender(t *testing.T, n int, pace time.Duration, send func(time.Time) error) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(pace)
		defer ticker.Stop()
		for i := 0; i < n; i++ {
			if err := send(time.Now()); err != nil {
				t.Errorf("send %d: %v", i, err)
				return
			}
			if i < n-1 {
				<-ticker.C
			}
		}
	}()
	return done
}

func logBrowserResult(t *testing.T, label string, r *BrowserResult) {
	t.Helper()
	if r == nil || r.N == 0 {
		t.Errorf("%s: no results", label)
		return
	}
	// Compute end-to-end latency:
	//   recv_ms (page-relative) - sent_ms (page-relative)
	// where sent_ms = (sentAtNs - first_sentAtNs) / 1e6 + first_recvAtMs - first_recv_to_first_sent_offset
	//
	// Cross-clock alignment: the first message sets the offset
	// between server-wall-clock (ns) and browser-monotonic (ms).
	// Subsequent message latencies are (recv[i] - recv[0]) -
	// (sent[i] - sent[0]) plus the first-message latency (which
	// we approximate as 0, since we can't measure it cross-clock).
	//
	// The result is RELATIVE end-to-end-latency variance from the
	// first message. p50/p99 of (recv_delta - sent_delta) reveals
	// jitter and tail latency.
	if len(r.RecvAtMs) != len(r.SentAtNs) {
		t.Errorf("%s: recv/sent length mismatch", label)
		return
	}
	deltas := make([]time.Duration, len(r.RecvAtMs))
	r0Ms := r.RecvAtMs[0]
	s0Ns := r.SentAtNs[0]
	for i := range r.RecvAtMs {
		recvDeltaMs := r.RecvAtMs[i] - r0Ms
		sentDeltaMs := (r.SentAtNs[i] - s0Ns) / 1e6
		deltas[i] = time.Duration((recvDeltaMs - sentDeltaMs) * 1e6) // ms→ns
	}
	stats := computeStats(deltas)
	t.Logf("%-22s n=%d  jitter min=%s mean=%s p50=%s p95=%s p99=%s max=%s",
		label, len(r.RecvAtMs),
		stats.Min.Round(time.Microsecond),
		stats.Mean.Round(time.Microsecond),
		stats.P50.Round(time.Microsecond),
		stats.P95.Round(time.Microsecond),
		stats.P99.Round(time.Microsecond),
		stats.Max.Round(time.Microsecond),
	)
}

type stats struct {
	Min, Mean, P50, P95, P99, Max time.Duration
}

func computeStats(samples []time.Duration) stats {
	if len(samples) == 0 {
		return stats{}
	}
	sorted := make([]time.Duration, len(samples))
	copy(sorted, samples)
	// Take absolute values for jitter measurement.
	for i := range sorted {
		if sorted[i] < 0 {
			sorted[i] = -sorted[i]
		}
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	pct := func(p float64) time.Duration {
		idx := int(float64(len(sorted)-1) * p)
		return sorted[idx]
	}
	return stats{
		Min:  sorted[0],
		Mean: sum / time.Duration(len(sorted)),
		P50:  pct(0.50),
		P95:  pct(0.95),
		P99:  pct(0.99),
		Max:  sorted[len(sorted)-1],
	}
}

// silence unused import warnings if test file is built standalone
var _ sync.Mutex
