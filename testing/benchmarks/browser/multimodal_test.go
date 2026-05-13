// Browser multi-modal benchmark. Tests whether the SCTP HOL
// blocking that bites pion's canonical 1-PC + 2-DC pattern in the
// Go-side benchmark also bites real Chrome's libwebrtc — and
// whether quicrtc's multi-stream architecture wins in the browser.
package browser

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/chromedp/cdproto/runtime"
	"github.com/chromedp/chromedp"

	"github.com/sachinkesiraju/quicrtc/datachannel"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/track"
)

const (
	mmTokensN   = 200
	mmVideoN    = 30
	mmTokenSize = 64
	mmVideoSize = 50 * 1024             // 50 KiB — chunky frame
	mmTokenPace = 5 * time.Millisecond  // 200 tokens/sec
	mmVideoPace = 33 * time.Millisecond // 30 fps
)

func TestBrowserMultimodal_QuicrtcNative(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping browser test in short mode")
	}
	o, err := NewOrchestrator()
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	// Set up a multi-modal quicrtc publisher: video via AddTrack
	// (AU pipeline → uni streams), tokens via DataChannel
	// (control stream).
	videoSender, err := o.QuicrtcPC().AddTrack(context.Background(), track.Video("primary", "test"))
	if err != nil {
		t.Fatal(err)
	}
	o.SetQuicrtcSender(videoSender)

	allocCtx, cancel := chromedp.NewExecAllocator(context.Background(), chromeOpts()...)
	defer cancel()
	ctx, cancel2 := chromedp.NewContext(allocCtx, chromedp.WithLogf(t.Logf))
	defer cancel2()
	chromedp.ListenTarget(ctx, func(ev interface{}) {
		if e, ok := ev.(*runtime.EventConsoleAPICalled); ok {
			args := []string{}
			for _, a := range e.Args {
				if a.Value != nil {
					args = append(args, string(a.Value))
				}
			}
			t.Logf("[browser %s] %v", e.Type, args)
		}
	})

	pageURL := o.QuicrtcMultimodalURL(mmTokensN, mmVideoN)
	t.Logf("→ %s", pageURL)
	go func() {
		_ = chromedp.Run(ctx, chromedp.Navigate(pageURL), chromedp.WaitVisible("#status"))
	}()
	time.Sleep(1500 * time.Millisecond) // let HELLO/SDP land + DC arrive

	// Get the subscriber's DC handle (server-side view) so we can
	// send tokens via DataChannel.
	tokenDC, err := o.QuicrtcPC().DataChannel()
	if err != nil {
		t.Fatalf("get DC: %v", err)
	}
	_ = tokenDC // we use it via send below

	// Drive video sends + token sends concurrently.
	doneVideo := make(chan struct{})
	doneToken := make(chan struct{})

	go func() {
		defer close(doneVideo)
		ticker := time.NewTicker(mmVideoPace)
		defer ticker.Stop()
		for i := 0; i < mmVideoN; i++ {
			isKey := i == 0
			ts := time.Now()
			err := videoSender.Send(context.Background(), pubsub.AccessUnit{
				Bytes:    makeTimedPayload(mmVideoSize, ts),
				Keyframe: isKey,
				PTSMicro: uint64(ts.UnixNano()),
			})
			if err != nil {
				t.Errorf("video send: %v", err)
				return
			}
			if i < mmVideoN-1 {
				<-ticker.C
			}
		}
	}()

	go func() {
		defer close(doneToken)
		ticker := time.NewTicker(mmTokenPace)
		defer ticker.Stop()
		for i := 0; i < mmTokensN; i++ {
			ts := time.Now()
			if err := sendDataChannelTimed(tokenDC, mmTokenSize, ts); err != nil {
				t.Errorf("token send: %v", err)
				return
			}
			if i < mmTokensN-1 {
				<-ticker.C
			}
		}
	}()

	<-doneVideo
	<-doneToken

	resCtx, resCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer resCancel()
	res, err := o.AwaitResult(resCtx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Error != "" {
		t.Fatalf("browser: %s", res.Error)
	}
	logMultimodalResult(t, "quicrtc-multimodal (browser)", res)
}

func TestBrowserMultimodal_PionWebRTC(t *testing.T) {
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
	ctx, cancel2 := chromedp.NewContext(allocCtx, chromedp.WithLogf(t.Logf))
	defer cancel2()

	tokenOpen := make(chan struct{})
	videoOpen := make(chan struct{})
	o.PionTokenDC().OnOpen(func() { close(tokenOpen) })
	o.PionVideoDC().OnOpen(func() { close(videoOpen) })

	pageURL := o.WebRTCMultimodalURL(mmTokensN, mmVideoN)
	t.Logf("→ %s", pageURL)
	go func() {
		_ = chromedp.Run(ctx, chromedp.Navigate(pageURL), chromedp.WaitVisible("#status"))
	}()

	for _, ch := range []chan struct{}{tokenOpen, videoOpen} {
		select {
		case <-ch:
		case <-time.After(15 * time.Second):
			t.Fatal("DC didn't open in 15s")
		}
	}

	doneVideo := make(chan struct{})
	doneToken := make(chan struct{})

	go func() {
		defer close(doneVideo)
		ticker := time.NewTicker(mmVideoPace)
		defer ticker.Stop()
		for i := 0; i < mmVideoN; i++ {
			ts := time.Now()
			if err := o.PionVideoDC().Send(makeTimedPayload(mmVideoSize, ts)); err != nil {
				t.Errorf("video send: %v", err)
				return
			}
			if i < mmVideoN-1 {
				<-ticker.C
			}
		}
	}()

	go func() {
		defer close(doneToken)
		ticker := time.NewTicker(mmTokenPace)
		defer ticker.Stop()
		for i := 0; i < mmTokensN; i++ {
			ts := time.Now()
			if err := o.PionTokenDC().Send(makeTimedPayload(mmTokenSize, ts)); err != nil {
				t.Errorf("token send: %v", err)
				return
			}
			if i < mmTokensN-1 {
				<-ticker.C
			}
		}
	}()

	<-doneVideo
	<-doneToken

	resCtx, resCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer resCancel()
	res, err := o.AwaitResult(resCtx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Error != "" {
		t.Fatalf("browser: %s", res.Error)
	}
	logMultimodalResult(t, "webrtc-multimodal (browser)", res)
}

// ----- helpers -----

func sendDataChannelTimed(dc *datachannel.Channel, size int, ts time.Time) error {
	if size < 8 {
		size = 8
	}
	b := make([]byte, size)
	binary.BigEndian.PutUint64(b[:8], uint64(ts.UnixNano()))
	return dc.Send(b)
}

func logMultimodalResult(t *testing.T, label string, r *BrowserResult) {
	t.Helper()
	if r == nil {
		t.Errorf("%s: nil result", label)
		return
	}
	if r.TokenN == 0 || r.VideoN == 0 {
		t.Errorf("%s: empty result (tokens=%d video=%d)", label, r.TokenN, r.VideoN)
		return
	}
	tokStats := computeJitter(r.TokenRecvMs, r.TokenSentNs)
	vidStats := computeJitter(r.VideoRecvMs, r.VideoSentNs)
	t.Logf("%-30s", label)
	t.Logf("  TOKEN  n=%d  jitter %s", r.TokenN, tokStats)
	t.Logf("  VIDEO  n=%d  jitter %s", r.VideoN, vidStats)
}

type jitterStats struct {
	Min, Mean, P50, P95, P99, Max time.Duration
}

func (s jitterStats) String() string {
	return fmt.Sprintf("min=%s mean=%s p50=%s p95=%s p99=%s max=%s",
		s.Min.Round(time.Microsecond), s.Mean.Round(time.Microsecond),
		s.P50.Round(time.Microsecond), s.P95.Round(time.Microsecond),
		s.P99.Round(time.Microsecond), s.Max.Round(time.Microsecond))
}

func computeJitter(recvMs, sentNs []float64) jitterStats {
	if len(recvMs) == 0 || len(recvMs) != len(sentNs) {
		return jitterStats{}
	}
	r0 := recvMs[0]
	s0 := sentNs[0]
	deltas := make([]time.Duration, len(recvMs))
	for i := range recvMs {
		recvDeltaMs := recvMs[i] - r0
		sentDeltaMs := (sentNs[i] - s0) / 1e6
		d := recvDeltaMs - sentDeltaMs
		if d < 0 {
			d = -d
		}
		deltas[i] = time.Duration(d * 1e6) // ms→ns
	}
	sort.Slice(deltas, func(i, j int) bool { return deltas[i] < deltas[j] })
	var sum time.Duration
	for _, d := range deltas {
		sum += d
	}
	pct := func(p float64) time.Duration {
		idx := int(float64(len(deltas)-1) * p)
		return deltas[idx]
	}
	return jitterStats{
		Min:  deltas[0],
		Mean: sum / time.Duration(len(deltas)),
		P50:  pct(0.50),
		P95:  pct(0.95),
		P99:  pct(0.99),
		Max:  deltas[len(deltas)-1],
	}
}

// suppress unused-import warning if test file is built standalone
var _ *server.Server
