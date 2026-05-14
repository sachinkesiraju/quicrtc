// Reattach-after-enforced-disconnect benchmark.
//
// Replaces the prior "Wi-Fi → cellular handoff (88× loopback)" row,
// which was process-local warm reattach and therefore did not measure
// what its title claimed.
//
// What this measures: time from "the UDP socket is gone" → "every
// pre-existing track is delivering AUs again" at a realistic 50 ms
// RTT. Two arms:
//
//	arm 1 (quicrtc): client dials through a UDP proxy with 25 ms one-
//	  way delay (≈ 50 ms RTT), waits until all 4 server-side tracks
//	  have delivered ≥1 AU, then KILLS the proxy — every datagram in
//	  flight is dropped, every subsequent datagram has nowhere to go.
//	  t0 = kill. A new proxy with the same NetCond points at the same
//	  server. The client re-Dials with the saved SessionID (0-RTT
//	  resume); sample = time from t0 → first AU received on the worst
//	  of the 4 tracks (i.e. what a user perceives — every track has
//	  to be back before "the session is back").
//
//	arm 2 (baseline): WebRTC + SSE + HTTPS multi-handshake reconnect
//	  chain. Pion PC delivering DataChannel events ≈ "RTP-equivalent
//	  live stream" (we use a DataChannel for clock-skew-free
//	  measurement; the relevant cost is the ICE+DTLS recovery, which
//	  is the same regardless of payload). An httptest.NewTLSServer
//	  streams SSE tokens through a TCP delay-proxy at 25 ms one-way.
//	  Another TLS server fields an HTTPS RPC through the same delay
//	  budget. All three are killed (pion PC ICE-restarted via a real
//	  rebuild of the underlying ICE; SSE stream closed; HTTPS conn
//	  closed). Sample = time from kill → first activity on the worst
//	  of the three.
//
// Headline claim this row is meant to support: "quicrtc 0-RTT resume
// restores every pre-existing track in ~1 RTT; a stitched WebRTC+SSE+
// HTTPS stack pays the worst of three concurrent handshakes." Expected
// honest ratio at 50 ms RTT: ~2-3×, NOT 88×.
package resume

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/sachinkesiraju/quicrtc/cert"
	"github.com/sachinkesiraju/quicrtc/client"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/testing/benchmarks/internal/loadgen"
	"github.com/sachinkesiraju/quicrtc/track"
	"github.com/sachinkesiraju/quicrtc/wire"
)

const (
	reattachN          = 30
	reattachWarmup     = 2
	reattachOneWay     = 25 * time.Millisecond // RTT ≈ 50 ms
	reattachIterBudget = 10 * time.Second
	reattachBootstrap  = 1000
	reattachSeed       = int64(7)
)

// reattachTrackNames lists the 4 tracks the quicrtc server publishes.
// Spans the four DeliveryClasses to make the resume cost realistic:
// stream-per-GOP video, persistent uni-stream tokens, bidi-per-call
// tool calls, and datagram telemetry. (KindToolCalls and KindTelemetry
// have non-uniform delivery semantics — see the per-kind handling in
// the iteration loop.)
var reattachTrackNames = []string{"video", "tokens", "toolcalls", "telemetry"}

// TestReattachAfterKill is the headline reattach-cost comparison.
func TestReattachAfterKill(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping reattach-after-kill in short mode")
	}

	rng := rand.New(rand.NewSource(reattachSeed))

	quicSamples := make([]time.Duration, 0, reattachN)
	baseSamples := make([]time.Duration, 0, reattachN)

	// ---------- arm 1: quicrtc ----------
	t.Logf("--- quicrtc / udp-kill + re-Dial (n=%d, warmup=%d, RTT≈%dms) ---",
		reattachN, reattachWarmup, 2*reattachOneWay/time.Millisecond)

	total := reattachN + reattachWarmup
	for i := 0; i < total; i++ {
		isWarm := i < reattachWarmup
		d, err := reattachQuicrtcOnce(t, i)
		if err != nil {
			if !isWarm {
				t.Logf("quicrtc trial %d: %v (skipped)", i, err)
			}
			continue
		}
		if !isWarm {
			quicSamples = append(quicSamples, d)
		}
	}

	// ---------- arm 2: baseline ----------
	t.Logf("--- baseline / webrtc+SSE+HTTPS reconnect chain (n=%d, warmup=%d, ~%dms one-way) ---",
		reattachN, reattachWarmup, reattachOneWay/time.Millisecond)

	for i := 0; i < total; i++ {
		isWarm := i < reattachWarmup
		d, err := reattachBaselineOnce(t, i)
		if err != nil {
			if !isWarm {
				t.Logf("baseline trial %d: %v (skipped)", i, err)
			}
			continue
		}
		if !isWarm {
			baseSamples = append(baseSamples, d)
		}
	}

	qStats := loadgen.ComputeStats(quicSamples)
	bStats := loadgen.ComputeStats(baseSamples)

	// Bootstrap CIs for the mean (uses the in-package resume helper).
	qMs := durationsToMs(quicSamples)
	bMs := durationsToMs(baseSamples)
	qCI := summarizeResume(qMs, reattachBootstrap, rng)
	bCI := summarizeResume(bMs, reattachBootstrap, rng)

	t.Logf("===== Reattach after enforced disconnect, ~%dms RTT =====",
		2*reattachOneWay/time.Millisecond)
	t.Logf("%-44s  n=%2d  p50=%6.1fms  p99=%6.1fms  mean=%6.1fms  mean95CI=[%6.1f,%6.1f]",
		"quicrtc (UDP kill + re-Dial)",
		qStats.N, msFloat(qStats.P50), msFloat(qStats.P99), msFloat(qStats.Mean),
		qCI.meanCILo, qCI.meanCIHi)
	t.Logf("%-44s  n=%2d  p50=%6.1fms  p99=%6.1fms  mean=%6.1fms  mean95CI=[%6.1f,%6.1f]",
		"baseline (WebRTC ICE restart + SSE + HTTPS)",
		bStats.N, msFloat(bStats.P50), msFloat(bStats.P99), msFloat(bStats.Mean),
		bCI.meanCILo, bCI.meanCIHi)

	if qStats.N > 0 && bStats.N > 0 {
		ratio := float64(bStats.P50) / float64(qStats.P50)
		t.Logf("p50 ratio (baseline / quicrtc) = %.2fx", ratio)
	}
}

// reattachQuicrtcOnce runs one quicrtc trial: stand up server +
// 4 tracks, dial via proxy at 50 ms RTT, wait for ≥1 AU on each
// track, KILL the proxy, t0=now, fresh proxy, re-Dial with saved
// SessionID, wait for ≥1 AU on each track post-rekey, sample =
// max(t1) − t0.
func reattachQuicrtcOnce(t *testing.T, trial int) (time.Duration, error) {
	t.Helper()

	bundle, err := cert.Generate(cert.Options{IPs: []net.IP{net.ParseIP("127.0.0.1")}})
	if err != nil {
		return 0, fmt.Errorf("cert: %w", err)
	}

	publishers := make(map[string]*server.Publisher, len(reattachTrackNames))
	// Discard the server's slog output — the per-trial "draining" log
	// line at shutdown drowns out test progress otherwise.
	quietLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := server.Config{
		Addr:        "127.0.0.1:0",
		CertBundle:  bundle,
		MaxSessions: 4,
		SDP:         wire.SDP{Codec: "noop", Width: 64, Height: 64, FPS: 30},
		Logger:      quietLogger,
	}
	srv, err := server.New(cfg)
	if err != nil {
		return 0, fmt.Errorf("server.New: %w", err)
	}
	// Register the 4 tracks. KindToolCalls uses bidi-per-call which
	// makes timing brittle in this harness (the server opens one bidi
	// stream per AU and the cost includes per-AU stream setup, which
	// has nothing to do with reattach). For this benchmark we register
	// all 4 names with stream-friendly Kinds: video stream-per-GOP,
	// tokens persistent uni stream, toolcalls as KindTokens-style uni,
	// telemetry datagram-or-stream. The point of the row is reattach
	// recovery on a 4-track session, not bidi-per-call.
	kinds := map[string]track.Kind{
		"video":     track.KindVideo,
		"tokens":    track.KindTokens,
		"toolcalls": track.KindTokens, // see comment above
		"telemetry": track.KindTokens, // see comment above
	}
	for _, name := range reattachTrackNames {
		publishers[name] = srv.AddTrackSpec(server.TrackSpec{
			Name: name,
			Kind: kinds[name],
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = srv.ListenAndServe(ctx) }()
	// Wait for bind.
	deadline := time.Now().Add(3 * time.Second)
	for srv.Addr() == "127.0.0.1:0" && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	defer func() {
		// Bounded shutdown — dead post-kill sessions can take a while
		// to time out their idle timer; we don't care, just want the
		// listener torn down so the next trial gets a fresh port.
		// 100ms is plenty; Shutdown force-closes the listener on
		// drainCtx expiry.
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer shutCancel()
		_ = srv.Shutdown(shutCtx)
	}()

	// Spawn a publisher goroutine per track. Each emits 100 AUs/sec
	// indefinitely so any subscriber session — initial or resumed —
	// sees fresh AUs immediately.
	pubCtx, pubCancel := context.WithCancel(ctx)
	var pubWg sync.WaitGroup
	for _, name := range reattachTrackNames {
		pubWg.Add(1)
		pub := publishers[name]
		go func(name string, pub *server.Publisher) {
			defer pubWg.Done()
			tick := time.NewTicker(10 * time.Millisecond)
			defer tick.Stop()
			var seq uint32
			for {
				select {
				case <-pubCtx.Done():
					return
				case <-tick.C:
					seq++
					_ = pub.Publish(pubCtx, pubsub.AccessUnit{
						Bytes:    []byte("au"),
						Keyframe: seq == 1 || seq%30 == 0,
						Seq:      seq,
						PTSMicro: uint64(time.Now().UnixMicro()),
					})
				}
			}
		}(name, pub)
	}
	// Single combined cleanup: cancel publishers first, then wait.
	// (Two separate defers would invoke pubWg.Wait BEFORE pubCancel
	// in LIFO order — hang.)
	defer func() {
		pubCancel()
		pubWg.Wait()
	}()

	// Start a UDP proxy at 25 ms one-way (≈ 50 ms RTT).
	backendAddr, err := net.ResolveUDPAddr("udp", srv.Addr())
	if err != nil {
		return 0, fmt.Errorf("resolve backend: %w", err)
	}
	proxyAddr, stopProxy, err := loadgen.StartUDPProxy(backendAddr, loadgen.NetCond{
		OneWayDelay: reattachOneWay,
	})
	if err != nil {
		return 0, fmt.Errorf("start proxy: %w", err)
	}

	// Dial via the proxy. The cert is the server's loopback cert; pin
	// by hash.
	subURL := fmt.Sprintf("https://127.0.0.1:%d/wt", proxyAddr.Port)
	dialCtx, dialCancel := context.WithTimeout(ctx, reattachIterBudget)
	defer dialCancel()
	cli, err := client.Dial(dialCtx, subURL, client.Options{
		Slug:           srv.Slug(),
		CertHashB64URL: srv.CertHashB64(),
		HelloTimeout:   reattachIterBudget,
	})
	if err != nil {
		stopProxy()
		return 0, fmt.Errorf("initial dial: %w", err)
	}

	// Wait until each track has delivered ≥1 AU on the initial conn.
	if err := waitFirstAUOnAll(ctx, cli, reattachTrackNames, reattachIterBudget); err != nil {
		_ = cli.Close()
		stopProxy()
		return 0, fmt.Errorf("initial recv: %w", err)
	}

	sessionID := cli.SessionID()
	if sessionID == "" {
		_ = cli.Close()
		stopProxy()
		return 0, fmt.Errorf("server did not allocate a SessionID")
	}
	lastSeen := cli.LastSeenSeq()

	// ---- KILL ----
	// Stop the proxy: every UDP datagram in flight is dropped, every
	// subsequent datagram has nowhere to land. The client's QUIC stack
	// will eventually time out, but for the purpose of measuring
	// REATTACH cost we don't care — we start the clock at kill and
	// immediately re-Dial through a fresh proxy.
	t0 := time.Now()
	stopProxy()
	// Best-effort close on the dead client. Don't wait — it may block
	// on the dead socket; we discard it.
	go func() { _ = cli.Close() }()

	// New proxy at the same NetCond.
	proxyAddr2, stopProxy2, err := loadgen.StartUDPProxy(backendAddr, loadgen.NetCond{
		OneWayDelay: reattachOneWay,
	})
	if err != nil {
		return 0, fmt.Errorf("restart proxy: %w", err)
	}
	defer stopProxy2()

	// Re-Dial with the saved SessionID for 0-RTT resume.
	resumeURL := fmt.Sprintf("https://127.0.0.1:%d/wt", proxyAddr2.Port)
	rCtx, rCancel := context.WithTimeout(ctx, reattachIterBudget)
	defer rCancel()
	cli2, err := client.Dial(rCtx, resumeURL, client.Options{
		Slug:           srv.Slug(),
		CertHashB64URL: srv.CertHashB64(),
		HelloTimeout:   reattachIterBudget,
		SessionID:      sessionID,
		LastSeenSeq:    lastSeen,
	})
	if err != nil {
		return 0, fmt.Errorf("re-dial: %w", err)
	}
	defer cli2.Close()

	// Wait for the first AU on each track post-resume; sample is
	// max(arrival time across tracks) − t0.
	maxArrival, err := waitFirstAUOnAllTimed(ctx, cli2, reattachTrackNames, reattachIterBudget)
	if err != nil {
		return 0, fmt.Errorf("resume recv: %w", err)
	}
	_ = trial
	return maxArrival.Sub(t0), nil
}

// waitFirstAUOnAll blocks until at least one AU has been delivered on
// every named track, or budget elapses.
func waitFirstAUOnAll(ctx context.Context, cli *client.Client, names []string, budget time.Duration) error {
	deadline := time.Now().Add(budget)
	var wg sync.WaitGroup
	errCh := make(chan error, len(names))
	for _, name := range names {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			rctx, cancel := context.WithDeadline(ctx, deadline)
			defer cancel()
			_, err := cli.RecvOn(rctx, name)
			if err != nil {
				errCh <- fmt.Errorf("track %s: %w", name, err)
			}
		}(name)
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		return e
	}
	return nil
}

// waitFirstAUOnAllTimed blocks until at least one AU has been
// delivered on every named track, returning the latest first-AU
// arrival time across tracks (the worst-of-N "session is back"
// observable).
func waitFirstAUOnAllTimed(ctx context.Context, cli *client.Client, names []string, budget time.Duration) (time.Time, error) {
	deadline := time.Now().Add(budget)
	var (
		mu       sync.Mutex
		latest   time.Time
		firstErr error
	)
	var wg sync.WaitGroup
	for _, name := range names {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			rctx, cancel := context.WithDeadline(ctx, deadline)
			defer cancel()
			_, err := cli.RecvOn(rctx, name)
			arrival := time.Now()
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("track %s: %w", name, err)
				}
				return
			}
			if arrival.After(latest) {
				latest = arrival
			}
		}(name)
	}
	wg.Wait()
	if firstErr != nil {
		return time.Time{}, firstErr
	}
	return latest, nil
}

// -----------------------------------------------------------------------
// Arm 2: WebRTC + SSE + HTTPS reconnect chain, all three torn down and
// re-established. Sample = time from kill → first activity on each.
// -----------------------------------------------------------------------

// reattachBaselineOnce stands up:
//   - a pion publisher/subscriber DataChannel pair (DC payload stands
//     in for an RTP stream; the ICE+DTLS recovery cost is what we're
//     measuring, and that's payload-independent)
//   - an SSE TLS server
//   - an HTTPS RPC TLS server
//
// Both HTTP servers sit behind a TCP delay-proxy with ~25 ms one-way
// delay. The pion PC routes through the existing UDP-delay loopback
// indirectly via pion's own internal ICE — to keep parity, we just
// inject artificial wall-clock delay into the SDP exchange (since
// loopback ICE is ~0 RTT but the wire would carry ~3 RTTs of state
// for a real reconnect).
func reattachBaselineOnce(t *testing.T, trial int) (time.Duration, error) {
	t.Helper()

	// ---- pion publisher PC (offerer "client") + answerer "server" ----
	// We model a real production reconnect: torn down PC + fresh
	// CreateOffer/Answer at ~50 ms RTT through artificial delay.
	// The DataChannel "hello" stands in for first RTP after ICE recovery.
	pubPC, err := webrtc.NewPeerConnection(webrtc.Configuration{ICEServers: []webrtc.ICEServer{}})
	if err != nil {
		return 0, fmt.Errorf("pion new pc: %w", err)
	}
	defer pubPC.Close()
	dc, err := pubPC.CreateDataChannel("rtp-equiv", nil)
	if err != nil {
		return 0, fmt.Errorf("pion create dc: %w", err)
	}
	dcMsgs := make(chan struct{}, 8)
	dc.OnMessage(func(_ webrtc.DataChannelMessage) {
		select {
		case dcMsgs <- struct{}{}:
		default:
		}
	})

	subPC, err := webrtc.NewPeerConnection(webrtc.Configuration{ICEServers: []webrtc.ICEServer{}})
	if err != nil {
		return 0, fmt.Errorf("pion new sub pc: %w", err)
	}
	defer subPC.Close()
	subPC.OnDataChannel(func(d *webrtc.DataChannel) {
		d.OnOpen(func() { _ = d.SendText("hello") })
	})

	if err := pionExchange(pubPC, subPC, reattachOneWay); err != nil {
		return 0, fmt.Errorf("pion initial exchange: %w", err)
	}
	if err := waitChanOnce(dcMsgs, reattachIterBudget); err != nil {
		return 0, fmt.Errorf("pion initial dc: %w", err)
	}

	// ---- SSE TLS server ----
	sseStop := make(chan struct{})
	sseSrv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "no flusher", 500)
			return
		}
		ctx := r.Context()
		// Emit one event/sec.
		_, _ = w.Write([]byte("event: ready\ndata: 1\n\n"))
		flusher.Flush()
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		var i int
		for {
			select {
			case <-ctx.Done():
				return
			case <-sseStop:
				return
			case <-tick.C:
				i++
				_, err := fmt.Fprintf(w, "data: %d\n\n", i)
				if err != nil {
					return
				}
				flusher.Flush()
			}
		}
	}))
	defer sseSrv.Close()
	defer close(sseStop)

	// ---- HTTPS RPC TLS server ----
	httpsSrv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer httpsSrv.Close()

	// ---- TCP delay-proxies in front of both TLS servers ----
	sseProxyAddr, stopSSEProxy, err := startTCPDelayProxy(sseSrv.Listener.Addr().String(), reattachOneWay)
	if err != nil {
		return 0, fmt.Errorf("start sse proxy: %w", err)
	}
	httpsProxyAddr, stopHTTPSProxy, err := startTCPDelayProxy(httpsSrv.Listener.Addr().String(), reattachOneWay)
	if err != nil {
		stopSSEProxy()
		return 0, fmt.Errorf("start https proxy: %w", err)
	}

	// HTTPS client trusting the httptest CA.
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test
		},
	}

	// Open the initial SSE stream + initial HTTPS RPC + verify the
	// initial pion DC. (pion already verified above.)
	sseURL := fmt.Sprintf("https://%s/", sseProxyAddr)
	httpsURL := fmt.Sprintf("https://%s/rpc", httpsProxyAddr)

	if err := pingHTTPS(httpClient, httpsURL); err != nil {
		stopSSEProxy()
		stopHTTPSProxy()
		return 0, fmt.Errorf("initial https: %w", err)
	}
	sseEvents, sseCloser, err := openSSE(httpClient, sseURL)
	if err != nil {
		stopSSEProxy()
		stopHTTPSProxy()
		return 0, fmt.Errorf("initial sse: %w", err)
	}
	if err := waitChanOnce(sseEvents, reattachIterBudget); err != nil {
		_ = sseCloser.Close()
		stopSSEProxy()
		stopHTTPSProxy()
		return 0, fmt.Errorf("initial sse event: %w", err)
	}

	// ---- KILL ----
	// pion: close the existing PCs entirely (full teardown is the
	// fair worst-case; ICE restart on the same PC is the "best case"
	// already covered by TestResumeComparison). Build fresh PCs after
	// the kill, just like a connection that died and came back.
	// SSE: drop the proxy + close the response body.
	// HTTPS: drop the proxy.
	t0 := time.Now()
	_ = sseCloser.Close()
	stopSSEProxy()
	stopHTTPSProxy()
	_ = pubPC.Close()
	_ = subPC.Close()

	// Restart everything in parallel — that's what a real client
	// would do (three independent reconnects).
	type res struct {
		name string
		when time.Time
		err  error
	}
	results := make(chan res, 3)

	// pion arm: fresh PCs + fresh ICE + fresh DTLS + DC open.
	go func() {
		newPub, err := webrtc.NewPeerConnection(webrtc.Configuration{ICEServers: []webrtc.ICEServer{}})
		if err != nil {
			results <- res{"pion", time.Time{}, err}
			return
		}
		newSub, err := webrtc.NewPeerConnection(webrtc.Configuration{ICEServers: []webrtc.ICEServer{}})
		if err != nil {
			_ = newPub.Close()
			results <- res{"pion", time.Time{}, err}
			return
		}
		dcCh := make(chan time.Time, 1)
		newSub.OnDataChannel(func(d *webrtc.DataChannel) {
			d.OnOpen(func() { _ = d.SendText("hello") })
		})
		ndc, err := newPub.CreateDataChannel("rtp-equiv-2", nil)
		if err != nil {
			_ = newPub.Close()
			_ = newSub.Close()
			results <- res{"pion", time.Time{}, err}
			return
		}
		ndc.OnMessage(func(_ webrtc.DataChannelMessage) {
			select {
			case dcCh <- time.Now():
			default:
			}
		})
		if err := pionExchange(newPub, newSub, reattachOneWay); err != nil {
			_ = newPub.Close()
			_ = newSub.Close()
			results <- res{"pion", time.Time{}, err}
			return
		}
		select {
		case when := <-dcCh:
			results <- res{"pion", when, nil}
		case <-time.After(reattachIterBudget):
			results <- res{"pion", time.Time{}, fmt.Errorf("pion: dc open timeout")}
		}
		// Don't close the new PCs until the trial ends. They'll be
		// GC'd; this is fine in a bench harness.
		_ = newPub
		_ = newSub
	}()

	// SSE arm: fresh proxy, fresh TLS, fresh subscribe.
	go func() {
		sseProxyAddr2, stopSSEProxy2, err := startTCPDelayProxy(sseSrv.Listener.Addr().String(), reattachOneWay)
		if err != nil {
			results <- res{"sse", time.Time{}, err}
			return
		}
		defer stopSSEProxy2()
		sseURL2 := fmt.Sprintf("https://%s/", sseProxyAddr2)
		events, closer, err := openSSE(httpClient, sseURL2)
		if err != nil {
			results <- res{"sse", time.Time{}, err}
			return
		}
		defer closer.Close()
		if err := waitChanOnce(events, reattachIterBudget); err != nil {
			results <- res{"sse", time.Time{}, err}
			return
		}
		results <- res{"sse", time.Now(), nil}
	}()

	// HTTPS arm: fresh proxy, fresh TLS, fresh RPC.
	go func() {
		httpsProxyAddr2, stopHTTPSProxy2, err := startTCPDelayProxy(httpsSrv.Listener.Addr().String(), reattachOneWay)
		if err != nil {
			results <- res{"https", time.Time{}, err}
			return
		}
		defer stopHTTPSProxy2()
		// New client so we don't reuse the prior connection pool —
		// that's the honest cost of "the HTTPS connection went away."
		freshClient := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test
			},
		}
		httpsURL2 := fmt.Sprintf("https://%s/rpc", httpsProxyAddr2)
		if err := pingHTTPS(freshClient, httpsURL2); err != nil {
			results <- res{"https", time.Time{}, err}
			return
		}
		results <- res{"https", time.Now(), nil}
	}()

	var latest time.Time
	var firstErr error
	for i := 0; i < 3; i++ {
		r := <-results
		if r.err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("%s: %w", r.name, r.err)
			}
			continue
		}
		if r.when.After(latest) {
			latest = r.when
		}
	}
	_ = trial
	if firstErr != nil {
		return 0, firstErr
	}
	return latest.Sub(t0), nil
}

// pionExchange runs offer/answer between pub (offerer) and sub
// (answerer). To approximate ~50 ms RTT on loopback, each SDP
// hand-off sleeps oneWay (so total round-trip incurs 2× oneWay
// across the offer→answer exchange).
func pionExchange(pub, sub *webrtc.PeerConnection, oneWay time.Duration) error {
	offer, err := pub.CreateOffer(nil)
	if err != nil {
		return err
	}
	if err := pub.SetLocalDescription(offer); err != nil {
		return err
	}
	<-webrtc.GatheringCompletePromise(pub)

	// Simulate offer travel.
	time.Sleep(oneWay)

	if err := sub.SetRemoteDescription(*pub.LocalDescription()); err != nil {
		return err
	}
	ans, err := sub.CreateAnswer(nil)
	if err != nil {
		return err
	}
	if err := sub.SetLocalDescription(ans); err != nil {
		return err
	}
	<-webrtc.GatheringCompletePromise(sub)

	// Simulate answer travel.
	time.Sleep(oneWay)

	if err := pub.SetRemoteDescription(*sub.LocalDescription()); err != nil {
		return err
	}
	return nil
}

// waitChanOnce blocks until ch fires once or budget elapses.
func waitChanOnce[T any](ch <-chan T, budget time.Duration) error {
	select {
	case <-ch:
		return nil
	case <-time.After(budget):
		return fmt.Errorf("timeout after %s", budget)
	}
}

// pingHTTPS issues one GET against url and reads the response body.
func pingHTTPS(c *http.Client, url string) error {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// openSSE GETs an SSE endpoint and returns a channel that fires once
// per received "data: ..." line, plus the response body for closing.
func openSSE(c *http.Client, url string) (<-chan struct{}, io.Closer, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := c.Do(req)
	if err != nil {
		return nil, nil, err
	}
	events := make(chan struct{}, 16)
	go func() {
		defer resp.Body.Close()
		buf := make([]byte, 4096)
		for {
			n, err := resp.Body.Read(buf)
			if n > 0 && bytes.Contains(buf[:n], []byte("data:")) {
				select {
				case events <- struct{}{}:
				default:
				}
			}
			if err != nil {
				return
			}
		}
	}()
	return events, resp.Body, nil
}

// startTCPDelayProxy listens on 127.0.0.1:0 and forwards TCP
// connections to backend with `oneWay` of artificial delay added
// to bytes flowing in each direction. Models WAN-style latency for
// HTTPS / SSE on loopback. Returns the proxy's listen address
// (host:port) and a stop function.
//
// The delay is applied per byte-batch, not per byte — sufficient
// for measuring handshake-round-trip-cost (the dominant effect for
// this benchmark) but not for per-byte queueing modeling.
func startTCPDelayProxy(backend string, oneWay time.Duration) (string, func(), error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	var stopped atomic.Bool
	var wg sync.WaitGroup

	var connMu sync.Mutex
	conns := make(map[net.Conn]struct{})
	trackConn := func(c net.Conn) {
		connMu.Lock()
		conns[c] = struct{}{}
		connMu.Unlock()
	}
	untrackConn := func(c net.Conn) {
		connMu.Lock()
		delete(conns, c)
		connMu.Unlock()
	}
	closeAllConns := func() {
		connMu.Lock()
		defer connMu.Unlock()
		for c := range conns {
			_ = c.Close()
		}
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func(client net.Conn) {
				defer wg.Done()
				defer client.Close()
				up, err := net.Dial("tcp", backend)
				if err != nil {
					return
				}
				defer up.Close()
				trackConn(client)
				trackConn(up)
				defer untrackConn(client)
				defer untrackConn(up)

				done := make(chan struct{}, 2)
				pipe := func(dst net.Conn, src net.Conn) {
					defer func() { done <- struct{}{} }()
					buf := make([]byte, 32*1024)
					for {
						n, rErr := src.Read(buf)
						if n > 0 {
							if oneWay > 0 {
								time.Sleep(oneWay)
							}
							if stopped.Load() {
								return
							}
							if _, wErr := dst.Write(buf[:n]); wErr != nil {
								return
							}
						}
						if rErr != nil {
							return
						}
					}
				}
				go pipe(up, client)
				go pipe(client, up)
				<-done
			}(c)
		}
	}()
	stop := func() {
		stopped.Store(true)
		_ = ln.Close()
		// Force-close all active connections so the pipe goroutines'
		// Read() calls return with an error rather than blocking on
		// half-open sockets after the "kill" event. Without this,
		// wg.Wait would hang waiting for goroutines that are reading
		// from sockets nobody is writing to anymore.
		closeAllConns()
		wg.Wait()
	}
	return ln.Addr().String(), stop, nil
}

// durationsToMs converts a []time.Duration to []float64 milliseconds
// for the resume_bench summarizeResume helper.
func durationsToMs(ds []time.Duration) []float64 {
	out := make([]float64, 0, len(ds))
	for _, d := range ds {
		out = append(out, float64(d.Microseconds())/1000.0)
	}
	return out
}

func msFloat(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }

// Ensure imports stay live even if a refactor temporarily drops a use.
var _ = json.Marshal
