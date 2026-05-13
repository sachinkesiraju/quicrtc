// Resume benchmark: three-way comparison of session-resumption latency.
//
// Measures three series per iteration:
//
//	quicrtc / resume
//	  quicrtc's resume protocol: client closes the WebTransport session,
//	  reconnects with the same SessionID, measures time-to-first-message
//	  on the DataChannel.
//
//	webrtc / full_reconnect
//	  Strawman baseline: tear down the RTCPeerConnection completely and
//	  stand up a brand-new PC. Fresh ICE gathering, fresh DTLS handshake,
//	  fresh SCTP association. This is "we have a feature, you don't"
//	  and is reported here as a reference, NOT as the comparison.
//
//	webrtc / ice_restart
//	  Production-recommended baseline: same RTCPeerConnection on both
//	  sides, call CreateOffer with ICERestart=true, exchange a fresh
//	  offer/answer, keep DTLS+SCTP. This is what production WebRTC code
//	  actually does on transient network changes (Wi-Fi→cellular, NAT
//	  rebinding). This is the fair comparison.
//
// All timings are client-side (single clock; no skew). The "ready"
// signal is uniformly defined as "first DataChannel message received
// after the (re)dial" — same observable across all three paths.
package resume

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/sachinkesiraju/quicrtc/cert"
	"github.com/sachinkesiraju/quicrtc/client"
	"github.com/sachinkesiraju/quicrtc/datachannel"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/wire"
)

const (
	resumeN           = 30
	resumeWarmup      = 3
	resumeBootstrap   = 1000
	resumeSeed        = int64(1)
	resumeIterTimeout = 5 * time.Second
)

type resumeSample struct {
	protocol string
	phase    string
	ms       float64
	err      error
}

// TestResumeComparison is the headline three-way comparison.
func TestResumeComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping resume comparison in short mode")
	}

	bundle, err := cert.Generate(cert.Options{IPs: []net.IP{net.ParseIP("127.0.0.1")}})
	if err != nil {
		t.Fatal(err)
	}
	qsrv, err := server.New(server.Config{
		Addr:         "127.0.0.1:0",
		ExternalHost: "127.0.0.1",
		CertBundle:   bundle,
		MaxSessions:  10000,
		SDP:          wire.SDP{Codec: "noop", Width: 64, Height: 64, FPS: 30},
		OnDataChannel: func(dc *datachannel.Channel) {
			_ = dc.Send([]byte("hello"))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = qsrv.ListenAndServe(ctx) }()
	for qsrv.Addr() == "127.0.0.1:0" {
		time.Sleep(10 * time.Millisecond)
	}
	defer qsrv.Shutdown(context.Background())

	pionMux := http.NewServeMux()

	pionMux.HandleFunc("/sig", func(w http.ResponseWriter, r *http.Request) {
		var offer webrtc.SessionDescription
		if err := json.NewDecoder(r.Body).Decode(&offer); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		pc, err := webrtc.NewPeerConnection(webrtc.Configuration{ICEServers: []webrtc.ICEServer{}})
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		pc.OnDataChannel(func(dc *webrtc.DataChannel) {
			dc.OnOpen(func() { _ = dc.SendText("hello") })
		})
		if err := pc.SetRemoteDescription(offer); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		gather := webrtc.GatheringCompletePromise(pc)
		ans, err := pc.CreateAnswer(nil)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		_ = pc.SetLocalDescription(ans)
		<-gather
		_ = json.NewEncoder(w).Encode(*pc.LocalDescription())
	})

	type pcEntry struct {
		pc *webrtc.PeerConnection
		dc *webrtc.DataChannel
	}
	var sessMu sync.Mutex
	sessions := map[string]*pcEntry{}

	pionMux.HandleFunc("/sig-restart", func(w http.ResponseWriter, r *http.Request) {
		sessID := r.URL.Query().Get("sid")
		if sessID == "" {
			http.Error(w, "missing sid", 400)
			return
		}
		var offer webrtc.SessionDescription
		if err := json.NewDecoder(r.Body).Decode(&offer); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		sessMu.Lock()
		entry, exists := sessions[sessID]
		sessMu.Unlock()

		if !exists {
			pc, err := webrtc.NewPeerConnection(webrtc.Configuration{ICEServers: []webrtc.ICEServer{}})
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			entry = &pcEntry{pc: pc}
			sessMu.Lock()
			sessions[sessID] = entry
			sessMu.Unlock()

			pc.OnDataChannel(func(dc *webrtc.DataChannel) {
				sessMu.Lock()
				entry.dc = dc
				sessMu.Unlock()
				dc.OnOpen(func() { _ = dc.SendText("hello") })
			})
			var connectCount int
			var ccMu sync.Mutex
			pc.OnICEConnectionStateChange(func(s webrtc.ICEConnectionState) {
				if s != webrtc.ICEConnectionStateConnected {
					return
				}
				ccMu.Lock()
				connectCount++
				n := connectCount
				ccMu.Unlock()
				if n < 2 {
					return
				}
				sessMu.Lock()
				dc := entry.dc
				sessMu.Unlock()
				if dc != nil && dc.ReadyState() == webrtc.DataChannelStateOpen {
					_ = dc.SendText("hello-restart")
				}
			})
		}
		pc := entry.pc
		if err := pc.SetRemoteDescription(offer); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		gather := webrtc.GatheringCompletePromise(pc)
		ans, err := pc.CreateAnswer(nil)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		_ = pc.SetLocalDescription(ans)
		<-gather
		_ = json.NewEncoder(w).Encode(*pc.LocalDescription())
	})

	pionMux.HandleFunc("/sig-restart-close", func(w http.ResponseWriter, r *http.Request) {
		sessID := r.URL.Query().Get("sid")
		sessMu.Lock()
		entry, ok := sessions[sessID]
		delete(sessions, sessID)
		sessMu.Unlock()
		if ok && entry != nil && entry.pc != nil {
			_ = entry.pc.Close()
		}
		w.WriteHeader(http.StatusNoContent)
	})

	pionLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pionLn.Close()
	pionSrv := &http.Server{Handler: pionMux}
	go func() { _ = pionSrv.Serve(pionLn) }()
	defer pionSrv.Shutdown(context.Background())
	pionAddr := pionLn.Addr().String()

	t.Logf("quicrtc=%s  pion=%s  n=%d  warmup=%d", qsrv.Addr(), pionAddr, resumeN, resumeWarmup)

	var samples []resumeSample
	var mu sync.Mutex

	{
		shareLink := qsrv.ShareLink()
		t.Logf("--- quicrtc / resume (n=%d, warmup=%d) ---", resumeN, resumeWarmup)
		total := resumeN + resumeWarmup
		for i := 0; i < total; i++ {
			isWarm := i < resumeWarmup

			t0 := time.Now()
			cli, err := client.Dial(ctx, shareLink, client.Options{HelloTimeout: resumeIterTimeout})
			if err != nil {
				if !isWarm {
					mu.Lock()
					samples = append(samples, resumeSample{"quicrtc", "initial_setup", 0, err})
					mu.Unlock()
				}
				continue
			}
			recvCtx, recvCancel := context.WithTimeout(ctx, resumeIterTimeout)
			_, err = cli.DataChannel().Recv(recvCtx)
			recvCancel()
			if err != nil {
				_ = cli.Close()
				continue
			}
			initial := time.Since(t0)
			sessionID := cli.SessionID()
			_ = cli.Close()

			t1 := time.Now()
			cli2, err := client.Dial(ctx, shareLink, client.Options{
				HelloTimeout: resumeIterTimeout,
				SessionID:    sessionID,
			})
			if err != nil {
				if !isWarm {
					mu.Lock()
					samples = append(samples, resumeSample{"quicrtc", "resume", 0, err})
					mu.Unlock()
				}
				continue
			}
			recvCtx2, recvCancel2 := context.WithTimeout(ctx, resumeIterTimeout)
			_, err = cli2.DataChannel().Recv(recvCtx2)
			recvCancel2()
			if err != nil {
				_ = cli2.Close()
				continue
			}
			resume := time.Since(t1)
			_ = cli2.Close()

			if !isWarm {
				mu.Lock()
				samples = append(samples, resumeSample{"quicrtc", "initial_setup", toMs(initial), nil})
				samples = append(samples, resumeSample{"quicrtc", "resume", toMs(resume), nil})
				mu.Unlock()
			}
		}
	}

	{
		t.Logf("--- webrtc / full_reconnect (n=%d, warmup=%d) ---", resumeN, resumeWarmup)
		total := resumeN + resumeWarmup
		for i := 0; i < total; i++ {
			isWarm := i < resumeWarmup

			initial, err := webrtcSetupOnce(ctx, "http://"+pionAddr+"/sig")
			if err != nil {
				if !isWarm {
					mu.Lock()
					samples = append(samples, resumeSample{"webrtc", "initial_setup", 0, err})
					mu.Unlock()
				}
				continue
			}
			reconn, err := webrtcSetupOnce(ctx, "http://"+pionAddr+"/sig")
			if err != nil {
				if !isWarm {
					mu.Lock()
					samples = append(samples, resumeSample{"webrtc", "full_reconnect", 0, err})
					mu.Unlock()
				}
				continue
			}
			if !isWarm {
				mu.Lock()
				samples = append(samples, resumeSample{"webrtc", "initial_setup", initial, nil})
				samples = append(samples, resumeSample{"webrtc", "full_reconnect", reconn, nil})
				mu.Unlock()
			}
		}
	}

	{
		t.Logf("--- webrtc / ice_restart (n=%d, warmup=%d) ---", resumeN, resumeWarmup)
		total := resumeN + resumeWarmup
		for i := 0; i < total; i++ {
			isWarm := i < resumeWarmup
			sessID := fmt.Sprintf("ir-%d-%d", time.Now().UnixNano(), i)
			_, restart, err := webrtcIceRestartOnce(ctx, "http://"+pionAddr, sessID)
			if err != nil {
				if !isWarm {
					mu.Lock()
					samples = append(samples, resumeSample{"webrtc", "ice_restart", 0, err})
					mu.Unlock()
				}
				continue
			}
			if !isWarm {
				mu.Lock()
				samples = append(samples, resumeSample{"webrtc", "ice_restart", restart, nil})
				mu.Unlock()
			}
		}
	}

	type rowKey struct{ proto, phase string }
	keys := []rowKey{
		{"quicrtc", "initial_setup"},
		{"quicrtc", "resume"},
		{"webrtc", "initial_setup"},
		{"webrtc", "full_reconnect"},
		{"webrtc", "ice_restart"},
	}

	rng := rand.New(rand.NewSource(resumeSeed))
	t.Logf("===== resume benchmark summary =====")
	t.Logf("%-8s %-15s %4s %7s %7s %7s %7s %7s %19s",
		"proto", "phase", "n", "p50", "p95", "p99", "mean", "stddev", "mean 95% CI")
	for _, k := range keys {
		xs := filterResumeByKey(samples, k.proto, k.phase)
		st := summarizeResume(xs, resumeBootstrap, rng)
		t.Logf("%-8s %-15s %4d %7.2f %7.2f %7.2f %7.2f %7.2f [%6.2f,%7.2f]",
			k.proto, k.phase, st.n, st.p50, st.p95, st.p99, st.mean, st.stddev, st.meanCILo, st.meanCIHi)
	}

	qResume := meanResume(filterResumeByKey(samples, "quicrtc", "resume"))
	wFull := meanResume(filterResumeByKey(samples, "webrtc", "full_reconnect"))
	wRestart := meanResume(filterResumeByKey(samples, "webrtc", "ice_restart"))

	t.Logf("===== headline ratios =====")
	if qResume > 0 && wFull > 0 {
		t.Logf("  quicrtc resume vs webrtc full_reconnect: %.2fms vs %.2fms (%.1fx faster) [STRAWMAN BASELINE]",
			qResume, wFull, wFull/qResume)
	}
	if qResume > 0 && wRestart > 0 {
		t.Logf("  quicrtc resume vs webrtc ice_restart:    %.2fms vs %.2fms (%.2fx)        [FAIR BASELINE]",
			qResume, wRestart, wRestart/qResume)
	}
}

func webrtcSetupOnce(ctx context.Context, url string) (float64, error) {
	t0 := time.Now()
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{ICEServers: []webrtc.ICEServer{}})
	if err != nil {
		return 0, err
	}
	defer pc.Close()
	dc, err := pc.CreateDataChannel("storm", nil)
	if err != nil {
		return 0, err
	}
	ready := make(chan struct{}, 1)
	dc.OnMessage(func(m webrtc.DataChannelMessage) {
		select {
		case ready <- struct{}{}:
		default:
		}
	})
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return 0, err
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		return 0, err
	}
	<-webrtc.GatheringCompletePromise(pc)

	body, err := json.Marshal(*pc.LocalDescription())
	if err != nil {
		return 0, err
	}
	resp, err := httpPost(ctx, url, body)
	if err != nil {
		return 0, err
	}
	var ans webrtc.SessionDescription
	if err := json.Unmarshal(resp, &ans); err != nil {
		return 0, err
	}
	if err := pc.SetRemoteDescription(ans); err != nil {
		return 0, err
	}
	select {
	case <-ready:
		return toMs(time.Since(t0)), nil
	case <-time.After(resumeIterTimeout):
		return 0, fmt.Errorf("webrtc setup timeout")
	}
}

func webrtcIceRestartOnce(ctx context.Context, baseURL, sessID string) (float64, float64, error) {
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{ICEServers: []webrtc.ICEServer{}})
	if err != nil {
		return 0, 0, err
	}
	defer func() {
		_ = pc.Close()
		_, _ = httpPost(ctx, baseURL+"/sig-restart-close?sid="+sessID, nil)
	}()
	dc, err := pc.CreateDataChannel("storm", nil)
	if err != nil {
		return 0, 0, err
	}

	msgs := make(chan struct{}, 16)
	dc.OnMessage(func(m webrtc.DataChannelMessage) {
		select {
		case msgs <- struct{}{}:
		default:
		}
	})

	t0 := time.Now()
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return 0, 0, err
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		return 0, 0, err
	}
	<-webrtc.GatheringCompletePromise(pc)
	body, _ := json.Marshal(*pc.LocalDescription())
	resp, err := httpPost(ctx, baseURL+"/sig-restart?sid="+sessID, body)
	if err != nil {
		return 0, 0, err
	}
	var ans webrtc.SessionDescription
	if err := json.Unmarshal(resp, &ans); err != nil {
		return 0, 0, err
	}
	if err := pc.SetRemoteDescription(ans); err != nil {
		return 0, 0, err
	}
	select {
	case <-msgs:
	case <-time.After(resumeIterTimeout):
		return 0, 0, fmt.Errorf("ice_restart: initial setup timeout")
	}
	initialMs := toMs(time.Since(t0))

	t1 := time.Now()
	rOffer, err := pc.CreateOffer(&webrtc.OfferOptions{ICERestart: true})
	if err != nil {
		return 0, 0, err
	}
	if err := pc.SetLocalDescription(rOffer); err != nil {
		return 0, 0, err
	}
	<-webrtc.GatheringCompletePromise(pc)
	rBody, _ := json.Marshal(*pc.LocalDescription())
	rResp, err := httpPost(ctx, baseURL+"/sig-restart?sid="+sessID, rBody)
	if err != nil {
		return 0, 0, err
	}
	var rAns webrtc.SessionDescription
	if err := json.Unmarshal(rResp, &rAns); err != nil {
		return 0, 0, err
	}
	if err := pc.SetRemoteDescription(rAns); err != nil {
		return 0, 0, err
	}
	select {
	case <-msgs:
	case <-time.After(resumeIterTimeout):
		return 0, 0, fmt.Errorf("ice_restart: post-restart timeout")
	}
	if dc.ReadyState() != webrtc.DataChannelStateOpen {
		return 0, 0, fmt.Errorf("ice_restart: dc not open after restart (state=%s)", dc.ReadyState())
	}
	restartMs := toMs(time.Since(t1))
	return initialMs, restartMs, nil
}

func httpPost(ctx context.Context, url string, body []byte) ([]byte, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", url, rdr)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func toMs(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }

type resumeStats struct {
	n                       int
	p25, p50, p75, p95, p99 float64
	mean, stddev, min, max  float64
	meanCILo, meanCIHi      float64
}

func summarizeResume(xs []float64, bootN int, rng *rand.Rand) resumeStats {
	if len(xs) == 0 {
		return resumeStats{}
	}
	sorted := make([]float64, len(xs))
	copy(sorted, xs)
	sort.Float64s(sorted)
	pct := func(p float64) float64 { return sorted[int(float64(len(sorted)-1)*p)] }

	var sum float64
	for _, v := range sorted {
		sum += v
	}
	m := sum / float64(len(sorted))
	var sq float64
	for _, v := range sorted {
		sq += (v - m) * (v - m)
	}
	sd := 0.0
	if len(sorted) > 1 {
		sd = math.Sqrt(sq / float64(len(sorted)-1))
	}

	var meanCILo, meanCIHi float64
	if bootN > 0 && len(sorted) > 1 {
		bootMeans := make([]float64, bootN)
		for b := 0; b < bootN; b++ {
			var s float64
			for i := 0; i < len(xs); i++ {
				s += xs[rng.Intn(len(xs))]
			}
			bootMeans[b] = s / float64(len(xs))
		}
		sort.Float64s(bootMeans)
		lo := int(0.025 * float64(bootN))
		hi := int(0.975 * float64(bootN))
		if hi >= bootN {
			hi = bootN - 1
		}
		meanCILo, meanCIHi = bootMeans[lo], bootMeans[hi]
	}

	return resumeStats{
		n: len(sorted), p25: pct(0.25), p50: pct(0.5), p75: pct(0.75),
		p95: pct(0.95), p99: pct(0.99),
		mean: m, stddev: sd, min: sorted[0], max: sorted[len(sorted)-1],
		meanCILo: meanCILo, meanCIHi: meanCIHi,
	}
}

func filterResumeByKey(samples []resumeSample, proto, phase string) []float64 {
	var xs []float64
	for _, s := range samples {
		if s.protocol == proto && s.phase == phase && s.err == nil {
			xs = append(xs, s.ms)
		}
	}
	return xs
}

func meanResume(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	var s float64
	for _, v := range xs {
		s += v
	}
	return s / float64(len(xs))
}
