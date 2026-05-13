package main

// WAN client: queries server's /info endpoint, runs the requested
// test (setup | multi | resume | scale), writes a CSV of measurements.
//
// Run on the "client" cloud VM:
//   ./wan_bench -mode=client -server=<server-public-ip> -test=setup -n=20

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"

	qclient "github.com/sachinkesiraju/quicrtc/client"
)

type serverInfo struct {
	ExternalHost            string `json:"externalHost"`
	QuicrtcShareLink        string `json:"quicrtcShareLink"`
	QuicrtcCertHashB64      string `json:"quicrtcCertHashB64"`
	QuicrtcSlug             string `json:"quicrtcSlug"`
	WebtransportURL         string `json:"webtransportURL"`
	WebtransportCertHashB64 string `json:"webtransportCertHashB64"`
	WebrtcSignaling         string `json:"webrtcSignaling"`
}

func fetchInfo(serverHost string) (*serverInfo, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s:8090/info", serverHost))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var info serverInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, err
	}
	return &info, nil
}

type sample struct {
	Protocol string
	Phase    string
	Index    int
	Ms       float64
	Err      string
}

func writeCSV(path string, ss []sample) {
	f, err := os.Create(path)
	if err != nil {
		fmt.Println("csv:", err)
		return
	}
	defer f.Close()
	fmt.Fprintln(f, "protocol,phase,index,ms,err")
	for _, s := range ss {
		fmt.Fprintf(f, "%s,%s,%d,%.3f,%q\n", s.Protocol, s.Phase, s.Index, s.Ms, s.Err)
	}
	// Also write a sibling summary CSV with per-protocol-phase stats.
	writeSummaryCSV(path, ss)
}

func writeSummaryCSV(perSamplePath string, ss []sample) {
	by := map[string]map[string][]float64{}
	for _, s := range ss {
		if s.Err != "" {
			continue
		}
		if by[s.Protocol] == nil {
			by[s.Protocol] = map[string][]float64{}
		}
		by[s.Protocol][s.Phase] = append(by[s.Protocol][s.Phase], s.Ms)
	}
	out := perSamplePath
	if i := len(out) - 4; i > 0 && out[i:] == ".csv" {
		out = out[:i] + "_summary.csv"
	} else {
		out = out + "_summary.csv"
	}
	f, err := os.Create(out)
	if err != nil {
		fmt.Println("summary csv:", err)
		return
	}
	defer f.Close()
	fmt.Fprintln(f, SummaryCSVHeader())
	protos := make([]string, 0, len(by))
	for p := range by {
		protos = append(protos, p)
	}
	sort.Strings(protos)
	for _, proto := range protos {
		phases := make([]string, 0, len(by[proto]))
		for ph := range by[proto] {
			phases = append(phases, ph)
		}
		sort.Strings(phases)
		for _, phase := range phases {
			s := computeFullStats(by[proto][phase])
			fmt.Fprintln(f, SummaryCSVRow(proto, phase, s))
		}
	}
}

func summarize(ss []sample) {
	by := map[string][]float64{}
	for _, s := range ss {
		if s.Err == "" {
			by[s.Protocol+":"+s.Phase] = append(by[s.Protocol+":"+s.Phase], s.Ms)
		}
	}
	keys := make([]string, 0, len(by))
	for k := range by {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Println("\n===== summary =====")
	for _, k := range keys {
		s := computeFullStats(by[k])
		fmt.Println(FormatSummaryLine(k, s, "ms"))
	}
}

// ----- Test 1: setup latency -----
func runSetupTest(info *serverInfo, n int) []sample {
	var samples []sample
	ctx := context.Background()

	fmt.Printf("\n--- setup latency (n=%d) ---\n", n)

	for i := 0; i < n; i++ {
		// quicrtc setup
		if ms, err := quicrtcSetup(ctx, info); err == nil {
			samples = append(samples, sample{"quicrtc", "media_ready", i, ms, ""})
			fmt.Printf("  quicrtc %3d: %7.2f ms\n", i, ms)
		} else {
			samples = append(samples, sample{"quicrtc", "media_ready", i, 0, err.Error()})
			fmt.Printf("  quicrtc %3d: ERR %v\n", i, err)
		}
		// webrtc setup
		if ms, err := webrtcSetup(ctx, info.WebrtcSignaling); err == nil {
			samples = append(samples, sample{"webrtc", "media_ready", i, ms, ""})
			fmt.Printf("  webrtc  %3d: %7.2f ms\n", i, ms)
		} else {
			samples = append(samples, sample{"webrtc", "media_ready", i, 0, err.Error()})
			fmt.Printf("  webrtc  %3d: ERR %v\n", i, err)
		}
	}
	return samples
}

func quicrtcSetup(ctx context.Context, info *serverInfo) (float64, error) {
	t0 := time.Now()
	c, err := qclient.Dial(ctx, info.QuicrtcShareLink, qclient.Options{
		HelloTimeout:      10 * time.Second,
		DataInboundBuffer: 8192,
	})
	if err != nil {
		return 0, err
	}
	defer c.Close()
	recvCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	msg, err := c.DataChannel().Recv(recvCtx)
	if err != nil {
		return 0, err
	}
	if string(msg) != "hello" {
		return 0, fmt.Errorf("unexpected: %q", string(msg))
	}
	return float64(time.Since(t0).Microseconds()) / 1000.0, nil
}

func webrtcSetup(ctx context.Context, signalingURL string) (float64, error) {
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
	gather := webrtc.GatheringCompletePromise(pc)
	<-gather
	body, _ := json.Marshal(*pc.LocalDescription())
	resp, err := http.Post(signalingURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	var ans webrtc.SessionDescription
	if err := json.Unmarshal(respBody, &ans); err != nil {
		return 0, err
	}
	if err := pc.SetRemoteDescription(ans); err != nil {
		return 0, err
	}
	select {
	case <-ready:
		return float64(time.Since(t0).Microseconds()) / 1000.0, nil
	case <-time.After(15 * time.Second):
		return 0, fmt.Errorf("setup timeout")
	}
}

// ----- Test 2: multi-stream isolation -----
func runMultiTest(info *serverInfo, runs int) []sample {
	var samples []sample
	ctx := context.Background()
	fmt.Printf("\n--- multi-stream (runs=%d) ---\n", runs)

	for r := 0; r < runs; r++ {
		// quicrtc: tokens via DC, burst via primary track
		if tok, err := quicrtcMulti(ctx, info); err == nil {
			for i, ms := range tok {
				samples = append(samples, sample{"quicrtc", "token", i, ms, ""})
			}
			sort.Float64s(tok)
			fmt.Printf("  quicrtc run %d: tokens n=%d p99=%.2f ms\n", r, len(tok), pctOf(tok, 0.99))
		} else {
			fmt.Printf("  quicrtc run %d: ERR %v\n", r, err)
		}
		// webrtc: 2 DCs (server pumps via burst+tokens)
		if tok, err := webrtcMulti(ctx, info); err == nil {
			for i, ms := range tok {
				samples = append(samples, sample{"webrtc", "token", i, ms, ""})
			}
			sort.Float64s(tok)
			fmt.Printf("  webrtc  run %d: tokens n=%d p99=%.2f ms\n", r, len(tok), pctOf(tok, 0.99))
		} else {
			fmt.Printf("  webrtc  run %d: ERR %v\n", r, err)
		}
	}
	return samples
}

func quicrtcMulti(ctx context.Context, info *serverInfo) ([]float64, error) {
	c, err := qclient.Dial(ctx, info.QuicrtcShareLink, qclient.Options{
		HelloTimeout:      10 * time.Second,
		DataInboundBuffer: 8192,
	})
	if err != nil {
		return nil, err
	}
	defer c.Close()
	tokens := []float64{}
	deadline := time.Now().Add(10 * time.Second)
	// First message is "hello", then 400 timestamped tokens.
	for time.Now().Before(deadline) && len(tokens) < 400 {
		recvCtx, cancel := context.WithTimeout(ctx, time.Until(deadline))
		msg, err := c.DataChannel().Recv(recvCtx)
		cancel()
		if err != nil {
			break
		}
		if len(msg) >= 8 {
			sentNs := decodeNs(msg[:8])
			if sentNs > 0 {
				tokens = append(tokens, float64(time.Now().UnixNano()-sentNs)/1e6)
			}
		}
	}
	return tokens, nil
}

func webrtcMulti(ctx context.Context, info *serverInfo) ([]float64, error) {
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{ICEServers: []webrtc.ICEServer{}})
	if err != nil {
		return nil, err
	}
	defer pc.Close()
	burstDC, _ := pc.CreateDataChannel("burst", nil)
	tokenDC, _ := pc.CreateDataChannel("tokens", nil)
	_ = burstDC
	tokens := []float64{}
	var mu sync.Mutex
	doneCh := make(chan struct{})
	tokenDC.OnMessage(func(m webrtc.DataChannelMessage) {
		if len(m.Data) >= 8 {
			sentNs := decodeNs(m.Data[:8])
			if sentNs > 0 {
				mu.Lock()
				tokens = append(tokens, float64(time.Now().UnixNano()-sentNs)/1e6)
				if len(tokens) >= 400 {
					select {
					case <-doneCh:
					default:
						close(doneCh)
					}
				}
				mu.Unlock()
			}
		}
	})

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return nil, err
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		return nil, err
	}
	gather := webrtc.GatheringCompletePromise(pc)
	<-gather
	body, _ := json.Marshal(*pc.LocalDescription())
	resp, err := http.Post(info.WebrtcSignaling+"?multi=1", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	var ans webrtc.SessionDescription
	if err := json.Unmarshal(respBody, &ans); err != nil {
		return nil, err
	}
	if err := pc.SetRemoteDescription(ans); err != nil {
		return nil, err
	}
	select {
	case <-doneCh:
	case <-time.After(15 * time.Second):
	}
	mu.Lock()
	defer mu.Unlock()
	out := append([]float64(nil), tokens...)
	return out, nil
}

// ----- Test 3: session resumption -----
func runResumeTest(info *serverInfo, n int) []sample {
	var samples []sample
	ctx := context.Background()
	fmt.Printf("\n--- session resumption (n=%d) ---\n", n)
	for i := 0; i < n; i++ {
		if ms, err := quicrtcSetup(ctx, info); err == nil {
			samples = append(samples, sample{"quicrtc", "initial_setup", i, ms, ""})
		} else {
			fmt.Printf("  quicrtc initial %d: ERR %v\n", i, err)
		}
		// quicrtc resume
		if ms, sid, err := quicrtcSetupCapture(ctx, info, ""); err == nil {
			samples = append(samples, sample{"quicrtc", "initial_setup", i, ms, ""})
			// resume with same SID
			if rms, _, err := quicrtcSetupCapture(ctx, info, sid); err == nil {
				samples = append(samples, sample{"quicrtc", "resume", i, rms, ""})
				fmt.Printf("  quicrtc %3d: initial=%.2f ms  resume=%.2f ms\n", i, ms, rms)
			}
		}
		// webrtc full reconnect
		if ms, err := webrtcSetup(ctx, info.WebrtcSignaling); err == nil {
			samples = append(samples, sample{"webrtc-reconnect", "initial_setup", i, ms, ""})
			if rms, err := webrtcSetup(ctx, info.WebrtcSignaling); err == nil {
				samples = append(samples, sample{"webrtc-reconnect", "resume", i, rms, ""})
				fmt.Printf("  webrtc  %3d: initial=%.2f ms  resume=%.2f ms\n", i, ms, rms)
			}
		}
	}
	return samples
}

func quicrtcSetupCapture(ctx context.Context, info *serverInfo, sessionID string) (float64, string, error) {
	t0 := time.Now()
	c, err := qclient.Dial(ctx, info.QuicrtcShareLink, qclient.Options{
		HelloTimeout:      10 * time.Second,
		DataInboundBuffer: 8192,
		SessionID:         sessionID,
	})
	if err != nil {
		return 0, "", err
	}
	defer c.Close()
	recvCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := c.DataChannel().Recv(recvCtx); err != nil {
		return 0, "", err
	}
	return float64(time.Since(t0).Microseconds()) / 1000.0, c.SessionID(), nil
}

// ----- Test 4: scale ramp -----
func runScaleTest(info *serverInfo, total int, batch int, interval time.Duration) []sample {
	var samples []sample
	ctx := context.Background()
	fmt.Printf("\n--- scale: %d sessions, batch=%d/%s ---\n", total, batch, interval)
	var wg sync.WaitGroup
	var (
		quicOK  atomic.Int64
		quicErr atomic.Int64
		wrtcOK  atomic.Int64
		wrtcErr atomic.Int64
		mu      sync.Mutex
	)
	t0 := time.Now()
	idx := 0
	for idx < total {
		size := batch
		if idx+size > total {
			size = total - idx
		}
		for i := 0; i < size; i++ {
			j := idx + i
			wg.Add(2)
			go func(k int) {
				defer wg.Done()
				if ms, err := quicrtcSetup(ctx, info); err == nil {
					mu.Lock()
					samples = append(samples, sample{"quicrtc", "scale_setup", k, ms, ""})
					mu.Unlock()
					quicOK.Add(1)
					// Stay connected ~30s
					time.Sleep(30 * time.Second)
				} else {
					mu.Lock()
					samples = append(samples, sample{"quicrtc", "scale_setup", k, 0, err.Error()})
					mu.Unlock()
					quicErr.Add(1)
				}
			}(j)
			go func(k int) {
				defer wg.Done()
				if ms, err := webrtcSetup(ctx, info.WebrtcSignaling); err == nil {
					mu.Lock()
					samples = append(samples, sample{"webrtc", "scale_setup", k, ms, ""})
					mu.Unlock()
					wrtcOK.Add(1)
					time.Sleep(30 * time.Second)
				} else {
					mu.Lock()
					samples = append(samples, sample{"webrtc", "scale_setup", k, 0, err.Error()})
					mu.Unlock()
					wrtcErr.Add(1)
				}
			}(j)
		}
		idx += size
		fmt.Printf("[%5.1fs] dispatched %d (quic ok=%d err=%d, wrtc ok=%d err=%d)\n",
			time.Since(t0).Seconds(), idx,
			quicOK.Load(), quicErr.Load(), wrtcOK.Load(), wrtcErr.Load())
		time.Sleep(interval)
	}
	wg.Wait()
	fmt.Printf("scale done. quicrtc ok=%d err=%d  webrtc ok=%d err=%d\n",
		quicOK.Load(), quicErr.Load(), wrtcOK.Load(), wrtcErr.Load())
	return samples
}

// helpers
func decodeNs(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	return int64(uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7]))
}

func pctOf(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	return xs[int(float64(len(xs)-1)*p)]
}
