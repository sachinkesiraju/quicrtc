package main

// Computer-use closed-loop WAN mode: action→DOM RTT under realistic
// load, quicrtc (4 tracks via AddTrackSpec) vs a single TLS-over-TCP
// baseline that multiplexes the same 4 logical streams with a 1-byte
// stream-id and length-prefixed framing.
//
// This mode is the WAN counterpart of
// testing/benchmarks/agent/computer_use_baseline_test.go, but with no
// synthetic netem proxy: both arms run back-to-back over the SAME
// real WAN path on the same VM pair, so the per-trial RTT/loss/jitter
// state is comparable. The user may add -loss-pct on the server to
// layer additional synthetic loss on top via `tc qdisc netem`, but
// the headline run is "no synthetic loss" — the WAN itself is the
// network condition.
//
// Wire layout (quicrtc arm)
// -------------------------
//   screen      AddTrackSpec(KindVideo)      server → client    bulk
//   actions     Client.Publish(KindToolCalls) client → server   round-trip
//   dom_events  AddTrackSpec(KindTokens)      server → client   echo of action ts
//   telemetry   AddTrackSpec(KindTelemetry)   server → client   sanity counter
//
// Wire layout (TCP-mux baseline)
// ------------------------------
//   single TLS connection.  Each frame:
//     [4-byte length BE][1-byte stream-id][payload ...]
//   stream-id: 0=screen 1=actions 2=dom 3=telemetry
//   FIFO writer mutex on both sides (mirrors a real product that ships
//   everything over a single WebSocket or HTTP/2 stream).
//
// Workload (per trial, per arm)
// -----------------------------
//   screen      30 fps @ size-derived-from -screen-mbps (default 12 Mbps -> 50 KB)
//   actions     100 / s × 64 B
//   dom_events  1 per inbound action × 256 B (echoes the action ts)
//   telemetry   200 / s × 64 B
//   duration    -trial-sec (default 12 s)
//   trials      -trials    (default 3)
//
// CSV output (wan_results/computer_use.csv)
// -----------------------------------------
//   protocol,trial,action_index,rtt_ms,err
// per-action samples, both arms, all trials.

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	qclient "github.com/sachinkesiraju/quicrtc/client"
	"github.com/sachinkesiraju/quicrtc/cert"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	qserver "github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/track"
)

// --- workload constants (mirror testing/benchmarks/agent constants) ---

const (
	cuScreenFPS       = 30
	cuScreenKeyEvery  = 30  // 1 keyframe / s
	cuTokenRate       = 200 // telemetry-like sanity counter
	cuTokenSize       = 64
	cuActionRate      = 100
	cuActionSize      = 64
	cuDomSize         = 256
	cuStreamID byte   = 0 // baseline stream IDs
	cuStreamAction byte = 1
	cuStreamDom    byte = 2
	cuStreamTokens byte = 3
)

// screenBytesPerFrame computes the per-frame screen payload size from
// the -screen-mbps flag. Default 12 Mbps → 50 KB/frame (matches the
// synthetic baseline test). At 60 Mbps → 250 KB/frame.
func screenBytesPerFrame(mbps float64) int {
	if mbps <= 0 {
		mbps = 12
	}
	// bytes/frame = mbps * 1e6 / 8 / fps
	bpf := int(mbps * 1e6 / 8.0 / float64(cuScreenFPS))
	if bpf < 1024 {
		bpf = 1024
	}
	return bpf
}

// timedPayload returns a buffer of `size` bytes with an 8-byte BE
// UnixNano timestamp prefix. Mirrors loadgen.TimedPayload but the
// wan_bench main package can't import the internal loadgen pkg.
func timedPayload(size int, ts time.Time) []byte {
	if size < 8 {
		size = 8
	}
	b := make([]byte, size)
	binary.BigEndian.PutUint64(b[:8], uint64(ts.UnixNano()))
	for i := 8; i < size; i++ {
		b[i] = 0xcd
	}
	return b
}

func decodeTS(b []byte) time.Time {
	if len(b) < 8 {
		return time.Time{}
	}
	return time.Unix(0, int64(binary.BigEndian.Uint64(b[:8])))
}

// ----------------------------------------------------------------------
// netem helper (Linux only)
// ----------------------------------------------------------------------

// startComputerUseNetem applies `tc qdisc add dev <iface> root netem
// loss <pct>%` when pct > 0 and we're on Linux with tc on PATH.
// Returns a cleanup func that removes the qdisc; safe to call even
// on no-op (returns a no-op cleanup).
//
// On macOS/Windows, or when tc is missing, prints a clear warning and
// returns a no-op. The bench then runs over whatever real loss the
// WAN already exhibits — which is the headline measurement anyway.
func startComputerUseNetem(lossPct float64, iface string) func() {
	if lossPct <= 0 {
		return func() {}
	}
	if runtime.GOOS != "linux" {
		fmt.Fprintf(os.Stderr, "[computer_use] WARNING: -loss-pct=%.2f set, but this OS (%s) does not support tc/netem; running with WAN-only loss\n",
			lossPct, runtime.GOOS)
		return func() {}
	}
	if _, err := exec.LookPath("tc"); err != nil {
		fmt.Fprintf(os.Stderr, "[computer_use] WARNING: -loss-pct=%.2f set, but 'tc' (iproute2) is not on PATH; running with WAN-only loss\n",
			lossPct)
		return func() {}
	}
	// `tc qdisc add dev <iface> root netem loss <pct>%`
	cmd := exec.Command("tc", "qdisc", "add", "dev", iface, "root", "netem", "loss", fmt.Sprintf("%.2f%%", lossPct))
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[computer_use] WARNING: tc netem add failed (%v): %s\n  (likely needs root; running with WAN-only loss)\n",
			err, strings.TrimSpace(string(out)))
		return func() {}
	}
	fmt.Printf("[computer_use] tc netem loss=%.2f%% applied on dev %s\n", lossPct, iface)
	return func() {
		del := exec.Command("tc", "qdisc", "del", "dev", iface, "root", "netem")
		if dout, derr := del.CombinedOutput(); derr != nil {
			fmt.Fprintf(os.Stderr, "[computer_use] WARNING: tc netem del failed (%v): %s\n", derr, strings.TrimSpace(string(dout)))
		} else {
			fmt.Printf("[computer_use] tc netem loss removed from dev %s\n", iface)
		}
	}
}

// ----------------------------------------------------------------------
// SERVER SIDE
// ----------------------------------------------------------------------

// computerUseServerState bundles the publishers the OnSession callback
// needs to drive a trial.
type computerUseServerState struct {
	screen *qserver.Publisher
	dom    *qserver.Publisher
	tokens *qserver.Publisher

	// sessionMu serializes OnSession entries so two simultaneous
	// quicrtc clients can't race the per-arm workload. The bench
	// contract says "Each handles one client connection at a time."
	sessionMu sync.Mutex
}

// installComputerUseTracks registers the 4 quicrtc tracks for the
// computer_use mode. Idempotent on track name (AddTrackSpec is
// idempotent server-side), so re-registering during runServer is
// safe but should only happen once.
func installComputerUseTracks(qsrv *qserver.Server) *computerUseServerState {
	st := &computerUseServerState{
		screen: qsrv.AddTrackSpec(qserver.TrackSpec{Name: "cu_screen", Kind: track.KindVideo}),
		dom:    qsrv.AddTrackSpec(qserver.TrackSpec{Name: "cu_dom", Kind: track.KindTokens}),
		tokens: qsrv.AddTrackSpec(qserver.TrackSpec{Name: "cu_telemetry", Kind: track.KindTelemetry, TrackID: 7}),
	}
	return st
}

// makeComputerUseOnSession returns a Config.OnSession handler that
// drives a single computer_use trial per session. The state getter is
// invoked lazily on each session so server.go can install OnSession
// in the qserver.Config BEFORE installComputerUseTracks runs.
//
// Pumps run only after the client announces the "cu_actions" track —
// gating other test modes (setup/multi/resume/scale) from triggering
// computer-use traffic. setup/multi/resume/scale clients never
// Publish, so InboundRecv times out fast and the goroutine exits.
//
// Trial ceiling: trialSec*4 — enough for slow-start and per-action
// retry headroom under loss. The screen/dom/telemetry goroutines wind
// down when totalScreen/totalActions/totalTokens are sent OR when the
// session context is cancelled.
func makeComputerUseOnSession(getState func() *computerUseServerState, screenMbps float64, trialSec int) func(qserver.SessionHandle) {
	return func(h qserver.SessionHandle) {
		st := getState()
		if st == nil {
			return
		}
		go func() {
			st.sessionMu.Lock()
			defer st.sessionMu.Unlock()

			sctx := h.Context()
			detectCtx, detectCancel := context.WithTimeout(sctx, 2*time.Second)
			defer detectCancel()
			firstAU, err := h.InboundRecv(detectCtx, "cu_actions")
			if err != nil {
				return
			}

			trialCtx, trialCancel := context.WithTimeout(sctx, time.Duration(trialSec*4)*time.Second)
			defer trialCancel()

			runComputerUseServerTrial(trialCtx, h, st, firstAU, screenMbps, trialSec)
		}()
	}
}

// runComputerUseServerTrial pumps screen + telemetry, echoes inbound
// actions as dom_events. firstAU is the first action AU already drained
// off the InboundRecv queue by the detection step — we must echo it as
// the first dom_event so the client doesn't see one missing.
func runComputerUseServerTrial(ctx context.Context, h qserver.SessionHandle, st *computerUseServerState, firstAU pubsub.AccessUnit, screenMbps float64, trialSec int) {
	totalActions := cuActionRate * trialSec
	totalScreen := cuScreenFPS * trialSec
	totalTokens := cuTokenRate * trialSec
	screenSize := screenBytesPerFrame(screenMbps)

	actionsRecvd := int32(0)

	// Echo firstAU immediately.
	echoDOM(ctx, st.dom, firstAU.Bytes, 1)
	atomic.AddInt32(&actionsRecvd, 1)

	var wg sync.WaitGroup

	// Inbound action echo loop.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for atomic.LoadInt32(&actionsRecvd) < int32(totalActions) {
			rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
			au, err := h.InboundRecv(rctx, "cu_actions")
			rcancel()
			if err != nil {
				return
			}
			n := atomic.AddInt32(&actionsRecvd, 1)
			echoDOM(ctx, st.dom, au.Bytes, uint32(n))
		}
	}()

	// Screen pump (30 fps × screenSize).
	wg.Add(1)
	go func() {
		defer wg.Done()
		pace := time.Second / time.Duration(cuScreenFPS)
		ticker := time.NewTicker(pace)
		defer ticker.Stop()
		for i := 0; i < totalScreen; i++ {
			if ctx.Err() != nil {
				return
			}
			payload := timedPayload(screenSize, time.Now())
			_ = st.screen.Publish(ctx, pubsub.AccessUnit{
				Bytes:    payload,
				Keyframe: i%cuScreenKeyEvery == 0,
				Seq:      uint32(i),
				PTSMicro: uint64(time.Now().UnixMicro()),
			})
			if i < totalScreen-1 {
				select {
				case <-ticker.C:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// Telemetry sanity counter (200 / s × 64 B). Datagrams may drop —
	// that is the point. Kept here only as the 4th-track sanity stream;
	// not measured in the headline numbers.
	wg.Add(1)
	go func() {
		defer wg.Done()
		pace := time.Second / time.Duration(cuTokenRate)
		ticker := time.NewTicker(pace)
		defer ticker.Stop()
		for i := 0; i < totalTokens; i++ {
			if ctx.Err() != nil {
				return
			}
			_ = st.tokens.Publish(ctx, pubsub.AccessUnit{
				Bytes: timedPayload(cuTokenSize, time.Now()),
				Seq:   uint32(i),
			})
			if i < totalTokens-1 {
				select {
				case <-ticker.C:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	wg.Wait()
}

func echoDOM(ctx context.Context, dom *qserver.Publisher, action []byte, seq uint32) {
	payload := make([]byte, cuDomSize)
	if len(action) >= 8 {
		copy(payload[:8], action[:8])
	}
	// seq==1 is a keyframe so a fresh subscriber's receiver (which starts
	// in need-keyframe state and would otherwise drop every DOM) begins
	// delivering. The shared cu_dom broadcaster caches the latest keyframe
	// and replays it to the next trial's subscriber, so trial N's client
	// is first handed trial N-1's STALE DOM (action timestamp ~a trial
	// old). The client discards any DOM stamped before the trial began
	// (see runComputerUseQuicArm), so that replayed-stale-keyframe
	// artifact — which was the entire bogus ~30s WAN "tail," not a
	// transport stall — is no longer mis-measured.
	_ = dom.Publish(ctx, pubsub.AccessUnit{
		Bytes:    payload,
		Keyframe: seq == 1,
		Seq:      seq,
	})
}

// ----------------------------------------------------------------------
// TCP-mux baseline server
// ----------------------------------------------------------------------

// startTCPBaselineServer listens on addr for TLS connections. Each
// connection is one trial: server reads actions, echoes dom_events
// with the action timestamp prefix, simultaneously pumps screen and
// telemetry. Uses a writer mutex to enforce the FIFO byte ordering of
// a single TCP connection — this is what makes TCP HOL bite under loss.
func startTCPBaselineServer(ctx context.Context, addr string, bundle *cert.Bundle, screenMbps float64, trialSec int) error {
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{bundle.TLS},
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"wan-bench-cu/1"},
	}
	ln, err := tls.Listen("tcp", addr, tlsCfg)
	if err != nil {
		return fmt.Errorf("tcp baseline listen %s: %w", addr, err)
	}
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	go func() {
		// One-at-a-time accept. Each connection IS one trial.
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handleTCPBaselineConn(ctx, conn, screenMbps, trialSec)
		}
	}()
	fmt.Printf("[computer_use] tcp-mux baseline listening on %s (tls)\n", addr)
	return nil
}

func handleTCPBaselineConn(ctx context.Context, conn net.Conn, screenMbps float64, trialSec int) {
	defer conn.Close()
	// Drive the TLS handshake explicitly so failures are visible.
	if tc, ok := conn.(*tls.Conn); ok {
		hctx, hcancel := context.WithTimeout(ctx, 10*time.Second)
		defer hcancel()
		if err := tc.HandshakeContext(hctx); err != nil {
			fmt.Fprintln(os.Stderr, "[computer_use] tcp baseline tls handshake:", err)
			return
		}
	}

	totalScreen := cuScreenFPS * trialSec
	totalTokens := cuTokenRate * trialSec
	totalActions := cuActionRate * trialSec
	screenSize := screenBytesPerFrame(screenMbps)

	var writeMu sync.Mutex
	br := bufio.NewReaderSize(conn, 256*1024)

	// Bound the trial so a wedged peer can't pin the connection.
	trialCtx, cancel := context.WithTimeout(ctx, time.Duration(trialSec*4)*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	// Action reader → dom echo. ALSO competes for writeMu with the
	// screen pump: this is exactly the HOL story the bench measures.
	wg.Add(1)
	go func() {
		defer wg.Done()
		echoes := 0
		for echoes < totalActions {
			if trialCtx.Err() != nil {
				return
			}
			sid, payload, err := readMuxFrame(br)
			if err != nil {
				return
			}
			if sid != cuStreamAction {
				continue
			}
			dom := make([]byte, cuDomSize)
			if len(payload) >= 8 {
				copy(dom[:8], payload[:8])
			}
			writeMu.Lock()
			err = writeMuxFrame(conn, cuStreamDom, dom)
			writeMu.Unlock()
			if err != nil {
				return
			}
			echoes++
		}
	}()

	// Screen pump.
	wg.Add(1)
	go func() {
		defer wg.Done()
		pace := time.Second / time.Duration(cuScreenFPS)
		ticker := time.NewTicker(pace)
		defer ticker.Stop()
		for i := 0; i < totalScreen; i++ {
			if trialCtx.Err() != nil {
				return
			}
			payload := timedPayload(screenSize, time.Now())
			writeMu.Lock()
			err := writeMuxFrame(conn, cuStreamID, payload)
			writeMu.Unlock()
			if err != nil {
				return
			}
			if i < totalScreen-1 {
				select {
				case <-ticker.C:
				case <-trialCtx.Done():
					return
				}
			}
		}
	}()

	// Telemetry pump.
	wg.Add(1)
	go func() {
		defer wg.Done()
		pace := time.Second / time.Duration(cuTokenRate)
		ticker := time.NewTicker(pace)
		defer ticker.Stop()
		for i := 0; i < totalTokens; i++ {
			if trialCtx.Err() != nil {
				return
			}
			writeMu.Lock()
			err := writeMuxFrame(conn, cuStreamTokens, timedPayload(cuTokenSize, time.Now()))
			writeMu.Unlock()
			if err != nil {
				return
			}
			if i < totalTokens-1 {
				select {
				case <-ticker.C:
				case <-trialCtx.Done():
					return
				}
			}
		}
	}()

	wg.Wait()
}

func writeMuxFrame(w io.Writer, sid byte, payload []byte) error {
	var hdr [5]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	hdr[4] = sid
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := w.Write(payload)
	return err
}

func readMuxFrame(r io.Reader) (byte, []byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	length := binary.BigEndian.Uint32(hdr[0:4])
	if length > 16*1024*1024 {
		return 0, nil, fmt.Errorf("oversized frame: %d", length)
	}
	if length == 0 {
		return hdr[4], nil, nil
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return hdr[4], payload, nil
}

// ----------------------------------------------------------------------
// CLIENT SIDE
// ----------------------------------------------------------------------

// cuSample is the per-action sample for the computer_use CSV. Distinct
// from the existing `sample` struct so the existing CSV writer is
// untouched.
type cuSample struct {
	Protocol string // "quicrtc" or "baseline"
	Trial    int
	Index    int
	RTTms    float64
	Err      string
}

// runComputerUseTest drives -trials trials. Each trial = one quicrtc
// arm run followed by one TCP-mux baseline arm run, both over the same
// real WAN path. Writes wan_results/computer_use.csv next to the
// existing wan_bench CSVs.
func runComputerUseTest(info *serverInfo, serverHost string) {
	screenMbps := *flagScreenMbps
	trials := *flagTrials
	trialSec := *flagTrialSec

	fmt.Printf("\n--- computer_use closed loop (trials=%d, %ds each, screen=%.0f Mbps) ---\n",
		trials, trialSec, screenMbps)

	// Prefer the server-advertised address (which carries the
	// configured port); fall back to <serverHost><-tcp-baseline-addr>
	// if /info didn't populate it (older server build).
	tcpAddr := info.TCPBaselineAddr
	if tcpAddr == "" {
		tcpAddr = fmt.Sprintf("%s%s", serverHost, *flagTCPBaseAddr)
		if strings.Contains(*flagTCPBaseAddr, ":") && !strings.HasPrefix(*flagTCPBaseAddr, ":") {
			tcpAddr = *flagTCPBaseAddr
		}
	}

	rttStart := measureRTT(serverHost)
	fmt.Printf("  baseline RTT (sanity) to %s: %s\n", serverHost, rttStart)

	var all []cuSample
	var allQuic, allBase []float64
	for trial := 0; trial < trials; trial++ {
		fmt.Printf("[trial %d/%d] quicrtc arm...\n", trial+1, trials)
		qSamples, err := runComputerUseQuicArm(info, trial, trialSec, screenMbps)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  quicrtc arm error: %v\n", err)
		}
		all = append(all, qSamples...)
		nq := 0
		for _, s := range qSamples {
			if s.Err == "" {
				allQuic = append(allQuic, s.RTTms)
				nq++
			}
		}
		fmt.Printf("  quicrtc:  actions=%d\n", nq)

		fmt.Printf("[trial %d/%d] baseline (tcp-mux) arm...\n", trial+1, trials)
		bSamples, err := runComputerUseBaselineArm(tcpAddr, trial, trialSec, screenMbps)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  baseline arm error: %v\n", err)
		}
		all = append(all, bSamples...)
		nb := 0
		for _, s := range bSamples {
			if s.Err == "" {
				allBase = append(allBase, s.RTTms)
				nb++
			}
		}
		fmt.Printf("  baseline: actions=%d\n", nb)
	}

	// Write the per-action CSV next to the existing wan_results/*.csv.
	csvPath := *flagCSV
	if csvPath == "" || csvPath == "results.csv" {
		csvPath = "computer_use.csv"
	}
	// If a wan_results dir exists alongside the binary, prefer that
	// — keeps these CSVs in the same place as multi.csv etc.
	if _, err := os.Stat("wan_results"); err == nil {
		csvPath = filepath.Join("wan_results", "computer_use.csv")
	}
	if err := writeComputerUseCSV(csvPath, all); err != nil {
		fmt.Fprintln(os.Stderr, "csv:", err)
	}

	// Headline.
	rttEnd := measureRTT(serverHost)
	fmt.Println()
	fmt.Printf("=== Computer-use over real WAN, RTT≈%s start / %s end, loss=%s ===\n",
		rttStart, rttEnd, lossLabel())
	sort.Float64s(allQuic)
	sort.Float64s(allBase)
	// p99.9 and max are reported because the failure mode this bench
	// exists to catch — a per-action stall that hangs for seconds — lives
	// in the EXTREME tail, not at p99. A healthy run shows max within a
	// few hundred ms of p99; a stuck action shows up as a multi-second
	// max (the pre-fix run had a ~30s max from 2/3600 stalled actions).
	qp50, qp99 := percentile(allQuic, 0.50), percentile(allQuic, 0.99)
	qp999, qmax, qmean := percentile(allQuic, 0.999), maxOf(allQuic), meanOf(allQuic)
	bp50, bp99 := percentile(allBase, 0.50), percentile(allBase, 0.99)
	bp999, bmax, bmean := percentile(allBase, 0.999), maxOf(allBase), meanOf(allBase)
	fmt.Printf("quicrtc:  trials=%d actions=%d  p50=%.2f p99=%.2f p99.9=%.2f max=%.2f mean=%.2f\n",
		trials, len(allQuic), qp50, qp99, qp999, qmax, qmean)
	fmt.Printf("baseline: trials=%d actions=%d  p50=%.2f p99=%.2f p99.9=%.2f max=%.2f mean=%.2f\n",
		trials, len(allBase), bp50, bp99, bp999, bmax, bmean)
	if qp50 > 0 && qp99 > 0 {
		fmt.Printf("ratio (baseline/quicrtc): p50 = %.2fx, p99 = %.2fx, max = %.2fx\n",
			bp50/qp50, bp99/qp99, safeRatio(bmax, qmax))
	} else {
		fmt.Println("ratio (baseline/quicrtc): n/a (zero samples on at least one arm)")
	}
	fmt.Printf("\nwrote %s (%d samples)\n", csvPath, len(all))
}

func lossLabel() string {
	if *flagLossPct > 0 {
		return fmt.Sprintf("synthetic %.2f%% (tc netem on server)", *flagLossPct)
	}
	return "real-WAN (no synthetic netem)"
}

func measureRTT(host string) time.Duration {
	// One-shot TCP-dial timing as a rough RTT proxy: avoids needing
	// ICMP/ping privileges on the VM, and we only want order-of-mag.
	// Returns 0 if unreachable.
	t0 := time.Now()
	c, err := net.DialTimeout("tcp", fmt.Sprintf("%s:8090", host), 3*time.Second)
	if err != nil {
		return 0
	}
	d := time.Since(t0)
	_ = c.Close()
	return d
}

func writeComputerUseCSV(path string, ss []cuSample) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil && filepath.Dir(path) != "." {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	fmt.Fprintln(f, "protocol,trial,action_index,rtt_ms,err")
	for _, s := range ss {
		fmt.Fprintf(f, "%s,%d,%d,%.3f,%q\n", s.Protocol, s.Trial, s.Index, s.RTTms, s.Err)
	}
	return nil
}

// runComputerUseQuicArm runs one trial over the quicrtc transport.
func runComputerUseQuicArm(info *serverInfo, trial int, trialSec int, screenMbps float64) ([]cuSample, error) {
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer dialCancel()
	c, err := qclient.Dial(dialCtx, info.QuicrtcShareLink, qclient.Options{
		HelloTimeout:      10 * time.Second,
		DataInboundBuffer: 8192,
	})
	if err != nil {
		return nil, fmt.Errorf("dial quicrtc: %w", err)
	}
	defer c.Close()

	// Trial duration ceiling (trialSec*4 for slow start + retries).
	trialCtx, trialCancel := context.WithTimeout(context.Background(), time.Duration(trialSec*4)*time.Second)
	defer trialCancel()

	// Subscriber-side outbound: actions.
	actionSender, err := c.Publish(trialCtx, track.LocalTrack{
		Name: "cu_actions", Kind: track.KindToolCalls,
	})
	if err != nil {
		return nil, fmt.Errorf("publish actions: %w", err)
	}
	defer actionSender.Close()

	// Let the Announce reach the server before we start firing.
	time.Sleep(150 * time.Millisecond)

	totalActions := cuActionRate * trialSec
	totalScreen := cuScreenFPS * trialSec
	totalTokens := cuTokenRate * trialSec
	actionPace := time.Second / time.Duration(cuActionRate)

	samples := make([]cuSample, 0, totalActions)
	var mu sync.Mutex

	var wg sync.WaitGroup

	// trialStart marks when this trial's live workload begins; the dom
	// drain uses it to discard the shared broadcaster's stale replayed
	// keyframe (stamped before this trial).
	trialStart := time.Now()

	// Drain screen — sanity, not measured.
	wg.Add(1)
	go func() {
		defer wg.Done()
		recvd := 0
		for recvd < totalScreen {
			rctx, rcancel := context.WithTimeout(trialCtx, 5*time.Second)
			_, err := c.RecvOn(rctx, "cu_screen")
			rcancel()
			if err != nil {
				return
			}
			recvd++
		}
	}()

	// Drain telemetry — sanity.
	wg.Add(1)
	go func() {
		defer wg.Done()
		recvd := 0
		for recvd < totalTokens {
			rctx, rcancel := context.WithTimeout(trialCtx, 5*time.Second)
			_, err := c.RecvOn(rctx, "cu_telemetry")
			rcancel()
			if err != nil {
				return
			}
			recvd++
		}
	}()

	// Drain dom_events — measure closed-loop RTT.
	wg.Add(1)
	go func() {
		defer wg.Done()
		count := 0
		for count < totalActions {
			rctx, rcancel := context.WithTimeout(trialCtx, 5*time.Second)
			au, err := c.RecvOn(rctx, "cu_dom")
			rcancel()
			if err != nil {
				return
			}
			now := time.Now()
			sentAt := decodeTS(au.Bytes)
			if sentAt.IsZero() {
				count++
				continue
			}
			// Discard a DOM stamped before this trial began: it's the
			// shared broadcaster replaying the PREVIOUS trial's cached
			// keyframe to this fresh subscriber, not a live echo. This is
			// what produced the bogus ~30s "stall." It does not count
			// toward the measured actions, so a full set of live samples
			// is still collected.
			if sentAt.Before(trialStart) {
				continue
			}
			mu.Lock()
			samples = append(samples, cuSample{
				Protocol: "quicrtc",
				Trial:    trial,
				Index:    count,
				RTTms:    float64(now.Sub(sentAt).Microseconds()) / 1000.0,
			})
			mu.Unlock()
			count++
		}
	}()

	// Action sender.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(actionPace)
		defer ticker.Stop()
		for i := 0; i < totalActions; i++ {
			if trialCtx.Err() != nil {
				return
			}
			payload := timedPayload(cuActionSize, time.Now())
			_ = actionSender.Send(trialCtx, pubsub.AccessUnit{
				Bytes:    payload,
				Keyframe: true, // KindToolCalls treats every AU as one call
				Seq:      uint32(i),
			})
			if i < totalActions-1 {
				select {
				case <-ticker.C:
				case <-trialCtx.Done():
					return
				}
			}
		}
	}()

	wg.Wait()
	return samples, nil
}

// runComputerUseBaselineArm runs one trial over the TCP-mux baseline.
func runComputerUseBaselineArm(tcpAddr string, trial int, trialSec int, screenMbps float64) ([]cuSample, error) {
	tlsCfg := &tls.Config{
		InsecureSkipVerify: true, // self-signed cert; we trust the WAN path same as the quicrtc arm
		MinVersion:         tls.VersionTLS12,
		NextProtos:         []string{"wan-bench-cu/1"},
	}
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer dialCancel()
	dialer := &tls.Dialer{Config: tlsCfg, NetDialer: &net.Dialer{Timeout: 10 * time.Second}}
	rawConn, err := dialer.DialContext(dialCtx, "tcp", tcpAddr)
	if err != nil {
		return nil, fmt.Errorf("dial tcp baseline %s: %w", tcpAddr, err)
	}
	conn := rawConn.(*tls.Conn)
	defer conn.Close()

	trialCtx, trialCancel := context.WithTimeout(context.Background(), time.Duration(trialSec*4)*time.Second)
	defer trialCancel()

	totalActions := cuActionRate * trialSec
	actionPace := time.Second / time.Duration(cuActionRate)

	samples := make([]cuSample, 0, totalActions)
	var mu sync.Mutex
	var writeMu sync.Mutex
	br := bufio.NewReaderSize(conn, 256*1024)

	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		domCount := 0
		for domCount < totalActions {
			if trialCtx.Err() != nil {
				return
			}
			sid, payload, err := readMuxFrame(br)
			if err != nil {
				return
			}
			switch sid {
			case cuStreamDom:
				now := time.Now()
				sentAt := decodeTS(payload)
				if !sentAt.IsZero() {
					mu.Lock()
					samples = append(samples, cuSample{
						Protocol: "baseline",
						Trial:    trial,
						Index:    domCount,
						RTTms:    float64(now.Sub(sentAt).Microseconds()) / 1000.0,
					})
					mu.Unlock()
				}
				domCount++
			default:
				// drop screen / telemetry frames on the floor; the
				// reader has to keep up so they don't fill the kernel
				// recv buffer (which would deadlock the writer half).
			}
		}
	}()

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		ticker := time.NewTicker(actionPace)
		defer ticker.Stop()
		for i := 0; i < totalActions; i++ {
			if trialCtx.Err() != nil {
				return
			}
			payload := timedPayload(cuActionSize, time.Now())
			writeMu.Lock()
			err := writeMuxFrame(conn, cuStreamAction, payload)
			writeMu.Unlock()
			if err != nil {
				return
			}
			if i < totalActions-1 {
				select {
				case <-ticker.C:
				case <-trialCtx.Done():
					return
				}
			}
		}
	}()

	<-writerDone
	// Wait for reader to drain (bounded by trialCtx).
	select {
	case <-readerDone:
	case <-trialCtx.Done():
	}
	mu.Lock()
	defer mu.Unlock()
	out := append([]cuSample(nil), samples...)
	return out, nil
}

// ----------------------------------------------------------------------
// small stat helpers (duplicated lightly from stats.go to keep the
// computer_use headline self-contained without modifying the existing
// shared helpers).
// ----------------------------------------------------------------------

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func meanOf(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	var sum float64
	for _, v := range xs {
		sum += v
	}
	return sum / float64(len(xs))
}

func maxOf(xs []float64) float64 {
	var m float64
	for _, v := range xs {
		if v > m {
			m = v
		}
	}
	return m
}

// safeRatio returns a/b, or 0 when b is 0 (avoids a divide-by-zero in the
// max-ratio line when an arm recorded no samples).
func safeRatio(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}
