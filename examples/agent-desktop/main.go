// agent-desktop — the flagship quicrtc demo.
//
// A cloud coding agent (think Devin desktop / Cursor cloud agent)
// works a scripted task on its remote desktop: reproduce a failing
// test, patch the bug, re-run the suite, open a PR, watch CI. Its
// desktop screen, reasoning tokens, tool calls, and telemetry are
// produced ONCE by an agent engine and delivered to the browser over
// TWO transports simultaneously:
//
//	quicrtc   four lanes on one QUIC/WebTransport connection —
//	          screen (stream-per-GOP), reasoning (low-latency
//	          stream), tool calls (bidi-per-call), telemetry
//	          (datagrams)
//	baseline  the same four lanes multiplexed on ONE WebSocket,
//	          the way most agent products ship today
//
// Both connections are routed through the SAME in-process emulated
// link (RTT + bottleneck bandwidth; see shaper.go), and every event
// carries the same publish timestamp, so the browser page can render
// the two desktops side by side and measure per-lane delivery latency
// on identical workloads. The page and the servers all run from this
// one binary:
//
//	go run ./examples/agent-desktop
//	# prints: open http://127.0.0.1:8420
//
// No API key, no Chrome automation, no npm build — the page speaks
// the quicrtc wire format directly over raw WebTransport.
//
// For a browserless run that prints the same comparison as a table
// (CI-able, and the source of the README numbers):
//
//	go run ./examples/agent-desktop -bench 30s
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/sachinkesiraju/quicrtc/examples/internal/status"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/track"
	"github.com/sachinkesiraju/quicrtc/wire"
)

//go:embed ui.html
var uiHTML []byte

// Track names, matched by the browser page and the bench client.
const (
	trackScreen    = "screen"
	trackReasoning = "reasoning"
	trackToolCalls = "toolcalls"
	trackTelemetry = "telemetry"

	telemetryTrackID uint8 = 1
)

// profiles are the built-in network shapes. Loss stays 0 by default
// because the TCP proxy cannot drop packets — a nonzero -loss impairs
// only the quicrtc path (see shaper.go), which is fine for stress
// testing quicrtc but would be unfair to advertise as the default
// comparison.
// The screen lane averages ~6 Mbps (10 fps × ~70 KB PNG), so "cafe"
// runs the link at ~80% utilization — congested but not collapsing,
// the regime where transport architecture is visible.
var profiles = map[string]Shape{
	"clean":     {},
	"broadband": {RTT: 30 * time.Millisecond, Mbps: 25},
	"cafe":      {RTT: 40 * time.Millisecond, Mbps: 8},
	"hotel":     {RTT: 80 * time.Millisecond, Mbps: 6},
}

func main() {
	var (
		httpAddr = flag.String("http", "127.0.0.1:8420", "address for the demo page")
		profile  = flag.String("profile", "cafe", "network profile: clean | broadband | cafe | hotel (or use -rtt/-mbps/-loss)")
		rttFlag  = flag.Duration("rtt", -1, "override: emulated round-trip time")
		mbpsFlag = flag.Float64("mbps", -1, "override: emulated per-direction bandwidth (0 = unlimited)")
		lossFlag = flag.Float64("loss", -1, "override: packet loss %% (UDP/QUIC path only; see shaper.go)")
		fps      = flag.Int("fps", 10, "desktop screen frame rate")
		pace     = flag.Duration("pace", 5*time.Second, "minimum wall time per agent step")
		bench    = flag.Duration("bench", 0, "run headless Go clients on both transports for this long, print the comparison table, and exit")
	)
	flag.Parse()

	shape, ok := profiles[*profile]
	if !ok {
		log.Fatalf("unknown -profile=%q (want clean | broadband | cafe | hotel)", *profile)
	}
	if *rttFlag >= 0 {
		shape.RTT = *rttFlag
	}
	if *mbpsFlag >= 0 {
		shape.Mbps = *mbpsFlag
	}
	if *lossFlag >= 0 {
		shape.Loss = *lossFlag / 100
	}

	st := status.New("agent-desktop")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// ── quicrtc server (backend; reached through the UDP shaper) ──
	srv, err := server.New(server.Config{
		Addr:         "127.0.0.1:0",
		CertExtraIPs: []net.IP{net.ParseIP("127.0.0.1")},
		ExternalHost: "127.0.0.1",
		SDP: wire.SDP{
			Codec: "image/png", Width: deskW, Height: deskH, FPS: *fps,
		},
		// This session knowingly mixes latency-sensitive lanes (tokens,
		// tool calls) with a sustained bulk lane (screen) over a
		// contended link — the exact case the per-session priority
		// scheduler exists for: token/tool-call writes drain ahead of
		// queued bulk frames.
		PriorityScheduler: server.SchedulerOn,
		OnSession:         func(h server.SessionHandle) { st.Inc("q_sessions", 1) },
	})
	if err != nil {
		log.Fatal(err)
	}
	sinks := &quicrtcSink{
		screen:    srv.AddTrackSpec(server.TrackSpec{Name: trackScreen, Kind: track.KindVideo}),
		reasoning: srv.AddTrackSpec(server.TrackSpec{Name: trackReasoning, Kind: track.KindTokens}),
		toolcalls: srv.AddTrackSpec(server.TrackSpec{Name: trackToolCalls, Kind: track.KindToolCalls}),
		telemetry: srv.AddTrackSpec(server.TrackSpec{Name: trackTelemetry, Kind: track.KindTelemetry, TrackID: telemetryTrackID}),
		st:        st,
	}
	go func() {
		if err := srv.ListenAndServe(ctx); err != nil && ctx.Err() == nil {
			log.Printf("quicrtc server: %v", err)
		}
	}()
	for srv.Addr() == "127.0.0.1:0" {
		time.Sleep(10 * time.Millisecond)
	}

	// ── WebSocket baseline server (backend; reached through the TCP
	// shaper) ──
	hub := newWSHub(st)
	wsMux := http.NewServeMux()
	wsMux.Handle("/ws", hub.Handler())
	wsListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	wsServer := &http.Server{Handler: wsMux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = wsServer.Serve(wsListener) }()
	defer func() { _ = wsServer.Close() }()

	// ── the emulated link in front of each backend ──
	quicAddr := srv.Addr()
	wsAddr := wsListener.Addr().String()
	if !shape.zero() {
		backendUDP, err := net.ResolveUDPAddr("udp", quicAddr)
		if err != nil {
			log.Fatal(err)
		}
		udpPublic, stopUDP, err := startUDPShaper(backendUDP, shape)
		if err != nil {
			log.Fatal(err)
		}
		defer stopUDP()
		quicAddr = udpPublic.String()

		tcpPublic, stopTCP, err := startTCPShaper(wsAddr, shape)
		if err != nil {
			log.Fatal(err)
		}
		defer stopTCP()
		wsAddr = tcpPublic.String()
	}

	shareURL := fmt.Sprintf("https://%s/wt#slug=%s&hash=%s", quicAddr, srv.Slug(), srv.CertHashB64())
	wsURL := fmt.Sprintf("ws://%s/ws", wsAddr)

	// ── the demo page (unshaped — the UI itself should be crisp) ──
	cfg := pageConfig{
		WTURL:    fmt.Sprintf("https://%s/wt", quicAddr),
		Slug:     srv.Slug(),
		CertHash: srv.CertHashB64(),
		WSURL:    wsURL,
		Profile:  *profile,
		RTTMs:    int(shape.RTT / time.Millisecond),
		Mbps:     shape.Mbps,
		LossPct:  shape.Loss * 100,
		FPS:      *fps,
		Width:    deskW,
		Height:   deskH,
	}
	uiMux := http.NewServeMux()
	uiMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(uiHTML)
	})
	uiMux.HandleFunc("/config", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(cfg)
	})
	uiServer := &http.Server{Addr: *httpAddr, Handler: uiMux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		err := uiServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) && ctx.Err() == nil {
			log.Fatalf("ui server: %v (is %s already in use?)", err, *httpAddr)
		}
	}()
	defer func() { _ = uiServer.Close() }()

	// ── the agent engine, fanning out to both transports ──
	eng := newEngine(*fps, *pace, st)
	eng.addSink(sinks)
	eng.addSink(hub)
	go eng.run(ctx)

	fmt.Printf("agent-desktop — cloud agent desktop over quicrtc vs a single WebSocket\n")
	fmt.Printf("  open       http://%s\n", *httpAddr)
	fmt.Printf("  network    %s (rtt=%v, bw=%s, loss=%.1f%%)\n",
		*profile, shape.RTT, mbpsStr(shape.Mbps), shape.Loss*100)
	fmt.Printf("  quicrtc    %s\n", shareURL)
	fmt.Printf("  websocket  %s\n", wsURL)

	if *bench > 0 {
		if err := runBench(ctx, shareURL, wsURL, *bench, st); err != nil {
			log.Fatalf("bench: %v", err)
		}
		return
	}
	fmt.Println("(Ctrl-C to stop)")

	stopTick := st.Tick(500*time.Millisecond, func(l *status.Line) {
		l.Field("viewers", "q=%d ws=%d", st.Get("q_sessions"), st.Get("ws_sessions"))
		l.Raw("step=%d", st.Get("steps"))
		l.Raw("screen=%d", st.Get("screen_au"))
		l.Raw("tokens=%d", st.Get("token_au"))
		l.Raw("tools=%d", st.Get("tool_au"))
	})
	<-ctx.Done()
	stopTick()
	st.Done(
		"agent-desktop finished — same agent, two wires",
		[]status.Field{
			{Key: "uptime", Value: st.Uptime().Truncate(time.Second).String()},
			{Key: "agent steps", Value: fmt.Sprintf("%d", st.Get("steps"))},
			{Key: "screen frames", Value: fmt.Sprintf("%s  %s",
				status.HumanCount(st.Get("screen_au")),
				status.HumanBytes(st.Get("screen_bytes")))},
			{Key: "reasoning tokens", Value: status.HumanCount(st.Get("token_au"))},
			{Key: "tool calls", Value: status.HumanCount(st.Get("tool_au"))},
			{Key: "quicrtc viewers", Value: fmt.Sprintf("%d", st.Get("q_sessions"))},
			{Key: "websocket viewers", Value: fmt.Sprintf("%d", st.Get("ws_sessions"))},
		})
}

func mbpsStr(m float64) string {
	if m == 0 {
		return "unlimited"
	}
	return strings.TrimSuffix(fmt.Sprintf("%.1f", m), ".0") + " Mbps"
}

// pageConfig is the JSON the page bootstraps from.
type pageConfig struct {
	WTURL    string  `json:"wtUrl"`
	Slug     string  `json:"slug"`
	CertHash string  `json:"certHash"`
	WSURL    string  `json:"wsUrl"`
	Profile  string  `json:"profile"`
	RTTMs    int     `json:"rttMs"`
	Mbps     float64 `json:"mbps"`
	LossPct  float64 `json:"lossPct"`
	FPS      int     `json:"fps"`
	Width    int     `json:"width"`
	Height   int     `json:"height"`
}

// quicrtcSink routes engine events onto the four quicrtc tracks.
// Publish hands the AU to the broadcaster (bounded per-subscriber
// queues, keyframe-aware drop policy) and returns immediately.
type quicrtcSink struct {
	screen    *server.Publisher
	reasoning *server.Publisher
	toolcalls *server.Publisher
	telemetry *server.Publisher
	st        *status.Status
}

func (q *quicrtcSink) Deliver(ev event) {
	au := pubsub.AccessUnit{
		Bytes:    ev.payload,
		Keyframe: ev.keyframe,
		PTSMicro: ev.ptsMicro,
		Seq:      ev.seq,
	}
	var pub *server.Publisher
	switch ev.lane {
	case laneScreen:
		pub = q.screen
	case laneReasoning:
		pub = q.reasoning
	case laneToolCalls:
		pub = q.toolcalls
	case laneTelemetry:
		pub = q.telemetry
	default:
		return
	}
	if err := pub.Publish(context.Background(), au); err == nil {
		q.st.Inc("q_bytes", int64(len(ev.payload)))
	}
}
