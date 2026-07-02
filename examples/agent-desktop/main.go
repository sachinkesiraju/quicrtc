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
	"encoding/binary"
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
	"sync"
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
	trackScreen      = "screen"
	trackReasoning   = "reasoning"
	trackToolCalls   = "toolcalls"
	trackTelemetry   = "telemetry"
	trackAcks        = "acks"         // server → client action acks
	trackUserActions = "user_actions" // client → server clicks (publish-back)
	trackFrameAcks   = "frame_acks"   // client → server frame receipts (publish-back)

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
		fps      = flag.Int("fps", 14, "desktop screen frame rate while the scene is changing (idles at 1/3 of this)")
		pace     = flag.Duration("pace", 5*time.Second, "minimum wall time per agent step")
		bench       = flag.Duration("bench", 0, "run headless Go clients on all three transports for this long, print the comparison table, and exit")
		naiveFrames = flag.Bool("naive-frames", false, "single-WS arm queues frames FIFO instead of ack-paced latest-only — the naive design many products actually ship")
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

	// The engine is constructed first so every transport's action
	// path can reference it; sinks attach below.
	eng := newEngine(*fps, *pace, st)

	// ── quicrtc server (backend; reached through the UDP shaper) ──
	var acksPub *server.Publisher
	var sinks *quicrtcSink
	srv, err := server.New(server.Config{
		Addr:         "127.0.0.1:0",
		CertExtraIPs: []net.IP{net.ParseIP("127.0.0.1")},
		ExternalHost: "127.0.0.1",
		SDP: wire.SDP{
			Codec: "image/png", Width: deskW, Height: deskH, FPS: *fps,
		},
		// Scheduler stays off: each lane's pump writes to its own QUIC
		// stream from its own goroutine and quic-go interleaves them
		// at the packet level, which is already the isolation the
		// demo is about. The app-level priority scheduler serializes
		// all pumps through one worker — under a congested link the
		// worker parks inside a blocking bulk-frame write and token
		// writes wait for chunk boundaries, adding tens of ms of
		// exactly the HOL the transport otherwise avoids.
		PriorityScheduler: server.SchedulerOff,
		// Freshness over depth: with a shallow per-subscriber buffer
		// the broadcaster's keyframe-aware drop policy sheds stale
		// P-frames as soon as the link falls behind, instead of
		// standing up seconds of queued screen. This is quicrtc's
		// native equivalent of the ack-paced latest-only sending the
		// steelmanned WS baselines use. Small lanes (tokens, tool
		// calls, acks) drain in microseconds and never fill 6 slots.
		PerSubscriberBuffer: 2,
		OnSession: func(h server.SessionHandle) {
			st.Inc("q_sessions", 1)
			// Interactive action lane: drain this session's
			// publish-back clicks; ripple the shared desktop and ack
			// on the acks track. The publisher pointer is set before
			// ListenAndServe, so it's non-nil by the first session.
			go drainUserActions(h, eng, func(au pubsub.AccessUnit) {
				_ = acksPub.Publish(context.Background(), au)
			})
			// Screen receipts: the viewer acks each frame it decodes,
			// gating the next publish (see quicrtcSink) — the same
			// client-receipt frame protocol the WS arms run.
			go func() {
				for {
					if _, err := h.InboundRecv(h.Context(), trackFrameAcks); err != nil {
						return
					}
					sinks.frameAcked()
				}
			}()
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	sinks = newQuicrtcSink(
		srv.AddTrackSpec(server.TrackSpec{Name: trackScreen, Kind: track.KindVideo}),
		srv.AddTrackSpec(server.TrackSpec{Name: trackReasoning, Kind: track.KindTokens}),
		srv.AddTrackSpec(server.TrackSpec{Name: trackToolCalls, Kind: track.KindToolCalls}),
		srv.AddTrackSpec(server.TrackSpec{Name: trackTelemetry, Kind: track.KindTelemetry, TrackID: telemetryTrackID}),
		st,
	)
	acksPub = srv.AddTrackSpec(server.TrackSpec{Name: trackAcks, Kind: track.KindTokens})
	go func() {
		if err := srv.ListenAndServe(ctx); err != nil && ctx.Err() == nil {
			log.Printf("quicrtc server: %v", err)
		}
	}()
	for srv.Addr() == "127.0.0.1:0" {
		time.Sleep(10 * time.Millisecond)
	}

	// ── baseline arm 1: everything on ONE WebSocket ──
	hub := newWSHub(st, eng.userAction, *naiveFrames)
	wsMux := http.NewServeMux()
	wsMux.Handle("/ws", hub.Handler())
	wsListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	wsServer := &http.Server{Handler: wsMux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = wsServer.Serve(wsListener) }()
	defer func() { _ = wsServer.Close() }()

	// ── baseline arm 2: the glued stack (frames WS + events WS + SSE) ──
	glued := newGluedHub(st, eng.userAction)
	gluedListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	gluedServer := &http.Server{Handler: glued.Mux(), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = gluedServer.Serve(gluedListener) }()
	defer func() { _ = gluedServer.Close() }()

	// ── one emulated link PER ARM (like three users on identical
	// networks; within the glued arm, its three connections share
	// one link) ──
	quicAddr := srv.Addr()
	wsAddr := wsListener.Addr().String()
	gluedAddr := gluedListener.Addr().String()
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

		wsPublic, stopWS, err := startTCPShaper(wsAddr, shape)
		if err != nil {
			log.Fatal(err)
		}
		defer stopWS()
		wsAddr = wsPublic.String()

		gluedPublic, stopGlued, err := startTCPShaper(gluedAddr, shape)
		if err != nil {
			log.Fatal(err)
		}
		defer stopGlued()
		gluedAddr = gluedPublic.String()
	}

	shareURL := fmt.Sprintf("https://%s/wt#slug=%s&hash=%s", quicAddr, srv.Slug(), srv.CertHashB64())
	wsURL := fmt.Sprintf("ws://%s/ws", wsAddr)
	gluedBase := gluedAddr

	// ── the demo page (unshaped — the UI itself should be crisp) ──
	cfg := pageConfig{
		WTURL:          fmt.Sprintf("https://%s/wt", quicAddr),
		Slug:           srv.Slug(),
		CertHash:       srv.CertHashB64(),
		WSURL:          wsURL,
		GluedFramesURL: fmt.Sprintf("ws://%s/frames", gluedBase),
		GluedEventsURL: fmt.Sprintf("ws://%s/events", gluedBase),
		GluedSSEURL:    fmt.Sprintf("http://%s/sse", gluedBase),
		Profile:        *profile,
		RTTMs:          int(shape.RTT / time.Millisecond),
		Mbps:           shape.Mbps,
		LossPct:        shape.Loss * 100,
		FPS:            *fps,
		Width:          deskW,
		Height:         deskH,
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
	// Bench mode is browserless; don't occupy the UI port so a live
	// demo and a bench run can coexist.
	if *bench == 0 {
		uiServer := &http.Server{Addr: *httpAddr, Handler: uiMux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			err := uiServer.ListenAndServe()
			if err != nil && !errors.Is(err, http.ErrServerClosed) && ctx.Err() == nil {
				log.Fatalf("ui server: %v (is %s already in use?)", err, *httpAddr)
			}
		}()
		defer func() { _ = uiServer.Close() }()
	}

	// ── start the engine, fanning identical events to all three arms ──
	eng.addSink(sinks)
	eng.addSink(hub)
	eng.addSink(glued)
	go eng.run(ctx)

	fmt.Printf("agent-desktop — one cloud agent, three wires, side by side\n")
	if *bench == 0 {
		fmt.Printf("  open       http://%s\n", *httpAddr)
	}
	fmt.Printf("  network    %s (rtt=%v, bw=%s, loss=%.1f%%)\n",
		*profile, shape.RTT, mbpsStr(shape.Mbps), shape.Loss*100)
	fmt.Printf("  quicrtc    %s\n", shareURL)
	fmt.Printf("  single-ws  %s\n", wsURL)
	fmt.Printf("  glued      ws://%s/frames + ws://%s/events + http://%s/sse\n", gluedBase, gluedBase, gluedBase)

	if *bench > 0 {
		if err := runBench(ctx, shareURL, cfg, *bench); err != nil {
			log.Fatalf("bench: %v", err)
		}
		return
	}
	fmt.Println("(Ctrl-C to stop)")

	stopTick := st.Tick(500*time.Millisecond, func(l *status.Line) {
		l.Field("viewers", "q=%d ws=%d glued=%d", st.Get("q_sessions"), st.Get("ws_sessions"), st.Get("glued_sessions"))
		l.Raw("step=%d", st.Get("steps"))
		l.Raw("screen=%d", st.Get("screen_au"))
		l.Raw("tokens=%d", st.Get("token_au"))
		l.Raw("actions=%d", st.Get("user_actions"))
	})
	<-ctx.Done()
	stopTick()
	st.Done(
		"agent-desktop finished — same agent, three wires",
		[]status.Field{
			{Key: "uptime", Value: st.Uptime().Truncate(time.Second).String()},
			{Key: "agent steps", Value: fmt.Sprintf("%d", st.Get("steps"))},
			{Key: "screen frames", Value: fmt.Sprintf("%s  %s",
				status.HumanCount(st.Get("screen_au")),
				status.HumanBytes(st.Get("screen_bytes")))},
			{Key: "reasoning tokens", Value: status.HumanCount(st.Get("token_au"))},
			{Key: "tool calls", Value: status.HumanCount(st.Get("tool_au"))},
			{Key: "user actions", Value: status.HumanCount(st.Get("user_actions"))},
			{Key: "quicrtc viewers", Value: fmt.Sprintf("%d", st.Get("q_sessions"))},
			{Key: "single-ws viewers", Value: fmt.Sprintf("%d", st.Get("ws_sessions"))},
			{Key: "glued viewers", Value: fmt.Sprintf("%d", st.Get("glued_sessions"))},
		})
}

// drainUserActions reads this session's publish-back "user_actions"
// track: each click ripples the shared desktop (visible on the screen
// lane) and is acked on the acks track with the client's timestamp
// echoed, so the viewer measures action→ack RTT.
func drainUserActions(h server.SessionHandle, eng *engine, publishAck func(pubsub.AccessUnit)) {
	var ackSeq uint32
	for {
		au, err := h.InboundRecv(h.Context(), trackUserActions)
		if err != nil {
			return
		}
		var act struct {
			TsUs uint64 `json:"t_us"`
			X    int    `json:"x"`
			Y    int    `json:"y"`
		}
		if err := json.Unmarshal(au.Bytes, &act); err != nil {
			continue
		}
		eng.userAction(act.X, act.Y)
		body, err := json.Marshal(struct {
			Seq  uint32 `json:"seq"`
			TsUs uint64 `json:"t_us"`
		}{au.Seq, act.TsUs})
		if err != nil {
			continue
		}
		publishAck(pubsub.AccessUnit{
			Bytes:    body,
			Keyframe: true,
			PTSMicro: uint64(time.Now().UnixMicro()),
			Seq:      ackSeq,
		})
		ackSeq++
	}
}

func mbpsStr(m float64) string {
	if m == 0 {
		return "unlimited"
	}
	return strings.TrimSuffix(fmt.Sprintf("%.1f", m), ".0") + " Mbps"
}

// pageConfig is the JSON the page (and the bench's glued client)
// bootstraps from.
type pageConfig struct {
	WTURL          string  `json:"wtUrl"`
	Slug           string  `json:"slug"`
	CertHash       string  `json:"certHash"`
	WSURL          string  `json:"wsUrl"`
	GluedFramesURL string  `json:"gluedFramesUrl"`
	GluedEventsURL string  `json:"gluedEventsUrl"`
	GluedSSEURL    string  `json:"gluedSseUrl"`
	Profile        string  `json:"profile"`
	RTTMs          int     `json:"rttMs"`
	Mbps           float64 `json:"mbps"`
	LossPct        float64 `json:"lossPct"`
	FPS            int     `json:"fps"`
	Width          int     `json:"width"`
	Height         int     `json:"height"`
}

// quicrtcSink routes engine events onto the quicrtc tracks. Small
// lanes publish immediately (Publish hands the AU to the broadcaster's
// bounded queues and returns). The screen lane runs the same
// client-receipt protocol as the WS arms — at most one frame between
// receipts, newest replaces pending — implemented with a publish-back
// receipts track instead of an ack message. All three arms therefore
// ship the identical frame strategy; the transports differ only in
// how everything rides the wire.
type quicrtcSink struct {
	screen    *server.Publisher
	reasoning *server.Publisher
	toolcalls *server.Publisher
	telemetry *server.Publisher
	st        *status.Status

	pacer   *framePacer
	mu      sync.Mutex
	lastAck time.Time
}

// freeRunAfter: with no viewer sending receipts (nobody connected on
// this arm), the pacer would starve; fall back to publishing every
// offered frame so the demo works with any subset of panes open.
const freeRunAfter = 2 * time.Second

func newQuicrtcSink(screen, reasoning, toolcalls, telemetry *server.Publisher, st *status.Status) *quicrtcSink {
	q := &quicrtcSink{
		screen: screen, reasoning: reasoning, toolcalls: toolcalls, telemetry: telemetry,
		st: st, pacer: newFramePacer(),
	}
	go q.runScreenPacer()
	return q
}

func (q *quicrtcSink) frameAcked() {
	q.mu.Lock()
	q.lastAck = time.Now()
	q.mu.Unlock()
	q.pacer.ack()
}

func (q *quicrtcSink) runScreenPacer() {
	for range q.pacer.kick {
		if raw := q.pacer.take(); raw != nil {
			var ev event
			decodeSinkFrame(raw, &ev)
			q.publish(q.screen, ev)
		}
	}
}

func (q *quicrtcSink) Deliver(ev event) {
	if ev.lane != laneScreen {
		q.publish(q.laneToPub(ev.lane), ev)
		return
	}
	q.mu.Lock()
	freeRun := time.Since(q.lastAck) > freeRunAfter
	q.mu.Unlock()
	if freeRun {
		q.publish(q.screen, ev)
		q.pacer.ack() // keep pacer state from wedging on stale pending
		return
	}
	q.pacer.offer(encodeSinkFrame(ev))
}

func (q *quicrtcSink) laneToPub(lane int) *server.Publisher {
	switch lane {
	case laneReasoning:
		return q.reasoning
	case laneToolCalls:
		return q.toolcalls
	case laneTelemetry:
		return q.telemetry
	default:
		return nil
	}
}

func (q *quicrtcSink) publish(pub *server.Publisher, ev event) {
	if pub == nil {
		return
	}
	au := pubsub.AccessUnit{
		Bytes:    ev.payload,
		Keyframe: ev.keyframe,
		PTSMicro: ev.ptsMicro,
		Seq:      ev.seq,
	}
	if err := pub.Publish(context.Background(), au); err == nil {
		q.st.Inc("q_bytes", int64(len(ev.payload)))
	}
}

// encode/decodeSinkFrame squeeze an event through the framePacer's
// []byte slot (reusing the WS envelope layout; zero-copy payload).
func encodeSinkFrame(ev event) []byte { return encodeWSEvent(ev) }

func decodeSinkFrame(raw []byte, ev *event) {
	ev.lane = int(raw[0])
	ev.seq = binary.BigEndian.Uint32(raw[1:5])
	ev.ptsMicro = binary.BigEndian.Uint64(raw[5:13])
	ev.keyframe = true
	ev.payload = raw[wsEnvelopeLen:]
}
