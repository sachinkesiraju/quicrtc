// cua-live/main — a runnable computer-use agent demo over quicrtc.
//
// An agent loop drives a Brain (scripted -fake, or Claude computer-use
// -live) and fans each turn's output onto four quicrtc tracks carried
// on one QUIC connection to the browser viewer:
//
//	screen     KindVideo      real PNG screenshots, stream-per-GOP
//	reasoning  KindTokens     the model's thoughts, token-by-token
//	toolcalls  KindToolCalls  the action it took, one bidi stream/call
//	telemetry  datagrams      per-session counters, fire-and-forget
//
// Modes:
//
//	-fake   scripted Brain + synthesized frames. No external runtime
//	        deps: no Chrome, no API key, no network. Runs anywhere.
//	        Exercises the transport with agent-shaped traffic.
//
//	-live   Claude computer-use over raw net/http driving a real
//	        Chromium via chromedp. Requires ANTHROPIC_API_KEY + a Chrome
//	        binary + network. Build with: go build -tags cualive ./...
//	        and run with -live. Does NOT run in CI. (Without the build
//	        tag, -live prints a clear message and exits.)
//
// Run the fake demo:
//
//	go run ./examples/cua-live -fake
//	# prints: share: https://127.0.0.1:NNNN/wt#slug=...&hash=...
//	# point ts-sdk/examples/viewer/ at the printed share URL.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"time"

	"github.com/sachinkesiraju/quicrtc/examples/internal/status"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/record"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/track"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// Track names are constants so the kind-agnostic viewer
// (ts-sdk/examples/viewer/) matches them exactly. The viewer renders a
// panel per track keyed on Kind, so screen→canvas, reasoning→text,
// toolcalls→event list, and telemetry datagrams→gauges all appear
// automatically.
const (
	trackScreen    = "screen"
	trackReasoning = "reasoning"
	trackToolCalls = "toolcalls"

	// telemetryTrackID is the 1-byte demux key in the datagram envelope
	// (wire.EncodeDatagram). The viewer's decodeDatagram filters on it.
	telemetryTrackID uint8 = 1

	// Screen dimensions. Matched between the SDP advertisement and the
	// synthesized / captured frames so the viewer scales correctly.
	screenWidth  = 1024
	screenHeight = 640
)

func main() {
	var (
		fake       = flag.Bool("fake", false, "scripted brain + synthesized frames; no Chrome, no API key, no network")
		live       = flag.Bool("live", false, "Claude computer-use over a real Chromium (needs ANTHROPIC_API_KEY + Chrome; build -tags cualive)")
		bind       = flag.String("bind", "127.0.0.1:0", "UDP bind address")
		stepEach   = flag.Duration("step", 1200*time.Millisecond, "minimum wall-time per agent turn (paces the demo so it's watchable)")
		goalFlag   = flag.String("goal", "", "task for the agent (live mode); empty uses a built-in default")
		startURL   = flag.String("url", "https://news.ycombinator.com", "initial page to open (live mode)")
		model      = flag.String("model", "claude-sonnet-4-20250514", "Anthropic model id (live mode; computer-use beta)")
		recordPath = flag.String("record", "", "also record the session to this .qrtc file; replay/scrub it with examples/replay")
	)
	flag.Parse()

	if *fake == *live {
		log.Fatal("pick exactly one of -fake or -live (see -h)")
	}

	st := status.New("cua-live")

	// Build the Brain and its screen source. In fake mode the brain
	// also synthesizes the screen frames; in live mode chromedp drives
	// the browser and captures real screenshots.
	var (
		brain  Brain
		screen screenSource
		err    error
	)
	if *fake {
		fb := NewFakeBrain(screenWidth, screenHeight)
		brain = fb
		screen = &fakeScreen{fb: fb}
	} else {
		brain, screen, err = newLiveBrain(liveConfig{
			Goal:     *goalFlag,
			StartURL: *startURL,
			Model:    *model,
			Width:    screenWidth,
			Height:   screenHeight,
		})
		if err != nil {
			log.Fatalf("live mode: %v", err)
		}
	}
	defer brain.Close()

	// Stand up the server. SDP advertises image/png at our screen
	// dimensions — the screen track ships real PNG bytes, so the codec
	// claim matches what the viewer decodes and draws.
	host, _, _ := net.SplitHostPort(*bind)
	if host == "" {
		host = "127.0.0.1"
	}
	srv, err := server.New(server.Config{
		Addr:         *bind,
		CertExtraIPs: []net.IP{net.ParseIP(host)},
		ExternalHost: host,
		SDP: wire.SDP{
			Codec: "image/png", Width: screenWidth, Height: screenHeight, FPS: 5,
		},
		OnSession: func(h server.SessionHandle) {
			st.Inc("sessions", 1)
			// One telemetry goroutine per accepted session, bound to
			// the session's lifetime. Fire-and-forget counters over
			// QUIC datagrams — the fourth lane.
			go pumpTelemetry(h, st)
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Register the three publish-out tracks BEFORE serving so they
	// appear in the handshake ANNOUNCE list. Each gets an explicit
	// Kind so feed/pump.go dispatches the right wire shape per lane.
	screenPub := srv.AddTrackSpec(server.TrackSpec{Name: trackScreen, Kind: track.KindVideo})
	reasoningPub := srv.AddTrackSpec(server.TrackSpec{Name: trackReasoning, Kind: track.KindTokens})
	toolPub := srv.AddTrackSpec(server.TrackSpec{Name: trackToolCalls, Kind: track.KindToolCalls})

	// Optionally record the session. AttachRecorder installs a sink on
	// every track's broadcaster, so the .qrtc captures exactly the AUs
	// subscribers receive — all on one capture clock, which is what makes
	// the replay scrubber's cross-lane reconstruction exact. Export it for
	// the browser scrubber with:
	//   go run ./examples/replay -file <path> -json -payloads > examples/replay/viewer/bundle.json
	if *recordPath != "" {
		rec, err := record.NewFileRecorder(*recordPath)
		if err != nil {
			log.Fatalf("record: %v", err)
		}
		detach := srv.AttachRecorder(rec)
		defer func() { detach(); _ = rec.Close() }()
		fmt.Printf("recording to %s — scrub with: go run ./examples/replay -file %s\n", *recordPath, *recordPath)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	go func() {
		if err := srv.ListenAndServe(ctx); err != nil && ctx.Err() == nil {
			log.Printf("server: %v", err)
		}
	}()
	for srv.Addr() == *bind && strings.HasSuffix(*bind, ":0") {
		time.Sleep(10 * time.Millisecond)
	}

	mode := "fake"
	if *live {
		mode = "live"
	}
	fmt.Println("share:", srv.ShareLink())
	fmt.Printf("(mode=%s, goal=%q)\n", mode, brain.Goal())
	fmt.Println("point ts-sdk/examples/viewer/ at the share URL above")
	fmt.Println("(Ctrl-C to stop)")

	// Keep a live status line while the agent loop runs.
	stopTick := st.Tick(250*time.Millisecond, func(l *status.Line) {
		l.Field("sessions", "%d", st.Get("sessions"))
		l.Raw("step=%d", st.Get("steps"))
		l.Raw("screen=%d", st.Get("screen_au"))
		l.Raw("tokens=%d", st.Get("token_au"))
		l.Raw("actions=%d", st.Get("action_au"))
		l.Raw("telemetry=%d", st.Get("telemetry_dg"))
	})

	// Run the agent loop. It publishes onto all three streamed tracks;
	// the telemetry datagram lane is driven per-session in OnSession.
	loop := &agentLoop{
		brain:        brain,
		screen:       screen,
		screenPub:    screenPub,
		reasoningPub: reasoningPub,
		toolPub:      toolPub,
		stepEach:     *stepEach,
		st:           st,
	}
	loop.run(ctx)

	stopTick()
	st.Done(
		"cua-live finished — four agent lanes on one QUIC connection",
		[]status.Field{
			{Key: "uptime", Value: st.Uptime().Truncate(time.Second).String()},
			{Key: "mode", Value: mode},
			{Key: "sessions", Value: fmt.Sprintf("%d", st.Get("sessions"))},
			{Key: "agent steps", Value: fmt.Sprintf("%d", st.Get("steps"))},
			{Key: "screen frames", Value: fmt.Sprintf("%s  %s",
				status.HumanCount(st.Get("screen_au")),
				status.HumanBytes(st.Get("screen_bytes")))},
			{Key: "reasoning tokens", Value: status.HumanCount(st.Get("token_au"))},
			{Key: "tool calls", Value: status.HumanCount(st.Get("action_au"))},
			{Key: "telemetry datagrams", Value: status.HumanCount(st.Get("telemetry_dg"))},
		})
}

// screenSource produces the screen PNG that follows an executed action.
// fake mode synthesizes it; live mode executes the action in Chromium
// and captures a real screenshot. step is the 0-based turn index;
// keyframe asks the source to mark this frame independently decodable.
type screenSource interface {
	// Frame executes act against the host (no-op for fake) and returns
	// the post-action screenshot as PNG bytes.
	Frame(ctx context.Context, step int, act Action, keyframe bool) ([]byte, error)
}

// fakeScreen adapts FakeBrain's synthesized painter to screenSource.
type fakeScreen struct{ fb *FakeBrain }

func (f *fakeScreen) Frame(_ context.Context, step int, act Action, keyframe bool) ([]byte, error) {
	return f.fb.Frame(step, act, keyframe)
}

// agentLoop walks the Brain step by step and fans each turn onto the
// right lane.
type agentLoop struct {
	brain  Brain
	screen screenSource

	screenPub    *server.Publisher
	reasoningPub *server.Publisher
	toolPub      *server.Publisher

	stepEach time.Duration
	st       *status.Status

	seqScreen uint32
	seqToken  uint32
	seqAction uint32
}

// run drives the loop until ctx is cancelled or the Brain errors. Each
// iteration:
//  1. ask the Brain for the next step (reasoning + action),
//  2. stream the reasoning token-by-token on the reasoning lane,
//  3. publish the action as one tool-call AU,
//  4. execute the action + capture the resulting screen frame, publish
//     it on the screen lane,
//  5. pace to stepEach so the demo is watchable.
//
// A turn's work runs on multiple lanes that don't block each other: a
// big screen frame in flight can't stall the next reasoning token.
func (a *agentLoop) run(ctx context.Context) {
	var lastShot []byte
	step := 0
	for {
		if ctx.Err() != nil {
			return
		}
		turnStart := time.Now()

		s, err := a.brain.NextStep(ctx, lastShot)
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("brain: %v", err)
			}
			return
		}
		a.st.Inc("steps", 1)

		// (2) reasoning → tokens lane, token by token. Splitting on
		// whitespace gives the viewer a believable token cadence; each
		// token is an independent AU (Keyframe:true) on the persistent
		// low-latency uni stream.
		a.streamReasoning(ctx, s.Reasoning)

		// (3) action → toolcalls lane. One JSON AU = one logical call,
		// carried on its own bidi stream so parallel calls wouldn't
		// HOL-block (here calls are serial, but the wire shape is the
		// teaching point). request_id in Meta lets a viewer correlate.
		a.publishAction(ctx, s.Action)

		// (4) execute the action + capture the post-action screen, then
		// publish it on the screen lane. Keyframe every 5th frame (and
		// always on the first) so a late-joining viewer can resync.
		keyframe := a.seqScreen%5 == 0
		shot, err := a.screen.Frame(ctx, step, s.Action, keyframe)
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("screen: %v", err)
			}
			return
		}
		lastShot = shot
		a.publishScreen(ctx, shot, keyframe)

		step++
		if s.Done {
			// Brief pause on the final frame so the viewer settles on
			// the completed state, then exit.
			select {
			case <-ctx.Done():
			case <-time.After(2 * time.Second):
			}
			return
		}

		// (5) pace the turn.
		if rem := a.stepEach - time.Since(turnStart); rem > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(rem):
			}
		}
	}
}

// streamReasoning publishes one reasoning string as a sequence of
// whitespace-delimited token AUs on the reasoning (Tokens) lane, paced
// to ~25 Hz so the viewer's text panel fills in visibly.
func (a *agentLoop) streamReasoning(ctx context.Context, text string) {
	if text == "" {
		return
	}
	tick := time.NewTicker(40 * time.Millisecond)
	defer tick.Stop()
	for _, tok := range strings.Fields(text) {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		payload := []byte(tok + " ")
		au := pubsub.AccessUnit{
			Bytes:    payload,
			Keyframe: true, // every token is self-contained
			PTSMicro: uint64(time.Now().UnixMicro()),
			Seq:      a.seqToken,
		}
		if err := a.reasoningPub.Publish(ctx, au); err != nil {
			return
		}
		a.st.Inc("token_au", 1)
		a.st.Inc("token_bytes", int64(len(payload)))
		a.seqToken++
	}
	// A newline AU so consecutive turns don't run together in the panel.
	_ = a.reasoningPub.Publish(ctx, pubsub.AccessUnit{
		Bytes: []byte("\n\n"), Keyframe: true,
		PTSMicro: uint64(time.Now().UnixMicro()), Seq: a.seqToken,
	})
	a.seqToken++
}

// publishAction publishes one Action as a JSON AU on the toolcalls
// (ToolCalls) lane. request_id in Meta is the v1.0.2 correlation
// convention so a viewer / responder could match a reply to this call.
func (a *agentLoop) publishAction(ctx context.Context, act Action) {
	body, err := json.Marshal(act)
	if err != nil {
		return
	}
	au := pubsub.AccessUnit{
		Bytes:    body,
		Keyframe: true,
		PTSMicro: uint64(time.Now().UnixMicro()),
		Seq:      a.seqAction,
		Meta:     map[string]string{"request_id": fmt.Sprintf("act-%d", a.seqAction)},
	}
	if err := a.toolPub.Publish(ctx, au); err != nil {
		return
	}
	a.st.Inc("action_au", 1)
	a.st.Inc("action_bytes", int64(len(body)))
	a.seqAction++
}

// publishScreen publishes one screenshot as a video AU on the screen
// (Video) lane.
func (a *agentLoop) publishScreen(ctx context.Context, png []byte, keyframe bool) {
	au := pubsub.AccessUnit{
		Bytes:    png,
		Keyframe: keyframe,
		PTSMicro: uint64(time.Now().UnixMicro()),
		Seq:      a.seqScreen,
	}
	if err := a.screenPub.Publish(ctx, au); err != nil {
		return
	}
	a.st.Inc("screen_au", 1)
	a.st.Inc("screen_bytes", int64(len(png)))
	a.seqScreen++
}

// pumpTelemetry sends per-session agent telemetry over QUIC datagrams
// at 2 Hz — the fourth lane. Each datagram uses wire.EncodeDatagram's
// 4-byte envelope ([type][trackID][2B seq]) so the viewer's
// decodeDatagram filters by trackID and renders the JSON body as
// gauges. Bound to the session's context so it exits on disconnect.
func pumpTelemetry(h server.SessionHandle, st *status.Status) {
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	var seq uint16
	var dst []byte
	for {
		select {
		case <-h.Context().Done():
			return
		case <-tick.C:
		}
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		body := []byte(fmt.Sprintf(
			`{"step":%d,"screen_frames":%d,"tokens":%d,"tool_calls":%d,"heap_mb":%d,"goroutines":%d}`,
			st.Get("steps"),
			st.Get("screen_au"),
			st.Get("token_au"),
			st.Get("action_au"),
			ms.HeapAlloc/(1<<20),
			runtime.NumGoroutine(),
		))
		// Reuse dst across sends. EncodeDatagram appends, so reset to
		// zero length each time.
		dst = dst[:0]
		env, err := wire.EncodeDatagram(dst, telemetryTrackID, seq, body)
		if err != nil {
			continue
		}
		dst = env
		if err := h.SendDatagram(env); err != nil {
			return
		}
		st.Inc("telemetry_dg", 1)
		st.Inc("telemetry_bytes", int64(len(env)))
		seq++
	}
}
