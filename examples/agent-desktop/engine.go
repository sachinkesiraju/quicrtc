// The agent engine: plays the scripted session and fans every event
// out to BOTH transports at the same instant with the same payload
// and the same publish timestamp. Neither side gets a head start —
// the only variable in the comparison is the wire.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/sachinkesiraju/quicrtc/examples/internal/status"
)

// lane identifiers, shared with the WebSocket envelope and the UI.
const (
	laneScreen    = 1
	laneReasoning = 2
	laneToolCalls = 3
	laneTelemetry = 4
)

// event is one emitted AU, delivered identically to every sink.
type event struct {
	lane     int
	seq      uint32
	ptsMicro uint64 // publish wall clock (Unix micros), stamped once
	keyframe bool
	payload  []byte
}

// sink receives the engine's fanout. Implementations must not block:
// each transport owns its own queueing/drop policy, which is part of
// what the demo compares.
type sink interface {
	Deliver(ev event)
}

type engine struct {
	painter *desktopPainter
	steps   []step
	fps     int           // burst frame rate, while the desktop is changing
	pace    time.Duration // extra dwell per step so the demo is watchable
	st      *status.Status

	mu           sync.Mutex
	scene        scene
	sinks        []sink
	lastMutation time.Time

	seqScreen uint32
	seqToken  uint32
	seqTool   uint32
	seqTele   uint32
}

func newEngine(fps int, pace time.Duration, st *status.Status) *engine {
	e := &engine{
		painter: newDesktopPainter(),
		steps:   script(),
		fps:     fps,
		pace:    pace,
		st:      st,
	}
	resetScene(&e.scene)
	return e
}

func (e *engine) addSink(s sink) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sinks = append(e.sinks, s)
}

// userAction registers a viewer click at desktop coordinates: the
// painter draws a ripple there for the next second, and the burst
// window opens so the response is visible promptly on the screen
// lane. Called from every transport's action path.
func (e *engine) userAction(x, y int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.scene.clicks = append(e.scene.clicks, clickMark{x: x, y: y, born: e.painter.tick})
	if len(e.scene.clicks) > 8 {
		e.scene.clicks = e.scene.clicks[len(e.scene.clicks)-8:]
	}
	e.lastMutation = time.Now()
	e.st.Inc("user_actions", 1)
}

func (e *engine) fanout(ev event) {
	e.mu.Lock()
	sinks := e.sinks
	e.mu.Unlock()
	for _, s := range sinks {
		s.Deliver(ev)
	}
}

func nowMicro() uint64 { return uint64(time.Now().UnixMicro()) }

// run starts the four lane producers and blocks until ctx is done.
// The lanes are independent goroutines on purpose: a screen frame
// being encoded or a token being paced never delays the others at
// the source — any cross-lane delay the receiver observes was added
// by the transport.
func (e *engine) run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); e.runScreen(ctx) }()
	go func() { defer wg.Done(); e.runScript(ctx) }()
	go func() { defer wg.Done(); e.runTelemetry(ctx) }()
	wg.Wait()
}

// burstWindow is how long after the last desktop mutation the screen
// keeps capturing at the burst rate before dropping to the idle rate.
const burstWindow = 1500 * time.Millisecond

// runScreen paints and publishes the desktop continuously — the
// "agent's screen is always streaming" signal. The capture rate is
// activity-driven, like a real screen pipeline: full rate (-fps)
// while the desktop is changing (terminal output landing, editor
// edits, CI updates), ~1/3 rate when the scene is static. The bursts
// are what periodically saturate the emulated link; the idle stretches
// let it drain, so the comparison reaches a steady state instead of
// unbounded queue growth.
//
// Every frame is marked keyframe because every PNG IS independently
// decodable — and on quicrtc that flag is load-bearing: stream-per-GOP
// opens a fresh QUIC stream per keyframe and RESETS the previous
// stream, so when the link falls behind, the half-sent stale frame is
// abandoned at the QUIC layer the instant a newer one exists. That is
// the transport-native equivalent of the ack-paced latest-only frame
// sending the steelmanned WS baselines implement by hand.
func (e *engine) runScreen(ctx context.Context) {
	idleFPS := e.fps / 3
	if idleFPS < 2 {
		idleFPS = 2
	}
	timer := time.NewTimer(time.Second / time.Duration(e.fps))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		e.mu.Lock()
		png, err := e.painter.Frame(&e.scene)
		active := time.Since(e.lastMutation) < burstWindow
		e.mu.Unlock()
		fps := idleFPS
		if active {
			fps = e.fps
		}
		timer.Reset(time.Second / time.Duration(fps))
		if err != nil {
			continue
		}
		seq := e.seqScreen
		e.seqScreen++
		e.fanout(event{
			lane: laneScreen, seq: seq, ptsMicro: nowMicro(),
			keyframe: true, payload: png,
		})
		e.st.Inc("screen_au", 1)
		e.st.Inc("screen_bytes", int64(len(png)))
	}
}

// runScript walks the scripted steps in a loop: publish the tool
// call, stream the reasoning token by token (~30 tok/s), and play the
// step's desktop mutations on their own timers.
func (e *engine) runScript(ctx context.Context) {
	for {
		for idx, s := range e.steps {
			if ctx.Err() != nil {
				return
			}
			stepStart := time.Now()

			e.mu.Lock()
			e.scene.stepIdx = idx
			e.scene.stepTitle = s.title
			e.mu.Unlock()

			e.publishToolCall(idx, s.call)

			// Mutations play concurrently with the token stream, the
			// way a real host executes while the model narrates.
			mutDone := make(chan struct{})
			go func(muts []mutation) {
				defer close(mutDone)
				for _, m := range muts {
					select {
					case <-ctx.Done():
						return
					case <-time.After(m.after):
					}
					e.mu.Lock()
					m.apply(&e.scene)
					e.lastMutation = time.Now()
					e.mu.Unlock()
				}
			}(s.mutations)

			e.streamReasoning(ctx, s.reasoning)
			<-mutDone
			e.st.Inc("steps", 1)

			if rem := e.pace - time.Since(stepStart); rem > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(rem):
				}
			}
		}
		e.mu.Lock()
		resetScene(&e.scene)
		e.mu.Unlock()
	}
}

// streamReasoning emits one whitespace token every ~33ms (≈30 tok/s,
// a realistic LLM decode rate). Every token is a self-contained AU.
func (e *engine) streamReasoning(ctx context.Context, text string) {
	tick := time.NewTicker(33 * time.Millisecond)
	defer tick.Stop()
	for _, tok := range strings.Fields(text) {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		e.emitToken([]byte(tok + " "))
	}
	e.emitToken([]byte("\n\n"))
}

func (e *engine) emitToken(payload []byte) {
	seq := e.seqToken
	e.seqToken++
	e.fanout(event{
		lane: laneReasoning, seq: seq, ptsMicro: nowMicro(),
		keyframe: true, payload: payload,
	})
	e.st.Inc("token_au", 1)
}

func (e *engine) publishToolCall(step int, call toolCall) {
	body, err := json.Marshal(struct {
		Step int `json:"step"`
		toolCall
	}{Step: step + 1, toolCall: call})
	if err != nil {
		return
	}
	seq := e.seqTool
	e.seqTool++
	e.fanout(event{
		lane: laneToolCalls, seq: seq, ptsMicro: nowMicro(),
		keyframe: true, payload: body,
	})
	e.st.Inc("tool_au", 1)
}

// runTelemetry emits a 2 Hz counters snapshot — the fire-and-forget
// lane. Small enough to always fit one datagram.
func (e *engine) runTelemetry(ctx context.Context) {
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		// ts_us rides inside the JSON because the quicrtc datagram
		// envelope (4 bytes) has no timestamp field; receivers on both
		// transports read it from the body.
		pts := nowMicro()
		body := fmt.Sprintf(
			`{"ts_us":%d,"step":%d,"screen_frames":%d,"tokens":%d,"tool_calls":%d,"heap_mb":%d,"goroutines":%d}`,
			pts, e.st.Get("steps"), e.st.Get("screen_au"), e.st.Get("token_au"),
			e.st.Get("tool_au"), ms.HeapAlloc/(1<<20), runtime.NumGoroutine(),
		)
		seq := e.seqTele
		e.seqTele++
		e.fanout(event{
			lane: laneTelemetry, seq: seq, ptsMicro: pts,
			keyframe: true, payload: []byte(body),
		})
		e.st.Inc("telemetry_au", 1)
	}
}
