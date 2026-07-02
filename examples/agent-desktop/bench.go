// Headless bench mode: connect a Go client to each of the three arms
// through the same emulated links the browser would use, drive the
// interactive action lane, sever every connection mid-run the way a
// network change would, and print the comparison — per-lane delivery
// latency, action→ack RTT, reconnect time, and data loss.
//
// Latency is one-way publish→receive wall time; action RTT is a full
// client→server→client echo. All clients run in this process on the
// same clock as the publisher, so the numbers are directly comparable
// with no clock-sync caveat.
//
// The drop at the halfway mark closes every arm's connection(s)
// simultaneously and reconnects immediately:
//
//	quicrtc    one re-dial carrying {session, last_seen}; the server
//	           replays the AUs that were in flight from its replay
//	           buffers — no lane loses data.
//	glued      three reconnects; the SSE lane replays tokens from its
//	           hand-rolled ring (?from=seq); frames resume at the
//	           latest frame; tool calls and telemetry in the gap are
//	           gone.
//	single WS  one reconnect; everything in the gap is gone.
//
// Latency samples inside the blackout window (drop → first AU + 1 s)
// are excluded for ALL arms so replayed-late AUs don't pollute the
// steady-state percentiles; the drop's cost is reported in its own
// columns instead.
package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/websocket"

	"github.com/sachinkesiraju/quicrtc/client"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/track"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// ── stats ───────────────────────────────────────────────────────────

type laneStats struct {
	mu      sync.Mutex
	samples []float64 // ms
	seqs    map[uint32]bool
	count   int
	lastAt  time.Time
}

func newLaneStats() *laneStats { return &laneStats{seqs: make(map[uint32]bool)} }

func (l *laneStats) add(latMs float64, seq uint32, inBlackout bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seqs[seq] = true
	l.count++
	l.lastAt = time.Now()
	if !inBlackout {
		l.samples = append(l.samples, latMs)
	}
}

func (l *laneStats) idleFor() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lastAt.IsZero() {
		return time.Hour
	}
	return time.Since(l.lastAt)
}

func (l *laneStats) percentile(p float64) float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.samples) == 0 {
		return 0
	}
	s := make([]float64, len(l.samples))
	copy(s, l.samples)
	sort.Float64s(s)
	return s[int(p*float64(len(s)-1))]
}

func (l *laneStats) n() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.count
}

// lost counts missing seqs between the min and max observed — data
// the arm never delivered (replayed AUs fill their gaps, so arms with
// working resume report 0).
func (l *laneStats) lost() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.seqs) == 0 {
		return 0
	}
	minSeq, maxSeq := uint32(1<<31), uint32(0)
	for s := range l.seqs {
		if s < minSeq {
			minSeq = s
		}
		if s > maxSeq {
			maxSeq = s
		}
	}
	return int(maxSeq-minSeq+1) - len(l.seqs)
}

// armStats is one transport arm's full scoreboard.
type armStats struct {
	name                             string
	screen, tokens, tools, telemetry *laneStats
	actions                          *laneStats // RTT samples

	mu          sync.Mutex
	blackoutTo  time.Time // samples before this are excluded
	reconnAt    time.Time // when the post-outage reconnect began
	reconnectMs float64   // reconnect start → first AU delivered
	conns       int       // connections re-established on reconnect
}

func newArmStats(name string) *armStats {
	return &armStats{
		name:   name,
		screen: newLaneStats(), tokens: newLaneStats(), tools: newLaneStats(),
		telemetry: newLaneStats(), actions: newLaneStats(),
	}
}

func (a *armStats) lane(lane int) *laneStats {
	switch lane {
	case laneScreen:
		return a.screen
	case laneReasoning:
		return a.tokens
	case laneToolCalls:
		return a.tools
	case laneTelemetry:
		return a.telemetry
	default:
		return nil
	}
}

// record logs one delivered AU; it also closes the reconnect
// measurement if this is the first AU after a drop.
func (a *armStats) record(lane int, seq uint32, ptsMicro uint64) {
	now := time.Now()
	a.mu.Lock()
	if !a.reconnAt.IsZero() && a.reconnectMs == 0 {
		a.reconnectMs = float64(now.Sub(a.reconnAt)) / 1e6
		a.blackoutTo = now.Add(time.Second)
	}
	blackout := now.Before(a.blackoutTo)
	a.mu.Unlock()
	if ls := a.lane(lane); ls != nil {
		ls.add(float64(now.UnixMicro()-int64(ptsMicro))/1e3, seq, blackout)
	}
}

// markDrop opens the blackout window; markReconnect starts the
// reconnect stopwatch once the outage has passed, so the metric
// isolates handshakes + resume rather than the (equal) dead air.
func (a *armStats) markDrop() {
	a.mu.Lock()
	a.reconnAt = time.Time{}
	a.reconnectMs = 0
	a.blackoutTo = time.Now().Add(24 * time.Hour) // until first AU lands
	a.mu.Unlock()
}

func (a *armStats) markReconnect() {
	a.mu.Lock()
	a.reconnAt = time.Now()
	a.mu.Unlock()
}

func (a *armStats) recordAck(tsMicro uint64) {
	a.mu.Lock()
	blackout := time.Now().Before(a.blackoutTo)
	a.mu.Unlock()
	a.actions.add(float64(time.Now().UnixMicro()-int64(tsMicro))/1e3, 0, blackout)
}

// ── the arms ────────────────────────────────────────────────────────

// arm is one transport client: connect, send actions, drop+reconnect.
type arm interface {
	connect(ctx context.Context) error
	sendAction(x, y int)
	// dropAndReconnect severs all connections, waits out the outage
	// (the walk between wifi APs — the engine keeps publishing into
	// the gap), then re-establishes them using whatever resume
	// machinery the transport has.
	dropAndReconnect(ctx context.Context, outage time.Duration) error
	close()
}

// benchOutage is how long the emulated network stays dead during the
// mid-run drop. Long enough that ~45 tokens and a few frames are
// published into the gap — the data whose fate differs per arm.
const benchOutage = 1500 * time.Millisecond

// ── quicrtc arm ─────────────────────────────────────────────────────

type quicArm struct {
	shareURL string
	stats    *armStats

	mu        sync.Mutex
	c         *client.Client
	sender    client.Sender
	actionSeq uint32
	cancel    context.CancelFunc
}

func (q *quicArm) connect(ctx context.Context) error {
	return q.dial(ctx, "", nil)
}

func (q *quicArm) dial(ctx context.Context, sessionID string, lastSeen map[string]uint32) error {
	c, err := client.Dial(ctx, q.shareURL, client.Options{
		SessionID:   sessionID,
		LastSeenSeq: lastSeen,
	})
	if err != nil {
		return err
	}
	sender, err := c.Publish(ctx, track.LocalTrack{Name: trackUserActions, Kind: track.KindToolCalls})
	if err != nil {
		_ = c.Close()
		return err
	}
	ackSender, err := c.Publish(ctx, track.LocalTrack{Name: trackFrameAcks, Kind: track.KindTokens})
	if err != nil {
		_ = c.Close()
		return err
	}
	recvCtx, cancel := context.WithCancel(ctx)
	q.mu.Lock()
	q.c, q.sender, q.cancel = c, sender, cancel
	q.mu.Unlock()

	for lane, name := range map[int]string{
		laneReasoning: trackReasoning, laneToolCalls: trackToolCalls,
	} {
		go func(lane int, name string) {
			for {
				au, err := c.RecvOn(recvCtx, name)
				if err != nil {
					return
				}
				q.stats.record(lane, au.Seq, au.PTSMicro)
			}
		}(lane, name)
	}
	go func() { // screen + frame receipts
		var ackSeq uint32
		for {
			au, err := c.RecvOn(recvCtx, trackScreen)
			if err != nil {
				return
			}
			q.stats.record(laneScreen, au.Seq, au.PTSMicro)
			_ = ackSender.Send(recvCtx, pubsub.AccessUnit{
				Bytes: []byte{1}, Keyframe: true,
				PTSMicro: uint64(time.Now().UnixMicro()), Seq: ackSeq,
			})
			ackSeq++
		}
	}()
	go func() { // acks
		for {
			au, err := c.RecvOn(recvCtx, trackAcks)
			if err != nil {
				return
			}
			var ack struct {
				TsUs uint64 `json:"t_us"`
			}
			if json.Unmarshal(au.Bytes, &ack) == nil && ack.TsUs > 0 {
				q.stats.recordAck(ack.TsUs)
			}
		}
	}()
	go func() { // telemetry datagrams
		for {
			raw, err := c.ReceiveDatagram(recvCtx)
			if err != nil {
				return
			}
			_, seq, payload, err := wire.DecodeDatagram(raw)
			if err != nil {
				continue
			}
			if pts, ok := telemetryTS(payload); ok {
				q.stats.record(laneTelemetry, uint32(seq), pts)
			}
		}
	}()
	return nil
}

func (q *quicArm) sendAction(x, y int) {
	q.mu.Lock()
	sender := q.sender
	seq := q.actionSeq
	q.actionSeq++
	q.mu.Unlock()
	if sender == nil {
		return
	}
	body, err := json.Marshal(struct {
		TsUs uint64 `json:"t_us"`
		X    int    `json:"x"`
		Y    int    `json:"y"`
	}{uint64(time.Now().UnixMicro()), x, y})
	if err != nil {
		return
	}
	_ = sender.Send(context.Background(), pubsub.AccessUnit{
		Bytes: body, Keyframe: true,
		PTSMicro: uint64(time.Now().UnixMicro()), Seq: seq,
	})
}

func (q *quicArm) dropAndReconnect(ctx context.Context, outage time.Duration) error {
	q.mu.Lock()
	c, cancel := q.c, q.cancel
	q.mu.Unlock()
	sessionID := c.SessionID()
	lastSeen := c.LastSeenSeq()
	q.stats.markDrop()
	cancel()
	_ = c.Close()
	time.Sleep(outage)
	q.stats.markReconnect()
	// One re-dial: HELLO carries {session, last_seen}; the server
	// splices replayed AUs ahead of live delivery.
	return q.dial(ctx, sessionID, lastSeen)
}

func (q *quicArm) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.cancel != nil {
		q.cancel()
	}
	if q.c != nil {
		_ = q.c.Close()
	}
}

// ── shared WS reader for the baseline arms ──────────────────────────

// runWSReader drains one baseline socket, dispatching frames (with
// acks), acks, and data lanes. Returns when the socket dies.
func runWSReader(ws *websocket.Conn, stats *armStats, wantFrameAck bool) {
	for {
		var msg []byte
		if err := websocket.Message.Receive(ws, &msg); err != nil {
			return
		}
		if len(msg) < wsEnvelopeLen {
			continue
		}
		lane := int(msg[0])
		seq := binary.BigEndian.Uint32(msg[1:5])
		pts := binary.BigEndian.Uint64(msg[5:13])
		switch lane {
		case laneAction:
			stats.recordAck(pts)
		case laneScreen:
			stats.record(lane, seq, pts)
			if wantFrameAck {
				_, _ = ws.Write(encodeWSMessage(laneFrameAck, seq, 0, nil))
			}
		default:
			stats.record(lane, seq, pts)
		}
	}
}

func wsDial(url string) (*websocket.Conn, error) {
	ws, err := websocket.Dial(url, "", "http://127.0.0.1/")
	if err != nil {
		return nil, err
	}
	ws.PayloadType = websocket.BinaryFrame
	return ws, nil
}

func sendWSAction(ws *websocket.Conn, seq uint32, x, y int) {
	if ws == nil {
		return
	}
	body, err := json.Marshal(struct {
		X int `json:"x"`
		Y int `json:"y"`
	}{x, y})
	if err != nil {
		return
	}
	_, _ = ws.Write(encodeWSMessage(laneAction, seq, uint64(time.Now().UnixMicro()), body))
}

// ── single-WebSocket arm ────────────────────────────────────────────

type singleWSArm struct {
	url   string
	stats *armStats

	mu        sync.Mutex
	ws        *websocket.Conn
	actionSeq uint32
}

func (a *singleWSArm) connect(_ context.Context) error {
	ws, err := wsDial(a.url)
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.ws = ws
	a.mu.Unlock()
	go runWSReader(ws, a.stats, true)
	return nil
}

func (a *singleWSArm) sendAction(x, y int) {
	a.mu.Lock()
	ws := a.ws
	seq := a.actionSeq
	a.actionSeq++
	a.mu.Unlock()
	sendWSAction(ws, seq, x, y)
}

func (a *singleWSArm) dropAndReconnect(ctx context.Context, outage time.Duration) error {
	a.mu.Lock()
	ws := a.ws
	a.mu.Unlock()
	a.stats.markDrop()
	if ws != nil {
		_ = ws.Close()
	}
	time.Sleep(outage)
	a.stats.markReconnect()
	return a.connect(ctx)
}

func (a *singleWSArm) close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.ws != nil {
		_ = a.ws.Close()
	}
}

// ── glued arm ───────────────────────────────────────────────────────

type gluedArm struct {
	framesURL, eventsURL, sseURL string
	stats                        *armStats

	mu        sync.Mutex
	frames    *websocket.Conn
	events    *websocket.Conn
	sseCancel context.CancelFunc
	lastToken uint32
	actionSeq uint32
}

func (a *gluedArm) connect(ctx context.Context) error {
	frames, err := wsDial(a.framesURL)
	if err != nil {
		return err
	}
	events, err := wsDial(a.eventsURL)
	if err != nil {
		_ = frames.Close()
		return err
	}
	sseCtx, cancel := context.WithCancel(ctx)
	a.mu.Lock()
	from := a.lastToken
	a.frames, a.events, a.sseCancel = frames, events, cancel
	a.mu.Unlock()

	go runWSReader(frames, a.stats, true)
	go runWSReader(events, a.stats, false)
	go a.runSSE(sseCtx, from)
	return nil
}

// runSSE consumes the token stream, resuming from `from` (the
// hand-rolled ring-buffer replay).
func (a *gluedArm) runSSE(ctx context.Context, from uint32) {
	url := a.sseURL
	if from > 0 {
		url = fmt.Sprintf("%s?from=%d", url, from)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64*1024), 64*1024)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var ev struct {
			Seq  uint32 `json:"seq"`
			TsUs uint64 `json:"ts_us"`
		}
		if err := json.Unmarshal([]byte(line[6:]), &ev); err != nil {
			continue
		}
		a.stats.record(laneReasoning, ev.Seq, ev.TsUs)
		a.mu.Lock()
		if ev.Seq > a.lastToken {
			a.lastToken = ev.Seq
		}
		a.mu.Unlock()
	}
}

func (a *gluedArm) sendAction(x, y int) {
	a.mu.Lock()
	ws := a.events
	seq := a.actionSeq
	a.actionSeq++
	a.mu.Unlock()
	sendWSAction(ws, seq, x, y)
}

func (a *gluedArm) dropAndReconnect(ctx context.Context, outage time.Duration) error {
	a.mu.Lock()
	frames, events, cancel := a.frames, a.events, a.sseCancel
	a.mu.Unlock()
	a.stats.markDrop()
	if cancel != nil {
		cancel()
	}
	if frames != nil {
		_ = frames.Close()
	}
	if events != nil {
		_ = events.Close()
	}
	time.Sleep(outage)
	a.stats.markReconnect()
	// Three reconnect dances; the SSE lane resumes from lastToken.
	return a.connect(ctx)
}

func (a *gluedArm) close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.sseCancel != nil {
		a.sseCancel()
	}
	if a.frames != nil {
		_ = a.frames.Close()
	}
	if a.events != nil {
		_ = a.events.Close()
	}
}

// ── orchestration ───────────────────────────────────────────────────

func runBench(ctx context.Context, shareURL string, cfg pageConfig, dur time.Duration) error {
	fmt.Printf("\nbench: three arms, %v sampling (3s warmup), simultaneous network drop at the midpoint…\n", dur)

	benchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	qStats := newArmStats("quicrtc")
	wStats := newArmStats("single WebSocket")
	gStats := newArmStats("glued stack")

	arms := []struct {
		arm   arm
		stats *armStats
		conns int
	}{
		{&quicArm{shareURL: shareURL, stats: qStats}, qStats, 1},
		{&singleWSArm{url: cfg.WSURL, stats: wStats}, wStats, 1},
		{&gluedArm{framesURL: cfg.GluedFramesURL, eventsURL: cfg.GluedEventsURL, sseURL: cfg.GluedSSEURL, stats: gStats}, gStats, 3},
	}
	for _, a := range arms {
		if err := a.arm.connect(benchCtx); err != nil {
			return fmt.Errorf("%s connect: %w", a.stats.name, err)
		}
		a.stats.conns = a.conns
		defer a.arm.close()
	}

	// Warmup gate: exclude the first 3s from stats via blackout.
	warmEnd := time.Now().Add(3 * time.Second)
	for _, a := range arms {
		a.stats.mu.Lock()
		a.stats.blackoutTo = warmEnd
		a.stats.mu.Unlock()
	}

	// Interactive action lane: the same click goes to all three arms
	// at the same instant, twice a second.
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	go func() {
		tick := time.NewTicker(500 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-benchCtx.Done():
				return
			case <-tick.C:
			}
			x, y := 40+rng.Intn(deskW-80), topBarH+20+rng.Intn(deskH-topBarH-80)
			for _, a := range arms {
				a.arm.sendAction(x, y)
			}
		}
	}()

	// First half → drop → second half. The drop waits for a moment
	// when tokens are actively streaming (deterministic: otherwise
	// whether the gap contains data depends on where the script's
	// pauses happen to fall).
	half := dur / 2
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(3*time.Second + half):
	}
	waitUntil := time.Now().Add(15 * time.Second)
	for time.Now().Before(waitUntil) && qStats.tokens.idleFor() > 200*time.Millisecond {
		time.Sleep(50 * time.Millisecond)
	}
	fmt.Printf("bench: dropping every arm's connection(s) mid-token-stream (%v outage, then reconnect)…\n", benchOutage)
	var wg sync.WaitGroup
	for _, a := range arms {
		wg.Add(1)
		go func(a arm, name string) {
			defer wg.Done()
			if err := a.dropAndReconnect(benchCtx, benchOutage); err != nil {
				fmt.Printf("bench: %s reconnect failed: %v\n", name, err)
			}
		}(a.arm, a.stats.name)
	}
	wg.Wait()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(half):
	}
	cancel()

	printBenchTable(qStats, wStats, gStats)
	return nil
}

// telemetryTS extracts "ts_us" from the telemetry JSON without a full
// parse dependency in the hot path.
func telemetryTS(body []byte) (uint64, bool) {
	const key = `"ts_us":`
	i := strings.Index(string(body), key)
	if i < 0 {
		return 0, false
	}
	i += len(key)
	var v uint64
	seen := false
	for ; i < len(body) && body[i] >= '0' && body[i] <= '9'; i++ {
		v = v*10 + uint64(body[i]-'0')
		seen = true
	}
	return v, seen
}

func printBenchTable(q, w, g *armStats) {
	fmt.Printf("\n── delivery latency, publish → receive, steady state (one-way, ms) ──────────────\n")
	fmt.Printf("%-18s %17s %17s %17s\n", "lane  (p50 / p99)", "single WS", "glued stack", "quicrtc")
	row := func(name string, pick func(*armStats) *laneStats) {
		f := func(a *armStats) string {
			ls := pick(a)
			if ls.n() == 0 {
				return "–"
			}
			return fmt.Sprintf("%.0f / %.0f", ls.percentile(0.50), ls.percentile(0.99))
		}
		fmt.Printf("%-18s %17s %17s %17s\n", name, f(w), f(g), f(q))
	}
	row("action → ack RTT", func(a *armStats) *laneStats { return a.actions })
	row("reasoning tokens", func(a *armStats) *laneStats { return a.tokens })
	row("tool calls", func(a *armStats) *laneStats { return a.tools })
	row("telemetry", func(a *armStats) *laneStats { return a.telemetry })
	row("screen frames", func(a *armStats) *laneStats { return a.screen })

	fmt.Printf("\n── the mid-run network drop (%v dead air, all arms at once) ──────────────────\n", benchOutage)
	fmt.Printf("%-26s %17s %17s %17s\n", "", "single WS", "glued stack", "quicrtc")
	fmt.Printf("%-26s %17d %17d %17d\n", "connections to re-open", w.conns, g.conns, q.conns)
	fmt.Printf("%-26s %16.0fms %16.0fms %16.0fms\n", "reconnect → first data", w.reconnectMs, g.reconnectMs, q.reconnectMs)
	loss := func(a *armStats) string {
		return fmt.Sprintf("%d tok / %d telem", a.tokens.lost(), a.telemetry.lost())
	}
	fmt.Printf("%-26s %17s %17s %17s\n", "data lost in the gap", loss(w), loss(g), loss(q))
	fmt.Println("\n(same engine, same events, same timestamps, one emulated link per arm;")
	fmt.Println(" glued arm's token replay is hand-rolled SSE ?from= — quicrtc's is the transport)")
}
