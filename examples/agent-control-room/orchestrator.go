package main

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sachinkesiraju/quicrtc/examples/internal/status"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/server"
)

// steerMsg is client→server control on the steer publish-back track.
type steerMsg struct {
	Type   string `json:"type"`             // cancel | inject
	Worker string `json:"worker,omitempty"` // empty = all
	Text   string `json:"text,omitempty"`
	TsUs   uint64 `json:"t_us,omitempty"`
}

// checkpoint is the fork primitive — serializable session state.
type checkpoint struct {
	AtMicro int64             `json:"at_us"`
	Workers map[string]int    `json:"workers"` // worker id → next step index
	Scene   sceneSnapshot     `json:"scene"`
}

type sceneSnapshot struct {
	StepTitle string `json:"step_title"`
	TermLines int    `json:"term_lines"`
	EditorFile string `json:"editor_file"`
	CIVisible bool   `json:"ci_visible"`
	CIPassing bool   `json:"ci_passing"`
	PRURL     string `json:"pr_url"`
}

type orchestrator struct {
	screenPub    *server.Publisher
	reasoningPub *server.Publisher
	toolPub      *server.Publisher
	st           *status.Status

	painter *desktopPainter
	plans   []workerPlan

	mu           sync.Mutex
	scene        scene
	lastMutation time.Time

	seqScreen uint32
	seqToken  uint32
	seqTool   uint32

	steerCh chan steerMsg

	// per-worker cancel; steer cancels without stopping peers.
	workerCancel map[workerID]context.CancelFunc

	// benchSingleStep runs only the first step per worker (for timing).
	benchSingleStep   bool
	benchSkipReasoning bool
	benchSkipMutations bool
	simulateToolDelay time.Duration
	toolDelayOverride map[workerID]time.Duration
	parallelMode      bool

	cancelCount    atomic.Uint32
	toolWorkActive atomic.Int32
	lastSteerUs    atomic.Uint64
	lastSteerKind  atomic.Value // string

	checkpointMu sync.RWMutex
	lastCP       checkpoint
	recorder     func(checkpoint)
}

func newOrchestrator(screen, reasoning, tool *server.Publisher, st *status.Status) *orchestrator {
	o := &orchestrator{
		screenPub:    screen,
		reasoningPub: reasoning,
		toolPub:      tool,
		st:           st,
		painter:      newDesktopPainter(),
		plans:        workerPlans(),
		steerCh:      make(chan steerMsg, 32),
		workerCancel: make(map[workerID]context.CancelFunc),
	}
	resetScene(&o.scene)
	return o
}

func (o *orchestrator) deliverSteer(msg steerMsg) {
	select {
	case o.steerCh <- msg:
	default:
	}
}

func (o *orchestrator) run(ctx context.Context, fps int) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		o.runScreen(ctx, fps)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		o.runSteerLoop(ctx)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		o.runWorkers(ctx)
	}()
	wg.Wait()
}

func (o *orchestrator) runSteerLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-o.steerCh:
			o.applySteer(msg)
		}
	}
}

func (o *orchestrator) applySteer(msg steerMsg) {
	o.lastSteerUs.Store(msg.TsUs)
	o.lastSteerKind.Store(msg.Type)
	switch msg.Type {
	case "cancel":
		o.cancelWorkers(msg.Worker)
		o.cancelCount.Add(1)
	case "inject":
		prefix := "[you] "
		if msg.Worker != "" {
			prefix = fmt.Sprintf("[%s steer] ", msg.Worker)
		}
		o.streamReasoning(context.Background(), prefix+msg.Text)
	}
}

func (o *orchestrator) cancelWorkers(target string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if target == "" {
		for id, cancel := range o.workerCancel {
			if cancel != nil {
				cancel()
			}
			delete(o.workerCancel, id)
		}
		return
	}
	id := workerID(target)
	if cancel, ok := o.workerCancel[id]; ok && cancel != nil {
		cancel()
		delete(o.workerCancel, id)
	}
}

func (o *orchestrator) runScreen(ctx context.Context, fps int) {
	if fps < 4 {
		fps = 4
	}
	tick := time.NewTicker(time.Second / time.Duration(fps))
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		o.mu.Lock()
		png, err := o.painter.Frame(&o.scene)
		o.mu.Unlock()
		if err != nil {
			continue
		}
		seq := o.seqScreen
		o.seqScreen++
		au := pubsub.AccessUnit{
			Bytes: png, Keyframe: true,
			PTSMicro: uint64(time.Now().UnixMicro()), Seq: seq,
		}
		if o.screenPub != nil {
			_ = o.screenPub.Publish(ctx, au)
		}
		o.st.Inc("screen_au", 1)
	}
}

func (o *orchestrator) runWorkers(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		o.runParallelCycle(ctx)
		o.mu.Lock()
		resetScene(&o.scene)
		o.mu.Unlock()
	}
}

func (o *orchestrator) runParallelCycle(ctx context.Context) {
	start := make(map[workerID]int)
	o.checkpointMu.RLock()
	for k, v := range o.lastCP.Workers {
		start[workerID(k)] = v
	}
	o.checkpointMu.RUnlock()

	if o.parallelMode {
		var wg sync.WaitGroup
		for _, plan := range o.plans {
			wg.Add(1)
			go func(p workerPlan) {
				defer wg.Done()
				o.runWorker(ctx, p, start[p.id])
			}(plan)
		}
		wg.Wait()
		return
	}
	for _, plan := range o.plans {
		o.runWorker(ctx, plan, start[plan.id])
	}
}

func (o *orchestrator) runWorker(ctx context.Context, plan workerPlan, from int) {
	for i := from; i < len(plan.steps); i++ {
		if o.benchSingleStep && i > from {
			break
		}
		if ctx.Err() != nil {
			return
		}
		wctx, cancel := context.WithCancel(ctx)
		o.mu.Lock()
		o.workerCancel[plan.id] = cancel
		o.mu.Unlock()

		s := plan.steps[i]
		o.mu.Lock()
		o.scene.stepTitle = fmt.Sprintf("[%s] %s", plan.id, s.title)
		o.mu.Unlock()

		o.saveCheckpoint(plan.id, i)

		if !o.benchSkipReasoning {
			prefix := fmt.Sprintf("[%s] ", plan.id)
			o.streamReasoning(wctx, prefix+s.reasoning)
			if wctx.Err() != nil {
				o.publishTool(wctx, plan.id, s.call, "cancelled")
				return
			}
		}

		o.publishTool(wctx, plan.id, s.call, "running")
		if err := o.simulateToolWork(wctx, plan.id); err != nil {
			o.publishTool(context.Background(), plan.id, s.call, "cancelled")
			o.mu.Lock()
			delete(o.workerCancel, plan.id)
			o.mu.Unlock()
			return
		}
		o.publishTool(wctx, plan.id, s.call, "done")

		if !o.benchSkipMutations {
			for _, m := range s.mutations {
				select {
				case <-wctx.Done():
					o.mu.Lock()
					delete(o.workerCancel, plan.id)
					o.mu.Unlock()
					return
				case <-time.After(m.after):
				}
				o.mu.Lock()
				m.apply(&o.scene)
				o.lastMutation = time.Now()
				o.mu.Unlock()
			}
		}
		o.mu.Lock()
		delete(o.workerCancel, plan.id)
		o.mu.Unlock()
		o.st.Inc("worker_steps", 1)
	}
}

func (o *orchestrator) simulateToolWork(ctx context.Context, id workerID) error {
	d := o.simulateToolDelay
	if o.toolDelayOverride != nil {
		if od, ok := o.toolDelayOverride[id]; ok {
			d = od
		}
	}
	if d <= 0 {
		return nil
	}
	o.toolWorkActive.Add(1)
	defer o.toolWorkActive.Add(-1)
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (o *orchestrator) streamReasoning(ctx context.Context, text string) {
	if text == "" {
		return
	}
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for _, tok := range strings.Fields(text) {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		payload := []byte(tok + " ")
		seq := o.seqToken
		o.seqToken++
		au := pubsub.AccessUnit{
			Bytes: payload, Keyframe: true,
			PTSMicro: uint64(time.Now().UnixMicro()), Seq: seq,
		}
		if o.reasoningPub != nil {
			_ = o.reasoningPub.Publish(ctx, au)
		}
		o.st.Inc("token_au", 1)
	}
}

func (o *orchestrator) publishTool(ctx context.Context, id workerID, call toolCall, state string) {
	body, err := json.Marshal(workerTool{
		Worker: string(id), toolCall: call, State: state,
	})
	if err != nil {
		return
	}
	seq := o.seqTool
	o.seqTool++
	au := pubsub.AccessUnit{
		Bytes: body, Keyframe: true,
		PTSMicro: uint64(time.Now().UnixMicro()), Seq: seq,
		Meta:     map[string]string{"request_id": fmt.Sprintf("%s-%d", id, seq), "worker": string(id)},
	}
	if o.toolPub != nil {
		_ = o.toolPub.Publish(ctx, au)
	}
	o.st.Inc("tool_au", 1)
}

func (o *orchestrator) saveCheckpoint(active workerID, stepIdx int) {
	o.mu.Lock()
	snap := sceneSnapshot{
		StepTitle:  o.scene.stepTitle,
		TermLines:  len(o.scene.term),
		EditorFile: o.scene.editorFile,
		CIVisible:  o.scene.ciVisible,
		CIPassing:  o.scene.ciPassing,
		PRURL:      o.scene.prURL,
	}
	o.mu.Unlock()

	cp := checkpoint{
		AtMicro: time.Now().UnixMicro(),
		Workers: map[string]int{
			string(workerTests): 0,
			string(workerCode):  0,
			string(workerShip):  0,
		},
		Scene: snap,
	}
	cp.Workers[string(active)] = stepIdx

	o.checkpointMu.Lock()
	o.lastCP = cp
	o.checkpointMu.Unlock()

	if o.recorder != nil {
		o.recorder(cp)
	}
}

func (o *orchestrator) forkFrom(cp checkpoint) {
	o.checkpointMu.Lock()
	o.lastCP = cp
	o.checkpointMu.Unlock()
	o.cancelWorkers("")
}

func (o *orchestrator) snapshot() checkpoint {
	o.checkpointMu.RLock()
	defer o.checkpointMu.RUnlock()
	return o.lastCP
}

// sessionTelemetry builds the datagram JSON for one viewer session.
func sessionTelemetry(st *status.Status, orch *orchestrator) []byte {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	cp := orch.snapshot()
	body, _ := json.Marshal(map[string]any{
		"ts_us":        time.Now().UnixMicro(),
		"screen_frames": st.Get("screen_au"),
		"tokens":       st.Get("token_au"),
		"tool_calls":   st.Get("tool_au"),
		"worker_steps": st.Get("worker_steps"),
		"steer_count":  orch.cancelCount.Load(),
		"checkpoint_us": cp.AtMicro,
		"goroutines":   runtime.NumGoroutine(),
	})
	return body
}
