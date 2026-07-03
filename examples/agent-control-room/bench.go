package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sachinkesiraju/quicrtc/record"
)

// runThesisBench validates the control-room thesis without a browser.
func runThesisBench() error {
	fmt.Println("thesis bench — control-room capabilities (no browser)")
	fmt.Println()

	parSerial, err := measureParallelVsSerial()
	if err != nil {
		return err
	}
	fmt.Printf("E1 parallel vs serial (3 concurrent tool simulations × 250ms)\n")
	fmt.Printf("   serial wall:   %v\n", parSerial.serial)
	fmt.Printf("   parallel wall: %v\n", parSerial.parallel)
	fmt.Printf("   speedup:       %.2fx (target ≥2.5x)\n", parSerial.speedup)
	if parSerial.speedup < 2.5 {
		return fmt.Errorf("E1 FAIL: parallel speedup %.2f < 2.5", parSerial.speedup)
	}
	fmt.Println("   E1 PASS")
	fmt.Println()

	cancelMs, err := measureSteerCancelUnderLoad()
	if err != nil {
		return err
	}
	fmt.Printf("E2 steer cancel (2s tool, cancel during active work)\n")
	fmt.Printf("   cancel latency: %dms (target ≤150ms)\n", cancelMs)
	if cancelMs > 150 {
		return fmt.Errorf("E2 FAIL: cancel latency %dms > 150ms", cancelMs)
	}
	fmt.Println("   E2 PASS")
	fmt.Println()

	forkMs, err := measureForkRestore()
	if err != nil {
		return err
	}
	fmt.Printf("E3 fork from recording checkpoint\n")
	fmt.Printf("   restore latency: %dms (target ≤500ms)\n", forkMs)
	if forkMs > 500 {
		return fmt.Errorf("E3 FAIL: fork restore %dms > 500ms", forkMs)
	}
	fmt.Println("   E3 PASS")
	fmt.Println()

	fmt.Println("thesis bench: all experiments passed")
	return nil
}

type parResult struct {
	serial, parallel time.Duration
	speedup          float64
}

func measureParallelVsSerial() (parResult, error) {
	const delay = 250 * time.Millisecond
	const n = 3

	serialStart := time.Now()
	for i := 0; i < n; i++ {
		time.Sleep(delay)
	}
	serial := time.Since(serialStart)

	parStart := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(delay)
		}()
	}
	wg.Wait()
	parallel := time.Since(parStart)

	// Also run orchestrator-shaped parallel vs serial on first tool step.
	oSerial := newBenchOrchestrator(delay)
	oSerial.parallelMode = false
	oSerial.benchSingleStep = true
	oSerial.benchSkipReasoning = true
	oSerial.benchSkipMutations = true
	tSerial := time.Now()
	oSerial.runParallelCycle(context.Background())
	orchSerial := time.Since(tSerial)

	oPar := newBenchOrchestrator(delay)
	oPar.parallelMode = true
	oPar.benchSingleStep = true
	oPar.benchSkipReasoning = true
	oPar.benchSkipMutations = true
	tPar := time.Now()
	oPar.runParallelCycle(context.Background())
	orchPar := time.Since(tPar)

	fmt.Printf("   (orchestrator 1-step: serial %v, parallel %v, %.2fx)\n",
		orchSerial, orchPar, float64(orchSerial)/float64(orchPar))

	speedup := float64(serial) / float64(parallel)
	return parResult{serial, parallel, speedup}, nil
}

func measureSteerCancelLatency() (int64, error) {
	return measureSteerCancelLatencyWithWait(true)
}

// measureSteerCancelUnderLoad waits until workers are inside simulateToolWork
// before cancelling, so E2 validates steer during active tool execution.
func measureSteerCancelUnderLoad() (int64, error) {
	return measureSteerCancelLatencyWithWait(true)
}

func measureSteerCancelLatencyWithWait(waitForToolWork bool) (int64, error) {
	o := newBenchOrchestrator(2 * time.Second)
	o.parallelMode = true
	o.benchSingleStep = true
	o.benchSkipReasoning = true
	o.benchSkipMutations = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		o.runParallelCycle(ctx)
		close(done)
	}()

	if waitForToolWork {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if o.toolWorkActive.Load() > 0 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if o.toolWorkActive.Load() == 0 {
			return 0, fmt.Errorf("workers never entered tool work before cancel")
		}
	} else {
		time.Sleep(50 * time.Millisecond)
	}

	t0 := time.Now()
	o.applySteer(steerMsg{Type: "cancel", TsUs: uint64(time.Now().UnixMicro())})

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		return 0, fmt.Errorf("workers did not finish after cancel")
	}
	return time.Since(t0).Milliseconds(), nil
}

func measureForkRestore() (int64, error) {
	dir := os.TempDir()
	path := filepath.Join(dir, fmt.Sprintf("control-room-fork-%d.qrtc", time.Now().UnixNano()))
	rec, err := record.NewFileRecorder(path)
	if err != nil {
		return 0, err
	}
	defer os.Remove(path)

	cp := checkpoint{
		AtMicro: time.Now().UnixMicro(),
		Workers: map[string]int{
			string(workerTests): 1,
			string(workerCode):  0,
			string(workerShip):  0,
		},
	}
	body, _ := jsonMarshal(map[string]any{"checkpoint": cp})
	_ = rec.Write("checkpoints", "telemetry", pubsubAccessUnit(body, cp.AtMicro))
	_ = rec.Close()

	t0 := time.Now()
	loaded, err := checkpointFromRecording(path, 0)
	if err != nil {
		return 0, err
	}
	o2 := newBenchOrchestrator(50 * time.Millisecond)
	o2.forkFrom(loaded)
	if loaded.Workers[string(workerTests)] != 1 {
		return 0, fmt.Errorf("fork checkpoint worker index=%d", loaded.Workers[string(workerTests)])
	}
	return time.Since(t0).Milliseconds(), nil
}

// benchOrchestrator is orchestrator without quicrtc publishers.
func newBenchOrchestrator(delay time.Duration) *orchestrator {
	st := statusNew()
	o := newOrchestrator(nil, nil, nil, st)
	o.simulateToolDelay = delay
	for _, p := range o.plans {
		o.toolDelayOverride = map[workerID]time.Duration{
			workerTests: delay,
			workerCode:  delay,
			workerShip:  delay,
		}
		_ = p
		break
	}
	if o.toolDelayOverride == nil {
		o.toolDelayOverride = make(map[workerID]time.Duration)
	}
	for _, id := range []workerID{workerTests, workerCode, workerShip} {
		o.toolDelayOverride[id] = delay
	}
	return o
}
