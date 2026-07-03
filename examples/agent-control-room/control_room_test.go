package main

import (
	"testing"
	"time"
)

func TestParallelVsSerialThesis(t *testing.T) {
	r, err := measureParallelVsSerial()
	if err != nil {
		t.Fatal(err)
	}
	if r.speedup < 2.5 {
		t.Fatalf("speedup %.2f < 2.5 (serial %v parallel %v)", r.speedup, r.serial, r.parallel)
	}
	t.Logf("parallel speedup %.2fx", r.speedup)
}

func TestSteerCancelThesis(t *testing.T) {
	ms, err := measureSteerCancelLatency()
	if err != nil {
		t.Fatal(err)
	}
	if ms > 150 {
		t.Fatalf("cancel latency %dms > 150ms", ms)
	}
	t.Logf("cancel latency %dms", ms)
}

func TestForkRestoreThesis(t *testing.T) {
	ms, err := measureForkRestore()
	if err != nil {
		t.Fatal(err)
	}
	if ms > 500 {
		t.Fatalf("fork restore %dms > 500ms", ms)
	}
	t.Logf("fork restore %dms", ms)
}

func TestCheckpointRoundTrip(t *testing.T) {
	o := newBenchOrchestrator(10 * time.Millisecond)
	o.saveCheckpoint(workerTests, 2)
	cp := o.snapshot()
	if cp.Workers[string(workerTests)] != 2 {
		t.Fatalf("worker index %d", cp.Workers[string(workerTests)])
	}
	o2 := newBenchOrchestrator(10 * time.Millisecond)
	o2.forkFrom(cp)
	cp2 := o2.snapshot()
	if cp2.Workers[string(workerTests)] != 2 {
		t.Fatalf("fork did not preserve worker index")
	}
}
