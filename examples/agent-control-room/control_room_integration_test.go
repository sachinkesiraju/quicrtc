package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/client"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/record"
	"github.com/sachinkesiraju/quicrtc/track"
)

func TestSteerCancelUnderLoadThesis(t *testing.T) {
	ms, err := measureSteerCancelUnderLoad()
	if err != nil {
		t.Fatal(err)
	}
	if ms > 150 {
		t.Fatalf("cancel latency %dms > 150ms", ms)
	}
	t.Logf("cancel during active tool work: %dms", ms)
}

func TestHTTPCheckpointAndFork(t *testing.T) {
	h, err := startControlRoomHarness(harnessOptions{FPS: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	deadline := time.Now().Add(3 * time.Second)
	var cp checkpoint
	for time.Now().Before(deadline) {
		resp, err := http.Get(h.HTTPURL + "/api/checkpoint")
		if err != nil {
			t.Fatal(err)
		}
		if err := json.NewDecoder(resp.Body).Decode(&cp); err != nil {
			resp.Body.Close()
			t.Fatal(err)
		}
		resp.Body.Close()
		if cp.AtMicro > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if cp.AtMicro == 0 {
		t.Fatal("no checkpoint produced")
	}

	body, _ := json.Marshal(cp)
	resp, err := http.Post(h.HTTPURL+"/api/fork", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("fork status %d", resp.StatusCode)
	}
	got := h.orch.snapshot()
	if got.Workers[string(workerTests)] != cp.Workers[string(workerTests)] {
		t.Fatalf("fork worker tests index=%d want %d", got.Workers[string(workerTests)], cp.Workers[string(workerTests)])
	}
}

func TestForkFromRecordingHTTP(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.qrtc")
	h, err := startControlRoomHarness(harnessOptions{FPS: 8, RecordPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	time.Sleep(2 * time.Second)

	resp, err := http.Post(h.HTTPURL+"/api/fork-from-recording?at_ms=500", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("fork-from-recording status %d: %s", resp.StatusCode, b)
	}
	var cp checkpoint
	if err := json.NewDecoder(resp.Body).Decode(&cp); err != nil {
		t.Fatal(err)
	}
	if cp.AtMicro == 0 {
		t.Fatal("empty checkpoint from recording")
	}
	got := h.orch.snapshot()
	if got.Workers[string(workerTests)] != cp.Workers[string(workerTests)] {
		t.Fatalf("fork worker tests index=%d want %d", got.Workers[string(workerTests)], cp.Workers[string(workerTests)])
	}
}

func TestRecordingSeekSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.qrtc")
	h, err := startControlRoomHarness(harnessOptions{FPS: 8, RecordPath: path})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)
	h.Close()

	tl, err := record.BuildTimeline(path)
	if err != nil {
		t.Fatal(err)
	}
	frames := tl.Frames()
	if len(frames) == 0 {
		t.Fatal("recording has no frames")
	}

	cp, err := checkpointFromRecording(path, 1000)
	if err != nil {
		t.Fatal(err)
	}
	target := frames[0].CapturedAtMicros + 1000*1000
	snap := tl.SeekSnapshot(target)

	if _, ok := snap[record.Track{Name: "checkpoints", Kind: "telemetry"}]; !ok {
		t.Fatal("SeekSnapshot missing checkpoints track at fork time")
	}
	if _, ok := snap[record.Track{Name: trackScreen, Kind: string(track.KindVideo)}]; !ok {
		t.Log("screen frame not yet in recording at 1s (may appear later)")
	}
	if cp.Workers[string(workerTests)] < 0 {
		t.Fatalf("bad worker index %v", cp.Workers)
	}
}

func TestWebTransportRecvScreen(t *testing.T) {
	h, err := startControlRoomHarness(harnessOptions{FPS: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cl, err := client.Dial(ctx, h.WTURL, client.Options{
		Slug:               h.Slug,
		CertHashB64URL:     h.CertHash,
		InsecureSkipVerify: true,
		HelloTimeout:       10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	recvCtx, recvCancel := context.WithTimeout(ctx, 10*time.Second)
	defer recvCancel()
	au, err := cl.RecvOn(recvCtx, trackScreen)
	if err != nil {
		t.Fatal(err)
	}
	if len(au.Bytes) == 0 {
		t.Fatal("empty screen frame")
	}
}

func TestWebTransportSteerCancel(t *testing.T) {
	h, err := startControlRoomHarness(harnessOptions{FPS: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cl, err := client.Dial(ctx, h.WTURL, client.Options{
		Slug:               h.Slug,
		CertHashB64URL:     h.CertHash,
		InsecureSkipVerify: true,
		HelloTimeout:       10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	steerSender, err := cl.Publish(ctx, track.LocalTrack{Name: trackSteer, Kind: track.KindToolCalls})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = cl.Publish(ctx, track.LocalTrack{Name: trackFrameAcks, Kind: track.KindTokens})

	before := h.orch.cancelCount.Load()
	body, _ := json.Marshal(steerMsg{Type: "cancel", TsUs: uint64(time.Now().UnixMicro())})
	if err := steerSender.Send(ctx, pubsub.AccessUnit{
		Bytes: body, Keyframe: true, PTSMicro: uint64(time.Now().UnixMicro()),
	}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if h.orch.cancelCount.Load() > before {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("steer cancel not applied via WebTransport")
}

func TestFullScriptParallelSpeedup(t *testing.T) {
	const delay = 80 * time.Millisecond
	serial := runFullScriptCycle(false, delay)
	parallel := runFullScriptCycle(true, delay)
	speedup := float64(serial) / float64(parallel)
	if speedup < 1.8 {
		t.Fatalf("full-script speedup %.2f < 1.8 (serial %v parallel %v)", speedup, serial, parallel)
	}
	t.Logf("full-script parallel speedup %.2fx (serial %v parallel %v)", speedup, serial, parallel)
}

func runFullScriptCycle(parallel bool, delay time.Duration) time.Duration {
	o := newBenchOrchestrator(delay)
	o.parallelMode = parallel
	o.benchSkipReasoning = true
	o.benchSkipMutations = true
	start := time.Now()
	o.runParallelCycle(context.Background())
	return time.Since(start)
}

// Ensure recording file is written on disk during live session.
func TestRecordingFileExists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "live.qrtc")
	h, err := startControlRoomHarness(harnessOptions{FPS: 8, RecordPath: path})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	h.Close()

	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() == 0 {
		t.Fatal("recording file empty")
	}
}
