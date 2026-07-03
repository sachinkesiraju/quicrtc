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
	"time"

	"github.com/sachinkesiraju/quicrtc/examples/internal/status"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/record"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/track"
	"github.com/sachinkesiraju/quicrtc/wire"
)

//go:embed ui.html
var uiHTML []byte

const (
	trackScreen    = "screen"
	trackReasoning = "reasoning"
	trackToolCalls = "toolcalls"
	trackSteer       = "steer"
	trackFrameAcks   = "frame_acks"

	telemetryTrackID uint8 = 1
)

func main() {
	var (
		httpAddr   = flag.String("http", "127.0.0.1:8430", "demo page address")
		fps        = flag.Int("fps", 12, "screen frame rate")
		recordPath = flag.String("record", "", "optional .qrtc recording path")
		thesisBench = flag.Bool("thesis-bench", false, "run thesis validation benchmarks and exit")
	)
	flag.Parse()

	if *thesisBench {
		if err := runThesisBench(); err != nil {
			log.Fatal(err)
		}
		return
	}

	st := status.New("agent-control-room")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	var orch *orchestrator
	var rec record.Recorder
	var detachRec func()

	srv, err := server.New(server.Config{
		Addr:         "127.0.0.1:0",
		CertExtraIPs: []net.IP{net.ParseIP("127.0.0.1")},
		ExternalHost: "127.0.0.1",
		SDP: wire.SDP{
			Codec: "image/png", Width: deskW, Height: deskH, FPS: *fps,
		},
		PriorityScheduler:   server.SchedulerOff,
		PerSubscriberBuffer: 4,
		OnSession: func(h server.SessionHandle) {
			st.Inc("sessions", 1)
			go drainSteer(h, orch)
			go drainFrameAcks(h, orch)
			go pumpSessionTelemetry(ctx, h, st, orch)
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	screenPub := srv.AddTrackSpec(server.TrackSpec{Name: trackScreen, Kind: track.KindVideo})
	reasoningPub := srv.AddTrackSpec(server.TrackSpec{Name: trackReasoning, Kind: track.KindTokens})
	toolPub := srv.AddTrackSpec(server.TrackSpec{Name: trackToolCalls, Kind: track.KindToolCalls})

	orch = newOrchestrator(screenPub, reasoningPub, toolPub, st)

	if *recordPath != "" {
		rec, err = record.NewFileRecorder(*recordPath)
		if err != nil {
			log.Fatal(err)
		}
		detachRec = srv.AttachRecorder(rec)
		orch.recorder = func(cp checkpoint) {
			body, _ := json.Marshal(map[string]any{"checkpoint": cp})
			_ = rec.Write("checkpoints", "telemetry", pubsub.AccessUnit{
				Bytes: body, Keyframe: true,
				PTSMicro: uint64(cp.AtMicro), Seq: uint32(cp.AtMicro),
			})
		}
	}

	go func() {
		if err := srv.ListenAndServe(ctx); err != nil && ctx.Err() == nil {
			log.Printf("server: %v", err)
		}
	}()
	for srv.Addr() == "127.0.0.1:0" {
		time.Sleep(10 * time.Millisecond)
	}

	shareURL := fmt.Sprintf("https://%s/wt#slug=%s&hash=%s", srv.Addr(), srv.Slug(), srv.CertHashB64())
	cfg := pageConfig{
		WTURL:    fmt.Sprintf("https://%s/wt", srv.Addr()),
		Slug:     srv.Slug(),
		CertHash: srv.CertHashB64(),
		FPS:      *fps,
		Width:    deskW,
		Height:   deskH,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(uiHTML)
	})
	mux.HandleFunc("/config", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(cfg)
	})
	mux.HandleFunc("/api/checkpoint", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(orch.snapshot())
	})
	mux.HandleFunc("/api/fork", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var cp checkpoint
		if err := json.NewDecoder(r.Body).Decode(&cp); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		orch.forkFrom(cp)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "forked"})
	})
	mux.HandleFunc("/api/fork-from-recording", func(w http.ResponseWriter, r *http.Request) {
		if *recordPath == "" {
			http.Error(w, "server not recording", http.StatusBadRequest)
			return
		}
		atMs := r.URL.Query().Get("at_ms")
		if atMs == "" {
			http.Error(w, "at_ms required", http.StatusBadRequest)
			return
		}
		var ms int64
		if _, err := fmt.Sscanf(atMs, "%d", &ms); err != nil {
			http.Error(w, "bad at_ms", http.StatusBadRequest)
			return
		}
		cp, err := checkpointFromRecording(*recordPath, ms)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		orch.forkFrom(cp)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(cp)
	})

	uiServer := &http.Server{Addr: *httpAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := uiServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("ui: %v", err)
		}
	}()
	defer func() { _ = uiServer.Close() }()

	orch.parallelMode = true
	go orch.run(ctx, *fps)

	fmt.Println("agent-control-room — parallel steerable session on quicrtc")
	fmt.Printf("  open   http://%s\n", *httpAddr)
	fmt.Printf("  share  %s\n", shareURL)
	if *recordPath != "" {
		fmt.Printf("  record %s\n", *recordPath)
	}
	fmt.Println("(Ctrl-C to stop)")

	<-ctx.Done()
	if detachRec != nil {
		detachRec()
	}
	if rec != nil {
		_ = rec.Close()
	}
}

func drainSteer(h server.SessionHandle, orch *orchestrator) {
	for {
		au, err := h.InboundRecv(h.Context(), trackSteer)
		if err != nil {
			return
		}
		var msg steerMsg
		if json.Unmarshal(au.Bytes, &msg) == nil {
			orch.deliverSteer(msg)
		}
	}
}

func drainFrameAcks(h server.SessionHandle, orch *orchestrator) {
	for {
		if _, err := h.InboundRecv(h.Context(), trackFrameAcks); err != nil {
			return
		}
	}
}

func pumpSessionTelemetry(ctx context.Context, h server.SessionHandle, st *status.Status, orch *orchestrator) {
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	var seq uint16
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.Context().Done():
			return
		case <-tick.C:
		}
		body := sessionTelemetry(st, orch)
		raw, err := wire.EncodeDatagram(nil, telemetryTrackID, seq, body)
		if err != nil {
			continue
		}
		_ = h.SendDatagram(raw)
		seq++
	}
}

// checkpointFromRecording finds the latest checkpoint at or before atMs.
func checkpointFromRecording(path string, atMs int64) (checkpoint, error) {
	tl, err := record.BuildTimeline(path)
	if err != nil {
		return checkpoint{}, err
	}
	frames := tl.Frames()
	if len(frames) == 0 {
		return checkpoint{}, fmt.Errorf("recording empty: %s", path)
	}
	base := frames[0].CapturedAtMicros
	target := base + atMs*1000
	var best checkpoint
	var found bool
	for _, e := range frames {
		if e.CapturedAtMicros > target {
			break
		}
		if e.TrackName != "checkpoints" {
			continue
		}
		au, err := tl.ReadPayload(e)
		if err != nil {
			continue
		}
		var wrap struct {
			Checkpoint checkpoint `json:"checkpoint"`
		}
		if json.Unmarshal(au.Bytes, &wrap) == nil && wrap.Checkpoint.AtMicro > 0 {
			best = wrap.Checkpoint
			found = true
		}
	}
	if !found {
		return checkpoint{}, fmt.Errorf("no checkpoint before %dms in %s", atMs, path)
	}
	return best, nil
}

type pageConfig struct {
	WTURL    string `json:"wtUrl"`
	Slug     string `json:"slug"`
	CertHash string `json:"certHash"`
	FPS      int    `json:"fps"`
	Width    int    `json:"width"`
	Height   int    `json:"height"`
}
