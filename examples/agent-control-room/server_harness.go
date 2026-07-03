package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/sachinkesiraju/quicrtc/examples/internal/status"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/record"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/track"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// harnessOptions configures a control-room instance for tests or demos.
type harnessOptions struct {
	HTTPAddr  string // default 127.0.0.1:0
	FPS       int
	RecordPath string
	LLM       bool
	LLMModel  string
	LLMTurns  int
	Workspace string // optional persistent workspace dir for -llm
}

// controlRoomHarness is a running agent-control-room (quicrtc + HTTP UI).
type controlRoomHarness struct {
	HTTPURL    string
	WTURL      string
	ShareURL   string
	Slug       string
	CertHash   string
	RecordPath string

	orch      *orchestrator
	srv       *server.Server
	ctx       context.Context
	cancel    context.CancelFunc
	uiServer  *http.Server
	rec       record.Recorder
	detachRec func()
}

func startControlRoomHarness(opts harnessOptions) (*controlRoomHarness, error) {
	if opts.FPS <= 0 {
		opts.FPS = 12
	}
	if opts.HTTPAddr == "" {
		opts.HTTPAddr = "127.0.0.1:0"
	}

	ctx, cancel := context.WithCancel(context.Background())
	st := status.New("agent-control-room")

	var orch *orchestrator
	var rec record.Recorder
	var detachRec func()

	srv, err := server.New(server.Config{
		Addr:         "127.0.0.1:0",
		CertExtraIPs: []net.IP{net.ParseIP("127.0.0.1")},
		ExternalHost: "127.0.0.1",
		SDP: wire.SDP{
			Codec: "image/png", Width: deskW, Height: deskH, FPS: opts.FPS,
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
		cancel()
		return nil, err
	}

	screenPub := srv.AddTrackSpec(server.TrackSpec{Name: trackScreen, Kind: track.KindVideo})
	reasoningPub := srv.AddTrackSpec(server.TrackSpec{Name: trackReasoning, Kind: track.KindTokens})
	toolPub := srv.AddTrackSpec(server.TrackSpec{Name: trackToolCalls, Kind: track.KindToolCalls})

	if opts.LLM {
		orch, err = newOrchestratorLLM(screenPub, reasoningPub, toolPub, st, llmConfig{
			Model:    opts.LLMModel,
			MaxTurns: opts.LLMTurns,
		}, opts.Workspace)
		if err != nil {
			cancel()
			return nil, err
		}
	} else {
		orch = newOrchestrator(screenPub, reasoningPub, toolPub, st)
	}

	if opts.RecordPath != "" {
		rec, err = record.NewFileRecorder(opts.RecordPath)
		if err != nil {
			cancel()
			return nil, err
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
			// tests ignore late shutdown errors
			_ = err
		}
	}()
	for srv.Addr() == "127.0.0.1:0" {
		time.Sleep(10 * time.Millisecond)
	}

	recordPath := opts.RecordPath
	cfg := pageConfig{
		WTURL:    fmt.Sprintf("https://%s/wt", srv.Addr()),
		Slug:     srv.Slug(),
		CertHash: srv.CertHashB64(),
		FPS:      opts.FPS,
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
		if recordPath == "" {
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
		cp, err := checkpointFromRecording(recordPath, ms)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		orch.forkFrom(cp)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(cp)
	})

	uiServer := &http.Server{Addr: opts.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	ln, err := net.Listen("tcp", opts.HTTPAddr)
	if err != nil {
		cancel()
		return nil, err
	}
	go func() {
		if err := uiServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			_ = err
		}
	}()

	h := &controlRoomHarness{
		HTTPURL:    "http://" + ln.Addr().String(),
		WTURL:      cfg.WTURL,
		ShareURL:   fmt.Sprintf("https://%s/wt#slug=%s&hash=%s", srv.Addr(), srv.Slug(), srv.CertHashB64()),
		Slug:       cfg.Slug,
		CertHash:   cfg.CertHash,
		RecordPath: recordPath,
		orch:       orch,
		srv:        srv,
		ctx:        ctx,
		cancel:     cancel,
		uiServer:   uiServer,
		rec:        rec,
		detachRec:  detachRec,
	}

	orch.parallelMode = true
	go orch.run(ctx, opts.FPS)

	return h, nil
}

func (h *controlRoomHarness) QUICAddr() string { return h.srv.Addr() }

func (h *controlRoomHarness) Orchestrator() *orchestrator { return h.orch }

func (h *controlRoomHarness) Close() {
	h.cancel()
	if h.orch != nil {
		h.orch.closeLLM()
	}
	if h.detachRec != nil {
		h.detachRec()
	}
	if h.rec != nil {
		_ = h.rec.Close()
	}
	_ = h.uiServer.Close()
}
