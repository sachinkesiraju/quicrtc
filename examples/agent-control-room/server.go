package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sachinkesiraju/quicrtc/examples/internal/status"
	"github.com/sachinkesiraju/quicrtc/record"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/wire"
)

//go:embed ui.html
var uiHTML []byte

const (
	trackScreen    = "screen"
	trackReasoning = "reasoning"
	trackToolCalls = "toolcalls"
	trackSteer     = "steer"
	trackFrameAcks = "frame_acks"

	telemetryTrackID uint8 = 1
)

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
