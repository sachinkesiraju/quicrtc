// Strong-fit benchmark #1: computer-use agent closed-loop latency.
//
// What this measures: an AI agent operating a user's computer needs
// four concurrent typed channels on one connection:
//   - screen track (server → client) — heavy 200KB frames at 10fps
//   - tokens track (server → client) — agent's reasoning narration
//   - actions track (client → server) — structured click/type/etc.
//   - dom_events track (server → client) — what happened
//
// The closed-loop latency that determines agent responsiveness:
//
//	action.send (client) → server processes → dom_event arrives (client)
//
// Under WebSocket+JSON-RPC (the ACP shape), all four streams share
// one TCP connection — screen frames head-of-line block actions and
// dom_events. Under quicrtc, each track has its own QUIC stream
// lineage with no cross-stream contention.
//
// Pass criterion (loopback):
//   - action→dom_event RTT p99 ≤ 10ms
//   - token p99 stays ≤ 5ms even with 4 streams active
//   - 100% delivery on tokens, dom_events, actions
package agent

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/benchmarks/internal/loadgen"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/track"
)

const (
	// Heavy workload designed to saturate WebRTC's single SCTP-over-DTLS
	// association. Modern SCTP with RFC 8260 user-message interleaving
	// handles light multi-channel loads fine; the HOL signature only
	// emerges when one channel's chunks crowd the others' send queues.
	//
	// Screen: 30fps × 500KB = ~120 Mbps — heavy enough that 500KB SCTP-
	// fragmented chunks compete with token + action chunks within the
	// same association.
	// Tokens: 200/sec — typical LLM emission rate.
	// Actions: 100/sec — agent-driven UI events at high rate.
	// dom_events: echoed by server one-per-action.
	//
	// Workload duration: ~3.3 seconds, ~800 AUs total per modality.
	cuTokenN     = 600 // 3 seconds at 200/sec
	cuScreenN    = 90  // 3 seconds at 30fps
	cuActionsN   = 300 // 3 seconds at 100/sec
	cuTokenSize  = 64
	cuScreenSize = 500 * 1024            // 500KB frames
	cuActionSize = 200                   // ~JSON action payload
	cuTokenPace  = 5 * time.Millisecond  // 200/sec
	cuScreenPace = 33 * time.Millisecond // 30fps
	cuActionPace = 10 * time.Millisecond // 100/sec
)

// TestApp1ComputerUseClosedLoop streams the four computer-use modalities
// concurrently and measures the action→DOM-event closed-loop RTT.
func TestApp1ComputerUseClosedLoop(t *testing.T) {
	pair, err := loadgen.NewPair()
	if err != nil {
		t.Fatal(err)
	}
	defer pair.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := pair.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	// Server-side outbound tracks.
	screenSender, err := pair.PubPC.AddTrack(ctx, track.Video("screen", "test"))
	if err != nil {
		t.Fatal(err)
	}
	tokensSender, err := pair.PubPC.AddTrack(ctx, track.Tokens("tokens"))
	if err != nil {
		t.Fatal(err)
	}
	domSender, err := pair.PubPC.AddTrack(ctx, track.LocalTrack{
		Name: "dom_events", Kind: track.KindToolCalls,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Subscriber-side outbound (PublishBack) actions track.
	actionSender, err := pair.SubPC.AddTrack(ctx, track.LocalTrack{
		Name: "actions", Kind: track.KindToolCalls,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Allow Announce frames to propagate.
	time.Sleep(50 * time.Millisecond)

	tokenE2E := make([]time.Duration, 0, cuTokenN)
	screenRecv := 0
	actionRTTs := make([]time.Duration, 0, cuActionsN)
	actionsRecvd := 0

	var wg sync.WaitGroup

	// Subscriber: drain screen.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for screenRecv < cuScreenN {
			rctx, rcancel := context.WithTimeout(ctx, 3*time.Second)
			_, err := pair.SubPC.RecvOn(rctx, "screen")
			rcancel()
			if err != nil {
				return
			}
			screenRecv++
		}
	}()

	// Subscriber: drain tokens, measure end-to-end latency.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for len(tokenE2E) < cuTokenN {
			rctx, rcancel := context.WithTimeout(ctx, 3*time.Second)
			au, err := pair.SubPC.RecvOn(rctx, "tokens")
			rcancel()
			if err != nil {
				return
			}
			now := time.Now()
			sentAt := time.Unix(0, int64(binary.BigEndian.Uint64(au.Bytes[:8])))
			tokenE2E = append(tokenE2E, now.Sub(sentAt))
		}
	}()

	// Subscriber: drain dom_events. Each carries the action-send-time;
	// arrival - send-time = closed-loop RTT.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for len(actionRTTs) < cuActionsN {
			rctx, rcancel := context.WithTimeout(ctx, 3*time.Second)
			au, err := pair.SubPC.RecvOn(rctx, "dom_events")
			rcancel()
			if err != nil {
				return
			}
			now := time.Now()
			sentAt := time.Unix(0, int64(binary.BigEndian.Uint64(au.Bytes[:8])))
			actionRTTs = append(actionRTTs, now.Sub(sentAt))
		}
	}()

	// Server-side: read inbound actions, immediately reply with dom_event.
	// Carries the original action's timestamp so the subscriber can
	// compute RTT.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for actionsRecvd < cuActionsN {
			rctx, rcancel := context.WithTimeout(ctx, 3*time.Second)
			au, err := pair.PubPC.RecvOn(rctx, "actions")
			rcancel()
			if err != nil {
				return
			}
			actionsRecvd++
			isKey := actionsRecvd == 1
			_ = domSender.Send(ctx, pubsub.AccessUnit{
				Bytes:    au.Bytes, // echo timestamp prefix
				Keyframe: isKey,
				Seq:      uint32(actionsRecvd),
			})
		}
	}()

	// Server-side: stream screen frames.
	go func() {
		ticker := time.NewTicker(cuScreenPace)
		defer ticker.Stop()
		for i := 0; i < cuScreenN; i++ {
			ts := time.Now()
			_ = screenSender.Send(ctx, pubsub.AccessUnit{
				Bytes:    loadgen.TimedPayload(cuScreenSize, ts),
				Keyframe: i%10 == 0,
				Seq:      uint32(i),
			})
			if i < cuScreenN-1 {
				<-ticker.C
			}
		}
	}()

	// Server-side: stream tokens.
	go func() {
		ticker := time.NewTicker(cuTokenPace)
		defer ticker.Stop()
		for i := 0; i < cuTokenN; i++ {
			ts := time.Now()
			_ = tokensSender.Send(ctx, pubsub.AccessUnit{
				Bytes:    loadgen.TimedPayload(cuTokenSize, ts),
				Keyframe: i == 0,
				Seq:      uint32(i),
			})
			if i < cuTokenN-1 {
				<-ticker.C
			}
		}
	}()

	// Client-side: stream actions (PublishBack).
	go func() {
		ticker := time.NewTicker(cuActionPace)
		defer ticker.Stop()
		for i := 0; i < cuActionsN; i++ {
			ts := time.Now()
			_ = actionSender.Send(ctx, pubsub.AccessUnit{
				Bytes:    loadgen.TimedPayload(cuActionSize, ts),
				Keyframe: true, // each action is a self-contained event
				Seq:      uint32(i),
			})
			if i < cuActionsN-1 {
				<-ticker.C
			}
		}
	}()

	wg.Wait()

	tStats := loadgen.ComputeStats(tokenE2E)
	rtStats := loadgen.ComputeStats(actionRTTs)

	t.Logf("TOKEN     recv=%d/%d  end-to-end p50=%s p95=%s p99=%s max=%s",
		len(tokenE2E), cuTokenN,
		tStats.P50.Round(time.Microsecond),
		tStats.P95.Round(time.Microsecond),
		tStats.P99.Round(time.Microsecond),
		tStats.Max.Round(time.Microsecond),
	)
	t.Logf("ACTION→DOM  echoed=%d/%d  RTT p50=%s p95=%s p99=%s max=%s",
		len(actionRTTs), cuActionsN,
		rtStats.P50.Round(time.Microsecond),
		rtStats.P95.Round(time.Microsecond),
		rtStats.P99.Round(time.Microsecond),
		rtStats.Max.Round(time.Microsecond),
	)
	t.Logf("SCREEN    recv=%d/%d", screenRecv, cuScreenN)

	if len(tokenE2E) != cuTokenN {
		t.Errorf("token delivery %d/%d", len(tokenE2E), cuTokenN)
	}
	if len(actionRTTs) != cuActionsN {
		t.Errorf("action→dom delivery %d/%d", len(actionRTTs), cuActionsN)
	}
	if screenRecv != cuScreenN {
		t.Errorf("screen delivery %d/%d", screenRecv, cuScreenN)
	}
	// Pass criteria for the heavy-load variant. Tokens and actions
	// are small messages; under proper per-stream isolation they
	// shouldn't be affected by the 30fps × 500KB screen flood.
	//
	// Token threshold is 5ms — half the 10ms action→DOM RTT
	// threshold, since tokens are a one-way path and a round-trip
	// is two. Both bounds are derived from "small messages don't
	// queue behind 500KB screen frames," not from a specific dev
	// machine's observed numbers; choosing too tight a number turns
	// the architectural assertion into a hardware-speed assertion
	// and makes the test flaky on slower CI runners.
	if rtStats.P99 > 10*time.Millisecond {
		t.Errorf("action→DOM RTT p99 %s > 10ms threshold (heavy load)", rtStats.P99)
	}
	if tStats.P99 > 5*time.Millisecond {
		t.Errorf("token p99 under 4-stream heavy load %s > 5ms threshold", tStats.P99)
	}
}
