// Network-conditions grid: each strong-fit benchmark run under
// varying RTT and packet-loss conditions, via the UDP proxy.
//
// The grid is intentionally compact (3 RTTs × 2 loss rates = 6 cells)
// so the whole sweep fits in a few minutes. Each cell runs the same
// reduced workload to keep total time manageable; we report headline
// numbers per cell.
//
// What we expect to validate:
//   - Setup latency degrades with RTT (1 RTT for QUIC handshake).
//   - Token p99 stays bounded under loss because per-stream loss
//     recovery doesn't stall other streams.
//   - Session resume completes in ~1 RTT regardless of loss.
package network

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/benchmarks/internal/loadgen"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/track"
)

type netCell struct {
	label   string
	delayMs int
	loss    float64
}

func networkCells() []netCell {
	return []netCell{
		{"loopback", 0, 0.00},
		{"50ms RTT", 25, 0.00},
		{"200ms RTT", 100, 0.00},
		{"1% loss", 0, 0.01},
		{"3% loss", 0, 0.03},
		{"50ms+1% loss", 25, 0.01},
	}
}

func TestNetworkGrid_App2_Multimodal(t *testing.T) {
	if testing.Short() {
		t.Skip("network grid takes minutes; -short")
	}
	const tokenN = 200
	const videoN = 30
	const tokenSize = 64
	const videoSize = 50 * 1024
	const tokenPace = 5 * time.Millisecond
	const videoPace = 33 * time.Millisecond

	results := make([]gridResult, 0, len(networkCells()))

	for _, cell := range networkCells() {
		pair, err := loadgen.NewPair()
		if err != nil {
			t.Fatal(err)
		}
		pair.Netcond = loadgen.NetCond{
			OneWayDelay: time.Duration(cell.delayMs) * time.Millisecond,
			LossRate:    cell.loss,
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		if err := pair.Connect(ctx); err != nil {
			pair.Close()
			cancel()
			t.Errorf("[%s] connect: %v", cell.label, err)
			continue
		}

		videoSender, err := pair.PubPC.AddTrack(ctx, track.Video("video", "test"))
		if err != nil {
			pair.Close()
			cancel()
			t.Errorf("[%s] AddTrack video: %v", cell.label, err)
			continue
		}
		tokensSender, err := pair.PubPC.AddTrack(ctx, track.Tokens("tokens"))
		if err != nil {
			pair.Close()
			cancel()
			t.Errorf("[%s] AddTrack tokens: %v", cell.label, err)
			continue
		}

		tokenE2E := make([]time.Duration, 0, tokenN)
		videoE2E := make([]time.Duration, 0, videoN)
		var muT, muV sync.Mutex

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for {
				rctx, rc := context.WithTimeout(ctx, 5*time.Second)
				au, err := pair.SubPC.RecvOn(rctx, "tokens")
				rc()
				if err != nil {
					return
				}
				now := time.Now()
				sentAt := time.Unix(0, int64(binary.BigEndian.Uint64(au.Bytes[:8])))
				muT.Lock()
				tokenE2E = append(tokenE2E, now.Sub(sentAt))
				done := len(tokenE2E) >= tokenN
				muT.Unlock()
				if done {
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for {
				rctx, rc := context.WithTimeout(ctx, 5*time.Second)
				au, err := pair.SubPC.RecvOn(rctx, "video")
				rc()
				if err != nil {
					return
				}
				now := time.Now()
				sentAt := time.Unix(0, int64(binary.BigEndian.Uint64(au.Bytes[:8])))
				muV.Lock()
				videoE2E = append(videoE2E, now.Sub(sentAt))
				done := len(videoE2E) >= videoN
				muV.Unlock()
				if done {
					return
				}
			}
		}()

		go func() {
			ticker := time.NewTicker(tokenPace)
			defer ticker.Stop()
			for i := 0; i < tokenN; i++ {
				_ = tokensSender.Send(ctx, pubsub.AccessUnit{
					Bytes: loadgen.TimedPayload(tokenSize, time.Now()), Keyframe: i == 0,
				})
				if i < tokenN-1 {
					<-ticker.C
				}
			}
		}()
		go func() {
			ticker := time.NewTicker(videoPace)
			defer ticker.Stop()
			for i := 0; i < videoN; i++ {
				_ = videoSender.Send(ctx, pubsub.AccessUnit{
					Bytes: loadgen.TimedPayload(videoSize, time.Now()), Keyframe: i%30 == 0,
				})
				if i < videoN-1 {
					<-ticker.C
				}
			}
		}()

		wg.Wait()
		muT.Lock()
		muV.Lock()
		tStats := loadgen.ComputeStats(tokenE2E)
		vStats := loadgen.ComputeStats(videoE2E)
		tn := len(tokenE2E)
		vn := len(videoE2E)
		muV.Unlock()
		muT.Unlock()

		results = append(results, gridResult{
			cell: cell.label, tokenN: tn, videoN: vn, expectedT: tokenN, expectedV: videoN,
			tokenP99: tStats.P99, tokenP50: tStats.P50,
			videoP99: vStats.P99, videoP50: vStats.P50,
		})
		pair.Close()
		cancel()
	}

	t.Logf("\n%-15s | %-12s | %-12s | %-12s | %-12s",
		"network", "token p50", "token p99", "video p50", "video p99")
	t.Logf("%-15s-+-%-12s-+-%-12s-+-%-12s-+-%-12s",
		"---------------", "------------", "------------", "------------", "------------")
	for _, r := range results {
		dlivT := fmt.Sprintf("%d/%d", r.tokenN, r.expectedT)
		dlivV := fmt.Sprintf("%d/%d", r.videoN, r.expectedV)
		t.Logf("%-15s | %-12s | %-12s | %-12s | %-12s    deliv tokens=%s video=%s",
			r.cell,
			r.tokenP50.Round(time.Microsecond),
			r.tokenP99.Round(time.Microsecond),
			r.videoP50.Round(time.Microsecond),
			r.videoP99.Round(time.Microsecond),
			dlivT, dlivV,
		)
	}
}

func TestNetworkGrid_App1_ComputerUse(t *testing.T) {
	if testing.Short() {
		t.Skip("network grid takes minutes; -short")
	}
	const tokenN = 100
	const screenN = 15
	const actionsN = 30

	type cellRow struct {
		cell                        string
		tokenP99, screenP99, rttP99 time.Duration
		tokenN, screenN, rttN       int
	}
	var rows []cellRow

	for _, cell := range networkCells() {
		row := cellRow{cell: cell.label}
		err := func() error {
			pair, err := loadgen.NewPair()
			if err != nil {
				return err
			}
			pair.Netcond = loadgen.NetCond{
				OneWayDelay: time.Duration(cell.delayMs) * time.Millisecond,
				LossRate:    cell.loss,
			}
			defer pair.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			if err := pair.Connect(ctx); err != nil {
				return err
			}
			screenSender, err := pair.PubPC.AddTrack(ctx, track.Video("screen", "test"))
			if err != nil {
				return err
			}
			tokensSender, err := pair.PubPC.AddTrack(ctx, track.Tokens("tokens"))
			if err != nil {
				return err
			}
			domSender, err := pair.PubPC.AddTrack(ctx, track.LocalTrack{Name: "dom_events", Kind: track.KindToolCalls})
			if err != nil {
				return err
			}
			actionSender, err := pair.SubPC.AddTrack(ctx, track.LocalTrack{Name: "actions", Kind: track.KindToolCalls})
			if err != nil {
				return err
			}
			time.Sleep(50 * time.Millisecond)

			tokenE2E := make([]time.Duration, 0, tokenN)
			rtts := make([]time.Duration, 0, actionsN)
			screenE2E := make([]time.Duration, 0, screenN)
			var muT, muR, muS sync.Mutex

			var wg sync.WaitGroup
			wg.Add(4)
			go func() {
				defer wg.Done()
				for {
					rctx, rc := context.WithTimeout(ctx, 5*time.Second)
					au, err := pair.SubPC.RecvOn(rctx, "tokens")
					rc()
					if err != nil {
						return
					}
					now := time.Now()
					sentAt := time.Unix(0, int64(binary.BigEndian.Uint64(au.Bytes[:8])))
					muT.Lock()
					tokenE2E = append(tokenE2E, now.Sub(sentAt))
					done := len(tokenE2E) >= tokenN
					muT.Unlock()
					if done {
						return
					}
				}
			}()
			go func() {
				defer wg.Done()
				for {
					rctx, rc := context.WithTimeout(ctx, 5*time.Second)
					au, err := pair.SubPC.RecvOn(rctx, "screen")
					rc()
					if err != nil {
						return
					}
					now := time.Now()
					sentAt := time.Unix(0, int64(binary.BigEndian.Uint64(au.Bytes[:8])))
					muS.Lock()
					screenE2E = append(screenE2E, now.Sub(sentAt))
					done := len(screenE2E) >= screenN
					muS.Unlock()
					if done {
						return
					}
				}
			}()
			go func() {
				defer wg.Done()
				for {
					rctx, rc := context.WithTimeout(ctx, 5*time.Second)
					au, err := pair.SubPC.RecvOn(rctx, "dom_events")
					rc()
					if err != nil {
						return
					}
					now := time.Now()
					sentAt := time.Unix(0, int64(binary.BigEndian.Uint64(au.Bytes[:8])))
					muR.Lock()
					rtts = append(rtts, now.Sub(sentAt))
					done := len(rtts) >= actionsN
					muR.Unlock()
					if done {
						return
					}
				}
			}()
			go func() {
				defer wg.Done()
				count := 0
				for count < actionsN {
					rctx, rc := context.WithTimeout(ctx, 5*time.Second)
					au, err := pair.PubPC.RecvOn(rctx, "actions")
					rc()
					if err != nil {
						return
					}
					count++
					_ = domSender.Send(ctx, pubsub.AccessUnit{
						Bytes: au.Bytes, Keyframe: count == 1, Seq: uint32(count),
					})
				}
			}()

			go func() {
				ticker := time.NewTicker(33 * time.Millisecond)
				defer ticker.Stop()
				for i := 0; i < screenN; i++ {
					_ = screenSender.Send(ctx, pubsub.AccessUnit{
						Bytes: loadgen.TimedPayload(50*1024, time.Now()), Keyframe: i%10 == 0,
					})
					if i < screenN-1 {
						<-ticker.C
					}
				}
			}()
			go func() {
				ticker := time.NewTicker(10 * time.Millisecond)
				defer ticker.Stop()
				for i := 0; i < tokenN; i++ {
					_ = tokensSender.Send(ctx, pubsub.AccessUnit{
						Bytes: loadgen.TimedPayload(64, time.Now()), Keyframe: i == 0,
					})
					if i < tokenN-1 {
						<-ticker.C
					}
				}
			}()
			go func() {
				ticker := time.NewTicker(33 * time.Millisecond)
				defer ticker.Stop()
				for i := 0; i < actionsN; i++ {
					_ = actionSender.Send(ctx, pubsub.AccessUnit{
						Bytes: loadgen.TimedPayload(200, time.Now()), Keyframe: true,
					})
					if i < actionsN-1 {
						<-ticker.C
					}
				}
			}()

			wg.Wait()
			muT.Lock()
			row.tokenP99 = loadgen.ComputeStats(tokenE2E).P99
			row.tokenN = len(tokenE2E)
			muT.Unlock()
			muS.Lock()
			row.screenP99 = loadgen.ComputeStats(screenE2E).P99
			row.screenN = len(screenE2E)
			muS.Unlock()
			muR.Lock()
			row.rttP99 = loadgen.ComputeStats(rtts).P99
			row.rttN = len(rtts)
			muR.Unlock()
			return nil
		}()
		if err != nil {
			row.cell = row.cell + " (err: " + err.Error() + ")"
		}
		rows = append(rows, row)
	}

	t.Logf("\n%-15s | %-12s | %-12s | %-15s | delivery",
		"network", "token p99", "screen p99", "act→DOM RTT p99")
	t.Logf("%-15s-+-%-12s-+-%-12s-+-%-15s-+---------------",
		"---------------", "------------", "------------", "---------------")
	for _, r := range rows {
		t.Logf("%-15s | %-12s | %-12s | %-15s | t=%d/%d s=%d/%d r=%d/%d",
			r.cell,
			r.tokenP99.Round(time.Microsecond),
			r.screenP99.Round(time.Microsecond),
			r.rttP99.Round(time.Microsecond),
			r.tokenN, tokenN, r.screenN, screenN, r.rttN, actionsN,
		)
	}
}

func TestNetworkGrid_App4_Resume(t *testing.T) {
	if testing.Short() {
		t.Skip("network grid takes minutes; -short")
	}
	type cellResult struct {
		cell    string
		resumes []time.Duration
		ok      bool
		err     error
	}
	var rows []cellResult

	for _, cell := range networkCells() {
		row := cellResult{cell: cell.label}
		row.ok, row.err = func() (bool, error) {
			pair, err := loadgen.NewPair()
			if err != nil {
				return false, err
			}
			pair.Netcond = loadgen.NetCond{
				OneWayDelay: time.Duration(cell.delayMs) * time.Millisecond,
				LossRate:    cell.loss,
			}
			defer pair.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			if err := pair.Connect(ctx); err != nil {
				return false, fmt.Errorf("connect: %w", err)
			}
			sender, err := pair.PubPC.AddTrack(ctx, track.Tokens("tokens"))
			if err != nil {
				return false, err
			}

			c, ok := loadgen.PickClient(pair.SubPC)
			if !ok || c.SessionID() == "" {
				return false, fmt.Errorf("no session id")
			}
			sessionID := c.SessionID()

			const N = 50
			seen := make(map[uint32]bool)
			received := 0
			sendDone := make(chan struct{})
			go func() {
				defer close(sendDone)
				ticker := time.NewTicker(50 * time.Millisecond)
				defer ticker.Stop()
				for i := 0; i < N; i++ {
					_ = sender.Send(ctx, pubsub.AccessUnit{
						Bytes: []byte{byte(i)}, Keyframe: i == 0, Seq: uint32(i),
					})
					<-ticker.C
				}
			}()
			for received < 20 {
				rctx, rc := context.WithTimeout(ctx, 5*time.Second)
				au, err := pair.SubPC.RecvOn(rctx, "tokens")
				rc()
				if err != nil {
					return false, fmt.Errorf("phase1 recv: %w", err)
				}
				if !seen[au.Seq] {
					seen[au.Seq] = true
					received++
				}
			}
			lastSeen := c.LastSeenSeq()
			pair.SubPC.Close()
			time.Sleep(200 * time.Millisecond)
			start := time.Now()
			if err := pair.ReconnectSubscriber(ctx, sessionID, lastSeen); err != nil {
				return false, fmt.Errorf("reconnect: %w", err)
			}
			rctx, rc := context.WithTimeout(ctx, 10*time.Second)
			_, err = pair.SubPC.RecvOn(rctx, "tokens")
			rc()
			if err != nil {
				return false, fmt.Errorf("resume recv: %w", err)
			}
			row.resumes = append(row.resumes, time.Since(start))
			for received < N {
				rctx, rc := context.WithTimeout(ctx, 5*time.Second)
				au, err := pair.SubPC.RecvOn(rctx, "tokens")
				rc()
				if err != nil {
					return false, fmt.Errorf("drain: %w", err)
				}
				if !seen[au.Seq] {
					seen[au.Seq] = true
					received++
				}
			}
			<-sendDone
			return received == N, nil
		}()
		rows = append(rows, row)
	}

	t.Logf("\n%-15s | %-15s | result", "network", "resume RTT")
	t.Logf("%-15s-+-%-15s-+-------", "---------------", "---------------")
	for _, r := range rows {
		var rtt string
		if len(r.resumes) > 0 {
			rtt = r.resumes[0].Round(100 * time.Microsecond).String()
		} else {
			rtt = "—"
		}
		status := "✓"
		if !r.ok {
			status = fmt.Sprintf("✗ (%v)", r.err)
		}
		t.Logf("%-15s | %-15s | %s", r.cell, rtt, status)
	}
}

type gridResult struct {
	cell                                 string
	tokenN, videoN, expectedT, expectedV int
	tokenP50, tokenP99                   time.Duration
	videoP50, videoP99                   time.Duration
}
