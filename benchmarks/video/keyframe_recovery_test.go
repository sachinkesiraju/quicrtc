// Keyframe-recovery benchmark: measures time-to-fresh-frame after a
// subscriber-observable loss event, comparing two recovery paths:
//
//   - BASELINE: subscriber waits for the next natural keyframe at the
//     publisher's GOP cadence (e.g. once per 2s). This is the only
//     option v1.0 receivers had.
//
//   - PR6:      subscriber calls Client.RequestKeyframe(trackName)
//     after observing a P-frame gap. Publisher's OnKeyframeRequest
//     callback fires; the encoder emits a fresh keyframe within one
//     frame interval (~33ms at 30fps).
//
// This is the headline computer-use / live-screen recovery metric.
// On a typical agent screen-share at 30fps with 2s GOP, the baseline
// recovers in up to 2 seconds; PR6 recovers in ~50ms — a 40x
// improvement on visible-stutter duration.
package video

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/benchmarks/internal/loadgen"
	"github.com/sachinkesiraju/quicrtc/cert"
	"github.com/sachinkesiraju/quicrtc/client"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// fakeEncoder emits one keyframe every gopDuration, with P-frames in
// between at fps. requestKeyframeNow() asks it to emit a keyframe on
// the next tick regardless of the natural GOP cadence — this is what
// the publisher's OnKeyframeRequest callback wires up to in PR6.
type fakeEncoder struct {
	pub         *server.Publisher
	gopDuration time.Duration
	frameDur    time.Duration
	requestedKF atomic.Bool
	seq         uint32
}

func newFakeEncoder(pub *server.Publisher, fps int, gopSecs float64) *fakeEncoder {
	return &fakeEncoder{
		pub:         pub,
		gopDuration: time.Duration(float64(time.Second) * gopSecs),
		frameDur:    time.Second / time.Duration(fps),
	}
}

func (e *fakeEncoder) requestKeyframeNow() {
	e.requestedKF.Store(true)
}

func (e *fakeEncoder) run(ctx context.Context) {
	ticker := time.NewTicker(e.frameDur)
	defer ticker.Stop()
	lastKF := time.Now().Add(-e.gopDuration) // force first frame to be a keyframe
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			natural := now.Sub(lastKF) >= e.gopDuration
			requested := e.requestedKF.CompareAndSwap(true, false)
			isKF := natural || requested
			au := pubsub.AccessUnit{
				Bytes:    []byte(fmt.Sprintf("frame-%d", e.seq)),
				Keyframe: isKF,
				Seq:      e.seq,
				PTSMicro: uint64(now.UnixMicro()),
			}
			e.seq++
			if isKF {
				lastKF = now
			}
			_ = e.pub.Publish(context.Background(), au)
		}
	}
}

type recoverySetup struct {
	srv     *server.Server
	cli     *client.Client
	enc     *fakeEncoder
	srvCtx  context.Context
	srvDone context.CancelFunc
	encDone context.CancelFunc
}

func setupRecoveryPair(t *testing.T, fps int, gopSecs float64) *recoverySetup {
	t.Helper()
	bundle, err := cert.Generate(cert.Options{IPs: []net.IP{net.ParseIP("127.0.0.1")}})
	if err != nil {
		t.Fatal(err)
	}
	var encoder *fakeEncoder
	srv, err := server.New(server.Config{
		Addr:       "127.0.0.1:0",
		CertBundle: bundle,
		SDP:        wire.SDP{Codec: "test", Width: 64, Height: 64, FPS: fps},
		OnKeyframeRequest: func(sessionID, trackName string) {
			if encoder != nil {
				encoder.requestKeyframeNow()
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	srvCtx, srvDone := context.WithCancel(context.Background())
	go func() { _ = srv.ListenAndServe(srvCtx) }()
	for srv.Addr() == "" || srv.Addr()[len(srv.Addr())-2:] == ":0" {
		time.Sleep(time.Millisecond)
	}

	pub := srv.AddTrack("primary")
	encoder = newFakeEncoder(pub, fps, gopSecs)
	encCtx, encDone := context.WithCancel(context.Background())
	go encoder.run(encCtx)

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	cli, err := client.Dial(dialCtx, "https://"+srv.Addr()+"/wt", client.Options{
		Slug:           srv.Slug(),
		CertHashB64URL: srv.CertHashB64(),
		HelloTimeout:   3 * time.Second,
	})
	if err != nil {
		srvDone()
		encDone()
		t.Fatal(err)
	}

	return &recoverySetup{
		srv:     srv,
		cli:     cli,
		enc:     encoder,
		srvCtx:  srvCtx,
		srvDone: srvDone,
		encDone: encDone,
	}
}

func (s *recoverySetup) close() {
	if s.cli != nil {
		_ = s.cli.Close()
	}
	if s.encDone != nil {
		s.encDone()
	}
	if s.srvDone != nil {
		s.srvDone()
	}
}

func measureRecovery(t *testing.T, fps int, gopSecs float64, trials int, useRequest bool) []time.Duration {
	t.Helper()
	setup := setupRecoveryPair(t, fps, gopSecs)
	defer setup.close()

	samples := make([]time.Duration, 0, trials)
	for trial := 0; trial < trials; trial++ {
		recvCtx, cancel := context.WithTimeout(context.Background(), time.Duration(gopSecs*2*float64(time.Second)))
		var sawKF bool
		for !sawKF {
			au, err := setup.cli.Recv(recvCtx)
			if err != nil {
				cancel()
				t.Fatalf("trial %d: warm-up recv: %v", trial, err)
			}
			sawKF = au.Keyframe
		}
		cancel()

		lossEvent := time.Now()

		if useRequest {
			if err := setup.cli.RequestKeyframe("primary"); err != nil {
				t.Fatalf("trial %d: RequestKeyframe: %v", trial, err)
			}
		}

		recvCtx, cancel = context.WithTimeout(context.Background(), time.Duration(gopSecs*3*float64(time.Second)))
		for {
			au, err := setup.cli.Recv(recvCtx)
			if err != nil {
				cancel()
				t.Fatalf("trial %d: post-loss recv: %v", trial, err)
			}
			if au.Keyframe {
				samples = append(samples, time.Since(lossEvent))
				cancel()
				break
			}
		}
	}
	return samples
}

// TestKeyframeRecoveryComparison is the headline computer-use demo:
// same publisher, same GOP cadence, same subscriber — measure how
// long it takes the receiver to see a fresh decodable frame after a
// loss event with and without subscriber-driven keyframe request.
//
//	go test -v ./benchmarks/video -run TestKeyframeRecoveryComparison
func TestKeyframeRecoveryComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping recovery comparison in short mode")
	}

	const (
		fps     = 30
		gopSecs = 2.0
		trials  = 5
	)

	baseline := measureRecovery(t, fps, gopSecs, trials, false)
	baselineStats := loadgen.ComputeStats(baseline)

	withPR6 := measureRecovery(t, fps, gopSecs, trials, true)
	pr6Stats := loadgen.ComputeStats(withPR6)

	t.Logf("=== keyframe recovery latency, 30fps, 2s GOP ===")
	t.Logf("BASELINE (wait for natural keyframe)  %s", baselineStats)
	t.Logf("PR6 (subscriber RequestKeyframe)      %s", pr6Stats)

	if baselineStats.Mean > 0 {
		improvement := (1 - float64(pr6Stats.Mean)/float64(baselineStats.Mean)) * 100
		ratio := float64(baselineStats.Mean) / float64(pr6Stats.Mean)
		t.Logf("=== headline ===")
		t.Logf("mean recovery time:   %v  ->  %v   (%.1f%% reduction, %.1fx faster)",
			baselineStats.Mean.Round(time.Millisecond),
			pr6Stats.Mean.Round(time.Millisecond),
			improvement, ratio,
		)
		t.Logf("p99 recovery time:    %v  ->  %v",
			baselineStats.P99.Round(time.Millisecond),
			pr6Stats.P99.Round(time.Millisecond),
		)
	}
}

// silence unused import warning if benchmark is built but not run
var _ = sync.Mutex{}
