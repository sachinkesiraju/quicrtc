// Pion + RTCP PLI baseline for the keyframe-recovery benchmark.
//
// This is the third measurement path requested by reviewers as a
// production-WebRTC steelman for the Row 4 video-recovery comparison.
// The existing keyframe_recovery_test.go file compares two quicrtc-
// internal arms (passive wait vs Client.RequestKeyframe). That misses
// what real WebRTC does on packet loss: the subscriber emits an RTCP
// PictureLossIndication (PLI) on its feedback channel; the publisher's
// encoder sees the PLI and force-flushes a fresh keyframe.
//
// What we measure here:
//   t0 = subscriber decides it needs a fresh keyframe (simulated loss
//        — we trigger this manually each iteration).
//   t1 = subscriber's RTP read loop observes the next IDR NAL unit
//        (NAL type 5 in the first byte of the RTP payload of a single-
//        NAL packet).
//   sample = t1 - t0.
//
// Wire:
//   - Two pion PeerConnections, in-process ICE candidate exchange (no
//     signalling server), direct SDP swap. Patterned after
//     testing/benchmarks/resume/resume_bench_test.go and
//     testing/benchmarks/internal/loadgen/pion.go.
//   - Publisher: TrackLocalStaticSample with H264 codec. A metronome
//     goroutine emits one IDR every gopSecs and a P-frame every
//     frameDur in between. On PLI receipt (RTPSender.ReadRTCP loop) the
//     metronome resets and the next emitted sample is forced to IDR.
//   - Subscriber: OnTrack fires; we keep the receiver and PC handles.
//     For each iteration we call pc.WriteRTCP with a PLI and then read
//     RTP packets via TrackRemote.ReadRTP until we see an IDR.
//
// Simplifications (called out for honesty):
//   - No real H264 encoder. The "frame" payloads are synthetic byte
//     blobs whose first byte is a valid single-NAL header (0x65 = IDR,
//     0x41 = non-IDR). The pion H264 packetizer treats these as plain
//     single-NAL units (small enough to skip FU-A fragmentation), so
//     the wire bytes received on the other end have the NAL header as
//     their first byte. We never send SPS/PPS, so the STAP-A path is
//     bypassed.
//   - No real packet-loss simulation. The subscriber just decides
//     "I need a keyframe now" each iteration, which is the same trigger
//     a production receiver would have after detecting decode failure.
//
// Comparison target: the existing PR6 arm (Client.RequestKeyframe) from
// TestKeyframeRecoveryComparison. Same GOP cadence, same fps, same
// trial count (n=100 to keep the p99 floor consistent).
package video

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"

	"github.com/sachinkesiraju/quicrtc/testing/benchmarks/internal/loadgen"
)

const (
	naluTypeIDR    byte = 5 // IDR slice — treated as the keyframe sentinel
	naluTypeNonIDR byte = 1 // non-IDR slice — P-frame sentinel
	naluTypeMask   byte = 0x1F
)

// naluHeader returns a single-byte NAL header with F=0, NRI=11 for IDR
// and NRI=10 for non-IDR. This is what real H264 encoders emit on the
// first byte of an Annex-B-stripped NAL.
func naluHeader(t byte) byte {
	switch t {
	case naluTypeIDR:
		return 0x65 // 0b0110_0101 = F=0 NRI=3 Type=5
	default:
		return 0x41 // 0b0100_0001 = F=0 NRI=2 Type=1
	}
}

// pionPubSub holds the publisher PC, subscriber PC, the video track on
// each side, and the encoder's keyframe-request signal.
type pionPubSub struct {
	pubPC     *webrtc.PeerConnection
	subPC     *webrtc.PeerConnection
	track     *webrtc.TrackLocalStaticSample
	sender    *webrtc.RTPSender
	remote    *webrtc.TrackRemote
	mediaSSRC uint32

	pliReceived atomic.Uint64 // counts PLIs the publisher's RTCP loop sees
	requestKF   atomic.Bool

	// rtpCh is the subscriber's RTP packet stream. The OnTrack goroutine
	// pushes packets here; iteration loops read from it.
	rtpCh chan rtpPayload

	closeOnce sync.Once
	closed    chan struct{}
}

type rtpPayload struct {
	naluType byte
	when     time.Time
}

func newPionPubSub(ctx context.Context, t *testing.T) (*pionPubSub, error) {
	t.Helper()

	cfg := webrtc.Configuration{ICEServers: []webrtc.ICEServer{}}

	// fireOnTrackBeforeFirstRTP lets OnTrack fire as soon as the
	// remote track is negotiated, instead of waiting for the first
	// RTP packet. Without this, we'd have a chicken-and-egg problem:
	// the encoder waits for ps.remote.SSRC() (set in OnTrack), but
	// OnTrack waits for media which the encoder hasn't started yet.
	se := webrtc.SettingEngine{}
	se.SetFireOnTrackBeforeFirstRTP(true)
	api := webrtc.NewAPI(webrtc.WithSettingEngine(se))

	pubPC, err := api.NewPeerConnection(cfg)
	if err != nil {
		return nil, err
	}
	subPC, err := api.NewPeerConnection(cfg)
	if err != nil {
		pubPC.Close()
		return nil, err
	}

	ps := &pionPubSub{
		pubPC:  pubPC,
		subPC:  subPC,
		rtpCh:  make(chan rtpPayload, 256),
		closed: make(chan struct{}),
	}

	// ICE candidate trickle, in-process.
	pubPC.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		_ = subPC.AddICECandidate(c.ToJSON())
	})
	subPC.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		_ = pubPC.AddICECandidate(c.ToJSON())
	})

	// Publisher: add an H264 video track.
	track, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000},
		"video", "stream",
	)
	if err != nil {
		ps.Close()
		return nil, err
	}
	sender, err := pubPC.AddTrack(track)
	if err != nil {
		ps.Close()
		return nil, err
	}
	ps.track = track
	ps.sender = sender

	// Subscriber: register OnTrack to capture the remote track + drain RTP.
	trackReady := make(chan struct{})
	subPC.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		ps.remote = remote
		ps.mediaSSRC = uint32(remote.SSRC())
		close(trackReady)
		go func() {
			for {
				select {
				case <-ps.closed:
					return
				default:
				}
				pkt, _, rerr := remote.ReadRTP()
				if rerr != nil {
					if errors.Is(rerr, io.EOF) {
						return
					}
					return
				}
				if len(pkt.Payload) == 0 {
					continue
				}
				// Single-NAL units (which is what our metronome emits
				// because the payloads are small) have the NAL header as
				// payload[0]. FU-A would put the original type in
				// payload[1]'s low 5 bits — we don't expect to hit that
				// path with our 32-byte payloads, but handle it defensively.
				first := pkt.Payload[0]
				nt := first & naluTypeMask
				if nt == 28 || nt == 29 { // FU-A / FU-B
					if len(pkt.Payload) < 2 {
						continue
					}
					nt = pkt.Payload[1] & naluTypeMask
					// Only surface the first fragment of an FU-A so we
					// don't count an IDR twice.
					if pkt.Payload[1]&0x80 == 0 {
						continue
					}
				}
				// STAP-A (type 24): rare for our payloads but if a real
				// encoder slipped through, peek at the first aggregated NAL.
				if nt == 24 && len(pkt.Payload) >= 4 {
					nt = pkt.Payload[3] & naluTypeMask
				}
				select {
				case ps.rtpCh <- rtpPayload{naluType: nt, when: time.Now()}:
				default:
					// drop on backpressure; we only care about timing
					// for the next IDR after a PLI.
				}
			}
		}()
	})

	// Publisher: drain RTCP from the sender and count PLIs.
	go func() {
		for {
			select {
			case <-ps.closed:
				return
			default:
			}
			pkts, _, rerr := sender.ReadRTCP()
			if rerr != nil {
				return
			}
			for _, p := range pkts {
				if _, ok := p.(*rtcp.PictureLossIndication); ok {
					ps.pliReceived.Add(1)
					ps.requestKF.Store(true)
				}
			}
		}
	}()

	// SDP offer/answer.
	offer, err := pubPC.CreateOffer(nil)
	if err != nil {
		ps.Close()
		return nil, err
	}
	if err := pubPC.SetLocalDescription(offer); err != nil {
		ps.Close()
		return nil, err
	}
	if err := subPC.SetRemoteDescription(offer); err != nil {
		ps.Close()
		return nil, err
	}
	answer, err := subPC.CreateAnswer(nil)
	if err != nil {
		ps.Close()
		return nil, err
	}
	if err := subPC.SetLocalDescription(answer); err != nil {
		ps.Close()
		return nil, err
	}
	if err := pubPC.SetRemoteDescription(answer); err != nil {
		ps.Close()
		return nil, err
	}

	// Wait for OnTrack to fire (i.e. media flow to start).
	select {
	case <-trackReady:
	case <-ctx.Done():
		ps.Close()
		return nil, fmt.Errorf("OnTrack didn't fire before context done: %w", ctx.Err())
	case <-time.After(10 * time.Second):
		ps.Close()
		return nil, fmt.Errorf("OnTrack didn't fire within 10s")
	}

	return ps, nil
}

func (p *pionPubSub) Close() {
	p.closeOnce.Do(func() {
		close(p.closed)
		if p.subPC != nil {
			_ = p.subPC.Close()
		}
		if p.pubPC != nil {
			_ = p.pubPC.Close()
		}
	})
}

// pionEncoder is the publisher-side metronome. It writes one IDR per
// gopDuration and a P-frame every frameDur in between. On
// requestKeyframeNow() it forces the *next* sample to be an IDR and
// resets the GOP clock.
type pionEncoder struct {
	ps          *pionPubSub
	gopDuration time.Duration
	frameDur    time.Duration
}

func (e *pionEncoder) run(ctx context.Context) {
	ticker := time.NewTicker(e.frameDur)
	defer ticker.Stop()
	// Force first frame to be an IDR by setting lastKF in the past.
	lastKF := time.Now().Add(-e.gopDuration)
	// Each sample is 32 bytes — well under any reasonable MTU, so the
	// H264 packetizer takes the single-NAL fast path.
	const payloadSize = 32
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			natural := now.Sub(lastKF) >= e.gopDuration
			requested := e.ps.requestKF.CompareAndSwap(true, false)
			isKF := natural || requested
			payload := make([]byte, payloadSize)
			if isKF {
				payload[0] = naluHeader(naluTypeIDR)
				lastKF = now
			} else {
				payload[0] = naluHeader(naluTypeNonIDR)
			}
			// Fill the rest with non-zero, non-start-code bytes so emitNalus
			// doesn't try to split this into multiple NALs.
			for i := 1; i < payloadSize; i++ {
				payload[i] = 0xAA
			}
			_ = e.ps.track.WriteSample(media.Sample{
				Data:     payload,
				Duration: e.frameDur,
			})
		}
	}
}

// TestKeyframeRecoveryPionPLI measures keyframe-recovery latency for a
// production-style pion + RTCP PLI loop. See the file-level comment for
// the wire/measurement description.
//
//	go test -v ./testing/benchmarks/video -run TestKeyframeRecoveryPionPLI -timeout=300s
func TestKeyframeRecoveryPionPLI(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping pion PLI recovery in short mode")
	}

	const (
		fps     = 30
		gopSecs = 2.0
		trials  = 100 // matches the methodology floor used by the quicrtc arm
	)
	frameDur := time.Second / time.Duration(fps)
	gopDur := time.Duration(gopSecs * float64(time.Second))

	setupCtx, setupCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer setupCancel()
	ps, err := newPionPubSub(setupCtx, t)
	if err != nil {
		t.Fatalf("setup pion pub/sub: %v", err)
	}
	defer ps.Close()

	encCtx, encCancel := context.WithCancel(context.Background())
	defer encCancel()
	enc := &pionEncoder{
		ps:          ps,
		gopDuration: gopDur,
		frameDur:    frameDur,
	}
	go enc.run(encCtx)

	// Warm up: drain everything emitted before the first measured trial
	// and require we've seen at least one IDR (so the pipeline is
	// actually flowing end-to-end).
	warmupCtx, warmupCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer warmupCancel()
	sawKF := false
	for !sawKF {
		select {
		case p := <-ps.rtpCh:
			if p.naluType == naluTypeIDR {
				sawKF = true
			}
		case <-warmupCtx.Done():
			t.Fatalf("warmup: never saw initial IDR over the pion track")
		}
	}

	samples := make([]time.Duration, 0, trials)

	for trial := 0; trial < trials; trial++ {
		// Drain whatever's queued so we start each trial clean. We want
		// the measurement to span [PLI -> next IDR], not "I happened to
		// see an IDR from the previous natural GOP boundary".
		drainLoop(ps.rtpCh)

		// Subscriber observes "loss" and requests a fresh keyframe.
		t0 := time.Now()
		if err := ps.subPC.WriteRTCP([]rtcp.Packet{
			&rtcp.PictureLossIndication{MediaSSRC: ps.mediaSSRC},
		}); err != nil {
			t.Fatalf("trial %d: WriteRTCP(PLI): %v", trial, err)
		}

		// Wait for the next IDR on the wire.
		recvCtx, cancel := context.WithTimeout(context.Background(), time.Duration(gopSecs*3*float64(time.Second)))
		var sample time.Duration
		gotKF := false
	WAIT:
		for {
			select {
			case <-recvCtx.Done():
				cancel()
				t.Fatalf("trial %d: waited %s without seeing IDR after PLI", trial, time.Duration(gopSecs*3*float64(time.Second)))
			case p := <-ps.rtpCh:
				if p.naluType == naluTypeIDR {
					sample = p.when.Sub(t0)
					gotKF = true
					break WAIT
				}
			}
		}
		cancel()
		if gotKF {
			samples = append(samples, sample)
		}
	}

	stats := loadgen.ComputeStats(samples)
	t.Logf("=== pion + RTCP PLI keyframe recovery latency, 30fps, 2s GOP ===")
	t.Logf("PION+PLI (subscriber RTCP PictureLossIndication)  %s", stats)
	t.Logf("PLI packets received by publisher: %d / %d trials", ps.pliReceived.Load(), trials)

	// Convenience headline for the README row author.
	t.Logf("=== headline ===")
	t.Logf("pion+PLI recovery:  mean=%v  p50=%v  p99=%v",
		stats.Mean.Round(time.Millisecond),
		stats.P50.Round(time.Millisecond),
		stats.P99.Round(time.Millisecond),
	)
}

func drainLoop(ch <-chan rtpPayload) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}
