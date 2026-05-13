// Multi-stream isolation benchmark: AI workload modeling a multi-
// modal agent with concurrent video + token streams. We saturate
// the video stream with large back-to-back frames and measure
// whether the token stream's latency is impacted.
//
// Why this matters for AI: a multi-modal agent often emits a
// chunky video stream alongside a fast token stream. If a head-of-
// line block on one channel stalls the other, voice/text feel laggy
// when video frames are heavy.
//
// Architectural prediction:
//
//   - WebRTC data channels share a single SCTP association over
//     DTLS. SCTP can be configured for unordered/unreliable, but
//     the underlying chunks compete for the same association.
//     Multiple ordered+reliable channels suffer head-of-line
//     blocking when one channel has a backlog.
//
//   - quicrtc native uses separate QUIC streams per track. Per-
//     stream FIFO; no cross-stream HOL. The slow video stream
//     should NOT delay the fast token stream.
//
// We test that prediction here.
package multimodal

import (
	"context"
	"encoding/binary"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/sachinkesiraju/quicrtc/benchmarks/internal/loadgen"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/track"
)

const (
	mmTokens         = 400
	mmVideoFrames    = 60
	mmTokenSize      = 64
	mmVideoFrameSize = 50 * 1024
	mmTokenPace      = 5 * time.Millisecond
	mmVideoPace      = 33 * time.Millisecond
	mmKeyframeEvery  = 30
	mmTimeout        = 30 * time.Second
)

type multimodalResult struct {
	tokenLatency loadgen.LatencyStats
	videoLatency loadgen.LatencyStats
	tokenJitter  loadgen.LatencyStats
	tokensRcvd   int
	videoRcvd    int
	tokensSent   int
	videoSent    int
}

func (r multimodalResult) String() string {
	var b strings.Builder
	b.WriteString("\n  TOKEN  recv=" + itoaPct(r.tokensRcvd, r.tokensSent) + "  end-to-end ")
	b.WriteString(r.tokenLatency.String())
	b.WriteString("\n  TOKEN  inter-arrival ")
	b.WriteString(r.tokenJitter.String())
	b.WriteString("\n  VIDEO  recv=" + itoaPct(r.videoRcvd, r.videoSent) + "  end-to-end ")
	b.WriteString(r.videoLatency.String())
	return b.String()
}

func itoaPct(got, total int) string {
	if total == 0 {
		return "0/0"
	}
	pct := 100 * got / total
	return strings.Join([]string{
		ftoa(got), "/", ftoa(total), " (", ftoa(pct), "%)",
	}, "")
}
func ftoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var digits [20]byte
	i := len(digits)
	for n > 0 {
		i--
		digits[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		digits[i] = '-'
	}
	return string(digits[i:])
}

func TestMultimodalIsolationPionParallel(t *testing.T) {
	tokenPair, err := loadgen.NewPionPair("tokens")
	if err != nil {
		t.Fatal(err)
	}
	defer tokenPair.Close()
	videoPair, err := loadgen.NewPionPair("video")
	if err != nil {
		t.Fatal(err)
	}
	defer videoPair.Close()

	tokenRecv := make(chan []byte, mmTokens+1)
	videoRecv := make(chan []byte, mmVideoFrames+1)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := tokenPair.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := videoPair.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	tokenPair.AnswerDC.OnMessage(func(m webrtc.DataChannelMessage) {
		tokenRecv <- append([]byte(nil), m.Data...)
	})
	videoPair.AnswerDC.OnMessage(func(m webrtc.DataChannelMessage) {
		videoRecv <- append([]byte(nil), m.Data...)
	})

	res := runMultimodalIsolation(t,
		func(t time.Time) error { return tokenPair.OfferDC.Send(loadgen.TimedPayload(mmTokenSize, t)) },
		func(t time.Time) error { return videoPair.OfferDC.Send(loadgen.TimedPayload(mmVideoFrameSize, t)) },
		tokenRecv, videoRecv)
	t.Logf("pion (parallel PCs) %s", res)
}

func TestMultimodalIsolationPionShared(t *testing.T) {
	cfg := webrtc.Configuration{ICEServers: []webrtc.ICEServer{}}
	offerer, err := webrtc.NewPeerConnection(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer offerer.Close()
	answerer, err := webrtc.NewPeerConnection(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer answerer.Close()

	offerer.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		_ = answerer.AddICECandidate(c.ToJSON())
	})
	answerer.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		_ = offerer.AddICECandidate(c.ToJSON())
	})

	tokenDC, err := offerer.CreateDataChannel("tokens", nil)
	if err != nil {
		t.Fatal(err)
	}
	videoDC, err := offerer.CreateDataChannel("video", nil)
	if err != nil {
		t.Fatal(err)
	}

	var openMu sync.Mutex
	openCount := 0
	openCh := make(chan struct{})
	openOnce := func() {
		openMu.Lock()
		openCount++
		done := openCount == 4
		openMu.Unlock()
		if done {
			close(openCh)
		}
	}

	tokenRecv := make(chan []byte, mmTokens+1)
	videoRecv := make(chan []byte, mmVideoFrames+1)

	tokenDC.OnOpen(openOnce)
	videoDC.OnOpen(openOnce)
	answerer.OnDataChannel(func(dc *webrtc.DataChannel) {
		dc.OnOpen(openOnce)
		switch dc.Label() {
		case "tokens":
			dc.OnMessage(func(m webrtc.DataChannelMessage) {
				tokenRecv <- append([]byte(nil), m.Data...)
			})
		case "video":
			dc.OnMessage(func(m webrtc.DataChannelMessage) {
				videoRecv <- append([]byte(nil), m.Data...)
			})
		}
	})

	offer, _ := offerer.CreateOffer(nil)
	_ = offerer.SetLocalDescription(offer)
	_ = answerer.SetRemoteDescription(offer)
	answer, _ := answerer.CreateAnswer(nil)
	_ = answerer.SetLocalDescription(answer)
	_ = offerer.SetRemoteDescription(answer)

	select {
	case <-openCh:
	case <-time.After(30 * time.Second):
		t.Fatal("data channels didn't open")
	}

	res := runMultimodalIsolation(t,
		func(t time.Time) error { return tokenDC.Send(loadgen.TimedPayload(mmTokenSize, t)) },
		func(t time.Time) error { return videoDC.Send(loadgen.TimedPayload(mmVideoFrameSize, t)) },
		tokenRecv, videoRecv)
	t.Logf("pion (shared PC)    %s", res)
}

func TestMultimodalIsolationQuicrtcNative(t *testing.T) {
	pair, err := loadgen.NewNativePair()
	if err != nil {
		t.Fatal(err)
	}
	defer pair.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := pair.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	pubDC, err := pair.PubPC.DataChannel()
	if err != nil {
		t.Fatal(err)
	}
	subDC, err := pair.SubPC.DataChannel()
	if err != nil {
		t.Fatal(err)
	}

	tokenRecv := make(chan []byte, mmTokens+1)
	go func() {
		for {
			msg, err := subDC.Recv(context.Background())
			if err != nil {
				return
			}
			tokenRecv <- msg
		}
	}()

	videoSender, err := pair.PubPC.AddTrack(ctx, track.Video("primary", "test"))
	if err != nil {
		t.Fatal(err)
	}

	videoRecv := make(chan []byte, mmVideoFrames+1)
	go func() {
		for {
			au, err := pair.SubPC.Recv(context.Background())
			if err != nil {
				return
			}
			videoRecv <- au.Bytes
		}
	}()

	frameIdx := 0
	res := runMultimodalIsolation(t,
		func(t time.Time) error { return pubDC.Send(loadgen.TimedPayload(mmTokenSize, t)) },
		func(t time.Time) error {
			isKey := frameIdx%mmKeyframeEvery == 0
			frameIdx++
			return videoSender.Send(ctx, pubsub.AccessUnit{
				Bytes:    loadgen.TimedPayload(mmVideoFrameSize, t),
				Keyframe: isKey,
			})
		},
		tokenRecv, videoRecv)
	t.Logf("quicrtc native (1-session, 2-stream)  %s", res)
}

func TestMultimodalIsolationQuicrtcMultiTrack(t *testing.T) {
	pair, err := loadgen.NewNativePair()
	if err != nil {
		t.Fatal(err)
	}
	defer pair.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := pair.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	videoSender, err := pair.PubPC.AddTrack(ctx, track.Video("video", "test"))
	if err != nil {
		t.Fatal(err)
	}
	tokensSender, err := pair.PubPC.AddTrack(ctx, track.Tokens("tokens"))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	tokenRecv := make(chan []byte, mmTokens+1)
	go func() {
		for {
			au, err := pair.SubPC.RecvOn(context.Background(), "tokens")
			if err != nil {
				return
			}
			tokenRecv <- au.Bytes
		}
	}()

	videoRecv := make(chan []byte, mmVideoFrames+1)
	go func() {
		for {
			au, err := pair.SubPC.RecvOn(context.Background(), "video")
			if err != nil {
				return
			}
			videoRecv <- au.Bytes
		}
	}()

	frameIdx := 0
	res := runMultimodalIsolation(t,
		func(t time.Time) error {
			return tokensSender.Send(ctx, pubsub.AccessUnit{
				Bytes:    loadgen.TimedPayload(mmTokenSize, t),
				Keyframe: true,
			})
		},
		func(t time.Time) error {
			isKey := frameIdx%mmKeyframeEvery == 0
			frameIdx++
			return videoSender.Send(ctx, pubsub.AccessUnit{
				Bytes:    loadgen.TimedPayload(mmVideoFrameSize, t),
				Keyframe: isKey,
			})
		},
		tokenRecv, videoRecv)
	t.Logf("quicrtc multi-track (1-PC, 2-AddTrack)  %s", res)
}

// runMultimodalIsolation drives both senders concurrently and
// measures token + video latencies. The video stream is saturated
// (back-to-back); the token stream is paced (5ms). The interesting
// metric is whether token end-to-end / inter-arrival latency stays
// flat despite the video stream eating bandwidth.
func runMultimodalIsolation(t *testing.T,
	sendToken, sendVideo func(time.Time) error,
	tokenRecv, videoRecv <-chan []byte,
) multimodalResult {
	t.Helper()

	tokenE2E := make([]time.Duration, 0, mmTokens)
	tokenJitter := make([]time.Duration, 0, mmTokens)
	videoE2E := make([]time.Duration, 0, mmVideoFrames)

	stopAt := time.Now().Add(time.Duration(mmTokens)*mmTokenPace + 2*time.Second)

	tokenDone := make(chan struct{})
	videoDone := make(chan struct{})

	go func() {
		defer close(tokenDone)
		var prev time.Time
		for {
			select {
			case msg := <-tokenRecv:
				now := time.Now()
				sentAt := time.Unix(0, int64(binary.BigEndian.Uint64(msg[:8])))
				tokenE2E = append(tokenE2E, now.Sub(sentAt))
				if !prev.IsZero() {
					tokenJitter = append(tokenJitter, now.Sub(prev))
				}
				prev = now
			case <-time.After(time.Until(stopAt) + 500*time.Millisecond):
				return
			}
			if len(tokenE2E) >= mmTokens {
				return
			}
		}
	}()

	go func() {
		defer close(videoDone)
		for {
			select {
			case msg := <-videoRecv:
				now := time.Now()
				sentAt := time.Unix(0, int64(binary.BigEndian.Uint64(msg[:8])))
				videoE2E = append(videoE2E, now.Sub(sentAt))
			case <-time.After(time.Until(stopAt) + 1*time.Second):
				return
			}
			if len(videoE2E) >= mmVideoFrames {
				return
			}
		}
	}()

	tokenSent := make(chan struct{})
	videoSent := make(chan struct{})

	go func() {
		defer close(tokenSent)
		ticker := time.NewTicker(mmTokenPace)
		defer ticker.Stop()
		for i := 0; i < mmTokens; i++ {
			if err := sendToken(time.Now()); err != nil {
				return
			}
			if i < mmTokens-1 {
				<-ticker.C
			}
		}
	}()

	go func() {
		defer close(videoSent)
		ticker := time.NewTicker(mmVideoPace)
		defer ticker.Stop()
		for i := 0; i < mmVideoFrames; i++ {
			if err := sendVideo(time.Now()); err != nil {
				return
			}
			if i < mmVideoFrames-1 {
				<-ticker.C
			}
		}
	}()

	<-tokenSent
	<-videoSent
	<-tokenDone
	<-videoDone

	return multimodalResult{
		tokenLatency: loadgen.ComputeStats(tokenE2E),
		tokenJitter:  loadgen.ComputeStats(tokenJitter),
		videoLatency: loadgen.ComputeStats(videoE2E),
		tokensRcvd:   len(tokenE2E),
		videoRcvd:    len(videoE2E),
		tokensSent:   mmTokens,
		videoSent:    mmVideoFrames,
	}
}
