// Live broadcast / SFU fan-out benchmark — the use case for the
// relay deployment shape, not direct native.
//
// Workload: 1 publisher → N subscribers. Publisher emits a paced
// video stream; each subscriber receives a copy. We measure the
// per-subscriber end-to-end latency p99 (and the worst-subscriber
// p99 across the fan-out), plus how the latencies scale with N.
//
// WebRTC topology: a minimal pion-based SFU. One PeerConnection
// from publisher to SFU; N PeerConnections from SFU to subscribers.
//
// quicrtc topology: the native relay (relay package) subscribes to
// one upstream publisher and fans every AccessUnit out to local
// downstream subscribers via the embedded server.Server. Wire
// format is the same end-to-end — subscribers can't tell they're
// talking to a relay vs a direct publisher.
package fanout

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/sachinkesiraju/quicrtc/testing/benchmarks/internal/loadgen"
	"github.com/sachinkesiraju/quicrtc/cert"
	"github.com/sachinkesiraju/quicrtc/client"
	"github.com/sachinkesiraju/quicrtc/peerconnection"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	relaylib "github.com/sachinkesiraju/quicrtc/relay"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/track"
	"github.com/sachinkesiraju/quicrtc/transport"
	"github.com/sachinkesiraju/quicrtc/wire"
)

const (
	bcastN         = 4
	bcastFrames    = 60
	bcastFrameSize = 50 * 1024
	bcastPace      = 33 * time.Millisecond
	bcastTimeout   = 30 * time.Second
)

type broadcastResult struct {
	perSub    []loadgen.LatencyStats
	worstP99  time.Duration
	bestP99   time.Duration
	totalRcvd int
	totalSent int
}

func (r broadcastResult) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "\n  TOTAL recv=%d/%d  worst-p99=%s  best-p99=%s",
		r.totalRcvd, r.totalSent,
		r.worstP99.Round(time.Microsecond),
		r.bestP99.Round(time.Microsecond))
	for i, s := range r.perSub {
		fmt.Fprintf(&b, "\n  sub %d %s", i, s)
	}
	return b.String()
}

func setupPionSFU(t *testing.T, n int) (
	publisherDC *webrtc.DataChannel,
	subRecvCh []chan []byte,
	cleanup func(),
) {
	t.Helper()
	cfg := webrtc.Configuration{ICEServers: []webrtc.ICEServer{}}

	cleanups := []func(){}

	pubPC, err := webrtc.NewPeerConnection(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cleanups = append(cleanups, func() { pubPC.Close() })
	sfuRecvPC, err := webrtc.NewPeerConnection(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cleanups = append(cleanups, func() { sfuRecvPC.Close() })

	wireICE(pubPC, sfuRecvPC)

	pubDC, err := pubPC.CreateDataChannel("video", nil)
	if err != nil {
		t.Fatal(err)
	}

	sfuSendDCs := make([]*webrtc.DataChannel, n)

	sfuRecvPC.OnDataChannel(func(dc *webrtc.DataChannel) {
		dc.OnMessage(func(m webrtc.DataChannelMessage) {
			payload := append([]byte(nil), m.Data...)
			for _, sd := range sfuSendDCs {
				if sd == nil || sd.ReadyState() != webrtc.DataChannelStateOpen {
					continue
				}
				_ = sd.Send(payload)
			}
		})
	})

	subRecvCh = make([]chan []byte, n)
	openWG := sync.WaitGroup{}
	openWG.Add(2 * n)
	pubReadyOpen := sync.WaitGroup{}
	pubReadyOpen.Add(1)
	pubDC.OnOpen(pubReadyOpen.Done)

	for i := 0; i < n; i++ {
		sfuSendPC, err := webrtc.NewPeerConnection(cfg)
		if err != nil {
			t.Fatal(err)
		}
		cleanups = append(cleanups, func() { sfuSendPC.Close() })

		subPC, err := webrtc.NewPeerConnection(cfg)
		if err != nil {
			t.Fatal(err)
		}
		cleanups = append(cleanups, func() { subPC.Close() })

		wireICE(sfuSendPC, subPC)

		sendDC, err := sfuSendPC.CreateDataChannel(fmt.Sprintf("sub-%d", i), nil)
		if err != nil {
			t.Fatal(err)
		}
		sfuSendDCs[i] = sendDC
		sendDC.OnOpen(openWG.Done)

		ch := make(chan []byte, 256)
		subRecvCh[i] = ch
		subPC.OnDataChannel(func(dc *webrtc.DataChannel) {
			dc.OnOpen(openWG.Done)
			dc.OnMessage(func(m webrtc.DataChannelMessage) {
				ch <- append([]byte(nil), m.Data...)
			})
		})

		go func() {
			offer, _ := sfuSendPC.CreateOffer(nil)
			_ = sfuSendPC.SetLocalDescription(offer)
			_ = subPC.SetRemoteDescription(offer)
			answer, _ := subPC.CreateAnswer(nil)
			_ = subPC.SetLocalDescription(answer)
			_ = sfuSendPC.SetRemoteDescription(answer)
		}()
	}

	go func() {
		offer, _ := pubPC.CreateOffer(nil)
		_ = pubPC.SetLocalDescription(offer)
		_ = sfuRecvPC.SetRemoteDescription(offer)
		answer, _ := sfuRecvPC.CreateAnswer(nil)
		_ = sfuRecvPC.SetLocalDescription(answer)
		_ = pubPC.SetRemoteDescription(answer)
	}()

	openCh := make(chan struct{})
	go func() {
		openWG.Wait()
		pubReadyOpen.Wait()
		close(openCh)
	}()
	select {
	case <-openCh:
	case <-time.After(60 * time.Second):
		t.Fatal("pion SFU: data channels didn't all open in 60s")
	}

	cleanup = func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}
	return pubDC, subRecvCh, cleanup
}

func wireICE(a, b *webrtc.PeerConnection) {
	a.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		_ = b.AddICECandidate(c.ToJSON())
	})
	b.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		_ = a.AddICECandidate(c.ToJSON())
	})
}

func TestLiveBroadcastPionSFU(t *testing.T) {
	pubDC, subRecvCh, cleanup := setupPionSFU(t, bcastN)
	defer cleanup()

	res := runLiveBroadcast(t, func(t time.Time) error {
		return pubDC.Send(loadgen.TimedPayload(bcastFrameSize, t))
	}, subRecvCh)
	t.Logf("pion SFU (N=%d)%s", bcastN, res)
}

func setupQuicrtcRelayFanout(t *testing.T, n int) (
	publisherSender transport.Sender,
	subRecvCh []chan []byte,
	cleanup func(),
) {
	t.Helper()
	loopback := net.ParseIP("127.0.0.1")
	cleanups := []func(){}
	addCleanup := func(f func()) { cleanups = append(cleanups, f) }
	cleanup = func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pubBundle, err := cert.Generate(cert.Options{IPs: []net.IP{loopback}})
	if err != nil {
		t.Fatal(err)
	}
	pubPC, err := peerconnection.New(peerconnection.Config{
		Transport: peerconnection.TransportNativeOnly,
		Native: &peerconnection.NativeConfig{
			Role: transport.RolePublisher,
			PublisherConfig: server.Config{
				Addr:         "127.0.0.1:0",
				CertBundle:   pubBundle,
				ExternalHost: "127.0.0.1",
				SDP:          wire.SDP{Codec: "test", Width: 64, Height: 64, FPS: 30},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	addCleanup(func() { pubPC.Close() })
	if err := pubPC.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	sender, err := pubPC.AddTrack(ctx, track.Video("primary", "test"))
	if err != nil {
		t.Fatal(err)
	}
	pubSrvAdapter, ok := pubPC.Transport().(interface{ Server() *server.Server })
	if !ok {
		t.Fatal("publisher Transport doesn't expose Server")
	}
	pubSrv := pubSrvAdapter.Server()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && strings.HasSuffix(pubSrv.Addr(), ":0") {
		time.Sleep(time.Millisecond)
	}
	if strings.HasSuffix(pubSrv.Addr(), ":0") {
		t.Fatal("publisher didn't bind")
	}
	pubShareLink := pubSrv.ShareLink()

	relayBundle, err := cert.Generate(cert.Options{IPs: []net.IP{loopback}})
	if err != nil {
		t.Fatal(err)
	}
	rly, err := relaylib.New(relaylib.Config{
		Server: server.Config{
			Addr:         "127.0.0.1:0",
			CertBundle:   relayBundle,
			ExternalHost: "127.0.0.1",
			SDP:          wire.SDP{Codec: "test", Width: 64, Height: 64, FPS: 30},
		},
		Upstreams:     []string{pubShareLink},
		ClientOptions: client.Options{HelloTimeout: 5 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	relayCtx, relayCancel := context.WithCancel(context.Background())
	addCleanup(relayCancel)
	go func() { _ = rly.ListenAndServe(relayCtx) }()
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && strings.HasSuffix(rly.Server().Addr(), ":0") {
		time.Sleep(time.Millisecond)
	}
	if strings.HasSuffix(rly.Server().Addr(), ":0") {
		t.Fatal("relay didn't bind")
	}
	time.Sleep(200 * time.Millisecond)

	relaySubURL := "https://" + rly.Server().Addr() + "/wt"
	relaySlug := rly.Server().Slug()
	relayHash := rly.Server().CertHashB64()

	subRecvCh = make([]chan []byte, n)
	for i := 0; i < n; i++ {
		subPC, err := peerconnection.New(peerconnection.Config{
			Transport: peerconnection.TransportNativeOnly,
			Native: &peerconnection.NativeConfig{
				Role:          transport.RoleSubscriber,
				SubscriberURL: relaySubURL,
				SubscriberOpts: client.Options{
					Slug:           relaySlug,
					CertHashB64URL: relayHash,
					HelloTimeout:   10 * time.Second,
				},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		addCleanup(func() { subPC.Close() })
		if err := subPC.Connect(ctx); err != nil {
			t.Fatal(err)
		}
		ch := make(chan []byte, 256)
		subRecvCh[i] = ch
		go func(pc *peerconnection.PeerConnection) {
			for {
				au, err := pc.Recv(context.Background())
				if err != nil {
					return
				}
				ch <- au.Bytes
			}
		}(subPC)
	}

	return sender, subRecvCh, cleanup
}

func TestLiveBroadcastQuicrtcRelay(t *testing.T) {
	sender, subRecvCh, cleanup := setupQuicrtcRelayFanout(t, bcastN)
	defer cleanup()

	frameIdx := 0
	res := runLiveBroadcast(t, func(t time.Time) error {
		isKey := frameIdx%30 == 0
		frameIdx++
		return sender.Send(context.Background(), pubsub.AccessUnit{
			Bytes:    loadgen.TimedPayload(bcastFrameSize, t),
			Keyframe: isKey,
			Seq:      uint32(frameIdx),
		})
	}, subRecvCh)
	t.Logf("quicrtc relay (N=%d)%s", bcastN, res)
}

func runLiveBroadcast(t *testing.T,
	send func(time.Time) error,
	subRecvCh []chan []byte,
) broadcastResult {
	t.Helper()
	n := len(subRecvCh)
	perSubE2E := make([][]time.Duration, n)
	totalRcvd := 0
	var rcvdMu sync.Mutex
	stopAt := time.Now().Add(time.Duration(bcastFrames+10)*bcastPace + 2*time.Second)

	wg := sync.WaitGroup{}
	for i := 0; i < n; i++ {
		i := i
		ch := subRecvCh[i]
		perSubE2E[i] = make([]time.Duration, 0, bcastFrames)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case msg := <-ch:
					now := time.Now()
					sentAt := time.Unix(0, int64(binary.BigEndian.Uint64(msg[:8])))
					perSubE2E[i] = append(perSubE2E[i], now.Sub(sentAt))
					rcvdMu.Lock()
					totalRcvd++
					rcvdMu.Unlock()
				case <-time.After(time.Until(stopAt)):
					return
				}
				if len(perSubE2E[i]) >= bcastFrames {
					return
				}
			}
		}()
	}

	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)
		ticker := time.NewTicker(bcastPace)
		defer ticker.Stop()
		for i := 0; i < bcastFrames; i++ {
			if err := send(time.Now()); err != nil {
				return
			}
			if i < bcastFrames-1 {
				<-ticker.C
			}
		}
	}()

	<-sendDone
	wg.Wait()

	perSub := make([]loadgen.LatencyStats, n)
	worstP99 := time.Duration(0)
	bestP99 := time.Hour
	for i := 0; i < n; i++ {
		perSub[i] = loadgen.ComputeStats(perSubE2E[i])
		if perSub[i].P99 > worstP99 {
			worstP99 = perSub[i].P99
		}
		if perSub[i].P99 > 0 && perSub[i].P99 < bestP99 {
			bestP99 = perSub[i].P99
		}
	}
	return broadcastResult{
		perSub:    perSub,
		worstP99:  worstP99,
		bestP99:   bestP99,
		totalRcvd: totalRcvd,
		totalSent: bcastFrames * n,
	}
}
