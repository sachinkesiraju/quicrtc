package loadgen

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/sachinkesiraju/quicrtc/cert"
	"github.com/sachinkesiraju/quicrtc/client"
	"github.com/sachinkesiraju/quicrtc/peerconnection"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/transport"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// PionPair is two pion PeerConnections on loopback with their ICE/SDP
// exchange wired in-process and a single data channel on each end.
// Used by the pion-comparison benchmarks.
type PionPair struct {
	OfferDC  *webrtc.DataChannel
	AnswerDC *webrtc.DataChannel

	offerer  *webrtc.PeerConnection
	answerer *webrtc.PeerConnection
	openMu   sync.Mutex
	openCh   chan struct{}
}

func NewPionPair(label string) (*PionPair, error) {
	cfg := webrtc.Configuration{ICEServers: []webrtc.ICEServer{}}
	offerer, err := webrtc.NewPeerConnection(cfg)
	if err != nil {
		return nil, err
	}
	answerer, err := webrtc.NewPeerConnection(cfg)
	if err != nil {
		offerer.Close()
		return nil, err
	}
	pair := &PionPair{
		offerer:  offerer,
		answerer: answerer,
		openCh:   make(chan struct{}),
	}

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

	openCount := 0
	openOnce := func() {
		pair.openMu.Lock()
		openCount++
		if openCount == 2 {
			close(pair.openCh)
		}
		pair.openMu.Unlock()
	}

	dc, err := offerer.CreateDataChannel(label, nil)
	if err != nil {
		offerer.Close()
		answerer.Close()
		return nil, err
	}
	pair.OfferDC = dc
	dc.OnOpen(openOnce)

	answerer.OnDataChannel(func(dc *webrtc.DataChannel) {
		pair.AnswerDC = dc
		dc.OnOpen(openOnce)
	})

	return pair, nil
}

func (p *PionPair) Connect(ctx context.Context) error {
	offer, err := p.offerer.CreateOffer(nil)
	if err != nil {
		return err
	}
	if err := p.offerer.SetLocalDescription(offer); err != nil {
		return err
	}
	if err := p.answerer.SetRemoteDescription(offer); err != nil {
		return err
	}
	answer, err := p.answerer.CreateAnswer(nil)
	if err != nil {
		return err
	}
	if err := p.answerer.SetLocalDescription(answer); err != nil {
		return err
	}
	if err := p.offerer.SetRemoteDescription(answer); err != nil {
		return err
	}
	select {
	case <-p.openCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *PionPair) Close() {
	if p.offerer != nil {
		p.offerer.Close()
	}
	if p.answerer != nil {
		p.answerer.Close()
	}
}

// NativePair is a quicrtc PeerConnection pair on loopback used by
// pion-vs-quicrtc comparison benchmarks (multistream isolation,
// SSE baseline). Unlike Pair, it does not inject network conditions.
type NativePair struct {
	PubPC *peerconnection.PeerConnection
	SubPC *peerconnection.PeerConnection

	bundle *cert.Bundle
}

func NewNativePair() (*NativePair, error) {
	bundle, err := cert.Generate(cert.Options{IPs: []net.IP{net.ParseIP("127.0.0.1")}})
	if err != nil {
		return nil, err
	}
	pubPC, err := peerconnection.New(peerconnection.Config{
		Transport: peerconnection.TransportNativeOnly,
		Native: &peerconnection.NativeConfig{
			Role: transport.RolePublisher,
			PublisherConfig: server.Config{
				Addr:       "127.0.0.1:0",
				CertBundle: bundle,
				SDP:        wire.SDP{Codec: "test", Width: 64, Height: 64, FPS: 30},
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return &NativePair{PubPC: pubPC, bundle: bundle}, nil
}

func (n *NativePair) Connect(ctx context.Context) error {
	if err := n.PubPC.Connect(ctx); err != nil {
		return err
	}
	srvAdapter, ok := n.PubPC.Transport().(interface{ Server() *server.Server })
	if !ok {
		return fmt.Errorf("publisher Transport doesn't expose Server")
	}
	s := srvAdapter.Server()
	for strings.HasSuffix(s.Addr(), ":0") {
		time.Sleep(time.Millisecond)
	}
	subPC, err := peerconnection.New(peerconnection.Config{
		Transport: peerconnection.TransportNativeOnly,
		Native: &peerconnection.NativeConfig{
			Role:          transport.RoleSubscriber,
			SubscriberURL: "https://" + s.Addr() + "/wt",
			SubscriberOpts: client.Options{
				Slug:           s.Slug(),
				CertHashB64URL: s.CertHashB64(),
				HelloTimeout:   3 * time.Second,
			},
		},
	})
	if err != nil {
		return err
	}
	if err := subPC.Connect(ctx); err != nil {
		subPC.Close()
		return err
	}
	n.SubPC = subPC
	return nil
}

func (n *NativePair) Close() {
	if n.SubPC != nil {
		n.SubPC.Close()
	}
	if n.PubPC != nil {
		n.PubPC.Close()
	}
}
