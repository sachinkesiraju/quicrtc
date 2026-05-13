package loadgen

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/sachinkesiraju/quicrtc/cert"
	"github.com/sachinkesiraju/quicrtc/client"
	"github.com/sachinkesiraju/quicrtc/peerconnection"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/transport"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// Pair is a publisher+subscriber QUIC PeerConnection pair on loopback.
// Tests configure Netcond before calling Connect to route the
// subscriber's UDP traffic through a netem-style proxy.
type Pair struct {
	PubPC *peerconnection.PeerConnection
	SubPC *peerconnection.PeerConnection

	// Netcond is the network condition the subscriber's UDP traffic
	// should traverse. Zero value = direct loopback.
	Netcond NetCond

	// proxyStop, if non-nil, tears down the UDP proxy on close.
	proxyStop func()
}

func NewPair() (*Pair, error) {
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
	return &Pair{PubPC: pubPC}, nil
}

func (p *Pair) Connect(ctx context.Context) error {
	if err := p.PubPC.Connect(ctx); err != nil {
		return err
	}
	srvAdapter, ok := p.PubPC.Transport().(interface{ Server() *server.Server })
	if !ok {
		return fmt.Errorf("publisher Transport doesn't expose Server")
	}
	s := srvAdapter.Server()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && strings.HasSuffix(s.Addr(), ":0") {
		time.Sleep(time.Millisecond)
	}
	subURL := "https://" + s.Addr() + "/wt"
	if p.Netcond.OneWayDelay > 0 || p.Netcond.LossRate > 0 || p.Netcond.JitterPlusMinus > 0 {
		backendAddr, err := net.ResolveUDPAddr("udp", s.Addr())
		if err != nil {
			return fmt.Errorf("resolve backend: %w", err)
		}
		proxyAddr, stop, err := StartUDPProxy(backendAddr, p.Netcond)
		if err != nil {
			return fmt.Errorf("start udp proxy: %w", err)
		}
		p.proxyStop = stop
		subURL = fmt.Sprintf("https://127.0.0.1:%d/wt", proxyAddr.Port)
	}
	subPC, err := peerconnection.New(peerconnection.Config{
		Transport: peerconnection.TransportNativeOnly,
		Native: &peerconnection.NativeConfig{
			Role:          transport.RoleSubscriber,
			SubscriberURL: subURL,
			SubscriberOpts: client.Options{
				Slug:           s.Slug(),
				CertHashB64URL: s.CertHashB64(),
				HelloTimeout:   10 * time.Second,
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
	p.SubPC = subPC
	return nil
}

// ReconnectSubscriber tears down the current subscriber PeerConnection
// and dials a fresh one with the given SessionID, traversing a fresh
// UDP proxy with the same Netcond. Used by the network-grid resume
// tests where we need to "drop the connection" but keep the publisher
// state in place.
func (p *Pair) ReconnectSubscriber(ctx context.Context, sessionID string, lastSeen map[string]uint32) error {
	if p.SubPC != nil {
		p.SubPC.Close()
		p.SubPC = nil
	}
	srvAdapter, ok := p.PubPC.Transport().(interface{ Server() *server.Server })
	if !ok {
		return fmt.Errorf("publisher Transport doesn't expose Server")
	}
	s := srvAdapter.Server()
	subURL := "https://" + s.Addr() + "/wt"
	if p.proxyStop != nil {
		p.proxyStop()
		backendAddr, err := net.ResolveUDPAddr("udp", s.Addr())
		if err != nil {
			return err
		}
		proxyAddr, stop, err := StartUDPProxy(backendAddr, p.Netcond)
		if err != nil {
			return err
		}
		p.proxyStop = stop
		subURL = fmt.Sprintf("https://127.0.0.1:%d/wt", proxyAddr.Port)
	}
	subPC, err := peerconnection.New(peerconnection.Config{
		Transport: peerconnection.TransportNativeOnly,
		Native: &peerconnection.NativeConfig{
			Role:          transport.RoleSubscriber,
			SubscriberURL: subURL,
			SubscriberOpts: client.Options{
				Slug:           s.Slug(),
				CertHashB64URL: s.CertHashB64(),
				HelloTimeout:   10 * time.Second,
				SessionID:      sessionID,
				LastSeenSeq:    lastSeen,
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
	p.SubPC = subPC
	return nil
}

func (p *Pair) Close() {
	if p.SubPC != nil {
		p.SubPC.Close()
	}
	if p.PubPC != nil {
		p.PubPC.Close()
	}
	if p.proxyStop != nil {
		p.proxyStop()
	}
}

// PickClient returns the underlying *client.Client behind a
// PeerConnection's transport. The native transport adapter implements
// a Client() accessor so tests can read SessionID for resume scenarios.
func PickClient(pc *peerconnection.PeerConnection) (*client.Client, bool) {
	type clientHaver interface {
		Client() *client.Client
	}
	if h, ok := pc.Transport().(clientHaver); ok {
		return h.Client(), true
	}
	return nil, false
}
