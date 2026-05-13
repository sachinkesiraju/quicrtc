// Package peerconnection is the unified API on top of transport.
// One PeerConnection holds one Transport; the Transport is
// instantiated at New() time based on Config.TransportPolicy.
//
// Supports the native transport (QUIC + WebTransport) in publisher
// and subscriber roles. For relay topologies, the subscriber simply
// dials the relay's downstream share link instead of an origin
// publisher — the wire format is identical and the relay is
// invisible to the subscriber. See the relay package for the
// upstream-aggregating relay implementation.
//
// Usage shape (publisher side):
//
//	pc, _ := peerconnection.New(peerconnection.Config{
//	    Transport: peerconnection.TransportNativeOnly,
//	    Native: &peerconnection.NativeConfig{
//	        Role:            transport.RolePublisher,
//	        PublisherConfig: server.Config{Addr: ":4433", SDP: ...},
//	    },
//	})
//	pc.Connect(ctx)
//	sender, _ := pc.AddTrack(ctx, track.Video("primary", "avc1.42E01F"))
//	for au := range encoder { sender.Send(ctx, au) }
//
// Subscriber side:
//
//	pc, _ := peerconnection.New(peerconnection.Config{
//	    Transport: peerconnection.TransportNativeOnly,
//	    Native: &peerconnection.NativeConfig{
//	        Role:           transport.RoleSubscriber,
//	        SubscriberURL:  shareURL,
//	        SubscriberOpts: client.Options{Slug: ..., CertHashB64URL: ...},
//	    },
//	})
//	pc.Connect(ctx)
//	for {
//	    au, err := pc.Recv(ctx)
//	    if err != nil { break }
//	    decoder.Feed(au)
//	}
package peerconnection

import (
	"context"
	"sync"

	"github.com/sachinkesiraju/quicrtc/datachannel"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/track"
	"github.com/sachinkesiraju/quicrtc/transport"
)

// PeerConnection is the unified entry point. Same shape regardless
// of underlying transport. Construct with New, drive with Connect /
// AddTrack / Recv / DataChannel / Close.
type PeerConnection struct {
	cfg Config
	tx  transport.Transport

	closeOnce sync.Once
	closeErr  error
}

// New constructs a PeerConnection with the given Config. The
// transport is instantiated but not connected yet — call Connect.
func New(cfg Config) (*PeerConnection, error) {
	tx, err := newTransport(cfg)
	if err != nil {
		return nil, err
	}
	return &PeerConnection{cfg: cfg, tx: tx}, nil
}

// Connect performs the transport's handshake. After this returns
// nil, the PeerConnection is ready for AddTrack / Recv / DataChannel.
func (pc *PeerConnection) Connect(ctx context.Context) error {
	return pc.tx.Connect(ctx, pc.cfg.connectOpts())
}

// AddTrack creates a publisher Sender for the given LocalTrack.
// Multiple AddTrack calls are supported; each track gets its own
// QUIC stream lineage with stream-header demux.
func (pc *PeerConnection) AddTrack(ctx context.Context, t track.LocalTrack) (transport.Sender, error) {
	return pc.tx.Publish(ctx, t)
}

// RemoveTrack tears down a previously-Added track. Subscribers
// receive a TypeUnannounce control frame; their per-track recv
// channels close cleanly. No-op if the track doesn't exist.
func (pc *PeerConnection) RemoveTrack(name string) error {
	return pc.tx.Unpublish(name)
}

// Recv blocks until the next AccessUnit arrives on the implicit
// "primary" track. For multi-track receivers, prefer RecvOn.
// Returns ErrWrongRole on publisher-side transports.
func (pc *PeerConnection) Recv(ctx context.Context) (pubsub.AccessUnit, error) {
	return pc.tx.Recv(ctx)
}

// RecvOn blocks until the next AccessUnit on the named track arrives.
// Used by multi-track subscribers that have published multiple typed
// tracks (video, tokens, etc.) on the same PeerConnection.
func (pc *PeerConnection) RecvOn(ctx context.Context, trackName string) (pubsub.AccessUnit, error) {
	return pc.tx.RecvOn(ctx, trackName)
}

// RemoteTrack returns metadata about the remote track this PC is
// subscribed to. Returns nil for publisher-side or pre-Connect.
func (pc *PeerConnection) RemoteTrack() *track.RemoteTrack {
	return pc.tx.RemoteTrack()
}

// DataChannel returns the session's reliable bidi message channel,
// backed by the control-stream DC.
func (pc *PeerConnection) DataChannel() (*datachannel.Channel, error) {
	return pc.tx.DataChannel()
}

// ActiveTransport returns which transport implementation is in use.
// Useful for logging and for transport-specific advanced operations
// via type assertion on the Transport.
func (pc *PeerConnection) ActiveTransport() transport.Kind {
	return pc.tx.Kind()
}

// Stats returns transport-level metrics.
func (pc *PeerConnection) Stats() *transport.Stats {
	return pc.tx.Stats()
}

// Transport exposes the underlying Transport for callers that need
// transport-specific operations (e.g., the publisher-side server
// adapter's ShareLink). Escape hatch; prefer the unified API.
func (pc *PeerConnection) Transport() transport.Transport {
	return pc.tx
}

// Close ends the session. Idempotent.
func (pc *PeerConnection) Close() error {
	pc.closeOnce.Do(func() {
		pc.closeErr = pc.tx.Close()
	})
	return pc.closeErr
}
