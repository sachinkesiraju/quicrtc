// Package transport defines the abstraction that quicrtc dispatches
// PeerConnection method calls through. One implementation: native
// (server-side and client-side adapters over webtransport-go).
//
// For relay topologies, the relay is just an upstream-aggregating
// server.Server that re-emits the same native wire format to its
// downstream subscribers — no separate transport implementation is
// needed. See the relay package.
package transport

import (
	"context"
	"time"

	"github.com/sachinkesiraju/quicrtc/datachannel"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/track"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// Transport is the dispatch target underneath PeerConnection.
//
// Native transports come in two roles (publisher / subscriber); a
// single Transport instance plays one role.
type Transport interface {
	// Connect performs the transport-specific handshake and brings
	// the session to a state where Publish/Subscribe will work.
	Connect(ctx context.Context, opts ConnectOpts) error

	// Publish creates an outbound track. Multiple Publish calls per
	// Transport are supported; each track gets its own sequence of
	// uni QUIC streams with stream-header demux.
	Publish(ctx context.Context, t track.LocalTrack) (Sender, error)

	// Unpublish removes a previously-Published track. The Sender
	// returned by Publish becomes unusable after this call. No-op if
	// the track doesn't exist or was never Published.
	Unpublish(name string) error

	// Recv blocks until the next AccessUnit on the implicit "primary"
	// track is available. Equivalent to RecvOn(ctx, "primary") for
	// transports that support multi-track demux.
	Recv(ctx context.Context) (pubsub.AccessUnit, error)

	// RecvOn blocks until the next AccessUnit on the named track is
	// available. Multi-track receivers route inbound feed streams to
	// per-track queues based on the TypeStreamHeader frame at the
	// head of each stream. Streams without a header route to
	// "primary" for backward compatibility.
	RecvOn(ctx context.Context, trackName string) (pubsub.AccessUnit, error)

	// RemoteTrack reports metadata about the remote track this
	// Transport is subscribed to (subscriber-side only). Returns nil
	// for publisher-side or before Connect.
	RemoteTrack() *track.RemoteTrack

	// DataChannel returns the session's reliable bidi message
	// channel, backed by the existing control-stream DC.
	DataChannel() (*datachannel.Channel, error)

	// Kind reports which transport implementation this is.
	Kind() Kind

	// Stats returns transport-level counters and gauges.
	Stats() *Stats

	// Close releases resources. Idempotent.
	Close() error
}

// Sender is the publisher side of a track.
type Sender interface {
	Send(ctx context.Context, au pubsub.AccessUnit) error
	Close() error
}

// ConnectOpts is a tagged union. With a single transport
// implementation today, Native must be non-nil.
type ConnectOpts struct {
	Native *NativeOpts // populated for native transport
}

// NativeOpts is the per-call setup for a native transport.
type NativeOpts struct {
	Role Role

	// Server-side fields (Role == RolePublisher):

	// ServerAddr is the UDP listen address, e.g., ":4433".
	ServerAddr string
	// SDP advertised to subscribers on HELLO.
	SDP wire.SDP
	// CertExtraIPs are extra SAN IPs for the auto-generated cert.
	CertExtraIPs []string
	// AllowedOrigins for the WebTransport upgrade.
	AllowedOrigins []string
	// MaxSessions caps concurrent subscribers.
	MaxSessions int

	// Client-side fields (Role == RoleSubscriber):

	// ServerURL is the share-link URL.
	ServerURL string
	// CertHashB64URL is SHA-256(DER) of the server cert in base64url.
	CertHashB64URL string
	// HelloTimeout caps the HELLO + SDP exchange.
	HelloTimeout time.Duration

	// Shared:

	// Slug is the auth secret echoed in HELLO.
	Slug string
}

// Role discriminates publisher vs subscriber for native transports.
type Role string

const (
	RolePublisher  Role = "publisher"
	RoleSubscriber Role = "subscriber"
)

// Stats is transport-level metrics. Per-track stats hang off
// LocalTrack/RemoteTrack in later phases.
type Stats struct {
	BytesIn      uint64
	BytesOut     uint64
	Subscribers  int
	HandshakeRTT time.Duration
	Native       *NativeStats // populated when Kind() == KindNative
}

// NativeStats holds native-transport-specific counters.
type NativeStats struct {
	FeedStreamsOpened uint64
	FeedStreamsReset  uint64
	AUDroppedKey      uint64
	AUDroppedP        uint64
}
