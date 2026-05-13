package peerconnection

import (
	"errors"

	"github.com/sachinkesiraju/quicrtc/client"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/transport"
)

// TransportPolicy picks the transport implementation. Since the
// MoQT-to-native-relay migration, there is exactly one transport
// (native QUIC + WebTransport). TransportNativeOnly is the only
// supported value. The enum is kept for forward compatibility with
// hypothetical future transports and to give callers a stable
// configuration surface.
type TransportPolicy int

const (
	// TransportAuto selects native (the only available transport).
	TransportAuto TransportPolicy = iota

	// TransportNativeOnly requires Config.Native.
	TransportNativeOnly
)

// Config drives PeerConnection construction.
type Config struct {
	Transport TransportPolicy

	// Native is the native-transport configuration. Required for
	// both TransportAuto and TransportNativeOnly.
	Native *NativeConfig
}

// NativeConfig wraps the existing server.Config / client.Options
// behind the unified API. The Role field discriminates.
//
// For a publisher: set Role=RolePublisher and PublisherConfig (the
// PeerConnection will spin up an embedded server.Server).
// For a subscriber: set Role=RoleSubscriber, SubscriberURL, and
// SubscriberOpts (the PeerConnection will dial as a client).
//
// To talk through a relay, the subscriber dials the relay's
// downstream share link instead of the origin publisher — the wire
// format is identical, the relay is invisible to the subscriber.
type NativeConfig struct {
	Role transport.Role

	// Publisher fields (Role == RolePublisher)
	PublisherConfig server.Config

	// Subscriber fields (Role == RoleSubscriber)
	SubscriberURL  string
	SubscriberOpts client.Options
}

// connectOpts assembles the transport.ConnectOpts for Transport.Connect.
// Most fields were captured at New() time; the per-call call here
// just forwards what little overrides we expose.
func (c Config) connectOpts() transport.ConnectOpts {
	if c.Native != nil {
		return transport.ConnectOpts{
			Native: &transport.NativeOpts{
				Role:           c.Native.Role,
				ServerAddr:     c.Native.PublisherConfig.Addr,
				SDP:            c.Native.PublisherConfig.SDP,
				ServerURL:      c.Native.SubscriberURL,
				Slug:           c.Native.SubscriberOpts.Slug,
				CertHashB64URL: c.Native.SubscriberOpts.CertHashB64URL,
				HelloTimeout:   c.Native.SubscriberOpts.HelloTimeout,
			},
		}
	}
	return transport.ConnectOpts{}
}

// newTransport instantiates the transport implementation based on
// Config.TransportPolicy.
func newTransport(cfg Config) (transport.Transport, error) {
	switch cfg.Transport {
	case TransportNativeOnly, TransportAuto:
		if cfg.Native == nil {
			return nil, errors.New("quicrtc/peerconnection: NativeConfig required")
		}
		return nativeTransportFactory(cfg.Native)
	default:
		return nil, errors.New("quicrtc/peerconnection: unknown TransportPolicy")
	}
}

// nativeTransportFactory dispatches to server.NewTransport or
// client.NewTransport based on Role.
func nativeTransportFactory(c *NativeConfig) (transport.Transport, error) {
	switch c.Role {
	case transport.RolePublisher:
		return server.NewTransport(c.PublisherConfig)
	case transport.RoleSubscriber:
		return client.NewTransport(c.SubscriberURL, c.SubscriberOpts)
	default:
		return nil, errors.New("quicrtc/peerconnection: NativeConfig.Role required (publisher | subscriber)")
	}
}
