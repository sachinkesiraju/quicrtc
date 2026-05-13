package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sachinkesiraju/quicrtc/datachannel"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/track"
	"github.com/sachinkesiraju/quicrtc/transport"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// transportAdapter makes *client.Client satisfy transport.Transport
// in the subscriber role.
type transportAdapter struct {
	mu      sync.Mutex
	c       *Client
	url     string
	opts    Options
	started bool
	remote  *track.RemoteTrack
}

// NewTransport returns a subscriber-role native Transport. It does
// not dial; call Connect.
func NewTransport(url string, opts Options) (transport.Transport, error) {
	return &transportAdapter{url: url, opts: opts}, nil
}

func (t *transportAdapter) Kind() transport.Kind { return transport.KindNative }

func (t *transportAdapter) Connect(ctx context.Context, copts transport.ConnectOpts) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.started {
		return transport.ErrAlreadyConnected
	}
	// Role is informational client-side; only RoleSubscriber makes
	// semantic sense, but we don't enforce that — Phase 2's
	// PublishBack will use the same client adapter with both roles
	// effectively active concurrently.
	if copts.Native != nil {
		// Apply per-call overrides.
		if copts.Native.ServerURL != "" {
			t.url = copts.Native.ServerURL
		}
		if copts.Native.Slug != "" {
			t.opts.Slug = copts.Native.Slug
		}
		if copts.Native.CertHashB64URL != "" {
			t.opts.CertHashB64URL = copts.Native.CertHashB64URL
		}
		if copts.Native.HelloTimeout > 0 {
			t.opts.HelloTimeout = copts.Native.HelloTimeout
		}
	}

	// Default HelloTimeout for the inner Dial.
	if t.opts.HelloTimeout <= 0 {
		t.opts.HelloTimeout = 5 * time.Second
	}

	dialCtx := ctx
	c, err := Dial(dialCtx, t.url, t.opts)
	if err != nil {
		return fmt.Errorf("native client dial: %w", err)
	}
	t.c = c
	sdp := c.SDP()
	// Build a RemoteTrack that mirrors the SDP advertisement. In
	// Phase 1 single-track land, the implicit name is "primary".
	t.remote = &track.RemoteTrack{
		Name:     "primary",
		Kind:     track.KindVideo,
		Codec:    sdp.Codec,
		Priority: track.PriorityHigh,
	}
	t.started = true
	return nil
}

func (t *transportAdapter) Publish(ctx context.Context, lt track.LocalTrack) (transport.Sender, error) {
	t.mu.Lock()
	c := t.c
	started := t.started
	t.mu.Unlock()
	if !started || c == nil {
		return nil, transport.ErrNotConnected
	}
	s, err := c.Publish(ctx, lt)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", transport.ErrUnsupported, err)
	}
	return &clientSenderAdapter{inner: s}, nil
}

// clientSenderAdapter bridges client.Sender to transport.Sender. The
// underlying types are isomorphic; the adapter exists so callers
// don't have to import the client package directly.
type clientSenderAdapter struct {
	inner Sender
}

func (a *clientSenderAdapter) Send(ctx context.Context, au pubsub.AccessUnit) error {
	return a.inner.Send(ctx, au)
}

func (a *clientSenderAdapter) Close() error { return a.inner.Close() }

// Unpublish on the subscriber side closes the named PublishBack
// track. Since the client tracks senders by name internally, this
// is just a passthrough to the per-track Sender.Close which sends
// TypeUnannounce.
func (t *transportAdapter) Unpublish(name string) error {
	t.mu.Lock()
	c := t.c
	t.mu.Unlock()
	if c == nil {
		return transport.ErrNotConnected
	}
	c.pubMu.Lock()
	pt, ok := c.pubTracks[name]
	if ok {
		delete(c.pubTracks, name)
	}
	c.pubMu.Unlock()
	if !ok {
		return nil
	}
	body, err := wire.Unannounce{Name: name}.Marshal()
	if err == nil {
		_ = c.writeCtl(wire.TypeUnannounce, body)
	}
	pt.bc.Close()
	return nil
}

func (t *transportAdapter) Recv(ctx context.Context) (pubsub.AccessUnit, error) {
	t.mu.Lock()
	c := t.c
	started := t.started
	t.mu.Unlock()
	if !started || c == nil {
		return pubsub.AccessUnit{}, transport.ErrNotConnected
	}
	return c.Recv(ctx)
}

func (t *transportAdapter) RecvOn(ctx context.Context, trackName string) (pubsub.AccessUnit, error) {
	t.mu.Lock()
	c := t.c
	started := t.started
	t.mu.Unlock()
	if !started || c == nil {
		return pubsub.AccessUnit{}, transport.ErrNotConnected
	}
	return c.RecvOn(ctx, trackName)
}

func (t *transportAdapter) RemoteTrack() *track.RemoteTrack {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.remote == nil {
		return nil
	}
	rt := *t.remote
	return &rt
}

func (t *transportAdapter) DataChannel() (*datachannel.Channel, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.started || t.c == nil {
		return nil, transport.ErrNotConnected
	}
	return t.c.DataChannel(), nil
}

func (t *transportAdapter) Stats() *transport.Stats {
	return &transport.Stats{
		Native: &transport.NativeStats{},
	}
}

// Client returns the underlying *Client for callers that need
// transport-specific operations (SessionID, low-level handles).
// Escape hatch; prefer the unified Transport API.
func (t *transportAdapter) Client() *Client {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.c
}

func (t *transportAdapter) Close() error {
	t.mu.Lock()
	c := t.c
	t.started = false
	t.c = nil
	t.mu.Unlock()
	if c == nil {
		return nil
	}
	return c.Close()
}
