package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sachinkesiraju/quicrtc/datachannel"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/session"
	"github.com/sachinkesiraju/quicrtc/track"
	"github.com/sachinkesiraju/quicrtc/transport"
)

// transportAdapter makes *server.Server satisfy transport.Transport
// in the publisher role.
type transportAdapter struct {
	srv     *Server
	cfg     Config // captured for ConnectOpts override
	mu      sync.Mutex
	started bool
	servCtx context.Context
	cancel  context.CancelFunc
	servErr chan error
	senders map[string]*senderAdapter // track name -> sender

	// Latched DataChannel from the most recent session, for the
	// publisher-side use case of "I want to talk to my subscriber."
	// dcCh is a chan(1) used as a one-shot notifier for the FIRST
	// subscriber; dcLatest holds the most recent DC so repeat reads
	// of Transport.DataChannel() see the current value rather than
	// blocking on an empty chan.
	dcMu     sync.Mutex
	dcCh     chan *datachannel.Channel
	dcLatest *datachannel.Channel
}

// NewTransport returns a publisher-role native Transport backed by
// a server.Server. The server is constructed but not started; call
// Connect to bind the listener.
func NewTransport(cfg Config) (transport.Transport, error) {
	s, err := New(cfg)
	if err != nil {
		return nil, err
	}
	t := &transportAdapter{
		srv:     s,
		cfg:     cfg,
		senders: make(map[string]*senderAdapter),
		dcCh:    make(chan *datachannel.Channel, 1),
	}
	// Wrap any user-supplied OnDataChannel so we also surface it
	// through Transport.DataChannel(). dcLatest holds the most
	// recent DC so subsequent reads see the current value; dcCh is
	// drained once on the first read for the "block until first DC"
	// case.
	userCB := cfg.OnDataChannel
	s.cfg.OnDataChannel = func(dc *datachannel.Channel) {
		t.dcMu.Lock()
		t.dcLatest = dc
		t.dcMu.Unlock()
		select {
		case t.dcCh <- dc:
		default:
		}
		if userCB != nil {
			userCB(dc)
		}
	}
	return t, nil
}

func (t *transportAdapter) Kind() transport.Kind { return transport.KindNative }

func (t *transportAdapter) Connect(ctx context.Context, opts transport.ConnectOpts) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.started {
		return transport.ErrAlreadyConnected
	}
	if opts.Native == nil || opts.Native.Role != transport.RolePublisher {
		return errors.New("server transport requires Native publisher opts")
	}
	// Apply per-call overrides from ConnectOpts. Most fields were
	// already captured at New(); the slug is the most likely
	// per-call thing to override.
	if opts.Native.Slug != "" {
		t.srv.slug = opts.Native.Slug
	}
	t.servCtx, t.cancel = context.WithCancel(context.Background())
	t.servErr = make(chan error, 1)
	go func() { t.servErr <- t.srv.ListenAndServe(t.servCtx) }()

	// Spin until bound (Addr() resolves to a non-:0 port). Honors
	// ctx for early cancellation.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if !strings.HasSuffix(t.srv.Addr(), ":0") {
			break
		}
		if ctx.Err() != nil {
			t.cancel()
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			t.cancel()
			return fmt.Errorf("server transport: bind timeout, addr=%q", t.srv.Addr())
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.started = true
	return nil
}

func (t *transportAdapter) Publish(ctx context.Context, lt track.LocalTrack) (transport.Sender, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.started {
		return nil, transport.ErrNotConnected
	}
	name := lt.Name
	if name == "" {
		name = "primary"
	}
	if existing, ok := t.senders[name]; ok {
		return existing, nil
	}
	pub := t.srv.AddTrackWithPriority(name, uint8(lt.Priority))
	sa := &senderAdapter{pub: pub}
	t.senders[name] = sa
	return sa, nil
}

func (t *transportAdapter) Unpublish(name string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.started {
		return transport.ErrNotConnected
	}
	if name == "" {
		name = "primary"
	}
	if _, ok := t.senders[name]; !ok {
		return nil
	}
	delete(t.senders, name)
	t.srv.RemoveTrack(name)
	return nil
}

func (t *transportAdapter) Recv(ctx context.Context) (pubsub.AccessUnit, error) {
	// On the publisher side, Recv reads from the implicit "primary"
	// inbound (PublishBack) track. Mirrors the subscriber-side
	// convention so symmetric callers can ignore role.
	return t.RecvOn(ctx, "primary")
}

func (t *transportAdapter) RecvOn(ctx context.Context, name string) (pubsub.AccessUnit, error) {
	if !t.started {
		return pubsub.AccessUnit{}, transport.ErrNotConnected
	}
	// Wait until at least one session is live, then read its inbound
	// track. For multi-subscriber publishers, callers should use a
	// session-aware API (Phase 3 capability negotiation will surface
	// per-session subscribers).
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	var sess *session.Session
	for sess == nil {
		sess = t.srv.AnySession()
		if sess != nil {
			break
		}
		select {
		case <-ctx.Done():
			return pubsub.AccessUnit{}, ctx.Err()
		case <-deadline.C:
			return pubsub.AccessUnit{}, errors.New("server transport: no subscriber session")
		case <-time.After(5 * time.Millisecond):
		}
	}
	return sess.InboundRecv(ctx, name)
}

func (t *transportAdapter) RemoteTrack() *track.RemoteTrack { return nil }

func (t *transportAdapter) DataChannel() (*datachannel.Channel, error) {
	if !t.started {
		return nil, transport.ErrNotConnected
	}
	// Server-side DCs are session-scoped (one per connected
	// subscriber). Return the most recent one. Fast path: if a DC
	// is latched, return it. Otherwise block briefly waiting for
	// the first one — repeat reads after that always hit the fast
	// path.
	t.dcMu.Lock()
	dc := t.dcLatest
	t.dcMu.Unlock()
	if dc != nil {
		return dc, nil
	}
	select {
	case dc := <-t.dcCh:
		// Mirror into dcLatest so subsequent reads hit the fast path
		// — the OnDataChannel wrapper also latches, but a caller can
		// race ahead of the wrapper on the first DC.
		t.dcMu.Lock()
		if t.dcLatest == nil {
			t.dcLatest = dc
		}
		t.dcMu.Unlock()
		return dc, nil
	case <-time.After(2 * time.Second):
		return nil, errors.New("server transport: no subscriber DC available yet")
	}
}

func (t *transportAdapter) Stats() *transport.Stats {
	subs := 0
	if t.srv != nil {
		subs = t.srv.SubscriberCount()
	}
	return &transport.Stats{
		Subscribers: subs,
		Native:      &transport.NativeStats{},
	}
}

func (t *transportAdapter) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.cancel != nil {
		t.cancel()
	}
	t.started = false
	return nil
}

// Server returns the underlying *Server for callers that need
// transport-specific operations (ShareLink, Slug, CertHashB64,
// Addr). Escape hatch; use sparingly.
func (t *transportAdapter) Server() *Server { return t.srv }

// senderAdapter wraps *Publisher to satisfy transport.Sender.
type senderAdapter struct {
	pub *Publisher
}

func (s *senderAdapter) Send(ctx context.Context, au pubsub.AccessUnit) error {
	return s.pub.Publish(ctx, au)
}

func (s *senderAdapter) Close() error { return nil }
