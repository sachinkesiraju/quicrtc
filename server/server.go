// Package server is the high-level quicrtc HTTP/3 + WebTransport
// listener. It binds the wire/feed/session/cert pieces together and
// exposes a simple Publisher handle for the consumer's encoder loop.
//
// One server = one publisher, many subscribers. For multi-upstream
// fan-out (one server aggregating from N publishers, or relay
// topologies), see the relay package.
package server

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"

	"github.com/sachinkesiraju/quicrtc/cert"
	"github.com/sachinkesiraju/quicrtc/datachannel"
	"github.com/sachinkesiraju/quicrtc/feed"
	"github.com/sachinkesiraju/quicrtc/metrics"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/session"
	"github.com/sachinkesiraju/quicrtc/track"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// Config holds tunables for a quicrtc server.
type Config struct {
	// Addr is the UDP address to listen on for QUIC + HTTP/3, e.g.
	// ":4433" or "0.0.0.0:4433".
	Addr string

	// CertBundle, if non-nil, is the TLS bundle to serve. Otherwise
	// an ephemeral ECDSA P-256 cert is generated covering loopback +
	// CertExtraIPs.
	CertBundle *cert.Bundle

	// CertExtraIPs are extra SAN IPs for the auto-generated cert
	// (e.g. LAN, public IPv6, Tailscale). Ignored if CertBundle is
	// supplied.
	CertExtraIPs []net.IP

	// Slug is the per-session shared secret subscribers must echo in
	// HELLO. Empty -> a fresh 128-bit base64url slug is generated.
	Slug string

	// ExternalHost is the host (no scheme/port) that subscribers will
	// reach the server on. Used in ShareLink. Defaults to the first
	// CertExtraIPs entry, or "127.0.0.1" if none.
	ExternalHost string

	// AllowedOrigins are the HTTP Origins permitted by the
	// WebTransport upgrade. Empty disables origin checking. Slug
	// auth is the security boundary; this is defense in depth.
	AllowedOrigins []string

	// MaxSessions caps concurrent subscribers. <=0 picks 16.
	MaxSessions int

	// SDP is the codec/dimensions advertisement sent on HELLO. Set
	// before publishing.
	SDP wire.SDP

	// Session is forwarded to session.New for handshake/idle/data
	// tunables; zero values pick session defaults.
	Session session.Config

	// PerSubscriberBuffer is the broadcaster's per-subscriber channel
	// depth. <=0 picks 30 (~1s @ 30fps).
	PerSubscriberBuffer int

	// OnDataChannel, if non-nil, is invoked once per session with the
	// session's DataChannel. The server passes this through to
	// session.Config.
	OnDataChannel func(*datachannel.Channel)

	// Metrics, if non-nil, receives observability events. nil =
	// metrics.NoopMetrics{} (zero cost). Plug a Prometheus-backed
	// implementation here for production.
	Metrics metrics.Metrics

	// Logger, if non-nil, is the structured logger used for server
	// events. nil = slog.Default(). Set to a discard handler to
	// silence completely.
	Logger *slog.Logger

	// SessionStore, if non-nil, is the backend that holds parked
	// session state across socket disconnects. Default is
	// NewMemorySessionStore — sufficient for single-instance
	// deployments. Multi-instance setups should pair sticky LB
	// routing (keyed on the SessionID issued in HELLO) with the
	// memory store, OR plug a shared backend.
	SessionStore SessionStore

	// AuthValidator, if non-nil, replaces shared-slug auth with a
	// caller-supplied validator. Receives the raw HELLO slug field
	// (callers can stuff a JWT or bearer token there). Returns the
	// tenant scope (used to namespace parked sessions for resume so
	// tenants cannot cross-resume each other's sessions) and nil if
	// the credential is valid; any error rejects.
	//
	// For shared-slug (single-tenant) auth, leave this nil and rely
	// on the auto-generated Slug. For multi-tenant SaaS, plug a JWT
	// validator that resolves to a tenant — return the tenant ID as
	// the first return value.
	AuthValidator func(credential string) (tenant string, err error)

	// OnBackpressure, if non-nil, is invoked when a subscriber
	// signals receive-side congestion. trackName identifies the
	// track (empty = session-level). level is 0..100 (drained..full).
	// Implementations should be cheap (called from the session's
	// control reader); offload heavy work to a goroutine. Use the
	// signal to lower bitrate, drop a modality, or pause non-critical
	// publishing.
	OnBackpressure func(sessionID, trackName string, level uint8)

	// OnKeyframeRequest, if non-nil, is invoked when a subscriber
	// asks for a fresh keyframe on a specific track via Client.
	// RequestKeyframe. The application should signal its encoder to
	// emit a keyframe NOW — the typical use is computer-use / live-
	// screen workloads recovering from a P-frame loss event without
	// waiting for the next natural keyframe.
	OnKeyframeRequest func(sessionID, trackName string)

	// OnSession, if non-nil, is invoked once per accepted session
	// after the HELLO handshake completes — i.e., the peer has
	// authenticated and SessionID is populated. The SessionHandle
	// exposes per-session capabilities (datagram send/receive, inbound
	// PublishBack track read) so applications can implement kind-aware
	// behaviors without changes to the core pump dispatch.
	//
	// The callback runs on the session goroutine; long-running work
	// should spawn its own goroutine. Note: prior to v0.1.1 this
	// callback fired pre-handshake against an unauthenticated peer;
	// the contract was tightened to fire post-authentication only.
	OnSession func(h SessionHandle)

	// InboundRateLimit caps inbound (PublishBack) traffic per session.
	// Production deployments should set this to prevent a single
	// misbehaving subscriber from saturating the session. Zero =
	// unlimited (default; safe for closed deployments).
	InboundRateLimit session.RateLimit

	// SupportedFeatures is the optional protocol features this server
	// offers. Intersected with the client's HELLO.Features and echoed
	// in SDP.Features. nil = use the defaults (all features the
	// current build implements).
	SupportedFeatures []string

	// CertGetter, if non-nil, replaces the static cert from CertBundle
	// with a per-handshake lookup. Plug a *cert.Reloader here to pick
	// up filesystem-rotated certs without restarting the server.
	// When nil, CertBundle's cert is used unchanged.
	//
	//	reloader, _ := cert.NewReloader(certPath, keyPath, cert.ReloaderOptions{...})
	//	cfg.CertGetter = reloader.GetCertificate
	CertGetter func(*tls.ClientHelloInfo) (*tls.Certificate, error)
}

// DefaultSupportedFeatures lists every optional protocol feature the
// current build implements. Use as a starting point for
// Config.SupportedFeatures; trim to disable specific features.
func DefaultSupportedFeatures() []string {
	return []string{
		"resume",        // TypeResume + replay buffer
		"backpressure",  // TypeBackpressure subscriber→publisher
		"publishback",   // subscriber-side AddTrack
		"stream-header", // TypeStreamHeader on uni feed streams
	}
}

// Server is one running quicrtc instance.
type Server struct {
	cfg     Config
	metrics metrics.Metrics
	logger  *slog.Logger

	bundle *cert.Bundle
	slug   string
	host   string

	// trackMu guards tracks and sessions.
	trackMu  sync.Mutex
	tracks   map[string]*trackEntry   // name -> broadcaster + publisher
	sessions map[*sessionRef]struct{} // live sessions for new-track notifications

	// store holds parked sessions for cross-disconnect resumption.
	store SessionStore

	mu      sync.Mutex
	wt      *webtransport.Server
	h3      *http3.Server
	addr    string // resolved local addr (with port)
	current atomic.Int32

	// draining is set by Shutdown(). When true, the upgrade handler
	// returns 503 to new clients so they can pick a different
	// instance (or retry after the drain completes).
	draining atomic.Bool
}

// trackEntry is one published track on this server.
type trackEntry struct {
	name     string
	kind     track.Kind // selects the per-track DeliveryClass; "" => KindVideo (legacy default)
	priority uint8      // RFC 9218 numeric scale, observability-only for now
	trackID  uint8      // datagram envelope demux key; 0 if not a datagram track
	bc       *pubsub.Broadcaster
	pub      *Publisher
}

// sessionRef is the server's handle to one running session, used to
// notify it about newly-added tracks and to read its inbound AUs.
type sessionRef struct {
	addTrack             func(name string, recv *pubsub.Receiver)
	addTrackWithPriority func(name string, recv *pubsub.Receiver, priority uint8)
	addTrackWithTrackID  func(name string, recv *pubsub.Receiver, priority uint8, trackID uint8)
	addTrackWithKind     func(name string, kind track.Kind, recv *pubsub.Receiver, priority uint8, trackID uint8)
	sess                 *session.Session
}

// Publisher is what the consumer uses to feed access units.
type Publisher struct {
	bc   *pubsub.Broadcaster
	name string
}

// SessionHandle exposes per-session capabilities to the application
// via Config.OnSession. Applications use it to implement kind-aware
// behaviors that the core pump dispatch doesn't yet integrate —
// principally, fire-and-forget telemetry over QUIC datagrams and
// receiving subscriber-published tracks per-session.
//
// SendDatagram and ReceiveDatagram pass directly through to the
// underlying webtransport.Session; the same MTU and reliability
// caveats apply (DatagramTooLargeError on oversize, no retransmit).
type SessionHandle struct {
	wt        *webtransport.Session
	sessionID string
	sess      *session.Session // for per-session inbound track receive
}

// SendDatagram sends one unreliable QUIC datagram to the subscriber
// attached to this session. Returns DatagramTooLargeError if the
// payload exceeds the path MTU.
func (h SessionHandle) SendDatagram(payload []byte) error {
	return h.wt.SendDatagram(payload)
}

// ReceiveDatagram blocks until one datagram from the subscriber
// arrives or ctx is done.
func (h SessionHandle) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	return h.wt.ReceiveDatagram(ctx)
}

// InboundRecv blocks until the next AccessUnit on a subscriber-
// published track arrives on THIS session. Unlike Server.AnySession,
// AUs from other sessions are not mixed in. Blocks until the
// subscriber announces the track if it hasn't yet.
func (h SessionHandle) InboundRecv(ctx context.Context, name string) (pubsub.AccessUnit, error) {
	if h.sess == nil {
		return pubsub.AccessUnit{}, errors.New("quicrtc/server: SessionHandle.InboundRecv called on zero handle")
	}
	return h.sess.InboundRecv(ctx, name)
}

// Context returns the underlying WebTransport session's context. It
// is cancelled when the session closes — callers can `select` on
// it to know when SendDatagram / ReceiveDatagram will no longer
// succeed, without having to call them and inspect the error.
func (h SessionHandle) Context() context.Context {
	return h.wt.Context()
}

// SessionID returns this session's unique identifier. Empty when
// called from OnSession — the ID is allocated during HELLO handshake.
func (h SessionHandle) SessionID() string {
	return h.sessionID
}

// Publish enqueues one AU for fanout. Never blocks longer than the
// broadcaster's per-subscriber drop policy permits. Caller must not
// mutate au.Bytes after Publish.
func (p *Publisher) Publish(_ context.Context, au pubsub.AccessUnit) error {
	p.bc.Publish(au)
	return nil
}

// Name returns the track name this Publisher feeds.
func (p *Publisher) Name() string { return p.name }

// New constructs a Server. ListenAndServe must be called separately
// to actually bind and serve.
func New(cfg Config) (*Server, error) {
	if cfg.Addr == "" {
		cfg.Addr = ":4433"
	}
	if cfg.MaxSessions <= 0 {
		cfg.MaxSessions = 16
	}

	bundle := cfg.CertBundle
	if bundle == nil {
		b, err := cert.Generate(cert.Options{IPs: cfg.CertExtraIPs})
		if err != nil {
			return nil, fmt.Errorf("cert: %w", err)
		}
		bundle = b
	}

	slug := cfg.Slug
	if slug == "" {
		var raw [16]byte
		if _, err := rand.Read(raw[:]); err != nil {
			return nil, fmt.Errorf("slug: %w", err)
		}
		slug = base64.RawURLEncoding.EncodeToString(raw[:])
	}

	host := cfg.ExternalHost
	if host == "" {
		if len(cfg.CertExtraIPs) > 0 && cfg.CertExtraIPs[0] != nil {
			host = cfg.CertExtraIPs[0].String()
		} else {
			host = "127.0.0.1"
		}
	}

	m := cfg.Metrics
	if m == nil {
		m = metrics.NoopMetrics{}
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	store := cfg.SessionStore
	if store == nil {
		store = NewMemorySessionStore(cfg.Session.ResumeWindow)
	}
	s := &Server{
		cfg:      cfg,
		metrics:  m,
		logger:   logger,
		bundle:   bundle,
		slug:     slug,
		host:     host,
		tracks:   make(map[string]*trackEntry),
		sessions: make(map[*sessionRef]struct{}),
		store:    store,
	}
	// "primary" exists by default to preserve single-track API
	// callers that grab Publisher() without ever calling AddTrack.
	s.AddTrack("primary")
	return s, nil
}

// TrackSpec is the modern entry point for AddTrack. All fields except
// Name are optional; zero values pick safe defaults (KindVideo at
// priority 4 with no trackID). The Kind field is what unlocks the
// per-kind delivery dispatch in feed.Pump — without it, the track
// falls through to the legacy stream-per-GOP path regardless of
// payload shape.
type TrackSpec struct {
	// Name uniquely identifies the track within this server. Required.
	Name string

	// Kind selects the DeliveryClass via track.DefaultDeliveryClass.
	// Zero value ("") is treated as KindVideo for back-compat with the
	// legacy AddTrack* methods that don't carry a Kind.
	Kind track.Kind

	// Priority is the RFC 9218 priority hint (lower = more urgent).
	// Zero value picks PriorityNormal (4).
	Priority uint8

	// TrackID is the 1-byte demux key used by datagram envelopes when
	// Kind is KindTelemetry (or any DatagramOrStream track). Pick
	// distinct values per session; collisions silently merge tracks
	// on the receiver side. Unused for stream-based deliveries.
	TrackID uint8
}

// AddTrack registers a new publishable track at default priority
// (KindVideo, priority 4). Equivalent to AddTrackSpec(TrackSpec{
// Name: name }). For non-video tracks, use AddTrackSpec so the per-
// kind DeliveryClass dispatch actually runs.
func (s *Server) AddTrack(name string) *Publisher {
	return s.AddTrackSpec(TrackSpec{Name: name})
}

// AddTrackWithTrackID is a legacy entry point preserved for backward
// compatibility. New code should use AddTrackSpec, which carries Kind
// — without a Kind, the track always uses the stream-per-GOP path.
//
// Idempotent on name; second call returns the existing Publisher.
func (s *Server) AddTrackWithTrackID(name string, priority uint8, trackID uint8) *Publisher {
	return s.AddTrackSpec(TrackSpec{Name: name, Priority: priority, TrackID: trackID})
}

// AddTrackWithPriority is a legacy entry point preserved for backward
// compatibility. New code should use AddTrackSpec.
//
// Idempotent on name; second call returns the existing Publisher and
// ignores the new priority (use RemoveTrack + AddTrack to change it).
func (s *Server) AddTrackWithPriority(name string, priority uint8) *Publisher {
	return s.AddTrackSpec(TrackSpec{Name: name, Priority: priority})
}

// AddTrackSpec registers a publishable track described by spec. The
// Kind field selects the DeliveryClass — KindTokens routes through
// the persistent low-latency uni stream, KindTelemetry through
// datagrams, KindToolCalls through one bidi stream per call, etc.
// Zero-Kind (or KindVideo) preserves the legacy stream-per-GOP path.
//
// Priority handling: spec.Priority's zero value (0) is treated as
// "use the Kind's default" via track.DefaultPriority. This lets callers
// register audio tracks at PriorityCritical (also 0) by leaving Priority
// unset, rather than getting silently demoted to PriorityNormal.
//
// Idempotent on Name; second call returns the existing Publisher and
// ignores subsequent spec values (use RemoveTrack + AddTrackSpec to
// change).
func (s *Server) AddTrackSpec(spec TrackSpec) *Publisher {
	kind := spec.Kind
	if kind == "" {
		kind = track.KindVideo
	}
	priority := spec.Priority
	if priority == 0 {
		priority = uint8(track.DefaultPriority(kind))
	}
	return s.addTrackInternal(spec.Name, kind, priority, spec.TrackID)
}

func (s *Server) addTrackInternal(name string, kind track.Kind, priority uint8, trackID uint8) *Publisher {
	s.trackMu.Lock()
	if t, ok := s.tracks[name]; ok {
		s.trackMu.Unlock()
		return t.pub
	}
	bc := pubsub.NewBroadcaster(s.cfg.PerSubscriberBuffer)
	t := &trackEntry{
		name:     name,
		kind:     kind,
		priority: priority,
		trackID:  trackID,
		bc:       bc,
		pub:      &Publisher{bc: bc, name: name},
	}
	s.tracks[name] = t
	// Snapshot sessions so we can notify outside trackMu.
	refs := make([]*sessionRef, 0, len(s.sessions))
	for r := range s.sessions {
		refs = append(refs, r)
	}
	s.trackMu.Unlock()
	for _, r := range refs {
		// Prefer the most-specific attach function the session
		// supports. addTrackWithKind is the canonical path (carries
		// Kind so the pump dispatches to the right DeliveryClass);
		// the older trackID / priority / bare-name fallbacks keep
		// older sessions working unchanged.
		switch {
		case r.addTrackWithKind != nil:
			r.addTrackWithKind(name, kind, bc.Subscribe(), priority, trackID)
		case r.addTrackWithTrackID != nil:
			r.addTrackWithTrackID(name, bc.Subscribe(), priority, trackID)
		case r.addTrackWithPriority != nil:
			r.addTrackWithPriority(name, bc.Subscribe(), priority)
		default:
			r.addTrack(name, bc.Subscribe())
		}
		// Notify subscriber via Announce so it learns the track exists
		// (without this, subscriber would only discover the track when
		// the first uni stream's stream-header arrives).
		if r.sess != nil {
			r.sess.SendAnnounce(name, string(kind), "")
		}
	}
	s.metrics.TrackAdded(name)
	s.logger.Debug("quicrtc/server: track added", "name", name, "kind", kind, "priority", priority, "trackID", trackID)
	return t.pub
}

// RemoveTrack tears down a previously-added track. The per-track
// broadcaster is closed (no more AUs accepted), all session pumps for
// this track are stopped, and a TypeUnannounce frame is sent to each
// connected subscriber so they can close their per-track recv queues.
// No-op if the track doesn't exist.
func (s *Server) RemoveTrack(name string) {
	s.trackMu.Lock()
	t, ok := s.tracks[name]
	if !ok {
		s.trackMu.Unlock()
		return
	}
	delete(s.tracks, name)
	refs := make([]*sessionRef, 0, len(s.sessions))
	for r := range s.sessions {
		refs = append(refs, r)
	}
	s.trackMu.Unlock()

	// Notify each session: stop its pump for this track, send
	// TypeUnannounce to its subscriber.
	for _, r := range refs {
		if r.sess != nil {
			r.sess.DetachTrack(name)
			r.sess.SendUnannounce(name)
		}
	}

	// Closing the broadcaster's receivers signals the pumps to drain
	// and exit (their range loop ends when the channel closes). Done
	// AFTER DetachTrack so per-track pumps see ctx-cancel first.
	t.bc.Close()
	s.metrics.TrackRemoved(name)
	s.logger.Debug("quicrtc/server: track removed", "name", name)
}

// Publisher returns the default ("primary") Publisher handle. Kept
// for single-track callers that pre-date AddTrack.
func (s *Server) Publisher() *Publisher {
	s.trackMu.Lock()
	defer s.trackMu.Unlock()
	if t, ok := s.tracks["primary"]; ok {
		return t.pub
	}
	return nil
}

// trackSnapshot returns the current track set as (name, fresh
// receiver) pairs. Used by session startup to subscribe to all
// tracks that exist at the moment the session begins.
func (s *Server) trackSnapshot() []trackSnap {
	s.trackMu.Lock()
	defer s.trackMu.Unlock()
	out := make([]trackSnap, 0, len(s.tracks))
	for _, t := range s.tracks {
		out = append(out, trackSnap{
			name:     t.name,
			recv:     t.bc.Subscribe(),
			priority: t.priority,
			trackID:  t.trackID,
		})
	}
	return out
}

type trackSnap struct {
	name     string
	recv     *pubsub.Receiver
	priority uint8
	trackID  uint8
}

// trackEntry returns the live trackEntry for name, or nil. Used by
// the resume path to find the broadcaster behind a parked receiver
// for replay-buffer lookups.
func (s *Server) trackEntry(name string) *trackEntry {
	s.trackMu.Lock()
	defer s.trackMu.Unlock()
	return s.tracks[name]
}

// trackIDFor returns the configured datagram trackID for a track,
// or 0 when the track is unknown / not a datagram track.
func (s *Server) trackIDFor(name string) uint8 {
	s.trackMu.Lock()
	defer s.trackMu.Unlock()
	if t, ok := s.tracks[name]; ok {
		return t.trackID
	}
	return 0
}

// priorityFor returns the configured priority for a track or 4
// (normal default) when the track is unknown.
func (s *Server) priorityFor(name string) uint8 {
	s.trackMu.Lock()
	defer s.trackMu.Unlock()
	if t, ok := s.tracks[name]; ok {
		return t.priority
	}
	return 4
}

// kindFor returns the configured Kind for a track or KindVideo (the
// legacy default) when the track is unknown.
func (s *Server) kindFor(name string) track.Kind {
	s.trackMu.Lock()
	defer s.trackMu.Unlock()
	if t, ok := s.tracks[name]; ok && t.kind != "" {
		return t.kind
	}
	return track.KindVideo
}

// addSession registers a session and returns a cleanup func.
func (s *Server) addSession(ref *sessionRef) func() {
	s.trackMu.Lock()
	s.sessions[ref] = struct{}{}
	s.trackMu.Unlock()
	return func() {
		s.trackMu.Lock()
		delete(s.sessions, ref)
		s.trackMu.Unlock()
	}
}

// AnySession returns the most recently-attached live Session, or nil.
// Used by the publisher-side Transport adapter to read inbound
// PublishBack AUs. In multi-subscriber deployments, each subscriber
// has its own Session and "most recent" is a pragmatic default; full
// per-subscriber routing arrives with capability negotiation in
// Phase 3.
func (s *Server) AnySession() *session.Session {
	s.trackMu.Lock()
	defer s.trackMu.Unlock()
	for r := range s.sessions {
		if r.sess != nil {
			return r.sess
		}
	}
	return nil
}

// Slug returns the auth secret the server expects in HELLO.
func (s *Server) Slug() string { return s.slug }

// SubscriberCount returns the current number of connected
// subscribers. Used by the transport adapter for Stats.
func (s *Server) SubscriberCount() int { return int(s.current.Load()) }

// CertHashB64 returns the SHA-256(DER) cert hash in the form
// browsers expect for serverCertificateHashes.
func (s *Server) CertHashB64() string { return s.bundle.HashB64URL() }

// Addr returns the local listen address (with port resolved if the
// configured port was 0). Available after Listen has begun.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.addr != "" {
		return s.addr
	}
	return s.cfg.Addr
}

// ShareLink returns a URL the browser can use to connect. Includes
// the slug and cert hash in the fragment so the page (served on the
// same origin) can read them with location.hash.
func (s *Server) ShareLink() string {
	addr := s.Addr()
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		port = addr
	}
	host := s.host
	// IPv6 literals need bracket-wrapping in URLs (RFC 3986 §3.2.2).
	// Detect by colon count: 2+ colons rules out host:port forms.
	if strings.Count(host, ":") >= 2 && !strings.HasPrefix(host, "[") {
		host = "[" + host + "]"
	}
	return fmt.Sprintf("https://%s:%s/wt#slug=%s&hash=%s", host, port, s.slug, s.bundle.HashB64URL())
}

// ListenAndServe binds the listener and runs until ctx is cancelled
// or a fatal error occurs. Returns nil on graceful shutdown.
func (s *Server) ListenAndServe(ctx context.Context) error {
	mux := http.NewServeMux()
	tlsCfg := &tls.Config{
		NextProtos: []string{"h3"},
	}
	if s.cfg.CertGetter != nil {
		// Hot-reloadable cert source (e.g., a *cert.Reloader). The
		// per-handshake callback lets cert-manager / certbot rotations
		// take effect without a server restart.
		tlsCfg.GetCertificate = s.cfg.CertGetter
	} else {
		tlsCfg.Certificates = []tls.Certificate{s.bundle.TLS}
	}
	h3 := &http3.Server{
		Addr:            s.cfg.Addr,
		Handler:         mux,
		TLSConfig:       tlsCfg,
		EnableDatagrams: true,
		QUICConfig: &quic.Config{
			InitialStreamReceiveWindow:       4 << 20,
			MaxStreamReceiveWindow:           16 << 20,
			InitialConnectionReceiveWindow:   8 << 20,
			MaxConnectionReceiveWindow:       32 << 20,
			EnableDatagrams:                  true,
			EnableStreamResetPartialDelivery: true,
		},
	}
	wts := &webtransport.Server{
		H3:          h3,
		CheckOrigin: makeOriginCheck(s.cfg.AllowedOrigins),
	}
	webtransport.ConfigureHTTP3Server(h3)

	mux.HandleFunc("/wt", func(w http.ResponseWriter, r *http.Request) {
		if s.draining.Load() {
			// Server is draining; reject new connections so clients
			// pick a different instance behind the LB.
			w.Header().Set("Retry-After", "5")
			http.Error(w, "draining", http.StatusServiceUnavailable)
			return
		}
		if int(s.current.Load()) >= s.cfg.MaxSessions {
			http.Error(w, "too many sessions", http.StatusServiceUnavailable)
			return
		}
		wtSess, err := wts.Upgrade(w, r)
		if err != nil {
			return
		}
		s.current.Add(1)
		// Run the session under a context tied to the SERVER lifetime,
		// not r.Context(). The HTTP handler returns immediately after
		// Upgrade hands off to WebTransport; if we used r.Context()
		// the WT session would be cancelled the moment we entered the
		// goroutine.
		go func() {
			defer s.current.Add(-1)
			defer wtSess.CloseWithError(0, "bye")
			s.runSession(ctx, wtSess)
		}()
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	s.mu.Lock()
	s.wt = wts
	s.h3 = h3
	s.mu.Unlock()

	// Bind a UDP listener up front so we can resolve the port (useful
	// when callers pass ":0" for tests) before serving.
	udpConn, err := net.ListenUDP("udp", mustResolveUDP(s.cfg.Addr))
	if err != nil {
		return fmt.Errorf("listen udp: %w", err)
	}
	s.mu.Lock()
	s.addr = udpConn.LocalAddr().String()
	s.mu.Unlock()

	go func() {
		<-ctx.Done()
		// If a Shutdown is in progress it has already drained sessions;
		// we just need to close the listener now. Otherwise this is a
		// hard stop and we close immediately.
		_ = wts.Close()
		_ = udpConn.Close()
	}()

	// Use wts.Serve, NOT h3.Serve — the webtransport.Server wires up
	// its own per-connection session manager during Serve, and
	// Upgrade depends on it being initialized for incoming sessions.
	if err := wts.Serve(udpConn); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, http.ErrServerClosed) || errors.Is(err, quic.ErrServerClosed) {
			return nil
		}
		return err
	}
	return nil
}

// Metrics returns the metrics sink. Useful for tests and for
// integrators that want to read snapshot counters from the in-memory
// sink without going through Prometheus.
func (s *Server) Metrics() metrics.Metrics { return s.metrics }

// Shutdown drains the server gracefully:
//  1. Stops accepting new sessions (sets draining flag).
//  2. Sends TypeClose on every live session's control stream so
//     subscribers learn to stop sending.
//  3. Waits up to drainCtx.Deadline for in-flight sessions to exit
//     cleanly.
//  4. Closes the underlying webtransport listener and UDP socket.
//
// Returns drainCtx.Err() if drain didn't complete in budget, nil
// otherwise. Safe to call multiple times.
func (s *Server) Shutdown(drainCtx context.Context) error {
	// Mark draining so the upgrade handler rejects new sessions.
	s.draining.Store(true)
	s.logger.Info("quicrtc/server: draining", "active_sessions", s.SubscriberCount())

	// Snapshot live sessions and notify each one.
	s.trackMu.Lock()
	refs := make([]*sessionRef, 0, len(s.sessions))
	for r := range s.sessions {
		refs = append(refs, r)
	}
	s.trackMu.Unlock()
	for _, r := range refs {
		if r.sess != nil {
			r.sess.SendClose("server-shutdown")
		}
	}

	// Wait for sessions to drain or drainCtx to expire.
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if int(s.current.Load()) == 0 {
			break
		}
		select {
		case <-drainCtx.Done():
			// Force-close any remaining.
			s.mu.Lock()
			wts := s.wt
			s.mu.Unlock()
			if wts != nil {
				_ = wts.Close()
			}
			return drainCtx.Err()
		case <-tick.C:
		}
	}

	// All sessions drained — close the listener.
	s.mu.Lock()
	wts := s.wt
	s.mu.Unlock()
	if wts != nil {
		_ = wts.Close()
	}
	return nil
}

// IsDraining reports whether a Shutdown is in progress. Used by the
// upgrade handler to reject new sessions during drain.
func (s *Server) IsDraining() bool { return s.draining.Load() }

func (s *Server) runSession(parent context.Context, wt *webtransport.Session) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	sessionStart := time.Now()
	s.metrics.SessionStarted()

	acceptCtx, acceptCancel := context.WithTimeout(ctx, deadlineOrDefault(s.cfg.Session.AcceptTimeout, 5*time.Second))
	ctl, err := wt.AcceptStream(acceptCtx)
	acceptCancel()
	if err != nil {
		s.metrics.SessionEnded(time.Since(sessionStart))
		s.logger.Debug("quicrtc/server: session accept-stream failed", "err", err)
		return
	}
	defer func() {
		s.metrics.SessionEnded(time.Since(sessionStart))
	}()

	cfg := s.cfg.Session
	cfg.ExpectSlug = s.slug
	cfg.AuthValidator = s.cfg.AuthValidator
	cfg.SDP = s.cfg.SDP
	cfg.OnDataChannel = s.cfg.OnDataChannel
	cfg.OnBackpressure = s.cfg.OnBackpressure
	cfg.OnKeyframeRequest = s.cfg.OnKeyframeRequest
	cfg.InboundRateLimit = s.cfg.InboundRateLimit
	if s.cfg.SupportedFeatures != nil {
		cfg.SupportedFeatures = s.cfg.SupportedFeatures
	} else {
		cfg.SupportedFeatures = DefaultSupportedFeatures()
	}
	// Wire per-session datagram + bidi-opener adapters into FeedConfig
	// so KindTelemetry tracks land on QUIC datagrams and KindToolCalls
	// tracks open one bidi stream per call. Without these the
	// per-Kind pumps fall back to stream-per-GOP (see feed/pump.go:Run).
	cfg.FeedConfig.DatagramSender = &wtDatagramSender{wt: wt}
	cfg.FeedConfig.BidiOpener = &wtBidiOpener{wt: wt}

	// Fire OnSession only after the session has authenticated — see
	// the Config.OnSession contract. The callback runs synchronously
	// from session.Run on the session goroutine.
	if s.cfg.OnSession != nil {
		cfg.OnHandshakeComplete = func(sess *session.Session) {
			s.cfg.OnSession(SessionHandle{wt: wt, sessionID: sess.SessionID, sess: sess})
		}
	}

	sess := session.New(cfg, ctl, &uniOpener{wt: wt})
	sess.OnAllocateID = s.store.AllocateID
	sess.OnResume = func(tenant, id string, lastSeen map[string]uint32) (map[string]*pubsub.Receiver, bool) {
		recvs := s.store.Resume(tenant, id)
		if recvs == nil {
			return nil, false
		}
		// For every track the client knew about, replay AUs that the
		// broadcaster still holds in its ring buffer with Seq >
		// lastSeen[trackName]. SpliceFront merges these into the
		// receiver's pending queue at the head (dedup by Seq) so the
		// resumed subscriber sees a continuous sequence starting just
		// after its last-seen AU, even if some AUs were in flight on
		// the QUIC wire when the previous connection dropped.
		for name, fromSeq := range lastSeen {
			r, ok := recvs[name]
			if !ok {
				continue
			}
			te := s.trackEntry(name)
			if te == nil {
				continue
			}
			replay, available := te.bc.ReplaySince(fromSeq + 1)
			if !available {
				// Client fell too far behind the broadcaster's replay
				// window — gap is unrecoverable. Behavior depends on
				// track kind:
				//   - video: flag receiver to wait for the next
				//     keyframe so the decoder resyncs at the next IDR
				//     rather than playing garbage.
				//   - anything else (tokens, tool calls, telemetry,
				//     audio): there are no "keyframes" to wait for —
				//     RequestKeyframe would silently block all
				//     subsequent AUs in Broadcaster.Publish. Log the
				//     gap and continue from current channel contents.
				if te.kind == track.KindVideo || te.kind == "" {
					r.RequestKeyframe()
				}
				s.logger.Warn("quicrtc/server: resume gap exceeds replay window",
					"track", name, "kind", te.kind, "from_seq", fromSeq)
				continue
			}
			if len(replay) > 0 {
				r.SpliceFront(replay)
			}
		}
		s.metrics.SessionResumed(0)
		s.logger.Debug("quicrtc/server: session resumed", "tenant", tenant, "session_id", id)
		return recvs, true
	}
	sess.OnFreshSession = func() map[string]*pubsub.Receiver {
		out := make(map[string]*pubsub.Receiver)
		for _, t := range s.trackSnapshot() {
			out[t.name] = t.recv
		}
		return out
	}
	sess.OnLookupPriority = s.priorityFor
	sess.OnLookupTrackID = s.trackIDFor
	sess.OnLookupKind = s.kindFor
	// Wire inbound uni-stream acceptance for PublishBack (subscriber-
	// originated tracks). Sessions without this still work in the
	// server-pushes-to-subscriber direction.
	sess.SetUniReceiver(&uniAcceptor{wt: wt})

	ref := &sessionRef{
		addTrack:             sess.AttachTrack,
		addTrackWithPriority: sess.AttachTrackWithPriority,
		addTrackWithTrackID:  sess.AttachTrackWithTrackID,
		addTrackWithKind:     sess.AttachTrackWithKind,
		sess:                 sess,
	}
	cleanup := s.addSession(ref)
	defer cleanup()

	defer func() {
		// Snapshot tracks once for both branches.
		s.trackMu.Lock()
		tracks := make([]*trackEntry, 0, len(s.tracks))
		for _, t := range s.tracks {
			tracks = append(tracks, t)
		}
		s.trackMu.Unlock()

		// No resume possible — unsubscribe the active receivers.
		// Three trigger conditions:
		//   sess.SessionID == ""     : handshake never completed
		//   ctx.Err() != nil         : server itself is shutting down
		//   s.draining.Load()        : Shutdown is draining sessions —
		//                               TypeClose was sent, the client
		//                               is expected to retry against
		//                               another instance, and we
		//                               should NOT park (would leak
		//                               state per drain).
		if sess.SessionID == "" || ctx.Err() != nil || s.draining.Load() {
			for _, t := range tracks {
				for _, r := range sess.Receivers(t.name) {
					t.bc.Unsubscribe(r)
				}
			}
			return
		}

		// Resume possible. Strategy: subscribe FRESH receivers for
		// the parked session, then unsubscribe the active ones. The
		// active receivers' buffered AUs get eaten by the dying
		// pumps; the fresh receivers buffer everything from now on
		// until a reconnect attaches them to new pumps. A tiny
		// window of AUs published RIGHT NOW (between Subscribe and
		// Unsubscribe) lands in both — fresh keeps it, dying pump
		// eats the duplicate. Tradeoff: zero loss going forward, vs
		// at most one duplicated AU at the disconnect boundary.
		parked := make(map[string]*pubsub.Receiver, len(tracks))
		// Capture (track entry, parked receiver) pairs so the
		// onEvict callback can unsubscribe the right receivers
		// from the right broadcasters when the resume window
		// expires without a reconnect.
		type parkedRcv struct {
			bc *pubsub.Broadcaster
			r  *pubsub.Receiver
		}
		parkedRcvs := make([]parkedRcv, 0, len(tracks))
		// Parked receivers need to hold (a) live AUs that arrive
		// during the disconnect window, and (b) the replay-buffer
		// AUs that SpliceFront injects at resume time. Use a
		// generous capacity so SpliceFront never has to evict.
		const parkedChanCap = pubsub.DefaultReplayCap + 64
		for _, t := range tracks {
			r := t.bc.SubscribeWithCapacity(parkedChanCap)
			parked[t.name] = r
			parkedRcvs = append(parkedRcvs, parkedRcv{bc: t.bc, r: r})
		}
		for _, t := range tracks {
			for _, r := range sess.Receivers(t.name) {
				t.bc.Unsubscribe(r)
			}
		}
		onEvict := func() {
			for _, p := range parkedRcvs {
				p.bc.Unsubscribe(p.r)
			}
		}
		s.store.Park(sess.Tenant, sess.SessionID, parked, onEvict)
	}()

	_ = sess.Run(ctx)
}

func deadlineOrDefault(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

func mustResolveUDP(addr string) *net.UDPAddr {
	u, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		// Don't actually panic in production; pre-validated by
		// callers via Config.Addr. Return a benign zero-value that
		// ListenUDP will reject with a clear error.
		return &net.UDPAddr{}
	}
	return u
}

// uniOpener adapts *webtransport.Session to feed.StreamOpener.
type uniOpener struct{ wt *webtransport.Session }

func (o *uniOpener) OpenSendStream(ctx context.Context) (feed.SendStream, error) {
	s, err := o.wt.OpenUniStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return &uniStream{s: s}, nil
}

// uniAcceptor adapts *webtransport.Session to session.UniReceiver
// for PublishBack: it accepts uni streams the SUBSCRIBER opens
// toward the server.
type uniAcceptor struct{ wt *webtransport.Session }

func (a *uniAcceptor) AcceptUniStream(ctx context.Context) (session.UniStream, error) {
	s, err := a.wt.AcceptUniStream(ctx)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// uniStream adapts *webtransport.SendStream to feed.SendStream. The
// types are isomorphic except for CancelWrite's argument type.
type uniStream struct{ s *webtransport.SendStream }

func (u *uniStream) Write(p []byte) (int, error)        { return u.s.Write(p) }
func (u *uniStream) Close() error                       { return u.s.Close() }
func (u *uniStream) SetWriteDeadline(t time.Time) error { return u.s.SetWriteDeadline(t) }
func (u *uniStream) CancelWrite(code uint32)            { u.s.CancelWrite(webtransport.StreamErrorCode(code)) }

// wtDatagramSender adapts *webtransport.Session to feed.DatagramSender
// so KindTelemetry tracks can write directly to the QUIC datagram path.
type wtDatagramSender struct{ wt *webtransport.Session }

func (d *wtDatagramSender) SendDatagram(payload []byte) error {
	return d.wt.SendDatagram(payload)
}

// wtBidiOpener adapts *webtransport.Session to feed.BidiOpener so
// KindToolCalls tracks can open one bidi stream per call.
type wtBidiOpener struct{ wt *webtransport.Session }

func (b *wtBidiOpener) OpenBidiStream(ctx context.Context) (feed.BidiStream, error) {
	s, err := b.wt.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return &wtBidiStream{s: s}, nil
}

// wtBidiStream adapts *webtransport.Stream to feed.BidiStream. Same
// shape as uniStream but exposes both ends of the stream and the
// CancelRead control needed for bidi timeouts.
type wtBidiStream struct{ s *webtransport.Stream }

func (b *wtBidiStream) Write(p []byte) (int, error) { return b.s.Write(p) }
func (b *wtBidiStream) Read(p []byte) (int, error)  { return b.s.Read(p) }
func (b *wtBidiStream) CloseWrite() error           { return b.s.Close() }
func (b *wtBidiStream) CancelWrite(code uint32)     { b.s.CancelWrite(webtransport.StreamErrorCode(code)) }
func (b *wtBidiStream) CancelRead(code uint32)      { b.s.CancelRead(webtransport.StreamErrorCode(code)) }

func makeOriginCheck(allowed []string) func(*http.Request) bool {
	if len(allowed) == 0 {
		return func(*http.Request) bool { return true }
	}
	set := make(map[string]struct{}, len(allowed))
	for _, o := range allowed {
		set[o] = struct{}{}
	}
	return func(r *http.Request) bool {
		_, ok := set[r.Header.Get("Origin")]
		return ok
	}
}
