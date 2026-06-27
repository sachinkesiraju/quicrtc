// Package session implements quicrtc's per-connection state machine.
//
// One Session = one subscriber's connection. Lifetime: from
// WebTransport upgrade to disconnect. The state machine:
//
//  1. Accept the bidi control stream (timeout: AcceptTimeout).
//  2. Read HELLO (timeout: ReadHelloTimeout). Constant-time slug
//     compare; reject mismatched role or version.
//  3. Send SDP.
//  4. Replay TypeAnnounce for every attached track. Clients must
//     tolerate an Announce burst interleaved with PONG/DATA after SDP.
//  5. Spawn three goroutines:
//     - feed pump (broadcaster receiver -> uni streams)
//     - control reader (PING/CLOSE/DATA inbound)
//     - idle watchdog (cancels ctx if feed-write progress stalls)
//  6. Block until any of those finish or ctx is cancelled.
//
// The watchdog deliberately keys on data-plane progress only, NOT
// control-stream activity. Otherwise an attacker who knows the slug
// could pin a session indefinitely by spamming PINGs while never
// reading the feed — receiver-side flow control would block our
// feed writes, but the heartbeat would keep the idle timer alive.
//
// "Progress" means: a successful outbound feed write, or an inbound
// (PublishBack) AU accepted past the rate limiter. The watchdog fires
// only when an outbound write has been ATTEMPTED more recently than
// the last progress — i.e. writes are stuck, not merely absent. A
// session with nothing to send (agent idle between responses,
// inbound-only publishers) never trips the watchdog; a peer that
// stops reading while we have data to deliver still does. Dead-peer
// detection for fully-quiet sessions is the QUIC layer's idle
// timeout, not this watchdog.
package session

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sachinkesiraju/quicrtc/datachannel"
	"github.com/sachinkesiraju/quicrtc/feed"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/track"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// ControlStream is the bidi stream used for HELLO / SDP / PING / DATA.
// webtransport.Stream satisfies this interface; tests use a
// (net.Pipe-backed) fake.
type ControlStream interface {
	io.Reader
	io.Writer
	io.Closer
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
}

// Config tunes the session state machine. Zero values pick reasonable
// defaults.
type Config struct {
	// ExpectSlug is the auth secret advertised in the share link; the
	// HELLO frame's slug field must match constant-time. Used when
	// AuthValidator is nil. For multi-tenant deployments use
	// AuthValidator instead.
	ExpectSlug string

	// AuthValidator, if non-nil, is consulted in place of ExpectSlug.
	// Receives the raw HELLO slug field (callers can stuff a JWT,
	// bearer token, or any opaque credential there). Returns the
	// tenant scope (used to namespace parked sessions for resume —
	// see OnResume) and nil if the credential is valid; any error
	// rejects with auth failed. Constant-time compare is the
	// validator's responsibility.
	//
	// Returning an empty tenant string is acceptable for single-
	// tenant deployments; resume will work session-wide. For
	// multi-tenant deployments, ALWAYS return a non-empty tenant
	// derived from the credential — the SessionStore uses it to
	// prevent cross-tenant session resumption.
	AuthValidator func(credential string) (tenant string, err error)

	// OnBackpressure, if non-nil, is invoked when the peer signals
	// receive-side backpressure via TypeBackpressure. trackName
	// identifies the track (empty = session-level). level is 0..100.
	OnBackpressure func(sessionID, trackName string, level uint8)

	// OnKeyframeRequest, if non-nil, is invoked when the subscriber
	// sends a Backpressure frame with NeedsKeyframe=true. The
	// application should signal its video source/encoder to emit a
	// fresh keyframe on the named track immediately — typically
	// within one encoded-frame interval (~33ms at 30fps).
	//
	// This is the receiver-driven recovery path for computer-use /
	// live-screen workloads: caps visible-stutter recovery time at
	// ~one frame instead of one GOP. Without this hook, the
	// subscriber waits for the next natural keyframe (could be up
	// to GOP seconds away).
	OnKeyframeRequest func(sessionID, trackName string)

	// InboundRateLimit caps inbound (PublishBack) traffic per session.
	// Zero = unlimited. Set sane defaults in production to prevent a
	// misbehaving client from saturating the session.
	InboundRateLimit RateLimit

	// SupportedFeatures is the set of optional protocol features the
	// server offers. The client's HELLO.Features is intersected with
	// this set; the result echoes back in SDP.Features. Both peers
	// should treat features outside the intersection as unsupported.
	//
	// Stable feature names so far:
	//   "resume"        — TypeResume + replay buffer
	//   "backpressure"  — TypeBackpressure subscriber→publisher
	//   "publishback"   — subscriber-side AddTrack
	//   "stream-header" — TypeStreamHeader on uni feed streams
	SupportedFeatures []string

	// SDP is sent to the subscriber after a successful HELLO.
	SDP wire.SDP

	// AcceptTimeout caps how long we wait for the subscriber to open
	// its bidi control stream. Default 5s.
	AcceptTimeout time.Duration

	// ReadHelloTimeout caps how long we wait for the first frame on
	// the control stream. Default 3s.
	ReadHelloTimeout time.Duration

	// IdleTimeout cancels the session if no successful feed write has
	// happened in this window. Default 60s.
	IdleTimeout time.Duration

	// ResumeWindow is how long the server keeps a parked session
	// alive after disconnect, waiting for the same client to
	// reconnect with the same SessionID. Default 60s.
	ResumeWindow time.Duration

	// FeedConfig is forwarded to feed.Pump. Zero values pick defaults.
	FeedConfig feed.Config

	// DataInboundBuffer sets the DataChannel inbound queue depth.
	// Default 64.
	DataInboundBuffer int

	// OnDataChannel, if non-nil, is invoked once after handshake with
	// the session's DataChannel. The callback runs in its own
	// goroutine; the session does not wait for it.
	OnDataChannel func(*datachannel.Channel)

	// OnInboundDropped, if non-nil, is invoked when an inbound (PublishBack)
	// AU is dropped because the per-track queue was full. Symmetrical
	// with feed.Config.OnAUDropped for the outbound direction. Optional;
	// useful for metrics that want visibility into receiver-side loss
	// (otherwise drops are silent).
	OnInboundDropped func(trackName string)

	// OnHandshakeComplete, if non-nil, is invoked from Run after the
	// HELLO/SDP handshake succeeds and SessionID is populated. Used by
	// the server to expose the SessionHandle to applications only after
	// the peer has authenticated. The callback runs on the session
	// goroutine; long-running work should spawn its own goroutine.
	OnHandshakeComplete func(s *Session)

	// OnAnnounce, if non-nil, gates inbound TypeAnnounce frames from
	// the subscriber. Called BEFORE the inbound track is allocated.
	// Returning a non-nil error rejects the announce: the server sends
	// a TypeError frame carrying a wire.ErrorPayload{Code:
	// "track_unauthorized"} and the track is not registered. Without
	// OnAnnounce set, all announces are accepted (legacy behavior).
	//
	// Use this to gate multi-tenant PublishBack: a subscriber
	// authenticated as tenant A should not be allowed to publish on a
	// track namespace owned by tenant B. The race between the
	// subscriber's Announce and a uni stream naming the same track is
	// closed at stream-header time. When OnAnnounce is set, a stream
	// naming a not-yet-announced (or rejected) track is dropped before
	// any AU is delivered to the inbound queue.
	OnAnnounce func(tenant, sessionID, trackName, kind string) error

	// OnKindStats, if non-nil, is invoked whenever the subscriber
	// emits a TypeKindStats observability frame. Fires only when the
	// "kind-stats" feature was negotiated in HELLO; older clients
	// don't emit and the callback never runs.
	//
	// Applications use this to learn the true end-to-end p99 per
	// Kind without guessing from local queue depth alone, and to
	// drive SLO-aware adaptation (a v1.x+ feature on top of this).
	OnKindStats func(sessionID string, stats wire.KindStats)

	// OnProtocolError, if non-nil, is invoked when a well-framed
	// control frame carries a payload that fails to parse (malformed
	// Announce/Unannounce/Backpressure/KindStats JSON). The frame is
	// still dropped — this hook exists so protocol violations are
	// observable instead of silent. Called from the control reader;
	// keep it cheap.
	OnProtocolError func(sessionID string, frameType byte, err error)
}

// ErrAuth is returned when the subscriber's HELLO slug doesn't match.
var ErrAuth = errors.New("quicrtc/session: auth")

// ErrBadHello indicates a HELLO frame that was malformed or had the
// wrong type / role.
var ErrBadHello = errors.New("quicrtc/session: bad hello")

// ErrIdle is returned when the idle watchdog fires.
var ErrIdle = errors.New("quicrtc/session: idle timeout")

// minPingInterval is the minimum interval between PONG responses we
// produce for inbound PINGs. Faster PINGs are silently dropped to
// prevent an authenticated peer from spending server CPU on
// heartbeat work. One PONG/sec is well above what any legitimate
// keepalive needs.
const minPingInterval = time.Second

// inboundFrameTimeout bounds how long the inbound drain loop waits
// for the REMAINDER of a frame once its first byte has arrived (and
// for the stream header on a freshly-opened stream). Gaps between
// complete frames may be arbitrarily long — sporadic token tracks
// are legitimate — but a peer that opens a stream and stalls mid-
// frame gets its drain goroutine reclaimed instead of pinned forever.
// Generous on purpose: the goal is reclamation, not latency.
const inboundFrameTimeout = 30 * time.Second

// maxInboundTracks caps how many distinct inbound (PublishBack)
// tracks a single session can materialize from the wire. Each track
// entry holds a 64-slot AU channel; without a cap an authenticated
// peer could grow the map without bound by announcing (or naming in
// stream headers) ever-new track names. 256 matches the relay's
// MaxTracksPerUpstream default and is far above any real workload
// (typically 1-5 tracks).
const maxInboundTracks = 256

// UniReceiver is the minimal subset of webtransport.Session needed to
// accept inbound uni streams from a subscriber that's running
// PublishBack. The session uses this to demux subscriber-originated
// feed streams into per-track receive queues.
type UniReceiver interface {
	AcceptUniStream(ctx context.Context) (UniStream, error)
}

// UniStream is the read side of an inbound uni QUIC stream.
// SetReadDeadline lets the drain loop bound how long a half-sent
// frame can pin its goroutine; webtransport.ReceiveStream satisfies
// this directly.
type UniStream interface {
	io.Reader
	SetReadDeadline(time.Time) error
}

// Session drives one subscriber connection. Use Run.
type Session struct {
	ID  uint64
	cfg Config

	ctl     ControlStream
	opener  feed.StreamOpener
	uniRecv UniReceiver // optional; nil disables PublishBack

	// SessionID is the persistent identifier for cross-disconnect
	// resumption. Allocated on the first HELLO if the client didn't
	// supply one; echoed in the SDP response. Stable across reconnects
	// when the client supplies the same value.
	SessionID string

	// NegotiatedFeatures is the intersection of client-declared and
	// server-supported optional protocol features. Populated after
	// handshake completes. Both peers should treat features outside
	// this list as unsupported.
	NegotiatedFeatures []string

	// NegotiatedVersion is the wire-format version both peers agreed
	// on at HELLO time. Equal to wire.CurrentVersion in single-version
	// deployments. Populated after handshake completes.
	NegotiatedVersion string

	// Tenant is the auth-scoped tenant identifier for this session,
	// returned by AuthValidator. Empty for single-tenant deployments.
	// Used as a namespace key in the SessionStore so resume requests
	// from one tenant cannot match another tenant's parked sessions.
	Tenant string

	// HelloSessionID is what the client requested in HELLO. If non-
	// empty and the server can honor it (matching parked session
	// found), SessionID equals HelloSessionID. Otherwise the server
	// allocates a fresh SessionID via the OnAllocateID callback.
	HelloSessionID string

	// HelloLastSeenSeq is the per-track continuation map sent by the
	// client in HELLO.LastSeenSeq. Populated unconditionally during
	// handshake (nil if the client didn't send the field) so the
	// OnResume callback can use it to replay missed AUs from each
	// broadcaster's replay buffer before the resumed receiver starts
	// pumping. Tracks absent from this map fall back to the existing
	// park-channel-contents-only semantics.
	HelloLastSeenSeq map[string]uint32

	// OnAllocateID is set by the caller (server.runSession) to a
	// function that mints a fresh session ID. Allows session pkg
	// to remain agnostic of the store implementation.
	OnAllocateID func() string

	// OnResume is called during handshake when the client requests
	// a resume (HELLO carries SessionID). The callback receives the
	// tenant scope derived from AuthValidator (empty for single-
	// tenant deployments), the helloSessionID, and the client's
	// per-track LastSeenSeq cursor (nil if the client didn't send
	// one — older clients still resume, just without the replay-
	// buffer gap fill). Returns the receivers to attach (one per
	// track) if a parked session exists scoped to this tenant;
	// ok=false means no resume — fresh session, fresh receivers come
	// from the OnFreshSession callback.
	//
	// Implementations MUST namespace by tenant: a resume request
	// from tenant A with a SessionID belonging to tenant B's parked
	// session must NOT match.
	OnResume func(tenant, helloSessionID string, lastSeenSeq map[string]uint32) (receivers map[string]*pubsub.Receiver, ok bool)

	// OnFreshSession is called during handshake for new sessions
	// (no resume) to obtain the initial track receivers from the
	// server-side track set.
	OnFreshSession func() map[string]*pubsub.Receiver

	// OnLookupPriority, if non-nil, is called per-track during
	// handshake/AttachTrack to obtain the RFC 9218 priority for
	// that track. Returns the default (4) when the lookup fails or
	// the callback is nil. Lets the session inherit per-track
	// priority hints from the publisher's TrackEntry registry.
	OnLookupPriority func(name string) uint8

	// OnLookupTrackID, if non-nil, returns the 1-byte trackID used by
	// the datagram envelope (wire/datagram.go) for a given track
	// name. Returns 0 when the lookup is nil or the track has no
	// reserved ID. Stream-based delivery classes ignore this value.
	OnLookupTrackID func(name string) uint8

	// OnLookupKind, if non-nil, returns the track.Kind for a given
	// track name; the session maps Kind to DeliveryClass via
	// track.DefaultDeliveryClass when populating the per-track pump
	// config. Empty/unknown returns KindVideo (the legacy default).
	OnLookupKind func(name string) track.Kind

	writer *ctlWriter
	dc     *datachannel.Channel

	bytesOut     atomic.Uint64
	lastFeedNano atomic.Int64
	// lastAttemptNano is when a pump last dequeued an AU for delivery.
	// The idle watchdog fires only when this is newer than lastFeedNano
	// (a write was attempted but never completed) — distinguishing
	// "stuck" from "nothing to send".
	lastAttemptNano atomic.Int64
	// lastPingNano rate-limits inbound PING handling so an authenticated
	// peer can't pin CPU by spamming PINGs. Stored as unix nanoseconds;
	// PINGs faster than minPingInterval are silently dropped.
	lastPingNano atomic.Int64

	// Track set. Tracks may be attached before Run starts (snapshot
	// at session start) or added dynamically while Run is in flight
	// (when the publisher calls AddTrack).
	trackMu    sync.Mutex
	tracks     map[string]*trackPump
	runStarted bool
	runCtx     context.Context
	pumpDone   chan error
	// resumedLeading marks tracks for which the resuming client sent a
	// LastSeenSeq cursor: the client holds decoder state through that
	// seq, so the pump may write the spliced mid-GOP P-frame run on
	// its first stream (feed.Config.AllowLeadingPFrames). Populated
	// during handshake before pumps start; read under trackMu.
	resumedLeading map[string]bool

	// Inbound (PublishBack) track state. Subscribers Announce a track
	// on the bidi control stream, then open uni streams whose stream-
	// header names that track. The session demuxes incoming AUs into
	// per-track inbound queues, exposed via InboundRecv.
	inboundMu     sync.Mutex
	inboundTracks map[string]*inboundTrack

	// inboundRL is the per-session rate limiter for inbound AUs.
	// nil-equivalent (no limits) when InboundRateLimit zero.
	inboundRL *rateLimiter
}

type inboundTrack struct {
	name      string
	kind      string
	codec     string
	ch        chan pubsub.AccessUnit
	announced bool
	// authorized indicates the Announce passed the OnAnnounce validator
	// (or no validator is configured, in which case it's true by
	// default). The uni-stream demuxer gates AU delivery on this when
	// OnAnnounce is set, closing the race where a uni stream arrives
	// for a track before the corresponding Announce frame.
	authorized bool
	// done is closed by handleUnannounce. Senders in drainInboundStream
	// and readers in InboundRecv both select on it, so unannouncing a
	// track in flight is race-free: closing t.ch directly would panic
	// any concurrent send. Closing done is safe (one-shot, guarded by
	// inboundMu) and unblocks readers via select default behavior.
	done chan struct{}
}

type trackPump struct {
	name string
	recv *pubsub.Receiver
	cfg  feed.Config
	stop chan struct{} // closed when Run returns to release pump
}

var sessionSeq atomic.Uint64

// New constructs a Session. ctl is the freshly-accepted bidi control
// stream; opener creates new uni feed streams. Tracks are attached
// via AttachTrack before or during Run.
func New(cfg Config, ctl ControlStream, opener feed.StreamOpener) *Session {
	applyDefaults(&cfg)
	w := &ctlWriter{w: ctl}
	send := func(payload []byte) error {
		return w.Write(wire.TypeData, payload)
	}
	return &Session{
		ID:            sessionSeq.Add(1),
		cfg:           cfg,
		ctl:           ctl,
		opener:        opener,
		writer:        w,
		dc:            datachannel.New(send, cfg.DataInboundBuffer),
		tracks:        make(map[string]*trackPump),
		pumpDone:      make(chan error, 16),
		inboundTracks: make(map[string]*inboundTrack),
		inboundRL:     newRateLimiter(cfg.InboundRateLimit),
	}
}

// SetUniReceiver attaches the inbound uni-stream acceptor used for
// PublishBack. Must be called before Run if the caller wants to
// receive subscriber-published tracks. Optional; sessions without a
// UniReceiver still work in the server-pushes-to-subscriber direction.
func (s *Session) SetUniReceiver(r UniReceiver) {
	s.uniRecv = r
}

// InboundRecv blocks until the next AccessUnit on the named inbound
// track arrives. The track must have been Announced by the subscriber
// (a TypeAnnounce frame on the control stream); otherwise this blocks
// until the announce arrives or ctx is done. Returns io.EOF when the
// subscriber Unannounces the track.
func (s *Session) InboundRecv(ctx context.Context, name string) (pubsub.AccessUnit, error) {
	t := s.inboundTrackChan(name)
	select {
	case au, ok := <-t.ch:
		if !ok {
			return pubsub.AccessUnit{}, io.EOF
		}
		return au, nil
	case <-t.done:
		// Drain any AUs buffered before the Unannounce arrived; if
		// the channel is empty, surface EOF.
		select {
		case au, ok := <-t.ch:
			if !ok {
				return pubsub.AccessUnit{}, io.EOF
			}
			return au, nil
		default:
			return pubsub.AccessUnit{}, io.EOF
		}
	case <-ctx.Done():
		return pubsub.AccessUnit{}, ctx.Err()
	}
}

// inboundTrackChan returns (creating if needed) the receive queue for
// an inbound (PublishBack) track. Used by both InboundRecv (caller-
// driven) and the uni-stream demuxer (creates entries when the wire
// names a track that the announce hasn't been processed for yet).
func (s *Session) inboundTrackChan(name string) *inboundTrack {
	s.inboundMu.Lock()
	defer s.inboundMu.Unlock()
	if t, ok := s.inboundTracks[name]; ok {
		return t
	}
	t := &inboundTrack{
		name: name,
		ch:   make(chan pubsub.AccessUnit, 64),
		done: make(chan struct{}),
	}
	s.inboundTracks[name] = t
	return t
}

// inboundTrackChanWire is inboundTrackChan for wire-driven creation
// (Announce frames, stream headers). Unlike the app-driven path
// (InboundRecv), it refuses to materialize new tracks past
// maxInboundTracks: ok=false means the caller should reject the
// announce / drop the stream. Existing tracks are always returned.
func (s *Session) inboundTrackChanWire(name string) (*inboundTrack, bool) {
	s.inboundMu.Lock()
	defer s.inboundMu.Unlock()
	if t, ok := s.inboundTracks[name]; ok {
		return t, true
	}
	if len(s.inboundTracks) >= maxInboundTracks {
		return nil, false
	}
	t := &inboundTrack{
		name: name,
		ch:   make(chan pubsub.AccessUnit, 64),
		done: make(chan struct{}),
	}
	s.inboundTracks[name] = t
	return t, true
}

// InboundTracks returns the names of all currently-known inbound
// tracks. Used by transport layer to expose RemoteTracks().
func (s *Session) InboundTracks() []string {
	s.inboundMu.Lock()
	defer s.inboundMu.Unlock()
	out := make([]string, 0, len(s.inboundTracks))
	for name, t := range s.inboundTracks {
		if t.announced {
			out = append(out, name)
		}
	}
	return out
}

// AttachTrack adds a track to this session at default priority.
// Equivalent to AttachTrackWithPriority(name, recv, 4).
func (s *Session) AttachTrack(name string, recv *pubsub.Receiver) {
	s.AttachTrackWithPriority(name, recv, 4)
}

// AttachTrackWithPriority adds a track with a specific RFC 9218
// priority hint. The priority is plumbed into feed.Config but is
// observability-only until quic-go exposes PRIORITY_UPDATE.
//
// Multi-track sessions write a TypeStreamHeader frame as the first
// frame on each new uni feed stream, so the receiver can demux. The
// implicit "primary" track keeps single-track wire compatibility:
// when only one track named "primary" is active, the stream-header
// frame is still written but receivers that ignore it still parse
// the rest of the stream as a single-track feed.
func (s *Session) AttachTrackWithPriority(name string, recv *pubsub.Receiver, priority uint8) {
	s.AttachTrackWithTrackID(name, recv, priority, 0)
}

// AttachTrackWithTrackID is AttachTrackWithPriority plus a per-track
// 1-byte ID used as the datagram envelope demux key for
// DeliveryDatagramOrStream tracks. Pass 0 if the track does not use
// datagrams (in which case the ID is ignored by stream-based pumps).
//
// The track's Kind (and thus its DeliveryClass) is recovered via
// OnLookupKind when set; otherwise the track falls through to the
// legacy stream-per-GOP path. New code should prefer AttachTrackWithKind,
// which passes the Kind explicitly and avoids the lookup race.
func (s *Session) AttachTrackWithTrackID(name string, recv *pubsub.Receiver, priority uint8, trackID uint8) {
	var kind track.Kind
	if s.OnLookupKind != nil {
		kind = s.OnLookupKind(name)
	}
	s.AttachTrackWithKind(name, kind, recv, priority, trackID)
}

// AttachTrackWithKind is the modern attach entry point: it carries
// the track's Kind explicitly and uses it to pick the DeliveryClass
// via track.DefaultDeliveryClass. Without this, the per-track pump
// runs the legacy stream-per-GOP path regardless of payload shape.
func (s *Session) AttachTrackWithKind(name string, kind track.Kind, recv *pubsub.Receiver, priority uint8, trackID uint8) {
	s.trackMu.Lock()
	if _, ok := s.tracks[name]; ok {
		s.trackMu.Unlock()
		return
	}
	tp := &trackPump{
		name: name,
		recv: recv,
		cfg:  s.cfg.FeedConfig,
		stop: make(chan struct{}),
	}
	tp.cfg.TrackName = name
	tp.cfg.Priority = priority
	tp.cfg.TrackID = trackID
	// Explicit kind wins; an empty kind preserves whatever FeedConfig
	// already set (lets callers pre-pin Delivery on the session config
	// for tests / single-kind deployments).
	if kind != "" {
		tp.cfg.Delivery = track.DefaultDeliveryClass(kind)
	}
	hooked := withFeedHook(s.cfg.FeedConfig, s.touchFeedAndCount, s.touchAttempt)
	tp.cfg.OnWriteBytes = hooked.OnWriteBytes
	tp.cfg.OnWriteAttempt = hooked.OnWriteAttempt
	if s.resumedLeading[name] {
		tp.cfg.AllowLeadingPFrames = true
	}
	// When this subscriber negotiated "kind-stats", stamp a publish
	// wall-clock on every outbound feed frame so it can compute true
	// publish->recv latency. v1 subscribers never negotiate it, so
	// they keep receiving plain feed frames.
	if s.hasNegotiatedFeature(featureKindStats) {
		tp.cfg.StampPublishWall = true
	}
	s.tracks[name] = tp
	started := s.runStarted
	ctx := s.runCtx
	s.trackMu.Unlock()
	if started {
		go s.runOnePump(ctx, tp)
	}
}

// Receivers returns the receivers attached to the named track. Used
// by the server to unsubscribe receivers from broadcasters at session
// close.
func (s *Session) Receivers(name string) []*pubsub.Receiver {
	s.trackMu.Lock()
	defer s.trackMu.Unlock()
	tp, ok := s.tracks[name]
	if !ok {
		return nil
	}
	return []*pubsub.Receiver{tp.recv}
}

// DetachTrack stops the pump for the named outbound track and
// removes it from the session's track set. Used by Server.RemoveTrack
// to tear down per-session machinery for a removed track.
func (s *Session) DetachTrack(name string) {
	s.trackMu.Lock()
	tp, ok := s.tracks[name]
	if !ok {
		s.trackMu.Unlock()
		return
	}
	delete(s.tracks, name)
	s.trackMu.Unlock()
	// Closing tp.stop signals the pump to exit even if the
	// broadcaster still holds the receiver (server's RemoveTrack
	// closes the broadcaster after this; either path stops the pump).
	close(tp.stop)
}

// SendAnnounce informs the connected peer about a track that became
// available on this session. Used by Server.AddTrack to push a
// TypeAnnounce after the per-session pump is wired up so subscribers
// can populate RemoteTracks() / start RecvOn loops eagerly.
func (s *Session) SendAnnounce(name, kind, codec string) {
	body, err := wire.Announce{Name: name, Kind: kind, Codec: codec}.Marshal()
	if err != nil {
		return
	}
	_ = s.writer.Write(wire.TypeAnnounce, body)
}

// SendUnannounce notifies the peer that a track has been removed.
// Used by Server.RemoveTrack so subscribers can close their per-
// track recv channels.
func (s *Session) SendUnannounce(name string) {
	body, err := wire.Unannounce{Name: name}.Marshal()
	if err != nil {
		return
	}
	_ = s.writer.Write(wire.TypeUnannounce, body)
}

// SendClose writes a TypeClose control frame with an optional reason.
// Used by Server.Shutdown to signal subscribers that the server is
// draining so they can disconnect cleanly (and reconnect to a
// different instance).
func (s *Session) SendClose(reason string) {
	_ = s.writer.Write(wire.TypeClose, []byte(reason))
}

func (s *Session) runOnePump(ctx context.Context, tp *trackPump) {
	// Derive a child context that cancels when DetachTrack closes
	// tp.stop. Without this, RemoveTrack on the server side would
	// leak the pump until session close.
	pumpCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-tp.stop:
			cancel()
		case <-pumpCtx.Done():
		}
	}()
	p := feed.New(s.opener, tp.cfg)
	err := p.Run(pumpCtx, tp.recv)
	// Pump exits in three ways:
	//   (a) graceful close — receiver channel closed (RemoveTrack
	//       on server cascades through broadcaster.Close); err==nil
	//   (b) track removed via DetachTrack — pumpCtx cancelled but
	//       session ctx alive
	//   (c) session shutdown — session ctx cancelled
	// Only (c) should terminate the session; (a) and (b) are benign
	// per-track exits. Filter accordingly.
	if err == nil || ctx.Err() == nil {
		return
	}
	select {
	case s.pumpDone <- err:
	default:
	}
}

func applyDefaults(c *Config) {
	if c.AcceptTimeout == 0 {
		c.AcceptTimeout = 5 * time.Second
	}
	if c.ReadHelloTimeout == 0 {
		c.ReadHelloTimeout = 3 * time.Second
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = 60 * time.Second
	}
	if c.DataInboundBuffer == 0 {
		c.DataInboundBuffer = 64
	}
}

// DataChannel returns this session's data channel. Available
// immediately after New; messages received before HELLO completes
// are not delivered (the control reader doesn't run until then).
func (s *Session) DataChannel() *datachannel.Channel { return s.dc }

// BytesOut is total feed-stream bytes written successfully.
func (s *Session) BytesOut() uint64 { return s.bytesOut.Load() }

// Run drives the state machine to completion. Returns nil on graceful
// close, or the first non-nil error from the handshake / pump / reader.
func (s *Session) Run(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	defer s.dc.Close()

	// Initialize feed clock to "now" so the watchdog doesn't see a
	// 1970 timestamp and immediately fire.
	s.touchFeed()

	if err := s.handshake(ctx); err != nil {
		return err
	}

	// Notify the embedder that the peer is now authenticated and the
	// SessionID is populated. The server uses this to expose the
	// per-session SessionHandle to its OnSession callback; without it
	// applications would receive a handle backed by an unauthenticated
	// peer.
	if s.cfg.OnHandshakeComplete != nil {
		s.cfg.OnHandshakeComplete(s)
	}

	// Replay Announces so RemoteTracks() reflects pre-existing tracks
	// without waiting for the first uni stream header. Goroutine'd to
	// avoid blocking Run on a slow control reader.
	go func() {
		s.trackMu.Lock()
		names := make([]string, 0, len(s.tracks))
		for n := range s.tracks {
			names = append(names, n)
		}
		s.trackMu.Unlock()
		for _, n := range names {
			var kindStr string
			if s.OnLookupKind != nil {
				kindStr = string(s.OnLookupKind(n))
			}
			s.SendAnnounce(n, kindStr, "")
		}
	}()

	if cb := s.cfg.OnDataChannel; cb != nil {
		go cb(s.dc)
	}

	// Mark Run started so subsequent AttachTrack spawns pumps inline.
	// Spawn pumps for every track attached before Run.
	s.trackMu.Lock()
	s.runCtx = ctx
	s.runStarted = true
	preTracks := make([]*trackPump, 0, len(s.tracks))
	for _, tp := range s.tracks {
		preTracks = append(preTracks, tp)
	}
	s.trackMu.Unlock()
	for _, tp := range preTracks {
		go s.runOnePump(ctx, tp)
	}

	readerDone := make(chan error, 1)
	go func() { readerDone <- s.controlReader(ctx) }()

	idleDone := make(chan error, 1)
	go func() { idleDone <- s.idleWatchdog(ctx) }()

	if s.uniRecv != nil {
		go s.uniAcceptLoop(ctx)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-s.pumpDone:
		// One pump errored out — terminate session. Other pumps will
		// see ctx cancel via the deferred cancel().
		return err
	case err := <-readerDone:
		return err
	case err := <-idleDone:
		return err
	}
}

func (s *Session) handshake(ctx context.Context) error {
	if err := s.ctl.SetReadDeadline(time.Now().Add(s.cfg.ReadHelloTimeout)); err != nil {
		return err
	}
	t, payload, err := wire.ReadControlFrame(s.ctl)
	if err != nil {
		return fmt.Errorf("read hello: %w", err)
	}
	if t != wire.TypeHello {
		s.bestEffortError([]byte("expected HELLO"))
		return fmt.Errorf("%w: first frame type %#x", ErrBadHello, t)
	}
	hello, err := wire.UnmarshalHello(payload)
	if err != nil {
		s.bestEffortError([]byte("bad hello json"))
		return fmt.Errorf("%w: %v", ErrBadHello, err)
	}
	// Wire-format version negotiation. Clients that send only the
	// legacy Version field (no min/max) are treated as min=max=Version
	// (the same single-version semantics v0.1 used). Clients that
	// send min/max can negotiate a range.
	clientMin := hello.MinVersion
	if clientMin == "" {
		clientMin = hello.Version
	}
	clientMax := hello.MaxVersion
	if clientMax == "" {
		clientMax = hello.Version
	}
	// Empty Version + empty min/max from a very old client: fall back
	// to CurrentVersion (the legacy hello.Version == "" pass-through).
	if clientMin == "" && clientMax == "" {
		clientMin = wire.CurrentVersion
		clientMax = wire.CurrentVersion
	}
	negotiated, ok := wire.NegotiateVersion(clientMin, clientMax, wire.MinSupportedVersion, wire.CurrentVersion)
	if !ok {
		s.bestEffortError([]byte("version"))
		return fmt.Errorf("%w: client offered %s-%s, server %s-%s",
			ErrBadHello, clientMin, clientMax, wire.MinSupportedVersion, wire.CurrentVersion)
	}
	s.NegotiatedVersion = negotiated
	// Auth: prefer the pluggable validator when set; otherwise fall
	// back to constant-time slug comparison.
	if s.cfg.AuthValidator != nil {
		tenant, err := s.cfg.AuthValidator(hello.Slug)
		if err != nil {
			s.bestEffortError([]byte("auth"))
			return fmt.Errorf("%w: %v", ErrAuth, err)
		}
		s.Tenant = tenant
	} else if subtle.ConstantTimeCompare([]byte(hello.Slug), []byte(s.cfg.ExpectSlug)) != 1 {
		s.bestEffortError([]byte("auth"))
		return ErrAuth
	}
	if hello.Role != "" && hello.Role != "recv" {
		s.bestEffortError([]byte("bad role"))
		return fmt.Errorf("%w: role %q", ErrBadHello, hello.Role)
	}
	_ = s.ctl.SetReadDeadline(time.Time{}) // clear deadline once authed

	s.HelloSessionID = hello.SessionID
	s.HelloLastSeenSeq = hello.LastSeenSeq

	// Decide: resume an existing session, or start fresh.
	resumed := false
	if hello.SessionID != "" && s.OnResume != nil {
		if receivers, ok := s.OnResume(s.Tenant, hello.SessionID, hello.LastSeenSeq); ok {
			s.SessionID = hello.SessionID
			// Tracks with a LastSeenSeq cursor may continue mid-GOP:
			// the client holds decoder state through that seq, so the
			// pump can write the spliced P-frame run on its first
			// stream instead of stalling until the next keyframe.
			if len(hello.LastSeenSeq) > 0 {
				s.trackMu.Lock()
				s.resumedLeading = make(map[string]bool, len(hello.LastSeenSeq))
				for name := range hello.LastSeenSeq {
					s.resumedLeading[name] = true
				}
				s.trackMu.Unlock()
			}
			for name, r := range receivers {
				s.AttachTrack(name, r)
			}
			resumed = true
		}
	}
	if !resumed {
		// Fresh session: allocate ID and attach the server's current
		// track set.
		if s.OnAllocateID != nil {
			s.SessionID = s.OnAllocateID()
		}
		if s.OnFreshSession != nil {
			for name, r := range s.OnFreshSession() {
				priority := uint8(4)
				if s.OnLookupPriority != nil {
					priority = s.OnLookupPriority(name)
				}
				var trackID uint8
				if s.OnLookupTrackID != nil {
					trackID = s.OnLookupTrackID(name)
				}
				var kind track.Kind
				if s.OnLookupKind != nil {
					kind = s.OnLookupKind(name)
				}
				s.AttachTrackWithKind(name, kind, r, priority, trackID)
			}
		}
	}

	// Echo / advertise the session ID in the SDP response so the
	// client can remember it for resume on reconnect.
	sdpVal := s.cfg.SDP
	sdpVal.SessionID = s.SessionID
	// Intersect declared client features with server-supported set
	// and echo in SDP.Features. Both peers MUST treat features outside
	// the intersection as unsupported.
	sdpVal.Features = intersectFeatures(hello.Features, s.cfg.SupportedFeatures)
	sdpVal.NegotiatedVersion = s.NegotiatedVersion
	s.NegotiatedFeatures = sdpVal.Features
	sdp, err := sdpVal.Marshal()
	if err != nil {
		return fmt.Errorf("marshal sdp: %w", err)
	}
	if err := s.writer.Write(wire.TypeSDP, sdp); err != nil {
		return fmt.Errorf("write sdp: %w", err)
	}
	return nil
}

// featureKindStats is the negotiated-feature token for periodic
// per-Kind subscriber observability (publish wall-clock + KindStats).
const featureKindStats = "kind-stats"

// hasNegotiatedFeature reports whether feat is in the post-handshake
// negotiated feature set.
func (s *Session) hasNegotiatedFeature(feat string) bool {
	for _, f := range s.NegotiatedFeatures {
		if f == feat {
			return true
		}
	}
	return false
}

// intersectFeatures returns the elements present in both slices.
// O(n×m) is fine — feature lists are tiny (single digits).
func intersectFeatures(a, b []string) []string {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	bset := make(map[string]struct{}, len(b))
	for _, x := range b {
		bset[x] = struct{}{}
	}
	out := make([]string, 0, len(a))
	for _, x := range a {
		if _, ok := bset[x]; ok {
			out = append(out, x)
		}
	}
	return out
}

func (s *Session) controlReader(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		t, payload, err := wire.ReadControlFrame(s.ctl)
		if err != nil {
			return err
		}
		// DELIBERATELY do NOT update lastFeedNano on control activity;
		// see package doc.
		switch t {
		case wire.TypePing:
			// Rate-limit: drop PINGs that arrive within minPingInterval
			// of the last PONG we sent. The idle watchdog keys on feed
			// progress, so suppressing a flood of PINGs doesn't risk
			// false-positive idle closes.
			now := time.Now().UnixNano()
			last := s.lastPingNano.Load()
			if last != 0 && now-last < int64(minPingInterval) {
				continue
			}
			s.lastPingNano.Store(now)
			if err := s.writer.Write(wire.TypePong, payload); err != nil {
				return err
			}
		case wire.TypeClose:
			return nil
		case wire.TypeError:
			// New (v1.0.2+) peers send JSON ErrorPayload{code, reason};
			// legacy peers send plain bytes. wire.UnmarshalError handles
			// both, surfacing legacy bytes as Reason. Format the result
			// readably instead of dumping raw JSON into the error string.
			ep, _ := wire.UnmarshalError(payload)
			if ep.Code != "" && ep.Reason != "" {
				return fmt.Errorf("peer error: %s: %s", ep.Code, ep.Reason)
			}
			if ep.Code != "" {
				return fmt.Errorf("peer error: %s", ep.Code)
			}
			return fmt.Errorf("peer error: %s", ep.Reason)
		case wire.TypeData:
			s.dc.Deliver(payload)
		case wire.TypeAnnounce:
			a, err := wire.UnmarshalAnnounce(payload)
			if err != nil {
				s.protocolError(t, err)
				continue
			}
			s.handleAnnounce(a)
		case wire.TypeUnannounce:
			u, err := wire.UnmarshalUnannounce(payload)
			if err != nil {
				s.protocolError(t, err)
				continue
			}
			s.handleUnannounce(u)
		case wire.TypeKindStats:
			if s.cfg.OnKindStats != nil {
				ks, err := wire.UnmarshalKindStats(payload)
				if err != nil {
					s.protocolError(t, err)
					continue
				}
				s.cfg.OnKindStats(s.SessionID, ks)
			}
		case wire.TypeBackpressure:
			bp, err := wire.UnmarshalBackpressure(payload)
			if err != nil {
				s.protocolError(t, err)
				continue
			}
			if bp.NeedsKeyframe && s.cfg.OnKeyframeRequest != nil {
				// Dispatch keyframe-request to its dedicated hook
				// first; subscribers signal NeedsKeyframe=true with
				// Level=100, which would also trigger backpressure
				// adaptation. Both callbacks fire so apps that wire
				// only one of them still see the signal.
				s.cfg.OnKeyframeRequest(s.SessionID, bp.TrackName)
			}
			if s.cfg.OnBackpressure != nil {
				s.cfg.OnBackpressure(s.SessionID, bp.TrackName, bp.Level)
			}
		default:
			// Defensive: ignore unknown types so a future protocol
			// extension on the peer doesn't break older servers.
		}
	}
}

// handleAnnounce records that the subscriber intends to publish a
// track on this session. The inbound track entry is created (or
// updated) so subsequent uni-stream AUs route correctly even if they
// arrive before the demuxer has seen the announce.
//
// When Config.OnAnnounce is set, it's invoked before the track is
// marked authorized. Rejected announces emit a TypeError carrying a
// wire.ErrorPayload{Code: "track_unauthorized"} and leave the track
// unauthorized. Subsequent uni streams for that track are dropped
// at stream-header time by drainInboundStream.
func (s *Session) handleAnnounce(a wire.Announce) {
	if s.cfg.OnAnnounce != nil {
		if err := s.cfg.OnAnnounce(s.Tenant, s.SessionID, a.Name, a.Kind); err != nil {
			payload, mErr := wire.ErrorPayload{
				Code:   "track_unauthorized",
				Reason: err.Error(),
			}.Marshal()
			if mErr == nil {
				_ = s.writer.Write(wire.TypeError, payload)
			}
			// Record the announcement but leave it unauthorized so a
			// retry would land in the same rejected state instead of
			// silently accumulating attempts.
			t, ok := s.inboundTrackChanWire(a.Name)
			if !ok {
				return
			}
			s.inboundMu.Lock()
			t.kind = a.Kind
			t.codec = a.Codec
			t.announced = true
			t.authorized = false
			s.inboundMu.Unlock()
			return
		}
	}
	t, ok := s.inboundTrackChanWire(a.Name)
	if !ok {
		// Per-session inbound track cap reached; reject so the peer
		// learns instead of publishing into the void.
		payload, mErr := wire.ErrorPayload{
			Code:   "too_many_tracks",
			Reason: fmt.Sprintf("inbound track cap (%d) reached", maxInboundTracks),
		}.Marshal()
		if mErr == nil {
			_ = s.writer.Write(wire.TypeError, payload)
		}
		return
	}
	s.inboundMu.Lock()
	t.kind = a.Kind
	t.codec = a.Codec
	t.announced = true
	t.authorized = true
	s.inboundMu.Unlock()
}

// handleUnannounce signals the inbound track's drain goroutines to
// exit by closing t.done. Subsequent stream-headers naming this track
// recreate the entry with a fresh done channel. We deliberately do NOT
// close t.ch — drainInboundStream may be mid-send on it from another
// goroutine, and "send on closed channel" panics. The done channel is
// closed under inboundMu so the one-shot close is race-free.
func (s *Session) handleUnannounce(u wire.Unannounce) {
	s.inboundMu.Lock()
	t, ok := s.inboundTracks[u.Name]
	if !ok {
		s.inboundMu.Unlock()
		return
	}
	delete(s.inboundTracks, u.Name)
	close(t.done)
	s.inboundMu.Unlock()
}

// uniAcceptLoop accepts subscriber-originated uni streams and drains
// each one through the per-track inbound queue. Runs as a goroutine
// when SetUniReceiver was called before Run.
func (s *Session) uniAcceptLoop(ctx context.Context) {
	if s.uniRecv == nil {
		return
	}
	for {
		stream, err := s.uniRecv.AcceptUniStream(ctx)
		if err != nil {
			return
		}
		go s.drainInboundStream(ctx, stream)
	}
}

func (s *Session) drainInboundStream(ctx context.Context, stream UniStream) {
	// Peek the stream header to learn which track this stream carries.
	// PeekStreamHeader returns trackName="" for legacy single-track
	// streams; for PublishBack we always require a header (the client
	// pump writes one). Default to "primary" if absent.
	//
	// The header read runs under a deadline: a peer that opens a
	// stream and never sends a complete header would otherwise pin
	// this goroutine until session teardown. Senders write the header
	// immediately on stream-open, so a 30s budget is generous.
	_ = stream.SetReadDeadline(time.Now().Add(inboundFrameTimeout))
	trackName, firstByte, err := wire.PeekStreamHeader(stream)
	_ = stream.SetReadDeadline(time.Time{})
	if err != nil {
		return
	}
	var src io.Reader = stream
	if trackName == "" {
		trackName = "primary"
		src = &peekedReader{first: firstByte, rest: stream}
	}
	// Stream-header-time auth gate: when OnAnnounce is configured the
	// track must already be announced AND authorized, otherwise we
	// drop the entire stream without creating an inbound queue. This
	// closes the race where a uni stream for a not-yet-authorized
	// track would otherwise enqueue AUs before handleAnnounce ran.
	if s.cfg.OnAnnounce != nil {
		s.inboundMu.Lock()
		existing, ok := s.inboundTracks[trackName]
		authorized := ok && existing.announced && existing.authorized
		s.inboundMu.Unlock()
		if !authorized {
			return
		}
	}
	t, ok := s.inboundTrackChanWire(trackName)
	if !ok {
		// Inbound track cap reached and this stream names a brand-new
		// track; drop the stream rather than materialize the queue.
		return
	}
	for {
		f, err := readInboundFrame(stream, src)
		if err != nil {
			return
		}
		au := pubsub.AccessUnit{
			Bytes:         f.Payload,
			Keyframe:      f.IsKeyframe(),
			PTSMicro:      f.PTSMicro,
			Seq:           f.Seq,
			Discontinuity: (f.Flags & wire.FlagDiscontinuity) != 0,
		}
		// Inbound rate limit: drop AUs that exceed configured AU/sec
		// or bytes/sec budget. Cheap fast path when no limit set.
		if s.inboundRL != nil && !s.inboundRL.noLimits() {
			if !s.inboundRL.allow(len(au.Bytes)) {
				continue
			}
		}
		// An accepted inbound AU is session progress: inbound-only
		// sessions (PublishBack publishers with a quiet outbound side)
		// must not trip the idle watchdog while actively delivering.
		s.touchFeed()
		select {
		case t.ch <- au:
		case <-t.done:
			// Track was Unannounced while in flight; stop draining.
			return
		case <-ctx.Done():
			return
		default:
			// Receiver not draining fast enough; drop.
			if s.cfg.OnInboundDropped != nil {
				s.cfg.OnInboundDropped(trackName)
			}
		}
	}
}

// readInboundFrame reads one feed frame off an inbound stream with a
// stall guard: the FIRST byte may take arbitrarily long (gaps between
// AUs on sporadic tracks are legitimate), but once a frame has begun,
// its remainder must arrive within inboundFrameTimeout. Without this,
// a peer could pin the drain goroutine forever with a half-sent frame.
func readInboundFrame(stream UniStream, src io.Reader) (wire.FeedFrame, error) {
	var first [1]byte
	if _, err := io.ReadFull(src, first[:]); err != nil {
		return wire.FeedFrame{}, err
	}
	_ = stream.SetReadDeadline(time.Now().Add(inboundFrameTimeout))
	defer func() { _ = stream.SetReadDeadline(time.Time{}) }()
	return wire.ReadFeedFrame(&peekedReader{first: first[0], rest: src})
}

// peekedReader presents a single peeked byte followed by the rest of
// the underlying reader. Mirrors the helper in client/client.go for
// the symmetric server-side demux path.
type peekedReader struct {
	first    byte
	consumed bool
	rest     io.Reader
}

func (p *peekedReader) Read(b []byte) (int, error) {
	if !p.consumed {
		if len(b) == 0 {
			return 0, nil
		}
		b[0] = p.first
		p.consumed = true
		return 1, nil
	}
	return p.rest.Read(b)
}

func (s *Session) idleWatchdog(ctx context.Context) error {
	// Poll at IdleTimeout/4 (capped at 5s) so the actual close happens
	// within ~25% of the configured deadline. Keeps short-timeout
	// tests fast without making prod chatty.
	period := s.cfg.IdleTimeout / 4
	if period <= 0 {
		period = 5 * time.Second
	}
	if period > 5*time.Second {
		period = 5 * time.Second
	}
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-ticker.C:
			lastProgress := s.lastFeedNano.Load()
			lastAttempt := s.lastAttemptNano.Load()
			// Fire only when a write has been attempted more recently
			// than the last progress AND that stall has outlived the
			// idle budget. A session with nothing to send (no attempts
			// newer than the last success) is quiet, not dead — the
			// QUIC layer's own idle timeout covers vanished peers.
			if lastAttempt > lastProgress &&
				now.Sub(time.Unix(0, lastProgress)) > s.cfg.IdleTimeout {
				return ErrIdle
			}
		}
	}
}

func (s *Session) touchFeed() {
	s.lastFeedNano.Store(time.Now().UnixNano())
}

// touchAttempt records that a pump dequeued an AU for delivery. See
// idleWatchdog: attempts without subsequent progress are what "idle"
// means.
func (s *Session) touchAttempt() {
	s.lastAttemptNano.Store(time.Now().UnixNano())
}

// protocolError surfaces a malformed-but-well-framed control payload
// to the embedder. The frame is dropped either way.
func (s *Session) protocolError(frameType byte, err error) {
	if s.cfg.OnProtocolError != nil {
		s.cfg.OnProtocolError(s.SessionID, frameType, err)
	}
}

func (s *Session) touchFeedAndCount(n int) {
	s.bytesOut.Add(uint64(n))
	s.touchFeed()
}

// withFeedHook composes a feed.Config that calls both the
// caller-supplied hooks and the session's own progress hooks
// (successful-write for OnWriteBytes, dequeue for OnWriteAttempt).
func withFeedHook(in feed.Config, writeHook func(int), attemptHook func()) feed.Config {
	out := in
	callerWrite := in.OnWriteBytes
	out.OnWriteBytes = func(n int) {
		writeHook(n)
		if callerWrite != nil {
			callerWrite(n)
		}
	}
	callerAttempt := in.OnWriteAttempt
	out.OnWriteAttempt = func() {
		attemptHook()
		if callerAttempt != nil {
			callerAttempt()
		}
	}
	return out
}

// bestEffortError attempts to send a TypeError control frame to the
// peer with a tight write deadline. Used during handshake failures:
// we want the peer to see WHY auth failed, but we don't want a
// non-reading peer to delay our return-to-caller.
func (s *Session) bestEffortError(msg []byte) {
	_ = s.ctl.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
	_ = s.writer.Write(wire.TypeError, msg)
	_ = s.ctl.SetWriteDeadline(time.Time{})
}

// ctlWriter serializes writes on the bidi control stream. The session
// loop's PONG response, the SDP write, and the DataChannel's Send all
// race for it; without a mutex the wire framing would interleave.
type ctlWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (c *ctlWriter) Write(typ byte, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return wire.WriteControlFrame(c.w, typ, payload)
}
