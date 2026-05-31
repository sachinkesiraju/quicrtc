// Package client is the Go-side quicrtc subscriber. It mirrors the
// browser receiver: open a WebTransport session to a quicrtc server,
// send HELLO, read SDP, then accept incoming uni feed streams and
// surface decoded access units one at a time.
//
// For browser receivers, use the sibling @quicrtc/client TS package.
package client

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"

	"github.com/sachinkesiraju/quicrtc/datachannel"
	"github.com/sachinkesiraju/quicrtc/feed"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/track"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// Options control client dial behavior.
type Options struct {
	// CertHashB64URL pins the server's TLS cert by its SHA-256 hash
	// in browser-WebTransport format. If empty and InsecureSkipVerify
	// is false, the cert is verified against the system trust store.
	CertHashB64URL string

	// InsecureSkipVerify disables cert validation entirely. Use only
	// for tests.
	InsecureSkipVerify bool

	// Slug is the auth secret to send in HELLO. Required.
	Slug string

	// HelloTimeout caps the HELLO + SDP exchange.
	HelloTimeout time.Duration

	// SessionID, when non-empty, requests a resume of an existing
	// session. The server matches against parked sessions; if a
	// match exists, the new connection picks up the prior track
	// state and replays AUs that arrived during the disconnect.
	// Empty SessionID requests a fresh session.
	SessionID string

	// LastSeenSeq, when present, scopes the resume to a per-track
	// continuation point. For each (track name → seq) entry the
	// server replays AUs with Seq > seq from its replay buffer
	// before resuming live delivery. Recovers AUs that were in
	// flight on the QUIC wire when the previous connection dropped.
	// Pass the value returned by the prior Client.LastSeenSeq()
	// here; safe to omit (older behavior, no replay-fill).
	LastSeenSeq map[string]uint32

	// Features is the list of optional protocol features the client
	// declares support for. The server intersects this with its own
	// supported set; the result is exposed via Client.NegotiatedFeatures().
	// nil = use the defaults (every feature this client build knows
	// how to handle).
	Features []string

	// DataInboundBuffer overrides the inbound DataChannel queue depth.
	// Default 64. Bursty senders that push faster than the consumer
	// can drain (e.g., 1000+ msgs/sec batches) need a larger buffer
	// to avoid silent drops at the receive demuxer.
	DataInboundBuffer int
}

// DefaultClientFeatures lists every optional protocol feature this
// client build supports. Override Options.Features to disable
// specific features (e.g., for testing legacy server compatibility).
func DefaultClientFeatures() []string {
	return []string{
		"resume",
		"backpressure",
		"publishback",
		"stream-header",
	}
}

// Client is one connected subscriber.
type Client struct {
	wt  *webtransport.Session
	ctl *webtransport.Stream
	rt  *http3.Transport

	// ctlWriteMu serializes all writes on the bidi control stream.
	// HELLO, DataChannel.Send, and TypeAnnounce/TypeResume all share
	// this writer; without the mutex their wire framing would
	// interleave on a slow socket.
	ctlWriteMu sync.Mutex

	closeOnce sync.Once
	closed    chan struct{}

	// sessionCtx is the lifetime context for session-long goroutines
	// (controlReader, feedAcceptor). Decoupled from the caller's
	// dial ctx so a `context.WithTimeout(ctx, 5*time.Second); defer
	// cancel(); Dial(ctx, ...)` pattern doesn't tear down the wire
	// reader the moment the dial timeout elapses. Cancelled by
	// Close().
	sessionCtx    context.Context
	sessionCancel context.CancelFunc

	sdp wire.SDP
	dc  *datachannel.Channel

	// Per-track receive queues. The "primary" track is created
	// implicitly to preserve the single-track Recv() API. Streams
	// that arrive without a TypeStreamHeader frame route to
	// "primary" too.
	feedMu       sync.Mutex
	tracks       map[string]chan pubsub.AccessUnit
	remoteTracks map[string]wire.Announce // tracks the server has Announced
	feedErr      chan error

	// Outbound (PublishBack) state. One entry per track this client
	// is publishing to the server. The broadcaster has a single
	// receiver — the feed pump — and is fed by the Sender returned
	// from Publish.
	pubMu      sync.Mutex
	pubTracks  map[string]*pubTrack
	pubCancels []context.CancelFunc // pump goroutine cancellers

	// Resume state. requestedSessionID is what the caller asked us
	// to resume (set from Options.SessionID). assignedSessionID is
	// what the server gave us back in the SDP response — call
	// SessionID() to read it.
	requestedSessionID  string
	assignedSessionID   string
	requestedLastSeen   map[string]uint32 // sent in HELLO.LastSeenSeq on resume

	// seqMu guards lastSeenSeq. Each successful RecvOn updates the
	// per-track high-water seq so the caller can read LastSeenSeq()
	// before closing this client and use it to populate
	// Options.LastSeenSeq when dialing the resumed connection.
	seqMu       sync.Mutex
	lastSeenSeq map[string]uint32

	// Capability negotiation state. declaredFeatures is what we
	// advertised in HELLO (Options.Features); negotiatedFeatures is
	// the server's intersection with its supported set, captured
	// from SDP.Features.
	declaredFeatures   []string
	negotiatedFeatures []string
}

// pubTrack is one outbound (subscriber→server) track.
type pubTrack struct {
	name string
	bc   *pubsub.Broadcaster
}

// Dial opens a quicrtc subscriber session against rawURL. The URL
// fragment may carry slug=...&hash=... — if you parsed it already,
// pass via opts; or pass a URL with #slug=...&hash=... and we'll
// honor it.
func Dial(ctx context.Context, rawURL string, opts Options) (*Client, error) {
	u, slug, hash, err := parseURL(rawURL, opts.Slug, opts.CertHashB64URL)
	if err != nil {
		return nil, err
	}
	tlsCfg := &tls.Config{NextProtos: []string{"h3"}}
	if opts.InsecureSkipVerify {
		tlsCfg.InsecureSkipVerify = true
	} else if hash != "" {
		want, err := base64.RawURLEncoding.DecodeString(hash)
		if err != nil || len(want) != 32 {
			return nil, fmt.Errorf("bad cert hash: %v", err)
		}
		tlsCfg.InsecureSkipVerify = true
		// On full handshakes only VerifyPeerCertificate runs; on
		// resumed handshakes only VerifyConnection runs. Both must
		// check the pin so a resumed session can't bypass it.
		checkPin := func(certs [][]byte) error {
			for _, c := range certs {
				h := sha256.Sum256(c)
				if subtleEqual(h[:], want) {
					return nil
				}
			}
			return errors.New("cert hash mismatch")
		}
		tlsCfg.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			return checkPin(rawCerts)
		}
		tlsCfg.VerifyConnection = func(cs tls.ConnectionState) error {
			raw := make([][]byte, len(cs.PeerCertificates))
			for i, c := range cs.PeerCertificates {
				raw[i] = c.Raw
			}
			return checkPin(raw)
		}
	}
	// Receive-window sizing mirrors the server's config in
	// server/server.go so high-BDP paths (e.g., 200ms RTT × 100 Mbps,
	// BDP ≈ 2.5 MiB) aren't capped by quic-go's conservative 512 KiB
	// defaults. Worst-case receive-buffer footprint is bounded at
	// MaxConnectionReceiveWindow × (open connections); a single client
	// holds one connection. Relay deployments fan in multiple clients
	// — see relay/relay.go — and should size their host accordingly.
	quicCfg := &quic.Config{
		InitialStreamReceiveWindow:       4 << 20,  // 4 MiB
		MaxStreamReceiveWindow:           16 << 20, // 16 MiB
		InitialConnectionReceiveWindow:   8 << 20,  // 8 MiB
		MaxConnectionReceiveWindow:       32 << 20, // 32 MiB
		EnableDatagrams:                  true,
		EnableStreamResetPartialDelivery: true,
	}
	rt := &http3.Transport{
		TLSClientConfig: tlsCfg,
		QUICConfig:      quicCfg,
	}
	dialer := &webtransport.Dialer{TLSClientConfig: tlsCfg, QUICConfig: quicCfg}

	timeout := opts.HelloTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	dialCtx, dialCancel := context.WithTimeout(ctx, timeout)
	defer dialCancel()

	_, sess, err := dialer.Dial(dialCtx, u.String(), http.Header{})
	if err != nil {
		_ = rt.Close()
		return nil, fmt.Errorf("dial wt: %w", err)
	}

	ctl, err := sess.OpenStreamSync(dialCtx)
	if err != nil {
		sess.CloseWithError(0, "")
		_ = rt.Close()
		return nil, fmt.Errorf("open ctl: %w", err)
	}

	sessionCtx, sessionCancel := context.WithCancel(context.Background())
	c := &Client{
		wt:                 sess,
		ctl:                ctl,
		rt:                 rt,
		closed:             make(chan struct{}),
		sessionCtx:         sessionCtx,
		sessionCancel:      sessionCancel,
		tracks:             map[string]chan pubsub.AccessUnit{"primary": make(chan pubsub.AccessUnit, 16)},
		feedErr:            make(chan error, 1),
		pubTracks:          make(map[string]*pubTrack),
		requestedSessionID: opts.SessionID,
		requestedLastSeen:  opts.LastSeenSeq,
		declaredFeatures:   opts.Features,
		lastSeenSeq:        make(map[string]uint32),
	}

	send := func(payload []byte) error {
		return c.writeCtl(wire.TypeData, payload)
	}
	dcBuf := opts.DataInboundBuffer
	if dcBuf <= 0 {
		dcBuf = 64
	}
	c.dc = datachannel.New(send, dcBuf)

	if err := c.handshake(slug); err != nil {
		c.Close()
		return nil, err
	}
	go c.controlReader()
	// feedAcceptor / bidiAcceptor run for the session's lifetime, NOT
	// the caller's dial ctx. Otherwise `context.WithTimeout(ctx, 5s);
	// defer cancel()` on the caller would tear down the wire readers
	// the instant the dial timeout fired, silently losing all
	// subsequent inbound AUs.
	go c.feedAcceptor(c.sessionCtx)
	go c.bidiAcceptor(c.sessionCtx)
	return c, nil
}

// SDP returns the codec advertisement received from the server.
func (c *Client) SDP() wire.SDP { return c.sdp }

// DataChannel returns this client's datachannel handle.
func (c *Client) DataChannel() *datachannel.Channel { return c.dc }

// Recv blocks until the next access unit arrives on the implicit
// "primary" track. For multi-track receivers, use RecvOn(name).
// Returned AU.Bytes is owned by the caller.
func (c *Client) Recv(ctx context.Context) (pubsub.AccessUnit, error) {
	return c.RecvOn(ctx, "primary")
}

// RecvOn blocks until the next AccessUnit on the named track arrives.
// The first time a track name is seen — either via this call or via
// an inbound stream-header frame — its receive queue is created.
func (c *Client) RecvOn(ctx context.Context, trackName string) (pubsub.AccessUnit, error) {
	ch := c.trackChan(trackName)
	select {
	case au, ok := <-ch:
		if !ok {
			return pubsub.AccessUnit{}, c.endError()
		}
		c.observeSeq(trackName, au.Seq)
		return au, nil
	case err := <-c.feedErr:
		return pubsub.AccessUnit{}, err
	case <-ctx.Done():
		return pubsub.AccessUnit{}, ctx.Err()
	case <-c.closed:
		return pubsub.AccessUnit{}, c.endError()
	}
}

// observeSeq records the highest Seq seen on a track so a future
// reconnect can replay AUs that didn't make it past the wire on the
// previous connection.
func (c *Client) observeSeq(trackName string, seq uint32) {
	c.seqMu.Lock()
	defer c.seqMu.Unlock()
	if prev, ok := c.lastSeenSeq[trackName]; !ok || seq > prev {
		c.lastSeenSeq[trackName] = seq
	}
}

// trackChan returns (creating if needed) the receive queue for a
// named track. Used by RecvOn callers and by the feed drainer when
// a stream-header frame names a previously-unseen track.
func (c *Client) trackChan(name string) chan pubsub.AccessUnit {
	c.feedMu.Lock()
	defer c.feedMu.Unlock()
	if ch, ok := c.tracks[name]; ok {
		return ch
	}
	ch := make(chan pubsub.AccessUnit, 16)
	c.tracks[name] = ch
	return ch
}

// SendBackpressure emits a TypeBackpressure control frame so the
// publisher can adapt rate. Level is 0..100 (drained..full).
// trackName identifies the affected track (empty = session-level).
// Cheap; rate-limit at the application layer if needed.
func (c *Client) SendBackpressure(trackName string, level uint8) error {
	body, err := wire.Backpressure{TrackName: trackName, Level: level}.Marshal()
	if err != nil {
		return err
	}
	return c.writeCtl(wire.TypeBackpressure, body)
}

// SendDatagram sends one unreliable QUIC datagram to the peer.
// Used by the kind-aware telemetry path (DeliveryDatagramOrStream)
// and exposed as a public API for benchmarks and apps that want
// fire-and-forget delivery. The payload should fit within
// wire.MaxDatagramPayload (1100 bytes) after envelope overhead.
// Returns DatagramTooLargeError from the underlying QUIC stack if
// the payload exceeds the path MTU.
func (c *Client) SendDatagram(payload []byte) error {
	return c.wt.SendDatagram(payload)
}

// ReceiveDatagram blocks until one datagram arrives or ctx is done.
// Returns the raw datagram bytes — the caller is responsible for
// parsing any envelope (e.g., wire.DecodeDatagram for kind-aware
// telemetry). Lost datagrams are not retransmitted; this API
// surfaces only what actually arrived.
func (c *Client) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	return c.wt.ReceiveDatagram(ctx)
}

// RequestKeyframe asks the publisher to emit a fresh keyframe on
// trackName NOW. Used by subscribers that detect a P-frame gap (loss
// event, decoder reset, or new viewer join mid-GOP) — caps the
// visible-stutter recovery time at ~50ms (next encoded frame) instead
// of waiting up to one GOP duration for the next natural keyframe.
//
// This is the receiver-driven half of stream-per-GOP recovery: the
// publisher's pump already cancels-and-reopens on its own write
// failures; RequestKeyframe lets the subscriber trigger the same
// recovery when the loss is invisible from the publisher's side
// (e.g., quic-go retransmitted the bytes successfully but the
// receiver's app couldn't decode them).
//
// Wire-piggybacks on TypeBackpressure with NeedsKeyframe=true and
// Level=100. v1.0 receivers ignore the boolean (it's optional JSON);
// v1.1+ receivers act on it by triggering OnKeyframeRequest in the
// publisher's session config.
func (c *Client) RequestKeyframe(trackName string) error {
	body, err := wire.Backpressure{
		TrackName:     trackName,
		Level:         100,
		NeedsKeyframe: true,
	}.Marshal()
	if err != nil {
		return err
	}
	return c.writeCtl(wire.TypeBackpressure, body)
}

func (c *Client) endError() error {
	select {
	case err := <-c.feedErr:
		return err
	default:
		return errors.New("quicrtc/client: closed")
	}
}

// writeCtl sends one framed control-stream message. Serialized with
// ctlWriteMu so concurrent callers (DataChannel.Send, Publish's
// Announce, Close, etc.) don't interleave their wire bytes.
func (c *Client) writeCtl(typ byte, payload []byte) error {
	c.ctlWriteMu.Lock()
	defer c.ctlWriteMu.Unlock()
	return wire.WriteControlFrame(c.ctl, typ, payload)
}

// Publish creates a subscriber-side outbound track (PublishBack). The
// caller's Sender feeds AUs that the client streams to the server on
// dedicated uni QUIC streams, demuxed server-side via the same track-
// header convention used in the publisher direction. An Announce
// frame on the bidi control stream tells the server to expect the
// inbound track.
//
// The returned Sender is safe for concurrent use. Closing it
// unannounces the track and stops the per-track pump.
func (c *Client) Publish(ctx context.Context, lt track.LocalTrack) (Sender, error) {
	select {
	case <-c.closed:
		return nil, errors.New("quicrtc/client: closed")
	default:
	}
	name := lt.Name
	if name == "" {
		name = "primary"
	}

	c.pubMu.Lock()
	if existing, ok := c.pubTracks[name]; ok {
		c.pubMu.Unlock()
		return &clientSender{c: c, name: name, bc: existing.bc}, nil
	}
	bc := pubsub.NewBroadcaster(0) // default per-subscriber buffer
	pt := &pubTrack{name: name, bc: bc}
	c.pubTracks[name] = pt
	c.pubMu.Unlock()

	// Announce the track on the control stream so the server knows
	// which inbound uni streams to expect.
	a := wire.Announce{Name: name, Kind: string(lt.Kind), Codec: lt.Codec}
	body, err := a.Marshal()
	if err != nil {
		c.removePubTrack(name)
		return nil, fmt.Errorf("marshal announce: %w", err)
	}
	if err := c.writeCtl(wire.TypeAnnounce, body); err != nil {
		c.removePubTrack(name)
		return nil, fmt.Errorf("write announce: %w", err)
	}

	// Spawn a feed pump that drains the broadcaster's receiver into
	// uni streams. Same pattern as the server-side publish pipeline.
	pumpCtx, cancel := context.WithCancel(context.Background()) // #nosec G118 -- cancel stored in c.pubCancels; Close() drains the slice and invokes each.
	c.pubMu.Lock()
	c.pubCancels = append(c.pubCancels, cancel)
	c.pubMu.Unlock()
	recv := bc.Subscribe()
	cfg := feed.Config{
		TrackName: name,
		Delivery:  clientPublishDelivery(lt.Kind),
		Priority:  uint8(track.DefaultPriority(lt.Kind)),
	}
	go func() {
		p := feed.New(&clientUniOpener{wt: c.wt}, cfg)
		_ = p.Run(pumpCtx, recv)
	}()

	return &clientSender{c: c, name: name, bc: bc}, nil
}

func (c *Client) removePubTrack(name string) {
	c.pubMu.Lock()
	defer c.pubMu.Unlock()
	delete(c.pubTracks, name)
}

// clientPublishDelivery picks the feed delivery class for a track the
// client publishes (PublishBack). The client publish path drives a
// single uni-stream opener, so video keeps stream-per-GOP and every
// other kind rides ONE persistent low-latency stream.
//
// This deliberately does not use BidiPerCall or DatagramOrStream for
// client→server tracks: a client Sender.Send enqueues and returns with
// no response plumbing on this side, so the traffic is fire-and-forget
// and a single low-latency lane is the right shape. Crucially it avoids
// the GOP pump's open-a-new-uni-stream-per-keyframe behavior — for a
// fire-every-AU-as-keyframe workload (e.g. 100 computer-use actions/sec,
// each Keyframe=true) that churned ~100 stream opens/sec and could
// exhaust the peer's uni-stream credit, producing the multi-second tail
// stalls seen on the WAN computer-use action path.
func clientPublishDelivery(k track.Kind) track.DeliveryClass {
	if k == track.KindVideo || k == "" {
		return track.DeliveryStreamGOP
	}
	return track.DeliveryStreamLowLatency
}

// Sender is the publisher-side handle for one outbound track.
type Sender interface {
	Send(ctx context.Context, au pubsub.AccessUnit) error
	Close() error
}

type clientSender struct {
	c    *Client
	name string
	bc   *pubsub.Broadcaster
}

func (s *clientSender) Send(_ context.Context, au pubsub.AccessUnit) error {
	select {
	case <-s.c.closed:
		return errors.New("quicrtc/client: closed")
	default:
	}
	s.bc.Publish(au)
	return nil
}

func (s *clientSender) Close() error {
	// Send Unannounce so the server tears down its inbound state for
	// this track. Track lifecycle (Phase 2) will unwire the per-track
	// state machine fully; for now we just signal the server.
	body, err := (wire.Unannounce{Name: s.name}).Marshal()
	if err == nil {
		_ = s.c.writeCtl(wire.TypeUnannounce, body)
	}
	s.c.removePubTrack(s.name)
	return nil
}

// clientUniOpener wraps webtransport.Session as a feed.StreamOpener so
// the publisher-side feed.Pump can run unchanged.
type clientUniOpener struct{ wt *webtransport.Session }

func (o *clientUniOpener) OpenSendStream(ctx context.Context) (feed.SendStream, error) {
	s, err := o.wt.OpenUniStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return &clientUniStream{s: s}, nil
}

type clientUniStream struct{ s *webtransport.SendStream }

func (u *clientUniStream) Write(p []byte) (int, error)        { return u.s.Write(p) }
func (u *clientUniStream) Close() error                       { return u.s.Close() }
func (u *clientUniStream) SetWriteDeadline(t time.Time) error { return u.s.SetWriteDeadline(t) }
func (u *clientUniStream) CancelWrite(code uint32) {
	u.s.CancelWrite(webtransport.StreamErrorCode(code))
}

// Close ends the session.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
		// Cancel the session-lifetime ctx so feedAcceptor /
		// controlReader unblock and exit cleanly.
		if c.sessionCancel != nil {
			c.sessionCancel()
		}
		// Stop all PublishBack pumps so they don't try writing to a
		// closing webtransport session.
		c.pubMu.Lock()
		for _, cancel := range c.pubCancels {
			cancel()
		}
		c.pubCancels = nil
		c.pubMu.Unlock()
		if c.dc != nil {
			c.dc.Close()
		}
		_ = c.writeCtl(wire.TypeClose, nil)
		c.wt.CloseWithError(0, "bye")
		_ = c.rt.Close()
	})
	return nil
}

func (c *Client) handshake(slug string) error {
	features := c.declaredFeatures
	if features == nil {
		features = DefaultClientFeatures()
	}
	hello := wire.Hello{
		Role:        "recv",
		Slug:        slug,
		Version:     wire.CurrentVersion,
		SessionID:   c.requestedSessionID,
		Features:    features,
		LastSeenSeq: c.requestedLastSeen,
	}
	hb, err := hello.Marshal()
	if err != nil {
		return err
	}
	if err := c.writeCtl(wire.TypeHello, hb); err != nil {
		return fmt.Errorf("write hello: %w", err)
	}
	t, payload, err := wire.ReadControlFrame(c.ctl)
	if err != nil {
		return fmt.Errorf("read sdp: %w", err)
	}
	if t == wire.TypeError {
		return fmt.Errorf("server rejected: %s", string(payload))
	}
	if t != wire.TypeSDP {
		return fmt.Errorf("expected SDP, got %#x", t)
	}
	sdp, err := wire.UnmarshalSDP(payload)
	if err != nil {
		return fmt.Errorf("bad sdp: %w", err)
	}
	c.sdp = sdp
	c.assignedSessionID = sdp.SessionID
	c.negotiatedFeatures = sdp.Features
	return nil
}

// SessionID returns the session identifier the server allocated for
// this connection. Pass this value back as Options.SessionID on a
// reconnect to resume the session.
func (c *Client) SessionID() string { return c.assignedSessionID }

// LastSeenSeq returns a snapshot of the highest sequence number this
// client has delivered on each track. Pass to Options.LastSeenSeq on
// a reconnect with the same SessionID so the server replays AUs that
// were in flight on the previous QUIC wire when it dropped.
func (c *Client) LastSeenSeq() map[string]uint32 {
	c.seqMu.Lock()
	defer c.seqMu.Unlock()
	if len(c.lastSeenSeq) == 0 {
		return nil
	}
	out := make(map[string]uint32, len(c.lastSeenSeq))
	for k, v := range c.lastSeenSeq {
		out[k] = v
	}
	return out
}

// NegotiatedFeatures returns the optional protocol features both
// peers agreed to use, captured from the server's SDP response. May
// be empty (no overlap with declared client features) or shorter
// than what was declared. Application code should branch on this
// value rather than assuming a feature is supported.
func (c *Client) NegotiatedFeatures() []string {
	out := make([]string, len(c.negotiatedFeatures))
	copy(out, c.negotiatedFeatures)
	return out
}

func (c *Client) controlReader() {
	for {
		t, payload, err := wire.ReadControlFrame(c.ctl)
		if err != nil {
			return
		}
		switch t {
		case wire.TypePing:
			_ = c.writeCtl(wire.TypePong, payload)
		case wire.TypePong:
			// drop
		case wire.TypeData:
			c.dc.Deliver(payload)
		case wire.TypeAnnounce:
			a, err := wire.UnmarshalAnnounce(payload)
			if err == nil {
				c.handleRemoteAnnounce(a)
			}
		case wire.TypeUnannounce:
			u, err := wire.UnmarshalUnannounce(payload)
			if err == nil {
				c.handleRemoteUnannounce(u)
			}
		case wire.TypeError:
			c.fail(fmt.Errorf("server error: %s", string(payload)))
			return
		case wire.TypeClose:
			c.fail(nil)
			return
		}
	}
}

// handleRemoteAnnounce eagerly creates the recv queue for a server-
// side track so RecvOn callers don't have to race with the first
// inbound stream-header. Updates RemoteTracks().
func (c *Client) handleRemoteAnnounce(a wire.Announce) {
	_ = c.trackChan(a.Name)
	c.feedMu.Lock()
	if c.remoteTracks == nil {
		c.remoteTracks = make(map[string]wire.Announce)
	}
	c.remoteTracks[a.Name] = a
	c.feedMu.Unlock()
}

// handleRemoteUnannounce removes the named track from the client's
// active set. The recv channel is NOT closed: any drainFeed goroutine
// still running for an in-flight stream of this track would race on
// send-to-closed-channel. The associated QUIC stream is cancelled by
// the server, so drainFeed will exit on its next ReadFeedFrame error.
//
// RecvOn callers blocked on the old channel will see no further AUs
// and should rely on context cancellation to unblock; a future Phase
// 4 enhancement can deliver an explicit io.EOF via a sentinel value.
func (c *Client) handleRemoteUnannounce(u wire.Unannounce) {
	c.feedMu.Lock()
	delete(c.tracks, u.Name)
	if c.remoteTracks != nil {
		delete(c.remoteTracks, u.Name)
	}
	c.feedMu.Unlock()
}

// RemoteTracks returns the names of tracks the server has Announced
// on this session. Useful for subscribers that want to surface
// available tracks before the first AU arrives.
func (c *Client) RemoteTracks() []string {
	c.feedMu.Lock()
	defer c.feedMu.Unlock()
	out := make([]string, 0, len(c.remoteTracks))
	for name := range c.remoteTracks {
		out = append(out, name)
	}
	return out
}

func (c *Client) feedAcceptor(ctx context.Context) {
	for {
		select {
		case <-c.closed:
			return
		default:
		}
		stream, err := c.wt.AcceptUniStream(ctx)
		if err != nil {
			c.fail(err)
			return
		}
		go c.drainFeed(stream)
	}
}

// bidiAcceptor accepts bidi streams the publisher opens for the
// BidiPerCall delivery class (KindToolCalls). Each accepted stream
// carries a TypeStreamHeader + one feed frame; we queue the AU on
// the per-track channel so Client.RecvOn returns BidiPerCall AUs
// the same way it returns uni-stream AUs. We don't write a response
// today — wire shape supports it; API surface for that lands when
// a consumer needs it.
func (c *Client) bidiAcceptor(ctx context.Context) {
	for {
		select {
		case <-c.closed:
			return
		default:
		}
		stream, err := c.wt.AcceptStream(ctx)
		if err != nil {
			// feedAcceptor surfaces the transport error; we exit quietly.
			return
		}
		go c.drainBidi(stream)
	}
}

func (c *Client) drainBidi(stream *webtransport.Stream) {
	// FIN our send half so the server's io.ReadAll returns. Without
	// this, the server blocks per call until BidiCallTimeout (30s)
	// and leaks a goroutine. Stream.Close() FINs send only — same
	// convention as wtBidiStream.CloseWrite in server/server.go.
	defer func() { _ = stream.Close() }()

	// Same shape as drainFeed. BidiPerCall sends one feed-frame per
	// stream then FINs; the loop handles that and any future stream
	// that carries multiple frames.
	trackName, firstByte, err := wire.PeekStreamHeader(stream)
	if err != nil {
		return
	}
	var src io.Reader = stream
	if trackName == "" {
		trackName = "primary"
		src = &peekedReader{first: firstByte, rest: stream}
	}
	ch := c.trackChan(trackName)
	for {
		f, err := wire.ReadFeedFrame(src)
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
		select {
		case ch <- au:
		case <-c.closed:
			return
		}
	}
}

func (c *Client) drainFeed(stream *webtransport.ReceiveStream) {
	// Peek the first byte. If it's TypeStreamHeader, the stream
	// belongs to a named track; otherwise the stream is legacy
	// single-track and routes to "primary," with the first byte
	// re-prepended via a small adapter so ReadFeedFrame still works.
	trackName, firstByte, err := wire.PeekStreamHeader(stream)
	if err != nil {
		return
	}
	var src io.Reader = stream
	if trackName == "" {
		trackName = "primary"
		src = &peekedReader{first: firstByte, rest: stream}
	}
	ch := c.trackChan(trackName)
	for {
		f, err := wire.ReadFeedFrame(src)
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
		select {
		case ch <- au:
		case <-c.closed:
			return
		}
	}
}

// peekedReader presents a single peeked byte followed by the rest of
// the underlying reader. Used when PeekStreamHeader determined the
// stream is legacy (no header) and we need to re-include the first
// byte before parsing it as a feed frame.
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
		// Return only the peeked byte; io.ReadFull calls us again for
		// the rest.
		return 1, nil
	}
	return p.rest.Read(b)
}

func (c *Client) fail(err error) {
	select {
	case c.feedErr <- err:
	default:
	}
	c.Close()
}

func parseURL(raw, optsSlug, optsHash string) (*url.URL, string, string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, "", "", err
	}
	slug, hash := optsSlug, optsHash
	if u.Fragment != "" {
		for _, kv := range strings.Split(u.Fragment, "&") {
			k, v, ok := strings.Cut(kv, "=")
			if !ok {
				continue
			}
			switch k {
			case "slug":
				if slug == "" {
					slug = v
				}
			case "hash":
				if hash == "" {
					hash = v
				}
			}
		}
		u.Fragment = ""
	}
	if slug == "" {
		return nil, "", "", errors.New("quicrtc/client: missing slug")
	}
	return u, slug, hash, nil
}

func subtleEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := range a {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}
