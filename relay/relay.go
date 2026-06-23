// Package relay is the quicrtc native-wire relay: a server.Server that
// also subscribes to one or more upstream publishers and fans their
// AccessUnits out to local downstream subscribers.
//
// Architecture:
//
//	[Publisher]                   ┌─────[Subscriber A]
//	    │                         │
//	    └─ native ─→ [Relay] ─────┼─────[Subscriber B]
//	                              │
//	                              └─────[Subscriber C]
//
// The relay embeds a normal server.Server on the downstream side;
// subscribers connect to it exactly as they would to a publishing
// server. On the upstream side it opens a client.Client per configured
// upstream URL and pumps every received AccessUnit on every announced
// track into the local server's per-track Broadcaster.
//
// One wire format end-to-end: the same 17-byte feed header travels
// from origin publisher through the relay to subscribers, unparsed by
// the relay (the relay reads through quicrtc's primitives but doesn't
// transcode). For relay-vs-MoQT measurement context, see
// quicrtc-experiments/relay_bench/v2/RELAY_MEASUREMENT_REPORT.md.
package relay

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sachinkesiraju/quicrtc/client"
	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/server"
)

// Config configures a Relay. The downstream Server is configured
// directly via Server; upstream subscriptions are listed in Upstreams.
type Config struct {
	// Server is the downstream server configuration. The relay creates
	// the Server with this config and exposes it via Relay.Server().
	// The SDP advertised to subscribers comes from Server.SDP — the
	// relay does not auto-discover SDP from upstream (caller's
	// responsibility to match the upstream's codec/dimensions).
	Server server.Config

	// Upstreams is the list of upstream publisher share-link URLs to
	// subscribe to. Each is dialed independently. The relay forwards
	// AccessUnits from every announced track on every upstream into
	// the local downstream server's per-track broadcaster.
	//
	// Must have at least one upstream. Multi-upstream support: each
	// upstream's tracks are merged into the same downstream namespace
	// (one local broadcaster per track name across all upstreams).
	// If two upstreams announce the same track name, AUs from both
	// are interleaved into one downstream stream — typically you do
	// not want this; arrange upstreams to use disjoint track names.
	Upstreams []string

	// ClientOptions is passed to each upstream client.Dial. Use this
	// to set HelloTimeout, Features, etc. The Slug and CertHashB64URL
	// are parsed from each upstream URL's fragment by client.Dial;
	// any values set here are overridden per-URL.
	ClientOptions client.Options

	// ReconnectInterval is how long to wait between upstream
	// reconnection attempts after a transient disconnect. Defaults
	// to 1 second. Resume is used automatically when available
	// (quicrtc's SessionID resumption); on persistent failure the
	// pump exits and ListenAndServe returns the error.
	ReconnectInterval time.Duration

	// MaxReconnectAttempts caps how many times an upstream pump tries
	// to recover before giving up. Defaults to 5; set to <0 for
	// unlimited retry. A nonrecoverable upstream is fatal to the
	// whole relay (ListenAndServe returns) — if you need partial
	// degradation, run separate relays.
	MaxReconnectAttempts int

	// MaxTracksPerUpstream caps the number of forwarding pumps the
	// relay will spawn for a single upstream session. A buggy or
	// hostile upstream that announces a runaway number of distinct
	// track names would otherwise leak goroutines + map entries.
	// Defaults to 256; set to <0 for no cap (not recommended).
	MaxTracksPerUpstream int
}

// Relay is one upstream-aggregating relay node.
//
// Construct with New; start with ListenAndServe (blocks until ctx is
// done or any upstream fails permanently). Subscribers connect to the
// downstream Server (Relay.Server()) using the same share link as a
// regular publishing server — the wire format is identical.
type Relay struct {
	cfg Config
	srv *server.Server

	// tracks publishers we've created on the downstream side. One
	// publisher per track name across all upstreams. trackOwner
	// records the upstream URL that first claimed each track name —
	// used to detect cross-upstream collisions (two upstreams
	// announcing the same name) which would silently interleave AUs
	// in a way subscribers can't decode.
	pubsMu     sync.Mutex
	pubs       map[string]*server.Publisher
	trackOwner map[string]string // track name -> upstream URL that owns it

	// closed once Shutdown completes; sync.Once protects the close.
	shutdownOnce sync.Once
	closed       chan struct{}
}

// New constructs a Relay. The downstream Server is created here but
// not started until ListenAndServe is called.
func New(cfg Config) (*Relay, error) {
	if len(cfg.Upstreams) == 0 {
		return nil, errors.New("relay: at least one upstream URL required")
	}
	if cfg.ReconnectInterval <= 0 {
		cfg.ReconnectInterval = 1 * time.Second
	}
	if cfg.MaxReconnectAttempts == 0 {
		cfg.MaxReconnectAttempts = 5
	}
	if cfg.MaxTracksPerUpstream == 0 {
		cfg.MaxTracksPerUpstream = 256
	}

	srv, err := server.New(cfg.Server)
	if err != nil {
		return nil, fmt.Errorf("relay: new downstream server: %w", err)
	}
	return &Relay{
		cfg:        cfg,
		srv:        srv,
		pubs:       make(map[string]*server.Publisher),
		trackOwner: make(map[string]string),
		closed:     make(chan struct{}),
	}, nil
}

// Server returns the embedded downstream Server. Useful for callers
// who want to query Addr(), ShareLink(), SubscriberCount(), Metrics(),
// etc., or to plumb the same server through other code paths.
func (r *Relay) Server() *server.Server { return r.srv }

// ListenAndServe starts the downstream server and connects to every
// configured upstream. Returns when ctx is cancelled or any upstream
// fails permanently (after MaxReconnectAttempts exhausted).
//
// Concurrency: pumps for each upstream run in parallel. Pumps within
// one upstream are also parallel (one per announced track).
func (r *Relay) ListenAndServe(ctx context.Context) error {
	// Run downstream server in its own goroutine; surface its error
	// via serverErr.
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- r.srv.ListenAndServe(ctx)
	}()

	// Wait for the downstream server to bind before dialing upstreams
	// — keeps share-link lookups deterministic.
	if err := r.waitForBind(ctx); err != nil {
		return fmt.Errorf("relay: downstream bind timeout: %w", err)
	}

	// Pump every upstream in parallel; collect their errors. The
	// pumps return nil on clean ctx cancel, or an error on permanent
	// upstream failure.
	pumpErr := make(chan error, len(r.cfg.Upstreams))
	var wg sync.WaitGroup
	for _, url := range r.cfg.Upstreams {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			pumpErr <- r.runUpstream(ctx, u)
		}(url)
	}

	// Wait for either ctx-done, downstream-server-error, or any
	// upstream-pump-error. First non-nil error wins.
	go func() { wg.Wait(); close(pumpErr) }()
	var firstErr error
	for {
		select {
		case <-ctx.Done():
			r.markShutdown()
			// Drain remaining pumps; ignore their errors (ctx done is
			// the expected shutdown signal, not an error condition).
			for range pumpErr {
			}
			return r.serverShutdownError(serverErr)
		case err := <-serverErr:
			// Downstream server died. Propagate.
			r.markShutdown()
			return fmt.Errorf("relay: downstream server: %w", err)
		case err, ok := <-pumpErr:
			if !ok {
				// All pumps finished without ctx cancel — relay has
				// nothing to do.
				return firstErr
			}
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("relay: upstream pump: %w", err)
				// One upstream's permanent failure is fatal — kick
				// downstream into shutdown so the caller's ctx unblocks.
				r.markShutdown()
			}
		}
	}
}

// Shutdown gracefully drains downstream subscribers and stops
// upstream pumps. Idempotent. Safe to call from any goroutine.
func (r *Relay) Shutdown(drainCtx context.Context) error {
	r.markShutdown()
	return r.srv.Shutdown(drainCtx)
}

func (r *Relay) markShutdown() {
	r.shutdownOnce.Do(func() { close(r.closed) })
}

// claimTrack atomically reserves a track name for the given upstream
// URL. Returns true if the claim succeeded (this upstream owns the
// name now, or already did). Returns false if a different upstream
// already owns it — caller should refuse to start a pump for the
// collision.
func (r *Relay) claimTrack(name, upstreamURL string) bool {
	r.pubsMu.Lock()
	defer r.pubsMu.Unlock()
	if owner, ok := r.trackOwner[name]; ok {
		return owner == upstreamURL
	}
	r.trackOwner[name] = upstreamURL
	return true
}

// runUpstream dials one upstream and pumps its tracks until the
// upstream is permanently dead or ctx is cancelled. Auto-reconnects
// on transient errors using quicrtc's resume protocol.
func (r *Relay) runUpstream(ctx context.Context, url string) error {
	var (
		sessionID   string
		lastSeenSeq map[string]uint32
		attempts    int
	)
	for {
		// Reconnect bookkeeping: capped retry, exponential off optional
		// (kept linear here; the reconnect interval is small by default
		// and ListenAndServe's caller can wrap with its own backoff).
		opts := r.cfg.ClientOptions
		if sessionID != "" {
			opts.SessionID = sessionID
			opts.LastSeenSeq = lastSeenSeq
		}
		c, err := client.Dial(ctx, url, opts)
		if err != nil {
			attempts++
			if r.cfg.MaxReconnectAttempts >= 0 && attempts > r.cfg.MaxReconnectAttempts {
				return fmt.Errorf("dial %s: %w (attempts exhausted)", url, err)
			}
			if !sleep(ctx, r.cfg.ReconnectInterval) {
				return nil // ctx cancelled
			}
			continue
		}
		// Successful dial — reset attempt counter and capture the
		// session ID for future resume.
		attempts = 0
		sessionID = c.SessionID()
		dialedAt := time.Now()

		// Drain tracks until disconnect; sessionLoop returns nil on
		// clean ctx cancel, or an error if the upstream session died
		// (in which case we loop to reconnect).
		err = r.sessionLoop(ctx, c, url)
		lastSeenSeq = c.LastSeenSeq()
		_ = c.Close()
		if err == nil {
			return nil
		}
		// A session that ran successfully for a while should reset
		// the attempt budget — otherwise a long-lived relay that
		// has 5 quick reconnects after hours of healthy uptime
		// would give up. The threshold matches ReconnectInterval to
		// avoid double-counting tight reconnect loops.
		if time.Since(dialedAt) > 10*r.cfg.ReconnectInterval {
			attempts = 0
		}
		// Transient disconnect — try to resume.
		if !sleep(ctx, r.cfg.ReconnectInterval) {
			return nil
		}
	}
}

// sessionLoop pumps every announced track of one upstream client into
// the downstream broadcaster. Returns nil on ctx cancel, an error if
// the upstream session ends abnormally.
//
// Per-track pumps are bounded by Config.MaxTracksPerUpstream and
// guarded against cross-upstream name collisions via claimTrack.
// Pump goroutines self-prune from the `pumped` map on exit so the
// map size stays bounded even across upstream churn.
func (r *Relay) sessionLoop(ctx context.Context, c *client.Client, upstreamURL string) error {
	// Per-session bookkeeping of which tracks already have a pump.
	pumped := make(map[string]context.CancelFunc)
	pumpedMu := sync.Mutex{}
	defer func() {
		pumpedMu.Lock()
		for _, cancel := range pumped {
			cancel()
		}
		pumpedMu.Unlock()
	}()

	// Per-session context for the pumps. Cancelled when this function
	// returns (so pump goroutines stop when the upstream session ends).
	sessCtx, cancelSess := context.WithCancel(ctx)
	defer cancelSess()

	// startPump idempotently spawns a forwarding goroutine for a
	// track. Multiple callers can race; the map guard protects.
	// Returns false if the cap was hit or the track is already owned
	// by a different upstream — the caller treats that as a no-op.
	startPump := func(name string) bool {
		pumpedMu.Lock()
		if _, exists := pumped[name]; exists {
			pumpedMu.Unlock()
			return true
		}
		if r.cfg.MaxTracksPerUpstream >= 0 && len(pumped) >= r.cfg.MaxTracksPerUpstream {
			pumpedMu.Unlock()
			return false
		}
		// Reserve the global slot before spawning; if a different
		// upstream owns this name, refuse the pump entirely.
		if !r.claimTrack(name, upstreamURL) {
			pumpedMu.Unlock()
			return false
		}
		pumpCtx, cancelPump := context.WithCancel(sessCtx)
		pumped[name] = cancelPump
		pumpedMu.Unlock()

		pub := r.getOrCreatePublisher(name)
		go func() {
			defer func() {
				pumpedMu.Lock()
				delete(pumped, name)
				pumpedMu.Unlock()
				cancelPump()
			}()
			for {
				au, err := c.RecvOn(pumpCtx, name)
				if err != nil {
					return
				}
				_ = pub.Publish(pumpCtx, pubsub.AccessUnit{
					Bytes:         au.Bytes,
					Keyframe:      au.Keyframe,
					PTSMicro:      au.PTSMicro,
					Seq:           au.Seq,
					Discontinuity: au.Discontinuity,
				})
			}
		}()
		return true
	}

	// Always pump the implicit "primary" track. It is auto-created on
	// the client side at Dial time, so the receive queue exists even
	// if the upstream hasn't announced a name yet. If "primary" is
	// already owned by another upstream, this call no-ops — the
	// upstream is effectively muted on the collided name, which is
	// safer than interleaving.
	startPump("primary")

	// Poll the upstream's announced tracks; start a pump for any new
	// name. Polling cadence is the same as the POC — short enough to
	// catch a new ANNOUNCE within ~100ms but coarse enough to not be
	// a per-frame hot path.
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-r.closed:
			return nil
		case <-c.Done():
			return errors.New("upstream client died")
		case <-tick.C:
			for _, name := range c.RemoteTracks() {
				startPump(name)
			}
		}
	}
}

// getOrCreatePublisher returns the downstream publisher for a track
// name, creating it on first use. The "primary" name is special-cased
// because it's auto-created by server.New.
func (r *Relay) getOrCreatePublisher(name string) *server.Publisher {
	r.pubsMu.Lock()
	defer r.pubsMu.Unlock()
	if pub, ok := r.pubs[name]; ok {
		return pub
	}
	var pub *server.Publisher
	if name == "primary" {
		pub = r.srv.Publisher()
	} else {
		pub = r.srv.AddTrack(name)
	}
	r.pubs[name] = pub
	return pub
}

// waitForBind polls srv.Addr until the bind port is concrete (no
// trailing ":0"). Caps the wait at 2 seconds, matching the existing
// pattern in benchmarks/helpers.go and the POC.
func (r *Relay) waitForBind(ctx context.Context) error {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		addr := r.srv.Addr()
		if addr != "" && !endsWith(addr, ":0") {
			return nil
		}
		if !sleep(ctx, 10*time.Millisecond) {
			return ctx.Err()
		}
	}
	return errors.New("downstream did not bind within 2s")
}

// serverShutdownError waits up to 1s for the server goroutine to
// observe ctx cancellation and return. If it doesn't, returns nil
// (ctx cancellation is the expected shutdown path).
func (r *Relay) serverShutdownError(serverErr <-chan error) error {
	select {
	case err := <-serverErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("relay: downstream shutdown: %w", err)
		}
		return nil
	case <-time.After(1 * time.Second):
		return nil
	}
}

// sleep waits for d or until ctx is cancelled. Returns true if d
// elapsed, false if ctx was cancelled first.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// endsWith is strings.HasSuffix without the import.
func endsWith(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}
