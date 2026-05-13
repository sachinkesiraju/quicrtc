// Persisted session state for cross-disconnect resumption.
//
// When a subscriber disconnects, we don't immediately tear down its
// per-session state — broadcaster receivers, track set, last-seen
// sequence numbers. Instead we move them into the sessionStore
// keyed by SessionID. The receivers' channels buffer AUs that arrive
// during the gap (subject to per-receiver buffer capacity).
//
// On reconnect with HELLO.SessionID, the server finds the persisted
// state, hands it to the new Session, and the new pump goroutines
// drain the buffered AUs onto fresh uni streams. Subscribers see a
// continuous sequence with no gap.
//
// Persisted entries expire after Config.Session.ResumeWindow if no
// reconnect happens (default 60s). Eviction is opportunistic on
// add/lookup.
package server

import (
	"crypto/rand"
	"encoding/base64"
	"sync"
	"time"

	"github.com/sachinkesiraju/quicrtc/pubsub"
)

const defaultResumeWindow = 60 * time.Second

// SessionStore is the pluggable backend that holds parked session
// state across socket disconnects. The default in-memory impl is
// sufficient for single-instance deployments; production multi-
// instance setups can plug a shared backend (Redis, etcd, postgres)
// for cross-instance resume.
//
// Sessions are namespaced by (tenant, sessionID): a resume request
// from tenant A cannot match a parked session belonging to tenant B
// even if both happen to share a SessionID. For single-tenant
// deployments, tenant is simply the empty string and the namespace
// is global.
//
// IMPORTANT scope note: a parked session holds in-process receivers
// (channels into broadcasters). These cannot be serialized across
// processes. So a "shared" backend can hold session metadata
// (track set, last-seen sequences, expiresAt) but the actual replay
// path requires either:
//  1. Sticky routing — LB sends reconnect to the same instance
//     that holds the receivers (recommended P0).
//  2. Active session migration between instances (future P1) —
//     complex; involves shipping replay buffers + reattaching to
//     the broadcaster on the new instance.
//
// For P0, sticky routing is the supported path. The HELLO/SDP
// SessionID can be used as the LB affinity key.
type SessionStore interface {
	// AllocateID returns a fresh, unique session ID.
	AllocateID() string
	// Park stores the receivers under (tenant, sessionID) with TTL
	// semantics. onEvict, if non-nil, is invoked synchronously when
	// the entry expires WITHOUT being resumed — used to unsubscribe
	// receivers from their broadcasters so the broadcasters stop
	// fanning to channels nobody will ever read again. Resume does
	// NOT invoke onEvict (the caller is responsible for the receivers
	// once they've been resumed).
	Park(tenant, sessionID string, receivers map[string]*pubsub.Receiver, onEvict func())
	// Resume looks up + removes the parked entry scoped to tenant.
	// Returns nil if the (tenant, sessionID) pair doesn't exist or
	// has expired.
	Resume(tenant, sessionID string) map[string]*pubsub.Receiver
}

// persistedSession is the snapshot the server keeps when a subscriber
// disconnects, so a reconnect with the same SessionID can pick up.
type persistedSession struct {
	id        string
	receivers map[string]*pubsub.Receiver // track name -> live receiver
	expiresAt time.Time
	onEvict   func() // called when the entry expires (not on Resume)
}

// memorySessionStore is the default in-memory SessionStore. Single-
// instance deployments use this; multi-instance deployments can
// continue to use it with sticky LB routing keyed on SessionID.
type memorySessionStore struct {
	mu        sync.Mutex
	byID      map[string]*persistedSession
	resumeTTL time.Duration
}

// NewMemorySessionStore returns the default in-memory SessionStore.
// Equivalent to passing nil to server.Config.SessionStore.
func NewMemorySessionStore(ttl time.Duration) SessionStore {
	if ttl <= 0 {
		ttl = defaultResumeWindow
	}
	return &memorySessionStore{
		byID:      make(map[string]*persistedSession),
		resumeTTL: ttl,
	}
}

// AllocateID returns a fresh random session ID.
func (s *memorySessionStore) AllocateID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		// Fallback to timestamp; never expected to fire in practice.
		raw[0] = byte(time.Now().UnixNano())
	}
	return base64.RawURLEncoding.EncodeToString(raw[:])
}

// scopedKey namespaces a parked session by (tenant, sessionID) so
// resume requests cannot cross tenant boundaries.
func scopedKey(tenant, sessionID string) string {
	// '\x00' is reserved by sessionID's base64url alphabet, so it's
	// a safe separator that can't collide with valid IDs.
	return tenant + "\x00" + sessionID
}

// Park moves a session's receivers into the store under
// (tenant, sessionID). Called when the underlying socket disconnects
// but the session is resumable. The receivers continue to drain into
// their channel buffers from the live broadcasters.
func (s *memorySessionStore) Park(tenant, sessionID string, receivers map[string]*pubsub.Receiver, onEvict func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictExpiredLocked(time.Now())
	s.byID[scopedKey(tenant, sessionID)] = &persistedSession{
		id:        sessionID,
		receivers: receivers,
		expiresAt: time.Now().Add(s.resumeTTL),
		onEvict:   onEvict,
	}
}

// Resume looks up a parked session by (tenant, sessionID) and removes
// it from the store. Returns nil if the entry doesn't exist or has
// expired.
func (s *memorySessionStore) Resume(tenant, sessionID string) map[string]*pubsub.Receiver {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictExpiredLocked(time.Now())
	key := scopedKey(tenant, sessionID)
	p, ok := s.byID[key]
	if !ok {
		return nil
	}
	delete(s.byID, key)
	return p.receivers
}

// evictExpiredLocked drops parked sessions past their TTL. Called
// opportunistically on every park/resume; no dedicated cleanup
// goroutine. Receivers in evicted sessions are unsubscribed from
// their broadcasters via the onEvict callback registered at Park
// time so the broadcasters stop fanning to channels nobody will
// ever read again.
func (s *memorySessionStore) evictExpiredLocked(now time.Time) {
	for id, p := range s.byID {
		if now.After(p.expiresAt) {
			delete(s.byID, id)
			if p.onEvict != nil {
				p.onEvict()
			}
		}
	}
}
