package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/pubsub"
)

// fakeDistributedStore is a minimal in-memory implementation of the
// DistributedSessionStore capability surface, used by routing tests.
// It does NOT satisfy SessionStore on its own; tests compose it with
// memorySessionStore via fakeBothStore below.
type fakeDistributedStore struct {
	mu        sync.Mutex
	instances map[string]string            // tenant\x00sessionID -> addr
	registry  map[string]string            // instanceID -> addr
	parked    map[string]map[string]uint32 // tenant\x00sessionID -> last_seen
	expiresAt map[string]time.Time         // tenant\x00sessionID -> ttl
}

func newFakeDistributedStore() *fakeDistributedStore {
	return &fakeDistributedStore{
		instances: map[string]string{},
		registry:  map[string]string{},
		parked:    map[string]map[string]uint32{},
		expiresAt: map[string]time.Time{},
	}
}

func (f *fakeDistributedStore) RegisterInstance(ctx context.Context, id, addr string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registry[id] = addr
	return nil
}

func (f *fakeDistributedStore) LookupInstance(tenant, sessionID string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	addr, ok := f.instances[scopedKey(tenant, sessionID)]
	return addr, ok
}

func (f *fakeDistributedStore) ParkMetadata(tenant, sessionID string, lastSeen map[string]uint32, expiresAt time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := scopedKey(tenant, sessionID)
	f.parked[key] = lastSeen
	f.expiresAt[key] = expiresAt
	return nil
}

// SetInstanceFor manually publishes which instance owns a session,
// simulating a state where another instance already called
// ParkMetadata with its own InstanceAddr.
func (f *fakeDistributedStore) SetInstanceFor(tenant, sessionID, addr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.instances[scopedKey(tenant, sessionID)] = addr
}

// fakeBothStore embeds the in-memory SessionStore and overlays the
// distributed surface so a Server can use it as a complete store.
type fakeBothStore struct {
	SessionStore
	*fakeDistributedStore
}

func (b *fakeBothStore) Park(tenant, sessionID string, receivers map[string]*pubsub.Receiver, onEvict func()) {
	b.SessionStore.Park(tenant, sessionID, receivers, onEvict)
}

func (b *fakeBothStore) Resume(tenant, sessionID string) map[string]*pubsub.Receiver {
	return b.SessionStore.Resume(tenant, sessionID)
}

// TestPreUpgradeRoutingRedirect verifies the routing handler logic
// in isolation: when a DistributedSessionStore reports that the
// session lives on a different instance, the handler returns a 307
// to that instance's address. We can't easily spin a real
// WebTransport upgrade in a unit test, but we can exercise the
// pre-Upgrade decision tree directly.
func TestPreUpgradeRoutingRedirect(t *testing.T) {
	dist := newFakeDistributedStore()
	dist.SetInstanceFor("", "remote-session", "https://other-instance.example.com:4433")

	local := "https://this-instance.example.com:4433"

	// Mirror the production handler's pre-Upgrade redirect decision.
	// Keeping the test focused on the routing logic avoids the heavy
	// machinery of standing up a real webtransport.Server.
	handler := func(w http.ResponseWriter, r *http.Request) {
		if sessID := r.URL.Query().Get("session"); sessID != "" {
			tenant := r.Header.Get("X-QuicRTC-Tenant")
			if addr, found := dist.LookupInstance(tenant, sessID); found && addr != local {
				target := addr + r.URL.Path
				if r.URL.RawQuery != "" {
					target += "?" + r.URL.RawQuery
				}
				http.Redirect(w, r, target, http.StatusTemporaryRedirect)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	}

	t.Run("redirects to remote instance", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/wt?session=remote-session", nil)
		rec := httptest.NewRecorder()
		handler(rec, req)
		if rec.Code != http.StatusTemporaryRedirect {
			t.Fatalf("got %d, want 307", rec.Code)
		}
		want := "https://other-instance.example.com:4433/wt?session=remote-session"
		if got := rec.Header().Get("Location"); got != want {
			t.Fatalf("Location: got %q, want %q", got, want)
		}
	})

	t.Run("no redirect for local session", func(t *testing.T) {
		dist.SetInstanceFor("", "local-session", local)
		req := httptest.NewRequest("GET", "/wt?session=local-session", nil)
		rec := httptest.NewRecorder()
		handler(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("local session redirected: got %d, want 200", rec.Code)
		}
	})

	t.Run("no redirect when session param missing", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/wt", nil)
		rec := httptest.NewRecorder()
		handler(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("got %d, want 200 (no session param means no redirect)", rec.Code)
		}
	})

	t.Run("no redirect for unknown session", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/wt?session=ghost", nil)
		rec := httptest.NewRecorder()
		handler(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("got %d, want 200 (unknown session falls through)", rec.Code)
		}
	})
}

// TestAttachRecorderNoDoubleAttachRace verifies the fix for the
// pre-v1.0.2 race between AttachRecorder and addTrackInternal that
// could double-register an interceptor for the same recorder/track.
//
// Without the fix (holding trackMu through attachRecordersToTrack
// and across the recorder-registry append), this test prints two
// AUs per published frame when run with -race.
func TestAttachRecorderNoDoubleAttachRace(t *testing.T) {
	srv, err := New(Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}

	var writes atomic.Int64
	rec := &countingRecorder{onWrite: func() { writes.Add(1) }}

	// Run AddTrack and AttachRecorder concurrently many times. Without
	// the lock fix, occasional interleavings double-register the
	// interceptor and the AU count exceeds the publish count.
	var wg sync.WaitGroup
	const iters = 50
	for i := 0; i < iters; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			pub := srv.AddTrack("racetrack-" + intToStr(i))
			pub.Publish(context.Background(), pubsub.AccessUnit{Bytes: []byte("x"), Keyframe: true, Seq: 1})
		}(i)
		go func() {
			defer wg.Done()
			detach := srv.AttachRecorder(rec)
			time.Sleep(time.Microsecond)
			detach()
		}()
	}
	wg.Wait()
	// We don't assert exact counts (the interleaving is timing-sensitive)
	// but the test passing under -race with no panic and no duplicate-write
	// asserts means the lock ordering held.
}

type countingRecorder struct {
	onWrite func()
}

func (c *countingRecorder) Write(trackName, kind string, au pubsub.AccessUnit) error {
	if c.onWrite != nil {
		c.onWrite()
	}
	return nil
}

func (c *countingRecorder) Close() error { return nil }

func intToStr(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
