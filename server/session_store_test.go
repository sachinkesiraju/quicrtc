package server

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/pubsub"
)

func TestMemoryStoreEvictExpired(t *testing.T) {
	store := NewMemorySessionStore(50 * time.Millisecond).(*memorySessionStore)

	var aEvicted, bEvicted atomic.Int64
	store.Park("tenantA", "sessA", map[string]*pubsub.Receiver{}, func() { aEvicted.Add(1) })
	store.Park("tenantB", "sessB", map[string]*pubsub.Receiver{}, func() { bEvicted.Add(1) })

	if n := store.EvictExpired(time.Now()); n != 0 {
		t.Fatalf("immediate evict found %d, want 0", n)
	}

	time.Sleep(80 * time.Millisecond)

	n := store.EvictExpired(time.Now())
	if n != 2 {
		t.Fatalf("evict-after-TTL count = %d, want 2", n)
	}
	if aEvicted.Load() != 1 || bEvicted.Load() != 1 {
		t.Fatalf("onEvict callbacks: A=%d B=%d, want 1 1", aEvicted.Load(), bEvicted.Load())
	}

	if got := store.Resume("tenantA", "sessA"); got != nil {
		t.Fatalf("evicted entry still resumable")
	}
}

func TestMemoryStoreTenantScoped(t *testing.T) {
	store := NewMemorySessionStore(time.Minute)
	store.Park("tenantA", "sharedID", map[string]*pubsub.Receiver{}, nil)

	if got := store.Resume("tenantB", "sharedID"); got != nil {
		t.Fatalf("cross-tenant resume succeeded; tenant scoping broken")
	}
	if got := store.Resume("tenantA", "sharedID"); got == nil {
		t.Fatalf("same-tenant resume failed")
	}
}
