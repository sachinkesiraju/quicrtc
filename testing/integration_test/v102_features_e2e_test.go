// End-to-end coverage for the two v1.0.2 fixes: datagram fallback and
// priority scheduler wire-up. These exercise the full server → session
// → wire → client stack over real QUIC + WebTransport on loopback, so
// any wire-format or demux regression that the feed-level unit tests
// missed would show up here.
package integration_test

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/server"
	"github.com/sachinkesiraju/quicrtc/track"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// TestEndToEndDatagramFallbackOversize publishes a KindTelemetry AU
// whose payload exceeds wire.MaxDatagramPayload. The fix should route
// it through a fallback uni stream; the receiver's stream demuxer must
// recover the AU on the named track. Before the v1.0.2 fix, this AU
// would have been dropped on the publisher side with OnAUDropped
// reason "datagram_too_large".
func TestEndToEndDatagramFallbackOversize(t *testing.T) {
	s, _ := startServer(t, server.Config{})
	c := dialClient(t, s)

	const trackName = "cu_telemetry"
	pub := s.AddTrackSpec(server.TrackSpec{
		Name:    trackName,
		Kind:    track.KindTelemetry,
		TrackID: 9,
	})

	// Wait for the session-side AttachTrack to propagate (the server
	// AddTrackSpec is async on the per-session pump start).
	time.Sleep(80 * time.Millisecond)

	// Build an oversize payload. Just above the datagram cap is enough
	// to trigger the encode-side rejection in feed/datagram.go.
	payload := bytes.Repeat([]byte{0xC0, 0xDE}, wire.MaxDatagramPayload)
	if len(payload) <= wire.MaxDatagramPayload {
		t.Fatalf("test setup: payload (%d) should exceed MaxDatagramPayload (%d)",
			len(payload), wire.MaxDatagramPayload)
	}

	au := pubsub.AccessUnit{
		Bytes:    payload,
		Keyframe: true, // self-contained, like every telemetry AU
		Seq:      1,
		PTSMicro: 1_000,
	}
	if err := pub.Publish(context.Background(), au); err != nil {
		t.Fatalf("publish: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	got, err := c.RecvOn(ctx, trackName)
	if err != nil {
		t.Fatalf("RecvOn(%q): %v -- oversize AU did NOT arrive via fallback stream",
			trackName, err)
	}
	if !bytes.Equal(got.Bytes, payload) {
		t.Fatalf("payload mismatch on fallback path: got %d bytes, want %d (first 16 of got: %x)",
			len(got.Bytes), len(payload), got.Bytes[:min(16, len(got.Bytes))])
	}
	if got.Seq != au.Seq {
		t.Errorf("seq mismatch: got %d, want %d", got.Seq, au.Seq)
	}
}

// TestEndToEndPrioritySchedulerWireUp verifies that with
// UsePriorityScheduler=true the server creates a per-session scheduler,
// plumbs it into every per-track pump, and delivery still works
// correctly for multi-track concurrent publishing.
//
// What this DOES verify (end-to-end through real QUIC):
//   - The config flag flips on, no panics or deadlocks at session
//     setup or teardown.
//   - Two tracks at different priorities both deliver all their AUs.
//   - Per-track in-stream order is preserved (the scheduler must not
//     reorder within a track).
//   - The session shuts down cleanly with the scheduler closed (no
//     leaked pump goroutine — the Submit-after-Close fix).
//
// What this does NOT verify (and intentionally so):
//   - Strict cross-track priority ordering. On fast loopback each pump
//     submits one AU and waits; the write completes before the other
//     pump submits, so the scheduler rarely sees both queues populated
//     at once. The unit test feed/scheduler_pump_test.go::
//     TestSchedulerPrioritizesAcrossPumps already proves the preemption
//     semantic under controlled contention.
func TestEndToEndPrioritySchedulerWireUp(t *testing.T) {
	s, _ := startServer(t, server.Config{
		UsePriorityScheduler: true,
	})
	c := dialClient(t, s)

	const (
		hiName = "tokens_hi"
		loName = "tokens_lo"
		n      = 12
	)

	hi := s.AddTrackSpec(server.TrackSpec{Name: hiName, Kind: track.KindTokens, Priority: 1})
	lo := s.AddTrackSpec(server.TrackSpec{Name: loName, Kind: track.KindTokens, Priority: 6})

	time.Sleep(80 * time.Millisecond)

	var publishWG sync.WaitGroup
	publishWG.Add(2)
	go func() {
		defer publishWG.Done()
		for i := 0; i < n; i++ {
			_ = hi.Publish(context.Background(), pubsub.AccessUnit{
				Bytes:    []byte{byte(i)},
				Keyframe: true,
				Seq:      uint32(i),
				PTSMicro: uint64(i) * 1000,
			})
		}
	}()
	go func() {
		defer publishWG.Done()
		for i := 0; i < n; i++ {
			_ = lo.Publish(context.Background(), pubsub.AccessUnit{
				Bytes:    []byte{byte(i)},
				Keyframe: true,
				Seq:      uint32(i),
				PTSMicro: uint64(i) * 1000,
			})
		}
	}()
	publishWG.Wait()

	type arrival struct {
		track string
		seq   uint32
		index int
	}
	var (
		arrivalsMu sync.Mutex
		arrivals   []arrival
		ordinal    atomic.Int32
	)
	record := func(name string, seq uint32) {
		i := int(ordinal.Add(1))
		arrivalsMu.Lock()
		arrivals = append(arrivals, arrival{track: name, seq: seq, index: i})
		arrivalsMu.Unlock()
	}

	var drainWG sync.WaitGroup
	drainWG.Add(2)
	drain := func(name string) {
		defer drainWG.Done()
		for got := 0; got < n; got++ {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			au, err := c.RecvOn(ctx, name)
			cancel()
			if err != nil {
				t.Errorf("drain %s: RecvOn after %d: %v", name, got, err)
				return
			}
			record(name, au.Seq)
		}
	}
	go drain(hiName)
	go drain(loName)
	drainWG.Wait()

	// Sanity: every track delivered exactly n AUs, no duplicates, no
	// gaps, no out-of-order within a track.
	perTrack := map[string][]uint32{hiName: nil, loName: nil}
	for _, a := range arrivals {
		perTrack[a.track] = append(perTrack[a.track], a.seq)
	}
	for name, seqs := range perTrack {
		if len(seqs) != n {
			t.Fatalf("%s: received %d AUs, want %d", name, len(seqs), n)
		}
		seen := make(map[uint32]bool, n)
		for i, s := range seqs {
			if seen[s] {
				t.Fatalf("%s: duplicate seq=%d", name, s)
			}
			seen[s] = true
			if i > 0 && s < seqs[i-1] {
				t.Fatalf("%s: in-stream reordering: seq %d arrived after %d (scheduler must not reorder within a track)",
					name, s, seqs[i-1])
			}
		}
	}

	// Visibility (not an assertion): log the interleaving pattern. A
	// fully-FIFO-by-pump session would show all of one track then all
	// of the other; the scheduler-wired path typically interleaves.
	var pattern []string
	for _, a := range arrivals {
		short := "L"
		if a.track == hiName {
			short = "H"
		}
		pattern = append(pattern, short)
	}
	t.Logf("interleave pattern (H=priority-1, L=priority-6): %v", pattern)
}

// TestEndToEndPrioritySchedulerOff is the symmetric guard: with
// UsePriorityScheduler=false, the legacy direct-write path runs and
// Priority is observability-only. We don't assert specific ordering
// here (no scheduler = no cross-track guarantee), only that delivery
// still works correctly so the off-by-default switch doesn't break
// the basic publish/subscribe path.
func TestEndToEndPrioritySchedulerOff(t *testing.T) {
	s, _ := startServer(t, server.Config{
		UsePriorityScheduler: false, // explicit
	})
	c := dialClient(t, s)

	const trackName = "tokens"
	pub := s.AddTrackSpec(server.TrackSpec{
		Name:     trackName,
		Kind:     track.KindTokens,
		Priority: 2,
	})
	time.Sleep(80 * time.Millisecond)

	const n = 6
	go func() {
		for i := 0; i < n; i++ {
			_ = pub.Publish(context.Background(), pubsub.AccessUnit{
				Bytes:    []byte{byte(i)},
				Keyframe: true,
				Seq:      uint32(i),
				PTSMicro: uint64(i) * 1000,
			})
		}
	}()

	got := 0
	deadline := time.Now().Add(3 * time.Second)
	for got < n && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_, err := c.RecvOn(ctx, trackName)
		cancel()
		if err != nil {
			t.Logf("RecvOn after %d: %v", got, err)
			continue
		}
		got++
	}
	if got < n {
		t.Fatalf("received %d/%d with scheduler off — legacy path broken", got, n)
	}
}
