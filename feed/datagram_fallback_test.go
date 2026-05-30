package feed

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/track"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// failingDatagramSender always returns the configured error from
// SendDatagram, exercising the send-failed fallback branch.
type failingDatagramSender struct {
	err   error
	sends atomic.Int64
}

func (s *failingDatagramSender) SendDatagram(_ []byte) error {
	s.sends.Add(1)
	return s.err
}

// recordingDatagramSender lets a test alternate between failure and
// success per call so it can verify that a transient send error spills
// to the fallback while subsequent successful datagrams do NOT use it.
type recordingDatagramSender struct {
	mu      sync.Mutex
	sends   [][]byte
	failNth map[int]error // 1-based call indices that should fail
	calls   int
}

func (s *recordingDatagramSender) SendDatagram(payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if err, ok := s.failNth[s.calls]; ok {
		return err
	}
	s.sends = append(s.sends, append([]byte(nil), payload...))
	return nil
}

func (s *recordingDatagramSender) Sends() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.sends))
	for i, p := range s.sends {
		out[i] = append([]byte(nil), p...)
	}
	return out
}

// TestDatagramFallbackOversize covers the headline case: an AU whose
// payload exceeds MaxDatagramPayload must NOT be dropped when a
// FallbackOpener is configured. Instead the pump opens a uni stream
// and writes the AU there as a self-contained feed frame.
func TestDatagramFallbackOversize(t *testing.T) {
	b := pubsub.NewBroadcaster(16)
	r := b.Subscribe()

	sender := &recordingDatagramSender{}
	op := &fakeOpener{}

	var fallbackReason atomic.Value // string
	var dropReason atomic.Value     // string
	cfg := Config{
		Delivery:       track.DeliveryDatagramOrStream,
		DatagramSender: sender,
		FallbackOpener: op,
		TrackName:      "telemetry",
		TrackID:        7,
		OnFallback: func(reason string) {
			fallbackReason.Store(reason)
		},
		OnAUDropped: func(reason string) {
			dropReason.Store(reason)
		},
	}
	p := New(op, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx, r) }()

	// Build an oversize payload deliberately.
	huge := bytes.Repeat([]byte{0xab}, wire.MaxDatagramPayload+200)
	b.Publish(pubsub.AccessUnit{Bytes: huge, Keyframe: true, Seq: 1, PTSMicro: 1000})

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(op.Streams()) == 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done
	b.Unsubscribe(r)

	streams := op.Streams()
	if len(streams) != 1 {
		t.Fatalf("expected 1 fallback stream, got %d", len(streams))
	}
	got := streams[0].Bytes()
	// Expect StreamHeader for "telemetry" + one feed frame.
	rdr := bytes.NewReader(got)
	trackName, _, err := wire.PeekStreamHeader(rdr)
	if err != nil {
		t.Fatalf("read stream header: %v", err)
	}
	if trackName != "telemetry" {
		t.Fatalf("stream header track=%q, want telemetry", trackName)
	}
	f, err := wire.ReadFeedFrame(rdr)
	if err != nil {
		t.Fatalf("read feed frame: %v", err)
	}
	if !bytes.Equal(f.Payload, huge) {
		t.Fatalf("fallback payload differs from input (lengths %d vs %d)", len(f.Payload), len(huge))
	}

	if got := fallbackReason.Load(); got != "datagram_too_large" {
		t.Fatalf("OnFallback reason = %v, want datagram_too_large", got)
	}
	if got := dropReason.Load(); got != nil {
		t.Fatalf("expected no drop, got %v", got)
	}
	if sender.calls != 0 {
		// EncodeDatagram rejects oversize before SendDatagram is called,
		// so we must NOT see the sender hit the wire for the oversize AU.
		t.Fatalf("SendDatagram called %d times for oversize AU; expected 0", sender.calls)
	}
}

// TestDatagramFallbackSendFailed covers the spillover case where the
// datagram is sized fine but the transport rejects it (e.g. quic-go
// returns DatagramTooLargeError for a path-MTU change, or the queue
// is full). The AU must land on the fallback stream.
func TestDatagramFallbackSendFailed(t *testing.T) {
	b := pubsub.NewBroadcaster(16)
	r := b.Subscribe()

	sendErr := errors.New("transport rejected")
	sender := &failingDatagramSender{err: sendErr}
	op := &fakeOpener{}

	var fallbackReason atomic.Value
	cfg := Config{
		Delivery:       track.DeliveryDatagramOrStream,
		DatagramSender: sender,
		FallbackOpener: op,
		TrackName:      "telemetry",
		OnFallback: func(reason string) {
			fallbackReason.Store(reason)
		},
	}
	p := New(op, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx, r) }()

	small := []byte("tok=42")
	b.Publish(pubsub.AccessUnit{Bytes: small, Keyframe: true, Seq: 1, PTSMicro: 1000})

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(op.Streams()) == 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done
	b.Unsubscribe(r)

	if sender.sends.Load() != 1 {
		t.Fatalf("SendDatagram called %d times; expected 1 (the failing call)", sender.sends.Load())
	}
	streams := op.Streams()
	if len(streams) != 1 {
		t.Fatalf("expected 1 fallback stream, got %d", len(streams))
	}
	rdr := bytes.NewReader(streams[0].Bytes())
	if _, _, err := wire.PeekStreamHeader(rdr); err != nil {
		t.Fatalf("read stream header: %v", err)
	}
	f, err := wire.ReadFeedFrame(rdr)
	if err != nil {
		t.Fatalf("read feed frame: %v", err)
	}
	if !bytes.Equal(f.Payload, small) {
		t.Fatalf("fallback payload differs from input")
	}
	if got := fallbackReason.Load(); got != "send_failed" {
		t.Fatalf("OnFallback reason = %v, want send_failed", got)
	}
}

// TestDatagramFallbackReusesStream confirms that a sequence of fallback
// activations uses ONE persistent uni stream, not one per AU. This is
// what makes the fallback path acceptable for steady-state oversize
// workloads: the open cost amortizes.
func TestDatagramFallbackReusesStream(t *testing.T) {
	b := pubsub.NewBroadcaster(16)
	r := b.Subscribe()

	sender := &failingDatagramSender{err: errors.New("rejected")}
	op := &fakeOpener{}

	cfg := Config{
		Delivery:       track.DeliveryDatagramOrStream,
		DatagramSender: sender,
		FallbackOpener: op,
		TrackName:      "telemetry",
	}
	p := New(op, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx, r) }()

	const n = 5
	for i := 0; i < n; i++ {
		b.Publish(pubsub.AccessUnit{Bytes: []byte{byte(i)}, Keyframe: true, Seq: uint32(i + 1), PTSMicro: uint64(i+1) * 1000})
	}

	// Wait for all AUs to land on the fallback stream.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(op.Streams()) > 0 {
			rdr := bytes.NewReader(op.Streams()[0].Bytes())
			if _, _, err := wire.PeekStreamHeader(rdr); err == nil {
				count := 0
				for rdr.Len() > 0 {
					if _, err := wire.ReadFeedFrame(rdr); err != nil {
						break
					}
					count++
				}
				if count == n {
					break
				}
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done
	b.Unsubscribe(r)

	streams := op.Streams()
	if len(streams) != 1 {
		t.Fatalf("expected 1 fallback stream reused, got %d (one-per-AU regression)", len(streams))
	}
}

// TestDatagramFallbackNotConfiguredDrops mirrors the pre-fix behavior
// when no FallbackOpener is supplied: oversize and send-failed AUs
// must still be dropped and surfaced via OnAUDropped. The drop reason
// disambiguates the cause.
func TestDatagramFallbackNotConfiguredDrops(t *testing.T) {
	b := pubsub.NewBroadcaster(16)
	r := b.Subscribe()

	sender := &failingDatagramSender{err: errors.New("rejected")}

	dropReasons := make(chan string, 4)
	cfg := Config{
		Delivery:       track.DeliveryDatagramOrStream,
		DatagramSender: sender,
		// No FallbackOpener.
		OnAUDropped: func(reason string) {
			select {
			case dropReasons <- reason:
			default:
			}
		},
	}
	p := New(&fakeOpener{}, cfg) // unused opener; pump never calls it

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx, r) }()

	// Send-failed path.
	b.Publish(pubsub.AccessUnit{Bytes: []byte("x"), Keyframe: true, Seq: 1})
	// Oversize path.
	huge := bytes.Repeat([]byte{0xcc}, wire.MaxDatagramPayload+1)
	b.Publish(pubsub.AccessUnit{Bytes: huge, Keyframe: true, Seq: 2})

	got := make(map[string]int)
	timeout := time.After(500 * time.Millisecond)
	for len(got) < 2 {
		select {
		case r := <-dropReasons:
			got[r]++
		case <-timeout:
			t.Fatalf("did not observe both drop reasons; got %v", got)
		}
	}
	cancel()
	<-done
	b.Unsubscribe(r)

	if got["send_failed"] == 0 {
		t.Fatalf("missing send_failed drop, got %v", got)
	}
	if got["datagram_too_large"] == 0 {
		t.Fatalf("missing datagram_too_large drop, got %v", got)
	}
}

// TestDatagramFallbackOpenFailureDrops covers the case where a
// FallbackOpener IS configured but the open itself fails (network
// down, peer gone). The AU must surface as fallback_open_failed so
// observers can distinguish "spillover unavailable" from "no fallback
// configured."
func TestDatagramFallbackOpenFailureDrops(t *testing.T) {
	b := pubsub.NewBroadcaster(16)
	r := b.Subscribe()

	sender := &failingDatagramSender{err: errors.New("rejected")}
	// Opener that always fails.
	op := &fakeOpener{responses: []openResponse{
		{err: errors.New("connection broken")},
		{err: errors.New("connection broken")},
	}}

	gotDrop := make(chan string, 1)
	cfg := Config{
		Delivery:       track.DeliveryDatagramOrStream,
		DatagramSender: sender,
		FallbackOpener: op,
		TrackName:      "t",
		OnAUDropped: func(reason string) {
			select {
			case gotDrop <- reason:
			default:
			}
		},
	}
	p := New(op, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx, r) }()

	b.Publish(pubsub.AccessUnit{Bytes: []byte("x"), Keyframe: true, Seq: 1})

	select {
	case reason := <-gotDrop:
		if reason != "fallback_open_failed" {
			t.Fatalf("drop reason = %q, want fallback_open_failed", reason)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("did not observe fallback_open_failed drop")
	}
	cancel()
	<-done
	b.Unsubscribe(r)
}

// TestDatagramSuccessNoFallback confirms the steady-state: when the
// datagram path is healthy, the FallbackOpener is never invoked. This
// guards against regressions that would silently force every AU through
// the slower stream path.
func TestDatagramSuccessNoFallback(t *testing.T) {
	b := pubsub.NewBroadcaster(16)
	r := b.Subscribe()

	sender := &recordingDatagramSender{}
	op := &fakeOpener{}

	cfg := Config{
		Delivery:       track.DeliveryDatagramOrStream,
		DatagramSender: sender,
		FallbackOpener: op,
		TrackName:      "telemetry",
		TrackID:        3,
	}
	p := New(op, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx, r) }()

	for i := 0; i < 4; i++ {
		b.Publish(pubsub.AccessUnit{Bytes: []byte{byte(i)}, Keyframe: true, Seq: uint32(i + 1)})
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(sender.Sends()) == 4 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done
	b.Unsubscribe(r)

	if got := len(sender.Sends()); got != 4 {
		t.Fatalf("datagram sends = %d, want 4", got)
	}
	if got := len(op.Streams()); got != 0 {
		t.Fatalf("fallback streams = %d, want 0 (fallback should not activate)", got)
	}
}
