package pubsub

import (
	"sort"
	"sync"
)

// Receiver is one subscriber handle. The caller reads from Frames()
// in a loop; quicrtc closes the channel when Unsubscribe is called.
type Receiver struct {
	ch chan AccessUnit

	// mu guards needKeyframe AND the closed flag. Unsubscribe takes
	// mu while closing the channel; Publish takes mu before sending.
	// This is the only thing that prevents send-on-closed-channel
	// when Unsubscribe runs concurrently with a Publish iteration.
	mu           sync.Mutex
	closed       bool
	needKeyframe bool
}

// Frames returns the read-side of the subscriber's channel. The
// channel is closed when the receiver is unsubscribed.
func (r *Receiver) Frames() <-chan AccessUnit { return r.ch }

// RequestKeyframe marks the receiver as needing the next keyframe.
// Subsequent P-frames will be skipped at fanout time until a keyframe
// arrives. Useful for the session loop to call when a stream-write
// fails for an individual frame: dropped P-frame downstream cascades
// garbage, so we'd rather skip until the next IDR.
func (r *Receiver) RequestKeyframe() {
	r.mu.Lock()
	r.needKeyframe = true
	r.mu.Unlock()
}

// NeedsKeyframe is true if the receiver is currently waiting for a
// keyframe (either because it just subscribed and none was cached, or
// because a previous P-frame was dropped).
func (r *Receiver) NeedsKeyframe() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.needKeyframe
}

// Broadcaster fans access units out to all current subscribers.
// One producer goroutine calls Publish; many consumer goroutines (one
// per subscriber session) read from their Receiver.Frames().
//
// In addition to fan-out, the broadcaster keeps a small ring buffer
// of the most-recently-published AUs (replayBuf). On session resume,
// the server can ask the broadcaster to replay AUs since a given seq
// so subscribers that missed AUs in flight at disconnect time (queued
// in QUIC send buffers or on the wire when their connection died)
// can catch up to the live stream with zero gap. Default capacity is
// 256 AUs — enough to cover a few seconds at typical token / video
// rates without unbounded memory growth.
type Broadcaster struct {
	mu             sync.Mutex
	receivers      map[*Receiver]struct{}
	latestKeyframe *AccessUnit
	chanSize       int
	maxAUBytes     int64 // per-AU size ceiling; 0 = unlimited

	// Replay ring. replayBuf is a slice grown lazily up to replayCap;
	// once at capacity it operates as a circular buffer where
	// replayHead is the index of the next write slot. replayBytes
	// tracks the total bytes currently held; appends past
	// replayMaxBytes evict the oldest entry until the budget fits.
	replayBuf      []AccessUnit
	replayHead     int
	replayCap      int
	replayBytes    int64
	replayMaxBytes int64
}

// NewBroadcaster constructs a fresh Broadcaster. chanSize sets the
// per-subscriber buffer depth; <=0 picks 30 frames (~1s at 30fps).
// Per-AU byte cap defaults to 16 MiB (the same MaxFeedPayload the
// wire layer enforces, so any AU that fits the wire fits memory).
// Replay buffer defaults to 256 AUs.
func NewBroadcaster(chanSize int) *Broadcaster {
	return NewBroadcasterWithBytes(chanSize, 16<<20)
}

// DefaultReplayCap is the default size of the per-broadcaster replay
// ring. At 100 Hz this covers ~2.5 seconds of history — plenty to
// recover AUs in flight at disconnect for any reasonable network RTT.
const DefaultReplayCap = 256

// DefaultReplayMaxBytes caps the total bytes the replay ring may hold.
// Without this, a misbehaving publisher pushing 16 MiB AUs (the wire
// max) at default cap would hold up to 4 GiB. 64 MiB is a sane
// default: comfortably covers 256 typical token / control AUs (a few
// MiB at most), and 4 keyframes of 16 MiB each for video. Operators
// streaming larger payloads should tune via SetReplayMaxBytes.
const DefaultReplayMaxBytes = 64 << 20

// SetReplayCap reconfigures the broadcaster's replay-ring capacity.
// Shrinking discards the oldest entries; growing keeps existing entries.
// Pass 0 to disable the replay buffer entirely. Safe to call before
// any Publish; calling concurrently with Publish is also safe but may
// race over which AUs land in the resized buffer.
func (b *Broadcaster) SetReplayCap(cap int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if cap < 0 {
		cap = 0
	}
	if cap == b.replayCap {
		return
	}
	// Snapshot current contents in chronological order, then re-fill
	// the resized ring through the normal append path so byte tracking
	// stays consistent.
	current := b.replaySnapshotLocked()
	b.replayCap = cap
	b.replayBuf = nil
	b.replayHead = 0
	b.replayBytes = 0
	if cap == 0 {
		return
	}
	if len(current) > cap {
		current = current[len(current)-cap:]
	}
	for _, au := range current {
		b.replayAppendLocked(au)
	}
}

// NewBroadcasterWithBytes is NewBroadcaster with an explicit per-AU
// byte cap. AUs larger than maxAUBytes are dropped at Publish time
// — protects against buggy or malicious publishers exhausting
// receiver memory. Combined with chanSize, gives a hard memory
// upper bound of (chanSize × maxAUBytes) per receiver.
//
// Set maxAUBytes <= 0 to disable the per-AU cap.
func NewBroadcasterWithBytes(chanSize int, maxAUBytes int64) *Broadcaster {
	if chanSize <= 0 {
		chanSize = 30
	}
	return &Broadcaster{
		receivers:      make(map[*Receiver]struct{}),
		chanSize:       chanSize,
		maxAUBytes:     maxAUBytes,
		replayCap:      DefaultReplayCap,
		replayMaxBytes: DefaultReplayMaxBytes,
	}
}

// SetReplayMaxBytes reconfigures the replay-ring byte ceiling. The
// ring evicts the oldest entries on append when the budget is
// exceeded, regardless of slot count. Set to 0 to disable byte-based
// eviction (slot-count-only). Safe to call concurrently with Publish.
func (b *Broadcaster) SetReplayMaxBytes(n int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n < 0 {
		n = 0
	}
	b.replayMaxBytes = n
	// Trim to budget if shrinking.
	for b.replayMaxBytes > 0 && b.replayBytes > b.replayMaxBytes && len(b.replayBuf) > 0 {
		b.replayEvictOldestLocked()
	}
}

// replayAppendLocked appends an AU to the replay ring. Caller holds b.mu.
// Evicts oldest entries to maintain both the slot-count cap (replayCap)
// and the byte-budget cap (replayMaxBytes).
func (b *Broadcaster) replayAppendLocked(au AccessUnit) {
	if b.replayCap <= 0 {
		return
	}
	auBytes := int64(len(au.Bytes))
	// Evict oldest entries while byte budget would be exceeded.
	for b.replayMaxBytes > 0 && len(b.replayBuf) > 0 && b.replayBytes+auBytes > b.replayMaxBytes {
		b.replayEvictOldestLocked()
	}
	if len(b.replayBuf) < b.replayCap {
		b.replayBuf = append(b.replayBuf, au)
		b.replayBytes += auBytes
		return
	}
	// Slot-count cap hit: overwrite the oldest entry (at replayHead).
	old := b.replayBuf[b.replayHead]
	b.replayBytes -= int64(len(old.Bytes))
	b.replayBuf[b.replayHead] = au
	b.replayBytes += auBytes
	b.replayHead = (b.replayHead + 1) % b.replayCap
}

// replayEvictOldestLocked removes the oldest entry from the ring.
// Caller holds b.mu and must check len(b.replayBuf) > 0.
func (b *Broadcaster) replayEvictOldestLocked() {
	n := len(b.replayBuf)
	if n == 0 {
		return
	}
	// Oldest is at replayHead when wrapped, index 0 when not.
	var oldestIdx int
	if n < b.replayCap {
		oldestIdx = 0
	} else {
		oldestIdx = b.replayHead
	}
	b.replayBytes -= int64(len(b.replayBuf[oldestIdx].Bytes))
	// Shrink by removing the oldest entry. If we haven't wrapped yet,
	// just slice; if wrapped, shift the head forward and shrink.
	if n < b.replayCap {
		b.replayBuf = b.replayBuf[1:]
	} else {
		// Move oldest out by overwriting with the next-oldest cycle
		// pattern: simpler to just shift everything into a fresh slice.
		snap := b.replaySnapshotLocked()
		// snap is in chronological order; drop the head.
		if len(snap) > 1 {
			b.replayBuf = snap[1:]
		} else {
			b.replayBuf = nil
		}
		b.replayHead = 0
	}
	if b.replayBytes < 0 {
		b.replayBytes = 0
	}
}

// replaySnapshotLocked returns the replay ring's contents in chronological
// (publish) order, NOT sorted by Seq. Caller holds b.mu.
func (b *Broadcaster) replaySnapshotLocked() []AccessUnit {
	n := len(b.replayBuf)
	if n == 0 {
		return nil
	}
	out := make([]AccessUnit, 0, n)
	if n < b.replayCap {
		// Buffer hasn't wrapped; entries 0..n-1 are in order.
		out = append(out, b.replayBuf...)
		return out
	}
	// Wrapped: start at replayHead (oldest entry).
	for i := 0; i < n; i++ {
		out = append(out, b.replayBuf[(b.replayHead+i)%n])
	}
	return out
}

// ReplaySince returns the AUs in the replay buffer with Seq >= fromSeq,
// sorted ascending by Seq. Returns (nil, true) if no buffered AUs match
// but the buffer contains an entry with Seq < fromSeq (i.e. the caller
// is up-to-date). Returns (nil, false) if the requested seq is older
// than the oldest buffered entry — the caller has fallen too far
// behind and the server should treat the resume as a fresh subscribe.
func (b *Broadcaster) ReplaySince(fromSeq uint32) ([]AccessUnit, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.replayCap <= 0 || len(b.replayBuf) == 0 {
		// No buffer, nothing to replay; treat as up-to-date.
		return nil, true
	}
	snap := b.replaySnapshotLocked()
	// Find the oldest Seq in the buffer to detect underrun.
	minSeq := snap[0].Seq
	for _, au := range snap[1:] {
		if au.Seq < minSeq {
			minSeq = au.Seq
		}
	}
	if fromSeq < minSeq {
		// Caller's last-seen seq is older than anything we still hold;
		// resume range unavailable.
		return nil, false
	}
	out := make([]AccessUnit, 0, len(snap))
	for _, au := range snap {
		if au.Seq >= fromSeq {
			out = append(out, au)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, true
}

// Subscribe registers a new receiver. If a recent keyframe is cached
// it's enqueued immediately so the new viewer can start decoding
// without waiting for the next natural IDR.
func (b *Broadcaster) Subscribe() *Receiver {
	return b.subscribeWithCap(b.chanSize)
}

// SubscribeWithCapacity is Subscribe with an explicit channel buffer
// depth. Used for parked sessions where the receiver must hold both
// replay-buffer contents and the live AUs that arrive during the
// disconnect window; the standard chanSize may be too small.
func (b *Broadcaster) SubscribeWithCapacity(chanSize int) *Receiver {
	if chanSize < b.chanSize {
		chanSize = b.chanSize
	}
	return b.subscribeWithCap(chanSize)
}

func (b *Broadcaster) subscribeWithCap(chanSize int) *Receiver {
	r := &Receiver{
		ch: make(chan AccessUnit, chanSize),
	}
	b.mu.Lock()
	b.receivers[r] = struct{}{}
	r.mu.Lock()
	if b.latestKeyframe != nil {
		// Channel is fresh and at least size 1, so the send never blocks.
		r.ch <- *b.latestKeyframe
	} else {
		r.needKeyframe = true
	}
	r.mu.Unlock()
	b.mu.Unlock()
	return r
}

// SpliceFront re-orders the receiver's pending AUs so prefix entries
// land at the head of the read order. Drains the current channel
// contents into a slice, merges with prefix (dedup by Seq), sorts
// ascending by Seq, and refills the channel. The receiver's channel
// capacity must accommodate len(prefix) + currently-buffered AUs;
// callers should subscribe via SubscribeWithCapacity when they intend
// to splice.
//
// The merge prefers existing channel entries on Seq collision (the
// channel's copy carries the up-to-date keyframe flag). prefix is
// expected to be in ascending Seq order but is sorted defensively.
//
// MUST NOT be called concurrently with a goroutine draining
// Frames() — in practice, this is called during resume handshake,
// before the per-session pumps for the resumed receivers are started.
func (r *Receiver) SpliceFront(prefix []AccessUnit) {
	if len(prefix) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	// Non-blocking drain of whatever's currently buffered.
	current := make([]AccessUnit, 0, len(r.ch))
drain:
	for {
		select {
		case au := <-r.ch:
			current = append(current, au)
		default:
			break drain
		}
	}
	// Merge prefix + current with dedup by Seq. current wins on collision.
	bySeq := make(map[uint32]AccessUnit, len(prefix)+len(current))
	for _, au := range prefix {
		bySeq[au.Seq] = au
	}
	for _, au := range current {
		bySeq[au.Seq] = au
	}
	merged := make([]AccessUnit, 0, len(bySeq))
	for _, au := range bySeq {
		merged = append(merged, au)
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Seq < merged[j].Seq })
	// Refill in seq order. Channel capacity should hold all merged
	// entries; if it doesn't (caller used the default Subscribe size
	// for a parked receiver), drop oldest non-keyframe to make room.
	for _, au := range merged {
		select {
		case r.ch <- au:
		default:
			// Channel full — make room by evicting one buffered AU
			// (oldest read first via non-blocking receive).
			select {
			case <-r.ch:
			default:
			}
			select {
			case r.ch <- au:
			default:
				// Pathological: even after eviction we can't enqueue.
				// Stop the merge — caller didn't size the channel.
				r.needKeyframe = true
				return
			}
		}
	}
}

// Unsubscribe removes a receiver and closes its channel. Idempotent.
//
// The channel is closed under r.mu (NOT under b.mu) so that any
// in-flight Publish iteration holding r.mu — about to send on r.ch —
// finishes (or sees r.closed and skips) before the close happens.
func (b *Broadcaster) Unsubscribe(r *Receiver) {
	b.mu.Lock()
	_, present := b.receivers[r]
	if present {
		delete(b.receivers, r)
	}
	b.mu.Unlock()
	if !present {
		return
	}
	r.mu.Lock()
	if !r.closed {
		r.closed = true
		close(r.ch)
	}
	r.mu.Unlock()
}

// Count returns the current subscriber count.
func (b *Broadcaster) Count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.receivers)
}

// Publish fans one access unit out to every subscriber. The caller
// must not mutate au.Bytes after Publish — the same slice is shared
// with every receiver.
//
// We snapshot the receiver set under b.mu and release it before
// iterating, so Subscribe / Unsubscribe / Count never block the
// per-frame fanout from another goroutine.
func (b *Broadcaster) Publish(au AccessUnit) {
	// Per-AU size cap: drop oversized AUs before they enter any
	// receiver's queue. This bounds memory at chanSize × maxAUBytes
	// per receiver. Default cap matches wire.MaxFeedPayload (16 MiB)
	// so anything that fits the wire fits memory.
	b.mu.Lock()
	cap := b.maxAUBytes
	if cap > 0 && int64(len(au.Bytes)) > cap {
		b.mu.Unlock()
		return
	}
	if au.Keyframe {
		// Cache by value-of-pointer: defensively copy the AU header so
		// a future caller mutation can't poison late joiners. Bytes
		// stays shared (caller-immutable contract).
		k := au
		b.latestKeyframe = &k
	}
	// Record every published AU into the replay ring so disconnect-time
	// in-flight AUs can be recovered on resume.
	b.replayAppendLocked(au)
	receivers := make([]*Receiver, 0, len(b.receivers))
	for r := range b.receivers {
		receivers = append(receivers, r)
	}
	b.mu.Unlock()

	for _, r := range receivers {
		r.mu.Lock()
		if r.closed {
			// Unsubscribe ran between snapshot and now. Skip — the
			// channel is closed and any send would panic.
			r.mu.Unlock()
			continue
		}
		if r.needKeyframe && !au.Keyframe {
			r.mu.Unlock()
			continue
		}
		select {
		case r.ch <- au:
			if au.Keyframe {
				r.needKeyframe = false
			}
		default:
			// Subscriber's channel is full.
			if au.Keyframe {
				// Try to evict one frame to make room — keyframes are
				// too costly to drop (next IDR may be 1+ second away).
				select {
				case <-r.ch:
				default:
				}
				select {
				case r.ch <- au:
					r.needKeyframe = false
				default:
					// Still full → genuinely can't keep up. Mark for
					// resync; a future keyframe will retry.
					r.needKeyframe = true
				}
			} else {
				r.needKeyframe = true
			}
		}
		r.mu.Unlock()
	}
}

// Close releases all receivers — every Receiver.Frames() channel is
// closed and no further Publish calls deliver AUs. Idempotent. Used
// by Server.RemoveTrack to signal per-session pumps to drain.
func (b *Broadcaster) Close() {
	b.mu.Lock()
	receivers := make([]*Receiver, 0, len(b.receivers))
	for r := range b.receivers {
		receivers = append(receivers, r)
		delete(b.receivers, r)
	}
	b.mu.Unlock()
	for _, r := range receivers {
		r.mu.Lock()
		if !r.closed {
			r.closed = true
			close(r.ch)
		}
		r.mu.Unlock()
	}
}

// LatestKeyframe returns the most recently published keyframe, or nil
// if none has been published yet. Useful for tests and for backends
// that want to seed a new stream.
func (b *Broadcaster) LatestKeyframe() *AccessUnit {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.latestKeyframe == nil {
		return nil
	}
	k := *b.latestKeyframe
	return &k
}
