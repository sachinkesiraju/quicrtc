/**
 * Wire-protocol round-trip tests.
 *
 * These tests cover the paths that the prior version got wrong and
 * that a happy-path "value=1234567890 round-trips by accident" smoke
 * test missed:
 *   • PTS values past 2^32 µs (BigInt encoding)
 *   • Sequence numbers past 2^31 (uint32 sign)
 *   • BufferedReader EOF semantics (must throw on short read)
 *   • peekStreamHeader legacy fallback (unread byte, parse next frame)
 *   • JSON field names matching Go's `json:` tags (ver, session,
 *     track, from_seq, level)
 *   • Oversize payloads at all three caps
 */

import {
  BufferedReader,
  DatagramTooLargeError,
  FrameTooLargeError,
  decodeDatagram,
  encodeDatagram,
  marshalAnnounce,
  marshalBackpressure,
  marshalHello,
  marshalKindStats,
  marshalResume,
  marshalSDP,
  marshalUnannounce,
  peekStreamHeader,
  readControlFrame,
  readFeedFrame,
  unmarshalAnnounce,
  unmarshalBackpressure,
  unmarshalError,
  unmarshalHello,
  unmarshalKindStats,
  unmarshalResume,
  unmarshalSDP,
  unmarshalUnannounce,
  writeControlFrame,
  writeFeedFrame,
  writeStreamHeader,
} from './wire.js';
import {
  Announce,
  Backpressure,
  FrameType,
  Hello,
  KindStats,
  Resume,
  SDP,
  Unannounce,
} from './types.js';
import { KindStatsCollector, percentile } from './kindstats.js';

// ============================================================================
// Test helpers
// ============================================================================

/**
 * mockReader builds a ReadableStreamDefaultReader<Uint8Array> from a
 * fixed byte buffer. Returns the buffer in one chunk on first read,
 * then signals done forever — matches a one-shot stream's behavior.
 */
function mockReader(data: Uint8Array): ReadableStreamDefaultReader<Uint8Array> {
  let delivered = false;
  const stream = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (!delivered) {
        controller.enqueue(data);
        delivered = true;
      } else {
        controller.close();
      }
    },
  });
  return stream.getReader();
}

/**
 * splitReader is like mockReader but yields the buffer in
 * `chunkSize`-byte slices, exercising BufferedReader's combine-and-
 * retry path. Real WebTransport streams may return arbitrary chunks
 * per read, so any wire decoder MUST tolerate this.
 */
function splitReader(data: Uint8Array, chunkSize: number): ReadableStreamDefaultReader<Uint8Array> {
  let offset = 0;
  const stream = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (offset >= data.length) {
        controller.close();
        return;
      }
      const end = Math.min(offset + chunkSize, data.length);
      controller.enqueue(data.slice(offset, end));
      offset = end;
    },
  });
  return stream.getReader();
}

/**
 * splitReaderWithGaps yields `chunkSize`-byte slices interleaved with
 * zero-length chunks, exercising the rare-but-legal case where the
 * underlying reader returns {value: empty, done: false}.
 */
function splitReaderWithGaps(data: Uint8Array, chunkSize: number): ReadableStreamDefaultReader<Uint8Array> {
  let offset = 0;
  let emitGap = false;
  const stream = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (offset >= data.length) {
        controller.close();
        return;
      }
      if (emitGap) {
        controller.enqueue(new Uint8Array(0));
        emitGap = false;
        return;
      }
      const end = Math.min(offset + chunkSize, data.length);
      controller.enqueue(data.slice(offset, end));
      offset = end;
      emitGap = true;
    },
  });
  return stream.getReader();
}

let pass = 0;
let fail = 0;

async function test(name: string, fn: () => Promise<void> | void): Promise<void> {
  try {
    await fn();
    pass++;
    console.log(`  ✓ ${name}`);
  } catch (err) {
    fail++;
    console.error(`  ✗ ${name}`);
    console.error(`    ${(err as Error).message}`);
  }
}

function assertEqual<T>(got: T, want: T, msg: string): void {
  if (got !== want) {
    throw new Error(`${msg}: got ${String(got)}, want ${String(want)}`);
  }
}

// ============================================================================
// Tests
// ============================================================================

async function run() {
  console.log('=== JSON field-name regression tests ===');

  await test('Hello uses Go json tags (ver, session, NOT version, sessionId)', () => {
    const h: Hello = {
      role: 'recv',
      slug: 'mysecret',
      ver: '1',
      session: 'sess-abc',
      features: ['resume', 'backpressure'],
      last_seen: { tokens: 42, video: 18 },
    };
    const json = JSON.parse(new TextDecoder().decode(marshalHello(h)));
    assertEqual(json.ver, '1', 'Hello must serialize "ver" not "version"');
    assertEqual(json.session, 'sess-abc', 'Hello must serialize "session" not "sessionId"');
    assertEqual(json.role, 'recv', 'role unchanged');
    assertEqual(json.slug, 'mysecret', 'slug unchanged');
    assertEqual(json.last_seen?.tokens, 42, 'last_seen tokens round-trip');
    assertEqual(json.last_seen?.video, 18, 'last_seen video round-trip');
    if ('version' in json) throw new Error('Hello has stray "version" field');
    if ('sessionId' in json) throw new Error('Hello has stray "sessionId" field');
    if ('lastSeen' in json) throw new Error('Hello has stray camelCase "lastSeen"');
  });

  await test('SDP unmarshals Go-side "session" key', () => {
    // Simulate exactly what the Go server emits.
    const goJson = JSON.stringify({
      codec: 'test',
      width: 64,
      height: 64,
      fps: 30,
      session: 'srv-xyz',
      features: ['resume'],
    });
    const sdp = unmarshalSDP(new TextEncoder().encode(goJson));
    assertEqual(sdp.session, 'srv-xyz', 'session ID round-trips from Go-side json');
    assertEqual(sdp.features?.length ?? 0, 1, 'features round-trip');
  });

  await test('Resume uses snake_case from_seq', () => {
    const r: Resume = { track: 'video', from_seq: 12345 };
    const json = JSON.parse(new TextDecoder().decode(marshalResume(r)));
    assertEqual(json.track, 'video', 'Resume must serialize "track"');
    assertEqual(json.from_seq, 12345, 'Resume must serialize "from_seq"');
    if ('trackName' in json) throw new Error('Resume has stray "trackName" field');
    if ('fromSeq' in json) throw new Error('Resume has stray "fromSeq" field');
  });

  await test('Backpressure uses Go json tags (track, level)', () => {
    const bp: Backpressure = { track: 'tokens', level: 75 };
    const json = JSON.parse(new TextDecoder().decode(marshalBackpressure(bp)));
    assertEqual(json.track, 'tokens', 'Backpressure must serialize "track"');
    assertEqual(json.level, 75, 'Backpressure must serialize "level"');
    if ('trackName' in json) throw new Error('Backpressure has stray "trackName" field');
  });

  await test('Backpressure round-trip preserves session-level (omitempty track)', () => {
    const bp: Backpressure = { level: 50 };
    const data = marshalBackpressure(bp);
    const got = unmarshalBackpressure(data);
    assertEqual(got.level, 50, 'level round-trips');
    // omitempty: track may be undefined or absent; either is fine.
  });

  await test('Backpressure carries needs_keyframe (v1.1 keyframe-request piggyback)', () => {
    const bp: Backpressure = { track: 'screen', level: 100, needs_keyframe: true };
    const json = JSON.parse(new TextDecoder().decode(marshalBackpressure(bp)));
    assertEqual(json.needs_keyframe, true, 'needs_keyframe serializes with snake_case key');
    const rt = unmarshalBackpressure(marshalBackpressure(bp));
    assertEqual(rt.needs_keyframe, true, 'needs_keyframe round-trips');
    assertEqual(rt.track, 'screen', 'track round-trips with needs_keyframe');
    assertEqual(rt.level, 100, 'level round-trips with needs_keyframe');
  });

  await test('Datagram envelope encode/decode round-trip', () => {
    const payload = new Uint8Array([1, 2, 3, 4, 5, 0xff, 0x00, 0xaa]);
    const enc = encodeDatagram(0x42, 0x1337, payload);
    assertEqual(enc[0], 0x24, 'envelope type byte = TypeDatagramAU');
    assertEqual(enc[1], 0x42, 'envelope trackId');
    assertEqual(enc[2], 0x13, 'envelope seq high byte (BE)');
    assertEqual(enc[3], 0x37, 'envelope seq low byte (BE)');
    assertEqual(enc.length, 4 + payload.length, 'total length = 4 + payload');

    const dec = decodeDatagram(enc);
    assertEqual(dec.trackId, 0x42, 'decoded trackId');
    assertEqual(dec.seq, 0x1337, 'decoded seq');
    assertEqual(dec.payload.length, payload.length, 'payload length');
    for (let i = 0; i < payload.length; i++) {
      if (dec.payload[i] !== payload[i]) {
        throw new Error(`payload byte ${i}: got ${dec.payload[i]}, want ${payload[i]}`);
      }
    }

    // Oversize → throws DatagramTooLargeError.
    let threw = false;
    try {
      encodeDatagram(0, 0, new Uint8Array(2000));
    } catch (e) {
      threw = e instanceof DatagramTooLargeError;
    }
    if (!threw) throw new Error('expected DatagramTooLargeError on oversize payload');
  });

  await test('Announce / Unannounce round-trip', () => {
    const a: Announce = { name: 'video', kind: 'video', codec: 'avc1.42E01F' };
    const ar = unmarshalAnnounce(marshalAnnounce(a));
    assertEqual(ar.name, 'video', 'announce name');
    assertEqual(ar.kind, 'video', 'announce kind');

    const u: Unannounce = { name: 'video' };
    const ur = unmarshalUnannounce(marshalUnannounce(u));
    assertEqual(ur.name, 'video', 'unannounce name');
  });

  console.log('\n=== Control frame tests ===');

  await test('Control frame round-trip via real ReadableStream', async () => {
    const payload = new TextEncoder().encode('hello world');
    const frame = writeControlFrame(FrameType.Hello, payload);
    const reader = new BufferedReader(mockReader(frame));
    const got = await readControlFrame(reader);
    assertEqual(got.type, FrameType.Hello, 'type');
    assertEqual(new TextDecoder().decode(got.payload), 'hello world', 'payload');
  });

  await test('Control frame survives byte-by-byte stream chunking', async () => {
    // Real WebTransport streams may return ANY number of bytes per
    // read(). The previous wrap-and-discard pattern would lose
    // over-read bytes; this test forces the combine-and-retry path.
    const payload = new TextEncoder().encode('chunked test');
    const frame = writeControlFrame(FrameType.SDP, payload);
    const reader = new BufferedReader(splitReader(frame, 1));
    const got = await readControlFrame(reader);
    assertEqual(got.type, FrameType.SDP, 'type');
    assertEqual(new TextDecoder().decode(got.payload), 'chunked test', 'payload');
  });

  await test('Control frame oversize throws FrameTooLargeError', () => {
    const huge = new Uint8Array(64 * 1024 + 1); // 1 byte past cap
    let threw = false;
    try {
      writeControlFrame(FrameType.Hello, huge);
    } catch (e) {
      if (e instanceof FrameTooLargeError) threw = true;
    }
    if (!threw) throw new Error('expected FrameTooLargeError');
  });

  console.log('\n=== Feed frame tests (BigInt PTS / uint32 seq) ===');

  await test('Feed frame round-trip with PTS past 2^32 µs', async () => {
    // ~71 minutes of microseconds. The previous 32-bit bitwise impl
    // silently truncated this to its low 32 bits. BigInt path must
    // round-trip exactly.
    const pts = 5_000_000_000n; // 5e9 µs ≈ 83 min — exceeds uint32
    const seq = 0xDEADBEEF >>> 0; // top bit set; sign-aliasing trap
    const payload = new Uint8Array([1, 2, 3, 4, 5]);
    const frame = writeFeedFrame(FrameType.Keyframe, pts, seq, 1, payload);
    const reader = new BufferedReader(mockReader(frame));
    const got = await readFeedFrame(reader);
    assertEqual(got.type, FrameType.Keyframe, 'type');
    if (got.ptsMicro !== pts) {
      throw new Error(`PTS round-trip failed: got ${got.ptsMicro}, want ${pts}`);
    }
    if (got.seq !== seq) {
      throw new Error(`seq round-trip failed: got ${got.seq}, want ${seq}`);
    }
    assertEqual(got.flags, 1, 'flags');
    assertEqual(got.payload.length, 5, 'payload length');
  });

  await test('Feed frame round-trip with realistic Date.now() PTS', async () => {
    // Date.now() * 1000 is ~1.7e15 µs in 2026 — way past uint32. This
    // is the value the examples use. The previous bitwise impl
    // corrupted it on every call.
    const pts = BigInt(Date.now()) * 1000n;
    const frame = writeFeedFrame(FrameType.PFrame, pts, 1, 0, new Uint8Array(10));
    const reader = new BufferedReader(mockReader(frame));
    const got = await readFeedFrame(reader);
    if (got.ptsMicro !== pts) {
      throw new Error(`Date.now()-scale PTS round-trip failed: got ${got.ptsMicro}, want ${pts}`);
    }
  });

  await test('Feed frame survives byte-level stream chunking', async () => {
    const pts = 1234567890n;
    const payload = new Uint8Array(1000).fill(0xab);
    const frame = writeFeedFrame(FrameType.Frame, pts, 100, 0, payload);
    const reader = new BufferedReader(splitReader(frame, 7));
    const got = await readFeedFrame(reader);
    assertEqual(got.ptsMicro === pts, true, 'pts');
    assertEqual(got.payload.length, 1000, 'payload length');
  });

  await test('Feed frame parses publish wall-clock extension', async () => {
    // The Go server writes header(17) + 8B BE wall + payload when
    // FlagPublishWall (1<<2) is set. Build that layout by hand since
    // the TS SDK only ever reads it (subscriber side).
    const payload = new Uint8Array([0x67, 0x42]);
    const wall = 1_700_000_000_000_000n; // Unix micros
    const frame = new Uint8Array(17 + 8 + payload.length);
    const view = new DataView(frame.buffer);
    frame[0] = FrameType.Keyframe;
    frame[1] = (payload.length >> 16) & 0xff;
    frame[2] = (payload.length >> 8) & 0xff;
    frame[3] = payload.length & 0xff;
    view.setBigUint64(4, 9999n, false); // pts
    view.setUint32(12, 7, false); // seq
    frame[16] = (1 << 0) | (1 << 2); // Keyframe | PublishWall
    view.setBigUint64(17, wall, false);
    frame.set(payload, 25);

    const reader = new BufferedReader(mockReader(frame));
    const got = await readFeedFrame(reader);
    assertEqual(got.seq, 7, 'seq');
    assertEqual(got.pubWallMicro, wall, 'pubWallMicro');
    assertEqual(got.payload.length, 2, 'payload length');
  });

  await test('Feed frame without wall flag yields undefined pubWallMicro', async () => {
    const frame = writeFeedFrame(FrameType.PFrame, 1n, 2, 0, new Uint8Array([9]));
    const reader = new BufferedReader(mockReader(frame));
    const got = await readFeedFrame(reader);
    assertEqual(got.pubWallMicro, undefined, 'pubWallMicro undefined for v1 frame');
  });

  console.log('\n=== kind-stats collector ===');

  await test('collector computes p50/p99 and resets window', async () => {
    const c = new KindStatsCollector();
    const pubMs = 1_000_000; // 1000s in ms-from-micros base
    const pubMicro = BigInt(pubMs) * 1000n;
    for (let i = 0; i < 99; i++) c.observe('video', i + 1, pubMicro, pubMs + 10); // 10ms
    c.observe('video', 100, pubMicro, pubMs + 200); // 200ms outlier
    const snaps = c.snapshot();
    assertEqual(snaps.length, 1, 'one kind');
    assertEqual(snaps[0].kind, 'video', 'kind');
    assertEqual(snaps[0].recv_p50_ms, 10, 'p50');
    assertEqual(snaps[0].recv_p99_ms, 10, 'p99 below outlier rank');
    assertEqual(snaps[0].last_seq, 100, 'last_seq');
    // Window resets; cumulative last_seq persists.
    const second = c.snapshot();
    assertEqual(second[0].recv_p50_ms, 0, 'p50 resets');
    assertEqual(second[0].last_seq, 100, 'last_seq persists');
  });

  await test('collector counts seq-gap drops', async () => {
    const c = new KindStatsCollector();
    c.observe('data', 1, undefined, Date.now());
    c.observe('data', 2, undefined, Date.now());
    c.observe('data', 5, undefined, Date.now()); // +2
    c.observe('data', 10, undefined, Date.now()); // +4
    const snaps = c.snapshot();
    assertEqual(snaps[0].dropped ?? 0, 6, 'dropped');
    assertEqual(snaps[0].last_seq, 10, 'last_seq');
    assertEqual(snaps[0].recv_p50_ms, 0, 'no wall -> zero p50');
  });

  await test('percentile nearest-rank', async () => {
    const s = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    assertEqual(percentile(s, 0.5), 6, 'p50');
    assertEqual(percentile(s, 0.99), 10, 'p99');
    assertEqual(percentile([42], 0.5), 42, 'single');
  });

  console.log('\n=== Stream header + legacy fallback ===');

  await test('Stream header round-trip', async () => {
    const frame = writeStreamHeader('actions');
    const reader = new BufferedReader(mockReader(frame));
    const got = await peekStreamHeader(reader);
    assertEqual(got.trackName, 'actions', 'trackName');
  });

  await test('peekStreamHeader legacy fallback unreads non-header byte', async () => {
    // Simulate a legacy single-track stream: starts with TypeKeyframe
    // (0x20), NOT TypeStreamHeader (0x23). peekStreamHeader must
    // unread that byte so the next ReadFeedFrame consumes it as the
    // type byte. Without the unread, the feed-frame parse misaligns
    // and silently corrupts.
    const pts = 999999n;
    const payload = new Uint8Array([10, 20, 30]);
    const frame = writeFeedFrame(FrameType.Keyframe, pts, 7, 1, payload);
    const reader = new BufferedReader(mockReader(frame));

    const { trackName } = await peekStreamHeader(reader);
    assertEqual(trackName, '', 'legacy stream → empty trackName');

    // Now read the feed frame from the same reader. The first byte
    // (TypeKeyframe) must still be there.
    const got = await readFeedFrame(reader);
    assertEqual(got.type, FrameType.Keyframe, 'type still parseable after unread');
    assertEqual(got.ptsMicro === pts, true, 'pts');
    assertEqual(got.seq, 7, 'seq');
  });

  console.log('\n=== BufferedReader EOF semantics ===');

  await test('BufferedReader.read throws on short EOF', async () => {
    // Stream contains 3 bytes; ask for 10. Must throw, not return short.
    const reader = new BufferedReader(mockReader(new Uint8Array([1, 2, 3])));
    let threw = false;
    try {
      await reader.read(10);
    } catch (e) {
      threw = true;
    }
    if (!threw) throw new Error('expected error on short EOF');
  });

  await test('BufferedReader handles over-read bytes correctly', async () => {
    // Single read returns 100 bytes; we ask for 10, then 50, then 40.
    // All from the same buffer — the over-read MUST be retained.
    const buf = new Uint8Array(100);
    for (let i = 0; i < 100; i++) buf[i] = i;
    const reader = new BufferedReader(mockReader(buf));

    const a = await reader.read(10);
    assertEqual(a.length, 10, 'first read length');
    assertEqual(a[0], 0, 'first byte');
    assertEqual(a[9], 9, 'tenth byte');

    const b = await reader.read(50);
    assertEqual(b.length, 50, 'second read length');
    assertEqual(b[0], 10, 'starts at 10');
    assertEqual(b[49], 59, 'ends at 59');

    const c = await reader.read(40);
    assertEqual(c.length, 40, 'third read length');
    assertEqual(c[0], 60, 'starts at 60');
    assertEqual(c[39], 99, 'ends at 99');
  });

  await test('BufferedReader.unread re-prepends bytes', async () => {
    const reader = new BufferedReader(mockReader(new Uint8Array([2, 3, 4])));
    reader.unread(new Uint8Array([0, 1]));
    const got = await reader.read(5);
    assertEqual(got.length, 5, 'unread + read');
    for (let i = 0; i < 5; i++) {
      if (got[i] !== i) throw new Error(`byte[${i}] = ${got[i]}, want ${i}`);
    }
  });

  await test('BufferedReader tolerates zero-length chunks', async () => {
    // Round-trip a feed frame across a reader that interleaves empty
    // chunks between real ones — legal per ReadableStream contract.
    const pts = 1234567n;
    const payload = new Uint8Array(64);
    for (let i = 0; i < 64; i++) payload[i] = i & 0xff;
    const frame = writeFeedFrame(FrameType.Keyframe, pts, 1, 0, payload);
    const reader = new BufferedReader(splitReaderWithGaps(frame, 4));
    const got = await readFeedFrame(reader);
    assertEqual(got.type, FrameType.Keyframe, 'type');
    assertEqual(got.ptsMicro === pts, true, 'pts');
    assertEqual(got.seq, 1, 'seq');
    assertEqual(got.payload.length, 64, 'payload length');
    for (let i = 0; i < 64; i++) {
      if (got.payload[i] !== (i & 0xff)) {
        throw new Error(`payload[${i}] = ${got.payload[i]}, want ${i & 0xff}`);
      }
    }
  });

  await test('BufferedReader handles repeated boundary-spanning leftover', async () => {
    // Build a 1024-byte stream with a known pattern; read it back in
    // alternating small (3-byte) and large (37-byte) chunks against a
    // 1-byte-per-chunk underlying reader. Stresses the leftover-spans-
    // boundary case across many cycles, not just once.
    const total = 1024;
    const buf = new Uint8Array(total);
    for (let i = 0; i < total; i++) buf[i] = (i * 7 + 3) & 0xff;
    const reader = new BufferedReader(splitReader(buf, 1));

    let consumed = 0;
    const sizes = [3, 37];
    let toggle = 0;
    while (consumed < total) {
      const want = Math.min(sizes[toggle++ & 1], total - consumed);
      const got = await reader.read(want);
      assertEqual(got.length, want, `chunk length at offset ${consumed}`);
      for (let i = 0; i < want; i++) {
        const expected = (consumed + i) * 7 + 3;
        if (got[i] !== (expected & 0xff)) {
          throw new Error(
            `byte at stream offset ${consumed + i}: got ${got[i]}, want ${expected & 0xff}`,
          );
        }
      }
      consumed += want;
    }
    assertEqual(consumed, total, 'consumed all bytes');
  });

  await test('BufferedReader.unread does not alias caller buffer', async () => {
    // Caller passes a buffer to unread, then mutates it. The next
    // read must return the ORIGINAL bytes — proves no aliasing.
    const reader = new BufferedReader(mockReader(new Uint8Array([9, 9, 9])));
    const callerBuf = new Uint8Array([0, 1]);
    reader.unread(callerBuf);
    callerBuf[0] = 0xff;
    callerBuf[1] = 0xff;
    const got = await reader.read(2);
    assertEqual(got.length, 2, 'unread length');
    if (got[0] !== 0 || got[1] !== 1) {
      throw new Error(`unread aliased caller buffer: got [${got[0]}, ${got[1]}], want [0, 1]`);
    }
  });

  await test('marshalKindStats / unmarshalKindStats round-trip', async () => {
    const ks: KindStats = {
      kind: 'tokens',
      // eslint-disable-next-line @typescript-eslint/naming-convention
      last_seq: 4242,
      // eslint-disable-next-line @typescript-eslint/naming-convention
      recv_p50_ms: 38,
      // eslint-disable-next-line @typescript-eslint/naming-convention
      recv_p99_ms: 71,
      dropped: 3,
    };
    const round = unmarshalKindStats(marshalKindStats(ks));
    assertEqual(round.kind, 'tokens', 'kind');
    assertEqual(round.last_seq, 4242, 'last_seq');
    assertEqual(round.recv_p50_ms, 38, 'recv_p50_ms');
    assertEqual(round.recv_p99_ms, 71, 'recv_p99_ms');
    assertEqual(round.dropped ?? 0, 3, 'dropped');
  });

  await test('unmarshalError parses JSON envelope', async () => {
    const json = new TextEncoder().encode(JSON.stringify({ code: 'track_unauthorized', reason: 'tenant mismatch' }));
    const ep = unmarshalError(json);
    assertEqual(ep.code, 'track_unauthorized', 'code');
    assertEqual(ep.reason ?? '', 'tenant mismatch', 'reason');
  });

  await test('unmarshalError falls back to plain bytes (legacy)', async () => {
    const legacy = new TextEncoder().encode('auth');
    const ep = unmarshalError(legacy);
    assertEqual(ep.code, '', 'legacy code is empty');
    assertEqual(ep.reason ?? '', 'auth', 'legacy reason carries the bytes');
  });

  await test('unmarshalError handles empty payload', async () => {
    const ep = unmarshalError(new Uint8Array(0));
    assertEqual(ep.code, '', 'empty code');
    assertEqual(ep.reason ?? '', '', 'empty reason');
  });

  console.log('\n=== Summary ===');
  console.log(`${pass} passed, ${fail} failed`);
  if (fail > 0) {
    if (typeof process !== 'undefined' && process.exit) process.exit(1);
    throw new Error(`${fail} tests failed`);
  }
}

run().catch((err) => {
  console.error('test runner error:', err);
  if (typeof process !== 'undefined' && process.exit) process.exit(1);
});
