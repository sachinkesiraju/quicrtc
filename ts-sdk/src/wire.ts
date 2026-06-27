/**
 * Wire protocol implementation for QUICRTC.
 *
 * Binary framing for control frames (bidi control stream) and feed
 * frames (uni feed streams). All multi-byte integers are big-endian.
 * uint64 fields (PTS) and uint32 fields (seq) use DataView's
 * native BigInt API to avoid the 32-bit-bitwise truncation bug that
 * silently corrupts values past 2^32 µs (~71 minutes).
 *
 * Reference: github.com/sachinkesiraju/quicrtc/wire/frame.go
 */

import {
  Announce,
  Backpressure,
  ErrorPayload,
  FrameType,
  Hello,
  KindStats,
  Resume,
  SDP,
  Unannounce,
} from './types.js';

// Caps must match Go's wire/frame.go.
const MAX_CONTROL_PAYLOAD = 64 * 1024;            // 64 KiB
const MAX_FEED_PAYLOAD = 16 * 1024 * 1024;        // 16 MiB
const MAX_STREAM_HEADER = 1024;                    // 1 KiB
const FEED_HEADER_LEN = 17;                        // type(1)+len(3)+pts(8)+seq(4)+flags(1)

/** Thrown when a frame size exceeds the protocol cap. */
export class FrameTooLargeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'FrameTooLargeError';
  }
}

// ============================================================================
// Control frames: [1B type][3B BE length][N bytes payload]
// ============================================================================

export interface ControlFrame {
  type: number;
  payload: Uint8Array;
}

export async function readControlFrame(reader: BufferedReader): Promise<ControlFrame> {
  const header = await reader.read(4);
  const type = header[0];
  const length = (header[1] << 16) | (header[2] << 8) | header[3];

  if (length > MAX_CONTROL_PAYLOAD) {
    throw new FrameTooLargeError(`control payload ${length} > ${MAX_CONTROL_PAYLOAD}`);
  }
  if (length === 0) {
    return { type, payload: new Uint8Array(0) };
  }
  const payload = await reader.read(length);
  return { type, payload };
}

export function writeControlFrame(type: number, payload?: Uint8Array): Uint8Array {
  const data = payload ?? new Uint8Array(0);
  if (data.length > MAX_CONTROL_PAYLOAD) {
    throw new FrameTooLargeError(`control payload ${data.length} > ${MAX_CONTROL_PAYLOAD}`);
  }
  const frame = new Uint8Array(4 + data.length);
  frame[0] = type;
  frame[1] = (data.length >> 16) & 0xff;
  frame[2] = (data.length >> 8) & 0xff;
  frame[3] = data.length & 0xff;
  if (data.length > 0) frame.set(data, 4);
  return frame;
}

// ============================================================================
// Feed frames: [1B type][3B BE length][8B BE pts µs][4B BE seq][1B flags][N bytes payload]
// ============================================================================

export interface FeedFrame {
  type: number;
  /** Presentation timestamp in microseconds; full uint64 range. */
  ptsMicro: bigint;
  /** Monotonic sequence counter; uint32. */
  seq: number;
  /** Flag bits — see FrameFlags. */
  flags: number;
  /**
   * Publisher wall-clock send time in Unix micros, present only when
   * FlagPublishWall (1<<2) is set in flags; undefined otherwise. Used
   * by the "kind-stats" feature to compute publish→recv latency.
   */
  pubWallMicro?: bigint;
  payload: Uint8Array;
}

/** FlagPublishWall mirrors FrameFlags.PublishWall (1<<2) in types.ts. */
const FLAG_PUBLISH_WALL = 1 << 2;
/** Byte length of the optional publish-wall-clock extension. */
const WALL_EXT_LEN = 8;

export async function readFeedFrame(reader: BufferedReader): Promise<FeedFrame> {
  const header = await reader.read(FEED_HEADER_LEN);
  const view = new DataView(header.buffer, header.byteOffset, header.byteLength);

  const type = header[0];
  const length = (header[1] << 16) | (header[2] << 8) | header[3];
  // Use BigInt for PTS — values past 2^32 µs (~71 min) overflow JS
  // Number arithmetic via the bitwise operators that the previous
  // implementation used. DataView.getBigUint64 is correct for the
  // full uint64 range.
  const ptsMicro = view.getBigUint64(4, false /* big-endian */);
  // uint32 seq via DataView; >>> on raw bitwise ops would alias the
  // sign bit when seq >= 2^31.
  const seq = view.getUint32(12, false);
  const flags = header[16];

  if (length > MAX_FEED_PAYLOAD) {
    throw new FrameTooLargeError(`feed payload ${length} > ${MAX_FEED_PAYLOAD}`);
  }
  // Optional publish-wall-clock extension between the fixed header and
  // the payload. Only present when the server set FlagPublishWall,
  // which it does solely for subscribers that negotiated "kind-stats".
  let pubWallMicro: bigint | undefined;
  if ((flags & FLAG_PUBLISH_WALL) !== 0) {
    const ext = await reader.read(WALL_EXT_LEN);
    const extView = new DataView(ext.buffer, ext.byteOffset, ext.byteLength);
    pubWallMicro = extView.getBigUint64(0, false);
  }
  let payload: Uint8Array = new Uint8Array(0);
  if (length > 0) {
    payload = await reader.read(length);
  }
  return { type, ptsMicro, seq, flags, pubWallMicro, payload };
}

export function writeFeedFrame(
  type: number,
  ptsMicro: bigint,
  seq: number,
  flags: number,
  payload?: Uint8Array,
): Uint8Array {
  const data = payload ?? new Uint8Array(0);
  if (data.length > MAX_FEED_PAYLOAD) {
    throw new FrameTooLargeError(`feed payload ${data.length} > ${MAX_FEED_PAYLOAD}`);
  }
  const frame = new Uint8Array(FEED_HEADER_LEN + data.length);
  const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
  frame[0] = type;
  frame[1] = (data.length >> 16) & 0xff;
  frame[2] = (data.length >> 8) & 0xff;
  frame[3] = data.length & 0xff;
  view.setBigUint64(4, ptsMicro, false);
  view.setUint32(12, seq >>> 0, false); // coerce to uint32
  frame[16] = flags & 0xff;
  if (data.length > 0) frame.set(data, FEED_HEADER_LEN);
  return frame;
}

// ============================================================================
// Stream-header frames: first frame on every uni feed stream.
// Layout matches a control frame: [0x23][3B length][N bytes UTF-8 track name]
// ============================================================================

export function writeStreamHeader(trackName: string): Uint8Array {
  const nameBytes = new TextEncoder().encode(trackName);
  if (nameBytes.length > MAX_STREAM_HEADER) {
    throw new FrameTooLargeError(`stream header ${nameBytes.length} > ${MAX_STREAM_HEADER}`);
  }
  const frame = new Uint8Array(4 + nameBytes.length);
  frame[0] = FrameType.StreamHeader;
  frame[1] = (nameBytes.length >> 16) & 0xff;
  frame[2] = (nameBytes.length >> 8) & 0xff;
  frame[3] = nameBytes.length & 0xff;
  frame.set(nameBytes, 4);
  return frame;
}

/**
 * peekStreamHeader looks at the first byte of a uni stream and either
 * (a) parses a complete TypeStreamHeader frame and returns the track
 *     name (and consumes the header from the reader), or
 * (b) detects a non-header type byte (legacy single-track stream),
 *     re-prepends that byte to the reader's internal buffer, and
 *     returns trackName="" so the caller routes to "primary".
 *
 * This matches Go's wire.PeekStreamHeader + the peekedReader adapter
 * used in client.go's drainFeed. Without the unread-byte semantics,
 * the very first feed-frame's type byte is silently consumed and
 * the receiver mis-parses every subsequent feed frame.
 */
export async function peekStreamHeader(reader: BufferedReader): Promise<{
  trackName: string;
  /** Always 0; included for API symmetry with the Go signature. */
  firstByte: number;
}> {
  const firstByteBuf = await reader.read(1);
  const firstByte = firstByteBuf[0];

  if (firstByte !== FrameType.StreamHeader) {
    // Legacy single-track stream — push the byte back so the next
    // ReadFeedFrame consumes it as the type byte.
    reader.unread(firstByteBuf);
    return { trackName: '', firstByte: 0 };
  }

  const lenBuf = await reader.read(3);
  const length = (lenBuf[0] << 16) | (lenBuf[1] << 8) | lenBuf[2];
  if (length > MAX_STREAM_HEADER) {
    throw new FrameTooLargeError(`stream header ${length} > ${MAX_STREAM_HEADER}`);
  }
  if (length === 0) {
    throw new Error('quicrtc/wire: empty stream header');
  }
  const nameBytes = await reader.read(length);
  const trackName = new TextDecoder().decode(nameBytes);
  return { trackName, firstByte: 0 };
}

// ============================================================================
// JSON wire payloads — field names match Go's json: tags exactly.
// JSON.stringify preserves the JS object key names verbatim, so the
// types defined in types.ts MUST use the Go-side names (ver, session,
// track, from_seq, etc.) for the wire to round-trip correctly.
// ============================================================================

const enc = new TextEncoder();
const dec = new TextDecoder();

function marshal<T>(msg: T): Uint8Array {
  return enc.encode(JSON.stringify(msg));
}
function unmarshal<T>(data: Uint8Array): T {
  return JSON.parse(dec.decode(data)) as T;
}

export function marshalHello(msg: Hello): Uint8Array { return marshal(msg); }
export function unmarshalHello(data: Uint8Array): Hello { return unmarshal<Hello>(data); }

export function marshalSDP(msg: SDP): Uint8Array { return marshal(msg); }
export function unmarshalSDP(data: Uint8Array): SDP { return unmarshal<SDP>(data); }

export function marshalAnnounce(msg: Announce): Uint8Array { return marshal(msg); }
export function unmarshalAnnounce(data: Uint8Array): Announce { return unmarshal<Announce>(data); }

export function marshalUnannounce(msg: Unannounce): Uint8Array { return marshal(msg); }
export function unmarshalUnannounce(data: Uint8Array): Unannounce { return unmarshal<Unannounce>(data); }

export function marshalResume(msg: Resume): Uint8Array { return marshal(msg); }
export function unmarshalResume(data: Uint8Array): Resume { return unmarshal<Resume>(data); }

export function marshalBackpressure(msg: Backpressure): Uint8Array { return marshal(msg); }
export function unmarshalBackpressure(data: Uint8Array): Backpressure { return unmarshal<Backpressure>(data); }

export function marshalKindStats(msg: KindStats): Uint8Array { return marshal(msg); }
export function unmarshalKindStats(data: Uint8Array): KindStats { return unmarshal<KindStats>(data); }

/**
 * unmarshalError decodes a TypeError control-frame payload. New
 * (v1.0.2+) servers send JSON ErrorPayload{code, reason}; older
 * servers wrote plain bytes like []byte("auth"). When the payload
 * isn't valid JSON or doesn't start with '{', the bytes are surfaced
 * as {code: "", reason: <bytes>} so callers always get a usable
 * struct.
 */
export function unmarshalError(data: Uint8Array): ErrorPayload {
  if (data.length === 0) {
    return { code: '' };
  }
  if (data[0] !== 0x7b /* '{' */) {
    return { code: '', reason: new TextDecoder().decode(data) };
  }
  try {
    return unmarshal<ErrorPayload>(data);
  } catch {
    return { code: '', reason: new TextDecoder().decode(data) };
  }
}

// ============================================================================
// Datagram envelope: [1B type][1B trackId][2B BE seq][payload]
// 4 bytes total. Used for kind-aware telemetry over WebTransport datagrams.
// Matches Go's wire/datagram.go layout exactly.
// ============================================================================

export class DatagramTooLargeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'DatagramTooLargeError';
  }
}

import { DatagramHeaderLen, TypeDatagramAU, MaxDatagramPayload } from './types.js';

/**
 * Encode a datagram envelope around a payload. Returns the 4-byte
 * header concatenated with the payload, ready to write to
 * WebTransport.datagrams.writable.
 *
 * Throws DatagramTooLargeError if payload exceeds MaxDatagramPayload.
 */
export function encodeDatagram(trackId: number, seq: number, payload: Uint8Array): Uint8Array {
  if (payload.length > MaxDatagramPayload) {
    throw new DatagramTooLargeError(`datagram payload ${payload.length} > ${MaxDatagramPayload}`);
  }
  const out = new Uint8Array(DatagramHeaderLen + payload.length);
  out[0] = TypeDatagramAU;
  out[1] = trackId & 0xff;
  out[2] = (seq >> 8) & 0xff;
  out[3] = seq & 0xff;
  out.set(payload, DatagramHeaderLen);
  return out;
}

export interface DecodedDatagram {
  trackId: number;
  seq: number;
  payload: Uint8Array;
}

/**
 * Decode a datagram received from WebTransport.datagrams.readable.
 * Returns the parsed envelope + payload. Throws on short buffer or
 * unrecognized type byte.
 */
export function decodeDatagram(buf: Uint8Array): DecodedDatagram {
  if (buf.length < DatagramHeaderLen) {
    throw new Error(`quicrtc/wire: datagram too short: ${buf.length}`);
  }
  if (buf[0] !== TypeDatagramAU) {
    throw new Error(`quicrtc/wire: unexpected datagram type 0x${buf[0].toString(16)}`);
  }
  const trackId = buf[1];
  const seq = (buf[2] << 8) | buf[3];
  const payload = buf.subarray(DatagramHeaderLen);
  return { trackId, seq, payload };
}

// ============================================================================
// BufferedReader — the only correct way to read from a WebTransport
// stream in this SDK. Holds a single underlying reader, stashes excess
// bytes from each underlying read(), and supports unread() so
// peekStreamHeader can re-prepend bytes when the legacy fallback
// fires.
//
// NEVER construct a fresh BufferedReader per read(); that pattern
// discards over-read bytes (which is the COMMON case for WebTransport
// streams that can return more bytes per read than the caller asked
// for). One BufferedReader per stream, reuse it for the lifetime of
// the stream.
//
// Allocation invariant: `read(n)` performs exactly ONE Uint8Array
// allocation (the result buffer that the caller owns) regardless of
// how many underlying chunks it takes to fill. Network chunks are
// copied directly into the result; over-read bytes are stashed as a
// `subarray` view of the most recent chunk — zero-copy. This keeps
// the inbound feed path off the GC mark-sweep critical path under
// burst load (e.g., 30 fps screen capture).
//
// Aliasing note: `leftover` is a subarray view into the most recent
// chunk returned by the underlying reader. WebTransport's native
// ReadableStreamDefaultReader and the test splitReader both return
// freshly allocated chunks per read, so this is safe. A BYOB or
// buffer-recycling reader would break this invariant — don't wrap one.
// ============================================================================

export class BufferedReader {
  private leftover: Uint8Array = new Uint8Array(0);
  private done = false;

  constructor(private readonly reader: ReadableStreamDefaultReader<Uint8Array>) {}

  /**
   * Read EXACTLY n bytes. Throws if the stream ends with fewer than n
   * bytes still buffered — matches Go's io.ReadFull semantics. Never
   * returns a short slice; callers can index without bounds-checking.
   */
  async read(n: number): Promise<Uint8Array> {
    const out = new Uint8Array(n);
    let filled = 0;

    if (this.leftover.length > 0) {
      const take = this.leftover.length <= n ? this.leftover.length : n;
      out.set(this.leftover.subarray(0, take), 0);
      filled = take;
      this.leftover = this.leftover.subarray(take);
    }

    while (filled < n) {
      if (this.done) {
        throw new Error(
          `quicrtc/wire: unexpected EOF (wanted ${n} bytes, have ${filled})`,
        );
      }
      const result = await this.reader.read();
      if (result.done) {
        this.done = true;
        throw new Error(
          `quicrtc/wire: unexpected EOF (wanted ${n} bytes, have ${filled})`,
        );
      }
      const chunk = result.value;
      if (chunk.length === 0) continue;

      const need = n - filled;
      if (chunk.length <= need) {
        out.set(chunk, filled);
        filled += chunk.length;
      } else {
        out.set(chunk.subarray(0, need), filled);
        this.leftover = chunk.subarray(need);
        filled = n;
      }
    }
    return out;
  }

  /**
   * Push bytes back to the front of the buffer. Used by peekStreamHeader
   * when the first byte turns out NOT to be a TypeStreamHeader — the
   * legacy single-track fallback needs the byte preserved as the next
   * feed frame's type byte.
   */
  unread(bytes: Uint8Array): void {
    if (bytes.length === 0) return;
    // Copy out of the caller's buffer — they may reuse it.
    const combined = new Uint8Array(bytes.length + this.leftover.length);
    combined.set(bytes, 0);
    combined.set(this.leftover, bytes.length);
    this.leftover = combined;
  }

  releaseLock(): void {
    try {
      this.reader.releaseLock();
    } catch {
      // already released, ignore
    }
    this.leftover = new Uint8Array(0);
  }

  /** Cancel the underlying stream. */
  async cancel(reason?: unknown): Promise<void> {
    try {
      await this.reader.cancel(reason);
    } catch {
      // already cancelled, ignore
    }
  }
}
