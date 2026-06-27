/**
 * QUICRTC TypeScript client.
 *
 * Browser-side counterpart to the Go server in this repo. Wire format
 * is documented in src/wire.ts and matches Go's wire/frame.go +
 * wire/hello.go byte-for-byte / field-for-field.
 *
 * Public surface mirrors the Go client at client/client.go:
 *   QuicRTCClient.connect / addTrack / recv / recvOn / dataChannel /
 *   sessionId / negotiatedFeatures / sendBackpressure / close.
 */

import {
  AccessUnit,
  Announce,
  Backpressure,
  ClientOptions,
  DefaultClientFeatures,
  FrameFlags,
  FrameType,
  Hello,
  KindStats,
  LocalTrack,
  RemoteTrack,
  Resume,
  SDP,
  TrackKind,
  TransportStats,
  Unannounce,
} from './types.js';
import { KindStatsCollector } from './kindstats.js';
import {
  BufferedReader,
  marshalAnnounce,
  marshalBackpressure,
  marshalHello,
  marshalKindStats,
  marshalResume,
  marshalUnannounce,
  peekStreamHeader,
  readControlFrame,
  readFeedFrame,
  unmarshalAnnounce,
  unmarshalBackpressure,
  unmarshalError,
  unmarshalKindStats,
  unmarshalSDP,
  unmarshalUnannounce,
  writeControlFrame,
  writeFeedFrame,
  writeStreamHeader,
} from './wire.js';

/** Client-side error type. */
export class ClientError extends Error {
  constructor(message: string, public readonly code?: string) {
    super(message);
    this.name = 'ClientError';
  }
}

export enum ConnectionState {
  Disconnected = 'disconnected',
  Connecting = 'connecting',
  Connected = 'connected',
  Error = 'error',
}

// =================== Validation ===================

/**
 * validateLocalTrackName — applied to track names the LOCAL caller
 * supplies (publish, recvOn). Rejects anything that could be confused
 * with a path or scheme. Server-supplied names go through a more
 * permissive validator since the Go server may legitimately use names
 * like `room:42/screen` in multi-tenant setups.
 */
function validateLocalTrackName(name: string): void {
  if (!name || typeof name !== 'string') {
    throw new ClientError('track name must be a non-empty string', 'invalid_track_name');
  }
  if (name.length > 256) {
    throw new ClientError('track name too long (max 256 chars)', 'invalid_track_name');
  }
  if (!/^[a-zA-Z0-9_.-]+$/.test(name)) {
    throw new ClientError('track name contains invalid characters', 'invalid_track_name');
  }
}

/**
 * validateRemoteTrackName — applied to track names the SERVER sends
 * (announce, stream-header). More permissive: only enforces the
 * 1024-byte cap from the wire format and rejects null bytes.
 */
function validateRemoteTrackName(name: string): void {
  if (typeof name !== 'string') {
    throw new ClientError('remote track name must be a string', 'invalid_track_name');
  }
  if (name.length === 0 || name.length > 1024) {
    throw new ClientError('remote track name length out of range', 'invalid_track_name');
  }
  if (name.includes('\x00')) {
    throw new ClientError('remote track name contains null byte', 'invalid_track_name');
  }
}

function validateUrl(url: string): void {
  if (!url || typeof url !== 'string') {
    throw new ClientError('URL must be a non-empty string', 'invalid_url');
  }
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    throw new ClientError('invalid URL format', 'invalid_url');
  }
  if (parsed.protocol !== 'https:') {
    throw new ClientError('URL must use https:// (WebTransport requires it)', 'invalid_url');
  }
}

// =================== DataChannel ===================

/**
 * DataChannel is a reliable bidi message channel layered on the
 * control stream's TypeData frame. Same shape as a WebRTC DataChannel.
 */
export class DataChannel {
  private onMessageCallback?: (data: Uint8Array) => void;
  private messageQueue: Uint8Array[] = [];
  private closed = false;

  constructor(private readonly sendCallback: (data: Uint8Array) => Promise<void>) {}

  async send(data: Uint8Array): Promise<void> {
    if (this.closed) throw new ClientError('data channel closed', 'closed');
    await this.sendCallback(data);
  }

  async sendText(text: string): Promise<void> {
    await this.send(new TextEncoder().encode(text));
  }

  onMessage(cb: (data: Uint8Array) => void): void {
    this.onMessageCallback = cb;
    while (this.messageQueue.length > 0) {
      const msg = this.messageQueue.shift()!;
      cb(msg);
    }
  }

  /** Internal: dispatch an inbound message. */
  deliver(data: Uint8Array): void {
    if (this.closed) return;
    if (this.onMessageCallback) {
      this.onMessageCallback(data);
    } else {
      this.messageQueue.push(data);
    }
  }

  close(): void {
    this.closed = true;
    this.onMessageCallback = undefined;
    this.messageQueue = [];
  }
}

// =================== TrackReceiver ===================

/**
 * TrackReceiver is the inbound side of a single track. Two modes:
 *   • Pull: caller awaits recv(). Returns a Promise that resolves
 *           when the next AU arrives. Multiple concurrent recv()
 *           calls are queued FIFO.
 *   • Push: caller registers onAU(cb). Each arriving AU invokes the
 *           callback synchronously.
 *
 * Closing rejects all pending pull-mode promises with EOFError so
 * callers don't hang. Push-mode handlers are simply detached.
 */
export class TrackReceiver {
  private readonly maxQueueSize: number;
  private droppedAUs = 0;

  // Push-mode: at most one callback at a time.
  private onAUCallback?: (au: AccessUnit) => void;

  // Pull-mode state. AUs arriving with no waiter and no callback land
  // in `queue` (capped at maxQueueSize, oldest dropped). Multiple
  // concurrent recv() calls queue their resolvers FIFO.
  private queue: AccessUnit[] = [];
  private waiters: Array<{
    resolve: (au: AccessUnit) => void;
    reject: (err: Error) => void;
    timer?: ReturnType<typeof setTimeout>;
  }> = [];

  private closed = false;

  constructor(private readonly name: string, maxQueueSize = 100) {
    this.maxQueueSize = maxQueueSize;
  }

  /**
   * recv blocks until the next AU arrives or `timeoutMs` elapses.
   * If the track has buffered AUs, returns immediately with the
   * oldest one. Concurrent recv() calls are served FIFO.
   */
  recv(timeoutMs?: number, signal?: AbortSignal): Promise<AccessUnit> {
    if (this.closed) {
      return Promise.reject(new ClientError('track closed', 'closed'));
    }
    if (signal?.aborted) {
      return Promise.reject(new DOMException('aborted', 'AbortError'));
    }
    if (this.queue.length > 0) {
      return Promise.resolve(this.queue.shift()!);
    }
    return new Promise<AccessUnit>((resolve, reject) => {
      let onAbort: (() => void) | undefined;
      const detach = () => { if (onAbort && signal) signal.removeEventListener('abort', onAbort); };
      const waiter: {
        resolve: (au: AccessUnit) => void;
        reject: (err: Error) => void;
        timer?: ReturnType<typeof setTimeout>;
      } = {
        resolve: (au: AccessUnit) => {
          if (waiter.timer) clearTimeout(waiter.timer);
          detach();
          resolve(au);
        },
        reject: (err: Error) => {
          if (waiter.timer) clearTimeout(waiter.timer);
          detach();
          reject(err);
        },
      };
      if (timeoutMs !== undefined && timeoutMs > 0) {
        waiter.timer = setTimeout(() => {
          // Remove from queue and reject. clearTimeout is idempotent
          // so the resolve/reject paths above are still safe if a
          // late AU also tries to resolve.
          const idx = this.waiters.indexOf(waiter);
          if (idx !== -1) this.waiters.splice(idx, 1);
          waiter.timer = undefined;
          waiter.reject(new ClientError(`recv timeout on track "${this.name}"`, 'recv_timeout'));
        }, timeoutMs);
      }
      if (signal) {
        // Abort must REMOVE the waiter from the queue; otherwise a later AU
        // would be handed to this orphaned (already-rejected) waiter by
        // deliver() and silently dropped.
        onAbort = () => {
          const idx = this.waiters.indexOf(waiter);
          if (idx !== -1) this.waiters.splice(idx, 1);
          waiter.reject(new DOMException('aborted', 'AbortError'));
        };
        signal.addEventListener('abort', onAbort, { once: true });
      }
      this.waiters.push(waiter);
    });
  }

  /**
   * onAU registers a push-mode callback. Mutually exclusive with
   * recv(): calling both is a programming error. Flushes any queued
   * AUs synchronously through the callback.
   */
  onAU(cb: (au: AccessUnit) => void): void {
    this.onAUCallback = cb;
    while (this.queue.length > 0) {
      const au = this.queue.shift()!;
      try { cb(au); } catch (e) { console.error('onAU callback threw:', e); }
    }
  }

  /** Internal: dispatch an inbound AU. */
  deliver(au: AccessUnit): void {
    if (this.closed) return;
    // Pull-mode: hand off to the oldest pending recv() if any.
    if (this.waiters.length > 0) {
      const w = this.waiters.shift()!;
      w.resolve(au);
      return;
    }
    // Push-mode: invoke the callback.
    if (this.onAUCallback) {
      try {
        this.onAUCallback(au);
      } catch (e) {
        console.error('onAU callback threw:', e);
      }
      return;
    }
    // No waiter, no callback — queue. Drop oldest if full.
    if (this.queue.length >= this.maxQueueSize) {
      this.queue.shift();
      this.droppedAUs++;
    }
    this.queue.push(au);
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    // Reject all pending recv() promises so callers don't hang.
    const err = new ClientError('track closed', 'closed');
    for (const w of this.waiters) w.reject(err);
    this.waiters = [];
    this.queue = [];
    this.onAUCallback = undefined;
  }

  getName(): string { return this.name; }
  getDroppedCount(): number { return this.droppedAUs; }
  setMaxQueueSize(size: number): void {
    while (this.queue.length > size) {
      this.queue.shift();
      this.droppedAUs++;
    }
  }
}

// =================== TrackSender ===================

/**
 * TrackSender is the outbound side of a published track. The send
 * pipeline behind it implements the stream-per-GOP pattern from the
 * Go feed.Pump: a persistent uni stream carries one GOP, and a fresh
 * keyframe RESETs the prior stream and opens a new one. For non-
 * video tracks (tokens, tools, telemetry) every AU can be marked
 * Keyframe=true and the SDK emits one stream per AU — which is fine
 * at low rates but expensive at video rates.
 */
export class TrackSender {
  private closed = false;

  constructor(
    private readonly name: string,
    private readonly sendCallback: (au: AccessUnit) => Promise<void>,
    private readonly closeCallback?: () => Promise<void>,
  ) {}

  async send(au: AccessUnit): Promise<void> {
    if (this.closed) throw new ClientError('track sender closed', 'closed');
    await this.sendCallback(au);
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    if (this.closeCallback) {
      try { await this.closeCallback(); } catch { /* ignore */ }
    }
  }

  getName(): string { return this.name; }
}

// =================== sendPump (per-track outbound stream pump) ===================

/**
 * sendPump implements the stream-per-GOP wire pattern on the SDK
 * side: holds a persistent uni stream for the current GOP, RESETs
 * it on the next keyframe, and writes feed frames in sequence. This
 * is the TS counterpart to feed.Pump in Go.
 */
class sendPump {
  // createUnidirectionalStream() returns Promise<WritableStream> in
  // the DOM lib's WebTransport types, so we just track the writer
  // (which is what we use anyway).
  private currentWriter?: WritableStreamDefaultWriter<Uint8Array>;
  private closed = false;

  constructor(
    private readonly transport: WebTransport,
    private readonly trackName: string,
    private readonly onBytesOut?: (n: number) => void,
  ) {}

  async send(au: AccessUnit): Promise<void> {
    if (this.closed) throw new ClientError('sendPump closed', 'closed');

    if (au.keyframe) {
      // Reset prior GOP's stream so any in-flight bytes drop. The
      // receiver will resync on this fresh keyframe regardless.
      await this.resetCurrent();
      // Open a new uni stream + write the stream-header naming the
      // track + then the feed frame.
      const stream = await this.transport.createUnidirectionalStream();
      this.currentWriter = stream.getWriter();
      const header = writeStreamHeader(this.trackName);
      await this.currentWriter.write(header);
      this.onBytesOut?.(header.length);
    }

    if (!this.currentWriter) {
      // P-frame with no current stream: receiver mid-GOP, can't
      // decode yet. Drop silently — Go pump does the same and
      // requests a keyframe next.
      return;
    }

    let flags = 0;
    if (au.keyframe) flags |= FrameFlags.Keyframe;
    if (au.discontinuity) flags |= FrameFlags.Discontinuity;

    const frame = writeFeedFrame(
      au.keyframe ? FrameType.Keyframe : FrameType.PFrame,
      au.ptsMicro,
      au.seq,
      flags,
      au.bytes,
    );
    try {
      await this.currentWriter.write(frame);
      this.onBytesOut?.(frame.length);
    } catch (err) {
      // Stream broke; clear so next keyframe starts fresh.
      await this.resetCurrent();
      throw err;
    }
  }

  private async resetCurrent(): Promise<void> {
    if (this.currentWriter) {
      try {
        // Cancel rather than close — abandon any in-flight bytes
        // immediately. Matches feed.Pump's CancelWrite(0) behavior.
        await this.currentWriter.abort('keyframe-reset');
      } catch { /* ignore */ }
    }
    this.currentWriter = undefined;
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    if (this.currentWriter) {
      try { await this.currentWriter.close(); } catch { /* ignore */ }
    }
    this.currentWriter = undefined;
  }
}

// =================== QuicRTCClient ===================

/**
 * Constructor options. All fields optional; defaults match Go client.
 */
export interface QuicRTCClientOptions {
  /** Per-track recv queue size when no consumer is reading. Default 100. */
  maxQueueSize?: number;
  /**
   * Automatic reconnect with exponential backoff and session resume.
   * Default true: the server parks disconnected sessions for its
   * resume window (60s default) and replays missed AUs on reconnect,
   * so coming back is cheap and lossless. Set false to surface every
   * disconnect to the application instead.
   */
  reconnect?: boolean;
  /** Max reconnect attempts before giving up. Default 5. */
  maxRetries?: number;
  /** Initial backoff in ms. Default 1000. */
  baseBackoffMs?: number;
  /** Max backoff in ms. Default 30000. */
  maxBackoffMs?: number;
}

export class QuicRTCClient {
  private transport?: WebTransport;
  private controlReader?: BufferedReader;
  private controlWriter?: WritableStreamDefaultWriter<Uint8Array>;
  private dataChannel?: DataChannel;

  // Lazily-acquired reader on the WebTransport datagrams readable.
  // Kept across receiveDatagram calls so we don't release+reacquire
  // the lock per message (which would race with concurrent readers).
  private datagramReader?: ReadableStreamDefaultReader<Uint8Array>;

  private tracks = new Map<string, TrackReceiver>();
  private remoteTracks = new Map<string, RemoteTrack>();
  private senders = new Map<string, { sender: TrackSender; pump: sendPump }>();

  // Last-seen sequence per track — used on resume to send TypeResume{from_seq}.
  private lastSeqByTrack = new Map<string, number>();

  private sdp?: SDP;
  private negotiatedFeaturesValue: string[] = [];
  private sessionIdValue?: string;

  // Lifecycle
  private closed = false;
  private connectionState: ConnectionState = ConnectionState.Disconnected;
  private stateChangeCallbacks: ((s: ConnectionState) => void)[] = [];
  private closePromise?: Promise<void>;
  private closeResolve!: () => void;

  // AbortController plumbed into all read loops so close() unblocks
  // them immediately rather than waiting for the next byte.
  private abortController = new AbortController();

  // Metrics
  private bytesIn = 0;
  private bytesOut = 0;
  private handshakeStartTime = 0;
  private handshakeDuration = 0;
  private auReceived = 0;
  private auSent = 0;

  // Reconnection
  private connectUrl?: string;
  private connectOptions?: ClientOptions;
  private currentRetry = 0;
  private reconnectTimer?: ReturnType<typeof setTimeout>;

  private backpressureCallbacks: ((trackName: string, level: number) => void)[] = [];
  private announceCallbacks: ((track: RemoteTrack) => void)[] = [];
  private unannounceCallbacks: ((name: string) => void)[] = [];
  private kindStatsCallbacks: ((ks: KindStats) => void)[] = [];

  // Subscriber-side kind-stats emitter state. Non-undefined only when
  // the "kind-stats" feature was negotiated; the feed-drain loop feeds
  // collector and kindStatsTimer flushes it ~1 Hz to the publisher.
  private kindStats?: KindStatsCollector;
  private kindStatsTimer?: ReturnType<typeof setInterval>;

  /** Reconnect defaults ON — see QuicRTCClientOptions.reconnect. */
  private reconnectEnabled(): boolean {
    return this.opts.reconnect ?? true;
  }

  constructor(private readonly opts: QuicRTCClientOptions = {}) {
    // Initialize closePromise eagerly so concurrent close() calls all
    // see the same promise.
    this.closePromise = new Promise<void>((resolve) => {
      this.closeResolve = resolve;
    });
  }

  // ---- Public API ----

  async connect(url: string, options: ClientOptions): Promise<void> {
    if (this.closed) throw new ClientError('client already closed', 'closed');
    validateUrl(url);
    if (!options || typeof options.slug !== 'string' || options.slug.length === 0) {
      throw new ClientError('options.slug is required', 'invalid_options');
    }
    this.connectUrl = url;
    this.connectOptions = options;
    this.currentRetry = 0;
    await this.doConnect();
  }

  /** AddTrack — returns a sender that streams AUs to the server. */
  async addTrack(track: LocalTrack): Promise<TrackSender> {
    if (this.closed) throw new ClientError('client closed', 'closed');
    if (!this.transport) throw new ClientError('not connected', 'not_connected');

    const name = track.name || 'primary';
    validateLocalTrackName(name);

    const existing = this.senders.get(name);
    if (existing) return existing.sender;

    // Send Announce on the bidi control stream so the server learns
    // which inbound uni streams to expect.
    const announce: Announce = {
      name,
      kind: track.kind,
      codec: track.codec,
    };
    await this.writeControl(FrameType.Announce, marshalAnnounce(announce));

    // Spawn a per-track pump.
    const pump = new sendPump(this.transport, name, (n) => { this.bytesOut += n; });
    const sender = new TrackSender(
      name,
      async (au) => {
        await pump.send(au);
        this.auSent++;
      },
      async () => {
        // On close: send Unannounce + tear down pump.
        const body = marshalUnannounce({ name } as Unannounce);
        try { await this.writeControl(FrameType.Unannounce, body); } catch { /* ignore */ }
        await pump.close();
        this.senders.delete(name);
      },
    );

    this.senders.set(name, { sender, pump });
    return sender;
  }

  /**
   * publish — alias for addTrack, kept for symmetry with the Go API.
   */
  async publish(track: LocalTrack): Promise<TrackSender> {
    return this.addTrack(track);
  }

  /** Recv from the implicit "primary" track. */
  recv(timeoutMs?: number): Promise<AccessUnit> {
    return this.recvOn('primary', timeoutMs);
  }

  /** RecvOn — recv from a specific track by name. */
  async recvOn(trackName: string, timeoutMs?: number, signal?: AbortSignal): Promise<AccessUnit> {
    validateLocalTrackName(trackName);
    const track = this.getOrCreateTrack(trackName);
    return track.recv(timeoutMs, signal);
  }

  /** OnTrack — subscribe in push mode. */
  onTrack(trackName: string, cb: (au: AccessUnit) => void): void {
    validateLocalTrackName(trackName);
    this.getOrCreateTrack(trackName).onAU(cb);
  }

  /**
   * SendBackpressure — emit a TypeBackpressure control frame so the
   * publisher can adapt rate. trackName='' for session-level pressure.
   */
  async sendBackpressure(trackName: string, level: number): Promise<void> {
    if (this.closed) throw new ClientError('client closed', 'closed');
    const lvl = Math.max(0, Math.min(100, Math.floor(level)));
    const body: Backpressure = { track: trackName || undefined, level: lvl };
    await this.writeControl(FrameType.Backpressure, marshalBackpressure(body));
  }

  /**
   * requestKeyframe — ask the publisher to emit a fresh keyframe on
   * trackName NOW. Used by subscribers that detect a P-frame gap (loss
   * event, decoder reset, or join-mid-GOP) to cap visible-stutter at
   * ~one frame (typically ~33ms) instead of waiting up to one GOP
   * duration for the next natural keyframe.
   *
   * Wire-piggybacks on TypeBackpressure with needs_keyframe=true and
   * level=100. The Go server's Config.OnKeyframeRequest callback fires
   * when this arrives, signalling the encoder to flush a fresh
   * keyframe. v1.0 servers ignore the optional field (they see only
   * level=100 and may adapt rate, which is also reasonable).
   */
  async requestKeyframe(trackName: string): Promise<void> {
    if (this.closed) throw new ClientError('client closed', 'closed');
    const body: Backpressure = {
      track: trackName || undefined,
      level: 100,
      // eslint-disable-next-line @typescript-eslint/naming-convention
      needs_keyframe: true,
    };
    await this.writeControl(FrameType.Backpressure, marshalBackpressure(body));
  }

  /**
   * sendDatagram — send one unreliable WebTransport datagram to the
   * server. Bypasses stream flow control entirely, so the message
   * doesn't queue behind video bytes or other stream traffic on the
   * same connection. Lost datagrams are NOT retransmitted.
   *
   * Maximum payload after envelope overhead is `MaxDatagramPayload`
   * (1100 bytes). For kind-aware telemetry, pair with `encodeDatagram`
   * to build the 4-byte envelope; this method passes the bytes
   * through to the underlying WebTransport as-is.
   *
   * Throws if the connection isn't open or the WebTransport doesn't
   * support datagrams (older browsers).
   */
  async sendDatagram(payload: Uint8Array): Promise<void> {
    if (this.closed) throw new ClientError('client closed', 'closed');
    if (!this.transport) throw new ClientError('not connected', 'not_connected');
    const dg = (this.transport as { datagrams?: { writable: WritableStream<Uint8Array> } }).datagrams;
    if (!dg || !dg.writable) {
      throw new ClientError('WebTransport datagrams unavailable', 'datagrams_unsupported');
    }
    const writer = dg.writable.getWriter();
    try {
      await writer.write(payload);
    } finally {
      writer.releaseLock();
    }
  }

  /**
   * receiveDatagram — block until one datagram from the server arrives.
   * Returns the raw datagram bytes (caller is responsible for parsing
   * any envelope — see decodeDatagram in wire.ts for kind-aware
   * telemetry).
   *
   * Lost datagrams are not surfaced (no way to know they were lost
   * from the receiver side); only what arrives is returned. Returns
   * undefined when the connection closes.
   *
   * IMPORTANT: the first call acquires a reader lock on the underlying
   * `transport.datagrams.readable` that is held for the lifetime of
   * the client. If you want to consume datagrams via a different
   * mechanism (e.g., direct ReadableStream piping), do NOT call
   * receiveDatagram — otherwise the alternate consumer cannot acquire
   * the reader. The lock is released only when the client closes.
   */
  async receiveDatagram(): Promise<Uint8Array | undefined> {
    if (this.closed) throw new ClientError('client closed', 'closed');
    if (!this.transport) throw new ClientError('not connected', 'not_connected');
    const dg = (this.transport as { datagrams?: { readable: ReadableStream<Uint8Array> } }).datagrams;
    if (!dg || !dg.readable) {
      throw new ClientError('WebTransport datagrams unavailable', 'datagrams_unsupported');
    }
    if (!this.datagramReader) {
      this.datagramReader = dg.readable.getReader();
    }
    const { value, done } = await this.datagramReader.read();
    if (done) return undefined;
    return value;
  }

  /**
   * sessionId returns the server-allocated session identifier.
   * Pass this back as ClientOptions.sessionId to resume on reconnect.
   */
  sessionId(): string | undefined { return this.sessionIdValue; }

  /** Negotiated features = client features ∩ server features. */
  negotiatedFeatures(): string[] { return [...this.negotiatedFeaturesValue]; }

  getDataChannel(): DataChannel | undefined { return this.dataChannel; }
  getSDP(): SDP | undefined { return this.sdp; }
  getRemoteTracks(): RemoteTrack[] { return Array.from(this.remoteTracks.values()); }
  getStats(): TransportStats {
    return {
      bytesIn: this.bytesIn,
      bytesOut: this.bytesOut,
      handshakeRTT: this.handshakeDuration,
    };
  }
  getDetailedStats() {
    return {
      bytesIn: this.bytesIn,
      bytesOut: this.bytesOut,
      handshakeRTT: this.handshakeDuration,
      auReceived: this.auReceived,
      auSent: this.auSent,
      sessionId: this.sessionIdValue,
      connectionState: this.connectionState,
      trackCount: this.tracks.size,
      remoteTrackCount: this.remoteTracks.size,
    };
  }
  getConnectionState(): ConnectionState { return this.connectionState; }
  onStateChange(cb: (s: ConnectionState) => void): void {
    this.stateChangeCallbacks.push(cb);
    cb(this.connectionState);
  }
  offStateChange(cb: (s: ConnectionState) => void): void {
    const i = this.stateChangeCallbacks.indexOf(cb);
    if (i !== -1) this.stateChangeCallbacks.splice(i, 1);
  }
  onBackpressure(cb: (track: string, level: number) => void): void {
    this.backpressureCallbacks.push(cb);
  }
  offBackpressure(cb: (track: string, level: number) => void): void {
    const i = this.backpressureCallbacks.indexOf(cb);
    if (i !== -1) this.backpressureCallbacks.splice(i, 1);
  }
  /** OnAnnounce — fires when the server announces a new remote track. */
  onAnnounce(cb: (track: RemoteTrack) => void): void {
    this.announceCallbacks.push(cb);
  }
  offAnnounce(cb: (track: RemoteTrack) => void): void {
    const i = this.announceCallbacks.indexOf(cb);
    if (i !== -1) this.announceCallbacks.splice(i, 1);
  }
  /** OnUnannounce — fires when the server retracts a remote track. */
  onUnannounce(cb: (name: string) => void): void {
    this.unannounceCallbacks.push(cb);
  }
  offUnannounce(cb: (name: string) => void): void {
    const i = this.unannounceCallbacks.indexOf(cb);
    if (i !== -1) this.unannounceCallbacks.splice(i, 1);
  }
  /**
   * OnKindStats — fires for each per-Kind stats report the server pushes
   * (TypeKindStats, ~1 Hz when the "kind-stats" feature is negotiated).
   */
  onKindStats(cb: (ks: KindStats) => void): void {
    this.kindStatsCallbacks.push(cb);
  }
  offKindStats(cb: (ks: KindStats) => void): void {
    const i = this.kindStatsCallbacks.indexOf(cb);
    if (i !== -1) this.kindStatsCallbacks.splice(i, 1);
  }

  /**
   * startKindStatsEmitter activates the subscriber-side per-Kind
   * observability emitter when the "kind-stats" feature was
   * negotiated. Idempotent: a prior timer is cleared first so a
   * reconnect doesn't stack intervals. The collector persists across
   * reconnects so cumulative last_seq / dropped survive a blip.
   */
  private startKindStatsEmitter(): void {
    if (!this.negotiatedFeaturesValue.includes('kind-stats')) return;
    this.stopKindStatsEmitter();
    if (!this.kindStats) this.kindStats = new KindStatsCollector();
    this.kindStatsTimer = setInterval(() => {
      void this.flushKindStats();
    }, 1000);
  }

  private stopKindStatsEmitter(): void {
    if (this.kindStatsTimer !== undefined) {
      clearInterval(this.kindStatsTimer);
      this.kindStatsTimer = undefined;
    }
  }

  private async flushKindStats(): Promise<void> {
    if (!this.kindStats || this.closed) return;
    for (const ks of this.kindStats.snapshot()) {
      try {
        await this.writeControl(FrameType.KindStats, marshalKindStats(ks));
      } catch {
        // Control stream gone; the emitter stops on next teardown.
        return;
      }
    }
  }

  /** kindForTrack returns the Kind the server announced for a track,
   * or '' when unknown. Used to bucket kind-stats receive samples. */
  private kindForTrack(name: string): string {
    return this.remoteTracks.get(name)?.kind ?? '';
  }

  /** Close — idempotent; concurrent calls share the same promise. */
  async close(): Promise<void> {
    if (this.closed) return this.closePromise;
    this.closed = true;
    this.cancelReconnect();
    this.stopKindStatsEmitter();
    this.setConnectionState(ConnectionState.Disconnected);

    // Abort all reading loops first so their await reader.read()
    // calls unblock and the goroutines can exit.
    this.abortController.abort();

    // Cancel the held datagram reader so a blocked receiveDatagram() (e.g.
    // the viewer's datagram drain loop) unblocks instead of leaking.
    if (this.datagramReader) {
      try { await this.datagramReader.cancel(); } catch { /* ignore */ }
      this.datagramReader = undefined;
    }

    // Close per-track receivers (rejects pending recv() calls).
    for (const t of this.tracks.values()) t.close();
    this.tracks.clear();

    // Close per-track senders + pumps.
    const senderEntries = Array.from(this.senders.values());
    this.senders.clear();
    for (const { sender } of senderEntries) {
      try { await sender.close(); } catch { /* ignore */ }
    }

    if (this.dataChannel) this.dataChannel.close();

    // Best-effort TypeClose on the wire.
    try {
      if (this.controlWriter) {
        await this.controlWriter.write(writeControlFrame(FrameType.Close));
      }
    } catch { /* ignore */ }

    if (this.controlReader) {
      try { await this.controlReader.cancel('client-close'); } catch { /* ignore */ }
    }
    if (this.controlWriter) {
      try { await this.controlWriter.close(); } catch { /* ignore */ }
    }

    if (this.transport) {
      try { await this.transport.close(); } catch { /* ignore */ }
    }

    this.closeResolve();
    return this.closePromise;
  }

  // ---- Internal: connect / handshake / loops ----

  private async doConnect(): Promise<void> {
    if (!this.connectUrl || !this.connectOptions) {
      throw new ClientError('connection parameters not set', 'invalid_state');
    }

    this.setConnectionState(ConnectionState.Connecting);

    try {
      this.bytesIn = 0;
      this.bytesOut = 0;
      this.handshakeStartTime = performance.now();

      // Build WebTransport options. Cert hash pinning is REQUIRED for
      // self-signed dev certs on the browser side; without this the
      // browser rejects with a TLS error and the connect hangs.
      const wtOpts: WebTransportOptions = {};
      if (this.connectOptions.certHash) {
        const hashBytes = base64UrlToBytes(this.connectOptions.certHash);
        // Allocate a fresh ArrayBuffer rather than reusing the
        // Uint8Array's underlying buffer (which TS narrows to
        // ArrayBuffer | SharedArrayBuffer; BufferSource only accepts
        // the former). Copying ~32 bytes once at connect-time is
        // negligible.
        const ab = new ArrayBuffer(hashBytes.length);
        new Uint8Array(ab).set(hashBytes);
        wtOpts.serverCertificateHashes = [{
          algorithm: 'sha-256',
          value: ab,
        }];
      }
      this.transport = new WebTransport(this.connectUrl, wtOpts);
      await this.transport.ready;

      // Open bidi control stream.
      const ctl = await this.transport.createBidirectionalStream();
      this.controlReader = new BufferedReader(ctl.readable.getReader());
      this.controlWriter = ctl.writable.getWriter();

      this.dataChannel = new DataChannel(async (data) => {
        await this.writeControl(FrameType.Data, data);
      });

      // Send HELLO + read SDP. Resume per-track via TypeResume frames
      // happens AFTER handshake if we have a prior sessionId.
      await this.handshake(this.connectOptions);

      // If this is a reconnect with prior tracks, send TypeResume per
      // track with last-seen seq so the server replays missed AUs.
      if (this.connectOptions.sessionId && this.lastSeqByTrack.size > 0) {
        for (const [trackName, fromSeq] of this.lastSeqByTrack) {
          const r: Resume = { track: trackName, from_seq: fromSeq + 1 };
          try {
            await this.writeControl(FrameType.Resume, marshalResume(r));
          } catch { /* tolerate; resume is best-effort */ }
        }
      }

      // Spawn read loops.
      this.startControlReader();
      this.startFeedAcceptor();
      this.startBidiAcceptor();
      this.startKindStatsEmitter();

      this.setConnectionState(ConnectionState.Connected);
      this.currentRetry = 0;
    } catch (err) {
      this.setConnectionState(ConnectionState.Error);
      if (this.reconnectEnabled() && !this.closed) {
        this.scheduleReconnect();
      } else {
        throw err;
      }
    }
  }

  private async handshake(options: ClientOptions): Promise<void> {
    const features = options.features ?? DefaultClientFeatures();
    const hello: Hello = {
      role: 'recv',
      slug: options.slug,
      ver: '1',
      session: options.sessionId,
      features,
    };
    // On reconnect with a known sessionId, populate last_seen so the
    // server can replay AUs from its parked-session per-track replay
    // ring straight after handshake — closes the in-flight gap that
    // would otherwise require a post-handshake Resume frame per track
    // (still sent below for back-compat with older servers).
    if (options.sessionId && this.lastSeqByTrack.size > 0) {
      const lastSeen: Record<string, number> = {};
      for (const [trackName, seq] of this.lastSeqByTrack) {
        lastSeen[trackName] = seq;
      }
      hello.last_seen = lastSeen;
    }
    await this.writeControl(FrameType.Hello, marshalHello(hello));

    // Read SDP with timeout. The setTimeout is paired with a clearTimeout
    // so successful resolution doesn't leak a stale timer.
    const timeoutMs = options.helloTimeout ?? 5000;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const sdpFrame = await Promise.race([
      this.readControlFrame(),
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new ClientError('handshake timeout', 'timeout')),
          timeoutMs,
        );
      }),
    ]).finally(() => {
      if (timer) clearTimeout(timer);
    });

    if (sdpFrame.type === FrameType.Error) {
      throw new ClientError(`server rejected: ${this.formatServerError(sdpFrame.payload)}`, 'rejected');
    }
    if (sdpFrame.type !== FrameType.SDP) {
      throw new ClientError(`expected SDP, got frame type 0x${sdpFrame.type.toString(16)}`, 'protocol');
    }

    this.sdp = unmarshalSDP(sdpFrame.payload);
    this.negotiatedFeaturesValue = this.sdp.features ?? [];
    this.sessionIdValue = this.sdp.session;
    this.handshakeDuration = performance.now() - this.handshakeStartTime;
  }

  private async readControlFrame() {
    if (!this.controlReader) {
      throw new ClientError('control reader not initialized', 'invalid_state');
    }
    const f = await readControlFrame(this.controlReader);
    this.bytesIn += 4 + f.payload.length;
    return f;
  }

  private async writeControl(type: number, payload?: Uint8Array): Promise<void> {
    if (!this.controlWriter) {
      throw new ClientError('control writer not initialized', 'invalid_state');
    }
    const frame = writeControlFrame(type, payload);
    await this.controlWriter.write(frame);
    this.bytesOut += frame.length;
  }

  /** Control reader loop — runs until close() aborts it. */
  private startControlReader(): void {
    void (async () => {
      const signal = this.abortController.signal;
      try {
        while (!this.closed && !signal.aborted) {
          const { type, payload } = await this.readControlFrame();
          switch (type) {
            case FrameType.Ping:
              try { await this.writeControl(FrameType.Pong, payload); } catch { /* ignore */ }
              break;
            case FrameType.Pong:
              break;
            case FrameType.Data:
              if (this.dataChannel) this.dataChannel.deliver(payload);
              break;
            case FrameType.Announce:
              this.handleAnnounce(payload);
              break;
            case FrameType.Unannounce:
              this.handleUnannounce(payload);
              break;
            case FrameType.Backpressure:
              this.handleBackpressure(payload);
              break;
            case FrameType.KindStats:
              this.handleKindStats(payload);
              break;
            case FrameType.Error:
              throw new ClientError(`server error: ${this.formatServerError(payload)}`, 'server_error');
            case FrameType.Close:
              // Server is draining (e.g., graceful shutdown in a
              // multi-instance deployment). With reconnect enabled,
              // tear down this connection but schedule a reconnect
              // so the client fails over to another instance.
              if (this.reconnectEnabled()) {
                await this.teardownTransport();
                this.scheduleReconnect();
                return;
              }
              await this.close();
              return;
            default:
              // Unknown / not-yet-implemented type (Subscribe, Switch,
              // Resume — Resume is client→server and shouldn't arrive
              // here). Ignore for forward-compatibility.
              break;
          }
        }
      } catch (err) {
        if (!this.closed) {
          this.setConnectionState(ConnectionState.Error);
          // Trigger close (which will fire reconnect if enabled).
          await this.close();
          if (this.reconnectEnabled()) {
            this.scheduleReconnect();
          }
        }
      }
    })();
  }

  /** Feed acceptor: accepts inbound uni streams + spawns drainers. */
  private startFeedAcceptor(): void {
    void (async () => {
      const signal = this.abortController.signal;
      const transport = this.transport;
      if (!transport || !transport.incomingUnidirectionalStreams) {
        // Browser doesn't support incomingUnidirectionalStreams — fail
        // visibly instead of silently dropping all media.
        if (!this.closed) {
          this.setConnectionState(ConnectionState.Error);
          throw new ClientError(
            'browser WebTransport missing incomingUnidirectionalStreams',
            'browser_unsupported',
          );
        }
        return;
      }

      const streamsReader = transport.incomingUnidirectionalStreams.getReader();
      try {
        while (!this.closed && !signal.aborted) {
          const { value, done } = await streamsReader.read();
          if (done || !value) break;
          // Drain each stream concurrently. The drainer captures the
          // BufferedReader for the lifetime of the stream — never
          // pass raw stream readers to wire.* helpers (the wrap-and-
          // discard antipattern).
          void this.drainFeedStream(value);
        }
      } catch (err) {
        if (!this.closed) {
          // Surface as state change; don't throw out of a goroutine.
          this.setConnectionState(ConnectionState.Error);
        }
      } finally {
        try { streamsReader.releaseLock(); } catch { /* ignore */ }
      }
    })();
  }

  /** Bidi acceptor: accepts inbound bidi streams (KindToolCalls /
   *  BidiPerCall delivery). Each stream carries TypeStreamHeader +
   *  one feed frame, identical to uni streams except the wire shape
   *  is bidirectional. We close our send half so the server's
   *  io.ReadAll returns without waiting for BidiCallTimeout (30 s),
   *  which would otherwise leak a goroutine + stream per AU. */
  private startBidiAcceptor(): void {
    void (async () => {
      const signal = this.abortController.signal;
      const transport = this.transport;
      if (!transport || !transport.incomingBidirectionalStreams) {
        // Browsers without bidi accept can't receive KindToolCalls;
        // not fatal — uni-stream tracks still work.
        return;
      }
      const streamsReader = transport.incomingBidirectionalStreams.getReader();
      try {
        while (!this.closed && !signal.aborted) {
          const { value, done } = await streamsReader.read();
          if (done || !value) break;
          void this.drainBidiStream(value);
        }
      } catch {
        // feedAcceptor surfaces the transport error; exit quietly.
      } finally {
        try { streamsReader.releaseLock(); } catch { /* ignore */ }
      }
    })();
  }

  private async drainBidiStream(stream: WebTransportBidirectionalStream): Promise<void> {
    // FIN our send half so the server's io.ReadAll returns. Without
    // this, the server's BidiPerCall path blocks per call until
    // BidiCallTimeout (default 30 s).
    try {
      await stream.writable.close();
    } catch {
      // close() failed (already closed, aborted, or transport error).
      // Fall back to abort() so the server sees a stream reset and its
      // io.ReadAll returns immediately — otherwise we leak a goroutine
      // + stream per AU until BidiCallTimeout fires.
      try {
        await stream.writable.abort();
      } catch {
        /* writable already gone; nothing to do */
      }
    }
    return this.drainFeedStream(stream.readable);
  }

  private async drainFeedStream(stream: ReadableStream<Uint8Array>): Promise<void> {
    const reader = stream.getReader();
    const buffered = new BufferedReader(reader);
    try {
      // Peek for stream-header. If it's a TypeStreamHeader, parse the
      // track name; otherwise the byte is unread() onto the front of
      // the buffer and the stream is treated as the implicit
      // "primary" track (legacy single-track wire compatibility).
      const { trackName } = await peekStreamHeader(buffered);
      const finalName = trackName || 'primary';

      // Server-supplied names get the permissive validator since they
      // legitimately may include `:` or `/` in multi-tenant setups.
      validateRemoteTrackName(finalName);

      const track = this.getOrCreateTrack(finalName);
      const kind = this.kindForTrack(finalName);
      while (!this.closed && !this.abortController.signal.aborted) {
        const f = await readFeedFrame(buffered);
        this.bytesIn += 17 + (f.pubWallMicro !== undefined ? 8 : 0) + f.payload.length;
        this.auReceived++;
        if (this.kindStats) {
          this.kindStats.observe(kind, f.seq, f.pubWallMicro, Date.now());
        }
        const isKey = f.type === FrameType.Keyframe ||
          (f.flags & FrameFlags.Keyframe) !== 0;
        const au: AccessUnit = {
          bytes: f.payload,
          keyframe: isKey,
          ptsMicro: f.ptsMicro,
          seq: f.seq,
          discontinuity: (f.flags & FrameFlags.Discontinuity) !== 0,
          pubWallMicro: f.pubWallMicro,
        };
        // Track last-seen seq for resume.
        this.lastSeqByTrack.set(finalName, f.seq);
        track.deliver(au);
      }
    } catch (err) {
      // EOF or stream error — exit loop. Not fatal to the session.
    } finally {
      try { await buffered.cancel(); } catch { /* ignore */ }
      buffered.releaseLock();
    }
  }

  private handleAnnounce(payload: Uint8Array): void {
    let a: Announce;
    try {
      a = unmarshalAnnounce(payload);
    } catch {
      // Malformed announce — ignore rather than tear down the session.
      return;
    }
    try {
      validateRemoteTrackName(a.name);
    } catch {
      // Server sent a name we won't accept — ignore. Don't kill the
      // control reader.
      return;
    }
    const rt: RemoteTrack = {
      name: a.name,
      kind: a.kind as TrackKind,
      codec: a.codec,
    };
    this.remoteTracks.set(a.name, rt);
    // Pre-create the track so concurrent recvOn() before the first
    // feed stream lands has a queue to wait on.
    this.getOrCreateTrack(a.name);
    for (const cb of this.announceCallbacks) {
      try { cb(rt); } catch (e) { console.error('announce callback threw:', e); }
    }
  }

  private handleUnannounce(payload: Uint8Array): void {
    let u: Unannounce;
    try {
      u = unmarshalUnannounce(payload);
    } catch {
      return;
    }
    this.remoteTracks.delete(u.name);
    const t = this.tracks.get(u.name);
    if (t) {
      // Match Go behavior: don't close the track here — drainFeed
      // goroutines may still be writing into its queue. Just remove
      // from the active map so future resumes don't reattach.
      this.tracks.delete(u.name);
      // Close — pending recv() callers should learn the track is gone.
      t.close();
    }
    for (const cb of this.unannounceCallbacks) {
      try { cb(u.name); } catch (e) { console.error('unannounce callback threw:', e); }
    }
  }

  private handleBackpressure(payload: Uint8Array): void {
    let bp: Backpressure;
    try {
      bp = unmarshalBackpressure(payload);
    } catch {
      return;
    }
    const trackName = bp.track || '';
    const level = typeof bp.level === 'number' ? bp.level : 0;
    for (const cb of this.backpressureCallbacks) {
      try { cb(trackName, level); } catch (e) { console.error('backpressure callback threw:', e); }
    }
  }

  private handleKindStats(payload: Uint8Array): void {
    let ks: KindStats;
    try {
      ks = unmarshalKindStats(payload);
    } catch {
      return;
    }
    for (const cb of this.kindStatsCallbacks) {
      try { cb(ks); } catch (e) { console.error('kind-stats callback threw:', e); }
    }
  }

  /**
   * formatServerError renders a structured TypeError payload as
   * "code: reason" (or whichever of the two is present). Legacy non-JSON
   * payloads surface as their raw bytes (via unmarshalError).
   */
  private formatServerError(payload: Uint8Array): string {
    const e = unmarshalError(payload);
    if (e.code && e.reason) return `${e.code}: ${e.reason}`;
    return e.code || e.reason || 'unknown error';
  }

  private getOrCreateTrack(name: string): TrackReceiver {
    let t = this.tracks.get(name);
    if (!t) {
      t = new TrackReceiver(name, this.opts.maxQueueSize ?? 100);
      this.tracks.set(name, t);
    }
    return t;
  }

  private setConnectionState(state: ConnectionState): void {
    if (this.connectionState !== state) {
      this.connectionState = state;
      for (const cb of this.stateChangeCallbacks) {
        try { cb(state); } catch (e) { console.error('state change callback threw:', e); }
      }
    }
  }

  private scheduleReconnect(): void {
    if (this.currentRetry >= (this.opts.maxRetries ?? 5)) return;
    const base = this.opts.baseBackoffMs ?? 1000;
    const max = this.opts.maxBackoffMs ?? 30000;
    const backoff = Math.min(base * Math.pow(2, this.currentRetry), max);
    this.currentRetry++;
    this.reconnectTimer = setTimeout(async () => {
      try {
        // The control-reader error path runs close() before scheduling
        // this reconnect, which sets the closed flag; clear it so the
        // resumed connection's API surface works again. Reset the
        // abort controller so the new connection's loops aren't
        // immediately killed by the previous abort.
        this.closed = false;
        this.abortController = new AbortController();
        if (this.connectOptions && this.sessionIdValue) {
          this.connectOptions.sessionId = this.sessionIdValue;
        }
        await this.doConnect();
      } catch {
        // Already handled in doConnect (errored out → state error).
      }
    }, backoff);
  }

  /**
   * teardownTransport tears down the current WebTransport session and
   * its read loops WITHOUT permanently closing the client. Used when
   * the server sends TypeClose (drain) and reconnect is enabled: the
   * client should fail over to another instance, not stop entirely.
   */
  private async teardownTransport(): Promise<void> {
    this.abortController.abort();
    // Pause the emitter for this transport; reconnect's
    // startKindStatsEmitter restarts it while preserving the collector.
    this.stopKindStatsEmitter();
    if (this.datagramReader) {
      try { await this.datagramReader.cancel(); } catch { /* ignore */ }
      this.datagramReader = undefined;
    }
    if (this.controlReader) {
      try { await this.controlReader.cancel('drain-close'); } catch { /* ignore */ }
    }
    if (this.controlWriter) {
      try { await this.controlWriter.close(); } catch { /* ignore */ }
    }
    if (this.transport) {
      try { await this.transport.close(); } catch { /* ignore */ }
    }
    this.transport = undefined;
    this.controlReader = undefined;
    this.controlWriter = undefined;
    this.abortController = new AbortController();
    this.setConnectionState(ConnectionState.Disconnected);
  }

  private cancelReconnect(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    this.currentRetry = 0;
  }
}

// =================== Helpers ===================

/**
 * base64url decode. Browser's atob doesn't handle the URL-safe
 * variant out of the box.
 */
function base64UrlToBytes(s: string): Uint8Array {
  let b64 = s.replace(/-/g, '+').replace(/_/g, '/');
  // Pad to multiple of 4.
  while (b64.length % 4 !== 0) b64 += '=';
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}
