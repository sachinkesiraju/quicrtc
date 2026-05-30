/**
 * quicrtc/viewer — framework-agnostic helpers for embedding a
 * quicrtc session in a frontend.
 *
 * Two layers:
 *
 *   - AgentSession (this file): an EventTarget wrapper around
 *     QuicRTCClient. AbortSignal-friendly, decoupled from React. Use
 *     directly from vanilla JS, plain DOM, or any framework.
 *
 *   - React hook (./react): useAgentSession returning live state
 *     for React apps. React is a peerDependency — the import lives
 *     under the ./react subpath so non-React consumers don't pay
 *     for it.
 *
 * Wire is unchanged. This is purely an ergonomic layer.
 */

import { QuicRTCClient } from '../client.js';
import { AccessUnit, ClientOptions, RemoteTrack, TrackKind, KindStats } from '../types.js';

export type SessionState = 'idle' | 'connecting' | 'connected' | 'reconnecting' | 'closed' | 'error';

/**
 * AgentSessionEvents enumerates the typed CustomEvent shapes the
 * AgentSession EventTarget dispatches. Use addEventListener with
 * the matching key to subscribe.
 */
export interface AgentSessionEvents {
  'state-change': CustomEvent<{ state: SessionState; previous: SessionState; error?: Error }>;
  'track-added':  CustomEvent<{ track: RemoteTrack }>;
  'track-removed': CustomEvent<{ name: string }>;
  'au':           CustomEvent<{ trackName: string; au: AccessUnit }>;
  'kind-stats':   CustomEvent<KindStats>;
  'datagram':     CustomEvent<{ data: Uint8Array }>;
  'reconnected':  CustomEvent<{ sessionId: string }>;
  'error':        CustomEvent<{ error: Error }>;
}

/**
 * ConnectOptions extends ClientOptions with the URL and a few
 * viewer-specific knobs the underlying client doesn't track.
 */
export interface ConnectOptions extends ClientOptions {
  url: string;
  /** Auto-reconnect on transport failure. Default: true. */
  reconnect?: boolean;
  /** Cap on reconnect attempts before giving up. Default: 5. */
  maxRetries?: number;
}

/**
 * AgentSession wraps QuicRTCClient with an EventTarget façade and
 * AbortSignal-aware helpers. Existing callback APIs on the underlying
 * client remain available; this layer is purely additive.
 */
export class AgentSession extends EventTarget {
  private client: QuicRTCClient;
  private opts: ConnectOptions;
  private _state: SessionState = 'idle';
  private _tracks = new Map<string, RemoteTrack>();
  private _closed = false;
  private _wired = false;
  private _auActive = false;
  private _auWired = new Set<string>();
  private _wantDatagrams = false;
  private _dgLoop = false;

  constructor(opts: ConnectOptions) {
    super();
    this.opts = opts;
    this.client = new QuicRTCClient();
  }

  get state(): SessionState { return this._state; }
  get tracks(): ReadonlyMap<string, RemoteTrack> { return this._tracks; }
  get sessionId(): string | undefined { return this.client.sessionId() ?? undefined; }

  async connect(signal?: AbortSignal): Promise<void> {
    this.setState('connecting');
    try {
      if (signal?.aborted) throw new Error('aborted');
      await this.client.connect(this.opts.url, this.opts);
      // Wire underlying client callbacks → event dispatch. Once per session;
      // a manual reconnect() reuses the same client, so don't double-register.
      if (!this._wired) {
        this._wired = true;
        this.client.onStateChange((s) => {
          if (s === 'connected') {
            const wasReconnecting = this._state === 'reconnecting';
            if (this._state !== 'connected') this.setState('connected');
            if (wasReconnecting) {
              this.dispatchEvent(new CustomEvent('reconnected', { detail: { sessionId: this.sessionId ?? '' } }));
            }
          }
          if (s === 'error') this.setState('error');
          if (s === 'disconnected' && !this._closed) {
            this._auWired.clear(); // tracks are torn down; re-wire 'au' on reconnect
            this.setState('reconnecting');
          }
        });
        this.client.onAnnounce((track) => {
          this._tracks.set(track.name, track);
          this.dispatchEvent(new CustomEvent('track-added', { detail: { track } }));
          this.wireAU(track.name);
        });
        this.client.onUnannounce((name) => {
          this._tracks.delete(name);
          this._auWired.delete(name); // re-allow 'au' wiring if the track is re-announced
          this.dispatchEvent(new CustomEvent('track-removed', { detail: { name } }));
        });
        this.client.onKindStats((ks) => {
          this.dispatchEvent(new CustomEvent('kind-stats', { detail: ks }));
        });
      }
      // Snapshot known tracks. Skip ones already seen so a manual reconnect()
      // doesn't re-dispatch track-added for tracks the consumer already has.
      for (const t of this.client.getRemoteTracks()) {
        const isNew = !this._tracks.has(t.name);
        this._tracks.set(t.name, t);
        if (isNew) this.dispatchEvent(new CustomEvent('track-added', { detail: { track: t } }));
        this.wireAU(t.name);
      }
      if (this._wantDatagrams) this.startDatagramLoop();
      this.setState('connected');
    } catch (err) {
      this.setState('error', err as Error);
      throw err;
    }
  }

  /**
   * Receive one AccessUnit on a track. Honors AbortSignal: cancelling
   * the signal rejects the returned promise with an AbortError instead
   * of leaving a dangling pending recv.
   */
  recv(trackName: string, signal?: AbortSignal): Promise<AccessUnit> {
    // Cancellation is handled in the client: aborting removes the receive
    // waiter, so no in-flight AU is silently consumed by an orphaned recv.
    return this.client.recvOn(trackName, undefined, signal);
  }

  /**
   * Send a datagram. Honors AbortSignal for pre-write cancellation;
   * after the write reaches the transport it cannot be unsent.
   */
  async sendDatagram(payload: Uint8Array, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) throw new DOMException('aborted', 'AbortError');
    await this.client.sendDatagram(payload);
  }

  /**
   * Add a publishable track. Returns the underlying TrackSender so
   * the caller can drive AUs into it. AbortSignal applies only to
   * the announce/handshake step.
   */
  async addTrack(name: string, kind: TrackKind, signal?: AbortSignal): Promise<unknown> {
    if (signal?.aborted) throw new DOMException('aborted', 'AbortError');
    return this.client.addTrack({ name, kind });
  }

  close(): void {
    if (this._closed) return;
    this._closed = true;
    this.setState('closed');
    this.client.close();
  }

  /**
   * 'au' and 'datagram' are push-consumption events, mutually exclusive
   * with the pull APIs (recv() / the underlying datagram reader). We only
   * switch a track to push mode, or start draining datagrams, once
   * something actually listens — so pull consumers that never attach these
   * listeners keep recv() semantics intact.
   */
  addEventListener(
    type: string,
    listener: EventListenerOrEventListenerObject | null,
    options?: AddEventListenerOptions | boolean,
  ): void {
    super.addEventListener(type, listener, options);
    if (type === 'au' && !this._auActive) {
      this._auActive = true;
      for (const name of this._tracks.keys()) this.wireAU(name);
    }
    if (type === 'datagram') {
      this._wantDatagrams = true;
      if (this._state === 'connected') this.startDatagramLoop();
    }
  }

  private wireAU(name: string): void {
    if (!this._auActive || this._auWired.has(name)) return;
    this._auWired.add(name);
    // Push mode for this track — mutually exclusive with recv() on it.
    this.client.onTrack(name, (au) => {
      this.dispatchEvent(new CustomEvent('au', { detail: { trackName: name, au } }));
    });
  }

  private startDatagramLoop(): void {
    if (this._dgLoop) return;
    this._dgLoop = true;
    void (async () => {
      while (!this._closed) {
        let d: Uint8Array | undefined;
        try { d = await this.client.receiveDatagram(); } catch { break; }
        if (!d) break; // datagrams unavailable or transport closed
        this.dispatchEvent(new CustomEvent('datagram', { detail: { data: d } }));
      }
    })();
  }

  private setState(next: SessionState, err?: Error): void {
    if (next === this._state) return;
    const prev = this._state;
    this._state = next;
    this.dispatchEvent(new CustomEvent('state-change', { detail: { state: next, previous: prev, error: err } }));
    if (err) {
      this.dispatchEvent(new CustomEvent('error', { detail: { error: err } }));
    }
  }
}

/**
 * Convenience: register a typed event listener with full IDE
 * autocomplete on the event name + detail payload.
 */
export function on<K extends keyof AgentSessionEvents>(
  s: AgentSession,
  type: K,
  listener: (ev: AgentSessionEvents[K]) => void,
  opts?: AddEventListenerOptions,
): () => void {
  s.addEventListener(type as string, listener as EventListener, opts);
  return () => s.removeEventListener(type as string, listener as EventListener);
}
