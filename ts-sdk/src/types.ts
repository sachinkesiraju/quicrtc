/**
 * Core types for QUICRTC TypeScript client.
 *
 * Wire-message types (Hello, SDP, Announce, Unannounce, Resume,
 * Backpressure) use field names that EXACTLY match the Go server's
 * `json:"..."` struct tags. Mismatches cause silent serialization
 * failures (server reads empty fields, can't resume, etc.). These
 * are internal types; user-facing types like ClientOptions use the
 * idiomatic camelCase.
 *
 * Reference: github.com/sachinkesiraju/quicrtc/wire/hello.go
 */

/**
 * Track kind constants. Values match Go's track.Kind string values
 * EXACTLY — these strings ride the wire on every Announce frame and
 * are compared verbatim against the Go-side `track.Kind` constants.
 * Mismatches silently route incoming tracks to the wrong handler.
 */
export enum TrackKind {
  Video = "video",
  Audio = "audio",
  Tokens = "tokens",
  ToolCalls = "toolcalls",
  Telemetry = "telemetry",
}

/**
 * AccessUnit — one publishable media or data frame. Wire layout
 * is documented in wire/frame.go (FeedFrame). The TS type uses
 * camelCase; wire encoding handles the binary layout, not JSON.
 */
export interface AccessUnit {
  /** Raw payload bytes (codec-specific). */
  bytes: Uint8Array;
  /** Self-contained checkpoint (video keyframe / token boundary). */
  keyframe: boolean;
  /**
   * Presentation timestamp in microseconds. STORED AS BIGINT to
   * avoid the 2^32 µs (≈71 min wall-clock) silent-truncation bug
   * that bites Number-based encoding. Use BigInt() / Number() at
   * the application boundary as appropriate.
   */
  ptsMicro: bigint;
  /** Monotonic sequence number per publisher. uint32 on the wire. */
  seq: number;
  /** Decoder-reset hint. */
  discontinuity: boolean;
}

/**
 * LocalTrack — track this peer publishes.
 */
export interface LocalTrack {
  name: string;
  kind: TrackKind;
  codec?: string;
  priority?: number;
}

/**
 * RemoteTrack — track this peer is subscribed to.
 */
export interface RemoteTrack {
  name: string;
  kind: TrackKind;
  codec?: string;
}

// ===== Wire-message types — field names match Go json: tags =====

/**
 * Hello — first frame on the bidi control stream from subscriber to
 * server. Field names MATCH Go's `json:"..."` tags exactly:
 *   role, slug, ver, session, features
 *
 * (NOT version/sessionId — those are TS-style names that don't
 * round-trip through Go.)
 */
export interface Hello {
  role: "recv" | "send";
  slug: string;
  ver: string;
  /** Resume an existing session by ID; omit for fresh session. */
  session?: string;
  /** Optional protocol features the client supports. */
  features?: string[];
  /**
   * Per-track continuation map for resume. Each entry asks the server
   * to replay AUs with Seq > value from its broadcaster's replay
   * buffer before resuming live delivery. Recovers AUs in flight on
   * the previous QUIC wire when it dropped. Only consulted when
   * `session` is set and matches a parked session.
   */
  last_seen?: Record<string, number>;
}

/**
 * SDP — server's response to a valid HELLO. Field names match Go:
 *   codec, width, height, fps, screenW, screenH, session, features
 */
export interface SDP {
  codec: string;
  width: number;
  height: number;
  fps: number;
  screenW?: number;
  screenH?: number;
  /** Server-allocated or echoed session ID — store for resume. */
  session?: string;
  /** Negotiated features = client.features ∩ server.SupportedFeatures. */
  features?: string[];
}

/**
 * Announce — declares a new track on the bidi control stream. Field
 * names match Go: name, kind, codec.
 */
export interface Announce {
  name: string;
  kind: string;
  codec?: string;
}

/**
 * Unannounce — track teardown. Field names match Go: name.
 */
export interface Unannounce {
  name: string;
}

/**
 * Resume — client → server request to replay AUs on a track from a
 * given sequence. Field names match Go: track, from_seq.
 *
 * NOT { sessionId, trackName, fromSeq } — those don't exist on the
 * wire. The session ID is conveyed via Hello.session, not Resume.
 */
export interface Resume {
  track: string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  from_seq: number;
}

/**
 * Backpressure — subscriber → publisher signal. Field names match
 * Go: track (omitempty), level, needs_keyframe (omitempty).
 *
 * `needs_keyframe` (added in v1.1) signals that the subscriber wants
 * a fresh keyframe NOW — used for computer-use / live-video recovery
 * paths to cap visible-stutter at ~one frame instead of one GOP. v1.0
 * receivers ignore the optional field, so this is backward-compatible.
 */
export interface Backpressure {
  track?: string;
  /** 0..100; 0 = drained, 100 = full. */
  level: number;
  /** Request immediate keyframe emission on the named track. */
  // eslint-disable-next-line @typescript-eslint/naming-convention
  needs_keyframe?: boolean;
}

/**
 * Datagram envelope constants. Wire layout (matches Go wire/datagram.go):
 *   [1B type][1B trackId][2B BE seq][payload]
 *
 * 4 bytes total. Used by the kind-aware telemetry path (Kind=telemetry
 * → DeliveryDatagramOrStream on the server side). The TS client
 * exposes `sendDatagram` / `receiveDatagram` to drive this path.
 */
export const DatagramHeaderLen = 4;
export const TypeDatagramAU = 0x24;
export const MaxDatagramPayload = 1100;

// ===== User-facing types (camelCase, not on the wire) =====

/**
 * ClientOptions — passed to QuicRTCClient.connect. NOT a wire type;
 * field names are the user-facing camelCase shape.
 */
export interface ClientOptions {
  /** Authentication slug, JWT, or bearer token. */
  slug: string;
  /** Cert pin — base64url-encoded SHA-256(DER) cert hash. */
  certHash?: string;
  /** Skip TLS verification (testing only). */
  insecureSkipVerify?: boolean;
  /** HELLO + SDP exchange timeout in ms. */
  helloTimeout?: number;
  /** Session ID to resume; empty/undefined = fresh session. */
  sessionId?: string;
  /** Features to advertise; nil = use DefaultClientFeatures(). */
  features?: string[];
}

/**
 * TransportStats — observability surface.
 */
export interface TransportStats {
  bytesIn: number;
  bytesOut: number;
  handshakeRTT: number;
}

/**
 * Frame type constants. Wire bytes match Go's wire.Type* exactly.
 */
export enum FrameType {
  // Control frames (bidi control stream)
  Hello = 0x01,
  SDP = 0x02,
  Ping = 0x03,
  Pong = 0x04,
  Error = 0x05,
  Close = 0x06,
  Data = 0x07,
  Switch = 0x08,
  Subscribe = 0x09,
  Announce = 0x0a,
  Backpressure = 0x0b,
  Unannounce = 0x0c,
  Resume = 0x0d,

  // Feed frames (uni streams)
  Keyframe = 0x20,
  PFrame = 0x21,
  Frame = 0x22,
  StreamHeader = 0x23,
}

/**
 * Feed-frame flag bits (single byte at offset 16 of feed header).
 */
export enum FrameFlags {
  Keyframe = 1 << 0,
  Discontinuity = 1 << 1,
}

/**
 * DefaultClientFeatures — every optional protocol feature this
 * client build supports. Mirrors client/client.go's
 * DefaultClientFeatures() so a TS client opts in to the same
 * features a Go client does by default.
 */
export function DefaultClientFeatures(): string[] {
  return ["resume", "backpressure", "publishback", "stream-header"];
}
