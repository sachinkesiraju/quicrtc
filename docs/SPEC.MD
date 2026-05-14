# Wire format — version 1

The on-the-wire protocol that quicrtc speaks. Any-language
implementations can interop by following this document.

Frames on the wire today: `0x01 HELLO`, `0x02 SDP`, `0x03/0x04
PING/PONG`, `0x05 ERROR`, `0x06 CLOSE`, `0x07 DATA`, `0x09 SUBSCRIBE`,
`0x0a ANNOUNCE`, `0x0b BACKPRESSURE`, `0x0c UNANNOUNCE`, `0x0d RESUME`
on the control stream; `0x20 KEYFRAME`, `0x21 PFRAME`, `0x22 FRAME` on
feed streams. Reserved for v2: HELLO `tracks: [...]` array, AccessUnit
metadata block, `SyncGroup`, `0x08 SWITCH`. v1 receivers silently
ignore unknown control frame types, so v2 additions are
forward-compatible.

Not wire-compatible with IETF Media-over-QUIC (MoQT). The
stream-per-GOP pattern is structurally similar to moq-lite's
group-as-stream model. The `relay` package speaks this same wire
format end-to-end — relays are invisible to subscribers.

## Transport

- **Underlying transport:** QUIC v1 (RFC 9000) with TLS 1.3.
- **Application protocol:** HTTP/3 + WebTransport (RFC 9220 / draft).
  ALPN: `h3`.
- **Cert validation:** the server MAY use an ephemeral self-signed
  ECDSA-P256 cert with validity ≤14 days. Clients MUST be able to
  validate by `serverCertificateHashes` (SHA-256 of DER).
- **Origin model:** the WebTransport upgrade MUST honor the standard
  HTTP `Origin` header allow-list.

## Connection setup

1. Client opens a WebTransport session at `https://<host>:<port>/wt`.
2. Client opens **one bidi stream**: the **control stream**.
3. Client sends `HELLO`. Server validates and replies `SDP` (success)
   or `ERROR` (failure).
4. On success, the server begins opening **unidirectional streams**
   to the client carrying media (one stream per GOP — see below).

## Stream shapes

### Control stream (bidi, exactly one per session)

Frames are typed and length-prefixed:

```
+--------+---------+----------------+
| 1 byte | 3 bytes | length bytes   |
| type   | length  | payload        |
+--------+---------+----------------+
```

The 3-byte length is big-endian. Maximum control payload: 64 KiB.

Frames may flow in either direction at any time after handshake.

### Feed stream (unidirectional, server → client, one per GOP)

Each frame:

```
+--------+---------+----------+--------+--------+----------------+
| 1 byte | 3 bytes | 8 bytes  | 4 bytes| 1 byte | length bytes   |
| type   | length  | pts µs   | seq    | flags  | payload        |
+--------+---------+----------+--------+--------+----------------+
```

All multi-byte fields big-endian. Maximum payload per feed frame:
16,777,215 bytes (16 MiB − 1, the 3-byte length-field cap). Enforced by `wire.MaxFeedPayload`.

The first frame on a feed stream MUST be a keyframe. Subsequent
frames within that GOP are P-frames in decode order. When the next
GOP begins, the server SHOULD `RESET_STREAM` (NOT `FIN`) the
previous feed stream so any in-flight stale bytes are dropped at
the QUIC layer.

Within a single feed stream, QUIC's per-stream FIFO byte ordering IS
the canonical decode order. Receivers MUST NOT introduce a reorder
buffer keyed on `seq`; `seq` is informational/diagnostic only.

## Frame types

| Code | Name | Stream | Payload | Status |
|------|------|--------|---------|--------|
| 0x01 | HELLO | control | UTF-8 JSON: `{"role":"recv","slug":"<base64url>","ver":"1"}` (v2: gains `tracks: [...]`) | v1 shipping |
| 0x02 | SDP | control | UTF-8 JSON: `{"codec":"<string>","width":N,"height":N,"fps":N,"screenW":N?,"screenH":N?}` | v1 shipping |
| 0x03 | PING | control | opaque (echoed in PONG) | v1 shipping |
| 0x04 | PONG | control | opaque (echo of PING payload) | v1 shipping |
| 0x05 | ERROR | control | UTF-8 string | v1 shipping |
| 0x06 | CLOSE | control | empty | v1 shipping |
| 0x07 | DATA | control | opaque application bytes | v1 shipping |
| 0x08 | SWITCH | control | layer ID (varint) | v2 reserved |
| 0x09 | SUBSCRIBE | control | track name (UTF-8) | v1 shipping |
| 0x0a | ANNOUNCE | control | track descriptor (JSON: name, kind) | v1 shipping |
| 0x0b | BACKPRESSURE | control | kind-aware slow-consumer hint (JSON: track, level) | v1 shipping |
| 0x0c | UNANNOUNCE | control | track name (JSON) | v1 shipping |
| 0x0d | RESUME | control | session resume request (JSON: sessionID) | v1 shipping |
| 0x20 | KEYFRAME | feed | codec payload (e.g. H.264 Annex B with SPS+PPS+IDR) | v1 shipping |
| 0x21 | PFRAME | feed | codec payload | v1 shipping |
| 0x22 | FRAME | feed | generic AU (non-video tracks: tokens, tool-calls, telemetry, custom codecs without GOP semantics) | v1 shipping |

For `0x22 FRAME`, the flag bits map to abstract semantics:
- `FlagKeyframe` (bit 0) means "self-contained checkpoint" —
  receivers can decode/process from this AU without prior context.
- `FlagDiscontinuity` (bit 1) means receivers should reset
  decoder/parser state before this AU.

Codes `0x08..0x1F` and `0x22..0x7F` are reserved for future quicrtc
versions. Receivers MUST silently ignore unknown control types so a
peer running a newer version doesn't break older receivers.
Application-defined extensions SHOULD use `0x80..0xFF`.

## Feed flag bits

| Bit | Meaning |
|-----|---------|
| 0 | Keyframe (redundant with type, kept for relay-safe forwarding) |
| 1 | Discontinuity — the receiver SHOULD reset its decoder before this AU |

## Handshake state machine

```
client                       server
  |  open bidi ctl stream      |
  |--------------------------->|
  |  HELLO                     |
  |--------------------------->|
  |                            |  validate slug constant-time;
  |                            |  reject with ERROR("auth") if mismatch
  |                            |
  |  SDP                       |
  |<---------------------------|
  |                            |
  |       ... media flows ...  |
  |  KEYFRAME (uni stream #1)  |
  |<===========================|
  |  PFRAME on stream #1       |
  |<===========================|
  |  PFRAME on stream #1       |
  |<===========================|
  |  RESET_STREAM #1           |
  |<- - - - - - - - - - - - - -|
  |  KEYFRAME (uni stream #2)  |
  |<===========================|
  ...
```

## Authentication

The server expects a 128-bit URL-safe base64 slug. The slug is
distributed out-of-band (e.g. URL fragment in a share link). The
server MUST compare the slug constant-time. The slug is the security
boundary; cert pinning + origin allow-list are defense in depth.

## Idle / liveness

The server SHOULD enforce an idle timeout based on **feed-write
progress**, NOT control-stream activity. A peer that knows the slug
could otherwise pin a session indefinitely by spamming PINGs while
never reading the feed (receiver-side flow control would block feed
writes, but heartbeats would keep an "any activity" timer alive).

PING/PONG MAY still be used by clients to measure RTT — they just
must not be load-bearing for liveness.

## Drop policy (server-side)

The reference broadcaster uses this policy:

- **Keyframes never dropped silently.** If a subscriber's queue is
  full, the oldest queued frame is evicted to make room.
- **P-frames droppable.** When dropped, the subscriber is flagged as
  "needs keyframe"; subsequent P-frames are skipped at fanout time
  until the next keyframe arrives.
- **Late-join replay.** A new subscriber receives the most recently
  cached keyframe immediately so it can begin decoding without
  waiting for the next natural IDR.

Implementations are not required to use this exact policy but
SHOULD ensure baseline H.264 receivers don't see cascading P-frame
loss without an opportunity to resync on a keyframe.

## Versioning

The wire-format version is in `HELLO.ver`. Servers SHOULD reject
mismatched majors. Currently shipping: `"1"`. v2 is reserved.

When v2 ships, HELLO will add a `tracks` array enumerating the
intended track set (with `name`, `kind`, `priority`, optional
`codec`), and feed frames will carry an optional metadata block:

```
[varint count]
  [varint key-len][key bytes]
  [varint val-len][val bytes]
  ...
[4 bytes SyncGroup]
[1 byte Priority]
```

between the existing 17-byte feed header and the payload. v2 peers
detect each other via `HELLO.ver = "2"`; v1↔v2 sessions fall back
to v1 framing.

## Out of scope (for v1)

- Multi-publisher pub/sub (the native relay is single-upstream
  fan-out; for multi-publisher conferencing, use an SFU).
- ANNOUNCE/SUBSCRIBE catalog (single static SDP suffices for one
  publisher).
- NAT traversal (out-of-scope by design — bring your own gateway,
  Tailscale, Cloudflare, etc.).
- Audio framing (planned for v2).
- DataChannel multiplexing (one channel per session in v1).
