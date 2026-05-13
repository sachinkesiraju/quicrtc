# Architecture

quicrtc is a single transport — native QUIC + WebTransport — under a
`PeerConnection`-style API. Same wire format end-to-end for direct
publisher↔subscriber sessions and for relayed 1:N fan-out via the
`relay` package.

For the wire format, see [`spec.md`](spec.md). For planned work, see
[`roadmap.md`](roadmap.md).

## Why a new transport

WebRTC was designed for browser↔browser video chat: symmetric,
peer-to-peer, codec-gated by libwebrtc. AI streams have the opposite
shape — server-originated, asymmetric, multi-modal (video, audio,
tokens, tool-calls, telemetry concurrently from one pipeline to many
viewers). WebRTC handles this by faking a media source on the server,
gating non-standard codecs, and bolting datachannels alongside RTP for
non-media data.

quicrtc starts from the server-originated, multi-modal assumption.
Codec-agnostic AccessUnit, kind-tagged tracks, datachannels and
datagrams alongside media, all on one QUIC session.

## Layers

```
PeerConnection (server/, client/, peerconnection/)
   AddTrack / OnTrack / DataChannel
   │
   ▼
Pump (feed/)
   DeliveryClass dispatch: StreamGOP | StreamLowLatency
                           BidiPerCall | DatagramOrStream
   │
   ▼
Wire (wire/)
   17-byte feed frame | 4-byte datagram envelope | control frames
   │
   ▼
QUIC + WebTransport
   quic-go, webtransport-go, TLS 1.3, connection migration
```

Two deployment shapes on the same wire:

```
Direct:   Publisher ───────────────────────────── Subscriber

Fan-out:  Publisher ──► Relay (relay package) ──┬── Subscriber A
                                                 ├── Subscriber B
                                                 └── Subscriber N
```

The relay is a `server.Server` that also dials an upstream publisher
as a `client.Client` and forwards AUs through its local broadcaster —
no parse/re-encode, just splice forwarding. Subscribers can't tell
they're talking to a relay vs a direct publisher.

## Delivery classes

Each track has a `Kind` that maps to a `DeliveryClass`. The pump
dispatcher routes AUs to the matching path:

| Kind                | DeliveryClass       | Stream shape                                       |
|---------------------|---------------------|----------------------------------------------------|
| `KindVideo`         | `StreamGOP`         | One uni stream per keyframe; RESET on next GOP     |
| `KindTokens`        | `StreamLowLatency`  | One persistent uni stream per track; pooled encoder |
| `KindToolCalls`     | `BidiPerCall`       | One bidi stream per AU (RPC-shaped)                |
| `KindTelemetry`     | `DatagramOrStream`  | QUIC datagrams with stream fallback intent        |

`KindAudio` uses `StreamLowLatency` today; real-time audio framing
(Opus + FEC + jitter buffer) is planned for Phase 3.

## Public API

```go
import (
    "github.com/sachinkesiraju/quicrtc/server"
    "github.com/sachinkesiraju/quicrtc/track"
)

srv, _ := server.New(server.Config{
    Addr: ":4433",
    SDP:  wire.SDP{Codec: "avc1.42E01F", Width: 1280, Height: 720, FPS: 30},
})

videoPub := srv.AddTrackSpec(server.TrackSpec{Name: "screen",    Kind: track.KindVideo,     Priority: 4})
tokenPub := srv.AddTrackSpec(server.TrackSpec{Name: "reasoning", Kind: track.KindTokens,    Priority: 2})
telePub  := srv.AddTrackSpec(server.TrackSpec{Name: "telemetry", Kind: track.KindTelemetry, Priority: 7, TrackID: 0x42})

videoPub.Publish(ctx, pubsub.AccessUnit{Bytes: frame, Keyframe: true, Seq: 0})
```

The `peerconnection` package wraps `server`/`client` for callers who
want a single `PeerConnection` ergonomics layer. See
[`../README.md`](../README.md) for usage.

## What we are and aren't

Built for:
- Server-originated AI streams (token streaming, voice agents, AI
  avatars, computer-use agents, robotic teleop, live inference SaaS).
- 1:N broadcast via the native relay.
- Multi-modal: video + audio + tokens + tool-calls + telemetry in one
  session.

Not built for:
- Browser↔browser P2P (browsers can't accept WebTransport).
- Video conferencing ecosystems (LiveKit / Daily / Mediasoup own
  that).
- Turnkey NAT traversal without a server.
- Native iOS / Android (Phase 4).

The top-level README points to `pion/webrtc` or libwebrtc for those
cases.

## Risks worth naming

1. **HELLO v2 is a wire bump.** v1 servers reject v2 HELLOs cleanly,
   but consumers on v1 will need a migration path when v2 lands.
2. **`PublishBack` doubles the bidi-feed surface.** Client→server uni
   streams need the same testing rigor as server→client.
3. **Single wire format = no IETF MoQ interop today.** If a partner
   needs Cloudflare-MoQ-relay or moq-js receivers, MoQT goes back on
   the roadmap as a parallel transport.
