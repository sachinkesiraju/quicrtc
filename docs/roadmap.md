# Roadmap

Planned work between v0.x and v1.0. See [`architecture.md`](architecture.md)
for positioning and [`spec.md`](spec.md) for the wire format.

## Tier 0 — make it shippable

### v0.1.0 release

Tag, push to pkg.go.dev, publish `quicrtc` 0.1.0 to npm.

**Acceptance:**
- `go get github.com/sachinkesiraju/quicrtc@v0.1.0` works.
- `npm install quicrtc@0.1.0` works.
- pkg.go.dev shows godoc for every public package.

### Runnable demo

`go run ./examples/publisher` + opening the share link in Chrome
should produce a moving test pattern with no manual ffmpeg or asset
fiddling. Bundle a tiny static H.264 IVF clip and loop it.

### README scope honesty

Add a "When NOT to use quicrtc" section so first-time readers see the
ceilings (browser-to-browser P2P, multi-publisher conferencing, audio
AEC/NS/AGC, turnkey NAT, native mobile).

## Tier 1 — polish what's there

### 1.1 Observability that means something

Replace generic "connections_total"-style counters with metrics an
operator would actually consult during an incident:

| Metric                                                  | Question it answers                                |
|---------------------------------------------------------|----------------------------------------------------|
| `feed_streams_opened_total{outcome}`                    | Did stream-per-GOP open succeed?                   |
| `feed_streams_reset_total{reason}`                      | Healthy GOP rotation vs. real errors               |
| `au_dropped_total{kind,reason}`                         | Drop-policy hit rate                               |
| `au_published_bytes` (histogram)                        | AU size distribution                               |
| `subscribers` (gauge)                                   | Live count per server                              |
| `handshake_duration_seconds` (histogram)                | HELLO → SDP latency                                |
| `path_migrations_total`                                 | QUIC connection migration events                   |

Exposed as `expvar.Var` plus optional `prometheus.Collector` behind a
build tag, so the default build has zero Prometheus dependency.

### 1.2 Datagram support

quic-go datagrams are enabled in our config but only used by the
telemetry kind today. Surface them via the public API for the same
use cases WebRTC reaches for SCTP unreliable mode: input forwarding,
60Hz position streams, low-latency control. Document the 1200-byte
practical limit.

### 1.3 Connection migration tested

QUIC migration (Wi-Fi → cellular without re-handshake) is inherited
from quic-go but untested through our session machinery. Add an
integration test that rebinds the client's UDP socket mid-session and
verifies no GOP gap. Surface `path_migrations_total` as a metric.

### 1.4 Handshake fuzzing

We fuzz `wire.ReadControlFrame`. Extend coverage to the full
handshake state machine: oversized JSON, nested objects, length-prefix
mismatches, unicode tricks. Target: `go test -fuzz=FuzzHandshake
-fuzztime=60s ./session/` runs clean for a minute with no panics.

### 1.5 Conformance vectors

A bytes-level test vector file (input frame → expected parsed values)
that the Go `wire` package and the TypeScript SDK both consume. Wire
spec stays the contract; any port (Python, Rust, ...) can verify
compliance against the same vectors.

## Tier 2 — production hardening

### 2.1 Publisher-side backpressure

`Publisher.Publish` is fire-and-forget today; the broadcaster's drop
policy handles slow subscribers, but the publisher has no signal.
Real consumers want to trade encoder bitrate for delivery when the
slowest subscriber is at risk.

- `(*Server).SubscriberStats() []SubscriberStats` with per-sub queue
  depth, drops, last-keyframe age.
- `Config.OnSlowSubscriber func(SubscriberStats)` callback.
- Documented encoder-feedback pattern.

### 2.2 Graceful shutdown

`(*Server).Shutdown(ctx) error` mirroring `http.Server.Shutdown`:
stop accepting new sessions, send `TypeClose` to existing sessions,
wait up to a deadline, force-close. Integration test verifies
in-flight subscribers exit `Recv` with `io.EOF`, not a stream reset.

### 2.3 Rate limiting at the upgrade boundary

Per-IP token bucket on `/wt` upgrade attempts plus per-slug max-sessions.
Configurable via `Config.RateLimit{PerIP, PerSlug}`.

### 2.4 Cert rotation without dropped sessions

The ephemeral cert is good for 13 days. Production needs to re-cert
before expiry without breaking live sessions: server holds N certs
simultaneously, new connections get the newest, existing sessions
stay on their original cert.

### 2.5 Quality layers (simulcast-lite)

K independent encodes from one publisher fanned out through K
broadcasters. Subscribers pick a layer at HELLO and can switch
mid-session via `0x08 SWITCH`. Not full SVC. This forces the v1→v2
wire bump.

### 2.6 Multi-track sessions

Named tracks per server with independent SDP, broadcaster, and feed
lifecycle. Subscribers list tracks in the SDP response and pick which
to subscribe via `0x09 SUBSCRIBE`. Piggybacks on the v2 wire bump from
2.5.

## Tier 3 — only when a real consumer asks

- **Real-time audio (Opus + FEC + jitter buffer + NACK)** for voice
  agents.
- **IETF MoQ interop** if MoQT graduates to RFC. Earlier draft-17
  implementation was deleted — measured overhead vs native was ~0.26ms
  on loopback, not worth maintaining two wire formats while the draft
  was still moving.
- **Python port** (wraps `aioquic`).
- **Node port** (wraps `@fails-components/webtransport`).
- **Native mobile (Swift / Kotlin)** via Cloudflare quiche +
  UniFFI/JNI. Defer until a real mobile consumer asks; browser path on
  iOS Safari 26.4+ covers most cases.
- **Recording / pipeline tee** as a documented pattern, not a feature.

## Out of scope

| Proposed                                  | Why we reject                                              |
|-------------------------------------------|------------------------------------------------------------|
| ICE / STUN / TURN                         | Reintroduces WebRTC's NAT machinery; quicrtc is server-centric |
| WebSocket fallback                        | WebTransport hit Baseline Mar 2026; fallback adds parallel wire format and reintroduces TCP HOL |
| Hand-rolled bandwidth estimator           | Duplicates quic-go's Cubic/BBR; two controllers oscillate |
| JWT / MFA inside the lib                  | Application-layer concern; use `AuthValidator`             |
| Compression in `datachannel`              | Codec payloads are already compressed; net negative        |
| Adaptive quality inside the lib           | Fights with the encoder's rate control; expose stats and let the consumer decide |
| CLI scaffolder (`quicrtc init`)           | Docs and a working README example do this better           |

## Success metrics

| Metric                                    | Target                                       |
|-------------------------------------------|----------------------------------------------|
| Time-to-first-frame on `examples/publisher` | < 200ms p50, < 500ms p99 on loopback        |
| Wire conformance vector coverage          | 100% of frame types × edge cases             |
| Race detector                             | Clean under `go test -race -count=10` weekly |
| Fuzz survives                             | 5 min/target with no new crashers nightly    |
| Spec/impl drift                           | Zero — spec change requires Go + JS PR in same commit |
| pkg.go.dev godoc coverage                 | Every exported symbol has a doc comment      |
