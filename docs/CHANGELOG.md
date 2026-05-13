# Changelog

All notable changes to quicrtc. Pre-1.0: minor versions may include
breaking API changes; patch versions don't.

## [0.1.0] — Initial public release

### Added — protocol

- **DeliveryClass abstraction.** Each track's `Kind` maps to one of
  four delivery shapes that the pump dispatcher routes to:
  - `StreamGOP` — one uni stream per keyframe (video).
  - `StreamLowLatency` — one persistent uni stream per track, pooled
    encoder, no per-AU stream churn (tokens, terminal output).
  - `BidiPerCall` — one bidi stream per AU, treated as a logical RPC
    so parallel calls don't HOL-block under loss (tool calls).
  - `DatagramOrStream` — QUIC datagrams via a 4-byte envelope, with
    stream fallback intent (telemetry).
- **Subscriber-driven keyframe request.** New `NeedsKeyframe` field
  on `wire.Backpressure` piggybacks a "flush a keyframe NOW" signal
  to the publisher; cuts visible-stutter recovery from ~one GOP
  duration to ~one frame interval.
- **4-byte datagram envelope** (`wire.EncodeDatagram` /
  `DecodeDatagram`). Lets multiple logical telemetry tracks share
  one datagram pipe with cheap demux.
- **App-layer priority scheduler** (`feed.Scheduler`). Cross-track
  write ordering with weighted preemption. quic-go has no native
  stream priority and RFC 9218 is HTTP-only, so this lives entirely
  above quic-go.
- **`AddTrackWithTrackID`** on `server.Server`; **`AttachTrackWithTrackID`**
  and **`OnLookupTrackID`** on `session.Session` for per-track
  datagram-envelope ID negotiation.

### Added — public APIs

- **`server.TrackSpec`** and **`Server.AddTrackSpec(spec)`** — the
  canonical entry point for adding tracks. Carries `Name`, `Kind`,
  `Priority`, and `TrackID`. The `Kind` field selects the per-track
  delivery dispatch; without it, tracks fall back to the legacy GOP
  path. Preferred over the `AddTrack*` ladder for any caller who
  wants the per-Kind pumps.
- **`session.Session.AttachTrackWithKind`** — attach entry point that
  carries `Kind` explicitly; the legacy `AttachTrackWithTrackID`
  consults `OnLookupKind` to recover Kind for callers that haven't
  migrated.
- **`session.Session.OnLookupKind`** callback, populated by
  `server.Server` from the per-track `trackEntry.kind`. Symmetrical
  with the existing `OnLookupPriority` / `OnLookupTrackID` callbacks.
- The server's per-session transport adapter wires
  `feed.DatagramSender` and `feed.BidiOpener` into `FeedConfig`
  automatically, so `KindTelemetry` / `KindToolCalls` tracks don't
  silently fall back to GOP when those hooks are missing.
- `client.Client.RequestKeyframe(trackName)` — sub→pub keyframe ask.
- `client.Client.SendDatagram(payload)` / `ReceiveDatagram(ctx)`.
- `server.Config.OnKeyframeRequest` callback.
- `server.Config.OnSession` callback + `server.SessionHandle` type
  exposing per-session `SendDatagram` / `ReceiveDatagram` / `Context`.
- TypeScript SDK: `QuicRTCClient.requestKeyframe`,
  `sendDatagram`, `receiveDatagram`; module-level
  `encodeDatagram`, `decodeDatagram`, `DatagramTooLargeError`.

### Added — benchmarks

- `feed/`: `BenchmarkTokenPump_GOP` / `_LowLatency`,
  `BenchmarkTelemetry_StreamReliable` / `_Datagram`,
  `BenchmarkBidiPerCall_Idle`,
  `BenchmarkScheduler_Submit` / `_SubmitDispatch`.
- `wire/`: `BenchmarkEncodeDatagram_Small` / `_NearMTU`,
  `BenchmarkDecodeDatagram`.
- `benchmarks/`: end-to-end real-QUIC tests for token streaming,
  datagram-vs-stream telemetry contention, keyframe recovery, and a
  competitor baseline against SSE-over-HTTP/2.

### Added — guardrails

- `TestLowLatencyEncodeMatchesWire` asserts the inline pooled
  encoder is byte-identical to `wire.WriteFeedFrame`.
- `TestDatagramEncodeDecodeRoundTrip` exercises the 4-byte envelope
  across six size profiles including the MTU edge.

### Changed

- `runStreamLowLatency` now lazily retries stream-open on each AU
  instead of returning an error that kills the pump; surfaces
  `OnAUDropped("stream_open_failed")` for observability.
- `OnBidiResponse` callback signature gained an `err` parameter so
  partial responses from cancel/timeout are distinguishable from
  successful completion.
- `feed.Scheduler.Close()` now uses a shutdown channel internally so
  closing without ctx cancellation no longer leaks the ctx-watcher
  goroutine.
- `DatagramHeaderLen` typed as untyped `int` constant (was `byte`).

### Compatibility

- Legacy `Server.AddTrack` / `AddTrackWithPriority` / `AddTrackWithTrackID`
  remain on the public surface. They default the missing `Kind` to
  `KindVideo`. Migrate to `AddTrackSpec` to get the per-Kind pumps.
- Callers that pre-set `session.Config.FeedConfig.Delivery` continue
  to have that respected when `Kind` is unset — the explicit-Kind
  assignment is conditional on `kind != ""`, so the "pin Delivery on
  the session config" escape hatch is preserved for tests and
  single-kind deployments.

### Wire format

v1 is the stable target. Receivers silently ignore unknown control
frame types, so v1.x additions are forward-compatible. See
[`spec.md`](spec.md).

[0.1.0]: https://github.com/sachinkesiraju/quicrtc/releases/tag/v0.1.0
