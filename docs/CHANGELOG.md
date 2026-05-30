# Changelog

All notable changes to quicrtc. Versions follow semver from 1.0
forward: minor versions add API surface back-compatibly; patch
versions fix bugs and tighten existing surface.

## [Unreleased]

Reliability + tooling work on the `feat/cua-reliability-and-demo` branch,
validated against real GCP cross-region WAN.

### Fixed

- **`feed/bidi.go`** — `BidiPerCall` now bounds the response read by
  `BidiCallTimeout` (previously only stream-open was bounded, so a
  fire-and-forget call could pin a stream slot until the peer FIN'd).
  Calls with no `OnBidiResponse` cancel the read immediately instead of
  waiting for a response that may never arrive.
- **`client/client.go`** — client-published tracks pick a delivery class
  from `Kind` instead of always using stream-per-GOP. A 100-action/sec
  `KindToolCalls` track no longer opens ~100 uni streams per second;
  non-video kinds ride one persistent low-latency stream.

### Added

- **`record.BuildTimeline` / `SeekSnapshot`** + the `examples/replay` CLI
  (`-gen` synthesizes a sample) — frame-aligned session replay of what the
  agent saw, reasoned, and did on one capture clock.
- **`examples/cua-live`** — a real (non-mocked) computer-use agent demo,
  isolated as a nested module so chromedp stays out of the library graph.
- **`ts-sdk/examples/compare`** — browser side-by-side of naive vs
  multistream lane isolation.
- **`server.SchedulerMode`** + cooperative chunked pacing in
  `feed/pump.go`, so the priority scheduler can preempt a bulk frame for a
  latency-sensitive AU. The scheduler stays **opt-in** (`SchedulerOn`):
  auto-enabling it for mixed-priority sessions regressed the loopback
  computer-use benchmark (pure overhead without real contention).
- **`testing/wan_bench`** computer_use headline reports p99.9 + max, plus
  the `run_computer_use_wan.sh` runner.

### WAN validation — the computer-use tail is clean (the "30 s stall" was a benchmark bug)

The bidi/client-delivery fix above was first committed as "kill the 30s
computer-use action tail." Real-WAN re-runs (GCP us-east1 ↔ us-west1) showed
a ~30 s `max` *persisting* — but per-leg instrumentation found it was a
**measurement bug in the wan_bench harness**, not a transport stall. The
shared `cu_dom` broadcaster replays its cached keyframe to each new
subscriber (correct for video late-joiners); because DOM echoes mark
`seq==1` a keyframe, every new trial's client got the *previous* trial's
stale DOM and counted its trial-old timestamp as a ~30 s "RTT." Trial 0
(empty cache) never stalled — the tell.

Fix (`testing/wan_bench/computer_use.go`): the client discards any DOM
stamped before the trial began. Over 5 trials / 6000 actions: **zero
stalls, quicrtc max 91 ms** (down from the bogus 30,932 ms), beating the
TCP-mux baseline at every tail point — p99 1.95×, p99.9 ~2.8×, max 2.94×
lower. The bidi/client-delivery changes remain valid correctness fixes.
See `testing/wan_bench/README.md` for the table.

## [1.0.2]: Production hardening

This is the 1.0 cut: the wire format, server / client / session
APIs, and the TS SDK public surface are stable. Existing v0.1
clients connect to v1.0.2 servers without changes; all new wire
fields and control frames are additive and feature-gated.

Zero new third-party Go dependencies. The Redis-backed
`DistributedSessionStore`, cloud recorder backends, and the
Prometheus metrics adapter are planned as separate sibling repos
so the core stays dependency-light; until those land, integrators
can write thin adapters directly against the interfaces in
`server/session_store.go`, `record/`, and `metrics/`.

### Added: broadcaster primitive

- **`pubsub.Broadcaster.AddInterceptor`** registers a function in
  the per-broadcaster interceptor chain. Interceptors can transform
  an AU, drop it (return `ErrDropAU`), or pass it through. The
  chain runs at the top of `Publish` before the size-cap re-check,
  so a buggy interceptor that grows `au.Bytes` cannot bypass the
  16 MiB ceiling. Hot-path overhead with no interceptors is one
  RWMutex RLock plus a length check (~25 ns uncontended).
- **`pubsub.ErrDropAU`** is the sentinel an interceptor returns
  for a silent drop (no fanout, no replay-buffer write).
- **`pubsub.Receiver.QueueDepth()`** exposes the receiver
  channel's current buffered count for observability.
- Interceptors are the substrate for recording, per-track auth
  filtering, and metrics export. Multiple registrations compose;
  the returned removal function is idempotent.

### Added: wire version negotiation

- **`wire.Hello.MinVersion` / `MaxVersion`** declare the inclusive
  range of wire versions the client can speak. Both are
  `omitempty`; clients that send only `Version` are treated as
  `min=max=Version` (the v0.1 behavior).
- **`wire.SDP.NegotiatedVersion`** echoes the chosen version back
  to the client.
- **`wire.NegotiateVersion`** picks the highest version both peers
  can speak. The handshake rejects mismatched ranges explicitly
  rather than silently downgrading.
- **`wire.MinSupportedVersion`** is the floor this build can still
  speak; `wire.CurrentVersion` remains the build's preferred
  version.
- **`session.Session.NegotiatedVersion`** surfaces the agreed
  version to applications that need to gate per-version behavior.

### Added: multi-instance session resume

- **`server.EvictableStore`** is an optional capability interface
  for stores that support background eviction sweeps. The
  in-memory store implements it; remote stores that lean on
  backend TTLs (e.g. Redis) implement neither.
- **`server.ClosableStore`** is an optional capability interface
  for stores that need to release backend resources on shutdown.
- **`server.DistributedSessionStore`** is an optional capability
  interface for stores that publish session metadata across
  instances. Receivers stay in-process; the store publishes
  enough metadata (instance ID, expiry, last-seen sequences) for a
  resume request that lands on the wrong instance to be redirected
  to the right one. A Redis-backed reference implementation is
  planned as a sibling repo.
- **Pre-Upgrade routing handler.** The `/wt` handler accepts a
  `?session=<id>` query param; when the store is distributed and
  the session lives elsewhere, the handler returns a 307 redirect
  before `wts.Upgrade` hijacks the response. Single-instance
  deployments are unaffected.
- **Background eviction loop.** When the store implements
  `EvictableStore`, the server runs a periodic sweep
  (`Config.EvictionInterval`, default 30 s). Idle servers now
  reclaim expired parked sessions without waiting for new traffic.
- **`server.Config.InstanceID` and `InstanceAddr`** identify the
  server to a distributed store. Auto-generated when empty.

### Added: per-track authorization

- **`server.Config.OnAnnounce`** gates inbound `TypeAnnounce`
  frames from subscribers. Called with the authenticated tenant,
  session ID, track name, and kind. Returning a non-nil error
  emits a `wire.ErrorPayload{Code: "track_unauthorized"}` and
  leaves the track unregistered. Without this set, all
  PublishBack announces are accepted (legacy single-tenant
  behavior).
- **Stream-header-time race fix.** When `OnAnnounce` is set, a uni
  stream whose stream header names a not-yet-announced or rejected
  track is dropped before any AU is enqueued. Closes the window
  where a subscriber could race an Announce by opening a stream
  for the same track name first.
- **`wire.ErrorPayload{Code, Reason}`** is the structured envelope
  for `TypeError` frames. JSON payloads round-trip into the
  struct; legacy plain-bytes payloads decode as
  `{Code: "", Reason: <bytes>}` so old servers stay readable.

### Added: session recording and replay

- **`record/` package.** New, in-tree, no third-party
  dependencies. Built on the interceptor primitive.
- **`record.NewFileRecorder` / `NewFileRecorderDepth`** open an
  append-only binary log. The recorder uses a buffered channel +
  single writer goroutine, so the broadcaster hot path never
  blocks on disk I/O. Bytes are copied at enqueue time;
  channel-full overflow increments `Dropped()` rather than
  blocking the publisher.
- **`record.Replayer.PublishInto`** reads a session log and
  invokes a routing callback per frame, optionally preserving
  original inter-frame timing scaled by a speed multiplier.
- **`server.AttachRecorder`** wires a recorder to every track's
  broadcaster as an interceptor and auto-attaches to tracks added
  later. Cloud backends (S3, GCS, Azure) are planned as the
  `quicrtc-storage` sibling repo.

### Added: per-Kind QoS observability

- **`wire.TypeKindStats` (0x0e)** is a new control-frame type for
  subscriber-to-publisher per-Kind observability reports, sent at
  ~1 Hz when the `"kind-stats"` feature is negotiated.
- **`wire.KindStats{Kind, LastSeq, RecvP50Ms, RecvP99Ms, Dropped}`**
  is the JSON payload. Lets the publisher see true end-to-end p99
  per lane without guessing from send-side queue depth alone.
- **`session.Config.OnKindStats`** and **`server.Config.OnKindStats`**
  surface the reports to applications.
- **`metrics.KindMetrics`** is an optional capability interface
  (methods `RecordKindLatency`, `RecordKindQueueDepth`,
  `RecordCongestion`)
  that `metrics.Metrics` implementations can additionally
  implement. Existing `Metrics` impls (`NoopMetrics`, third-party
  adapters) continue to compile unchanged.
- **`feed.Pump.Stats() PumpStats`** exposes the pump's outbound
  queue depth and oldest-AU age via atomic loads. Backed by an
  `atomic.Pointer[pubsub.Receiver]` set when `Run` starts.
- **`transport.CongestionState`** is a new interface
  (`EstimatedSendBandwidth`, `SmoothedRTT`, `BytesInFlight`,
  `SendBufferAvailable`) implementations wrap around the
  underlying QUIC connection's state plus a rolling rate counter.
- **`transport.SendRateMeter`** is a sliding-window byte-rate
  counter useful for adapters that need to derive a bandwidth
  estimate when the transport doesn't expose one directly.
- **`server.DefaultSupportedFeatures`** now includes
  `"kind-stats"`.

### Added: request correlation convention

- **`AccessUnit.Meta` reserved keys.** `"request_id"`,
  `"trace_id"`, and `"span_id"` are now documented conventions for
  application-level correlation (especially useful for parallel
  tool calls and W3C-style distributed tracing). No wire change;
  the field already existed and round-trips end to end.

### Added: TS SDK viewer subpath

- **`quicrtc/viewer`** (new subpath export). `AgentSession`
  extends `EventTarget` and dispatches typed
  `state-change` / `track-added` / `track-removed` / `kind-stats` /
  `au` / `datagram` / `reconnected` / `error` events. `on(s, type,
  listener)` is a typed helper.
- **AbortSignal support.** `AgentSession.recv` / `sendDatagram` /
  `addTrack` accept an optional `signal: AbortSignal`. Aborted
  signals reject pending operations with a `DOMException` of
  name `AbortError`.
- **`quicrtc/viewer/react`** (new subpath export).
  `useAgentSession(opts)` is a React hook returning typed live
  state. React is a peer dependency injected via `setReact(React)`
  at app startup; the core SDK does not import React.
- **TS wire support.** `marshalKindStats` / `unmarshalKindStats`,
  `unmarshalError` (with legacy plain-bytes fallback), and
  `TypeKindStats = 0x0e` are exported.
- **`package.json` subpath exports** (`.`, `./viewer`,
  `./viewer/react`). React listed as an optional peer dependency.
- **`scripts/size-check.mjs`** gates the core gzipped bundle size
  in CI. Current threshold (22 KiB) reflects tsc-only output;
  tightens to 8 KiB once minification lands.

### Added: datagram-to-stream fallback (closes a documented gap)

- **`feed.Config.FallbackOpener`** lets the `DeliveryDatagramOrStream`
  pump spill onto a persistent uni stream when an AU cannot ride the
  datagram path. Two trigger conditions: the encoded payload exceeds
  `MaxDatagramPayload` (1100 bytes), or `SendDatagram` returns an
  error (queue full, path-MTU change). Previously both conditions
  dropped the AU with `OnAUDropped("datagram_too_large")` or
  `"send_failed"`. The class name promised a stream fallback that
  did not exist.
- **`feed.Config.OnFallback(reason string)`** fires whenever an AU
  is routed through the fallback rather than dropped, so observers
  can count spillover activations separately from hard losses.
- New `OnAUDropped` reasons: `"fallback_open_failed"` (FallbackOpener
  was set but opening the uni stream failed; AU is dropped) and
  `"fallback_write_failed"` (fallback stream existed but the write
  errored; stream is torn down, AU is dropped, next fallback
  re-opens).
- The fallback stream is **lazy and persistent**: opened on the
  first spillover, reused for subsequent ones, closed on pump
  shutdown. Steady-state cost of a stream of oversize AUs is one
  open, not one per AU.
- `server.Server` wires the FallbackOpener automatically: the same
  `uniOpener` that backs stream-based tracks. No configuration
  required for users of the high-level server.

### Added: per-session priority scheduler wired into the pumps

- **`feed.Config.Scheduler`** plumbs an existing-since-0.1.0
  priority queue (`feed.Scheduler`) into the per-track pumps. When
  set, each pump's AU write is submitted to the shared scheduler
  with the track's `Priority`. The scheduler's single worker drains
  items in priority order, so a high-priority track's queued write
  preempts a low-priority track's queued write at AU granularity.
  Before this change the Priority hint was emit-only observability
  with no effect on actual write order.
- **`server.Config.UsePriorityScheduler`** opts a session into
  per-track priority gating. The server creates one
  `feed.Scheduler` per session, plumbs it into every per-track
  `feed.Config`, and closes it when the session ends. Off by
  default: the scheduler adds a Submit/Wait roundtrip per AU which
  is measurable for sessions that don't carry mixed kinds.
- Honest semantic, documented inline: this gives application-level
  Submit-ordering at AU granularity, NOT RFC 9218 byte-level
  interleaving. quic-go does not yet expose `PRIORITY_UPDATE` on
  its public API; when it does we will plumb that through and the
  Submit gate becomes redundant.
- `feed.Scheduler.Submit` now returns `bool` (true on enqueue,
  false if the scheduler is closed) so callers that synchronize on
  an in-Do signal can take the inline fallback path instead of
  blocking forever. `scheduledDo` uses this to avoid a goroutine
  leak on session shutdown.

### Compatibility

- Wire-format: every new field and frame type is additive and
  feature-gated. v0.1 clients connecting to v1.0.2 servers see
  no changes. v1.0.2 clients connecting to v0.1 servers negotiate
  down (no `"kind-stats"`, no neg_ver in SDP).
- API: `SessionStore` interface unchanged. The new
  `EvictableStore`, `ClosableStore`, and `DistributedSessionStore`
  are separate capability interfaces, type-asserted at runtime.
  External `SessionStore` implementations compile and run
  unchanged.
- `metrics.Metrics` interface unchanged. `KindMetrics` is a
  separate optional interface.

### Sibling repos (planned, separate Go modules)

These ship outside the core repo so the core stays dependency-light.
None has been published yet; each one is a thin adapter against
interfaces that are already stable in v1.0.2.

- **`quicrtc-redis`**: Redis-backed `DistributedSessionStore`
  implementation for multi-instance deployments without sticky
  load-balancer routing.
- **`quicrtc-storage`**: cloud recorder backends (S3 / GCS /
  Azure Blob) implementing the `record.Recorder` interface.
- **`quicrtc-prometheus`**: Prometheus collectors satisfying the
  core `metrics.Metrics` (and optionally `metrics.KindMetrics`)
  interfaces.

[1.0.2]: https://github.com/sachinkesiraju/quicrtc/releases/tag/v1.0.2

## [0.1.1] — Patch release

### Fixed — wire correctness

- **`MaxFeedPayload` off-by-one (silent corruption).** The constant was
  `16 * 1024 * 1024 = 0x1000000`, but the on-the-wire feed-frame length
  field is 3 bytes (cap `0xFFFFFF`). A payload of exactly 16 MiB passed
  validation, then the encoder wrote length `0` and the next frame
  desynced the stream. Constant is now `0xFFFFFF`, and the pooled
  encoder rejects oversize payloads explicitly. The same length check
  is now exercised by `TestMaxFeedPayloadBoundaryRoundTrip`.
- **TypeScript `TrackKind.ToolCalls` wire value.** Was `"tool_calls"`;
  Go's `track.KindToolCalls` is `"toolcalls"` — the kind string on
  every Announce frame. Mismatch caused TS viewers to silently route
  incoming tool-call tracks to the tokens panel. Now matches Go.
- **TS SDK accepts incoming bidirectional streams.** `KindToolCalls`
  tracks from Go publishers are delivered to the browser SDK
  (previously dropped on the floor because the TS client never called
  `incomingBidirectionalStreams.getReader()`).

### Fixed — server stability and safety

- **`handleUnannounce` race-panic.** Closing the inbound track's recv
  channel while `drainInboundStream` was still sending on it crashed
  the session goroutine with "send on closed channel" on legitimate
  client behavior. Replaced with a `done` channel that senders and
  readers select against, so close is one-shot and race-free.
- **`AddTrackSpec` silently demoted `PriorityCritical`.** Spec
  `Priority: 0` was treated as "unset" and overridden to
  `PriorityNormal` (4). Since `PriorityCritical` is also `0`, callers
  could not register critical-priority tracks via the modern entry
  point. Now defaults via the new `track.DefaultPriority(kind)` —
  audio tracks get `PriorityCritical`, video/tokens/tool calls get
  `PriorityHigh`, telemetry gets `PriorityBackground`.
- **`OnSession` now fires after handshake.** The callback previously
  ran synchronously with `session.New`, before the HELLO/SDP exchange
  authenticated the peer. Applications received a `SessionHandle`
  backed by an unauthenticated connection. Now wired through a new
  `session.Config.OnHandshakeComplete` callback that fires post-auth
  with `SessionID` populated.
- **`runBidiPerCall` response read is bounded.** `io.ReadAll` was
  unbounded; an authenticated peer could OOM the server with a large
  bidi-stream response. Now capped by a new `feed.Config.MaxBidiResponse`
  (default `DefaultMaxBidiResponse` = `wire.MaxFeedPayload`); the read
  is wrapped in `io.LimitReader` and the stream is `CancelRead`d on
  cap-hit to release flow-control credit.
- **`ShareLink` produces valid URLs for IPv6 hosts.** IPv6 host strings
  now get bracket-wrapped (RFC 3986 §3.2.2).

### Added — public API

- **`track.DefaultPriority(kind Kind) Priority`** — per-Kind default
  priority lookup. Used by `server.AddTrackSpec` so `spec.Priority=0`
  picks the right default per Kind.
- **`session.Config.OnHandshakeComplete`** — fires from `Run` after
  handshake succeeds and `SessionID` is populated.
- **`session.Config.OnInboundDropped`** — symmetrical with
  `feed.Config.OnAUDropped` for the inbound (PublishBack) direction.
  Lets operators see receiver-side AU drops (previously silent).
- **`feed.Config.MaxBidiResponse`** — caps bytes read off each
  BidiPerCall response stream. Zero picks `DefaultMaxBidiResponse`.
- **`feed.DefaultMaxBidiResponse`** + **`feed.ErrBidiResponseTooLarge`**.
- **TS SDK populates `Hello.last_seen` on reconnect.** Closes the
  in-flight gap on resume in one round-trip rather than relying on
  post-handshake `Resume` frames per track (those still ship for
  back-compat).

### Changed — TS SDK

- `drainBidiStream` falls back to `writable.abort()` when
  `writable.close()` fails, so the server's `io.ReadAll` returns
  immediately rather than blocking for `BidiCallTimeout` (30 s) per
  AU and leaking a goroutine + stream per failure.

[0.1.1]: https://github.com/sachinkesiraju/quicrtc/releases/tag/v0.1.1

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
- `server.SessionHandle.InboundRecv` — per-session receive of a track
  the *subscriber* published (PublishBack). Unlike `Server.AnySession`,
  this is scoped to the originating session, so AUs from other
  subscribers do not mix in.
- Post-handshake Announce replay — clients see `TypeAnnounce` for every
  pre-existing server-side track right after the SDP response, so
  `RemoteTracks()` is populated without waiting for the first uni
  stream's stream-header.
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
[`SPEC.MD`](SPEC.MD).

[0.1.0]: https://github.com/sachinkesiraju/quicrtc/releases/tag/v0.1.0
