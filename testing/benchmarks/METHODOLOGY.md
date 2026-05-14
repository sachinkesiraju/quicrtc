# Benchmark methodology

How the benchmarks measure, what they intentionally do not measure,
and how to reproduce them.

## What's compared

- **quicrtc against itself** — the legacy stream-per-GOP path vs. the
  kind-aware delivery classes, to isolate the architectural win.
- **quicrtc vs. pion/webrtc/v4** — like-for-like Go WebRTC comparison.
- **quicrtc vs. SSE over HTTP/2** — industry-standard LLM token
  delivery baseline.

Out of scope: libwebrtc (C++), LiveKit / Mediasoup (production SFUs),
MoQ (different architecture), real WAN deployments. Structural wins
reproduce on loopback; running on a WAN would scale absolute numbers
while preserving ratios.

## Workload pacing

- Token streams: 200/sec, 64-byte payload (typical LLM emission rate).
- Video: 30fps, 50KB frames, 1 keyframe/sec.
- Telemetry: 100Hz, 48-byte payload.
- Tool calls: parallel bidi streams, one stalled mid-call to test HOL
  isolation.

Synthetic deterministic payloads — no real encoders involved, so
encoder variance doesn't enter the measurement. All payloads carry an
8-byte UnixNano timestamp prefix; receiver computes end-to-end latency
single-clock.

## Network impairment

`benchmarks/internal/loadgen/proxy.go` is a Go-level approximation of
`tc qdisc add dev lo netem`:

- Configurable one-way delay (e.g., 25ms = 50ms RTT).
- Per-packet drop probability.
- Optional uniform jitter.

It's not perfect — goroutine scheduling adds its own jitter, and
bottleneck-bandwidth queuing isn't modeled — but it captures the
dominant effects (RTT, loss) needed to validate the structural claims.
Required so the suite runs on macOS / Windows / cloud CI without root.

## Statistical floor

- `loadgen.ComputeStats` reports n, min, mean, p50, p95, p99, max.
- Sample sizes: ≥100 per condition for stable p99 estimates, kept
  under ~5s wall-clock per test so the full suite fits in 1-2 minutes.
  - Current state (some tests below the floor; tracked for re-runs):
    resume n=30, keyframe-recovery trials=5, multimodal n=400,
    fanout broadcast frames=60. Tests below n=100 should not be cited
    for p99 — treat them as max-of-N only.
- Resume benchmark adds bootstrap CIs on the mean (1000 resamples) so
  setup-latency claims are accompanied by uncertainty bounds.

## Reporting

Each benchmark logs a one-line summary for the headline number plus
the full distribution (mean, p50, p95, p99, max). The README's
performance table pulls headline rows from these.

## Test layout

Each subpackage under `benchmarks/` is one topic:

| Subpackage | Test(s) | Asks |
|---|---|---|
| `agent/` | `TestApp1ComputerUseClosedLoop` | action→DOM RTT p99, 4 concurrent tracks |
| `video/` | `TestKeyframeRecoveryComparison` | Time-to-fresh-frame after loss |
| `tokens/` | `TestTokenStreamingVsSSE` | StreamLowLatency vs. SSE-over-HTTP/2 |
| `telemetry/` | `TestDatagramVsStreamContention` | Datagram vs. stream during video burst |
| `multimodal/` | `TestMultimodalIsolation_*` | Token p99 during concurrent video burst |
| `fanout/` | `TestLiveBroadcast_*`, `TestRelayOverhead` | Per-sub p99 at N=4; relay overhead |
| `resume/` | `TestResumeComparison` | Resume vs. ICE restart vs. full reconnect |
| `network/` | `TestNetworkGrid_*` | Workloads across 3 RTTs × 2 loss rates |
| `internal/loadgen/` | (helpers) | Shared `Pair`, `NetCond`, `UDPProxy`, stats |
| `browser/` | (Go submodule) | Chrome-driver E2E with ts-sdk viewer |

## Running them

```bash
# Single test
go test -v -run TestKeyframeRecoveryComparison ./benchmarks/video

# All benchmarks, serial (matches CI; the heavy real-QUIC packages
# starve each other for CPU when run in parallel)
go test -v -p 1 ./benchmarks/... -timeout=600s

# Short mode (skips the expensive end-to-end tests)
go test -p 1 -short ./benchmarks/... -timeout=180s

# With race detector on the concurrency-heavy library packages
go test -race ./feed ./wire ./session ./pubsub -count=1
```

CI runs `-short -p 1` against the benchmark subpackages and parallel
non-benchmark tests for the library; see
[`.github/workflows/test.yml`](../.github/workflows/test.yml).

## Honest limitations

1. **Single hardware.** All measurements on one machine; CI uses
   shared GitHub Actions runners. Absolute numbers shift with CPU,
   but ratios are stable.
2. **In-process network sim.** The UDP proxy approximates `tc netem`,
   not the real kernel queueing discipline.
3. **No WAN.** Architectural wins are visible at the proxy level;
   real WAN would scale absolutes while preserving ratios.
4. **Single WebRTC implementation (pion).** libwebrtc comparison is
   out of scope.
5. **Pion baselines use host candidates only.** Every benchmark passes
   `webrtc.Configuration{ICEServers: []webrtc.ICEServer{}}`. Real-world
   cold-starts add 20-100 ms of STUN gathering. Cold-start ratios are
   accurate against the no-STUN baseline; do not extrapolate to
   "production WebRTC setup cost" without re-baselining.
6. **HTTP/2-multiplexed steelman for Row 3 — done.** The original
   serial-dispatch baseline showed 20×; the steelman (HTTP/2 mux, 8
   concurrent streams sharing one TCP+TLS connection, per-stream flow
   control) shows HTTP/2 ~7 ms median vs quicrtc `BidiPerCall` ~13 ms
   on lossless loopback. Near-parity, HTTP/2 slightly ahead. The
   structural QUIC win (no TCP head-of-line blocking under loss)
   shows up in the WAN row, not here. See
   [`feed/bidi_http2_bench_test.go`](../../feed/bidi_http2_bench_test.go).
7. **PLI-aware steelman for Row 4 — done.** A pion publisher +
   subscriber pair with the subscriber sending RTCP
   `PictureLossIndication` recovers in ~33 ms mean / ~35 ms p99 —
   identical to quicrtc's `Client.RequestKeyframe()` on lossless
   loopback. Both arms bottleneck on the 30 fps frame interval
   (33.3 ms). The original 62× ratio was against a no-signaling-at-all
   WebRTC subscriber. quicrtc's advantage (in-band, doesn't depend on
   RTCP packets surviving loss) shows up under RTCP feedback loss or
   in fan-out scenarios — not in clean loopback benchmarks. See
   [`video/keyframe_recovery_pion_test.go`](video/keyframe_recovery_pion_test.go).
8. **Wi-Fi → cellular row measures warm-session reattach, not path
   migration.** Same loopback IP throughout; no CID change, no
   PATH_CHALLENGE, no NAT rebind. Re-label as "Warm reattach" until a
   real migration test lands.
9. **Computer-use baselines explored before Row 5.** Two synthetic-
   proxy baselines were tested and discarded: TCP-mux (quicrtc behind
   across 4 configs; proxy understates real TCP pain) and pion-DCs-
   for-everything (quicrtc 300×+ ahead but unrealistic — production
   WebRTC uses RTP for video, not SCTP DCs). Only the real-WAN vs
   TCP-mux run shipped — see
   [`testing/wan_bench/computer_use.go`](../wan_bench/computer_use.go).

## Version pinning

Numbers in the README's performance table were measured against:

- pion/webrtc/v4 — see `go.sum` for the exact patch version
- quic-go v0.59.0
- Go 1.26.x
- macOS (Darwin 25.x) on Apple Silicon, single-host loopback
- `GOMAXPROCS=` runtime.NumCPU (unmodified)

Absolute numbers will drift across hardware and library versions; structural ratios should not. Run `go test -v -p 1 ./testing/benchmarks/...` on your hardware to validate locally.
