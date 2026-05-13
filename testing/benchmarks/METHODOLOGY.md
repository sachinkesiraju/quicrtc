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
