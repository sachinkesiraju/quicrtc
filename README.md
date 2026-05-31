# quicrtc

[![CI](https://github.com/sachinkesiraju/quicrtc/actions/workflows/test.yml/badge.svg)](https://github.com/sachinkesiraju/quicrtc/actions/workflows/test.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/sachinkesiraju/quicrtc.svg)](https://pkg.go.dev/github.com/sachinkesiraju/quicrtc)
[![npm version](https://img.shields.io/npm/v/quicrtc.svg)](https://www.npmjs.com/package/quicrtc)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

**Real-time transport for AI agent sessions over QUIC/WebTransport.**

quicrtc is a Go library and TypeScript SDK designed for the traffic shape that modern AI agents produce. It streams video, low-latency tokens, RPC-shaped tool calls, and fire-and-forget telemetry over a single QUIC/WebTransport connection.

## One connection, four lanes

An AI agent sends a user several kinds of data at once: live screen video, the model's tokens, tool calls, and telemetry. The usual way to ship that is four separate protocols — WebRTC for video, SSE for tokens, gRPC for tool calls, OTLP for telemetry. Four connections, four handshakes, four auth paths, four things to reconnect.

quicrtc carries all four on **one** connection. Each kind of traffic gets its own lane, and a busy lane never blocks the others.

<p align="center"><img src="docs/assets/cua_flow.svg" alt="One QUIC connection carrying four parallel lanes during a computer-use agent turn: continuous screen video, the per-turn action request and result, and small snapshot datagrams." width="880"></p>

| Lane | Carries | How it's sent | Why it matters |
|---|---|---|---|
| 🟦 **Video** | screen frames | one stream per group-of-pictures | a dropped frame recovers in **~33 ms**, not ~1–2 s |
| 🟩 **Tokens** | the model's reasoning | a low-latency stream | tokens keep flowing **during** a video burst |
| 🟧 **Tool calls** | each turn's action + result | a bidirectional stream per call | calls run in **parallel** |
| ⬜ **Telemetry** | small fire-and-forget snapshots | QUIC datagrams | metrics **skip the queue** entirely |

### vs. the glued stack

The four-protocol stack costs you on everything the lanes table doesn't show:

| | WebRTC + SSE + gRPC + OTLP | quicrtc |
|---|---|---|
| Connections to manage | 4 | **1** |
| Handshake + auth | 4× | **1×** |
| Reconnect after a drop | 3 round-trips | **1** |
| 1-to-many fanout | SFU re-encodes per hop | **native relay forwards bytes as-is** |
| Browser client size | heavy | **~6 KB gzipped** (~20 KB minified) |
| Session recording / replay | bolt on a 5th system | **built in, on one capture clock** |

## Performance

Head-to-head: same workload, same machine, same network. Lower is better. Loopback rows use a synthetic 50 ms RTT; WAN rows use two GCP VMs across US regions. [Full methodology](testing/benchmarks/METHODOLOGY.md).

| # | Workload | Baseline | quicrtc | Result |
|---|---|---|---|---|
| 1 | [Cold start: open 4 streams](testing/benchmarks/setup/setup_bench_test.go) | 4 protocols, serial handshakes: p50 **647 ms** | one QUIC handshake covers all four: p50 **167 ms** | **~4× faster** |
| 2 | [Reconnect after network drop](testing/benchmarks/resume/reattach_after_kill_test.go) | WebRTC + SSE + HTTPS reconnect: p50 **233 ms** | one re-dial: p50 **160 ms** | **~1.5× faster** |
| 3 | [Telemetry while video is busy](testing/benchmarks/telemetry/datagram_realquic_test.go) | reliable QUIC stream + 60 Mbps video: mean **285 µs** | fire-and-forget datagrams: mean **175 µs** | **~1.6× lower latency** |
| 4 | [LLM tokens during a 60 Mbps screen burst — real WAN](testing/wan_bench/client.go) | pion shared WebRTC connection: p50 **41 ms**, p99 **378 ms** | per-lane isolation: p50 **37 ms**, p99 **47 ms** | **~8× faster at the tail** |
| 5 | [Computer-use action→DOM RTT — real WAN](testing/wan_bench/computer_use.go) | TCP-multiplexed channels: p50 **63 ms**, p99 **157 ms** | quicrtc: p50 **65 ms**, p99 **80 ms** | **~2× faster at the tail** |
| 6 | [1-to-many broadcast (4 viewers)](testing/benchmarks/fanout/live_broadcast_test.go) | pion SFU re-encoded per hop: worst viewer p99 **8.42 ms** | native relay: worst viewer p99 **4.62 ms** | **~1.8× lower tail** |

**Row 4 is the headline:** tokens ride a separate lane, so a 60 Mbps video burst doesn't drag their tail.

**Where it doesn't win:** a single token stream alone on a clean connection (HTTP/2 SSE is ~20% faster at the median — lanes only pay off when you have multiple things to keep apart), browser-to-browser (use WebRTC), conversational voice that needs the browser's audio cleanup (use WebRTC), and native iOS/Android (no port yet).

## Install

```bash
go get github.com/sachinkesiraju/quicrtc@latest   # Go server / client
npm install quicrtc                                # TypeScript browser SDK
```

Requires Go 1.26+, Node 18+, and a browser with WebTransport (Chrome/Edge 114+, Safari 18.2+, Firefox with a flag).

## Quick start

**Server (Go)** — publish each kind of data on its own lane:

```go
srv, _ := server.New(server.Config{
    Addr: ":4433",
    SDP:  wire.SDP{Codec: "avc1.42E01F", Width: 1280, Height: 720, FPS: 30},
})

videoPub := srv.AddTrackSpec(server.TrackSpec{Name: "screen",    Kind: track.KindVideo})
tokenPub := srv.AddTrackSpec(server.TrackSpec{Name: "reasoning", Kind: track.KindTokens})
telePub  := srv.AddTrackSpec(server.TrackSpec{Name: "telemetry", Kind: track.KindTelemetry})

go srv.ListenAndServe(ctx)

for token := range llm.Tokens() {
    tokenPub.Publish(ctx, pubsub.AccessUnit{Bytes: token.Bytes, Keyframe: true})
}
```

**Client (TypeScript)** — subscribe to the lanes you care about:

```typescript
import { QuicRTCClient } from 'quicrtc';

const client = new QuicRTCClient();
await client.connect('https://your-server/wt', { slug: 'auth-token' });

client.onTrack('screen', (au) => { /* decode with WebCodecs, render to <canvas> */ });

const tokens = await client.recvOn('reasoning');
console.log(new TextDecoder().decode(tokens.bytes));
```

Runnable: [`examples/publisher/`](examples/publisher/) (server) and [`ts-sdk/examples/viewer/`](ts-sdk/examples/viewer/) (a browser viewer that drives all four lanes at once). More — a real computer-use loop, the native relay, a session-replay scrubber — in [`examples/`](examples/) and [`ts-sdk/examples/`](ts-sdk/examples/). Deploying? See [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md). Stuck? [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md).

## FAQ

**How is this different from WebRTC / WebSockets?** A WebSocket is one queue, so different kinds of traffic stall each other. WebRTC is built for browser-to-browser and for voice/video that needs the browser's audio cleanup — the right tool for Zoom or conversational voice. quicrtc is for one server pushing many kinds of data to a client on a single connection.

**Is it production-ready?** The wire format is stable, cross-language tested, and the benchmark numbers reproduce on real GCP VMs. It's a solo open-source project with no known third-party deployments — vet it like any new dependency.

**Browser-to-browser?** No — browsers can only initiate WebTransport, not accept it. Use WebRTC.

**How does it relate to Media over QUIC (MoQ)?** Not MoQ-compatible. The stream-per-GOP pattern is inspired by MoQ-lite's group-as-stream model, but quicrtc is shaped for AI-agent traffic rather than MoQ's "replace HLS and WebRTC" scope. Use MoQ if you need MoQ interop.

## Status

**v1.0.2.** Wire format stable across Go and TypeScript (v0.1 clients connect to v1.0.2 servers unchanged); public Go API committed. Missing: native iOS/Android and known production deployments.

Docs: [architecture](docs/architecture.md) · [wire spec](docs/SPEC.MD) · [auth](docs/auth.md) · [changelog](docs/CHANGELOG.md) · [issues](https://github.com/sachinkesiraju/quicrtc/issues)

## Contributing & license

PRs welcome — see [`docs/CONTRIBUTING.md`](docs/CONTRIBUTING.md). Security issues: [`docs/SECURITY.md`](docs/SECURITY.md). Apache 2.0; see [`LICENSE`](LICENSE).

Stream-per-GOP pattern inspired by IETF [MoQ-lite](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/) (not wire-compatible). Built on [`quic-go`](https://github.com/quic-go/quic-go); WebRTC baselines use [pion](https://github.com/pion/webrtc).
