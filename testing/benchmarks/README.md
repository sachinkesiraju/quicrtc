# benchmarks

End-to-end performance suite comparing quicrtc against pion/webrtc and
SSE-over-HTTP/2 on AI-streaming workloads. Each workload picks the
right topology on each side: direct vs. P2P for 1:1, relay vs. SFU
for 1:N. See [`METHODOLOGY.md`](METHODOLOGY.md) for measurement
details.

## Use case 1: token streaming (1:1)

ChatGPT-style streaming from server to one user. Server emits 200
tokens/sec; client renders them.

Topologies:
- WebRTC: one PeerConnection, one ordered+reliable data channel
- quicrtc: native PeerConnection

| Metric                       | pion     | quicrtc  | Advantage   |
|------------------------------|----------|----------|-------------|
| Setup latency p50            | 138 ms   | 0.83 ms  | **165×**    |
| Setup latency p99            | 141 ms   | 1.44 ms  | **98×**     |
| Token e2e p99 (200/sec, 64B) | 289 µs   | 275 µs   | tied        |
| Inter-arrival jitter p99     | 6.22 ms  | 6.21 ms  | tied        |
| Delivery rate                | 100%     | 100%     | tied        |

Setup latency is dominated by ICE candidate gathering + DTLS on
WebRTC. Per-message latency is near-tied because a single ordered
channel has no architectural pressure on either protocol. For
**time-to-first-token** — the load-bearing UX metric for chat apps —
quicrtc is ~137 ms faster on cold start.

## Use case 2: multi-modal agent (1:1)

A multi-modal agent emits **video + tokens** concurrently. Tokens at
200/sec; video at 30fps × 50KB (~12 Mbps); 2-second window.

Topologies:
- WebRTC canonical: 1 PeerConnection, 2 data channels (what 99% of
  WebRTC apps build)
- WebRTC parallel: 2 separate PeerConnections (extra signaling
  cost; rarely done in practice)
- quicrtc: native PeerConnection, DataChannel for tokens, AU pipeline
  for video

| Metric              | pion canonical | pion parallel | quicrtc     | Advantage                              |
|---------------------|----------------|---------------|-------------|----------------------------------------|
| **Token e2e p99**   | **1,958 µs**   | 374 µs        | **365 µs**  | **5.4× vs canonical**, tied vs parallel |
| Video e2e p99       | 3.21 ms        | 3.38 ms       | **1.84 ms** | **1.7× faster**                        |
| Token IA jitter p99 | 6.30 ms        | 6.26 ms       | 6.20 ms     | tied                                   |
| Delivery (both)     | 100%           | 100%          | 100%        | tied                                   |

WebRTC data channels share a single SCTP-over-DTLS association. When
the video channel bursts a 50KB chunk, that chunk's SCTP segments
queue ahead of any other channel's segments — small token messages
arriving at the wrong time get stuck behind video. quicrtc gives each
track its own QUIC stream; bursts on one stream cannot delay another.

The 5× win compares the **default ergonomic pattern** of each
protocol. WebRTC's parallel-PC workaround narrows the gap, at the cost
of running multiple complete signaling + ICE + DTLS handshakes.

## Use case 3: live broadcast / 1:N fan-out

1 publisher → N subscribers at 30fps × 50KB. Per-subscriber latency
must stay low as N grows.

Topologies:
- WebRTC: minimal pion SFU (publisher → SFU → N subscribers via
  per-sub data channels)
- quicrtc: native-wire relay (`relay` package)

N=4 subscribers, 60 frames × 50KB:

| Metric                                  | pion SFU | quicrtc relay | Advantage   |
|-----------------------------------------|----------|---------------|-------------|
| **Worst-subscriber e2e p99**            | 8.42 ms  | **4.62 ms**   | **1.8×**    |
| **Mean per-subscriber p99**             | 8.12 ms  | **4.40 ms**   | **1.8×**    |
| Best-subscriber e2e p99                 | 7.91 ms  | 3.98 ms       | 2.0×        |
| Delivery rate                           | 100%     | 100%          | tied        |

Each leg of pion's SFU is a full WebRTC stack hop — DTLS-encrypted
SCTP frames repacked with their full overhead per hop. quicrtc's
relay forwards QUIC stream messages without re-encoding; the
per-subscriber publish loop is a tight broadcaster fan-out over
independent QUIC streams.

## Architectural ceilings (where WebRTC keeps winning)

| Use case                                  | Why WebRTC wins                                                          |
|-------------------------------------------|---------------------------------------------------------------------------|
| Browser↔browser P2P with no infra         | Browsers can only initiate WebTransport, not accept it                   |
| Native iOS/Android apps                   | libwebrtc ships in iOS WebKit and Android Chrome; quicrtc Phase 4 work   |
| Audio AEC/NS/AGC                          | `getUserMedia` returns processed audio via libwebrtc; we piggyback but don't ship our own pipeline |
| Production SFU ecosystem                  | LiveKit / Daily / Mediasoup ship recording, transcription, layer switching policies, compliance — out of scope here |
| Spec maturity for risk-averse buyers      | WebRTC is an RFC; quicrtc's wire format ([`../docs/spec.md`](../docs/spec.md)) isn't a standard |

## Reproducing

```bash
go test -v -p 1 -run TestApp1ComputerUseClosedLoop  ./benchmarks/agent
go test -v -p 1 -run TestKeyframeRecoveryComparison ./benchmarks/video
go test -v -p 1 -run TestTokenStreamingVsSSE        ./benchmarks/tokens
go test -v -p 1 -run TestDatagramVsStreamContention ./benchmarks/telemetry
go test -v -p 1 -run TestMultimodalIsolation        ./benchmarks/multimodal
go test -v -p 1 -run TestLiveBroadcast              ./benchmarks/fanout
go test -v -p 1 -run TestRelayOverhead              ./benchmarks/fanout
go test -v -p 1 -run TestResumeComparison           ./benchmarks/resume
go test -v -p 1 -run TestNetworkGrid                ./benchmarks/network

# Everything
go test -v -p 1 ./benchmarks/... -timeout=600s
```

`-p 1` is important: the heavy real-QUIC subpackages starve each
other for CPU when they run in parallel and inflate tail latencies.

Tested on macOS, Apple Silicon. Absolute numbers vary with hardware;
**the ratios between implementations are stable**.

## What these numbers don't claim

- **No WAN advantage claim.** Loopback strips network conditions.
  WebRTC's libwebrtc has decade-mature loss recovery that may narrow
  some gaps over real internet links.
- **No feature parity claim.** WebRTC has simulcast layer switching,
  fully implemented audio processing, mature recording infrastructure.
  Phase 3 covers some of these; full parity is years out.
- **No production multi-tenant claim.** quicrtc relay here is
  hermetic in-process; production SFUs add per-tenant isolation, rate
  limiting, and observability that aren't benchmarked.

What they DO show: for the AI streaming use cases that are the
project thesis — token streaming, multi-modal agents, live broadcasts
— quicrtc beats canonical WebRTC by quantifiable factors, and the
wins are architectural (protocol-level), not micro-optimization.
