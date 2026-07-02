# agent-desktop — the flagship demo

A cloud coding agent (think Devin desktop or a Cursor cloud agent)
works a task on its remote desktop: reproduce a failing test, find
the rounding bug, patch it, re-run the suite, open a PR, watch CI go
green. Its **screen, reasoning tokens, tool calls, and telemetry**
are produced once by one agent engine and streamed to your browser
over **two transports at the same time**:

| pane | transport | wire shape |
|---|---|---|
| left | **single WebSocket** | all four lanes multiplexed on one TCP stream — the glued stack most agent products ship today |
| right | **quicrtc** | four lanes on one QUIC/WebTransport connection — screen on stream-per-GOP, tokens on a low-latency stream, tool calls on bidi-per-call streams, telemetry on datagrams |

Both connections run through the **same in-process emulated link**
(default: café wifi — 40 ms RTT, 8 Mbps, shared queue model), every
event carries the same publish timestamp, and the page renders the
two desktops side by side with live per-lane latency. The only
variable is the wire.

<p align="center"><img src="../../docs/assets/agent_desktop.png" alt="Side-by-side cloud agent desktops: single WebSocket vs quicrtc, with per-lane latency chips" width="820"></p>

## Run it

One binary serves the page, both transports, and the emulated
network. No API key, no npm build, no browser automation:

```bash
go run ./examples/agent-desktop
# open http://127.0.0.1:8420
```

Within ~30 seconds the headline strip settles: reasoning tokens,
tool calls, and telemetry arrive several times faster over quicrtc,
while the screen lane — the bulk traffic both transports carry
identically — stays roughly even. That asymmetry is the point: the
agent's *interactive* output stops queueing behind its own
screen share.

Flags: `-profile clean|broadband|cafe|hotel`, or override with
`-rtt`, `-mbps`, `-loss` (loss applies to the QUIC path only — a
userspace TCP proxy can't drop packets — so it only ever handicaps
quicrtc). `-fps` sets the burst capture rate; the screen idles at a
third of it when the desktop is static, like a real capture pipeline.

## Measured numbers (no browser needed)

`-bench` runs a Go client on each transport through the same shaped
link and prints the table — the browserless version of what the page
shows, and where the numbers below come from
(`go run ./examples/agent-desktop -bench 2m`):

| lane | café wifi (40 ms / 8 Mbps) | hotel wifi (80 ms / 6 Mbps) |
|---|---|---|
| reasoning tokens p99 | ws **378 ms** → quicrtc **164 ms** (~2.3×) | ws **1450 ms** → quicrtc **219 ms** (~6.6×) |
| tool calls p99 | ws **490 ms** → quicrtc **128 ms** (~3.8×) | ws **1550 ms** → quicrtc **181 ms** (~8.6×) |
| telemetry p99 | ws **895 ms** → quicrtc **166 ms** (~5.4×) | ws **1786 ms** → quicrtc **215 ms** (~8.3×) |
| screen frames p50 | ws 163 ms ≈ quicrtc 186 ms | ws 621 ms ≈ quicrtc 1000 ms |

(Latency is one-way publish→receive; both clients share the
publisher's clock. Your absolute numbers will vary with hardware;
the *shape* — interactive lanes isolated, bulk lane even — is the
reproducible part.)

## Why the gap exists

The WebSocket pane is not a strawman: it uses a lean 13-byte binary
envelope (no JSON framing, no base64) and a bounded drop-oldest send
queue. Its handicap is structural — one TCP stream is one FIFO
queue:

- A ~70 KB screen frame accepted by the socket ahead of a 10-byte
  token must finish transmitting before the token's first byte
  leaves.
- During screen bursts the queue grows, and *everything* in it —
  tokens, tool calls, metrics — ages together.
- Under packet loss, TCP's in-order delivery holds back every lane
  behind the gap (this demo doesn't even exercise that; add `-loss`
  and it handicaps only quicrtc).

quicrtc gives each lane its own QUIC stream (or datagrams), so lanes
share bandwidth but never a queue, and the per-session priority
scheduler (`server.SchedulerOn`) drains token/tool-call writes ahead
of queued bulk frames. The screen lane itself gets no miracle — both
transports carry the same ~6 Mbps of PNG through the same bottleneck
— but quicrtc's keyframe-aware drop policy sheds stale frames to
protect freshness where the WebSocket must deliver every byte, late.

## What's real vs. scripted

Real: everything on the wire. PNG desktop frames the browser
actually decodes (a painted window manager — terminal, editor, CI
browser — with a real bitmap font), 30 tok/s token cadence, JSON
tool calls, telemetry, both protocol stacks, and the link emulation
(one-way delay + single-server serialization queue per direction,
identical for both).

Scripted: the agent's reasoning text and action sequence — no model
is running. The demo page speaks the quicrtc wire format directly
over raw WebTransport (see `ui.html`); the production path for
browsers is the [TypeScript SDK](../../ts-sdk/).

## Files

- [`main.go`](./main.go) — servers (quicrtc, WebSocket, page), track
  setup, wiring
- [`engine.go`](./engine.go) — the agent loop; fans identical events
  to both transports
- [`script.go`](./script.go) — the scripted coding task
- [`desktop.go`](./desktop.go) + [`font.go`](./font.go) — the desktop
  painter (windows, terminal, editor, bitmap font)
- [`baseline.go`](./baseline.go) — the single-WebSocket transport
- [`shaper.go`](./shaper.go) — the emulated link (UDP + TCP, same
  queue model)
- [`bench.go`](./bench.go) — headless measurement mode
- [`ui.html`](./ui.html) — the side-by-side page (raw WebTransport
  client included)

## Where to go next

- A real Claude computer-use loop on the same four-lane shape:
  [`examples/cua-live`](../cua-live/)
- The naive-vs-multistream dispatch benchmark with tunable handler
  latencies: [`examples/cua`](../cua/)
- Real-WAN (two GCP VMs) validation of the same comparison:
  [`testing/wan_bench`](../../testing/wan_bench/)
