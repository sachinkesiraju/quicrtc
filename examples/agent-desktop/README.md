# agent-desktop — the flagship demo

A cloud coding agent (think Devin desktop or a Cursor cloud agent)
works a task on its remote desktop: reproduce a failing test, find
the rounding bug, patch it, re-run the suite, open a PR, watch CI go
green. Its **screen, reasoning tokens, tool calls, and telemetry**
are produced once by one agent engine and streamed to your browser
over **three transports at the same time**, rendered side by side
with live per-lane latency:

| pane | transport | what it models |
|---|---|---|
| left | **single WebSocket** | all four lanes multiplexed on one TCP stream — single-gateway products (noVNC-class desktop streaming, firewall-friendly one-connection designs) |
| middle | **glued stack** | frames on their own WebSocket, tokens on SSE, tool calls + telemetry + input on an events WebSocket — what well-engineered agent products ship today |
| right | **quicrtc** | four lanes plus an input lane on **one** QUIC/WebTransport connection — screen on stream-per-GOP, tokens on a low-latency stream, tool calls on bidi-per-call streams, telemetry on datagrams, input on publish-back |

Every arm gets its **own identical emulated link** (default: café
wifi — 40 ms RTT, 8 Mbps, shared single-bottleneck queue; within the
glued arm its three connections share one link, like one user's
wifi). Every event carries the same publish timestamp. All three
arms run the **identical latest-frame-wins screen protocol** — the
WS arms via an ack message, quicrtc via a publish-back receipts
track — so the bulk lane is a controlled variable and the
interactive lanes are the experiment.

<p align="center"><img src="../../docs/assets/agent_desktop.png" alt="Three cloud agent desktops side by side: single WebSocket, glued stack, quicrtc — with per-lane latency chips and drop-recovery readouts" width="900"></p>

## Run it

One binary serves the page, all three transports, and the emulated
networks. No API key, no npm build, no browser automation:

```bash
go run ./examples/agent-desktop
# open http://127.0.0.1:8420
```

Three things to try on the page:

1. **Watch the lane chips settle** (~30 s). The screen lane is even
   across all three arms by construction; the interactive lanes —
   click RTT, tokens, telemetry — run consistently lower on quicrtc.
   On a good link (café or broadband) quicrtc's median is about
   **2× faster than the single-WebSocket arm** while the glued stack
   sits between the two.
2. **Click any desktop.** The click travels to the server on that
   arm's wire, ripples the shared desktop, and is acked back — the
   `click → response RTT` chip is the number a takeover user feels.
   (Auto-interact keeps this lane fed on its own.)
3. **Press "simulate network drop."** Every arm's connection(s) close,
   1.5 s of dead air passes (the engine keeps streaming into the gap),
   then all arms reconnect at once. quicrtc re-dials **once** with
   `{session, last_seen}` and the server replays what was missed —
   **0 messages lost**. The glued arm re-opens **three** connections
   and recovers only its SSE lane (that's the one lane where resume is
   idiomatic to hand-roll). The single WS recovers nothing.

Flags: `-profile clean|broadband|cafe|hotel`, or override with
`-rtt`, `-mbps`, `-loss` (loss applies to the QUIC path only — a
userspace TCP proxy can't drop packets — so it only ever handicaps
quicrtc). `-naive-frames` switches the single-WS arm from the
steelman to the FIFO frame queue many products actually ship.

## Measured numbers (no browser needed)

`-bench` runs Go clients on all three arms through the same shaped
links, drives the action lane at 2 clicks/s, performs the same
mid-run drop, and prints the table
(`go run ./examples/agent-desktop -bench 60s`):

**Café wifi (40 ms RTT / 8 Mbps), steady state p50 / p99 ms:**

| lane | single WS | glued stack | quicrtc |
|---|---|---|---|
| action → ack RTT | 121 / 197 | 75 / 157 | **70 / 116** |
| reasoning tokens | 96 / 179 | 57 / 136 | **49 / 95** |
| telemetry | 101 / 187 | 59 / 139 | **50 / 98** |
| screen frames | 172 / 225 | 170 / 227 | 173 / 229 |

**Broadband (30 ms RTT / 25 Mbps), steady state p50 / p99 ms:**

| lane | single WS | glued stack | quicrtc |
|---|---|---|---|
| action → ack RTT | 76 / 121 | 53 / 107 | **50 / 74** |
| reasoning tokens | 60 / 109 | 30 / 79 | **29 / 56** |
| tool calls | 56 / 104 | 29 / 76 | **31 / 51** |
| telemetry | 63 / 119 | 36 / 92 | **33 / 66** |
| screen frames | 106 / 146 | 106 / 145 | 106 / 147 |

On unlimited loopback (`-profile clean`) every arm measures ~0 ms —
there is no queue to compare. The architectural gap appears as soon
as the link has any serialization delay (café, broadband, or custom
`-rtt`/`-mbps`), without resorting to degraded profiles or tighter
bandwidth caps.

**The mid-run network drop (1.5 s dead air, all arms at once):**

| | single WS | glued stack | quicrtc |
|---|---|---|---|
| connections to re-open | 1 | 3 | **1** |
| reconnect → first data | 48 ms | 162 ms | 135 ms |
| data lost in the gap | **86 tokens + telemetry** | telemetry only | **nothing** |

With `-naive-frames` (the frame handling many single-socket products
actually ship), the single-WS column collapses to **1.3–2.5 s** on
every lane while the other two arms are unchanged — worth seeing once
to understand what the steelman ack-pacing is doing for the baseline.

On the hotel profile (80 ms / 6 Mbps) the shape is the same with the
tails further apart (tokens 178 → 127, actions 202 → 168 at p99).
Absolute numbers vary with hardware; the reproducible part is the
shape: **interactive lanes isolated, bulk lane even, nothing lost on
reconnect.**

## How to read the result (the honest version)

The agent engine now runs a **continuously busy** workload: the screen
streams flat-out at a rate sized to ~90% of the shaped link, reasoning
tokens never pause between steps, and telemetry fires at 10 Hz. That
keeps the median sample on a good link landing while a ~70 KB frame is
in flight — the regime where transport architecture matters at p50, not
just the tail.

Against the **single-WS** arm the mechanism is structural: one TCP
stream is one FIFO, so a token published while the viewer hasn't acked
the in-flight frame waits for that ack before the sender will even
write it — and during loss TCP's in-order delivery holds every lane
behind the gap. On café wifi that shows up as **~2× lower median**
latency for tokens and telemetry on quicrtc (≈50 ms vs ≈100 ms).
Steelman pacing shrinks the damage; it can't remove it.

Against the **glued stack** the latency gap at p50 is smaller and
honest: separate TCP connections don't share an app FIFO, so the
remaining difference comes from per-lane QUIC streams sharing one
congestion controller and packet-level interleaving at the bottleneck.
Where quicrtc separates from the glued stack is everything around
latency:

- **one connection** to open, secure, monitor, and reconnect — vs
  three reconnect dances and per-connection auth;
- **replay for every lane** on resume, from the transport — the glued
  arm's token replay took a hand-rolled ring buffer and `?from=`
  protocol in this demo (~200 lines a real product must own), and
  still covers one lane out of four;
- **fire-and-forget telemetry** on datagrams — the glued arm's
  telemetry is reliable-ordered whether it wants that or not;
- the **latest-frame-wins screen protocol came free** (stream-per-GOP
  reset + keyframe-aware drops) where both baselines needed the
  ack-pacing implemented by hand.

## What's real vs. scripted

Real: everything on the wire. All three protocol stacks (including
the raw-WebTransport quicrtc client in `ui.html`, resume included),
PNG desktop frames the browser actually decodes (painted window
manager with a real bitmap font), ~50 tok/s token cadence, the
interactive click → ripple → ack loop, the link emulation (one-way
delay + single-server serialization queue per direction, identical
for every arm), and the drop/reconnect mechanics.

Scripted: the agent's reasoning text and action sequence — no model
is running. The production path for browsers is the
[TypeScript SDK](../../ts-sdk/), which wraps everything `ui.html`
does by hand (including resume) behind `QuicRTCClient`.

What this demo does NOT claim: real-WAN numbers (the link is emulated
in-process; for the same comparison on real GCP VMs see
[`testing/wan_bench`](../../testing/wan_bench/)), video-codec parity
with WebRTC screen pipelines (frames are PNG, not H.264), or that
UDP is never blocked (corporate networks that drop UDP need a TCP
fallback path regardless of transport choice).

## Files

- [`main.go`](./main.go) — servers for all three arms + the page,
  track setup, the receipt-paced quicrtc screen sink
- [`engine.go`](./engine.go) — the agent loop; fans identical events
  to every arm; the click-ripple action path
- [`script.go`](./script.go) — the scripted coding task
- [`desktop.go`](./desktop.go) + [`font.go`](./font.go) — the desktop
  painter (windows, terminal, editor, bitmap font, click ripples)
- [`baseline.go`](./baseline.go) — shared baseline plumbing + the
  single-WS arm (ack-paced frames, bounded FIFOs)
- [`glued.go`](./glued.go) — the glued arm (frames WS + events WS +
  SSE with ring-buffer replay)
- [`shaper.go`](./shaper.go) — the emulated links (UDP + TCP, one
  shared wire per arm)
- [`bench.go`](./bench.go) — headless three-arm measurement with the
  drop phase
- [`ui.html`](./ui.html) — the three-pane page (raw WebTransport
  client with resume, drop button, recovery readouts)

## Where to go next

- A real Claude computer-use loop on the same four-lane shape:
  [`examples/cua-live`](../cua-live/)
- The naive-vs-multistream dispatch benchmark with tunable handler
  latencies: [`examples/cua`](../cua/)
- Real-WAN (two GCP VMs) validation of the transport comparison:
  [`testing/wan_bench`](../../testing/wan_bench/)
