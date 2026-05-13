# examples

End-to-end examples for the Go server + client APIs. Read them in
order — each one builds on the previous.

## Reading order

1. **[`publisher/`](publisher/)** + **[`subscriber/`](subscriber/)** —
   the minimal "hello world." One track, explicit `Kind`, server
   prints a share URL, subscriber dials it. Status line shows live
   progress; boxed summary on Ctrl-C confirms what flowed.

2. **[`datachannel/`](datachannel/)** — the bidi control channel. A
   stdin chat loop on the client; the server echoes and pushes
   heartbeats independently. Demonstrates that the control stream
   is long-lived and concurrent, not request/reply.

3. **[`agent_pubsub/`](agent_pubsub/)** — four channels on one QUIC
   connection: `screen` (KindVideo, real PNG frames), `reasoning`
   (KindTokens), `actions` (KindTokens), and `telemetry` (datagrams
   with the 4-byte envelope). Demonstrates per-Kind dispatch + the
   `OnKeyframeRequest` recovery handshake + per-session
   `SessionHandle.InboundRecv` for publish-back. **The agent loop
   is mocked** — this example exists to show the WIRE SHAPE.

4. **[`cua/`](cua/)** — same workload as `agent_pubsub` but
   instrumented for **measurement**. Two server modes (`naive` =
   one track for everything, `multistream` = one track per Kind)
   and a client that prints p50 / p95 / max per response type and
   per turn. Pair the two modes, compare the stat blocks, **see**
   the multistream advantage in tail latency. Numbers are honest:
   small on clean loopback, larger under stress, larger still over
   real WAN.

5. **[`relay/`](relay/)** — 1:N fan-out. Front any of the above
   servers with a relay; downstream view is transparent. Open
   extra subscribers to watch `subs=N` tick up live.

## The visual companion

Every server example above is paired with a browser viewer:
[`../ts-sdk/examples/viewer/`](../ts-sdk/examples/viewer/). It's
kind-agnostic — paste the share URL and it renders the right panel
for each announced track:

- `Video` → canvas painted with the **real PNG bytes** the publisher
  sent (not a debug overlay); `K`/`P` marker, seq, size overlaid in
  a corner.
- `Tokens` → live-appending text stream.
- `ToolCalls` → JSON event list.
- `Telemetry` → key/value gauges.
- *(datagrams)* → auto-drained from the WebTransport datagrams pipe.

The screen panel has a **"simulate slow render"** checkbox so you
can deliberately trigger backpressure (see the indicator at the top
turn yellow → red as the receive queue saturates and the SDK fires
`onBackpressure`).

Use the CLI examples to read the SDK code; use the browser viewer to
*see* the wire alive.

## What to expect vs. what's mocked

| Example       | What is real on the wire                              | What is mocked                                  |
|---------------|-------------------------------------------------------|-------------------------------------------------|
| publisher     | QUIC + WT, one track, real synthetic AUs              | "synthetic" codec (random bytes) — clearly labeled |
| subscriber    | nothing — just reads what publisher sent              | n/a                                             |
| datachannel   | bidi control channel, real echo + heartbeat           | nothing                                         |
| agent_pubsub  | 4 channels, real per-Kind dispatch, **real PNG** screen | agent reasoning sentences + tool-call list      |
| cua           | dispatch comparison with **measured** p50/p95/max     | browser handler latency (simulated with sleeps) |
| relay         | real fan-out, identical wire on both sides            | nothing                                         |

For a **real** CUA pipeline (Puppeteer + Anthropic + side-by-side
WebSocket vs QUICRTC comparison) build on top of the Go + TS SDKs
directly — that layer is downstream of this repo.

## Run any example

```bash
go run ./examples/<name>
```

Server examples print a share URL on stdout; copy-paste it into the
client/subscriber/viewer terminal. The TLS cert is auto-generated
per run and pinned into the URL fragment (`#slug=...&hash=...`); no
system CA changes required.

Each example prints a one-sentence "what this run proved" headline
above its boxed summary on Ctrl-C — read it to know whether your
run behaved correctly.

## Project layout

```
examples/
  publisher/               # one track, the basics
  subscriber/              # CLI receiver for the above
  datachannel/
    server/                # bidi-control-channel server
    client/                # stdin chat loop
  agent_pubsub/
    server/                # four channels on one connection
    viewer/                # CLI viewer (interleaved scroll)
  cua/
    server/                # CUA host: -mode={naive,multistream}
    client/                # agent simulator; prints p50/p95/max
  relay/                   # fan-out
  internal/
    status/                # shared live-status + summary helper
    synframe/              # real PNG frame generator (for screen)
```

For the TypeScript SDK examples, see
[`../ts-sdk/examples/`](../ts-sdk/examples/).
