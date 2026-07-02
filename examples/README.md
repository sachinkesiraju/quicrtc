# examples

End-to-end examples for the Go server + client APIs. Each builds on the
previous; read in order.

## Start here: the flagship demo

**agent-desktop** — a cloud coding agent's
desktop streamed to your browser over three wires *side by side* —
quicrtc, a single WebSocket, and the multi-connection glued stack
(frames WS + tokens SSE + events WS) — each through its own identical
emulated network, with live per-lane latency, an interactive
click→response lane, and a network-drop button that shows quicrtc's
single-re-dial resume-with-replay against the baselines' reconnect
scramble. One command, zero setup:

```bash
go run ./examples/agent-desktop          # open http://127.0.0.1:8420
go run ./examples/agent-desktop -bench 60s   # browserless comparison table
```

See [agent-desktop/README.md](agent-desktop/README.md) for the
measured numbers and the honest-comparison methodology. The examples
below then build the same machinery up piece by piece.

## Examples (reading order)

1. **publisher** + **subscriber** —
   minimal hello world. One track, explicit `Kind`; server prints a
   share URL, subscriber dials it.

2. **agent_pubsub** — four channels on one QUIC
   connection: screen (`KindVideo`, real PNG frames), reasoning
   (`KindTokens`), actions (`KindToolCalls`), telemetry (datagrams).
   Per-Kind dispatch + `OnKeyframeRequest` recovery + per-session
   `SessionHandle.InboundRecv` for publish-back. The agent loop is
   mocked — this exists to show the wire shape.

3. **cua** — same workload as `agent_pubsub` but
   instrumented for measurement. Two server modes (`-mode=naive` =
   one track for everything, `-mode=multistream` = one track per
   Kind); client prints p50 / p95 / max per response type. Pair the
   modes to see the multistream tail-latency advantage. Numbers are
   honest: small on clean loopback, larger under stress, larger
   still over real WAN.

4. **relay** — 1:N fan-out. Front any of the above
   servers with a relay; the downstream view is transparent. Open
   extra subscribers to watch `subs=N` tick up live.

Every server example pairs with a kind-agnostic browser viewer in
ts-sdk/examples/viewer — paste
the share URL and it renders the right panel per announced track
(canvas + K/P overlay for video, text stream for tokens, JSON list
for tool calls, gauges for telemetry, auto-drained datagrams). A
"simulate slow render" checkbox triggers backpressure; the SDK's
`onBackpressure` indicator turns yellow → red.

## Run any example

```bash
go run ./examples/<name>
```

Server examples print a share URL on stdout; paste it into the
subscriber or viewer terminal. TLS cert is auto-generated per run
and pinned in the URL fragment (`#slug=...&hash=...`) — no system CA
changes needed. Each example prints a "what this run proved"
headline above its boxed summary on Ctrl-C.

## What's real vs. mocked

| Example       | Real on the wire                                       | Mocked                                            |
|---------------|--------------------------------------------------------|---------------------------------------------------|
| agent-desktop | three full protocol stacks + emulated links, real PNG desktop, interactive input, drop/resume, measured per-lane latency | the agent's reasoning + action script (no model)  |
| publisher     | QUIC + WT, one track, synthetic AUs                    | "synthetic" codec (random bytes), clearly labeled |
| subscriber    | reads what publisher sent                              | —                                                 |
| agent_pubsub  | 4 channels, per-Kind dispatch, real PNG screen         | agent reasoning sentences + tool-call list        |
| cua           | dispatch comparison with measured p50 / p95 / max      | browser handler latency (simulated with sleeps)   |
| relay         | real fan-out, identical wire on both sides             | —                                                 |

For a real CUA pipeline (Puppeteer + Anthropic + side-by-side
WebSocket vs. quicrtc comparison), build on top of the Go + TS SDKs
directly — that layer is downstream of this repo.

For TypeScript SDK examples, see
ts-sdk/examples.
