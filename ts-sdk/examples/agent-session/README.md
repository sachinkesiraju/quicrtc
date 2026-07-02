# ts-sdk/examples/agent-session — watch & steer your agent

A product-shaped demo that reads like the live session view of an AI
coding agent (a Devin-style "Machine" tab: a live desktop, an activity
feed, and a **Take control** click). It opens the **same** agent workload
two ways, side by side, so a non-technical viewer can see the difference
at a glance:

- **Left — Standard transport** → a `cua/server -mode=naive` server. One
  connection where the desktop video and your control clicks share a
  single stream (what a glued SSE/WebSocket stack effectively does).
- **Right — quicrtc** → a `cua/server -mode=multistream` server. One
  connection, but each stream gets its own lane (+ datagrams for tiny
  messages), so a heavy desktop burst can't block your input.

## The one number that matters: take-control latency

Each side continuously fires a **control ping** (the `action` round-trip
from [`examples/cua`](../../../examples/cua/)) and shows its latency as a
big gauge. Clicking a desktop fires an extra ping and drops a latency
badge where you clicked. A center **scoreboard** shows how many times
more responsive quicrtc is *right now*.

Under a heavy desktop stream:

- **Left** — control pings queue behind desktop bytes on the one stream.
  The gauge climbs, turns red (**Frozen**), and the desktop stops
  repainting.
- **Right** — control pings ride their own lane/datagram. The gauge stays
  green (**Instant ✓**) and the desktop keeps repainting.

## Quick start (one command)

```bash
ts-sdk/examples/agent-session/run.sh
```

That builds the SDK + this page, starts both servers under a heavy
desktop stream, starts a static server, and prints two share URLs. Open
<http://localhost:5173/examples/agent-session/>, paste each URL into its
field (left = standard, right = quicrtc), and click **connect & start**.

## Manual setup

```bash
# 1. Build the SDK and this page
cd ts-sdk
npm install && npm run build                  # writes ts-sdk/dist/
( cd examples/agent-session && npx tsc )       # writes agent-session.js

# 2. Start both servers (two terminals), heavy desktop stream
FLAGS="-screen-fps=60 -screen-kb=3000 -datagram-metadata -action-ms=1"
go run ./examples/cua/server -mode=naive        $FLAGS   # left
go run ./examples/cua/server -mode=multistream  $FLAGS   # right

# 3. Serve ts-sdk/ and open the page
cd ts-sdk && python3 -m http.server 5173
# open http://localhost:5173/examples/agent-session/
```

## Why the heavy load (read this)

On a clean localhost link there is no contention — both sides look
instant, which is the honest result, not a bug (loopback is 100+ Gbps).
The `run.sh` default drives the desktop stream hard (60 Hz × ~3 MB) so the
single-stream side backs up **without** needing `tc netem` or a WAN VM. On
a real network (Wi-Fi/4G/WAN) the same freeze appears at far lower load —
which is exactly the situation this demo is standing in for.

## What's real vs. staged

- **Real:** the wire (QUIC + WebTransport), the per-lane dispatch, the PNG
  desktop frames, and every latency number (a true browser→server→browser
  round-trip, measured client-side).
- **Staged:** the agent itself is the deterministic `cua` mock (no LLM).
  For a real Claude computer-use loop see [`examples/cua-live`](../../../examples/cua-live/).
