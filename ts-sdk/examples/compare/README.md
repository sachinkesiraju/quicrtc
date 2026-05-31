# ts-sdk/examples/compare — naive vs multistream, side by side

A browser page that opens **two quicrtc sessions at once** and drives the
*identical* computer-use-agent workload against both:

- **Pane A** → a `cua/server -mode=naive` server (everything on one stream).
- **Pane B** → a `cua/server -mode=multistream` server (one lane per Kind +
  datagrams).

Each turn, the page fires four request datagrams per pane
(`action`, `a11y`, `dom`, `perf` — the exact wire shape from
[`examples/cua`](../../../examples/cua/)) and times every echo. For each pane
you get:

- the **live screen frames** (real PNG bytes the server is pushing),
- a **scrolling per-lane feed** of echo latencies,
- a **running p50 / p99** per lane, and
- a prominent **per-lane "last update age" + FREEZE badge** so a stall is
  obvious at a glance.

Under a screen burst that saturates the link, pane A's small lanes
(`a11y`/`dom`/`perf`) queue behind screen bytes on the single stream and the
freeze badge goes **red**, while pane B's lanes ride their own streams /
datagrams and keep flowing green.

This page uses **only** the public TypeScript SDK
(`QuicRTCClient.connect / onTrack / sendDatagram / receiveDatagram /
getRemoteTracks`). It does not touch the wire, the SDK source, or any Go code.

## Quick start (one command)

```bash
ts-sdk/examples/compare/run.sh
```

That builds the SDK + this page, starts both servers under `-stress`, starts a
static file server, prints the two share URLs, and opens the page. Ctrl-C stops
everything. Then paste each URL into its field and click
**connect & start driving**.

## Manual setup

### 1. Build the SDK and this page

```bash
cd ts-sdk
npm install
npm run build                       # writes ts-sdk/dist/
( cd examples/compare && npx tsc )  # writes examples/compare/compare.js
```

### 2. Start both servers (two terminals), with `-stress`

`-stress` is the localhost-optimized preset: 30 Hz × 1 MB screen (~30 MB/s),
tiny metadata, and `-datagram-metadata` on. See
[`examples/cua/README.md`](../../../examples/cua/README.md) for every knob.

```bash
# Terminal 1 — pane A
go run ./examples/cua/server -mode=naive -stress
# prints: share: https://127.0.0.1:NNNN/wt#slug=...&hash=...

# Terminal 2 — pane B
go run ./examples/cua/server -mode=multistream -stress
# prints: share: https://127.0.0.1:MMMM/wt#slug=...&hash=...
```

### 3. Serve the page and open it

Serve the **`ts-sdk/` directory** (so the page's `../../dist/index.js` import
resolves):

```bash
# from ts-sdk/
python3 -m http.server 5173
# open: http://localhost:5173/examples/compare/
```

Paste the **naive** share URL into Pane A, the **multistream** URL into Pane B,
set turns/sec (20 is a good default), and click **connect & start driving**.

## Why localhost won't show it (read this)

**On localhost the two panes will look essentially identical — and that is the
honest result, not a bug.** The loopback interface is 100+ Gbps, so the 30 MB/s
screen burst clears in microseconds, nothing queues, and the per-lane dispatch
difference falls below the noise floor. Same caveat as the CLI version in
[`examples/cua/README.md`](../../../examples/cua/README.md).

To make pane A's lanes **visibly freeze** you need real contention — RTT, loss,
or a bandwidth cap — which loopback cannot provide:

- **`tc netem` against a second host or VM** (NOT localhost — localhost ignores
  most netem shaping):

  ```bash
  # on the server host / VM
  sudo tc qdisc add dev eth0 root netem delay 50ms 10ms loss 1% rate 50mbit
  go run ./examples/cua/server -mode=naive -stress -bind=0.0.0.0:4444
  # open the page on another machine on the LAN, paste the LAN share URL
  sudo tc qdisc del dev eth0 root   # remove afterward
  ```

- **A WAN VM** (real RTT + bandwidth limits). Use the
  [`deploy-vm.sh` instructions in `examples/cua/README.md`](../../../examples/cua/README.md#vm-deployment-for-wan-testing)
  to stand the servers up on a cloud VM, then open this page locally and paste
  the VM's share URLs.

Under those conditions, pane A's `a11y`/`dom`/`perf` freeze badges flip to red
and the p99 readout balloons, while pane B stays live — the tail-latency win
from README row 4.

## Files

- [`index.html`](./index.html) — two-pane layout + lane / pane templates.
- [`compare.ts`](./compare.ts) — all SDK calls, the turn driver, per-lane
  quantiles, and the freeze logic. Type-checked against the SDK.
- [`compare.js`](./compare.js) — emitted output the page loads (`npx tsc` here).
- [`styles.css`](./styles.css) — framework-free styling.
- [`run.sh`](./run.sh) — one-command launcher.
- [`tsconfig.json`](./tsconfig.json) — standalone type-check/emit config (kept
  out of the SDK build; the root tsconfig excludes `examples/`).

## Notes on how it works

- **Same workload, both panes.** One `setInterval` fires a turn against every
  connected pane on the same tick, so the comparison is apples-to-apples.
- **Echo timing.** Each request carries a `msg_id`; the matching response
  (track AU in naive / acks-lane, or a raw JSON datagram for metadata in
  multistream) resolves the pending entry and records `now − sentAt`. That is a
  true round-trip per lane, measured client-side.
- **Freeze logic.** A lane with no echo for >400 ms shows `stalling`; >1.5 s
  shows a pulsing red `FROZEN`. At 20 turns/sec the normal inter-echo gap is
  ~50 ms, so any multi-hundred-ms gap is a genuine stall.
- **Mode detection** mirrors the Go client: a `data` track ⇒ naive; the
  `acks/a11y/dom/perf` tracks ⇒ multistream. The page subscribes to whatever
  tracks the server announces and also drains datagrams.
