# ts-sdk/examples/viewer — universal browser viewer

A framework-free browser page that connects to any quicrtc server,
inspects the announced tracks, and visualizes each one based on its
`Kind`. One page, many servers.

| Kind | Panel |
|---|---|
| `Video`      | Canvas painted with the **real PNG bytes** the publisher sent. `K`/`P` marker, seq, and size overlaid in a corner. Click → publishes `user_actions` back to the server. A "simulate slow render" checkbox in the panel meta row deliberately blocks the receive loop for 50 ms per frame so the receive queue saturates and backpressure fires. |
| `Tokens`     | Live-appending text stream. |
| `ToolCalls`  | JSON event list, newest at the top. |
| `Telemetry`  | Key/value gauges parsed from JSON. |
| *(datagrams)* | Auto-drained from the WebTransport datagrams pipe with the 4-byte envelope; rendered as a gauge panel. |

A green/yellow/red **status pill** tracks the connection lifecycle.
A **backpressure indicator** lights up when the SDK fires
`onBackpressure(track, level≥2)`. To deliberately trigger it, tick
the "simulate slow render" checkbox on the screen panel: the receive
loop blocks 50 ms per frame, the SDK's per-track queue saturates,
the indicator turns yellow → red, and the screen panel header shows
a drop counter incrementing.

If the server provides a `DataChannel`, a transcript box + send form
appears at the bottom.

## Setup (one time)

```bash
cd ts-sdk
npm install
npm run build      # writes ts-sdk/dist/
```

## Run

Start any quicrtc server from the Go side, e.g.:

```bash
go run ./examples/agent_pubsub/server
# prints: share: https://127.0.0.1:NNNN/wt#slug=...&hash=...
```

Then serve `ts-sdk/` over HTTP and open the viewer with any static file
server:

```bash
# from the ts-sdk/ directory
python3 -m http.server 5173
# then open: http://localhost:5173/examples/viewer/
```

Paste the share URL printed by the server into the form, click
**connect**. You should see:

- The status pill go `connecting` → `connected`.
- Up to four track panels light up as ANNOUNCE frames arrive
  (`screen`, `reasoning`, `actions` for `agent_pubsub`).
- A `(datagrams)` panel filling in as telemetry datagrams arrive.
- The screen canvas redrawing every 200 ms with a K/P marker.
- Per-track AU counts ticking up in the chip row at the top.
- `in=…  out=…  AU=…  dg=…` in the SDP line, updated every second.

Click anywhere on the screen canvas to publish a `user_actions`
event back to the server — `agent_pubsub/server` logs each one to
its stderr.

## Pointing the viewer at other examples

The viewer is server-agnostic. Try:

- `go run ./examples/publisher` — single `video` panel filling with
  K/P frames.
- `go run ./examples/relay -listen … -upstream …` — same view as
  `publisher` (that's the teaching point: a relay is transparent on
  the wire).
- `go run ./examples/datachannel/server` — the DataChannel transcript
  appears; heartbeats arrive every second.

## Implementation notes

- Pure JavaScript (no TypeScript) so there's no build step inside
  the example. Imports the SDK from `../../dist/index.js`.
- The viewer parses the share-URL fragment (`#slug=…&hash=…`) and
  passes those as the `slug` and `certHash` connect options. The
  cert hash is required for self-signed dev certs.
- Track panels mount lazily as ANNOUNCE frames arrive. We poll
  `getRemoteTracks()` every 200 ms for the first 5 s so late
  announces aren't missed.
- Click-to-publish creates a `user_actions` track via `addTrack`
  the first time a Video panel mounts. If the server isn't expecting
  inbound tracks the `addTrack` Promise rejects silently — clicks
  remain visual.

## What this teaches

- How to **discover remote tracks** dynamically with `getRemoteTracks()`.
- How `Kind` shapes the right consumer pattern: `onTrack` push for
  video, append for tokens, list for tool_calls, gauges for telemetry.
- How to **request keyframes** on gap detection (`requestKeyframe`).
- How to **react to backpressure** — not just log it.
- How to **publish back** with `addTrack` on the receive side.
- How to drain **WebTransport datagrams** with a track-id envelope so
  they coexist with stream-based tracks.

## Source

- [`index.html`](./index.html) — DOM layout.
- [`viewer.js`](./viewer.js) — all the SDK calls.
- [`styles.css`](./styles.css) — minimal styling, no framework.
