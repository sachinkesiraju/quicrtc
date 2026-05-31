# replay scrubber — interactive session replay in the browser

A zero-dependency, vanilla-JS scrubber for a quicrtc session recording.
Drag the timeline (or the slider / ▶ play) and watch each lane reconstruct
what the agent **saw**, **reasoned**, and **did** at that instant — the
browser equivalent of `record.Timeline.SeekSnapshot`.

**Where quicrtc's value shows up here:** all four lanes ride one connection
on one capture clock, so the cross-lane reconstruction is *exact*. The
`reconstruct as` toggle flips to a simulated glued stack (WebRTC + SSE +
gRPC + OTLP) where each lane has its own connection and clock — the panels
drift apart and the skew chip jumps from `0 ms · exact` to a few hundred ms,
so you can no longer say what the agent saw at the instant it acted. That's
the observability payoff.

## Run it

```bash
# 1. record (or synthesize) a session, then export the bundle the page reads
go run ./examples/replay -gen /tmp/sess.qrtc
go run ./examples/replay -file /tmp/sess.qrtc -json -payloads \
    > examples/replay/viewer/bundle.json

# 2. serve this folder and open it (file:// can't fetch the bundle)
cd examples/replay/viewer && python3 -m http.server 5174
# open http://localhost:5174/
```

The committed `bundle.json` is a real browser session — a headless Chromium
captured opening Hacker News, reading the top story, and scrolling — pushed
through the real record pipeline (`replay -import`), so the page works the
moment you serve the folder. To inspect a real `cua-live` session, point a
recorder at the server (`server.AttachRecorder`), then export its `.qrtc`
with `replay -json -payloads`.

## What it shows

- **Timeline** (canvas): one swimlane per track on the shared capture clock,
  each labelled with its quicrtc delivery class (`stream/GOP`, `low-latency`,
  `bidi/call`, `datagram`); gold outline = keyframe; the red playhead and
  red-outlined frames are the reconstructed cross-track state at that moment.
- **reconstruct as** (toggle): `quicrtc · 1 clock` vs `glued stack · 4 clocks`
  — the latter applies illustrative per-lane clock offsets so the panels
  de-sync, with a live cross-lane skew readout.
- **screen** — the actual frame (JPEG/PNG, sniffed) the agent saw at the playhead.
- **reasoning** — the token transcript up to the playhead (newest tinted).
- **tool calls** — every action issued up to the playhead (newest first).
- **telemetry** — the latest telemetry gauges.

The whole session is one `bundle.json`: `replay -json -payloads`
base64-embeds each frame's bytes, so the page is fully self-contained — no
server-side payload fetches, no build step.
