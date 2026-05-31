# cua-live — a real computer-use agent over quicrtc

A runnable computer-use agent whose **screen, reasoning, tool calls,
and telemetry** stream through four quicrtc tracks on **one** QUIC
connection to the browser viewer. Unlike [`examples/cua`](../cua/) —
which is a deterministic *benchmark* that mocks the browser with
configurable sleeps — this example runs an actual agent loop and fans
real agent-shaped traffic onto each lane.

It has two modes:

| Mode | Brain | Browser | Needs | Runs in CI? |
|---|---|---|---|---|
| `-fake` | scripted session | synthesized PNG frames | **nothing** | yes |
| `-live` | Claude computer-use API (raw `net/http`) | real Chromium via [chromedp](https://github.com/chromedp/chromedp) | `ANTHROPIC_API_KEY` + a Chrome binary + network | no |

> **What is real in `-fake`:** the transport, the per-Kind dispatch
> (screen on a video stream, tokens on a low-latency stream, tool
> calls on bidi-per-call streams, telemetry on QUIC datagrams), the
> screen frames (honest PNG bytes the viewer decodes), the token
> cadence, and the bidirectional wiring. **What is scripted:** the
> reasoning text and the action sequence — no model, no browser. The
> point of `-fake` is to prove the transport carries real
> agent-shaped traffic *anywhere, with zero setup*.
>
> **What is real in `-live`:** all of the above **plus** a real
> Chromium driven by a real Claude computer-use loop. The model looks
> at actual screenshots and decides actions; chromedp clicks, types,
> scrolls, and navigates; the resulting screenshots stream back to the
> viewer.

## The four lanes

The agent loop walks the `Brain` step by step and fans each turn onto
the lane whose `Kind` matches its payload shape, so `feed/pump.go`
picks the right wire pattern per modality:

```
┌────────────┬───────────────┬───────────────────────────────────┐
│ Track      │ Kind          │ On-wire delivery (per feed/pump.go)│
├────────────┼───────────────┼───────────────────────────────────┤
│ screen     │ KindVideo     │ stream-per-GOP uni stream (PNG AUs)│
│ reasoning  │ KindTokens    │ persistent low-latency uni stream  │
│ toolcalls  │ KindToolCalls │ fresh bidi stream per AU           │
│ telemetry  │ datagrams     │ unreliable QUIC datagrams          │
└────────────┴───────────────┴───────────────────────────────────┘
```

A big screen frame in flight cannot stall the next reasoning token —
that lane isolation is the point of carrying all four on one
connection instead of gluing together WebRTC + SSE + gRPC + OTLP.

## Run the no-deps demo (`-fake`)

```bash
# cua-live is a nested module (it isolates the chromedp dep), so cd in first.
cd examples/cua-live
go run . -fake
# prints: share: https://127.0.0.1:NNNN/wt#slug=...&hash=...
```

Then point the universal browser viewer at the printed share URL:

```bash
# one-time SDK build
cd ts-sdk && npm install && npm run build && cd ..

# serve ts-sdk/ and open the viewer
cd ts-sdk && python3 -m http.server 5173
# open http://localhost:5173/examples/viewer/ and paste the share URL
```

You'll see four panels light up as the scripted agent works through a
login → dashboard task:

- **screen** — a canvas painting the synthesized "browser window";
  the click crosshair and typed-field bars track each step.
- **reasoning** — the model's thoughts filling in token-by-token.
- **toolcalls** — a list of the JSON actions (`navigate`, `left_click`,
  `type`, `key`, `scroll`, `wait`), newest at the top.
- **(datagrams)** — live telemetry gauges (step, frame/token/call
  counts, heap, goroutines) parsed from the datagram envelope.

Useful flags: `-step 250ms` (turn pacing), `-bind 0.0.0.0:4433`.

## Run the live demo (`-live`)

Live mode is behind the `cualive` build tag so the default build and
the `-fake` demo never need chromedp. **It will not run in CI** — it
needs an API key, a Chrome binary, and outbound network.

```bash
# build with the tag (pulls in chromedp); cua-live is a nested module
cd examples/cua-live
go build -tags cualive -o cua-live .

# run it
ANTHROPIC_API_KEY=sk-ant-... ./cua-live -live \
    -url https://news.ycombinator.com \
    -goal "Find the top story and tell me its title" \
    -model claude-sonnet-4-20250514
```

Requirements:

- `ANTHROPIC_API_KEY` in the environment.
- A Chrome / Chromium binary on `PATH` (chromedp launches it headless;
  install Chrome, or set the usual chromedp env to point at one).
- Outbound network to `api.anthropic.com`.

Each turn, the loop sends the latest screenshot to Claude's
computer-use tool (`computer_20250124`, beta `computer-use-2025-01-24`)
over raw `net/http`, parses the returned reasoning + action, executes
the action against the real page with chromedp, and streams the new
screenshot back to the viewer — all four lanes carrying genuine agent
output.

Running `-live` **without** the build tag prints a clear message and
exits (the stub explains how to build it).

## Files

- [`brain.go`](./brain.go) — the `Brain` interface, the `Action` /
  `Step` types, and `liveConfig`. The transport-agnostic agent contract.
- [`fake.go`](./fake.go) — the scripted `FakeBrain` + the synthesized
  browser-frame painter. No external deps.
- [`live.go`](./live.go) — the real `liveBrain`: Claude computer-use
  over `net/http` + chromedp execution. Build tag `cualive`.
- [`live_stub.go`](./live_stub.go) — default-build stub so `-live`
  fails gracefully without the tag.
- [`main.go`](./main.go) — server setup, the four track publishers, the
  agent loop, and the per-session telemetry datagram pump.

## How this differs from the other examples

- [`examples/cua`](../cua/) — a *measured* naive-vs-multistream
  latency benchmark. Mocks the browser on purpose so wire-level
  differences aren't swamped by Chromium's screenshot cost. Keep it
  for the numbers.
- [`examples/agent_pubsub`](../agent_pubsub/) — demonstrates the four
  wire shapes with hardcoded content and no agent loop.
- **`examples/cua-live`** (this one) — an actual agent *loop*. `-fake`
  proves the wiring with zero setup; `-live` runs the real thing.
