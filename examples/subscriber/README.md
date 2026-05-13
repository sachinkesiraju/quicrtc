# subscriber

The minimal "hello world" subscriber. Dials a quicrtc server, prints
the SDP, and reads AUs from the named track. Companion to
[`../publisher`](../publisher/).

## What this teaches

- `client.Dial(ctx, shareURL, opts)` — the SDK parses `slug` + `hash`
  out of the URL fragment for you. No manual fragment parsing.
- `c.RecvOn(ctx, trackName)` — the multi-track-aware receive
  primitive. **Prefer this over `c.Recv()`**, which is the legacy
  "primary" shim.
- Per-AU `Keyframe` / `Seq` semantics: gap detection (`K`/`P` markers,
  `gaps` counter), age-of-last-keyframe display.

## Run

```bash
# Terminal 1
go run ./examples/publisher
# share: https://127.0.0.1:NNNN/wt#slug=...&hash=...

# Terminal 2 — paste the URL exactly as printed
go run ./examples/subscriber 'https://127.0.0.1:NNNN/wt#slug=...&hash=...'
```

## Expected output

```
connected sid=...
sdp: codec=synthetic 64x64@30
subscribing to: video

[subscriber] frames=486 │ last K @ seq=480 (200ms ago) │ gaps=0 │ 1.9 MB
^C
── summary ──────────────────────────
  uptime   16.2s
  AU recv  486 (K 16, P 470)
  gaps     0
  bytes    1.9 MB
```

The `gaps` count will typically read `1` if you join mid-stream
(the very first AU's seq won't be 0). That's the headline message
the summary calls out — it isn't a wire defect.

## What to read next

- relay — same wire, with 1:N fan-out.
- agent_pubsub/viewer — multi-track CLI viewer.
- ts-sdk/examples/viewer — the browser equivalent of this example.
