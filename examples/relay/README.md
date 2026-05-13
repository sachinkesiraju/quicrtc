# relay — 1:N fan-out

Native-wire fan-out. One upstream publisher → many downstream
subscribers. The relay re-emits the same wire format, so downstreams
can't tell they're talking to a relay vs the original publisher.
That's the teaching point: **the relay is transparent on the wire**.

## What this teaches

- `relay.New(relay.Config{ Server, Upstreams, ClientOptions })` — one
  call, no SFU boilerplate.
- The relay reuses `server.Config` for its downstream half and
  `client.Options` for its upstream half. Same APIs as the rest of
  the SDK.
- `Server().SubscriberCount()` as a live observable — open more
  subscriber terminals and watch the count climb.

## Run (three terminals)

```bash
# Terminal 1 — upstream publisher
go run ./examples/publisher
# share: https://127.0.0.1:NNNN/wt#slug=...&hash=...  (PUB_URL)

# Terminal 2 — relay
go run ./examples/relay \
    -listen 127.0.0.1:4445 \
    -upstream 'PUB_URL'
# share: https://127.0.0.1:4445/wt#slug=...&hash=...   (RELAY_URL)

# Terminal 3 (and 4, and 5…) — downstream subscribers
go run ./examples/subscriber 'RELAY_URL'
```

## Expected output (relay terminal)

```
share: https://127.0.0.1:4445/wt#slug=...&hash=...
(open more subscribers against this URL to watch fan-out)
[relay] upstreams=1 (configured) │ subs=3 │ uptime=12s
^C
── summary ──────────────────────────
  uptime     12s
  upstreams  1
  peak subs  3
```

`subs=N` updates live as subscribers connect and disconnect. Open
two more subscriber terminals to watch the count tick from 1 → 2 → 3.

## What this does NOT do

- No transcoding — bytes flow straight through.
- No recording — AUs are forwarded, not stored.
- No track filtering — every announced track is re-served. If two
  upstreams announce the same track name, AUs interleave (a SFU
  with proper merging is a bigger design).

## What to read next

- agent_pubsub — multi-track flow that exercises
  the same fan-out machinery under load.
- ts-sdk/examples/viewer — open
  the browser viewer against the relay's URL to confirm transparency.
