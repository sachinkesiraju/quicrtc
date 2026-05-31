# publisher

The minimal "hello world" for the Go SDK. Stands up a server, registers
**one** video track with an explicit `Kind`, and publishes 4 KB
synthetic frames at 30 fps.

## What this teaches

- `server.New` + `server.Config` — auto-generated cert, listen-on-zero port.
- `srv.AddTrackSpec(server.TrackSpec{Name, Kind})` — the recommended
  track registration path. *Never* use the legacy `srv.Publisher()`
  singular; it bypasses the per-`Kind` delivery dispatch.
- `OnSession` — fires when a subscriber attaches.
- The shared live-status + boxed-summary pattern used by every Go
  example in this repo.

## Run

```bash
go run ./examples/publisher
```

Prints a share URL on stdout. Paste it into:

- `go run ./examples/subscriber '<share-url>'` for the Go CLI subscriber, or
- `ts-sdk/examples/viewer/` for the visual browser viewer.

## Expected output

```
share: https://127.0.0.1:NNNN/wt#slug=...&hash=...
(Ctrl-C to stop)
session connected sid=...
[publisher] sessions=1 │ frames=487 K=16 P=471 │ 1.9 MB │ 30.0 fps
^C
── summary ──────────────────────────
  uptime    16.2s
  sessions  1
  AU sent   487 (K 16, P 471)
  bytes     1.9 MB
  avg rate  30.0 fps
```

The `[publisher] …` line updates in place every 200 ms on a TTY (no
scrolling). Output is suppressed when stderr is redirected to a file
or pipe, so log files stay clean.

## What to read next

- subscriber — paste the share URL into this.
- agent_pubsub — multi-track + datagrams + keyframe recovery.
- ts-sdk/examples/viewer — paste the share URL into a browser.
