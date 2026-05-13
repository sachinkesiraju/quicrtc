# datachannel — bidi control channel

quicrtc multiplexes a "DataChannel" on the session's bidi control
stream (the same stream that carries HELLO/SDP/Announce). Apps use
it to send small reliable messages without opening per-message
streams. Equivalent to WebRTC's `RTCDataChannel` but layered on
QUIC instead of SCTP.

This example is split into `server/` and `client/` subdirs so it's
self-contained (the old version coupled the echo handler to the
`publisher` example, which was a footgun).

## What this teaches

- `server.Config{ OnDataChannel: func(*datachannel.Channel) }` —
  fires once per session with the channel handle.
- `c.DataChannel()` on the client side returns the same handle.
- The channel is **persistent and bidirectional** — both sides can
  `Send` and `Recv` concurrently. It's not request/reply.
- A real-life chat-style stdin loop on the client side (not 5
  canned pings).

## Run (two terminals)

```bash
# Terminal 1 — server
go run ./examples/datachannel/server
# share: https://127.0.0.1:NNNN/wt#slug=...&hash=...

# Terminal 2 — client
go run ./examples/datachannel/client 'https://127.0.0.1:NNNN/wt#slug=...&hash=...'
```

Type a line in terminal 2, press Enter. The server echoes; meanwhile
a 1 Hz heartbeat from the server interleaves naturally.

Press **Ctrl-D** (EOF) or **Ctrl-C** to exit.

## Expected output (client terminal)

```
connected sid=...
type a line + Enter to send. Ctrl-D / Ctrl-C to exit.

client> hello
server< hello
server* heartbeat (uptime 1s)
client> second message
server< second message
server* heartbeat (uptime 2s)
[client] 2 sent · 2 acks · 2 heartbeats — bye
```

Transcript prefixes:
- `client>` — what you typed.
- `server<` — echo reply.
- `server*` — server-initiated heartbeat.

The interleaving of echoes and heartbeats is the *point*: it's a true
bidi channel, not request/reply.

## What to read next

- [`../publisher`](../publisher/) + [`../subscriber`](../subscriber/) — the track-based primitive.
- [`../agent_pubsub`](../agent_pubsub/) — multi-track + datachannel together.
- [`../../ts-sdk/examples/viewer`](../../ts-sdk/examples/viewer/) — pasting this server's URL into the viewer surfaces the datachannel transcript box.
