# ts-sdk/examples

Browser examples for the `quicrtc` package. One example, which covers the
full SDK surface:

## viewer — universal browser viewer

A framework-free HTML page that connects to **any** quicrtc Go server
via a pasted share URL, inspects the announced remote tracks, and
renders the right panel for each one:

- `KindVideo` → canvas K/P debug overlay. Click to publish back
  `user_actions`.
- `KindTokens` → live-appending text stream.
- `KindToolCalls` → JSON event list.
- `KindTelemetry` → key/value gauges.
- *(datagrams)* → auto-drained from the WebTransport datagrams pipe.
- `DataChannel` → transcript box + send form (if exposed).

Plus a status pill tracking `Disconnected | Connecting | Connected |
Error` and a backpressure indicator that lights up when the SDK fires
`onBackpressure(track, level≥2)` (the viewer then drops further
screen AUs until level returns to 0).

One viewer, many servers — point it at any Go example under
examples.

See viewer/README.md for setup + run instructions.
