# quicrtc

TypeScript client for [quicrtc](https://github.com/sachinkesiraju/quicrtc),
wire-compatible with the Go server.

## Install

```bash
npm install quicrtc
```

Requires WebTransport in the browser or Node 18+ with a
WebTransport-capable runtime.

## Quick start

```typescript
import { QuicRTCClient, TrackKind } from 'quicrtc';

const client = new QuicRTCClient();
await client.connect('https://your-server/wt', {
  slug: 'auth-token',
  certHash: 'base64url-sha256-der-hash', // only for self-signed dev certs
});

// Receive on a named track
const au = await client.recvOn('reasoning');
console.log(new TextDecoder().decode(au.bytes));

// Publish a track back to the server
const sender = await client.publish({ name: 'actions', kind: TrackKind.ToolCalls });
await sender.send({
  bytes: new TextEncoder().encode('{"type":"click","x":100,"y":200}'),
  keyframe: true,
  ptsMicro: 0,
  seq: 0,
});

await client.close();
```

A framework-free browser viewer that exercises every track kind lives
in [`examples/viewer/`](examples/viewer/).

## API

### `QuicRTCClient`

```typescript
const client = new QuicRTCClient();
await client.connect(url, options);
```

```typescript
interface ClientOptions {
  slug: string;                  // auth credential (shared secret or JWT)
  certHash?: string;             // SHA-256 cert pin for self-signed dev certs
  insecureSkipVerify?: boolean;  // dev only
  helloTimeout?: number;         // ms
  sessionId?: string;            // resume an earlier session
  features?: string[];           // capabilities to negotiate
}
```

Methods:
- `recv()`, `recvOn(trackName)` — pull next AU from primary or named track
- `publish(track)` — bidirectional: publish a track back to the server
- `requestKeyframe(trackName)` — subscriber-driven keyframe request
- `sendDatagram(payload)`, `receiveDatagram()` — raw QUIC datagrams
- `getDataChannel()` — reliable bidi messaging
- `getSDP()`, `getRemoteTracks()`, `getStats()` — introspection
- `close()`

### Tracks

```typescript
enum TrackKind {
  Video = 'video',
  Audio = 'audio',
  Tokens = 'tokens',
  ToolCalls = 'tool_calls',
  Telemetry = 'telemetry',
}

// Pull
const au = await track.recv();

// Push
track.onAU((au) => { /* ... */ });
```

### Data channel

```typescript
const dc = client.getDataChannel();
await dc.sendText('hello');
await dc.send(new Uint8Array([1, 2, 3]));
dc.onMessage((data) => { /* ... */ });
```

## Wire codec performance

The wire-format implementation is allocation-free on the hot path.
Reproduce with `npm run bench`.

| Operation                                | Time per op  | Allocations |
|------------------------------------------|--------------|-------------|
| Control frame encode                     | ~0.1 µs      | 0           |
| Feed frame encode (1KB payload)          | ~0.3 µs      | 0           |
| Datagram envelope encode (64B)           | ~0.1 µs      | 0           |
| Datagram envelope decode                 | <0.1 µs      | 0           |
| Backpressure marshal (with needs_keyframe) | ~0.2 µs    | 0           |
| HELLO / SDP JSON marshal+unmarshal       | ~0.4 µs each | 0           |

End-to-end numbers (Go server side) are in the repo's
[`benchmarks/`](../benchmarks/) suite.

## Browser support

| Browser     | WebTransport | Status                  |
|-------------|--------------|-------------------------|
| Chrome 114+ | Native       | Full                    |
| Edge 114+   | Native       | Full                    |
| Safari 26.4+ (iOS / macOS) | Native | Full              |
| Firefox     | Behind flag  | Enable `network.webtransport.enabled` |

## License

Apache 2.0 — see [LICENSE](../LICENSE) and [NOTICE](../NOTICE).
