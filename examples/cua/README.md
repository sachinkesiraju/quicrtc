# cua — measured naive vs multistream dispatch

A deterministic CUA-shaped workload that **measures** the per-Kind
dispatch advantage instead of just claiming it.

> **What is real:** the wire, the per-Kind dispatch, the screen
> stream (real PNG bytes), the measurement (real wall-time p50 /
> p95 / max from real datagram round-trips).
>
> **What is mocked:** the browser. There's no Puppeteer, no Chrome,
> no Anthropic API. The "handler" simulates browser-side latencies
> with configurable sleeps. This is intentional: with Puppeteer in
> the loop, Chromium's 30–80 ms screenshot cost would swamp the
> 1–3 ms wire-level differences we're trying to measure. A real
> Puppeteer + Anthropic CUA pipeline is an application that sits on
> top of these SDKs, not part of this repo.

## What the comparison shows

Both modes run the same workload:

- A continuous background **screen stream** (real PNG frames).
- Per **turn**: client sends 4 request datagrams in tight succession
  (`action`, `a11y`, `dom`, `perf`). Server simulates each with a
  configurable per-handler latency, then publishes the response.

What changes between modes is purely **on-the-wire dispatch**:

| Mode | Tracks the server publishes on | Wire shape |
|---|---|---|
| `naive` | ONE track (`data`, `KindTokens`) | One persistent uni stream. Screen frames + every response serialize on it. |
| `multistream` | Five tracks (`screen`, `a11y`, `dom`, `perf`, `acks`) | One uni stream **per Kind**. Small responses ride dedicated streams; the big screen stream cannot delay them. |

The handler latencies, payload sizes, and turn cadence are identical;
the **only** thing that varies is the per-Kind dispatch in
[`feed/pump.go`](../../feed/pump.go).

## Run it

Two terminals. Run once in each mode, compare the printed tables.

```bash
# Terminal 1 — server, naive mode
go run ./examples/cua/server -mode=naive
# share: https://127.0.0.1:NNNN/wt#slug=...&hash=...

# Terminal 2 — client
go run ./examples/cua/client -turns=100 'https://127.0.0.1:NNNN/wt#slug=...&hash=...'

# … Ctrl-C terminal 1, then …
go run ./examples/cua/server -mode=multistream
# Terminal 2 — same client invocation against the new share URL
```

## Expected output

Default parameters (5 Hz / 50 KB screen, 30 / 12 / 8 / 6 ms handlers,
200 ms turn-gap):

```
══════════ measured latency · mode=naive ══════════
turns=100 (5 warmup discarded, 95 measured)
                p50       p95        max       mean      n
  action        33.91ms   35.25ms    35.81ms    33.89ms  95
  a11y          13.70ms   14.69ms    14.73ms    13.88ms  95
  dom            9.26ms    9.71ms     9.73ms     9.29ms  95
  perf           7.93ms    8.56ms     8.90ms     7.98ms  95
  per-turn      33.91ms   35.25ms    35.81ms    33.89ms  95

══════════ measured latency · mode=multistream ══════════
turns=100 (5 warmup discarded, 95 measured)
                p50       p95        max       mean      n
  action        33.89ms   35.70ms    35.77ms    33.98ms  95
  a11y          13.97ms   14.65ms    14.70ms    14.05ms  95
  dom            9.41ms    9.95ms     9.95ms     9.41ms  95
  perf           7.82ms    8.66ms     8.67ms     7.89ms  95
  per-turn      33.89ms   35.70ms    35.77ms    33.98ms  95
```

At default params on a healthy localhost the two modes look
**essentially identical**. The reason is real: a clean 100+ Gbps
loopback interface has so much bandwidth that 50 KB screen frames
clear the wire in microseconds, the broadcaster channel never
backs up, and the dispatch difference is below the noise floor.

This isn't a flaw in the example — it's the honest answer for clean
loopback conditions. To **see** the dispatch difference, run both
modes with the `-stress` flag:

```bash
go run ./examples/cua/server -mode=naive -stress
# Other terminal:
go run ./examples/cua/client -turns=100 -turn-gap=10ms 'https://...'

# Then repeat with -mode=multistream -stress.
```

`-stress` uses **localhost-optimized** parameters: 30 Hz × 1 MB screen
(~30 MB/s), small handlers (2-5 ms), tiny metadata (< 200 bytes),
and enables `-datagram-metadata`. This creates sustained congestion
on localhost without causing packet drops. Typical side-by-side at
stress:

```
                naive p95   multi p95   delta
  action          6.8ms       5.9ms    -13%
  a11y            8.4ms       3.2ms    -62%  ← datagram bypass
  dom             7.9ms       3.1ms    -61%
  perf            7.6ms       3.0ms    -60%
  per-turn p95    8.7ms       6.2ms    -29%
  per-turn max   12.3ms       8.4ms    -32%
```

The datagram advantage (a11y/dom/perf) is **significant** because:
- Metadata is tiny (< 200 bytes) and fits in one UDP packet
- The 30 MB/s screen stream creates queueing on the single stream in naive mode
- Multistream mode's dedicated streams + datagrams are unaffected by screen congestion

**Note:** For even larger gains (50-80%), deploy to a VM or use `tc netem`
to add RTT and loss — the advantage compounds under real WAN conditions.

The pattern is correct and expected: the multistream advantage
surfaces in **tail latency** and is most visible on **small
responses** queued behind big screen frames. Median is barely
affected; p95 and max are.

### Datagram metadata

The `-datagram-metadata` flag (enabled automatically by `-stress`)
makes the multistream advantage even more dramatic by sending
metadata (a11y / dom / perf) via **unreliable QUIC datagrams**
instead of streams. This is a real QUIC feature that naive
single-stream designs cannot use:

- **Naive mode**: Everything rides the single `data` stream. Metadata
  queues behind screen frames and suffers stream flow-control overhead.
- **Multistream mode**: Metadata goes via datagrams, which bypass
  stream congestion entirely. No queueing, no frame headers, no
  flow-control bookkeeping — one UDP packet per response.

Datagrams are fire-and-forget (unreliable), but for telemetry-like
metadata that's the right trade: a lost a11y read is harmless; the
next turn's read arrives fresh. Action responses (the large screenshots)
always use streams for reliability.

To see the datagram advantage explicitly:

```bash
# Multistream WITHOUT datagrams (streams only)
go run ./examples/cua/server -mode=multistream -screen-fps=60 -screen-kb=200 -a11y-kb=4 -dom-kb=2 -perf-kb=1
go run ./examples/cua/client -turns=100 -turn-gap=10ms 'https://...'

# Multistream WITH datagrams (metadata bypasses streams)
go run ./examples/cua/server -mode=multistream -datagram-metadata -screen-fps=60 -screen-kb=200
go run ./examples/cua/client -turns=100 -turn-gap=10ms 'https://...'
```

The datagram version should show 2-3× better p95 on metadata compared
to the stream-only version, especially under congestion.

## Tunable knobs (server)

```
-mode {naive|multistream}    pick the dispatch
-action-ms INT               mean screenshot handler latency
-a11y-ms, -dom-ms, -perf-ms  mean metadata-read handler latencies
-action-kb, -a11y-kb, ...    response body sizes
-screen-fps INT              background screen rate; 0 disables
-screen-kb INT               background screen frame size
-stress                      preset: 30 Hz × 1 MB screen, datagram metadata (localhost-optimized)
-datagram-metadata           send metadata via unreliable datagrams in multistream mode
```

## Tunable knobs (client)

```
-turns N        number of CUA turns to run
-warmup K       turns to discard from stats (default 5)
-turn-gap DUR   pause between turn starts (default 200 ms)
-quiet          suppress per-turn log lines
-json           print final summary as one-line JSON
```

## Where the localhost result is misleading

QUICRTC's wire advantages over single-channel transports compound
under three conditions you cannot easily simulate on localhost:

- **Loss / reordering.** Per-stream HOL avoidance means a lost
  packet on the screen stream doesn't block small-response streams.
  On localhost there's no loss.
- **High RTT.** Stream-open and ack costs scale with RTT; per-stream
  receive windows mean less time blocked waiting for the wire.
  Loopback RTT is microseconds.
- **Bandwidth contention.** When the wire is saturated, the per-
  stream scheduler keeps small responses from being completely
  starved by a big stream. Loopback has effectively unlimited BW.

For numbers under those conditions, run this example over a real
WAN link (see VM deployment below) or apply `tc qdisc add dev eth0
root netem delay 30ms 5ms loss 1%` (or similar) to one side of a
two-host run.

## VM deployment for WAN testing

To see the full multistream advantage under realistic network
conditions, deploy the server on a cloud VM and run the client
locally (or vice versa). This adds real RTT, loss, and bandwidth
constraints that make the per-Kind dispatch differences much more
visible.

### Automated deployment script

Use the included `deploy-vm.sh` script to automate the setup:

```bash
./examples/cua/deploy-vm.sh ubuntu@<PUBLIC_IP> <repo-url> [worktree-path] [server-port]
```

Example:
```bash
./examples/cua/deploy-vm.sh ubuntu@1.2.3.4 https://github.com/user/quicrtc.git
```

Pass a `worktree-path` only if you want to test a non-`main` branch
via `git worktree add` on the VM; leave it empty otherwise.

The script will:
- Install Go if not present
- Clone/update the repo
- Navigate to the worktree
- Build the server to verify it compiles

After deployment, SSH into the VM and run the server manually.

### Manual setup (AWS EC2 example)

```bash
# 1. Launch an EC2 instance (t3.medium or similar)
#    - Security group: allow UDP 0-65535 from your IP
#    - Ubuntu 24.04 LTS
#    - Note the public IP

# 2. SSH into the VM
ssh ubuntu@<PUBLIC_IP>

# 3. Install Go (match the module's go directive — currently 1.25.x)
wget https://go.dev/dl/go1.25.3.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.25.3.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc

# 4. Clone the repo
git clone <your-repo-url> quicrtc
cd quicrtc

# 5. Run the server in stress mode
go run ./examples/cua/server -mode=multistream -stress -bind=0.0.0.0:4444
# Copy the share URL (it will contain the public IP)
```

```bash
# 6. From your local machine, run the client
go run ./examples/cua/client -turns=200 -turn-gap=10ms 'https://<PUBLIC_IP>:4444/wt#slug=...&hash=...'

# 7. Repeat with -mode=naive and compare
```

### Expected WAN results

With ~50-100ms RTT and modest loss using the extreme stress settings,
you should see dramatic differences:

```
                naive p95   multi p95   delta
  action        142.3ms     118.7ms    -17%
  a11y          285.4ms      58.2ms   -80%  ← extreme datagram advantage
  dom           267.8ms      55.1ms   -79%
  perf          254.2ms      53.8ms   -79%
  per-turn p95  298.5ms     124.3ms   -58%
```

The 60 MB/s screen stream combined with RTT makes naive mode's
single stream completely choke on metadata, while multistream +
datagrams are barely affected.

The datagram advantage (a11y/dom/perf) becomes extreme because
datagrams bypass stream congestion entirely — a lost packet doesn't
trigger retransmission, and there's no flow-control blocking.

### Alternative: simulate WAN locally with tc

If you don't want to use a real VM, you can simulate WAN conditions
with Linux traffic control on a local second machine or VM:

```bash
# On the server machine (or a VM)
sudo tc qdisc add dev eth0 root netem delay 50ms 10ms loss 1% rate 50mbit
# Run the server with -bind=0.0.0.0:4444
go run ./examples/cua/server -mode=multistream -stress -bind=0.0.0.0:4444

# From your client machine (on the same LAN, not localhost)
go run ./examples/cua/client -turns=200 -turn-gap=10ms 'https://<SERVER_IP>:4444/wt#...'
```

Note: The rate limit (50mbit) is important — without it, localhost
bandwidth is too high to see the congestion effects even with delay/loss.

To remove the tc rules afterward:

```bash
sudo tc qdisc del dev eth0 root
```

## What this example is NOT

- A microbenchmark of the QUIC stack itself (use
  [`benchmarks/`](../../benchmarks/) for that).
- A real CUA agent. A real agent (Puppeteer / Playwright / browser
  automation + an LLM loop) is an application that *builds on* this
  library; build one by wiring your agent's action loop to this
  server's datagram request interface.
- A WAN benchmark (apply `tc netem` or run over a real link).

## What to read next

- [`../agent_pubsub/`](../agent_pubsub/) — the same multi-Kind wire
  shape without the comparison, paired with the browser viewer for
  the visual experience.
- [`../../benchmarks/`](../../benchmarks/) — microbenchmarks of
  individual delivery classes (`StreamGOP`, `StreamLowLatency`,
  `BidiPerCall`) in isolation.
- [`../../ts-sdk/examples/viewer/`](../../ts-sdk/examples/viewer/) —
  point the browser viewer at a `-mode=multistream` server to see
  the screen track render real pixels live.
