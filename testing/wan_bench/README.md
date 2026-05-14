# wan_bench — Cross-region WAN benchmark

Tests quicrtc, raw WebTransport, and Pion WebRTC across two cloud VMs in
different regions. Single static binary; runs in `-mode=server` on one
VM and `-mode=client` on the other. Tests:

- **setup** — per-PC time to first message (media-ready)
- **multi** — concurrent burst (50KB × 30fps) + tokens (200/s × 64B), measure token tail
- **resume** — quicrtc resume vs WebRTC full reconnect
- **scale** — ramp to N parallel sessions, measure success rate + setup degradation
- **computer_use** — closed-loop action→DOM RTT, quicrtc (4 tracks via
  `AddTrackSpec`) vs a TCP-mux baseline (single TLS connection, 1-byte
  stream-id, length-prefix frames). Both arms run back-to-back over the
  same real WAN path. Per-action samples written to
  `wan_results/computer_use.csv`.

## Multi-Region Testing

This benchmark supports testing across multiple region pairs to capture
different network characteristics:

- **Default**: us-east1 ↔ us-west1 (~50ms RTT, continental US)
- **Intercontinental**: us-east1 ↔ europe-west1 (~80ms RTT, transatlantic)
- **Asia-Pacific**: us-west1 ↔ asia-northeast1 (~120ms RTT, transpacific)
- **Custom**: Any GCP region pair via command-line flags

Multi-region testing addresses reviewer feedback about single-region
measurements by showing performance across different RTT bands and
geographic paths.

## Quick start (GCP free tier)

```bash
# 1. Authenticate
gcloud auth login
export PROJECT_ID=your-gcp-project

# 2. Spin up two VMs in different US regions
./gcp_setup.sh
# -> prints VM names + IPs; export the suggested env vars

# 3. Cross-compile + deploy + run all four tests
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o wan_bench-linux-amd64 .
export SERVER_VM=quicrtc-bench-server  ZONE_SERVER=us-east1-b
export CLIENT_VM=quicrtc-bench-client  ZONE_CLIENT=us-west1-a
export SERVER_IP=<from gcp_setup.sh output>
./deploy.sh all

# 4. Inspect CSVs
ls wan_results/

# 5. Tear down
gcloud compute instances delete $SERVER_VM --zone=$ZONE_SERVER --quiet
gcloud compute instances delete $CLIENT_VM --zone=$ZONE_CLIENT --quiet
gcloud compute firewall-rules delete quicrtc-bench-allow --quiet
```

## Multi-region testing

The top-level `wan-bench.sh` orchestrator simplifies multi-region testing:

```bash
# Single region pair (default us-east1 ↔ us-west1)
./wan-bench.sh

# Custom single pair
./wan-bench.sh --server-region us-east1 --client-region europe-west1

# Multiple region pairs (results separated by pair)
./wan-bench.sh --region-pairs us-east1:us-west1,us-east1:europe-west1,us-west1:asia-northeast1

# Run specific tests only across all pairs
./wan-bench.sh --region-pairs us-east1:us-west1,us-east1:europe-west1 --tests setup,resume

# Keep VMs for debugging (don't auto-teardown)
./wan-bench.sh --keep
```

**Supported regions** (auto-mapped to zones):
- US: `us-east1`, `us-east4`, `us-central1`, `us-west1`, `us-west2`
- Europe: `europe-west1`, `europe-west2`
- Asia: `asia-southeast1`, `asia-northeast1`
- Any GCP region (fallback: `<region>-a`)

**Results organization**:
```
wan_results/
├── us-east1_to_us-west1/
│   ├── setup.csv
│   ├── multi.csv
│   ├── resume.csv
│   └── scale.csv
├── us-east1_to_europe-west1/
│   └── ...
└── us-west1_to_asia-northeast1/
    └── ...
```

## Manual run (any 2 Linux machines)

```bash
# server side:
./wan_bench-linux-amd64 -mode=server -external-host=$(curl -s ifconfig.me) -ivf=input.ivf

# client side, one at a time:
./wan_bench-linux-amd64 -mode=client -server=<server-ip> -test=setup -n=20 -csv=setup.csv
./wan_bench-linux-amd64 -mode=client -server=<server-ip> -test=multi -runs=5 -csv=multi.csv
./wan_bench-linux-amd64 -mode=client -server=<server-ip> -test=resume -n=20 -csv=resume.csv
./wan_bench-linux-amd64 -mode=client -server=<server-ip> -test=scale -total=200 -batch=25 -interval=1s -csv=scale.csv

# computer_use: closed-loop action→DOM RTT, quicrtc vs TCP-mux baseline
./wan_bench-linux-amd64 -mode=client -server=<server-ip> -mode-name=computer_use \
    -screen-mbps=12 -trials=3 -trial-sec=12
# writes wan_results/computer_use.csv
```

## computer_use mode

Why it exists: the synthetic-proxy sweep at
`testing/benchmarks/agent/computer_use_sweep_test.go` suggests quicrtc
may lose to a TCP-mux baseline at synthetic ~50 ms RTT + 1-5 % loss.
We don't yet know whether that holds under REAL WAN conditions because
the synthetic proxy doesn't model TCP cwnd halving, slow-start, or RTO.
This mode runs both arms back-to-back over the same real WAN path
between the two VMs created by `gcp_setup.sh`, so the per-trial
RTT/loss/jitter state is comparable.

### Wire layout

| Arm | Transport | screen | actions (client→server) | dom_events (server→client) | telemetry |
| --- | --- | --- | --- | --- | --- |
| quicrtc | one quicrtc session, 4 tracks | `AddTrackSpec(KindVideo)` | client `Publish(KindToolCalls)` | `AddTrackSpec(KindTokens)` | `AddTrackSpec(KindTelemetry)` |
| baseline | one TLS connection over TCP | length-prefix frame, sid=0 | sid=1 | sid=2 | sid=3 |

Both arms see the same payload sizes (screen=`-screen-mbps` Mbps / 30
fps; actions=64 B at 100/s; dom_events=256 B echoed once per action;
telemetry=64 B at 200/s) and the same duration (`-trial-sec`, default
12 s).

### Flags

| Flag | Default | Notes |
| --- | --- | --- |
| `-mode-name=computer_use` | — | selects this mode (alias for `-test`) |
| `-screen-mbps` | `12` | per-frame size derived from this; try `60` for stress |
| `-trials` | `3` | trials per arm |
| `-trial-sec` | `12` | seconds per trial |
| `-loss-pct` | `0` | server-side: if >0, applies `tc qdisc add dev <iface> root netem loss <pct>%` on Linux. Removed on SIGTERM. No-op + WARN on non-Linux or when `tc` is missing. **Headline runs should set this to 0** — the WAN itself is the network condition. |
| `-loss-iface` | `eth0` | server-side iface for `-loss-pct` (use `ens4` etc. on some GCP images) |
| `-tcp-baseline-addr` | `:4435` | TCP-mux baseline TLS listen addr (server) / dial addr (client, when /info doesn't carry it) |

### Output

`wan_results/computer_use.csv` columns:

```
protocol,trial,action_index,rtt_ms,err
```

One row per action, both arms, all trials. The headline emitted to
stdout at end of run looks like:

```
=== Computer-use over real WAN, RTT≈<measured> start / <measured> end, loss=<label> ===
quicrtc:  trials=N actions=N  p50=X p99=Y mean=Z
baseline: trials=N actions=N  p50=A p99=B mean=C
ratio (baseline/quicrtc): p50 = R1x, p99 = R2x
```

### Firewall

The TCP-mux baseline binds `:4435/tcp` on the server VM. If you
deviate from the canned `gcp_setup.sh` rule, make sure `:4435/tcp`
ingress is allowed alongside the existing `:4433/udp`, `:4434/udp`,
`:8080/tcp`, `:8090/tcp`.

### Caveats

- `-loss-pct` requires root on the bench VM to invoke `tc`. The GCP
  free-tier images may need `sudo apt-get install -y iproute2` first
  (the package ships `tc`). If `tc` is missing, the flag is a logged
  no-op and the bench runs with WAN-only loss — which is the honest
  headline anyway.
- The bench is "one-at-a-time" per arm: the server's OnSession gate
  serialises trial pumps on a session mutex so two concurrent
  computer_use clients don't share screen frames. Other test modes
  (setup/multi/resume/scale) are unaffected — they don't announce
  the `cu_actions` track, so the OnSession gate times out fast and
  the pumps never start for them.

## What gets measured

- `setup.csv` — `protocol,phase=media_ready,index,ms,err` per setup
- `multi.csv` — `protocol,phase=token,index,ms,err` per token arrival
- `resume.csv` — `protocol,phase={initial_setup|resume},index,ms,err`
- `scale.csv` — `protocol,phase=scale_setup,index,ms,err` per session attempt

## Required ports (firewall)

- 4433/udp — quicrtc (WebTransport)
- 4434/udp — raw WebTransport
- 8080/tcp — Pion WebRTC signaling
- 8090/tcp — info endpoint
