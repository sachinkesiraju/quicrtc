# wan_bench — Cross-region WAN benchmark

Tests quicrtc, raw WebTransport, and Pion WebRTC across two cloud VMs in
different regions. Single static binary; runs in `-mode=server` on one
VM and `-mode=client` on the other. Tests:

- **setup** — per-PC time to first message (media-ready)
- **multi** — concurrent burst (50KB × 30fps) + tokens (200/s × 64B), measure token tail
- **resume** — quicrtc resume vs WebRTC full reconnect
- **scale** — ramp to N parallel sessions, measure success rate + setup degradation

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
```

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
