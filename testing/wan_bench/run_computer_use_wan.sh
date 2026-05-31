#!/bin/bash
# run_computer_use_wan.sh — re-run the closed-loop computer-use action->DOM
# RTT benchmark across the two GCP VMs, and report the EXTREME tail.
#
# This is the validation for the bidi/client-delivery reliability fix: the
# pre-fix run left a ~30s max in wan_results/computer_use.csv (2/3600
# stalled actions hiding past p99). This script regenerates the data and
# prints p50/p99/p99.9/max so you can confirm the multi-second max is gone.
#
# Prereqs (same contract as deploy.sh — typically from gcp_setup.sh):
#   SERVER_VM, ZONE_SERVER, CLIENT_VM, ZONE_CLIENT, SERVER_IP
# Optional knobs (env):
#   SCREEN_MBPS (default 12; try 60 to stress), TRIALS (3), TRIAL_SEC (12),
#   LOSS_PCT (0 = honest real-WAN; >0 applies tc netem on the server to
#   force the tail to surface), LOSS_IFACE (default ens4 — GCP images often
#   use ens4, not eth0).
#
# Usage:
#   GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o wan_bench-linux-amd64 .
#   SERVER_VM=... ZONE_SERVER=... CLIENT_VM=... ZONE_CLIENT=... SERVER_IP=... \
#     ./run_computer_use_wan.sh
set -euo pipefail

: "${SERVER_VM:?set SERVER_VM (e.g., quicrtc-bench-server)}"
: "${ZONE_SERVER:?set ZONE_SERVER (e.g., us-east1-b)}"
: "${CLIENT_VM:?set CLIENT_VM (e.g., quicrtc-bench-client)}"
: "${ZONE_CLIENT:?set ZONE_CLIENT (e.g., us-west1-a)}"
: "${SERVER_IP:?set SERVER_IP}"

SCREEN_MBPS="${SCREEN_MBPS:-12}"
TRIALS="${TRIALS:-3}"
TRIAL_SEC="${TRIAL_SEC:-12}"
LOSS_PCT="${LOSS_PCT:-0}"
LOSS_IFACE="${LOSS_IFACE:-ens4}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS="$SCRIPT_DIR/wan_results"
mkdir -p "$RESULTS"
BIN="$SCRIPT_DIR/wan_bench-linux-amd64"
[ -f "$BIN" ] || { echo "missing $BIN — run: GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o wan_bench-linux-amd64 ."; exit 1; }

IVF="$SCRIPT_DIR/../../webrtc-bench/server/input.ivf"
if [ ! -f "$IVF" ]; then
  echo ">>> generating input.ivf via ffmpeg"
  ffmpeg -y -f lavfi -i testsrc=duration=20:size=1280x720:rate=30 -g 30 -b:v 2M "$IVF" 2>&1 | tail -2
fi

echo ">>> scp binary + ivf to server, binary to client"
gcloud compute scp "$BIN" "$IVF" "$SERVER_VM:~/" --zone="$ZONE_SERVER"
gcloud compute scp "$BIN" "$CLIENT_VM:~/" --zone="$ZONE_CLIENT"

# Server invocation. -loss-pct needs root (tc netem), so prefix sudo when set.
SRV="./wan_bench-linux-amd64 -mode=server -external-host=$SERVER_IP -ivf=input.ivf"
if [ "$(printf '%.0f' "$(echo "$LOSS_PCT * 100" | bc 2>/dev/null || echo 0)")" != "0" ]; then
  echo ">>> note: LOSS_PCT=$LOSS_PCT on $LOSS_IFACE (server runs under sudo for tc)"
  SRV="sudo $SRV -loss-pct=$LOSS_PCT -loss-iface=$LOSS_IFACE"
fi

echo ">>> starting server on $SERVER_VM"
gcloud compute ssh "$SERVER_VM" --zone="$ZONE_SERVER" --command="\
  sudo pkill -f wan_bench-linux-amd64 2>/dev/null || true; sleep 1; \
  nohup $SRV > server.log 2>&1 &"
for i in $(seq 1 10); do
  curl -s --max-time 5 "http://$SERVER_IP:8090/info" >/dev/null && { echo "  /info reachable"; break; }
  sleep 2
  [ "$i" = 10 ] && { echo "  ERROR: /info not reachable"; gcloud compute ssh "$SERVER_VM" --zone="$ZONE_SERVER" --command="tail -20 server.log"; exit 1; }
done

echo ">>> running computer_use client (screen=${SCREEN_MBPS}Mbps trials=$TRIALS trial-sec=$TRIAL_SEC)"
gcloud compute ssh "$CLIENT_VM" --zone="$ZONE_CLIENT" --command="\
  ./wan_bench-linux-amd64 -mode=client -server=$SERVER_IP -mode-name=computer_use \
    -screen-mbps=$SCREEN_MBPS -trials=$TRIALS -trial-sec=$TRIAL_SEC" | tee "$RESULTS/computer_use_postfix.headline.txt"

echo ">>> stopping server"
gcloud compute ssh "$SERVER_VM" --zone="$ZONE_SERVER" --command="sudo pkill -f wan_bench-linux-amd64 || true"

echo ">>> downloading CSV -> wan_results/computer_use_postfix.csv (keeps the committed pre-fix CSV intact)"
gcloud compute scp "$CLIENT_VM:~/computer_use.csv" "$RESULTS/computer_use_postfix.csv" --zone="$ZONE_CLIENT" || true

echo ""
echo "=================================================================="
echo "DONE. Compare the new tail against the committed pre-fix baseline:"
echo "  pre-fix : $RESULTS/computer_use.csv          (quicrtc max ~30,932 ms)"
echo "  post-fix: $RESULTS/computer_use_postfix.csv"
echo "Headline above reports p50/p99/p99.9/max for both arms. The fix is"
echo "validated if the quicrtc 'max' is now within a few hundred ms of p99"
echo "(no multi-second stalls), not just a healthy p99."
echo "=================================================================="
