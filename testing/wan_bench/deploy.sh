#!/bin/bash
# Deploy + run the WAN benchmark.
# Assumes gcp_setup.sh has run; expects env vars exported by it.
#
# Usage: ./deploy.sh [test_name]    test_name: setup | multi | resume | scale | all
#
# Saves CSVs to wan_results/.

set -e
TEST="${1:-all}"

: "${SERVER_VM:?set SERVER_VM (e.g., quicrtc-bench-server)}"
: "${ZONE_SERVER:?set ZONE_SERVER (e.g., us-east1-b)}"
: "${CLIENT_VM:?set CLIENT_VM (e.g., quicrtc-bench-client)}"
: "${ZONE_CLIENT:?set ZONE_CLIENT (e.g., us-west1-a)}"
: "${SERVER_IP:?set SERVER_IP}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS="$SCRIPT_DIR/wan_results"
mkdir -p "$RESULTS"

BIN="$SCRIPT_DIR/wan_bench-linux-amd64"
if [ ! -f "$BIN" ]; then
  echo "missing $BIN — run: GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o wan_bench-linux-amd64 ."
  exit 1
fi

# Find an input.ivf next to webrtc-bench (or generate one)
IVF="$SCRIPT_DIR/../../webrtc-bench/server/input.ivf"
if [ ! -f "$IVF" ]; then
  echo "generating input.ivf via ffmpeg..."
  ffmpeg -y -f lavfi -i testsrc=duration=20:size=1280x720:rate=30 -g 30 -b:v 2M "$IVF" 2>&1 | tail -3
fi

echo ">>> scp binary + ivf to SERVER VM ($SERVER_VM)"
gcloud compute scp "$BIN" "$IVF" "$SERVER_VM:~/" --zone="$ZONE_SERVER"

echo ">>> scp binary to CLIENT VM ($CLIENT_VM)"
gcloud compute scp "$BIN" "$CLIENT_VM:~/" --zone="$ZONE_CLIENT"

echo ">>> measure RTT between client and server (sanity)"
gcloud compute ssh "$CLIENT_VM" --zone="$ZONE_CLIENT" --command="ping -c 5 $SERVER_IP" || true

start_server() {
  echo ">>> starting server (mode=server, external-host=$SERVER_IP) on $SERVER_VM"
  gcloud compute ssh "$SERVER_VM" --zone="$ZONE_SERVER" --command="\
    pkill -f wan_bench-linux-amd64 2>/dev/null || true; \
    sleep 1; \
    nohup ./wan_bench-linux-amd64 -mode=server -external-host=$SERVER_IP -ivf=input.ivf > server.log 2>&1 &"
  sleep 3
  echo "  server started; checking /info..."
  for i in $(seq 1 10); do
    if curl -s --max-time 5 "http://$SERVER_IP:8090/info" > /dev/null; then
      echo "  /info reachable"
      return 0
    fi
    sleep 2
  done
  echo "  ERROR: /info not reachable after 20s"
  gcloud compute ssh "$SERVER_VM" --zone="$ZONE_SERVER" --command="tail -20 server.log"
  return 1
}

stop_server() {
  echo ">>> stopping server"
  gcloud compute ssh "$SERVER_VM" --zone="$ZONE_SERVER" --command="pkill -f wan_bench-linux-amd64; sleep 1"
}

run_test() {
  local name=$1
  local args=$2
  local csv="$RESULTS/${name}.csv"
  echo ">>> running test: $name  ($args)"
  gcloud compute ssh "$CLIENT_VM" --zone="$ZONE_CLIENT" --command="\
    ./wan_bench-linux-amd64 -mode=client -server=$SERVER_IP -test=$name -csv=${name}.csv $args"
  echo ">>> downloading $name CSV"
  gcloud compute scp "$CLIENT_VM:~/${name}.csv" "$csv" --zone="$ZONE_CLIENT" || true
  if [ -f "$csv" ]; then
    echo "  saved $csv"
  fi
}

start_server

case "$TEST" in
  setup)
    run_test setup "-n=100"
    ;;
  multi)
    run_test multi "-runs=20"
    ;;
  resume)
    run_test resume "-n=100"
    ;;
  scale)
    run_test scale "-total=200 -batch=25 -interval=1s"
    ;;
  all)
    run_test setup "-n=100"
    run_test multi "-runs=20"
    run_test resume "-n=100"
    run_test scale "-total=200 -batch=25 -interval=1s"
    ;;
  *)
    echo "unknown test: $TEST"
    ;;
esac

stop_server

echo ""
echo "=========================="
echo "WAN BENCHMARK DONE"
echo "=========================="
echo "CSVs in $RESULTS/"
ls -la "$RESULTS/"
echo ""
echo "Tear down VMs when finished:"
echo "  gcloud compute instances delete $SERVER_VM --zone=$ZONE_SERVER --quiet"
echo "  gcloud compute instances delete $CLIENT_VM --zone=$ZONE_CLIENT --quiet"
