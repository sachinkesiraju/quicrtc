#!/bin/bash
# One-time GCP setup for the WAN benchmark.
# Creates 2 e2-micro VMs in different US regions, opens firewall ports,
# and prints the IPs for use with deploy.sh.
#
# Prerequisites:
#   - gcloud CLI installed and authenticated (`gcloud auth login`)
#   - export PROJECT_ID=your-gcp-project-id    (or pass as $1)
#   - GCP free-tier eligible regions: us-west1, us-central1, us-east1
#     (only one of these gets the free always-on e2-micro per project)
#
# Usage: ./gcp_setup.sh [PROJECT_ID]

set -e
PROJECT_ID="${1:-${PROJECT_ID:-}}"
if [ -z "$PROJECT_ID" ]; then
  echo "ERROR: set PROJECT_ID env or pass it as the first arg"
  exit 1
fi

REGION_SERVER=us-east1
ZONE_SERVER=us-east1-b
REGION_CLIENT=us-west1
ZONE_CLIENT=us-west1-a

VM_SERVER=quicrtc-bench-server
VM_CLIENT=quicrtc-bench-client

MACHINE_TYPE=e2-micro
IMAGE_FAMILY=debian-12
IMAGE_PROJECT=debian-cloud

# Pull in cloud-init if a sibling pair is present at the repo root. This
# bootstraps Go, ffmpeg, tc/netem, and UDP buffer tuning on each VM so
# deploy.sh can scp + run with no extra apt steps.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CLOUD_INIT_SERVER="$SCRIPT_DIR/../cloud-init-server.yaml"
CLOUD_INIT_CLIENT="$SCRIPT_DIR/../cloud-init-client.yaml"
SERVER_METADATA_ARGS=()
CLIENT_METADATA_ARGS=()
if [ -f "$CLOUD_INIT_SERVER" ]; then
  SERVER_METADATA_ARGS=(--metadata-from-file "user-data=$CLOUD_INIT_SERVER")
  echo ">>> using cloud-init for server VM: $CLOUD_INIT_SERVER"
fi
if [ -f "$CLOUD_INIT_CLIENT" ]; then
  CLIENT_METADATA_ARGS=(--metadata-from-file "user-data=$CLOUD_INIT_CLIENT")
  echo ">>> using cloud-init for client VM: $CLOUD_INIT_CLIENT"
fi
if [ ${#SERVER_METADATA_ARGS[@]} -eq 0 ]; then
  echo ">>> NOTE: no cloud-init-*.yaml found at repo root — VMs will boot vanilla Debian"
fi

echo ">>> Setting project: $PROJECT_ID"
gcloud config set project "$PROJECT_ID"

echo ">>> Creating firewall rule: allow ports 4433/udp, 4434/udp, 8080/tcp, 8090/tcp"
gcloud compute firewall-rules create quicrtc-bench-allow \
  --network=default \
  --direction=INGRESS \
  --action=ALLOW \
  --rules=tcp:8080,tcp:8090,udp:4433,udp:4434 \
  --source-ranges=0.0.0.0/0 \
  --target-tags=quicrtc-bench \
  || echo "  (firewall rule may already exist)"

echo ">>> Creating SERVER VM in $ZONE_SERVER"
gcloud compute instances create "$VM_SERVER" \
  --zone="$ZONE_SERVER" \
  --machine-type="$MACHINE_TYPE" \
  --image-family="$IMAGE_FAMILY" \
  --image-project="$IMAGE_PROJECT" \
  --tags=quicrtc-bench \
  --boot-disk-size=10GB \
  "${SERVER_METADATA_ARGS[@]}"

echo ">>> Creating CLIENT VM in $ZONE_CLIENT"
gcloud compute instances create "$VM_CLIENT" \
  --zone="$ZONE_CLIENT" \
  --machine-type="$MACHINE_TYPE" \
  --image-family="$IMAGE_FAMILY" \
  --image-project="$IMAGE_PROJECT" \
  --tags=quicrtc-bench \
  --boot-disk-size=10GB \
  "${CLIENT_METADATA_ARGS[@]}"

echo ">>> Waiting for VMs to boot + cloud-init to finish..."
sleep 20
if [ -f "$CLOUD_INIT_SERVER" ]; then
  for vm in "$VM_SERVER:$ZONE_SERVER" "$VM_CLIENT:$ZONE_CLIENT"; do
    name="${vm%:*}"
    zone="${vm##*:}"
    echo ">>> waiting for cloud-init on $name"
    for i in $(seq 1 36); do
      if gcloud compute ssh "$name" --zone="$zone" \
           --command='cloud-init status --wait >/dev/null 2>&1 && echo ready' \
           >/dev/null 2>&1; then
        break
      fi
      sleep 5
    done
  done
fi

SERVER_IP=$(gcloud compute instances describe "$VM_SERVER" --zone="$ZONE_SERVER" \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)')
CLIENT_IP=$(gcloud compute instances describe "$VM_CLIENT" --zone="$ZONE_CLIENT" \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)')

cat <<EOF

==========================
SETUP COMPLETE
==========================
Server VM: $VM_SERVER  ($ZONE_SERVER)  public IP: $SERVER_IP
Client VM: $VM_CLIENT  ($ZONE_CLIENT)  public IP: $CLIENT_IP

Save these for deploy.sh:
  export SERVER_VM=$VM_SERVER  ZONE_SERVER=$ZONE_SERVER
  export CLIENT_VM=$VM_CLIENT  ZONE_CLIENT=$ZONE_CLIENT
  export SERVER_IP=$SERVER_IP

Then run:
  ./deploy.sh

To tear down later:
  gcloud compute instances delete $VM_SERVER --zone=$ZONE_SERVER --quiet
  gcloud compute instances delete $VM_CLIENT --zone=$ZONE_CLIENT --quiet
  gcloud compute firewall-rules delete quicrtc-bench-allow --quiet

EOF
