#!/usr/bin/env bash
# run.sh — launch both cua servers (naive + multistream) under -stress
# plus a static file server for the compare page, and print the two
# share URLs to paste into the browser.
#
#   ts-sdk/examples/compare/run.sh
#
# Stop everything with Ctrl-C.
#
# HONEST NOTE: on localhost the link is too fast to SEE the divergence
# (loopback is 100+ Gbps; the 30 MB/s screen burst clears in microseconds).
# To make the freeze visible you need real RTT / loss / a bandwidth cap.
# See README.md → "Why localhost won't show it" and examples/cua/README.md
# for tc-netem and WAN-VM instructions.

set -euo pipefail

# Repo root = four levels up from this script (ts-sdk/examples/compare/run.sh).
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TSSDK="$(cd "$HERE/../.." && pwd)"
ROOT="$(cd "$TSSDK/.." && pwd)"

HTTP_PORT="${HTTP_PORT:-5173}"
SERVER_FLAGS="${SERVER_FLAGS:--stress}"   # override e.g. SERVER_FLAGS="-screen-fps=60 -screen-kb=200 -datagram-metadata"

cleanup() {
  echo
  echo "stopping…"
  [[ -n "${NAIVE_PID:-}" ]] && kill "$NAIVE_PID" 2>/dev/null || true
  [[ -n "${MULTI_PID:-}" ]] && kill "$MULTI_PID" 2>/dev/null || true
  [[ -n "${HTTP_PID:-}"  ]] && kill "$HTTP_PID"  2>/dev/null || true
}
trap cleanup EXIT INT TERM

# 0. Make sure the SDK + the compare page are built.
if [[ ! -f "$TSSDK/dist/index.js" ]]; then
  echo "==> building SDK (ts-sdk/dist)"
  ( cd "$TSSDK" && npm run build )
fi
echo "==> building compare page (compare.js)"
( cd "$HERE" && npx tsc -p tsconfig.json )

# 1. Start the two servers, capturing their share lines.
NAIVE_LOG="$(mktemp)"; MULTI_LOG="$(mktemp)"
echo "==> starting naive server ($SERVER_FLAGS)"
( cd "$ROOT" && go run ./examples/cua/server -mode=naive $SERVER_FLAGS ) >"$NAIVE_LOG" 2>&1 &
NAIVE_PID=$!
echo "==> starting multistream server ($SERVER_FLAGS)"
( cd "$ROOT" && go run ./examples/cua/server -mode=multistream $SERVER_FLAGS ) >"$MULTI_LOG" 2>&1 &
MULTI_PID=$!

# 2. Wait for each to print its share URL.
wait_for_share() {
  local log="$1" name="$2" i=0
  while (( i < 200 )); do
    if grep -q '^share: ' "$log" 2>/dev/null; then
      grep -m1 '^share: ' "$log" | sed 's/^share: //'
      return 0
    fi
    sleep 0.2; (( i++ ))
  done
  echo "ERROR: $name server never printed a share URL — see $log" >&2
  cat "$log" >&2
  return 1
}

NAIVE_URL="$(wait_for_share "$NAIVE_LOG" naive)"
MULTI_URL="$(wait_for_share "$MULTI_LOG" multistream)"

# 3. Static file server for the page (serve the ts-sdk root so ../../dist resolves).
echo "==> starting static server on :$HTTP_PORT"
( cd "$TSSDK" && python3 -m http.server "$HTTP_PORT" ) >/dev/null 2>&1 &
HTTP_PID=$!
sleep 0.5

PAGE="http://localhost:$HTTP_PORT/examples/compare/"

cat <<EOF

────────────────────────────────────────────────────────────────────
  Open:   $PAGE

  Pane A (naive):        $NAIVE_URL
  Pane B (multistream):  $MULTI_URL

  Paste each URL into its field, click "connect & start driving".
  (Paste once; the page remembers them in localStorage.)

  NOTE: on localhost the lanes will look identical — the loopback link
  is too fast to congest. To SEE pane A freeze, add real RTT/loss/a
  bandwidth cap (tc netem or a WAN VM). See README.md.
────────────────────────────────────────────────────────────────────
EOF

# Try to open the browser (best effort).
( command -v open >/dev/null && open "$PAGE" ) 2>/dev/null \
  || ( command -v xdg-open >/dev/null && xdg-open "$PAGE" ) 2>/dev/null || true

echo "Ctrl-C to stop all three processes."
wait
