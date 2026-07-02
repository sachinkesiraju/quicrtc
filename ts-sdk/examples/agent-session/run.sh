#!/usr/bin/env bash
# run.sh — launch the "Agent Session" demo: two cua servers (naive +
# multistream) under a heavy desktop stream, a static file server for the
# page, and print the two share URLs to paste into the browser.
#
#   ts-sdk/examples/agent-session/run.sh
#
# Stop everything with Ctrl-C.
#
# The heavy desktop load (30 Hz x ~3 MB frames) is what makes the
# contention visible WITHOUT a WAN link: on the naive side the control
# pings queue behind desktop bytes on the single stream, so the take-
# control latency climbs and the desktop freezes; the quicrtc side keeps
# every lane on its own stream/datagram and stays instant. On a real
# network the same divergence shows at far lower load.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TSSDK="$(cd "$HERE/../.." && pwd)"
ROOT="$(cd "$TSSDK/.." && pwd)"

HTTP_PORT="${HTTP_PORT:-5173}"
# Heavy desktop stream to force contention on localhost. Override via env.
SERVER_FLAGS="${SERVER_FLAGS:--screen-fps=30 -screen-kb=3000 -datagram-metadata -action-ms=1 -a11y-kb=1 -dom-kb=1 -perf-kb=1}"

cleanup() {
  echo
  echo "stopping…"
  [[ -n "${NAIVE_PID:-}" ]] && kill "$NAIVE_PID" 2>/dev/null || true
  [[ -n "${MULTI_PID:-}" ]] && kill "$MULTI_PID" 2>/dev/null || true
  [[ -n "${HTTP_PID:-}"  ]] && kill "$HTTP_PID"  2>/dev/null || true
}
trap cleanup EXIT INT TERM

# 0. Build the SDK + this page if needed.
if [[ ! -f "$TSSDK/dist/index.js" ]]; then
  echo "==> building SDK (ts-sdk/dist)"
  ( cd "$TSSDK" && npm run build )
fi
echo "==> building agent-session page (agent-session.js)"
( cd "$HERE" && npx tsc -p tsconfig.json )

# 1. Start the two servers.
NAIVE_LOG="$(mktemp)"; MULTI_LOG="$(mktemp)"
echo "==> starting standard (naive) server"
( cd "$ROOT" && go run ./examples/cua/server -mode=naive $SERVER_FLAGS ) >"$NAIVE_LOG" 2>&1 &
NAIVE_PID=$!
echo "==> starting quicrtc (multistream) server"
( cd "$ROOT" && go run ./examples/cua/server -mode=multistream $SERVER_FLAGS ) >"$MULTI_LOG" 2>&1 &
MULTI_PID=$!

# 2. Wait for each share URL.
wait_for_share() {
  local log="$1" name="$2" i=0
  while (( i < 300 )); do
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

# 3. Static file server (serve ts-sdk/ so ../../dist resolves).
echo "==> starting static server on :$HTTP_PORT"
( cd "$TSSDK" && python3 -m http.server "$HTTP_PORT" ) >/dev/null 2>&1 &
HTTP_PID=$!
sleep 0.5

PAGE="http://localhost:$HTTP_PORT/examples/agent-session/"
cat <<EOF

────────────────────────────────────────────────────────────────────
  Open:   $PAGE

  Left  (Standard / naive):  $NAIVE_URL
  Right (quicrtc / multi):   $MULTI_URL

  Paste each URL into its field, click "connect & start", then click
  either desktop to "take control" and watch the latency badge.
────────────────────────────────────────────────────────────────────
EOF

( command -v open >/dev/null && open "$PAGE" ) 2>/dev/null \
  || ( command -v xdg-open >/dev/null && xdg-open "$PAGE" ) 2>/dev/null || true

echo "Ctrl-C to stop all three processes."
wait
