#!/usr/bin/env bash
# Attempt to move kernel Udp: RcvbufErrors. This is Linux-only and still not a
# calibrated experiment: the receiving process has to stall with a tiny
# SO_RCVBUF while datagrams keep arriving.
#
# Docker Desktop (macOS/Windows): skip. /proc/net/snmp belongs to the VM, and
# SIGSTOP inside a container does not reproduce AKS node-buffer starvation.
#
# The counter that *does* move in this lab is SndbufErrors (make scenario-buffer).
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE=${COMPOSE:-docker compose}

if [ "$(uname -s)" != "Linux" ]; then
  echo "rcvbuf-pressure: native Linux only."
  echo "On Docker Desktop the UDP stack lives in the VM; a zero RcvbufErrors"
  echo "counter here is inconclusive, not a pass."
  echo "Use: make scenario-buffer   # SndbufErrors on a rate-capped path"
  exit 2
fi

echo "==> starting stack with a tiny edge receive buffer"
EDGE_UDP_RECV_BUFFER=212992 $COMPOSE up -d --build origin edge connector >/dev/null
sleep 2

before=$($COMPOSE exec -T edge sh -c 'grep -A1 "^Udp:" /proc/net/snmp' | tail -1)
echo "udp before: $before"

echo "==> blasting while briefly stopping the edge process (receiver stall)"
$COMPOSE run --rm -T blast blast \
  -url='https://edge:8443/bytes?n=1048576' \
  -certs=/certs -server-name=edge \
  -conns=8 -workers=16 -duration=12s -timeout=5s \
  -log=/logs/rcvbuf-pressure.jsonl >/tmp/quicshot-rcvbuf-blast.out 2>&1 &
blast_pid=$!

for _ in $(seq 1 8); do
  $COMPOSE exec -T edge sh -c 'kill -STOP 1 2>/dev/null || true; sleep 0.2; kill -CONT 1 2>/dev/null || true' || true
  sleep 0.5
done
wait "$blast_pid" || true

after=$($COMPOSE exec -T edge sh -c 'grep -A1 "^Udp:" /proc/net/snmp' | tail -1)
echo "udp after:  $after"
echo
echo "Compare RcvbufErrors in the two Udp: lines. If it did not move, that is"
echo "the documented gap: this setup still cannot reliably stall the receiver."
echo "SndbufErrors from make scenario-buffer is the evidence you can take to AKS."
