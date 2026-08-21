#!/bin/sh
# Apply or clear network impairment on a running container's eth0 using netem.
# Requires NET_ADMIN (already granted to edge/connector/blast in compose).
#
#   scripts/impair.sh edge loss 3%
#   scripts/impair.sh edge delay 80ms 30ms
#   scripts/impair.sh edge rate 5mbit
#   scripts/impair.sh edge reorder 5% 50%
#   scripts/impair.sh edge clear
#
# Pick the target deliberately — each container sits on a different hop:
#   blast     -> client <-> edge only (the HTTP/3 hop end users see)
#   connector -> connector <-> edge only (the tunnel hop)
#   edge      -> BOTH hops, since they share eth0
#
# UDP buffer pressure is best produced by combining `rate` (to build a backlog)
# with a high -workers count in blast, then watching /proc/net/snmp -> Udp:
# RcvbufErrors and SndbufErrors climb.
set -eu

SERVICE="${1:?usage: impair.sh <service> <loss|delay|rate|reorder|clear> [args...]}"
MODE="${2:?missing mode}"
shift 2

run() { docker compose exec -T "$SERVICE" "$@"; }

case "$MODE" in
  clear)
    run tc qdisc del dev eth0 root 2>/dev/null || true
    echo "cleared impairment on $SERVICE"
    ;;
  loss)
    run tc qdisc replace dev eth0 root netem loss "${1:?e.g. 3%}"
    ;;
  delay)
    run tc qdisc replace dev eth0 root netem delay "${1:?e.g. 80ms}" "${2:-0ms}" distribution normal
    ;;
  reorder)
    run tc qdisc replace dev eth0 root netem delay 10ms reorder "${1:?e.g. 5%}" "${2:-50%}"
    ;;
  rate)
    run tc qdisc replace dev eth0 root netem rate "${1:?e.g. 5mbit}" limit 100
    ;;
  *)
    echo "unknown mode: $MODE" >&2
    exit 2
    ;;
esac

run tc qdisc show dev eth0
