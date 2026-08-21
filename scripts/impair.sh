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
# Multi-queue veths get netem on every TX queue under an mq root. A single
# root netem is applied unevenly (refcnt > 1) and is not a calibrated shaper.
set -eu

SERVICE="${1:?usage: impair.sh <service> <loss|delay|rate|reorder|clear> [args...]}"
MODE="${2:?missing mode}"
shift 2

run() { docker compose exec -T "$SERVICE" "$@"; }

netem_args() {
  case "$MODE" in
    loss) printf 'loss %s' "${1:?e.g. 3%}" ;;
    delay) printf 'delay %s %s distribution normal' "${1:?e.g. 80ms}" "${2:-0ms}" ;;
    reorder) printf 'delay 10ms reorder %s %s' "${1:?e.g. 5%}" "${2:-50%}" ;;
    rate) printf 'rate %s limit 100' "${1:?e.g. 5mbit}" ;;
    *) echo "unknown mode: $MODE" >&2; exit 2 ;;
  esac
}

tx_queues() {
  run sh -c 'ls -d /sys/class/net/eth0/queues/tx-* 2>/dev/null | wc -l' | tr -d ' \r'
}

apply_mq() {
  args=$(netem_args "$@")
  n=$(tx_queues)
  if [ -z "$n" ] || [ "$n" -lt 2 ]; then
    # shellcheck disable=SC2086
    run tc qdisc replace dev eth0 root netem $args
    return
  fi
  run tc qdisc replace dev eth0 root handle 1: mq
  i=1
  while [ "$i" -le "$n" ]; do
    id=$((i + 10))
    hex=$(printf '%x' "$i")
    # shellcheck disable=SC2086
    run tc qdisc replace dev eth0 parent "1:$hex" handle "$id:" netem $args
    i=$((i + 1))
  done
}

case "$MODE" in
  clear)
    run tc qdisc del dev eth0 root 2>/dev/null || true
    echo "cleared impairment on $SERVICE"
    ;;
  loss|delay|reorder|rate)
    apply_mq "$@"
    ;;
  *)
    echo "unknown mode: $MODE" >&2
    exit 2
    ;;
esac

run tc qdisc show dev eth0
