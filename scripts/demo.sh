#!/usr/bin/env bash
# One-command guided demo. Each act proves one thing and pauses so you can talk.
#
#   ./scripts/demo.sh            # interactive, pauses between acts
#   ./scripts/demo.sh --auto     # no pauses, for recording
#   ./scripts/demo.sh --fast     # short durations, for rehearsal
#   ./scripts/demo.sh --act 4    # jump straight to one act
set -euo pipefail
cd "$(dirname "$0")/.."

# Everything lives in main() so bash parses the whole file before running any of
# it. Without this, editing the script while a demo is in progress corrupts the
# running shell (bash reads scripts incrementally, by byte offset).
main() {

AUTO=0; FAST=0; ONLY_ACT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --auto) AUTO=1 ;;
    --fast) FAST=1 ;;
    --act)  ONLY_ACT="${2:?act number}"; shift ;;
    -h|--help) sed -n '2,9p' "$0"; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
  shift
done

if [ "$FAST" = 1 ]; then D_BASE=8s; D_524=25s; D_IDLE=15s; D_BUF=20s
else                     D_BASE=20s; D_524=45s; D_IDLE=25s; D_BUF=30s; fi

BOLD=$'\033[1m'; DIM=$'\033[2m'; CYAN=$'\033[36m'; YELLOW=$'\033[33m'; GREEN=$'\033[32m'; OFF=$'\033[0m'

banner() { printf '\n%s\n%s  ACT %s — %s%s\n%s\n' "${CYAN}════════════════════════════════════════════════════════════${OFF}" \
                  "$BOLD" "$1" "$2" "$OFF" "${CYAN}════════════════════════════════════════════════════════════${OFF}"; }
say()    { printf '%s▸ %s%s\n' "$DIM" "$1" "$OFF"; }
point()  { printf '\n%s★ %s%s\n' "$YELLOW" "$1" "$OFF"; }
run()    { printf '\n%s$ %s%s\n' "$GREEN" "$*" "$OFF"; "$@"; }
pause()  { [ "$AUTO" = 1 ] && { sleep 2; return; }; printf '\n%s[enter to continue]%s ' "$DIM" "$OFF"; read -r _; }

# Every blast run streams its JSON events to a file rather than the terminal, so
# the demo output stays readable. $LAST_LOG points at the most recent one.
STEP=0; LAST_LOG=""
blast() {
  STEP=$((STEP + 1))
  LAST_LOG="logs/demo-$STEP.jsonl"
  : > "$LAST_LOG"
  docker compose run --rm -T blast blast "$@" -certs=/certs -server-name=edge "-log=/logs/demo-$STEP.jsonl" 2>&1 |
    grep -v -e '^ *Container ' -e '^\[+\]' || true
}
edgelog() { docker compose logs edge --since "${1:-30s}" --no-log-prefix 2>/dev/null | grep -v '"msg":"udp counters"' | tail -"${2:-4}"; }

# A heavy run can knock the tunnel over briefly; without this gate the next act
# races the connector's reconnect and reports 502/1033 instead of its own point.
wait_for_tunnel() {
  local i code
  for i in $(seq 1 40); do
    code=$(docker compose exec -T edge curl -sk -o /dev/null -w '%{http_code}' --max-time 3 \
             https://localhost:8443/fast 2>/dev/null || true)
    [ "$code" = "200" ] && return 0
    sleep 1
  done
  printf '%s! tunnel did not come back \u2014 continuing anyway%s\n' "$YELLOW" "$OFF"
  return 0
}

want_act() { [ -z "$ONLY_ACT" ] || [ "$ONLY_ACT" = "$1" ]; }

# ---------------------------------------------------------------------- setup
docker info >/dev/null 2>&1 || { echo "docker is not running" >&2; exit 1; }
mkdir -p logs

if [ -z "$ONLY_ACT" ]; then
  banner 0 "Bring up the tunnel"
  say "origin  = your app (plain HTTP/1.1)"
  say "edge    = the Cloudflare edge (terminates HTTP/3, owns the 524)"
  say "connector = cloudflared (dials OUT to the edge over QUIC)"
  run docker compose up -d --build origin edge connector
  sleep 4
  run docker compose logs edge connector --no-log-prefix --tail=6
  point "The connector dialled OUTBOUND. No inbound firewall rule anywhere — that is the whole point of a tunnel."
  pause
fi

# ---------------------------------------------------------------------- act 1
if want_act 1; then
  banner 1 "Baseline — the tunnel is not the problem"
  wait_for_tunnel
  blast -url=https://edge:8443/fast -conns=4 -workers=16 -duration=$D_BASE -stats-every=10s
  point "Five figures of requests per second over HTTP/3, zero failures, zero connection drops. Whatever broke on AKS, it was not raw throughput."
  pause
fi

# ---------------------------------------------------------------------- act 2
if want_act 2; then
  banner 2 "Manufacture a 524"
  wait_for_tunnel
  say "Origin sleeps 20s. The edge only waits 10s (-origin-timeout)."
  blast -url='https://edge:8443/slow?ms=20000' -conns=1 -workers=2 -duration=$D_524 -timeout=40s -stats-every=15s
  say "what the edge logged:"
  edgelog 60s 3
  point "elapsed_ms lands on the EDGE's 10s timeout, nowhere near the origin's 20s sleep. A 524 is a stopwatch on the edge — nothing to do with QUIC or the network."
  pause

  banner 2b "…and the same failure with NO 524 at all"
  say "Same slow origin, but now the CLIENT is less patient than the edge (-timeout=3s)."
  say "(letting in-flight work from act 2 drain first, so the log window is clean)"
  sleep 6
  MARK=$(date +%s)
  blast -url='https://edge:8443/slow?ms=20000' -conns=1 -workers=2 -duration=10s -timeout=3s -stats-every=10s || true
  say "what the edge logged in that window:"
  if edgelog "$(( $(date +%s) - MARK ))s" 5 | grep -q 'origin failure'; then
    edgelog "$(( $(date +%s) - MARK ))s" 5
  else
    printf '  %s(nothing — no 524, no error of any kind)%s\n' "$DIM" "$OFF"
  fi
  point "Client reports client_deadline. The edge reported NOTHING. If your dashboards only watch Cloudflare, this failure mode is invisible."
  pause
fi

# ---------------------------------------------------------------------- act 3
if want_act 3; then
  banner 3 "Kill the connector — 1033 vs 524"
  run docker compose stop connector
  sleep 1
  blast -url=https://edge:8443/fast -conns=1 -workers=1 -requests=5 -duration=10s -stats-every=30s || true
  edgelog 20s 3
  run docker compose start connector
  sleep 3
  wait_for_tunnel
  point "502 / error code 1033, not 524. The status code tells you WHICH HOP died."
  pause
fi

# ---------------------------------------------------------------------- act 4
if want_act 4; then
  banner 4 "Client-side disconnects — the one you came for"
  wait_for_tunnel
  say "No keep-alives, 3s idle timeout, origin takes 9s. The connection dies while waiting."
  blast -url='https://edge:8443/slow?ms=9000' -conns=1 -workers=1 -duration=$D_IDLE \
        -max-idle-timeout=3s -keepalive=0s -stats-every=15s || true
  say "the receipts:"
  if command -v jq >/dev/null 2>&1; then
    jq -c 'select(.event=="conn_closed" and .kind=="quic_idle_timeout") | {kind, detail}' "$LAST_LOG" | tail -3
  else
    grep quic_idle_timeout "$LAST_LOG" | tail -3
  fi
  point "packets_lost=0 — the connection did not break from packet loss. It broke because NOTHING WAS SENT."
  pause

  banner 4b "Same test, keep-alives on"
  blast -url='https://edge:8443/slow?ms=9000' -conns=1 -workers=1 -duration=$D_IDLE \
        -max-idle-timeout=3s -keepalive=1s -stats-every=15s || true
  point "conn drops: 0. That one flag is your AKS fix."
  pause
fi

# ---------------------------------------------------------------------- act 5
if want_act 5; then
  banner 5 "UDP buffer pressure — real kernel counters"
  wait_for_tunnel
  run ./scripts/impair.sh edge rate 5mbit
  say "256KB responses over a 5mbit link — small enough to complete, slow enough to queue."
  blast -url='https://edge:8443/bytes?n=262144' -conns=2 -workers=4 -duration=$D_BUF -stats-every=10s || true
  say "kernel UDP counters inside the edge container:"
  docker compose exec -T edge sh -c 'grep -A1 "^Udp:" /proc/net/snmp' | head -2
  run ./scripts/impair.sh edge clear
  point "SndbufErrors is a real kernel counter, not a log line — and look at the latency TAIL, not the median. This is the number to pull off your AKS nodes."
  say "NOTE: netem shaping on a multi-queue veth is not applied evenly, so the request"
  say "count and median here vary a lot between runs. The counter and the tail are the signal."
  pause

  banner 5b "The OTHER buffer problem — the one that actually bit you"
  say "net.core.rmem_max is NOT network-namespaced, so it is a property of the HOST kernel."
  run ./scripts/host-rmem.sh show || true
  point "Whatever that says is what EVERY pod on the node gets. A pod securityContext.sysctls entry for net.core.* silently does nothing — it has to be the node pool's linuxOSConfig.sysctls, or a privileged DaemonSet."
  say "change it with: ./scripts/host-rmem.sh set 7500000   (affects the whole Docker VM)"
fi

printf '\n%s%s✔ demo complete%s  — stack is still up. "make down" to stop it.\n\n' "$BOLD" "$GREEN" "$OFF"

}

main "$@"
