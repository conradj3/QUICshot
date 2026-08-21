#!/usr/bin/env bash
# Deterministic end-to-end checks for the local Docker stack.
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE=${COMPOSE:-docker compose}
KEEP_STACK=${KEEP_STACK:-0}

log() { printf '\n==> %s\n' "$*"; }
fail() { printf '\nsmoke: %s\n' "$*" >&2; exit 1; }

cleanup() {
  if [ "$KEEP_STACK" != 1 ]; then
    $COMPOSE down -v >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

mkdir -p logs/qlog/smoke
rm -f logs/smoke-*.jsonl
rm -rf logs/qlog/smoke/*

wait_fast_path() {
  local out=""
  for _ in $(seq 1 30); do
    if out=$($COMPOSE run --rm -T blast blast \
        -url=https://edge:8443/fast \
        -certs=/certs -server-name=edge \
        -conns=1 -workers=1 -requests=1 -duration=10s -timeout=5s \
        -max-failure-pct=0 \
        -log=/logs/smoke-fast.jsonl 2>&1) && \
        printf '%s\n' "$out" | grep -q 'HTTP/3.0' && \
        printf '%s\n' "$out" | grep -q 'failures        0'; then
      printf '%s\n' "$out"
      return 0
    fi
    sleep 1
  done
  printf '%s\n' "$out"
  return 1
}

log "starting stack"
$COMPOSE up -d --build origin edge connector

log "waiting for HTTP/3 fast path"
fast_out=""
for _ in $(seq 1 30); do
  if fast_out=$(wait_fast_path); then
    break
  fi
done
printf '%s\n' "$fast_out"
printf '%s\n' "$fast_out" | grep -q 'HTTP/3.0' || fail "fast path did not negotiate HTTP/3"
printf '%s\n' "$fast_out" | grep -q 'failures        0' || fail "fast path reported failures"

log "checking qlog output"
$COMPOSE run --rm -T blast blast \
  -url=https://edge:8443/fast \
  -certs=/certs -server-name=edge \
  -conns=1 -workers=1 -requests=1 -duration=10s -timeout=5s \
  -qlog-dir=/logs/qlog/smoke \
  -log=/logs/smoke-qlog.jsonl >/tmp/quicshot-smoke-qlog.out 2>&1
find logs/qlog/smoke -type f -name '*.sqlog' -print -quit | grep -q . || fail "qlog run did not produce a .sqlog file"

log "checking 524 classification"
EDGE_ORIGIN_TIMEOUT=1s $COMPOSE up -d --force-recreate edge connector
wait_fast_path >/dev/null || fail "fast path did not recover after edge restart"
$COMPOSE run --rm -T blast blast \
  -url='https://edge:8443/slow?ms=3000' \
  -certs=/certs -server-name=edge \
  -conns=1 -workers=1 -requests=1 -duration=10s -timeout=5s \
  -log=/logs/smoke-524.jsonl >/tmp/quicshot-smoke-524.out 2>&1 || true
grep -q '"status":524' logs/smoke-524.jsonl || fail "slow origin did not produce status 524"
grep -q 'cf_error_code=524' logs/smoke-524.jsonl || fail "slow origin did not record cf_error_code=524"

log "checking connector-down classification"
$COMPOSE stop connector >/dev/null
$COMPOSE run --rm -T blast blast \
  -url=https://edge:8443/fast \
  -certs=/certs -server-name=edge \
  -conns=1 -workers=1 -requests=1 -duration=10s -timeout=5s \
  -log=/logs/smoke-1033.jsonl >/tmp/quicshot-smoke-1033.out 2>&1 || true
grep -q '"status":502' logs/smoke-1033.jsonl || fail "connector-down path did not produce status 502"
grep -q 'cf_error_code=1033' logs/smoke-1033.jsonl || fail "connector-down path did not record cf_error_code=1033"

log "restoring connector and edge timeouts"
$COMPOSE start connector >/dev/null
EDGE_ORIGIN_TIMEOUT=10s $COMPOSE up -d --force-recreate edge connector
wait_fast_path >/dev/null || fail "fast path did not recover after connector restore"

log "checking origin RST classification (1014)"
$COMPOSE run --rm -T blast blast \
  -url=https://edge:8443/reset \
  -certs=/certs -server-name=edge \
  -conns=1 -workers=1 -requests=1 -duration=10s -timeout=5s \
  -log=/logs/smoke-1014.jsonl >/tmp/quicshot-smoke-1014.out 2>&1 || true
grep -q '"status":502' logs/smoke-1014.jsonl || fail "origin RST did not produce status 502"
grep -q 'cf_error_code=1014' logs/smoke-1014.jsonl || fail "origin RST did not record cf_error_code=1014"

log "checking invisible client-deadline (no 524)"
rm -f logs/smoke-invisible.jsonl
$COMPOSE run --rm -T blast blast \
  -url='https://edge:8443/slow?ms=5000' \
  -certs=/certs -server-name=edge \
  -conns=1 -workers=1 -requests=1 -duration=8s -timeout=1s \
  -log=/logs/smoke-invisible.jsonl >/tmp/quicshot-smoke-invisible.out 2>&1 || true
grep -q 'client_deadline' logs/smoke-invisible.jsonl || fail "short client timeout did not record client_deadline"
if grep -q '"status":524' logs/smoke-invisible.jsonl; then
  fail "invisible-failure path logged a 524; the client should have given up first"
fi

log "checking idle timeout vs keepalive"
$COMPOSE run --rm -T blast blast \
  -url='https://edge:8443/slow?ms=5000' \
  -certs=/certs -server-name=edge \
  -conns=1 -workers=1 -duration=8s -timeout=8s \
  -max-idle-timeout=2s -keepalive=0s \
  -log=/logs/smoke-idle.jsonl >/tmp/quicshot-smoke-idle.out 2>&1 || true
grep -q 'quic_idle_timeout' logs/smoke-idle.jsonl || fail "idle path did not record quic_idle_timeout"

$COMPOSE run --rm -T blast blast \
  -url='https://edge:8443/slow?ms=5000' \
  -certs=/certs -server-name=edge \
  -conns=1 -workers=1 -duration=8s -timeout=8s \
  -max-idle-timeout=2s -keepalive=1s \
  -log=/logs/smoke-keepalive.jsonl >/tmp/quicshot-smoke-keepalive.out 2>&1 || true
if grep -q 'quic_idle_timeout' logs/smoke-keepalive.jsonl; then
  fail "keepalive path still recorded quic_idle_timeout"
fi
grep -q 'conn drops[[:space:]]\+0' /tmp/quicshot-smoke-keepalive.out || fail "keepalive path did not report zero conn drops"

log "checking Alt-Svc on the TCP listener"
alt=$($COMPOSE run --rm --entrypoint curl blast -skI --http1.1 https://edge:8443/fast || true)
printf '%s\n' "$alt"
printf '%s\n' "$alt" | grep -qi 'alt-svc' || fail "TCP listener did not advertise Alt-Svc"

log "checking POST /echo through the tunnel"
$COMPOSE run --rm -T blast blast \
  -url=https://edge:8443/echo \
  -certs=/certs -server-name=edge \
  -method=POST -body=quicshot-echo \
  -conns=1 -workers=1 -requests=1 -duration=10s -timeout=5s \
  -max-failure-pct=0 \
  -log=/logs/smoke-echo.jsonl >/tmp/quicshot-smoke-echo.out 2>&1
grep -q 'failures[[:space:]]\+0' /tmp/quicshot-smoke-echo.out || fail "POST /echo reported failures"

if curl --help 2>&1 | grep -q -- '--http3-only'; then
  log "checking host curl --http3-only"
  ./scripts/h3-clients.sh curl https://localhost:8443/fast --insecure | tee /tmp/quicshot-smoke-curl.out
  grep -qi 'HTTP/3' /tmp/quicshot-smoke-curl.out || fail "host curl --http3-only did not negotiate HTTP/3"
else
  log "skipping host curl HTTP/3 (this curl has no --http3-only)"
fi

log "smoke passed"