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

log "smoke passed"