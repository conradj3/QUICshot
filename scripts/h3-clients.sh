#!/usr/bin/env bash
# First-class non-quic-go HTTP/3 clients against the same URL blast uses.
#
#   ./scripts/h3-clients.sh curl  https://edge:8443/fast [--insecure]
#   ./scripts/h3-clients.sh chrome https://localhost:8443/fast [--insecure]
#
# curl: host curl if it supports --http3-only, otherwise a helper image on the
# compose network. ymuski/curl-http3 is amd64-only; on Apple Silicon we retry
# with --platform linux/amd64. Chrome/Chromium: skipped unless a binary is on PATH.
set -euo pipefail
cd "$(dirname "$0")/.."

TOOL="${1:?usage: h3-clients.sh <curl|chrome> <url> [--insecure]}"
URL="${2:?url required}"
shift 2
INSECURE=0
while [ $# -gt 0 ]; do
  case "$1" in
    --insecure|-k) INSECURE=1 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
  shift
done

CURL_H3_IMAGE=${CURL_H3_IMAGE:-ymuski/curl-http3:latest}

host_curl_has_h3() {
  command -v curl >/dev/null 2>&1 && curl --help 2>&1 | grep -q -- '--http3-only'
}

compose_network() {
  if [ -n "${COMPOSE_NETWORK:-}" ]; then
    printf '%s\n' "$COMPOSE_NETWORK"
    return
  fi
  if docker network inspect quicshot_default >/dev/null 2>&1; then
    printf 'quicshot_default\n'
    return
  fi
  printf '\n'
}

run_host_curl() {
  local args=(--http3-only --max-time 15 -sS -D- -o /tmp/quicshot-h3-body)
  if [ "$INSECURE" = 1 ]; then
    args+=(-k)
  fi
  echo "$ curl ${args[*]} $URL"
  curl "${args[@]}" "$URL"
  echo
  echo "body (truncated):"
  head -c 256 /tmp/quicshot-h3-body 2>/dev/null || true
  echo
}

# docker_run_curl NETWORK [PLATFORM]
docker_run_curl() {
  local net="$1"
  local platform="${2:-}"
  local args=(--rm --network "$net")
  if [ -n "$platform" ]; then
    args=(--rm --platform "$platform" --network "$net")
  fi
  local curl_args=(--http3-only --max-time 15 -sS -D- -o -)
  if [ "$INSECURE" = 1 ]; then
    curl_args+=(-k)
  fi
  echo "$ docker run ${args[*]} $CURL_H3_IMAGE ${curl_args[*]} $URL"
  docker run "${args[@]}" "$CURL_H3_IMAGE" "${curl_args[@]}" "$URL"
}

run_docker_curl() {
  local net
  net=$(compose_network)
  if [ -z "$net" ]; then
    echo "no compose network found; start the stack with Up first" >&2
    exit 1
  fi
  echo "host curl has no --http3-only; using $CURL_H3_IMAGE on $net"
  if docker_run_curl "$net"; then
    return 0
  fi
  echo "native platform has no $CURL_H3_IMAGE image; retrying linux/amd64"
  docker_run_curl "$net" linux/amd64
}

find_chrome() {
  if [ -n "${CHROME:-}" ]; then
    printf '%s\n' "$CHROME"
    return
  fi
  command -v google-chrome 2>/dev/null || \
  command -v google-chrome-stable 2>/dev/null || \
  command -v chromium 2>/dev/null || \
  command -v chromium-browser 2>/dev/null || \
  command -v "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" 2>/dev/null || \
  true
}

run_chrome() {
  local bin
  bin=$(find_chrome)
  if [ -z "$bin" ]; then
    echo "Chrome/Chromium not installed; skipping. Install Chrome or set CHROME=/path/to/chrome."
    exit 0
  fi
  local host port
  host=$(python3 - <<PY
from urllib.parse import urlparse
u=urlparse("$URL")
print(u.hostname or "")
PY
)
  port=$(python3 - <<PY
from urllib.parse import urlparse
u=urlparse("$URL")
print(u.port or (443 if u.scheme=="https" else 80))
PY
)
  local args=(--headless=new --disable-gpu --no-first-run --no-default-browser-check
    --enable-quic --origin-to-force-quic-on="${host}:${port}" --dump-dom)
  if [ "$INSECURE" = 1 ]; then
    args+=(--ignore-certificate-errors)
  fi
  echo "$ $bin ${args[*]} $URL"
  "$bin" "${args[@]}" "$URL" | head -c 4096
  echo
}

case "$TOOL" in
  curl)
    if host_curl_has_h3; then
      run_host_curl
    else
      run_docker_curl
    fi
    ;;
  chrome)
    run_chrome
    ;;
  *)
    echo "tool must be curl or chrome" >&2
    exit 2
    ;;
esac
