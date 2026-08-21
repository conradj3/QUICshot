#!/bin/sh
# net.core.rmem_max is NOT network-namespaced, so Docker refuses to set it per
# container. To reproduce the "failed to sufficiently increase receive buffer
# size" condition you must change it on the host kernel — on macOS/Windows that
# means the Docker Desktop Linux VM.
#
#   scripts/host-rmem.sh show
#   scripts/host-rmem.sh set 212992     # small: reproduces the AKS default
#   scripts/host-rmem.sh set 7500000    # what quic-go wants
#
# On an AKS node pool the equivalent fix is a DaemonSet or a Node Configuration
# (linuxOSConfig.sysctls) that sets net.core.rmem_max / net.core.wmem_max.
set -eu

ACTION="${1:?usage: host-rmem.sh <show|set> [bytes]}"
NSENTER="docker run --rm --privileged --pid=host alpine:3.22 nsenter -t 1 -m -u -n -i"

case "$ACTION" in
  show)
    $NSENTER sysctl net.core.rmem_max net.core.wmem_max net.core.rmem_default
    ;;
  set)
    BYTES="${2:?bytes required}"
    $NSENTER sysctl -w net.core.rmem_max="$BYTES" -w net.core.wmem_max="$BYTES"
    ;;
  *)
    echo "unknown action: $ACTION" >&2
    exit 2
    ;;
esac
