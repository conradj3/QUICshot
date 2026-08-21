# QUICshot

Aim it at a tunnel and see what breaks. QUICshot is a local, Docker-only
reproduction of the HTTP/3 + QUIC path you get with a Cloudflare Tunnel in front
of a workload — built so you can trigger **524s** and **UDP receive-buffer
starvation** on demand, and blast the front door with HTTP/3 while logging every
client-side disconnect.

It uses the same QUIC stack as the real thing (`quic-go`, which is what
`cloudflared` is built on), so the failure signatures match what you saw on the
AKS node pool.

```
 blast (HTTP/3 client)
   │  udp/8443   h3
   ▼
 edge          ← plays the Cloudflare edge: terminates h3, owns the origin
   │             timeout, synthesises "error code: 524"
   │  udp/7844   QUIC tunnel (connector dials OUT, exactly like cloudflared)
   ▼
 connector     ← plays cloudflared: accepts request streams, proxies to origin
   │  tcp/8080   plain HTTP/1.1
   ▼
 origin        ← your app: /fast /slow /hang /drip /bytes /reset /flaky /echo /headers /stall
```

## Quick start

```sh
make demo        # guided end-to-end demo, one command, pauses between acts
```

That builds the images, brings the stack up, and walks through baseline load →
524 → connector loss → client disconnects → UDP buffer pressure, printing the
point of each act as it goes. `make demo-auto` removes the pauses (for
recording), `make demo-fast` shortens each run (for rehearsal), and
`./scripts/demo.sh --act 4` jumps straight to one act.

To drive it yourself instead:

```sh
make up          # origin + edge + connector
make blast       # baseline h3 load, JSONL to logs/blast.jsonl
make scenario-524
make logs
make down
```

Manual check from the host (curl needs HTTP/3 support — `brew install curl` or use `docker compose run`):

```sh
curl -k --http3-only https://localhost:8443/fast
curl -k --http3-only https://localhost:8443/slow?ms=20000   # -> error code: 524
```

## For contributors and forks

Prerequisites:

- Docker with Compose v2.
- Go 1.25+ for host-side builds and tests.
- Optional: `jq`, `tcpdump`, and a curl build with HTTP/3 support for manual debugging.

After cloning, this is the fastest confidence check:

```sh
make test        # gofmt check + go test + go vet
make smoke       # builds the Docker stack and checks h3, qlog, 524, and 1033
```

`make smoke` is intentionally short and deterministic. It starts the stack,
proves `/fast` negotiates `HTTP/3.0`, verifies qlog files are written, then
checks 524, 1033, origin RST (1014), invisible client-deadline, idle timeout vs
keepalive, Alt-Svc on the TCP listener, and POST `/echo`. Host `curl --http3-only`
is used when that curl exists. Set `KEEP_STACK=1 make smoke` if you want to
inspect the containers afterward.

For local development:

```sh
make up
make ui          # http://127.0.0.1:8088
make down
```

## Testing a real Cloudflare tunnel

Always `probe` before you `blast`. The probe is safe — one request per step — and
it tells you whether a load test would even mean anything.

```sh
make probe URL=https://app.example.com/
```

```
[  ok  ] DNS                        162.159.142.193, 172.66.2.189
[  ok  ] TCP + TLS                  alpn=h2 issuer="YE2" expires=2026-11-13
[  ok  ] HTTP over TCP              HTTP/2.0 302 Found  via=Cloudflare (cf-ray present)
[ warn ] Alt-Svc (h3 advertised?)   no Alt-Svc header: server is not advertising HTTP/3
[ FAIL ] HTTP/3 (QUIC over UDP/443) client_deadline (after 8.001s)
[ FAIL ] control HTTP/3 (cloudflare-quic.com)  client_deadline (after 8.001s)

VERDICT: UDP/443 is being blocked on your side ...
```

The **control endpoint** is what makes this useful: without it, a failed QUIC
handshake is ambiguous between "this server has no HTTP/3" and "my network eats
UDP". Probing a known-good public h3 endpoint in the same run separates them.

Two signals to read carefully:

- **No `Alt-Svc` header.** Cloudflare sends `alt-svc: h3=":443"; ma=86400` when
  HTTP/3 is enabled for the zone. Without it, browsers never even *try* HTTP/3 —
  they stay on HTTP/2 forever. Check Speed → Optimization → HTTP/3 (with QUIC).
- **Control also failed.** Then the target's result is unusable. Corporate
  networks and VPNs very commonly drop UDP/443. Retest from somewhere else.

### Running from somewhere that passes QUIC

If your workstation network blocks UDP/443, run it from a host that doesn't —
ideally one on the same network as the workload, which also tells you whether
egress QUIC works from *there*:

```sh
make build-linux                                      # dist/quicshot-linux-{amd64,arm64}
scp dist/quicshot-linux-amd64 somevm:/tmp/quicshot
ssh somevm /tmp/quicshot probe -url=https://app.example.com/
```

From inside Kubernetes, which is the most representative place to test a tunnel:

```sh
kubectl run quicshot --rm -it --restart=Never --image=<your-registry>/quicshot:latest \
  -- probe -url=https://app.example.com/
```

### Then blast it

Only once the probe says HTTP/3 works end to end:

```sh
make blast-remote URL=https://app.example.com/ RPS=20 DURATION=5m MAX_FAIL=0.5
```

`blast-remote` defaults to a **rate limit** (`RPS`) and a failure threshold that
exits non-zero, so it is safe to run in CI as a synthetic check. Get
authorisation before pointing sustained load at someone else's production.

For authenticated endpoints:

```sh
quicshot blast -url=https://app.example.com/api/health \
  -header 'Authorization: Bearer ...' -rps=10 -duration=2m -max-failure-pct=1
```

Watch for this in the summary — it is the whole point of the exercise:

```
negotiated protocol
  HTTP/3.0   482
  HTTP/2.0    18  <-- NOT HTTP/3
```

## Reproducing 524

Cloudflare returns 524 when it **connected to the origin but got no response** in
time (100s in production). The edge here does the same thing with a much shorter
`-origin-timeout` (default `10s`) so you get answers in seconds.

| Scenario | Command | Expected |
| --- | --- | --- |
| Slow origin | `make scenario-524` | `524`, `cf-error-code: 524` |
| Wedged origin (never responds) | `make scenario-hang` | `524` + connector logs the request still in flight |
| Origin RST | `make scenario-reset` | `502`, `error code: 1014` |
| Connector down | `docker compose stop connector` then curl | `502`, `error code: 1033` |
| Invisible failure | `make scenario-invisible` | client `client_deadline`, **no** 524 in blast JSONL |
| Idle disconnect | `make scenario-idle` | `quic_idle_timeout` with `packets_lost=0` |
| Keepalive fix | `make scenario-keepalive` | zero connection drops |

Change the deadline to bracket the real behaviour:

```sh
EDGE_ORIGIN_TIMEOUT=3s docker compose up -d --force-recreate edge
```

The three timeouts that decide who reports the failure are deliberately separate:

- `edge -origin-timeout` — the 524 line.
- `connector -origin-timeout` — cloudflared's own origin timeout (`100s` default,
  matching `--proxy-connection-timeout` semantics).
- `blast -timeout` — the client's patience.

Set them so `client > edge > origin latency` and you see a clean 524. Set
`client < edge` instead and you reproduce the other confusing case: the client
gives up first and you see `client_deadline` / stream resets, and *no* 524 in the
edge logs at all.

## Reproducing UDP buffer blocking

Two distinct problems get called "UDP buffer" issues. Reproduce them separately.

### 1. The host sysctl that governs every pod

`net.core.rmem_max` is **not** network-namespaced, so Docker cannot set it per
container — it is a property of the host kernel (the Docker Desktop VM on macOS).
This is exactly why it bites on AKS: the node's sysctl governs every pod.

```sh
./scripts/host-rmem.sh show
./scripts/host-rmem.sh set 212992      # small — the classic default
./scripts/host-rmem.sh set 7500000     # what quic-go wants
```

> Older quic-go versions logged `failed to sufficiently increase receive buffer
> size` on startup when this value was low. **v0.61 does not emit that warning**
> — verified here against a VM with `rmem_max=212992`. Do not build a demo around
> seeing it. The durable evidence is the sysctl value itself plus the kernel
> counters below.

> On AKS the fix is the node pool's `linuxOSConfig.sysctls`
> (`net.core.rmem_max` / `net.core.wmem_max`), or a privileged DaemonSet. A pod
> `securityContext.sysctls` entry will **not** work for `net.core.*`.

The `-udp-recv-buffer` flag on `edge`/`blast` sets `SO_RCVBUF` explicitly, but
quic-go raises the receive buffer toward its own preferred size at startup, so a
deliberately tiny value only sticks if `rmem_max` is lower still.

### 2. Datagrams actually dropped because the buffer filled

This is the one that causes real packet loss and connection drops. Build a
backlog with a rate limit, then push volume:

```sh
make scenario-buffer
make udpstats       # Udp: ... SndbufErrors  <-- this is the counter that moves here
```

`SndbufErrors` climbing in `/proc/net/snmp` inside the edge container is the
smoking gun on this stack. The edge also logs deltas every 10s
(`"msg":"udp counters"`), and `blast` prints them per interval on stderr.

`RcvbufErrors` is a different failure: the **receiving** process has to stall
with a tiny `SO_RCVBUF` while datagrams keep arriving. That has **not** been
reproduced on Docker Desktop. On native Linux you can try:

```sh
make rcvbuf-pressure
```

Treat a zero `RcvbufErrors` counter as inconclusive, not a pass.

A real 15s run at `rate 5mbit` on this stack produced:

```
Udp: InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors ...
Udp: 220016      0       0        200818       0            79
```

…alongside `502`s and `http3_error` client failures — i.e. the tunnel hop broke
before the client hop did. Note `SndbufErrors` moves reliably here;
`RcvbufErrors` has **not** been reproduced with this setup, since it needs the
receiving process to stall rather than the link to be slow.

Choose the impairment target deliberately, since each container sits on a
different hop:

| Target | Affects |
| --- | --- |
| `blast` | client ↔ edge only (the HTTP/3 hop end users see) |
| `connector` | connector ↔ edge only (the tunnel hop) |
| `edge` | **both** hops — they share `eth0` |

> **Caveat:** `tc netem` attached to `root` on a container veth is not applied
> evenly across TX queues (`tc qdisc show` reports `refcnt 13` here, i.e. multi
> queue). Identical `rate 5mbit` runs have produced both 41 requests at p50
> 3.5s and 4,994 requests at p50 4ms. Treat the kernel counters and the latency
> **tail** as the signal; do not quote the median or the request count as
> reproducible. Shaping evenly needs an `mq`-aware qdisc setup per TX queue.

Other impairments:

```sh
./scripts/impair.sh edge loss 3%
./scripts/impair.sh edge delay 80ms 30ms
./scripts/impair.sh edge reorder 5% 50%
./scripts/impair.sh edge clear
```

## The HTTP/3 harness

`blast` opens N independent QUIC connections (each on its own UDP socket, so
kernel drops can be attributed to one connection). **Closed-loop** (default) runs
M workers per connection that wait for each response — completed RPS collapses
when latency rises. **Open-loop** (`-mode=open -rps=N`) starts requests from a
token bucket so offered load stays put; `omitted` counts ticks dropped because
`max-inflight` was full.

```sh
docker compose run --rm blast blast \
  -url='https://edge:8443/bytes?n=1048576' \
  -conns=8 -workers=32 -duration=2m \
  -keepalive=5s -max-idle-timeout=15s \
  -log=/logs/run.jsonl
```

Useful flags:

| Flag | Why you'd change it |
| --- | --- |
| `-keepalive` | `0` (default) lets idle QUIC connections die — the fastest way to reproduce NAT/UDP-timeout disconnects. `5s` proves keep-alives fix it. |
| `-max-idle-timeout` | Lower it to force `quic_idle_timeout` classification quickly. |
| `-read-body=false` | Abandons response bodies, generating `STOP_SENDING`/stream resets so you can see how the edge reports them. |
| `-udp-recv-buffer` | Per-socket `SO_RCVBUF` on the client side. |
| `-conns` / `-workers` | `conns` spreads load over QUIC connections; `workers` multiplexes streams on one connection (this is what exposes head-of-line and flow-control effects). |
| `-mode` / `-rps` / `-max-inflight` | `closed` (default) waits for responses. `open` requires `-rps` and holds offered load. |
| `-warmup` | Traffic discarded from the summary so percentiles are not cold-start. |
| `-body` / `-body-file` / `-method` | Request bodies (POST `/echo` through the tunnel). |
| `-urls` / `-urls-file` | Mix paths round-robin with `-url`. |
| `-probe-0rtt` | Dial twice before the run; the second handshake is the 0-RTT attempt. |

Second-stack clients (not quic-go):

```sh
make curl-h3 URL=https://localhost:8443/fast
make chrome-h3 URL=https://localhost:8443/fast
```

Host curl is used when it supports `--http3-only`; otherwise a helper image joins
the compose network. Chrome is skipped if it is not installed. Blast itself
advertises **only** `h3`, so it will not silently fall back to HTTP/2.

### Output

Every failure is one JSON line, so you can aggregate without a parser:

```jsonc
{"time":"...","event":"conn_closed","conn":3,"kind":"quic_idle_timeout",
 "detail":"no recent network activity ... | packets_sent=8123 packets_lost=214 smoothed_rtt=83ms",
 "local_addr":"172.19.0.6:54321","remote_addr":"172.19.0.4:8443"}
{"time":"...","event":"request_failed","conn":1,"worker":7,"url":"...",
 "kind":"quic_application_error","detail":"code=0x105 remote=true reason=\"\"","latency_ms":9021}
{"time":"...","event":"request_5xx","conn":0,"status":524,"kind":"http_524","detail":"524"}
```

Disconnect kinds are normalised (`internal/quicerr`):
`quic_idle_timeout`, `quic_handshake_failure`, `quic_stateless_reset`,
`quic_transport_error`, `quic_application_error`, `quic_stream_reset`,
`http3_error`, `client_deadline`, `udp_buffer_pressure`, `unknown`.

Quick rollup of a run:

```sh
jq -r 'select(.kind) | .kind' logs/run.jsonl | sort | uniq -c | sort -rn
```

`blast` also prints a summary on exit with status-code and disconnect-reason
histograms plus latency percentiles.

### Deep QUIC debugging

When the JSONL events tell you *that* a connection failed but not *why*, enable
qlog and inspect the trace in [qvis](https://qvis.quictools.info/):

```sh
mkdir -p logs/qlog/{edge,connector,blast}
EDGE_QLOG_DIR=/logs/qlog/edge \
CONNECTOR_QLOG_DIR=/logs/qlog/connector \
  docker compose up -d --force-recreate edge connector

docker compose run --rm blast blast \
  -url='https://edge:8443/drip?chunks=20&ms=250&size=8192' \
  -certs=/certs -server-name=edge \
  -conns=1 -workers=1 -duration=20s \
  -qlog-dir=/logs/qlog/blast \
  -log=/logs/qlog-run.jsonl
```

The files under `logs/qlog/` are one trace per QUIC connection. Use them when
you need frame-level evidence for handshake failures, idle timeout negotiation,
packet loss, PTO/backoff, stream resets, or whether keep-alives were actually
sent.

Packet captures are the other half of the story. On the Docker host:

```sh
sudo tcpdump -i any -nn -s0 -w logs/quic.pcap 'udp port 8443 or udp port 7844'
```

Inside a Linux container, correlate three layers in the same time window:

```sh
jq -r 'select(.kind) | [.time,.event,.kind,.detail] | @tsv' logs/qlog-run.jsonl
docker compose logs edge connector --since 2m --no-log-prefix
docker compose exec -T edge sh -c 'grep -A1 "^Udp:" /proc/net/snmp'
```

Read the evidence in this order:

1. `blast` summary: did responses negotiate `HTTP/3.0`, and what failed?
2. Edge log: did the edge synthesize 524/1033, or did the client vanish first?
3. UDP counters: did `RcvbufErrors`, `SndbufErrors`, or `InErrors` move?
4. qlog/qvis: did the trace show loss recovery, idle timeout, stream reset, or handshake failure?
5. pcap: did UDP packets disappear before they reached the process?

Environment notes:

| Environment | What to trust | Caveat |
| --- | --- | --- |
| macOS Docker Desktop | App logs, `blast` JSONL, qlog | `/proc/net/snmp` and sysctls belong to the Docker VM, not macOS. |
| Native Linux Docker | App logs, qlog, `/proc/net/snmp`, tcpdump | Best local environment for UDP buffer work. |
| CI Linux runner | Fast smoke tests and deterministic 524/1033 checks | Avoid drawing conclusions from high-rate packet-loss scenarios. |
| AKS pod/node | Node sysctls, pod logs, tcpdump where permitted | `net.core.*` sysctls are node-level, not pod network namespace settings. |

## Mapping back to AKS

| Local knob | AKS / Cloudflare equivalent |
| --- | --- |
| `./scripts/host-rmem.sh` | node pool `linuxOSConfig.sysctls.netCoreRmemMax` |
| `edge -origin-timeout` | Cloudflare's fixed 100s origin timeout (524) |
| `connector -keepalive` | `cloudflared` `--keepalive-connections` / QUIC keep-alive |
| `CONNECTOR_REPLICAS=4 docker compose up -d` | replica count of the `cloudflared` Deployment |
| `impair.sh edge loss` | lossy node egress / overloaded NAT gateway |
| `blast -conns/-workers` | client concurrency hitting the tunnel hostname |

Run more connectors to test whether the drops follow one replica:

```sh
CONNECTOR_REPLICAS=4 docker compose up -d --scale connector=4 connector
```

## Layout

```
cmd/quicshot/          single binary, four subcommands
internal/edge/         h3 termination, tunnel listener, 524 logic
internal/connector/    cloudflared-like outbound QUIC connector
internal/origin/       controllable origin
internal/blast/        HTTP/3 load harness + disconnect logging
internal/quicerr/      QUIC/HTTP3 error -> stable label
internal/udpsock/      UDP socket buffers + /proc/net/snmp counters
scripts/impair.sh      tc netem helpers
scripts/host-rmem.sh   host-kernel sysctl helper
scripts/h3-clients.sh  curl --http3-only and headless Chrome
scripts/rcvbuf-pressure.sh  Linux-only RcvbufErrors attempt
```

Tunnel hop knobs (independent of the public HTTP/3 hop):

```sh
EDGE_TUNNEL_MAX_IDLE=15s EDGE_TUNNEL_KEEPALIVE=2s EDGE_TUNNEL_LB=hash \
  docker compose up -d --force-recreate edge
```

`EDGE_TUNNEL_LB=hash` pins a request path to one connector so you can see
whether drops follow a replica. `rr` is the default.

## Still out of scope

This is a tunnel-path failure lab, not an HTTP/3 conformance suite. It does not
implement WebTransport, HTTP datagrams, CONNECT-UDP/MASQUE, connection migration,
QUIC v2 / retry / key-update tests, PMTUD, ECN, or a live qvis panel. qlog files
plus `tcpdump` remain the escape hatch for frame-level work.

Build without Docker: `go build ./cmd/quicshot`.
