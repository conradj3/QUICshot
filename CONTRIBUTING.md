# Contributing

Thanks for taking a look at QUICshot. The goal of this repo is to be
easy to clone, run locally, and modify when someone needs to explain an HTTP/3,
QUIC, Cloudflare Tunnel, or UDP-buffer failure mode.

## Local Setup

Prerequisites:

- Go 1.25 or newer (required by quic-go v0.61).
- Docker with Compose v2.
- Optional: `jq`, `tcpdump`, and curl with HTTP/3 support.

Run the local quality gate before opening a PR:

```sh
make test
make smoke
```

`make smoke` builds the image, starts the local stack, verifies HTTP/3 on the
fast path, verifies qlog output, then checks 524, 1033, origin RST (1014),
invisible client-deadline, idle vs keepalive, Alt-Svc, and POST `/echo`. Use
`KEEP_STACK=1 make smoke` to leave the containers running for inspection.

## Code Style

- Keep the single-binary shape: new functionality should usually be another
  package under `internal/` plus a subcommand or flag in `cmd/quicshot`.
- Keep network failure labels stable. People use the JSONL fields for grep,
  dashboards, and CI checks.
- Prefer small, deterministic tests for classifiers and parsers. Use the Docker
  smoke test for end-to-end HTTP/3 behavior.
- Do not make high-rate packet-loss scenarios part of required CI; they are too
  sensitive to host and runner networking.
- Treat real public endpoints carefully. `probe` is safe by design; `blast`
  should be rate-limited and authorized.

## Pull Requests

Please include:

- What behavior changed.
- How you validated it.
- Any environment-specific caveat, especially macOS Docker Desktop vs native
  Linux vs Kubernetes.

Useful validation commands:

```sh
make test
make smoke
docker compose config
```