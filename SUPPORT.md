# Support

This repo is designed for self-service local reproduction.

Before filing an issue, run:

```sh
make test
make smoke
```

For network-behavior issues, include:

- OS and architecture.
- Docker Desktop or native Docker version.
- `docker compose version`.
- The exact command you ran.
- Relevant `logs/*.jsonl` events.
- Whether qlog was enabled, and which side produced traces.
- Whether you are on VPN, corporate Wi-Fi, or another network that may block UDP.
