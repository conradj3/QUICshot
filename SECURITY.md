# Security Policy

This project is a local diagnostic harness. It is not intended to be exposed to
the public internet.

## Reporting Security Issues

Please do not open a public issue for a vulnerability. If this repository is
published under a GitHub account or organization, enable private vulnerability
reporting and use that channel. Until then, contact the maintainer privately.

## Operating Guidelines

- The control UI binds to `127.0.0.1` by default. Keep it local.
- The generated certificates are for local reproduction only.
- The Docker services add `NET_ADMIN` so `tc netem` can shape traffic. Do not
  run this compose stack on shared or production hosts.
- Get authorization before running sustained `blast` traffic against any real
  endpoint.
- Avoid putting secrets in `-header` flags in shared terminals, shell history,
  CI logs, or issue reports.