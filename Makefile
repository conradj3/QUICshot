COMPOSE ?= docker compose
QUICshot_REVISION ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
export QUICshot_REVISION
BLAST   ?= $(COMPOSE) run --rm blast blast -certs=/certs -server-name=edge
# Point at a real Cloudflare tunnel hostname: make blast-remote URL=https://app.example.com/
REMOTE  ?= $(COMPOSE) run --rm blast blast

.PHONY: help test ci smoke build build-linux up down logs ps demo demo-auto demo-fast ui ui-stop probe blast blast-remote \
        scenario-524 scenario-hang scenario-loss scenario-idle scenario-buffer scenario-keepalive scenario-invisible \
        scenario-reset scenario-open scenario-0rtt curl-h3 chrome-h3 rcvbuf-pressure interop \
        udpstats clear-impair clean

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

test: ## Run unit tests, vet, and gofmt check
	@test -z "$$(gofmt -l .)"
	go test ./...
	go vet ./...

ci: test build ## Run the same local checks as CI, except the Docker smoke test

smoke: ## Run deterministic Docker end-to-end smoke checks
	./scripts/smoke.sh

interop: ## In-process HTTP/3 stack test (blast + optional curl)
	go test ./internal/integration/ -count=1 -timeout 60s

demo: ## Guided end-to-end demo (pauses between acts)
	./scripts/demo.sh

demo-auto: ## Same demo, no pauses (for recording)
	./scripts/demo.sh --auto

demo-fast: ## Same demo, short durations (for rehearsal)
	./scripts/demo.sh --fast

build: ## Build the QUICshot image
	$(COMPOSE) build

up: ## Start origin + edge + connector
	$(COMPOSE) up -d --build origin edge connector
	@echo "edge is on https://localhost:8443 (h3 on udp/8443)"

down: ## Stop everything
	$(COMPOSE) down -v

ps: ## Show container state
	$(COMPOSE) ps

logs: ## Tail all logs
	$(COMPOSE) logs -f --tail=50

UI_PORT ?= 8088

ui: ## Open the control panel (frees the port first)
	@$(MAKE) --no-print-directory ui-stop
	go run ./cmd/quicshot ui -addr 127.0.0.1:$(UI_PORT)

ui-stop: ## Kill whatever is listening on the UI port
	@pids=$$(lsof -ti :$(UI_PORT) 2>/dev/null); \
	 if [ -n "$$pids" ]; then echo "killing $$pids on :$(UI_PORT)"; kill $$pids 2>/dev/null || true; sleep 1; \
	 else echo "nothing listening on :$(UI_PORT)"; fi

probe: ## Diagnose one URL (safe, single requests): make probe URL=https://app.example.com/
	@test -n "$(URL)" || (echo 'usage: make probe URL=https://your-hostname/' && exit 2)
	go run ./cmd/quicshot probe -url='$(URL)' $(PROBE_ARGS)

build-linux: ## Static linux/amd64 binary in dist/, to run from a VM or pod
	mkdir -p dist
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -o dist/quicshot-linux-amd64 ./cmd/quicshot
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -o dist/quicshot-linux-arm64 ./cmd/quicshot
	@ls -la dist/

blast: ## Baseline load against /fast
	$(BLAST) -url=https://edge:8443/fast -conns=4 -workers=16 -duration=30s -log=/logs/blast.jsonl

blast-remote: ## Blast a real HTTP/3 endpoint: make blast-remote URL=https://app.example.com/
	@test -n "$(URL)" || (echo 'usage: make blast-remote URL=https://your-tunnel-hostname/' && exit 2)
	$(REMOTE) -url='$(URL)' -conns=$(or $(CONNS),4) -workers=$(or $(WORKERS),8) \
	  -duration=$(or $(DURATION),60s) -rps=$(or $(RPS),50) -keepalive=5s \
	  -max-failure-pct=$(or $(MAX_FAIL),1) -log=/logs/remote.jsonl

scenario-524: ## Origin sleeps longer than the edge origin-timeout -> 524
	$(BLAST) -url='https://edge:8443/slow?ms=20000' -conns=2 -workers=4 -duration=45s -timeout=40s -log=/logs/524.jsonl

scenario-hang: ## Origin never responds -> 524 with a wedged worker
	$(BLAST) -url=https://edge:8443/hang -conns=2 -workers=4 -duration=45s -timeout=40s -log=/logs/hang.jsonl

scenario-loss: ## 3% packet loss on the edge, then blast
	./scripts/impair.sh edge loss 3%
	$(BLAST) -url='https://edge:8443/bytes?n=2097152' -conns=4 -workers=16 -duration=60s -log=/logs/loss.jsonl
	./scripts/impair.sh edge clear

scenario-idle: ## No keep-alives + slow origin -> client-side idle timeouts
	EDGE_MAX_IDLE=5s EDGE_KEEPALIVE=0s $(COMPOSE) up -d --force-recreate edge
	$(BLAST) -url='https://edge:8443/slow?ms=8000' -conns=2 -workers=2 -duration=60s -keepalive=0s -max-idle-timeout=5s -log=/logs/idle.jsonl

scenario-keepalive: ## Same as idle, with keep-alives — expect zero connection drops
	$(BLAST) -url='https://edge:8443/slow?ms=8000' -conns=2 -workers=2 -duration=60s -keepalive=1s -max-idle-timeout=5s -log=/logs/keepalive.jsonl

scenario-invisible: ## Client timeout shorter than the edge origin timeout: client_deadline, no 524
	$(BLAST) -url='https://edge:8443/slow?ms=20000' -conns=1 -workers=2 -duration=15s -timeout=3s -log=/logs/invisible.jsonl

scenario-reset: ## Origin RSTs the TCP socket -> 502 / error code 1014
	$(BLAST) -url=https://edge:8443/reset -conns=1 -workers=1 -requests=4 -duration=15s -timeout=5s -log=/logs/reset.jsonl

scenario-open: ## Open-loop 100 rps; offered load does not collapse when latency rises
	$(BLAST) -url=https://edge:8443/fast -mode=open -rps=100 -max-inflight=128 -conns=4 -workers=8 -duration=20s -log=/logs/open.jsonl

scenario-0rtt: ## Dial twice and record whether the second handshake used 0-RTT
	$(BLAST) -url=https://edge:8443/fast -probe-0rtt -conns=1 -workers=1 -requests=1 -duration=5s -log=/logs/0rtt.jsonl

curl-h3: ## HTTP/3 GET with curl --http3-only (host curl or helper image)
	./scripts/h3-clients.sh curl $(or $(URL),https://localhost:8443/fast) --insecure

chrome-h3: ## Headless Chrome forced onto HTTP/3 (skips if Chrome is not installed)
	./scripts/h3-clients.sh chrome $(or $(URL),https://localhost:8443/fast) --insecure

rcvbuf-pressure: ## Linux-only attempt to move Udp RcvbufErrors (exits 2 on Docker Desktop)
	./scripts/rcvbuf-pressure.sh

scenario-buffer: ## Rate-limit the edge to build a UDP backlog, watch SndbufErrors
	./scripts/impair.sh edge rate 5mbit
	$(BLAST) -url='https://edge:8443/bytes?n=8388608' -conns=8 -workers=16 -duration=60s -log=/logs/buffer.jsonl
	$(MAKE) udpstats
	./scripts/impair.sh edge clear

udpstats: ## Kernel UDP counters inside the edge container
	@$(COMPOSE) exec -T edge sh -c 'grep -A1 "^Udp:" /proc/net/snmp'

clear-impair: ## Remove any netem qdisc from the edge
	./scripts/impair.sh edge clear

clean: down ## Stop and remove logs
	rm -rf logs/*.jsonl logs/*.pcap logs/qlog dist
