// Package ui serves a local control panel for the whole rig: start/stop the
// stack, retune the edge, apply network impairment, run probe/blast, and watch
// every container log plus the run output in one stream.
package ui

import (
	"bufio"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed static
var staticFS embed.FS

func Main(args []string) error {
	addr := "127.0.0.1:8088"
	dir := "."
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-addr":
			if i+1 < len(args) {
				addr = args[i+1]
				i++
			}
		case "-dir":
			if i+1 < len(args) {
				dir = args[i+1]
				i++
			}
		case "-h", "--help":
			fmt.Println("quicshot ui [-addr 127.0.0.1:8088] [-dir .]")
			return nil
		}
	}

	s := &Server{hub: newHub(), dir: dir}
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(sub)))
	mux.HandleFunc("/api/events", s.handleEvents)
	mux.HandleFunc("/api/stack", s.handleStack)
	mux.HandleFunc("/api/impair", s.handleImpair)
	mux.HandleFunc("/api/run", s.handleRun)
	mux.HandleFunc("/api/reset", s.handleReset)
	mux.HandleFunc("/api/stop", s.handleStop)

	go s.tailLogs("origin")
	go s.tailLogs("edge")
	go s.tailLogs("connector")
	go s.pollConnectors()
	go s.pollUDP()
	go s.publishFlow()

	fmt.Printf("\n  QUICshot control panel:  http://%s\n\n", addr)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	return srv.ListenAndServe()
}

type Server struct {
	hub *hub
	dir string

	mu      sync.Mutex
	cancel  context.CancelFunc
	current string

	rl   sync.Mutex
	rate map[string]*bucket

	flow flowState
}

// flowState is derived from the containers' own log lines so the diagram stays
// accurate even when the UI rate limit drops lines from the display.
type flowState struct {
	mu           sync.Mutex
	tunnelUp     bool
	tunnelKnown  bool // false until traffic or a register/lost line tells us
	connectors   int
	connectorReq map[int]int64
	impair       string
	clientReq    int64
	clientFail   int64

	edgeReq, edge524, edge502 int64 // totals
	dReq, d524, d502          int64 // this interval
	sumMs, cntMs              int64
}

var (
	blastTotalRE  = regexp.MustCompile(`total=(\d+) fail=(\d+)`)
	statusRE      = regexp.MustCompile(`"status":(\d+)`)
	elapsedRE     = regexp.MustCompile(`"elapsed_ms":(\d+)`)
	connectorsRE  = regexp.MustCompile(`"connectors":(\d+)`)
	connectorIDRE = regexp.MustCompile(`"connector_id":(\d+)`)
)

func atoi64(s string) int64 { n, _ := strconv.ParseInt(s, 10, 64); return n }

func atoi(s string) int { n, _ := strconv.Atoi(s); return n }

// observe runs on every log line, before rate limiting.
func (s *Server) observe(src, line string) {
	f := &s.flow
	if src == "blast" && strings.HasPrefix(line, "[blast]") {
		if m := blastTotalRE.FindStringSubmatch(line); m != nil {
			f.mu.Lock()
			f.clientReq, f.clientFail = atoi64(m[1]), atoi64(m[2])
			f.mu.Unlock()
		}
		return
	}
	if src != "edge" || len(line) == 0 || line[0] != '{' {
		return
	}

	switch {
	case strings.Contains(line, `"msg":"proxied"`):
		var ms int64
		if m := elapsedRE.FindStringSubmatch(line); m != nil {
			ms = atoi64(m[1])
		}
		connectorID := 0
		if m := connectorIDRE.FindStringSubmatch(line); m != nil {
			connectorID = atoi(m[1])
		}
		f.mu.Lock()
		if f.connectorReq == nil {
			f.connectorReq = map[int]int64{}
		}
		f.edgeReq++
		f.dReq++
		f.sumMs += ms
		f.cntMs++
		if connectorID > 0 {
			f.connectorReq[connectorID]++
		}
		f.tunnelUp, f.tunnelKnown = true, true // a proxied request proves the tunnel works
		f.mu.Unlock()
	case strings.Contains(line, `"msg":"origin failure"`):
		var st int64
		if m := statusRE.FindStringSubmatch(line); m != nil {
			st = atoi64(m[1])
		}
		f.mu.Lock()
		f.edgeReq++
		f.dReq++
		if st == statusOriginTimeout {
			f.edge524++
			f.d524++
			f.tunnelUp, f.tunnelKnown = true, true // reached the origin, so the tunnel is up
		} else {
			f.edge502++
			f.d502++
		}
		f.mu.Unlock()
	case strings.Contains(line, `"msg":"no connector"`):
		f.mu.Lock()
		f.edgeReq++
		f.dReq++
		f.edge502++
		f.d502++
		f.tunnelUp, f.tunnelKnown, f.connectors = false, true, 0
		f.mu.Unlock()
	case strings.Contains(line, `"msg":"connector registered"`), strings.Contains(line, `"msg":"connector lost"`):
		n := int64(0)
		if m := connectorsRE.FindStringSubmatch(line); m != nil {
			n = atoi64(m[1])
		}
		f.mu.Lock()
		f.connectors = int(n)
		f.tunnelUp, f.tunnelKnown = n > 0, true
		f.mu.Unlock()
	}
}

// statusOriginTimeout mirrors the edge's non-standard 524.
const statusOriginTimeout = 524

func (s *Server) publishFlow() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for range t.C {
		f := &s.flow
		f.mu.Lock()
		var p50 int64
		if f.cntMs > 0 {
			p50 = f.sumMs / f.cntMs
		}
		connectorReq := map[int]int64{}
		for id, n := range f.connectorReq {
			connectorReq[id] = n
		}
		msg := map[string]any{
			"t":      "flow",
			"client": map[string]any{"req": f.clientReq, "fail": f.clientFail},
			"edge": map[string]any{"rps": f.dReq, "s524": f.d524, "s502": f.d502,
				"total": f.edgeReq, "t524": f.edge524, "t502": f.edge502, "avgMs": p50},
			"tunnel": map[string]any{"up": f.tunnelUp, "n": f.connectors, "known": f.tunnelKnown, "connectorReq": connectorReq},
			"origin": map[string]any{"rps": f.dReq - f.d502},
			"impair": f.impair,
		}
		f.dReq, f.d524, f.d502, f.sumMs, f.cntMs = 0, 0, 0, 0, 0
		f.mu.Unlock()
		s.hub.publish(msg)
	}
}

// A blast can emit tens of thousands of lines a second, which would drown the
// browser. Each source gets a per-second budget; the overflow is counted and
// reported as a single line instead.
const linesPerSecond = 120

type bucket struct {
	window     time.Time
	count      int
	suppressed int
}

func (s *Server) allow(src string) (ok bool, note string) {
	s.rl.Lock()
	defer s.rl.Unlock()
	if s.rate == nil {
		s.rate = map[string]*bucket{}
	}
	b := s.rate[src]
	if b == nil {
		b = &bucket{window: time.Now()}
		s.rate[src] = b
	}
	if time.Since(b.window) >= time.Second {
		if b.suppressed > 0 {
			note = fmt.Sprintf("... %d more lines suppressed (UI rate limit)", b.suppressed)
		}
		b.window, b.count, b.suppressed = time.Now(), 0, 0
	}
	b.count++
	if b.count > linesPerSecond {
		b.suppressed++
		return false, note
	}
	return true, note
}

func (s *Server) log(src, line string) {
	s.observe(src, line)
	ok, note := s.allow(src)
	if note != "" {
		s.hub.publish(map[string]any{"t": "log", "src": src, "line": note,
			"ts": time.Now().Format("15:04:05")})
	}
	if !ok {
		return
	}
	s.hub.publish(map[string]any{"t": "log", "src": src, "line": line, "ts": time.Now().Format("15:04:05")})
}

// ------------------------------------------------------------------ event bus

type hub struct {
	mu   sync.Mutex
	subs map[chan []byte]struct{}
}

func newHub() *hub { return &hub{subs: map[chan []byte]struct{}{}} }

func (h *hub) sub() chan []byte {
	c := make(chan []byte, 512)
	h.mu.Lock()
	h.subs[c] = struct{}{}
	h.mu.Unlock()
	return c
}

func (h *hub) unsub(c chan []byte) {
	h.mu.Lock()
	delete(h.subs, c)
	h.mu.Unlock()
	close(c)
}

func (h *hub) publish(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.subs {
		select {
		case c <- b:
		default: // slow client: drop rather than stall the producer
		}
	}
}

func (s *Server) status(running bool, name string) {
	s.hub.publish(map[string]any{"t": "status", "running": running, "name": name})
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	c := s.hub.sub()
	defer s.hub.unsub(c)

	s.mu.Lock()
	running, name := s.cancel != nil, s.current
	s.mu.Unlock()
	s.status(running, name)

	ping := time.NewTicker(20 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case b := <-c:
			fmt.Fprintf(w, "data: %s\n\n", b)
			fl.Flush()
		case <-ping.C:
			fmt.Fprint(w, ": ping\n\n")
			fl.Flush()
		}
	}
}

// -------------------------------------------------------------- process plumbing

// runStreaming executes cmd and publishes every output line under src. All
// commands are built from validated argument slices, never a shell string.
func (s *Server) runStreaming(ctx context.Context, src string, env []string, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = s.dir
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = cmd.Stdout.(interface{ Write([]byte) (int, error) }).(io.Writer)
	stderr, err := cmd.StderrPipe()
	if err == nil {
		go s.scan(src, stderr)
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	s.scan(src, stdout)
	return cmd.Wait()
}

func (s *Server) scan(src string, r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		s.log(src, sc.Text())
	}
	if err := sc.Err(); err != nil {
		s.log(src, "scan error: "+err.Error())
	}
}

// tailLogs follows one compose service, restarting if the container cycles.
func (s *Server) tailLogs(service string) {
	for {
		cmd := exec.Command("docker", "compose", "logs", "-f", "--no-log-prefix", "--tail", "20", service)
		cmd.Dir = s.dir
		out, err := cmd.StdoutPipe()
		if err == nil && cmd.Start() == nil {
			s.scan(service, out)
			cmd.Wait()
		}
		time.Sleep(2 * time.Second)
	}
}

func (s *Server) pollUDP() {
	type counters struct{ in, out, rcv, snd uint64 }
	var prev counters
	for {
		time.Sleep(3 * time.Second)
		cmd := exec.Command("docker", "compose", "exec", "-T", "edge", "cat", "/proc/net/snmp")
		cmd.Dir = s.dir
		out, err := cmd.Output()
		if err != nil {
			continue
		}
		var hdr []string
		for _, line := range strings.Split(string(out), "\n") {
			if !strings.HasPrefix(line, "Udp:") {
				continue
			}
			f := strings.Fields(line)
			if hdr == nil {
				hdr = f
				continue
			}
			vals := map[string]uint64{}
			for i := 1; i < len(f) && i < len(hdr); i++ {
				n, _ := strconv.ParseUint(f[i], 10, 64)
				vals[hdr[i]] = n
			}
			cur := counters{vals["InDatagrams"], vals["OutDatagrams"], vals["RcvbufErrors"], vals["SndbufErrors"]}
			if prev != (counters{}) {
				s.hub.publish(map[string]any{"t": "udp",
					"in": cur.in - prev.in, "out": cur.out - prev.out,
					"rcvbuf": cur.rcv - prev.rcv, "sndbuf": cur.snd - prev.snd,
					"rcvbufTotal": cur.rcv, "sndbufTotal": cur.snd})
			}
			prev = cur
			break
		}
	}
}

func (s *Server) pollConnectors() {
	for {
		time.Sleep(2 * time.Second)
		cmd := exec.Command("docker", "compose", "ps", "-q", "--status", "running", "connector")
		cmd.Dir = s.dir
		out, err := cmd.Output()
		if err != nil {
			continue
		}
		n := countLines(string(out))
		s.flow.mu.Lock()
		s.flow.connectors = n
		s.flow.tunnelKnown = true
		s.flow.tunnelUp = n > 0
		s.flow.mu.Unlock()
	}
}

func countLines(s string) int {
	n := 0
	for _, line := range strings.Split(s, "\n") {
		if strings.TrimSpace(line) != "" {
			n++
		}
	}
	return n
}

// start claims the single run slot so two actions cannot interleave.
func (s *Server) start(name string, fn func(ctx context.Context)) error {
	s.mu.Lock()
	if s.cancel != nil {
		s.mu.Unlock()
		return errors.New("a run is already in progress")
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel, s.current = cancel, name
	s.mu.Unlock()

	s.status(true, name)
	go func() {
		fn(ctx)
		s.mu.Lock()
		s.cancel, s.current = nil, ""
		s.mu.Unlock()
		s.status(false, "")
	}()
	return nil
}

func (s *Server) handleStop(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Unlock()
	s.log("ui", "stop requested")
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	s.resetFlow()
	s.log("ui", "new test: counters reset")
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) resetFlow() {
	s.flow.mu.Lock()
	defer s.flow.mu.Unlock()
	s.flow.clientReq, s.flow.clientFail = 0, 0
	s.flow.edgeReq, s.flow.edge524, s.flow.edge502 = 0, 0, 0
	s.flow.dReq, s.flow.d524, s.flow.d502 = 0, 0, 0
	s.flow.sumMs, s.flow.cntMs = 0, 0
	s.flow.connectorReq = map[int]int64{}
}

// ---------------------------------------------------------------- API handlers

type stackReq struct {
	Action        string `json:"action"`
	OriginTimeout string `json:"originTimeout"`
	MaxIdle       string `json:"maxIdle"`
	KeepAlive     string `json:"keepAlive"`
	Connectors    int    `json:"connectors"`
}

func (s *Server) handleStack(w http.ResponseWriter, r *http.Request) {
	var req stackReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpErr(w, err)
		return
	}
	env, args, err := stackCommand(req)
	if err != nil {
		httpErr(w, err)
		return
	}

	name := "stack:" + req.Action
	err = s.start(name, func(ctx context.Context) {
		s.log("ui", "$ docker "+strings.Join(args, " ")+" "+strings.Join(env, " "))
		if err := s.runStreaming(ctx, "ui", env, "docker", args...); err != nil {
			s.log("ui", "error: "+err.Error())
		}
	})
	if err != nil {
		httpErr(w, err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func stackCommand(req stackReq) ([]string, []string, error) {
	env := []string{}
	for k, v := range map[string]string{
		"EDGE_ORIGIN_TIMEOUT": req.OriginTimeout,
		"EDGE_MAX_IDLE":       req.MaxIdle,
		"EDGE_KEEPALIVE":      req.KeepAlive,
	} {
		if v != "" {
			if !durationRE.MatchString(v) {
				return nil, nil, fmt.Errorf("%s: expected a duration like 10s", k)
			}
			env = append(env, k+"="+v)
		}
	}

	var args []string
	switch req.Action {
	case "up":
		args = []string{"compose", "up", "-d", "--build", "origin", "edge", "connector"}
	case "down":
		args = []string{"compose", "down"}
	case "restart-edge":
		args = []string{"compose", "up", "-d", "--force-recreate", "edge"}
	case "scale-connectors":
		n := req.Connectors
		if n < 0 || n > 10 {
			return nil, nil, errors.New("connectors must be 0..10")
		}
		args = []string{"compose", "up", "-d", "--scale", "connector=" + strconv.Itoa(n), "connector"}
	case "stop-connector":
		args = []string{"compose", "stop", "connector"}
	case "start-connector":
		args = []string{"compose", "up", "-d", "--scale", "connector=1", "connector"}
	default:
		return nil, nil, errors.New("unknown action")
	}
	return env, args, nil
}

var (
	durationRE = regexp.MustCompile(`^[0-9]+(ms|s|m)$`)
	netemArgRE = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?(%|ms|s|kbit|mbit|gbit)$`)
	services   = map[string]bool{"edge": true, "connector": true, "origin": true}
	netemModes = map[string]bool{"loss": true, "delay": true, "rate": true, "reorder": true, "clear": true}
)

type impairReq struct {
	Service string `json:"service"`
	Mode    string `json:"mode"`
	A       string `json:"a"`
	B       string `json:"b"`
}

func (s *Server) handleImpair(w http.ResponseWriter, r *http.Request) {
	var req impairReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpErr(w, err)
		return
	}
	if !services[req.Service] || !netemModes[req.Mode] {
		httpErr(w, errors.New("unknown service or mode"))
		return
	}
	tc := []string{"compose", "exec", "-T", req.Service, "tc", "qdisc"}
	switch req.Mode {
	case "clear":
		tc = append(tc, "del", "dev", "eth0", "root")
	default:
		for _, v := range []string{req.A, req.B} {
			if v != "" && !netemArgRE.MatchString(v) {
				httpErr(w, fmt.Errorf("bad netem value %q", v))
				return
			}
		}
		tc = append(tc, "replace", "dev", "eth0", "root", "netem")
		switch req.Mode {
		case "loss":
			tc = append(tc, "loss", req.A)
		case "rate":
			tc = append(tc, "rate", req.A, "limit", "100")
		case "delay":
			tc = append(tc, "delay", req.A)
			if req.B != "" {
				tc = append(tc, req.B, "distribution", "normal")
			}
		case "reorder":
			tc = append(tc, "delay", "10ms", "reorder", req.A)
			if req.B != "" {
				tc = append(tc, req.B)
			}
		}
	}
	name := "impair:" + req.Service + ":" + req.Mode
	if err := s.start(name, func(ctx context.Context) {
		s.log("ui", "$ docker "+strings.Join(tc, " "))
		if err := s.runStreaming(ctx, "ui", nil, "docker", tc...); err != nil {
			s.log("ui", "impair: "+err.Error())
		}
	}); err != nil {
		httpErr(w, err)
		return
	}
	s.flow.mu.Lock()
	if req.Mode == "clear" {
		s.flow.impair = ""
	} else {
		s.flow.impair = req.Service + " " + req.Mode + " " + req.A
	}
	s.flow.mu.Unlock()
	w.WriteHeader(http.StatusAccepted)
}

type runReq struct {
	Tool       string   `json:"tool"` // blast | probe
	Runner     string   `json:"runner"`
	URL        string   `json:"url"`
	Conns      int      `json:"conns"`
	Workers    int      `json:"workers"`
	Duration   string   `json:"duration"`
	Timeout    string   `json:"timeout"`
	KeepAlive  string   `json:"keepAlive"`
	MaxIdle    string   `json:"maxIdle"`
	RPS        float64  `json:"rps"`
	ReadBody   bool     `json:"readBody"`
	Insecure   bool     `json:"insecure"`
	LocalCerts bool     `json:"localCerts"`
	Headers    []string `json:"headers"`
}

func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	var req runReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpErr(w, err)
		return
	}
	target, err := safeURL(req.URL)
	if err != nil {
		httpErr(w, err)
		return
	}
	if req.Tool != "blast" && req.Tool != "probe" {
		httpErr(w, errors.New("tool must be blast or probe"))
		return
	}

	toolArgs := []string{req.Tool, "-url=" + target}
	for _, h := range req.Headers {
		if h == "" {
			continue
		}
		if !strings.Contains(h, ":") || strings.ContainsAny(h, "\r\n") {
			httpErr(w, fmt.Errorf("bad header %q", h))
			return
		}
		toolArgs = append(toolArgs, "-header="+h)
	}
	if req.Insecure {
		toolArgs = append(toolArgs, "-insecure")
	}
	if req.LocalCerts {
		toolArgs = append(toolArgs, "-certs=/certs", "-server-name=edge")
	}
	if req.Tool == "blast" {
		for _, kv := range [][2]string{
			{"-duration=", req.Duration}, {"-timeout=", req.Timeout},
			{"-keepalive=", req.KeepAlive}, {"-max-idle-timeout=", req.MaxIdle},
		} {
			if kv[1] == "" {
				continue
			}
			if !durationRE.MatchString(kv[1]) {
				httpErr(w, fmt.Errorf("%s expected a duration like 30s", kv[0]))
				return
			}
			toolArgs = append(toolArgs, kv[0]+kv[1])
		}
		toolArgs = append(toolArgs,
			"-conns="+strconv.Itoa(clamp(req.Conns, 1, 64)),
			"-workers="+strconv.Itoa(clamp(req.Workers, 1, 256)),
			"-stats-every=5s",
			"-read-body="+strconv.FormatBool(req.ReadBody),
		)
		if req.RPS > 0 {
			toolArgs = append(toolArgs, "-rps="+strconv.FormatFloat(req.RPS, 'f', -1, 64))
		}
	}

	var name string
	var argv []string
	if req.Runner == "host" {
		self, err := os.Executable()
		if err != nil {
			httpErr(w, err)
			return
		}
		name, argv = self, toolArgs
	} else {
		name = "docker"
		argv = append([]string{"compose", "run", "--rm", "-T", "blast"}, toolArgs...)
	}

	err = s.start(req.Tool, func(ctx context.Context) {
		s.log("ui", "$ "+name+" "+strings.Join(argv, " "))
		if err := s.runStreaming(ctx, req.Tool, nil, name, argv...); err != nil {
			s.log(req.Tool, "exit: "+err.Error())
		}
	})
	if err != nil {
		httpErr(w, err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func safeURL(s string) (string, error) {
	if s == "" {
		return "", errors.New("url is required")
	}
	u, err := url.Parse(s)
	if err != nil {
		return "", fmt.Errorf("bad url: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", errors.New("url must be http or https")
	}
	if u.Host == "" {
		return "", errors.New("url must include a host")
	}
	return u.String(), nil
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func httpErr(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}
