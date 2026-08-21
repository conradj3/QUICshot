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
	"path/filepath"
	"regexp"
	"sort"
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
	go s.pollServiceHealth()
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

	healthMu sync.Mutex
	health   map[string]serviceHealth
}

// flowState is derived from the containers' own log lines so the diagram stays
// accurate even when the UI rate limit drops lines from the display.
type flowState struct {
	mu           sync.Mutex
	tunnelUp     bool
	tunnelKnown  bool // false until traffic or a register/lost line tells us
	connectors   int
	connectorReq map[int]int64
	connectorP95 map[int]int64
	connectorP99 map[int]int64
	connectorErr map[int]int64
	impair       string
	clientReq    int64
	clientFail   int64

	edgeReq, edge524, edge502 int64   // totals
	dReq, d524, d502          int64   // this interval
	edgeLatency               []int64 // this interval latency samples in ms

	connOpened, connClosed           int64
	connOpenedTotal, connClosedTotal int64
	connDropsTotal                   int64
	activeConns                      int64

	quicVersions  map[string]int64
	protos        map[string]int64
	zeroRTTAccept int64
	zeroRTTReject int64

	connectorLat map[int][]int64
}

type serviceHealth struct {
	CPU      float64 `json:"cpu"`
	Mem      float64 `json:"mem"`
	Replicas int     `json:"replicas"`
	Updated  int64   `json:"updated"`
}

var (
	blastTotalRE  = regexp.MustCompile(`total=(\d+) fail=(\d+)`)
	connDropRE    = regexp.MustCompile(`^conn drops\s+(\d+)`)
	protoLineRE   = regexp.MustCompile(`^\s*(HTTP/\d\.\d)\s+(\d+)`)
	statusRE      = regexp.MustCompile(`"status":(\d+)`)
	elapsedRE     = regexp.MustCompile(`"elapsed_ms":(\d+)`)
	connectorsRE  = regexp.MustCompile(`"connectors":(\d+)`)
	connectorIDRE = regexp.MustCompile(`"connector_id":(\d+)`)
)

func atoi64(s string) int64 { n, _ := strconv.ParseInt(s, 10, 64); return n }

func atoi(s string) int { n, _ := strconv.Atoi(s); return n }

// observe runs on every log line, before rate limiting.
func (s *Server) observe(src, line string) {
	if src == "blast" {
		s.observeBlast(src, line)
		return
	}
	if !isEdgeJSON(src, line) {
		return
	}
	s.observeEdgeLine(line)
}

func (s *Server) observeBlast(_ string, line string) {
	f := &s.flow
	if strings.HasPrefix(line, "[blast]") {
		if m := blastTotalRE.FindStringSubmatch(line); m != nil {
			f.mu.Lock()
			f.clientReq, f.clientFail = atoi64(m[1]), atoi64(m[2])
			f.mu.Unlock()
		}
		return
	}
	s.observeBlastLine(line)
}

func isEdgeJSON(src, line string) bool {
	return src == "edge" && len(line) > 0 && line[0] == '{'
}

func (s *Server) observeEdgeLine(line string) {
	switch {
	case strings.Contains(line, `"msg":"proxied"`):
		s.observeEdgeProxied(line)
	case strings.Contains(line, `"msg":"origin failure"`):
		s.observeEdgeOriginFailure(line)
	case strings.Contains(line, `"msg":"no connector"`):
		s.observeEdgeNoConnector()
	case strings.Contains(line, `"msg":"connector registered"`), strings.Contains(line, `"msg":"connector lost"`):
		s.observeEdgeConnectorState(line)
	}
}

func (s *Server) observeEdgeProxied(line string) {
	f := &s.flow
	ms := extractElapsedMs(line)
	connectorID := extractConnectorID(line)

	f.mu.Lock()
	f.edgeReq++
	f.dReq++
	f.edgeLatency = appendSample(f.edgeLatency, ms, 1024)
	if connectorID > 0 {
		ensureConnectorMaps(f)
		f.connectorReq[connectorID]++
		f.connectorLat[connectorID] = appendSample(f.connectorLat[connectorID], ms, 512)
	}
	f.tunnelUp, f.tunnelKnown = true, true // a proxied request proves the tunnel works
	f.mu.Unlock()
}

func (s *Server) observeEdgeOriginFailure(line string) {
	f := &s.flow
	status := extractStatus(line)
	connectorID := extractConnectorID(line)

	f.mu.Lock()
	f.edgeReq++
	f.dReq++
	if status == statusOriginTimeout {
		f.edge524++
		f.d524++
		f.tunnelUp, f.tunnelKnown = true, true // reached the origin, so the tunnel is up
	} else {
		f.edge502++
		f.d502++
	}
	if connectorID > 0 {
		if f.connectorErr == nil {
			f.connectorErr = map[int]int64{}
		}
		f.connectorErr[connectorID]++
	}
	f.mu.Unlock()
}

func (s *Server) observeEdgeNoConnector() {
	f := &s.flow
	f.mu.Lock()
	f.edgeReq++
	f.dReq++
	f.edge502++
	f.d502++
	f.tunnelUp, f.tunnelKnown, f.connectors = false, true, 0
	f.mu.Unlock()
}

func (s *Server) observeEdgeConnectorState(line string) {
	f := &s.flow
	n := int64(0)
	if m := connectorsRE.FindStringSubmatch(line); m != nil {
		n = atoi64(m[1])
	}
	f.mu.Lock()
	f.connectors = int(n)
	f.tunnelUp, f.tunnelKnown = n > 0, true
	f.mu.Unlock()
}

func extractElapsedMs(line string) int64 {
	if m := elapsedRE.FindStringSubmatch(line); m != nil {
		return atoi64(m[1])
	}
	return 0
}

func extractConnectorID(line string) int {
	if m := connectorIDRE.FindStringSubmatch(line); m != nil {
		return atoi(m[1])
	}
	return 0
}

func extractStatus(line string) int64 {
	if m := statusRE.FindStringSubmatch(line); m != nil {
		return atoi64(m[1])
	}
	return 0
}

func ensureConnectorMaps(f *flowState) {
	if f.connectorReq == nil {
		f.connectorReq = map[int]int64{}
	}
	if f.connectorLat == nil {
		f.connectorLat = map[int][]int64{}
	}
}

func (s *Server) observeBlastLine(line string) {
	if s.observeBlastJSONLine(line) {
		return
	}
	if s.observeBlastConnDrop(line) {
		return
	}
	s.observeBlastProtocolLine(line)
}

func (s *Server) observeBlastJSONLine(line string) bool {
	if len(line) == 0 || line[0] != '{' {
		return false
	}
	var ev struct {
		Event    string `json:"event"`
		Version  string `json:"quic_version"`
		Used0RTT bool   `json:"used_0rtt"`
	}
	if err := json.Unmarshal([]byte(line), &ev); err != nil {
		return false
	}
	s.applyBlastEvent(ev.Event, ev.Version, ev.Used0RTT)
	return true
}

func (s *Server) applyBlastEvent(eventName, version string, used0RTT bool) {
	f := &s.flow
	f.mu.Lock()
	defer f.mu.Unlock()

	switch eventName {
	case "conn_open":
		f.connOpened++
		f.connOpenedTotal++
		f.activeConns++
		if f.quicVersions == nil {
			f.quicVersions = map[string]int64{}
		}
		if version != "" {
			f.quicVersions[version]++
		}
		if used0RTT {
			f.zeroRTTAccept++
		} else {
			f.zeroRTTReject++
		}
	case "conn_closed":
		f.connClosed++
		f.connClosedTotal++
		if f.activeConns > 0 {
			f.activeConns--
		}
		f.connDropsTotal++
	}
}

func (s *Server) observeBlastConnDrop(line string) bool {
	m := connDropRE.FindStringSubmatch(strings.TrimSpace(line))
	if m == nil {
		return false
	}
	f := &s.flow
	f.mu.Lock()
	f.connDropsTotal = atoi64(m[1])
	f.mu.Unlock()
	return true
}

func (s *Server) observeBlastProtocolLine(line string) {
	m := protoLineRE.FindStringSubmatch(line)
	if m == nil {
		return
	}
	f := &s.flow
	f.mu.Lock()
	if f.protos == nil {
		f.protos = map[string]int64{}
	}
	f.protos[m[1]] = atoi64(m[2])
	f.mu.Unlock()
}

func appendSample(samples []int64, v int64, keep int) []int64 {
	samples = append(samples, v)
	if len(samples) > keep {
		samples = append([]int64{}, samples[len(samples)-keep:]...)
	}
	return samples
}

func percentileMs(samples []int64, p float64) int64 {
	if len(samples) == 0 {
		return 0
	}
	cpy := append([]int64{}, samples...)
	sort.Slice(cpy, func(i, j int) bool { return cpy[i] < cpy[j] })
	idx := int((p / 100.0) * float64(len(cpy)-1))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(cpy) {
		idx = len(cpy) - 1
	}
	return cpy[idx]
}

func avgMs(samples []int64) int64 {
	if len(samples) == 0 {
		return 0
	}
	var sum int64
	for _, n := range samples {
		sum += n
	}
	return sum / int64(len(samples))
}

// statusOriginTimeout mirrors the edge's non-standard 524.
const statusOriginTimeout = 524

func (s *Server) publishFlow() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for range t.C {
		f := &s.flow
		f.mu.Lock()
		p50 := percentileMs(f.edgeLatency, 50)
		p95 := percentileMs(f.edgeLatency, 95)
		p99 := percentileMs(f.edgeLatency, 99)
		avg := avgMs(f.edgeLatency)
		connectorReq := map[int]int64{}
		for id, n := range f.connectorReq {
			connectorReq[id] = n
		}
		connectorP95 := map[int]int64{}
		connectorP99 := map[int]int64{}
		for id, lats := range f.connectorLat {
			connectorP95[id] = percentileMs(lats, 95)
			connectorP99[id] = percentileMs(lats, 99)
		}
		connectorErr := map[int]int64{}
		for id, n := range f.connectorErr {
			connectorErr[id] = n
		}
		versions := map[string]int64{}
		for k, v := range f.quicVersions {
			versions[k] = v
		}
		protos := map[string]int64{}
		for k, v := range f.protos {
			protos[k] = v
		}
		health := s.healthSnapshot()
		msg := map[string]any{
			"t":      "flow",
			"client": map[string]any{"req": f.clientReq, "fail": f.clientFail},
			"edge": map[string]any{"rps": f.dReq, "s524": f.d524, "s502": f.d502,
				"total": f.edgeReq, "t524": f.edge524, "t502": f.edge502,
				"avgMs": avg, "p50Ms": p50, "p95Ms": p95, "p99Ms": p99},
			"tunnel": map[string]any{"up": f.tunnelUp, "n": f.connectors, "known": f.tunnelKnown,
				"connectorReq": connectorReq, "connectorP95": connectorP95, "connectorP99": connectorP99, "connectorErr": connectorErr},
			"origin": map[string]any{"rps": f.dReq - f.d502},
			"lifecycle": map[string]any{"active": f.activeConns, "openSec": f.connOpened, "closedSec": f.connClosed,
				"openTotal": f.connOpenedTotal, "closedTotal": f.connClosedTotal, "dropsTotal": f.connDropsTotal},
			"protocol": map[string]any{"versions": versions, "negotiated": protos,
				"zeroRTTAccept": f.zeroRTTAccept, "zeroRTTReject": f.zeroRTTReject},
			"health": health,
			"impair": f.impair,
		}
		f.dReq, f.d524, f.d502 = 0, 0, 0
		f.connOpened, f.connClosed = 0, 0
		f.edgeLatency = nil
		f.mu.Unlock()
		s.hub.publish(msg)
	}
}

func (s *Server) healthSnapshot() map[string]serviceHealth {
	s.healthMu.Lock()
	defer s.healthMu.Unlock()
	out := map[string]serviceHealth{}
	for k, v := range s.health {
		out[k] = v
	}
	return out
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
		cur, ok := s.readUDPCounters()
		if !ok {
			continue
		}
		if prev != (counters{}) {
			s.publishUDPDelta(prev, cur)
		}
		prev = cur
	}
}

func (s *Server) readUDPCounters() (struct{ in, out, rcv, snd uint64 }, bool) {
	type counters struct{ in, out, rcv, snd uint64 }
	cmd := exec.Command("docker", "compose", "exec", "-T", "edge", "cat", "/proc/net/snmp")
	cmd.Dir = s.dir
	out, err := cmd.Output()
	if err != nil {
		return counters{}, false
	}
	cur, ok := parseUDPSNMP(string(out))
	if !ok {
		return counters{}, false
	}
	return cur, true
}

func parseUDPSNMP(snmp string) (struct{ in, out, rcv, snd uint64 }, bool) {
	type counters struct{ in, out, rcv, snd uint64 }
	var hdr []string
	for _, line := range strings.Split(snmp, "\n") {
		if !strings.HasPrefix(line, "Udp:") {
			continue
		}
		fields := strings.Fields(line)
		if hdr == nil {
			hdr = fields
			continue
		}
		vals := map[string]uint64{}
		for i := 1; i < len(fields) && i < len(hdr); i++ {
			n, _ := strconv.ParseUint(fields[i], 10, 64)
			vals[hdr[i]] = n
		}
		return counters{vals["InDatagrams"], vals["OutDatagrams"], vals["RcvbufErrors"], vals["SndbufErrors"]}, true
	}
	return counters{}, false
}

func (s *Server) publishUDPDelta(prev, cur struct{ in, out, rcv, snd uint64 }) {
	s.hub.publish(map[string]any{"t": "udp",
		"in": cur.in - prev.in, "out": cur.out - prev.out,
		"rcvbuf": cur.rcv - prev.rcv, "sndbuf": cur.snd - prev.snd,
		"rcvbufTotal": cur.rcv, "sndbufTotal": cur.snd})
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

func (s *Server) pollServiceHealth() {
	for {
		time.Sleep(3 * time.Second)
		s.updateServiceHealth("edge")
		s.updateServiceHealth("connector")
		s.updateServiceHealth("origin")
	}
}

func (s *Server) updateServiceHealth(service string) {
	cmd := exec.Command("docker", "compose", "ps", "-q", "--status", "running", service)
	cmd.Dir = s.dir
	out, err := cmd.Output()
	if err != nil {
		return
	}
	ids := []string{}
	for _, line := range strings.Split(string(out), "\n") {
		id := strings.TrimSpace(line)
		if id != "" {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		s.setServiceHealth(service, serviceHealth{Replicas: 0, Updated: time.Now().Unix()})
		return
	}

	args := append([]string{"stats", "--no-stream", "--format", "{{.CPUPerc}}|{{.MemPerc}}"}, ids...)
	statsCmd := exec.Command("docker", args...)
	statsCmd.Dir = s.dir
	statsOut, err := statsCmd.Output()
	if err != nil {
		return
	}

	maxCPU := 0.0
	maxMem := 0.0
	for _, line := range strings.Split(string(statsOut), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Split(line, "|")
		if len(parts) < 2 {
			continue
		}
		cpu := parsePercent(parts[0])
		mem := parsePercent(parts[1])
		if cpu > maxCPU {
			maxCPU = cpu
		}
		if mem > maxMem {
			maxMem = mem
		}
	}

	s.setServiceHealth(service, serviceHealth{CPU: maxCPU, Mem: maxMem, Replicas: len(ids), Updated: time.Now().Unix()})
}

func (s *Server) setServiceHealth(service string, health serviceHealth) {
	s.healthMu.Lock()
	defer s.healthMu.Unlock()
	if s.health == nil {
		s.health = map[string]serviceHealth{}
	}
	s.health[service] = health
}

func parsePercent(s string) float64 {
	s = strings.TrimSpace(strings.TrimSuffix(s, "%"))
	v, _ := strconv.ParseFloat(s, 64)
	return v
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
	s.flow.edgeLatency = nil
	s.flow.connectorReq = map[int]int64{}
	s.flow.connectorLat = map[int][]int64{}
	s.flow.connectorErr = map[int]int64{}
	s.flow.connectorP95 = map[int]int64{}
	s.flow.connectorP99 = map[int]int64{}
	s.flow.connOpened, s.flow.connClosed = 0, 0
	s.flow.connOpenedTotal, s.flow.connClosedTotal, s.flow.connDropsTotal = 0, 0, 0
	s.flow.activeConns = 0
	s.flow.quicVersions = map[string]int64{}
	s.flow.protos = map[string]int64{}
	s.flow.zeroRTTAccept, s.flow.zeroRTTReject = 0, 0
}

// ---------------------------------------------------------------- API handlers

type stackReq struct {
	Action          string `json:"action"`
	OriginTimeout   string `json:"originTimeout"`
	MaxIdle         string `json:"maxIdle"`
	KeepAlive       string `json:"keepAlive"`
	TunnelMaxIdle   string `json:"tunnelMaxIdle"`
	TunnelKeepAlive string `json:"tunnelKeepAlive"`
	TunnelLB        string `json:"tunnelLB"`
	Connectors      int    `json:"connectors"`
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
		"EDGE_ORIGIN_TIMEOUT":   req.OriginTimeout,
		"EDGE_MAX_IDLE":         req.MaxIdle,
		"EDGE_KEEPALIVE":        req.KeepAlive,
		"EDGE_TUNNEL_MAX_IDLE":  req.TunnelMaxIdle,
		"EDGE_TUNNEL_KEEPALIVE": req.TunnelKeepAlive,
	} {
		if v != "" {
			if !durationRE.MatchString(v) {
				return nil, nil, fmt.Errorf("%s: expected a duration like 10s", k)
			}
			env = append(env, k+"="+v)
		}
	}
	if req.TunnelLB != "" {
		if req.TunnelLB != "rr" && req.TunnelLB != "hash" {
			return nil, nil, errors.New("tunnelLB must be rr or hash")
		}
		env = append(env, "EDGE_TUNNEL_LB="+req.TunnelLB)
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
	durationRE     = regexp.MustCompile(`^[0-9]+(ms|s|m)$`)
	zeroDurationRE = regexp.MustCompile(`^0+(ms|s|m)$`)
	netemArgRE     = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?(%|ms|s|kbit|mbit|gbit)$`)
	services       = map[string]bool{"edge": true, "connector": true, "origin": true}
	netemModes     = map[string]bool{"loss": true, "delay": true, "rate": true, "reorder": true, "clear": true}
)

type impairReq struct {
	Service string `json:"service"`
	Mode    string `json:"mode"`
	A       string `json:"a"`
	B       string `json:"b"`
	Burst   string `json:"burst"`
}

func (s *Server) handleImpair(w http.ResponseWriter, r *http.Request) {
	var req impairReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpErr(w, err)
		return
	}
	tc, err := buildImpairCommand(req)
	if err != nil {
		httpErr(w, err)
		return
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

func buildImpairCommand(req impairReq) ([]string, error) {
	if !services[req.Service] || !netemModes[req.Mode] {
		return nil, errors.New("unknown service or mode")
	}
	tc := []string{"compose", "exec", "-T", req.Service, "tc", "qdisc"}
	if req.Mode == "clear" {
		return append(tc, "del", "dev", "eth0", "root"), nil
	}
	if err := validateNetemInput(req); err != nil {
		return nil, err
	}
	tc = append(tc, "replace", "dev", "eth0", "root", "netem")
	return append(tc, netemModeArgs(req)...), nil
}

func validateNetemInput(req impairReq) error {
	for _, v := range []string{req.A, req.B, req.Burst} {
		if v != "" && !netemArgRE.MatchString(v) {
			return fmt.Errorf("bad netem value %q", v)
		}
	}
	return nil
}

func netemModeArgs(req impairReq) []string {
	switch req.Mode {
	case "loss":
		args := []string{"loss", req.A}
		if req.Burst != "" {
			args = append(args, req.Burst)
		}
		return args
	case "rate":
		return []string{"rate", req.A, "limit", "100"}
	case "delay":
		args := []string{"delay", req.A}
		if req.B != "" {
			args = append(args, req.B, "distribution", "normal")
		}
		return args
	case "reorder":
		args := []string{"delay", "10ms", "reorder", req.A}
		if req.B != "" {
			args = append(args, req.B)
		}
		return args
	default:
		return nil
	}
}

type runReq struct {
	Tool        string   `json:"tool"` // blast | probe | curl | chrome
	Runner      string   `json:"runner"`
	URL         string   `json:"url"`
	Conns       int      `json:"conns"`
	Workers     int      `json:"workers"`
	Duration    string   `json:"duration"`
	Timeout     string   `json:"timeout"`
	KeepAlive   string   `json:"keepAlive"`
	MaxIdle     string   `json:"maxIdle"`
	RPS         float64  `json:"rps"`
	ReadBody    bool     `json:"readBody"`
	Insecure    bool     `json:"insecure"`
	LocalCerts  bool     `json:"localCerts"`
	Headers     []string `json:"headers"`
	Mode        string   `json:"mode"`
	Warmup      string   `json:"warmup"`
	MaxInflight int      `json:"maxInflight"`
	Method      string   `json:"method"`
	Body        string   `json:"body"`
	URLs        string   `json:"urls"`
	Probe0RTT   bool     `json:"probe0rtt"`
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
	if err := validateRunRequest(req); err != nil {
		httpErr(w, err)
		return
	}
	bin, argv, err := planRun(s.dir, req, target)
	if err != nil {
		httpErr(w, err)
		return
	}
	s.startLogged(w, req.Tool, bin, argv)
}

func planRun(dir string, req runReq, target string) (string, []string, error) {
	if req.Tool == "curl" || req.Tool == "chrome" {
		script := filepath.Join(dir, "scripts", "h3-clients.sh")
		argv := []string{req.Tool, target}
		if req.Insecure || req.LocalCerts {
			argv = append(argv, "--insecure")
		}
		return script, argv, nil
	}
	toolArgs, err := buildRunArgs(req, target)
	if err != nil {
		return "", nil, err
	}
	return resolveRunCommand(req.Runner, toolArgs)
}

func (s *Server) startLogged(w http.ResponseWriter, src, bin string, argv []string) {
	err := s.start(src, func(ctx context.Context) {
		s.log("ui", "$ "+bin+" "+strings.Join(argv, " "))
		if err := s.runStreaming(ctx, src, nil, bin, argv...); err != nil {
			s.log(src, "exit: "+err.Error())
		}
	})
	if err != nil {
		httpErr(w, err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func validateRunRequest(req runReq) error {
	switch req.Tool {
	case "blast", "probe", "curl", "chrome":
	default:
		return errors.New("tool must be blast, probe, curl, or chrome")
	}
	for _, h := range req.Headers {
		if h == "" {
			continue
		}
		if !strings.Contains(h, ":") || strings.ContainsAny(h, "\r\n") {
			return fmt.Errorf("bad header %q", h)
		}
	}
	return nil
}

func buildRunArgs(req runReq, target string) ([]string, error) {
	toolArgs := []string{req.Tool, "-url=" + target}
	toolArgs = appendRunHeaders(toolArgs, req.Headers)
	if req.Insecure {
		toolArgs = append(toolArgs, "-insecure")
	}
	if req.LocalCerts {
		toolArgs = append(toolArgs, "-certs=/certs", "-server-name=edge")
	}
	if req.Tool != "blast" {
		return toolArgs, nil
	}
	return appendBlastArgs(toolArgs, req)
}

func appendRunHeaders(args []string, headers []string) []string {
	for _, h := range headers {
		if h != "" {
			args = append(args, "-header="+h)
		}
	}
	return args
}

func appendBlastArgs(args []string, req runReq) ([]string, error) {
	args, err := appendDurationFlags(args, req)
	if err != nil {
		return nil, err
	}
	args = append(args,
		"-conns="+strconv.Itoa(clamp(req.Conns, 1, 64)),
		"-workers="+strconv.Itoa(clamp(req.Workers, 1, 256)),
		"-stats-every=5s",
		"-read-body="+strconv.FormatBool(req.ReadBody),
	)
	return appendOptionalBlastFlags(args, req)
}

func appendDurationFlags(args []string, req runReq) ([]string, error) {
	pairs := [][2]string{
		{"-duration=", req.Duration},
		{"-timeout=", req.Timeout},
		{"-keepalive=", req.KeepAlive},
		{"-max-idle-timeout=", req.MaxIdle},
	}
	for _, kv := range pairs {
		if kv[1] == "" {
			continue
		}
		if !durationRE.MatchString(kv[1]) {
			return nil, fmt.Errorf("%s expected a duration like 30s", kv[0])
		}
		args = append(args, kv[0]+kv[1])
	}
	return appendWarmupFlag(args, req.Warmup)
}

func appendWarmupFlag(args []string, warmup string) ([]string, error) {
	if warmup == "" || zeroDuration(warmup) {
		return args, nil
	}
	if !durationRE.MatchString(warmup) {
		return nil, fmt.Errorf("-warmup= expected a duration like 2s")
	}
	return append(args, "-warmup="+warmup), nil
}

func appendOptionalBlastFlags(args []string, req runReq) ([]string, error) {
	if req.RPS > 0 {
		args = append(args, "-rps="+strconv.FormatFloat(req.RPS, 'f', -1, 64))
	}
	if req.Mode == "open" {
		args = append(args, "-mode=open")
	}
	if req.MaxInflight > 0 {
		args = append(args, "-max-inflight="+strconv.Itoa(clamp(req.MaxInflight, 1, 4096)))
	}
	if req.Method != "" && !strings.EqualFold(req.Method, http.MethodGet) {
		args = append(args, "-method="+req.Method)
	}
	if req.Body != "" {
		args = append(args, "-body="+req.Body)
	}
	if req.URLs != "" {
		args = append(args, "-urls="+req.URLs)
	}
	if req.Probe0RTT {
		args = append(args, "-probe-0rtt")
	}
	return args, nil
}

func zeroDuration(s string) bool {
	return zeroDurationRE.MatchString(s)
}

func resolveRunCommand(runner string, toolArgs []string) (string, []string, error) {
	if runner == "host" {
		self, err := os.Executable()
		if err != nil {
			return "", nil, err
		}
		return self, toolArgs, nil
	}
	return "docker", append([]string{"compose", "run", "--build", "--rm", "-T", "blast"}, toolArgs...), nil
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
