// Package blast is the HTTP/3 load harness. Its job is not throughput numbers —
// it is to keep hammering an h3 endpoint until connections break, and to record
// exactly how they broke.
package blast

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"

	"github.com/conrad/quicshot/internal/certs"
	"github.com/conrad/quicshot/internal/qlogtrace"
	"github.com/conrad/quicshot/internal/quicerr"
	"github.com/conrad/quicshot/internal/udpsock"
)

type event struct {
	Time      string `json:"time"`
	Event     string `json:"event"`
	Conn      int    `json:"conn"`
	Worker    int    `json:"worker,omitempty"`
	URL       string `json:"url,omitempty"`
	Status    int    `json:"status,omitempty"`
	Bytes     int64  `json:"bytes,omitempty"`
	LatencyMs int64  `json:"latency_ms,omitempty"`
	Kind      string `json:"kind,omitempty"`
	Detail    string `json:"detail,omitempty"`
	Local     string `json:"local_addr,omitempty"`
	Remote    string `json:"remote_addr,omitempty"`
	Version   string `json:"quic_version,omitempty"`
	Used0RTT  bool   `json:"used_0rtt,omitempty"`
}

type recorder struct {
	mu        sync.Mutex
	enc       *json.Encoder
	lat       *latencyHist
	status    map[int]int
	kinds     map[string]int
	protos    map[string]int // negotiated application protocol per response
	cfCodes   map[int]int    // Cloudflare "error code: N" values seen in 5xx bodies
	connDrops int
	requests  atomic.Int64
	failures  atomic.Int64
	abandoned atomic.Int64 // in flight when the run ended: neither success nor failure
	omitted   atomic.Int64 // open-loop ticks dropped because max-inflight was full
	offered   atomic.Int64
	bytes     atomic.Int64
}

// headerList collects repeated -header flags.
type headerList []string

func (h *headerList) String() string     { return strings.Join(*h, ", ") }
func (h *headerList) Set(v string) error { *h = append(*h, v); return nil }

// reqSpec is the fixed shape of every request in a run.
type reqSpec struct {
	url         string
	urls        []string
	n           atomic.Uint64
	method      string
	hostHdr     string
	headers     headerList
	timeout     time.Duration
	readBody    bool
	body        []byte
	contentType string
}

func (s *reqSpec) nextURL() string {
	if len(s.urls) > 0 {
		if len(s.urls) == 1 {
			return s.urls[0]
		}
		i := s.n.Add(1) - 1
		return s.urls[i%uint64(len(s.urls))]
	}
	return s.url
}

type config struct {
	target      string
	urls        string
	urlsFile    string
	targets     []string
	conns       int
	workers     int
	duration    time.Duration
	warmup      time.Duration
	requests    int64
	timeout     time.Duration
	maxIdle     time.Duration
	keepAlive   time.Duration
	recvBuf     int
	certDir     string
	serverName  string
	insecure    bool
	logPath     string
	statsEvery  time.Duration
	readBody    bool
	method      string
	hostHdr     string
	headers     headerList
	bodyStr     string
	bodyFile    string
	body        []byte
	contentType string
	rps         float64
	mode        string
	maxInflight int
	probe0RTT   bool
	maxFailPct  float64
	maxDrops    int
	qlogDir     string
}

var cfErrorRE = regexp.MustCompile(`error code: (\d+)`)

func newRecorder(w io.Writer) *recorder {
	return &recorder{
		enc:     json.NewEncoder(w),
		lat:     newLatencyHist(200000),
		status:  map[int]int{},
		kinds:   map[string]int{},
		protos:  map[string]int{},
		cfCodes: map[int]int{},
	}
}

func (r *recorder) emit(e event) {
	e.Time = time.Now().UTC().Format(time.RFC3339Nano)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.enc.Encode(e)
}

func (r *recorder) success(e event, d time.Duration) {
	r.requests.Add(1)
	r.bytes.Add(e.Bytes)
	r.mu.Lock()
	r.lat.add(d)
	r.status[e.Status]++
	r.mu.Unlock()
}

func (r *recorder) resetRequestStats() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lat = newLatencyHist(200000)
	r.status = map[int]int{}
	r.kinds = map[string]int{}
	r.protos = map[string]int{}
	r.cfCodes = map[int]int{}
	r.connDrops = 0
	r.requests.Store(0)
	r.failures.Store(0)
	r.abandoned.Store(0)
	r.omitted.Store(0)
	r.offered.Store(0)
	r.bytes.Store(0)
}

func (r *recorder) failure(e event) {
	r.requests.Add(1)
	r.failures.Add(1)
	r.mu.Lock()
	r.kinds[e.Kind]++
	if e.Status != 0 {
		r.status[e.Status]++
	}
	r.mu.Unlock()
	r.emit(e)
}

func parseConfig(args []string) (config, error) {
	cfg := config{}
	fs := flag.NewFlagSet("blast", flag.ExitOnError)
	fs.StringVar(&cfg.target, "url", "https://edge:8443/fast", "target URL (must be reachable over HTTP/3)")
	fs.StringVar(&cfg.urls, "urls", "", "comma-separated extra URLs, mixed round-robin with -url")
	fs.StringVar(&cfg.urlsFile, "urls-file", "", "file of extra URLs, one per line")
	fs.IntVar(&cfg.conns, "conns", 4, "number of independent QUIC connections")
	fs.IntVar(&cfg.workers, "workers", 8, "concurrent requests per QUIC connection (closed-loop)")
	fs.DurationVar(&cfg.duration, "duration", 30*time.Second, "measured run length after -warmup (0 = until -requests is met)")
	fs.DurationVar(&cfg.warmup, "warmup", 0, "traffic sent before measurement starts; discarded from the summary")
	fs.Int64Var(&cfg.requests, "requests", 0, "stop after this many requests (0 = unlimited)")
	fs.DurationVar(&cfg.timeout, "timeout", 30*time.Second, "per-request timeout")
	fs.DurationVar(&cfg.maxIdle, "max-idle-timeout", 30*time.Second, "QUIC max idle timeout")
	fs.DurationVar(&cfg.keepAlive, "keepalive", 0, "QUIC keep-alive period (0 disables; try 5s to survive idle NAT/UDP gaps)")
	fs.IntVar(&cfg.recvBuf, "udp-recv-buffer", 0, "SO_RCVBUF per client socket, 0 = leave to quic-go")
	fs.StringVar(&cfg.certDir, "certs", "", "directory holding ca.pem for a private CA; empty = system trust store")
	fs.StringVar(&cfg.serverName, "server-name", "", "TLS SNI override; empty = host from -url")
	fs.BoolVar(&cfg.insecure, "insecure", false, "skip certificate verification")
	fs.StringVar(&cfg.logPath, "log", "", "append JSONL events to this file (default stdout)")
	fs.DurationVar(&cfg.statsEvery, "stats-every", 5*time.Second, "interval for progress lines on stderr")
	fs.BoolVar(&cfg.readBody, "read-body", true, "drain response bodies (disable to force stream resets)")
	fs.StringVar(&cfg.method, "method", http.MethodGet, "HTTP method")
	fs.StringVar(&cfg.hostHdr, "host", "", "override the Host header")
	fs.Var(&cfg.headers, "header", "extra request header 'Key: Value' (repeatable)")
	fs.StringVar(&cfg.bodyStr, "body", "", "request body string (for POST/PUT)")
	fs.StringVar(&cfg.bodyFile, "body-file", "", "read request body from this file")
	fs.StringVar(&cfg.contentType, "content-type", "", "Content-Type for -body / -body-file")
	fs.Float64Var(&cfg.rps, "rps", 0, "offered request rate across all workers (0 = unlimited; required for -mode=open)")
	fs.StringVar(&cfg.mode, "mode", "closed", "load mode: closed (workers wait for responses) or open (token bucket, independent of latency)")
	fs.IntVar(&cfg.maxInflight, "max-inflight", 0, "cap on in-flight requests in open mode (0 = conns*workers*8, max 4096)")
	fs.BoolVar(&cfg.probe0RTT, "probe-0rtt", false, "dial twice before the run and record whether the second handshake used 0-RTT")
	fs.Float64Var(&cfg.maxFailPct, "max-failure-pct", -1, "exit non-zero if the failure rate exceeds this percentage")
	fs.IntVar(&cfg.maxDrops, "max-conn-drops", -1, "exit non-zero if QUIC connection drops exceed this count")
	fs.StringVar(&cfg.qlogDir, "qlog-dir", "", "write QUIC qlog traces to this directory (empty disables)")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func Main(args []string) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}

	if _, err := url.Parse(cfg.target); err != nil {
		return fmt.Errorf("bad -url: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	if err := cfg.loadPayload(); err != nil {
		return err
	}

	out, closeOut, err := outputWriter(cfg.logPath)
	if err != nil {
		return err
	}
	defer closeOut()
	rec := newRecorder(out)

	tlsConf, err := tlsConfig(cfg)
	if err != nil {
		return err
	}
	quicConf, err := quicConfig(cfg)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	transports := makeTransports(cfg, tlsConf, quicConf, rec)
	defer closeTransports(transports)

	if cfg.probe0RTT {
		if err := probe0RTT(ctx, cfg, tlsConf, quicConf, rec); err != nil {
			rec.emit(event{Event: "0rtt_probe", Kind: "error", Detail: err.Error()})
		}
	}

	spec := cfg.spec()
	go progress(ctx, rec, cfg.statsEvery)

	if cfg.warmup > 0 {
		wctx, cancel := context.WithTimeout(ctx, cfg.warmup)
		runLoad(wctx, cfg, transports, rec, spec)
		cancel()
		rec.resetRequestStats()
	}

	runCtx := ctx
	if cfg.duration > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, cfg.duration)
		defer cancel()
	}
	start := time.Now()
	runLoad(runCtx, cfg, transports, rec, spec)

	rec.summary(os.Stderr, time.Since(start))
	return rec.verdict(cfg.maxFailPct, cfg.maxDrops)
}

func (c config) validate() error {
	if c.mode != "closed" && c.mode != "open" {
		return fmt.Errorf("-mode must be closed or open")
	}
	if c.mode == "open" && c.rps <= 0 {
		return fmt.Errorf("-mode=open requires -rps > 0 so offered load does not depend on latency")
	}
	if c.conns < 1 || c.workers < 1 {
		return fmt.Errorf("-conns and -workers must be >= 1")
	}
	return nil
}

func (c *config) loadPayload() error {
	if c.bodyFile != "" {
		b, err := os.ReadFile(c.bodyFile)
		if err != nil {
			return fmt.Errorf("read -body-file: %w", err)
		}
		c.body = b
	} else if c.bodyStr != "" {
		c.body = []byte(c.bodyStr)
	}

	c.targets = []string{c.target}
	if c.urls != "" {
		for _, u := range strings.Split(c.urls, ",") {
			u = strings.TrimSpace(u)
			if u != "" {
				c.targets = append(c.targets, u)
			}
		}
	}
	if c.urlsFile != "" {
		raw, err := os.ReadFile(c.urlsFile)
		if err != nil {
			return fmt.Errorf("read -urls-file: %w", err)
		}
		for _, line := range strings.Split(string(raw), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			c.targets = append(c.targets, line)
		}
	}
	return nil
}

func (c config) spec() *reqSpec {
	return &reqSpec{
		url: c.target, urls: c.targets, method: c.method, hostHdr: c.hostHdr,
		headers: c.headers, timeout: c.timeout, readBody: c.readBody,
		body: c.body, contentType: c.contentType,
	}
}

func (c config) inflight() int {
	if c.maxInflight > 0 {
		if c.maxInflight > 4096 {
			return 4096
		}
		return c.maxInflight
	}
	n := c.conns * c.workers
	if c.mode == "open" {
		n *= 8
	}
	if n < 1 {
		return 1
	}
	if n > 4096 {
		return 4096
	}
	return n
}

func outputWriter(path string) (io.Writer, func() error, error) {
	if path == "" {
		return os.Stdout, func() error { return nil }, nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, err
	}
	return f, f.Close, nil
}

func tlsConfig(cfg config) (*tls.Config, error) {
	tlsConf := &tls.Config{
		ServerName:         cfg.serverName,
		InsecureSkipVerify: cfg.insecure,
		NextProtos:         []string{http3.NextProtoH3},
		MinVersion:         tls.VersionTLS13,
		ClientSessionCache: tls.NewLRUClientSessionCache(128),
	}
	if cfg.certDir == "" || cfg.insecure {
		return tlsConf, nil
	}
	pool, err := certs.LoadCAPool(cfg.certDir)
	if err != nil {
		return nil, err
	}
	tlsConf.RootCAs = pool
	return tlsConf, nil
}

func quicConfig(cfg config) (*quic.Config, error) {
	quicConf := &quic.Config{MaxIdleTimeout: cfg.maxIdle, KeepAlivePeriod: cfg.keepAlive}
	if err := qlogtrace.Configure(quicConf, cfg.qlogDir); err != nil {
		return nil, err
	}
	return quicConf, nil
}

func makeTransports(cfg config, tlsConf *tls.Config, quicConf *quic.Config, rec *recorder) []*http3.Transport {
	transports := make([]*http3.Transport, cfg.conns)
	for i := range transports {
		idx := i
		transports[i] = &http3.Transport{
			TLSClientConfig: tlsConf,
			QUICConfig:      quicConf,
			Dial: func(dctx context.Context, addr string, tc *tls.Config, qc *quic.Config) (*quic.Conn, error) {
				// Each connection gets its own UDP socket so per-socket buffer
				// limits and kernel drops can be attributed to one connection.
				udp, err := udpsock.Listen(":0", cfg.recvBuf, 0)
				if err != nil {
					return nil, err
				}
				tr := &quic.Transport{Conn: udp}
				raddr, err := resolveUDP(addr)
				if err != nil {
					udp.Close()
					return nil, err
				}
				conn, err := tr.DialEarly(dctx, raddr, tc, qc)
				if err != nil {
					kind, detail := quicerr.Classify(err)
					rec.emit(event{Event: "conn_dial_failed", Conn: idx, Kind: string(kind), Detail: detail})
					udp.Close()
					return nil, err
				}
				state := conn.ConnectionState()
				rec.emit(event{
					Event: "conn_open", Conn: idx,
					Local: conn.LocalAddr().String(), Remote: conn.RemoteAddr().String(),
					Version: fmt.Sprintf("0x%x", uint32(state.Version)), Used0RTT: state.Used0RTT,
				})
				go func() {
					<-conn.Context().Done()
					kind, detail := quicerr.CloseReason(conn.Context())
					stats := conn.ConnectionStats()
					rec.mu.Lock()
					rec.connDrops++
					rec.kinds["conn_closed:"+string(kind)]++
					rec.mu.Unlock()
					rec.emit(event{
						Event: "conn_closed", Conn: idx, Kind: string(kind),
						Detail: fmt.Sprintf("%s | packets_sent=%d packets_lost=%d bytes_lost=%d smoothed_rtt=%s",
							detail, stats.PacketsSent, stats.PacketsLost, stats.BytesLost, stats.SmoothedRTT),
						Local: conn.LocalAddr().String(), Remote: conn.RemoteAddr().String(),
					})
					tr.Close()
					udp.Close()
				}()
				return conn, nil
			},
		}
	}
	return transports
}

func closeTransports(transports []*http3.Transport) {
	for _, t := range transports {
		t.Close()
	}
}

// verdict turns the run into a pass/fail signal so this can gate a pipeline.
func (r *recorder) verdict(maxFailPct float64, maxDrops int) error {
	total := r.requests.Load()
	fails := r.failures.Load()
	if maxFailPct >= 0 && total > 0 {
		pct := 100 * float64(fails) / float64(total)
		if pct > maxFailPct {
			return fmt.Errorf("failure rate %.2f%% exceeds -max-failure-pct %.2f%%", pct, maxFailPct)
		}
	}
	if maxDrops >= 0 {
		r.mu.Lock()
		drops := r.connDrops
		r.mu.Unlock()
		if drops > maxDrops {
			return fmt.Errorf("%d connection drops exceed -max-conn-drops %d", drops, maxDrops)
		}
	}
	return nil
}

func doRequest(ctx context.Context, rec *recorder, client *http.Client, spec *reqSpec,
	connIdx, workerIdx int) {

	reqCtx, cancel := context.WithTimeout(ctx, spec.timeout)
	defer cancel()

	target := spec.nextURL()
	req, err := spec.newRequest(reqCtx, target)
	if err != nil {
		return
	}

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		recordDialError(ctx, rec, err, connIdx, workerIdx, target, start)
		return
	}

	rec.recordProto(resp.Proto)
	n, cfCode, err := drainResponse(resp, spec.readBody)
	resp.Body.Close()
	elapsed := time.Since(start)

	recordHTTPResult(ctx, rec, httpResult{
		err: err, cfCode: cfCode, elapsed: elapsed, ray: resp.Header.Get("cf-ray"),
		event: event{Conn: connIdx, Worker: workerIdx, URL: target,
			Status: resp.StatusCode, Bytes: n, LatencyMs: elapsed.Milliseconds()},
	})
}

func (s *reqSpec) newRequest(ctx context.Context, target string) (*http.Request, error) {
	var body io.Reader
	if len(s.body) > 0 {
		body = bytes.NewReader(s.body)
	}
	req, err := http.NewRequestWithContext(ctx, s.method, target, body)
	if err != nil {
		return nil, err
	}
	s.applyHeaders(req)
	return req, nil
}

func (s *reqSpec) applyHeaders(req *http.Request) {
	if len(s.body) > 0 {
		req.ContentLength = int64(len(s.body))
		if s.contentType != "" {
			req.Header.Set("Content-Type", s.contentType)
		}
	}
	for _, h := range s.headers {
		if k, v, ok := strings.Cut(h, ":"); ok {
			req.Header.Add(strings.TrimSpace(k), strings.TrimSpace(v))
		}
	}
	if s.hostHdr != "" {
		req.Host = s.hostHdr
	}
}

func drainResponse(resp *http.Response, readBody bool) (n int64, cfCode int, err error) {
	// Cloudflare puts the real reason in the body ("error code: 524"), not a header,
	// so 5xx bodies are read even when -read-body is off.
	if resp.StatusCode >= 500 {
		return drainErrorBody(resp)
	}
	if readBody {
		n, err = io.Copy(io.Discard, resp.Body)
	}
	return n, 0, err
}

func drainErrorBody(resp *http.Response) (int64, int, error) {
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	io.Copy(io.Discard, resp.Body)
	cfCode := 0
	if m := cfErrorRE.FindSubmatch(snippet); m != nil {
		cfCode, _ = strconv.Atoi(string(m[1]))
	}
	return int64(len(snippet)), cfCode, nil
}

func recordDialError(ctx context.Context, rec *recorder, err error, connIdx, workerIdx int, url string, start time.Time) {
	if ctx.Err() != nil { // shutting down, not a real failure
		rec.abandoned.Add(1)
		return
	}
	kind, detail := quicerr.Classify(err)
	rec.failure(event{
		Event: "request_failed", Conn: connIdx, Worker: workerIdx, URL: url,
		Kind: string(kind), Detail: detail, LatencyMs: time.Since(start).Milliseconds(),
	})
}

type httpResult struct {
	err     error
	cfCode  int
	elapsed time.Duration
	ray     string
	event   event
}

func recordHTTPResult(ctx context.Context, rec *recorder, res httpResult) {
	e := res.event
	if res.err != nil {
		if ctx.Err() != nil { // run ended mid-body; not a real truncation
			rec.abandoned.Add(1)
			return
		}
		kind, detail := quicerr.Classify(res.err)
		e.Event = "body_truncated"
		e.Kind = string(kind)
		e.Detail = detail
		rec.failure(e)
		return
	}
	if e.Status >= 500 {
		e.Event = "request_5xx"
		e.Kind = fmt.Sprintf("http_%d", e.Status)
		e.Detail = fmt.Sprintf("cf_error_code=%d cf_ray=%s", res.cfCode, res.ray)
		if res.cfCode != 0 {
			rec.recordCFCode(res.cfCode)
		}
		rec.failure(e)
		return
	}
	e.Event = "request"
	rec.success(e, res.elapsed)
}

func (r *recorder) recordProto(proto string) {
	r.mu.Lock()
	r.protos[proto]++
	r.mu.Unlock()
}

func (r *recorder) recordCFCode(code int) {
	r.mu.Lock()
	r.cfCodes[code]++
	r.mu.Unlock()
}

func progress(ctx context.Context, rec *recorder, every time.Duration) {
	if every <= 0 {
		return
	}
	t := time.NewTicker(every)
	defer t.Stop()
	prevReq, prevFail := int64(0), int64(0)
	prevUDP, hasUDP := udpsock.ReadCounters()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			req, fail := rec.requests.Load(), rec.failures.Load()
			line := fmt.Sprintf("[blast] +%d req  +%d fail  total=%d fail=%d omit=%d",
				req-prevReq, fail-prevFail, req, fail, rec.omitted.Load())
			if hasUDP {
				cur, _ := udpsock.ReadCounters()
				d := cur.Sub(prevUDP)
				prevUDP = cur
				line += fmt.Sprintf("  udp_rcvbuf_errors=+%d in_errors=+%d", d.RcvbufErrors, d.InErrors)
			}
			prevReq, prevFail = req, fail
			fmt.Fprintln(os.Stderr, line)
		}
	}
}

func (r *recorder) summary(w io.Writer, elapsed time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	total := r.requests.Load()
	fmt.Fprintf(w, "\n=== blast summary =========================================\n")
	fmt.Fprintf(w, "duration        %s\n", elapsed.Round(time.Millisecond))
	sec := elapsed.Seconds()
	if sec <= 0 {
		sec = 1
	}
	fmt.Fprintf(w, "requests        %d (%.1f/s completed)\n", total, float64(total)/sec)
	if offered := r.offered.Load(); offered > 0 {
		fmt.Fprintf(w, "offered         %d (%.1f/s)\n", offered, float64(offered)/sec)
	}
	fmt.Fprintf(w, "failures        %d\n", r.failures.Load())
	if n := r.abandoned.Load(); n > 0 {
		fmt.Fprintf(w, "abandoned       %d (still in flight when the run ended)\n", n)
	}
	if n := r.omitted.Load(); n > 0 {
		fmt.Fprintf(w, "omitted         %d (open-loop ticks dropped: max-inflight full)\n", n)
	}
	if n := r.bytes.Load(); n > 0 {
		fmt.Fprintf(w, "bytes           %d (%.1f KB/s)\n", n, float64(n)/sec/1024)
	}
	fmt.Fprintf(w, "conn drops      %d\n", r.connDrops)

	writeIntHistogram(w, "status codes", r.status)
	writeProtoSummary(w, r.protos)
	writeCFCodeSummary(w, r.cfCodes)
	writeStringHistogram(w, "disconnect / failure reasons", r.kinds)
	writeLatencySummary(w, r.lat)
	fmt.Fprintf(w, "===========================================================\n")
}

func writeIntHistogram(w io.Writer, title string, values map[int]int) {
	if len(values) == 0 {
		return
	}
	fmt.Fprintf(w, "\n%s\n", title)
	keys := make([]int, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	for _, k := range keys {
		fmt.Fprintf(w, "  %-6d %d\n", k, values[k])
	}
}

func writeStringHistogram(w io.Writer, title string, values map[string]int) {
	if len(values) == 0 {
		return
	}
	fmt.Fprintf(w, "\n%s\n", title)
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(w, "  %-34s %d\n", k, values[k])
	}
}

func writeProtoSummary(w io.Writer, protos map[string]int) {
	if len(protos) == 0 {
		return
	}
	fmt.Fprintf(w, "\nnegotiated protocol\n")
	keys := make([]string, 0, len(protos))
	for k := range protos {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		note := ""
		if k != "HTTP/3.0" {
			note = "  <-- NOT HTTP/3"
		}
		fmt.Fprintf(w, "  %-10s %d%s\n", k, protos[k], note)
	}
}

func writeCFCodeSummary(w io.Writer, cfCodes map[int]int) {
	if len(cfCodes) == 0 {
		return
	}
	fmt.Fprintf(w, "\ncloudflare error codes\n")
	codes := make([]int, 0, len(cfCodes))
	for c := range cfCodes {
		codes = append(codes, c)
	}
	sort.Ints(codes)
	for _, c := range codes {
		fmt.Fprintf(w, "  %-6d %d  %s\n", c, cfCodes[c], cfMeaning(c))
	}
}

func writeLatencySummary(w io.Writer, hist *latencyHist) {
	if hist == nil || hist.len() == 0 {
		return
	}
	latencies := hist.sorted()
	fmt.Fprintf(w, "\nlatency (successful requests)\n")
	if hist.seen > len(latencies) {
		fmt.Fprintf(w, "  (percentiles from last %d of %d samples)\n", len(latencies), hist.seen)
	}
	for _, p := range []float64{50, 90, 99, 99.9} {
		fmt.Fprintf(w, "  p%-5.4g %s\n", p, percentile(latencies, p).Round(time.Millisecond))
	}
	fmt.Fprintf(w, "  max    %s\n", latencies[len(latencies)-1].Round(time.Millisecond))
}

func resolveUDP(addr string) (*net.UDPAddr, error) {
	return net.ResolveUDPAddr("udp", addr)
}

// cfMeaning translates the codes Cloudflare puts in its error bodies.
func cfMeaning(code int) string {
	switch code {
	case 520:
		return "origin returned an unknown/empty response"
	case 521:
		return "origin refused the connection (is it up? firewalled?)"
	case 522:
		return "connection to origin timed out"
	case 523:
		return "origin unreachable (DNS / routing)"
	case 524:
		return "origin connected but did not respond in time"
	case 525, 526:
		return "TLS handshake with origin failed"
	case 1033:
		return "tunnel not found / no connector registered"
	case 1014:
		return "CNAME cross-user banned"
	default:
		return ""
	}
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p / 100 * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
