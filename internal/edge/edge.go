// Package edge plays the Cloudflare edge: it terminates client HTTP/3, holds the
// QUIC tunnel that the connector dials in on, and owns the origin timeout that
// turns into a 524.
package edge

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
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

type config struct {
	publicAddr    string
	tunnelAddr    string
	certDir       string
	originTimeout time.Duration
	maxIdle       time.Duration
	keepAlive     time.Duration
	recvBuf       int
	sendBuf       int
	statsEvery    time.Duration
	qlogDir       string
}

type Edge struct {
	cfg config
	log *slog.Logger

	mu              sync.RWMutex
	connectors      []connectorRef
	rr              atomic.Uint64
	nextConnectorID atomic.Uint64
}

type connectorRef struct {
	id   uint64
	conn *quic.Conn
}

func Main(args []string) error {
	fs := flag.NewFlagSet("edge", flag.ExitOnError)
	var cfg config
	fs.StringVar(&cfg.publicAddr, "addr", ":8443", "public listen address (TCP for h1/h2, UDP for h3)")
	fs.StringVar(&cfg.tunnelAddr, "tunnel-addr", ":7844", "QUIC listen address for connectors")
	fs.StringVar(&cfg.certDir, "certs", "/certs", "directory holding server.pem / server.key")
	fs.DurationVar(&cfg.originTimeout, "origin-timeout", 10*time.Second, "no response from origin within this window yields a 524")
	fs.DurationVar(&cfg.maxIdle, "max-idle-timeout", 30*time.Second, "QUIC max idle timeout for client connections")
	fs.DurationVar(&cfg.keepAlive, "keepalive", 0, "QUIC keep-alive period for client connections (0 disables)")
	fs.IntVar(&cfg.recvBuf, "udp-recv-buffer", 0, "SO_RCVBUF for the public UDP socket, 0 = leave to quic-go")
	fs.IntVar(&cfg.sendBuf, "udp-send-buffer", 0, "SO_SNDBUF for the public UDP socket, 0 = leave to quic-go")
	fs.DurationVar(&cfg.statsEvery, "stats-every", 10*time.Second, "how often to log kernel UDP counters")
	fs.StringVar(&cfg.qlogDir, "qlog-dir", "", "write QUIC qlog traces to this directory (empty disables)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	e := &Edge{cfg: cfg, log: slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("role", "edge")}

	cert, err := tls.LoadX509KeyPair(cfg.certDir+"/"+certs.CertFile, cfg.certDir+"/"+certs.KeyFile)
	if err != nil {
		return fmt.Errorf("load key pair: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := e.serveTunnel(ctx, cert); err != nil {
		return err
	}
	go e.logUDPCounters(ctx)

	return e.servePublic(ctx, cert)
}

// ---------------------------------------------------------------- tunnel side

func (e *Edge) serveTunnel(ctx context.Context, cert tls.Certificate) error {
	tlsConf := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{certs.TunnelALPN},
		MinVersion:   tls.VersionTLS13,
	}
	// The tunnel hop deliberately uses a short idle timeout with keep-alives, the
	// same shape as cloudflared's edge connection.
	quicConf := &quic.Config{
		MaxIdleTimeout:     30 * time.Second,
		KeepAlivePeriod:    5 * time.Second,
		MaxIncomingStreams: 1 << 14,
	}
	if err := qlogtrace.Configure(quicConf, e.cfg.qlogDir); err != nil {
		return err
	}
	ln, err := quic.ListenAddr(e.cfg.tunnelAddr, tlsConf, quicConf)
	if err != nil {
		return fmt.Errorf("listen for connectors: %w", err)
	}
	e.log.Info("tunnel listener up", "addr", e.cfg.tunnelAddr, "alpn", certs.TunnelALPN)

	go func() {
		for {
			conn, err := ln.Accept(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				e.log.Error("accept connector", "err", err.Error())
				continue
			}
			e.addConnector(conn)
		}
	}()
	return nil
}

func (e *Edge) addConnector(conn *quic.Conn) {
	id := e.nextConnectorID.Add(1)
	e.mu.Lock()
	e.connectors = append(e.connectors, connectorRef{id: id, conn: conn})
	n := len(e.connectors)
	e.mu.Unlock()
	e.log.Info("connector registered", "connector_id", id, "remote", conn.RemoteAddr().String(), "connectors", n)

	go func() {
		<-conn.Context().Done()
		kind, detail := quicerr.CloseReason(conn.Context())
		e.mu.Lock()
		for i, c := range e.connectors {
			if c.conn == conn {
				e.connectors = append(e.connectors[:i], e.connectors[i+1:]...)
				break
			}
		}
		n := len(e.connectors)
		e.mu.Unlock()
		e.log.Warn("connector lost", "connector_id", id, "remote", conn.RemoteAddr().String(), "connectors", n,
			"close_kind", string(kind), "close_detail", detail)
	}()
}

func (e *Edge) pickConnector() (connectorRef, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if len(e.connectors) == 0 {
		return connectorRef{}, false
	}
	i := (e.rr.Add(1) - 1) % uint64(len(e.connectors))
	return e.connectors[i], true
}

// ---------------------------------------------------------------- public side

func (e *Edge) servePublic(ctx context.Context, cert tls.Certificate) error {
	quicConf := &quic.Config{
		MaxIdleTimeout:  e.cfg.maxIdle,
		KeepAlivePeriod: e.cfg.keepAlive,
		Allow0RTT:       true,
	}
	if err := qlogtrace.Configure(quicConf, e.cfg.qlogDir); err != nil {
		return err
	}

	udpConn, err := udpsock.Listen(e.cfg.publicAddr, e.cfg.recvBuf, e.cfg.sendBuf)
	if err != nil {
		return fmt.Errorf("public udp socket: %w", err)
	}
	defer udpConn.Close()

	h3 := &http3.Server{
		Addr:       e.cfg.publicAddr,
		Handler:    e,
		TLSConfig:  http3.ConfigureTLSConfig(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}),
		QUICConfig: quicConf,
		Logger:     e.log.With("component", "http3"),
	}

	tcpSrv := &http.Server{
		Addr: e.cfg.publicAddr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h3.SetQUICHeaders(w.Header())
			e.ServeHTTP(w, r)
		}),
		TLSConfig:         &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
		ReadHeaderTimeout: 15 * time.Second,
	}

	errCh := make(chan error, 2)
	go func() {
		e.log.Info("h3 listener up", "addr", e.cfg.publicAddr,
			"origin_timeout", e.cfg.originTimeout.String(),
			"max_idle_timeout", e.cfg.maxIdle.String(),
			"keepalive", e.cfg.keepAlive.String(),
			"udp_recv_buffer", e.cfg.recvBuf,
			"qlog_dir", e.cfg.qlogDir)
		errCh <- h3.Serve(udpConn)
	}()
	go func() {
		e.log.Info("tcp tls listener up", "addr", e.cfg.publicAddr)
		errCh <- tcpSrv.ListenAndServeTLS("", "")
	}()

	select {
	case <-ctx.Done():
		h3.Close()
		tcpSrv.Close()
		return nil
	case err := <-errCh:
		return err
	}
}

func (e *Edge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ray := newRay()
	w.Header().Set("cf-ray", ray)
	w.Header().Set("server", "quicshot-edge")

	log := e.log.With("ray", ray, "path", r.URL.Path, "proto", r.Proto)

	connector, ok := e.pickConnector()
	if !ok {
		// Cloudflare's "error code: 1033 — tunnel not found / no connector".
		writeCFError(w, http.StatusBadGateway, 1033, "no connector registered")
		log.Error("no connector", "status", 502, "cf_code", 1033)
		return
	}
	log = log.With("connector_id", connector.id, "connector_remote", connector.conn.RemoteAddr().String())

	// The origin timeout starts when we begin talking to the tunnel, matching
	// Cloudflare's definition of a 524.
	ctx, cancel := context.WithTimeout(r.Context(), e.cfg.originTimeout)
	defer cancel()

	str, err := connector.conn.OpenStreamSync(ctx)
	if err != nil {
		if clientGone(r) {
			log.Debug("client gave up before the tunnel stream opened")
			return
		}
		kind, detail := quicerr.Classify(err)
		writeCFError(w, http.StatusBadGateway, 1033, "tunnel stream unavailable")
		log.Error("open tunnel stream", "status", 502, "err_kind", string(kind), "err_detail", detail)
		return
	}
	defer str.CancelRead(0)

	deadline := time.Now().Add(e.cfg.originTimeout)
	str.SetDeadline(deadline)

	outbound := r.Clone(ctx)
	outbound.RequestURI = ""
	if err := outbound.Write(str); err != nil {
		kind, detail := quicerr.Classify(err)
		writeCFError(w, http.StatusBadGateway, 1033, "failed to forward request")
		log.Error("write tunnel request", "status", 502, "err_kind", string(kind), "err_detail", detail)
		return
	}
	str.Close() // half-close: request fully sent

	resp, err := http.ReadResponse(bufio.NewReader(str), r)
	if err != nil {
		// A client that walked away is not an edge failure, and must not be logged
		// as one — otherwise it looks like a 524 that nobody actually received.
		if clientGone(r) {
			log.Debug("client gave up before the origin responded",
				"elapsed_ms", time.Since(start).Milliseconds())
			return
		}
		status, code, label := classifyOriginFailure(err, ctx)
		writeCFError(w, status, code, label)
		kind, detail := quicerr.Classify(err)
		log.Error("origin failure", "status", status, "cf_code", code,
			"err_kind", string(kind), "err_detail", detail,
			"elapsed_ms", time.Since(start).Milliseconds())
		return
	}
	defer resp.Body.Close()

	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	n, copyErr := io.Copy(w, resp.Body)

	attrs := []any{"status", resp.StatusCode, "bytes", n, "elapsed_ms", time.Since(start).Milliseconds()}
	if copyErr != nil {
		kind, detail := quicerr.Classify(copyErr)
		// A failure here is a mid-body disconnect: the status line already went
		// out, so the client sees a truncated response rather than a 5xx.
		log.Warn("body copy interrupted", append(attrs, "err_kind", string(kind), "err_detail", detail)...)
		return
	}
	log.Info("proxied", attrs...)
}

// clientGone reports whether the downstream client cancelled the request.
func clientGone(r *http.Request) bool {
	return errors.Is(r.Context().Err(), context.Canceled)
}

// statusOriginTimeout is Cloudflare's non-standard "a timeout occurred" status.
const statusOriginTimeout = 524

func classifyOriginFailure(err error, ctx context.Context) (status, cfCode int, label string) {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) || isTimeout(err) {
		return statusOriginTimeout, 524, "origin did not respond in time"
	}
	return http.StatusBadGateway, 1014, "origin connection error"
}

func isTimeout(err error) bool {
	var ne interface{ Timeout() bool }
	return errors.As(err, &ne) && ne.Timeout()
}

// writeCFError mimics the plain-text error bodies Cloudflare returns, so tests
// can assert on "error code: 524" the same way they would against production.
func writeCFError(w http.ResponseWriter, status, cfCode int, detail string) {
	w.Header().Set("content-type", "text/plain; charset=utf-8")
	w.Header().Set("cf-error-code", fmt.Sprint(cfCode))
	w.WriteHeader(status)
	fmt.Fprintf(w, "error code: %d\n%s\n", cfCode, detail)
}

func newRay() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func (e *Edge) logUDPCounters(ctx context.Context) {
	if e.cfg.statsEvery <= 0 {
		return
	}
	prev, ok := udpsock.ReadCounters()
	if !ok {
		e.log.Info("kernel UDP counters unavailable (non-Linux host)")
		return
	}
	t := time.NewTicker(e.cfg.statsEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cur, _ := udpsock.ReadCounters()
			d := cur.Sub(prev)
			prev = cur
			level := slog.LevelInfo
			if d.RcvbufErrors > 0 || d.SndbufErrors > 0 || d.InErrors > 0 {
				level = slog.LevelWarn
			}
			e.log.Log(ctx, level, "udp counters",
				"delta_in", d.InDatagrams, "delta_out", d.OutDatagrams,
				"delta_rcvbuf_errors", d.RcvbufErrors,
				"delta_sndbuf_errors", d.SndbufErrors,
				"delta_in_errors", d.InErrors,
				"total_rcvbuf_errors", cur.RcvbufErrors)
		}
	}
}
