// Package connector plays cloudflared: it dials *out* to the edge over QUIC,
// then serves request streams the edge opens back down that connection.
package connector

import (
	"bufio"
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/conrad/quicshot/internal/certs"
	"github.com/conrad/quicshot/internal/qlogtrace"
	"github.com/conrad/quicshot/internal/quicerr"
)

func Main(args []string) error {
	fs := flag.NewFlagSet("connector", flag.ExitOnError)
	edgeAddr := fs.String("edge", "edge:7844", "edge QUIC address to dial")
	originURL := fs.String("origin", "http://origin:8080", "origin base URL")
	certDir := fs.String("certs", "/certs", "directory holding ca.pem")
	serverName := fs.String("server-name", "edge", "TLS server name presented to the edge")
	insecure := fs.Bool("insecure", false, "skip edge certificate verification")
	maxIdle := fs.Duration("max-idle-timeout", 30*time.Second, "QUIC max idle timeout on the tunnel hop")
	keepAlive := fs.Duration("keepalive", 5*time.Second, "QUIC keep-alive period on the tunnel hop")
	originTimeout := fs.Duration("origin-timeout", 100*time.Second, "connector-side timeout when talking to the origin")
	qlogDir := fs.String("qlog-dir", "", "write QUIC qlog traces to this directory (empty disables)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("role", "connector")

	tlsConf := &tls.Config{
		NextProtos:         []string{certs.TunnelALPN},
		ServerName:         *serverName,
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: *insecure,
	}
	if !*insecure {
		pool, err := certs.LoadCAPool(*certDir)
		if err != nil {
			return err
		}
		tlsConf.RootCAs = pool
	}

	origin := &http.Client{
		Timeout: *originTimeout,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 256,
			DialContext:         (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
		},
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	backoff := 500 * time.Millisecond
	for ctx.Err() == nil {
		err := runOnce(ctx, log, runConfig{
			edgeAddr:  *edgeAddr,
			originURL: *originURL,
			tlsConf:   tlsConf,
			quicConf: &quic.Config{
				MaxIdleTimeout:  *maxIdle,
				KeepAlivePeriod: *keepAlive,
			},
			qlogDir: *qlogDir,
			origin:  origin,
		})
		if ctx.Err() != nil {
			return nil
		}
		kind, detail := quicerr.Classify(err)
		log.Warn("tunnel down, reconnecting", "backoff", backoff.String(),
			"close_kind", string(kind), "close_detail", detail)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff < 10*time.Second {
			backoff *= 2
		}
	}
	return nil
}

type runConfig struct {
	edgeAddr  string
	originURL string
	tlsConf   *tls.Config
	quicConf  *quic.Config
	qlogDir   string
	origin    *http.Client
}

func runOnce(ctx context.Context, log *slog.Logger, cfg runConfig) error {

	if err := qlogtrace.Configure(cfg.quicConf, cfg.qlogDir); err != nil {
		return err
	}

	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	conn, err := quic.DialAddr(dialCtx, cfg.edgeAddr, cfg.tlsConf, cfg.quicConf)
	if err != nil {
		return fmt.Errorf("dial edge: %w", err)
	}
	log.Info("tunnel established", "edge", cfg.edgeAddr, "local", conn.LocalAddr().String())
	defer conn.CloseWithError(0, "connector shutting down")

	for {
		str, err := conn.AcceptStream(ctx)
		if err != nil {
			return err
		}
		go serveStream(log, str, cfg.originURL, cfg.origin)
	}
}

func serveStream(log *slog.Logger, str *quic.Stream, originURL string, client *http.Client) {
	defer str.Close()

	req, err := http.ReadRequest(bufio.NewReader(str))
	if err != nil {
		log.Warn("read tunnel request", "stream", int64(str.StreamID()), "err", err.Error())
		str.CancelRead(0)
		return
	}

	target := originURL + req.URL.RequestURI()
	// Tie the origin request to the tunnel stream: when the edge gives up (524) it
	// sends STOP_SENDING, which cancels this context and releases the origin
	// immediately instead of leaving it wedged for the full sleep.
	reqCtx, cancelOrigin := context.WithCancel(str.Context())
	defer cancelOrigin()

	outbound, err := http.NewRequestWithContext(reqCtx, req.Method, target, req.Body)
	if err != nil {
		writeStatus(str, http.StatusBadRequest)
		return
	}
	outbound.Header = req.Header.Clone()
	outbound.Header.Set("x-forwarded-proto", "https")

	start := time.Now()
	resp, err := client.Do(outbound)
	if err != nil {
		if reqCtx.Err() != nil {
			log.Info("origin request abandoned by edge", "stream", int64(str.StreamID()),
				"target", target, "elapsed_ms", time.Since(start).Milliseconds())
			return
		}
		// Origin connection errors (RST, dial fail) must surface as a broken
		// tunnel response so the edge can synthesize Cloudflare 1014. Writing a
		// bare 502 here would be proxied through and lose the cf error code.
		log.Warn("origin request failed", "stream", int64(str.StreamID()), "target", target,
			"elapsed_ms", time.Since(start).Milliseconds(), "err", err.Error())
		str.CancelWrite(0)
		return
	}
	defer resp.Body.Close()

	if err := resp.Write(str); err != nil {
		kind, detail := quicerr.Classify(err)
		log.Warn("write tunnel response", "stream", int64(str.StreamID()),
			"err_kind", string(kind), "err_detail", detail)
		return
	}
	log.Debug("served", "stream", int64(str.StreamID()), "target", target,
		"status", resp.StatusCode, "elapsed_ms", time.Since(start).Milliseconds())
}

func writeStatus(str *quic.Stream, code int) {
	resp := &http.Response{
		StatusCode: code,
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     http.Header{},
		Body:       http.NoBody,
	}
	resp.Write(str)
}
