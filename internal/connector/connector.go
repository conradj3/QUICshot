// Package connector plays cloudflared: it dials *out* to the edge over QUIC,
// then serves request streams the edge opens back down that connection.
package connector

import (
	"bufio"
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/conrad/quicshot/internal/certs"
	"github.com/conrad/quicshot/internal/qlogtrace"
	"github.com/conrad/quicshot/internal/quiccfg"
	"github.com/conrad/quicshot/internal/quicerr"
	"github.com/conrad/quicshot/internal/tunnelproto"
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
	ha := fs.Int("ha-connections", 1, "QUIC connections this process opens to the edge (cloudflared default is 4)")
	originReuse := fs.Bool("origin-reuse", true, "pool idle TCP connections to the origin")
	originHTTP2 := fs.Bool("origin-http2", false, "allow HTTP/2 to the origin")
	if err := fs.Parse(args); err != nil {
		return err
	}

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

	origin := optsOriginClient(*originTimeout, *originReuse, *originHTTP2)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return Start(ctx, StartOpts{
		EdgeAddr: *edgeAddr, OriginURL: *originURL, TLS: tlsConf,
		Idle: *maxIdle, KeepAlive: *keepAlive, QlogDir: *qlogDir, Origin: origin,
		HA: *ha, Hostname: *serverName,
	})
}

func optsOriginClient(timeout time.Duration, reuse, http2 bool) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 256,
			IdleConnTimeout:     90 * time.Second,
			DisableKeepAlives:   !reuse,
			ForceAttemptHTTP2:   http2,
			DialContext:         (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
		},
	}
}

// StartOpts is the in-process connector configuration used by tests.
type StartOpts struct {
	EdgeAddr, OriginURL, QlogDir, Hostname string
	TLS                                    *tls.Config
	Idle, KeepAlive                        time.Duration
	Origin                                 *http.Client
	Log                                    *slog.Logger
	HA                                     int
	SkipRegister                           bool
	RegisterDelay                          time.Duration
}

// Start dials the edge (HA times) and serves origin proxy streams until ctx is cancelled.
func Start(ctx context.Context, opts StartOpts) error {
	log := opts.Log
	if log == nil {
		log = slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("role", "connector")
	}
	if opts.Origin == nil {
		opts.Origin = optsOriginClient(100*time.Second, true, false)
	}
	n := opts.HA
	if n < 1 {
		n = 1
	}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(haID int) {
			defer wg.Done()
			reconnectLoop(ctx, log.With("ha_id", haID), opts, haID)
		}(i)
	}
	wg.Wait()
	return nil
}

func reconnectLoop(ctx context.Context, log *slog.Logger, opts StartOpts, haID int) {
	backoff := 500 * time.Millisecond
	for ctx.Err() == nil {
		err := runOnce(ctx, log, runConfig{
			edgeAddr:      opts.EdgeAddr,
			originURL:     opts.OriginURL,
			tlsConf:       opts.TLS,
			quicConf:      quiccfg.Client(opts.Idle, opts.KeepAlive),
			qlogDir:       opts.QlogDir,
			origin:        opts.Origin,
			haID:          haID,
			hostname:      opts.Hostname,
			keepAlive:     opts.KeepAlive,
			skipRegister:  opts.SkipRegister,
			registerDelay: opts.RegisterDelay,
		})
		if ctx.Err() != nil {
			return
		}
		kind, detail := quicerr.Classify(err)
		log.Warn("tunnel down, reconnecting", "backoff", backoff.String(),
			"close_kind", string(kind), "close_detail", detail)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 10*time.Second {
			backoff *= 2
		}
	}
}

type runConfig struct {
	edgeAddr      string
	originURL     string
	tlsConf       *tls.Config
	quicConf      *quic.Config
	qlogDir       string
	origin        *http.Client
	haID          int
	hostname      string
	keepAlive     time.Duration
	skipRegister  bool
	registerDelay time.Duration
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

	if !cfg.skipRegister {
		if cfg.registerDelay > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(cfg.registerDelay):
			}
		}
		ctrl, err := conn.OpenStreamSync(ctx)
		if err != nil {
			return fmt.Errorf("open control stream: %w", err)
		}
		host := cfg.hostname
		if host == "" {
			host = "local"
		}
		if err := tunnelproto.Write(ctrl, tunnelproto.Msg{T: tunnelproto.TypeRegister, Hostname: host, HAID: cfg.haID}); err != nil {
			return fmt.Errorf("register: %w", err)
		}
		go pingLoop(ctx, ctrl, cfg.keepAlive)
		go func() {
			<-ctx.Done()
			_ = tunnelproto.Write(ctrl, tunnelproto.Msg{T: tunnelproto.TypeUnregister, Reason: "shutdown"})
		}()
	}

	var inflight sync.WaitGroup
	defer func() {
		done := make(chan struct{})
		go func() { inflight.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	}()

	for {
		str, err := conn.AcceptStream(ctx)
		if err != nil {
			return err
		}
		inflight.Add(1)
		go func() {
			defer inflight.Done()
			serveStream(log, str, cfg.originURL, cfg.origin)
		}()
	}
}

func pingLoop(ctx context.Context, w io.Writer, every time.Duration) {
	if every <= 0 {
		every = 5 * time.Second
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := tunnelproto.Write(w, tunnelproto.Msg{T: tunnelproto.TypePing}); err != nil {
				return
			}
		}
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
		_ = writeStatus(str, http.StatusBadRequest)
		return
	}
	outbound.Header = req.Header.Clone()
	outbound.Header.Set("x-forwarded-proto", "https")

	start := time.Now()
	resp, err := client.Do(outbound)
	if err != nil {
		if !resetTunnelOnOriginError(err, reqCtx.Err()) {
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

func writeStatus(w io.Writer, code int) error {
	resp := &http.Response{
		StatusCode: code,
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     http.Header{},
		Body:       http.NoBody,
	}
	return resp.Write(w)
}

// resetTunnelOnOriginError reports whether an origin dial/request error should
// cancel the tunnel stream (so the edge emits 1014) instead of writing a 502.
func resetTunnelOnOriginError(originErr, reqCtxErr error) bool {
	if originErr == nil {
		return false
	}
	return reqCtxErr == nil
}
