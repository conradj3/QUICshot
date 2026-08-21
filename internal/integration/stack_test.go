package integration

import (
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/conrad/quicshot/internal/blast"
	"github.com/conrad/quicshot/internal/certs"
	"github.com/conrad/quicshot/internal/connector"
	"github.com/conrad/quicshot/internal/edge"
	"github.com/conrad/quicshot/internal/origin"
)

type stack struct {
	dir, base, origin, publicPort string
	edge                          *edge.Edge
	tls                           *tls.Config
	originClient                  *http.Client
	log                           *slog.Logger
}

func startStack(t *testing.T, originTimeout time.Duration) (context.Context, *stack) {
	t.Helper()
	dir := t.TempDir()
	if err := certs.Generate(dir, []string{"localhost", "127.0.0.1"}, true); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	originAddr, err := origin.Listen(ctx, "127.0.0.1:0", log)
	if err != nil {
		t.Fatal(err)
	}
	ed, err := edge.StartLocal(ctx, dir, "127.0.0.1:0", "127.0.0.1:0", originTimeout)
	if err != nil {
		t.Fatal(err)
	}
	host, port, err := net.SplitHostPort(ed.PublicAddr())
	if err != nil {
		t.Fatal(err)
	}
	if host == "" || host == "::" {
		host = "127.0.0.1"
	}
	pool, err := certs.LoadCAPool(dir)
	if err != nil {
		t.Fatal(err)
	}
	st := &stack{
		dir: dir, origin: originAddr, publicPort: port,
		base: "https://" + net.JoinHostPort(host, port),
		edge: ed, log: log,
		tls: &tls.Config{
			NextProtos: []string{certs.TunnelALPN},
			ServerName: "localhost",
			RootCAs:    pool,
			MinVersion: tls.VersionTLS13,
		},
		originClient: &http.Client{
			Timeout:   5 * time.Second,
			Transport: &http.Transport{DialContext: (&net.Dialer{Timeout: time.Second}).DialContext},
		},
	}
	return ctx, st
}

func (st *stack) startConnector(ctx context.Context, opts connector.StartOpts) {
	opts.EdgeAddr = st.edge.TunnelAddr()
	opts.OriginURL = "http://" + st.origin
	opts.TLS = st.tls
	if opts.Idle == 0 {
		opts.Idle = 30 * time.Second
	}
	if opts.KeepAlive == 0 {
		opts.KeepAlive = 5 * time.Second
	}
	if opts.Origin == nil {
		opts.Origin = st.originClient
	}
	if opts.Log == nil {
		opts.Log = st.log
	}
	go func() { _ = connector.Start(ctx, opts) }()
}

func (st *stack) waitReg(t *testing.T, n int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := st.edge.WaitRegistered(ctx, n); err != nil {
		t.Fatal(err)
	}
}

func TestInProcessStack(t *testing.T) {
	ctx, st := startStack(t, 400*time.Millisecond)
	st.startConnector(ctx, connector.StartOpts{Hostname: "localhost"})
	st.waitReg(t, 1)

	t.Run("fast", func(t *testing.T) {
		logPath, err := blastOnce(t, st.dir, st.base+"/fast", 2*time.Second, "-max-failure-pct=0")
		if err != nil {
			t.Fatalf("healthy path failed: %v\n%s", err, readLog(logPath))
		}
	})
	t.Run("524", func(t *testing.T) {
		logPath, _ := blastOnce(t, st.dir, st.base+"/slow?ms=2000", 3*time.Second)
		assertJSONL(t, logPath, `"status":524`, `cf_error_code=524`)
	})
	t.Run("1014", func(t *testing.T) {
		logPath, _ := blastOnce(t, st.dir, st.base+"/reset", 3*time.Second)
		assertJSONL(t, logPath, `"status":502`, `cf_error_code=1014`)
	})
	t.Run("headers-then-hang-not-524", func(t *testing.T) {
		logPath, _ := blastOnce(t, st.dir, st.base+"/headers-then-hang?ms=5000", 800*time.Millisecond)
		b := readLog(logPath)
		if strings.Contains(b, `"status":524`) || strings.Contains(b, `cf_error_code=524`) {
			t.Fatalf("headers-then-hang must not be a 524:\n%s", b)
		}
	})
}

func TestConnectorRegisterIsTunnelUp(t *testing.T) {
	ctx, st := startStack(t, time.Second)
	t.Run("1033-before-register", func(t *testing.T) {
		st.startConnector(ctx, connector.StartOpts{SkipRegister: true, Hostname: "localhost"})
		time.Sleep(200 * time.Millisecond)
		if st.edge.Registered() != 0 {
			t.Fatalf("registered = %d, want 0", st.edge.Registered())
		}
		logPath, _ := blastOnce(t, st.dir, st.base+"/fast", 2*time.Second)
		assertJSONL(t, logPath, `"status":502`, `cf_error_code=1033`)
	})
}

func TestConnectorHA(t *testing.T) {
	ctx, st := startStack(t, time.Second)
	st.startConnector(ctx, connector.StartOpts{HA: 2, Hostname: "localhost"})
	st.waitReg(t, 2)
	logPath, err := blastOnce(t, st.dir, st.base+"/fast", 2*time.Second, "-max-failure-pct=0")
	if err != nil {
		t.Fatalf("HA path failed: %v\n%s", err, readLog(logPath))
	}
}

func TestInteropMatrix(t *testing.T) {
	ctx, st := startStack(t, time.Second)
	st.startConnector(ctx, connector.StartOpts{Hostname: "localhost"})
	st.waitReg(t, 1)

	t.Run("quicgo-h3", func(t *testing.T) {
		logPath, err := blastOnce(t, st.dir, st.base+"/fast", 2*time.Second, "-max-failure-pct=0")
		if err != nil {
			t.Fatalf("%v\n%s", err, readLog(logPath))
		}
	})

	t.Run("stdlib-tcp", func(t *testing.T) {
		pool, err := certs.LoadCAPool(st.dir)
		if err != nil {
			t.Fatal(err)
		}
		client := &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					MinVersion: tls.VersionTLS12,
					ServerName: "localhost",
					RootCAs:    pool,
					NextProtos: []string{"http/1.1"},
				},
				DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
					return (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, network, net.JoinHostPort("127.0.0.1", st.publicPort))
				},
			},
		}
		resp, err := client.Get("https://localhost:" + st.publicPort + "/fast")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("tcp status %d proto %s", resp.StatusCode, resp.Proto)
		}
		if resp.Proto != "HTTP/1.1" && resp.Proto != "HTTP/2.0" {
			t.Fatalf("tcp proto %s", resp.Proto)
		}
		alt := resp.Header.Get("Alt-Svc")
		if !strings.Contains(strings.ToLower(alt), "h3") {
			t.Fatalf("Alt-Svc %q missing h3", alt)
		}
	})

	t.Run("curl-h3", func(t *testing.T) {
		url := "https://127.0.0.1:" + st.publicPort + "/fast"
		out, err := runCurlH3(url, filepath.Join(st.dir, certs.CAFile))
		if err != nil {
			t.Skip(err.Error())
		}
		got := strings.ToLower(out)
		if !strings.Contains(out, "200") && !strings.Contains(got, "http/3") {
			t.Fatalf("curl h3 output:\n%s", out)
		}
	})
}

func blastOnce(t *testing.T, certDir, url string, timeout time.Duration, extra ...string) (string, error) {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), "blast.jsonl")
	args := append([]string{
		"-url=" + url,
		"-certs=" + certDir,
		"-server-name=localhost",
		"-conns=1", "-workers=1", "-requests=1",
		"-duration=5s", "-timeout=" + timeout.String(),
		"-log=" + logPath,
	}, extra...)
	deadline := time.Now().Add(8 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		last = blast.Main(args)
		if _, err := os.Stat(logPath); err == nil {
			if last == nil || containsAny(logPath, `"event":`) {
				return logPath, last
			}
		}
		time.Sleep(150 * time.Millisecond)
		_ = os.Remove(logPath)
	}
	return logPath, last
}

func readLog(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return err.Error()
	}
	return string(b)
}

func containsAny(path string, needles ...string) bool {
	b, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	body := string(b)
	for _, n := range needles {
		if strings.Contains(body, n) {
			return true
		}
	}
	return false
}

func assertJSONL(t *testing.T, path string, needles ...string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	body := string(b)
	for _, n := range needles {
		if !strings.Contains(body, n) {
			t.Fatalf("log missing %q\n%s", n, body)
		}
	}
}
