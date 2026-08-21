package integration

import (
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
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

func TestInProcessStack(t *testing.T) {
	dir := t.TempDir()
	if err := certs.Generate(dir, []string{"localhost", "127.0.0.1"}, true); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	originAddr, err := origin.Listen(ctx, "127.0.0.1:0", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}

	ed, err := edge.StartLocal(ctx, dir, "127.0.0.1:0", "127.0.0.1:0", 400*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	publicHost, publicPort, err := net.SplitHostPort(ed.PublicAddr())
	if err != nil {
		t.Fatal(err)
	}
	if publicHost == "" || publicHost == "::" {
		publicHost = "127.0.0.1"
	}
	base := "https://" + net.JoinHostPort(publicHost, publicPort)

	pool, err := certs.LoadCAPool(dir)
	if err != nil {
		t.Fatal(err)
	}
	tlsConf := &tls.Config{
		NextProtos: []string{certs.TunnelALPN},
		ServerName: "localhost",
		RootCAs:    pool,
		MinVersion: tls.VersionTLS13,
	}
	originClient := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{DialContext: (&net.Dialer{Timeout: time.Second}).DialContext},
	}
	go func() {
		_ = connector.Start(ctx, connector.StartOpts{
			EdgeAddr:  ed.TunnelAddr(),
			OriginURL: "http://" + originAddr,
			TLS:       tlsConf,
			Idle:      30 * time.Second,
			KeepAlive: 5 * time.Second,
			Origin:    originClient,
			Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
	}()

	t.Run("fast", func(t *testing.T) {
		logPath, err := blastOnce(t, dir, base+"/fast", 2*time.Second, "-max-failure-pct=0")
		if err != nil {
			t.Fatalf("healthy path failed: %v\n%s", err, readLog(logPath))
		}
	})
	t.Run("524", func(t *testing.T) {
		logPath, _ := blastOnce(t, dir, base+"/slow?ms=2000", 3*time.Second)
		assertJSONL(t, logPath, `"status":524`, `cf_error_code=524`)
	})
	t.Run("1014", func(t *testing.T) {
		logPath, _ := blastOnce(t, dir, base+"/reset", 3*time.Second)
		assertJSONL(t, logPath, `"status":502`, `cf_error_code=1014`)
	})
	t.Run("interop-curl", func(t *testing.T) {
		if !curlHasH3() {
			t.Skip("host curl has no --http3-only")
		}
		cmd := exec.Command("curl", "--http3-only", "--cacert", filepath.Join(dir, certs.CAFile),
			"--resolve", "localhost:"+publicPort+":127.0.0.1",
			"--max-time", "5", "-sS", "-o", os.DevNull, "-w", "%{http_version} %{http_code}",
			"https://localhost:"+publicPort+"/fast")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("curl: %v\n%s", err, out)
		}
		got := strings.TrimSpace(string(out))
		if !strings.Contains(got, "3") || !strings.Contains(got, "200") {
			t.Fatalf("curl HTTP/3 interop = %q, want HTTP/3 200", got)
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
			if last == nil || containsAny(logPath, `"status":502`, `"status":524`) {
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

func curlHasH3() bool {
	out, err := exec.Command("curl", "--help").CombinedOutput()
	return err == nil && strings.Contains(string(out), "--http3-only")
}
