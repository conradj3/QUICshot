// Package probe is a single-shot, non-invasive diagnostic for one endpoint. It
// answers "does HTTP/3 work to this host, and if not, whose fault is it" without
// putting load on anything.
package probe

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"

	"github.com/conrad/quicshot/internal/qlogtrace"
	"github.com/conrad/quicshot/internal/quicerr"
)

const (
	pass = "\033[32m  ok  \033[0m"
	fail = "\033[31m FAIL \033[0m"
	warn = "\033[33m warn \033[0m"
)

func Main(args []string) error {
	fs := flag.NewFlagSet("probe", flag.ExitOnError)
	target := fs.String("url", "", "URL to probe, e.g. https://app.example.com/")
	timeout := fs.Duration("timeout", 8*time.Second, "per-step timeout")
	insecure := fs.Bool("insecure", false, "skip certificate verification")
	qlogDir := fs.String("qlog-dir", "", "write QUIC qlog traces to this directory (empty disables)")
	control := fs.String("control", "https://cloudflare-quic.com/",
		"known-good HTTP/3 endpoint, used to tell 'this server has no h3' apart from 'UDP/443 is blocked here' (empty to skip)")
	var hdrs headerList
	fs.Var(&hdrs, "header", "extra request header 'Key: Value' (repeatable, e.g. for auth)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *target == "" {
		return fmt.Errorf("-url is required")
	}

	u, err := url.Parse(*target)
	if err != nil {
		return fmt.Errorf("bad -url: %w", err)
	}
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "443"
	}

	fmt.Printf("\nprobing %s\n%s\n", *target, strings.Repeat("=", 60))

	step("DNS", resolveDNS(host, *timeout))
	altSvc, tcpOK := probeTCP(*target, host, port, *timeout, *insecure, hdrs)
	h3OK, h3Detail := probeH3(*target, *timeout, *insecure, *qlogDir, hdrs)
	step("HTTP/3 (QUIC over UDP/"+port+")", result{h3OK, h3Detail})

	// The control tells us whether QUIC works from this machine at all.
	controlOK := true
	controlTested := false
	if *control != "" && !h3OK {
		controlTested = true
		ok, detail := probeH3(*control, *timeout, false, *qlogDir, nil)
		controlOK = ok
		step("control HTTP/3 ("+*control+")", result{ok, detail})
	}

	fmt.Printf("%s\n", strings.Repeat("=", 60))
	verdict(tcpOK, h3OK, altSvc, controlTested, controlOK)
	if !h3OK {
		os.Exit(1)
	}
	return nil
}

type result struct {
	ok     bool
	detail string
}

// headerList collects repeated -header flags.
type headerList []string

func (h *headerList) String() string     { return strings.Join(*h, ", ") }
func (h *headerList) Set(v string) error { *h = append(*h, v); return nil }

func applyHeaders(req *http.Request, hdrs headerList) {
	for _, h := range hdrs {
		if k, v, ok := strings.Cut(h, ":"); ok {
			req.Header.Add(strings.TrimSpace(k), strings.TrimSpace(v))
		}
	}
}

func step(name string, r result) {
	mark := pass
	if !r.ok {
		mark = fail
	}
	fmt.Printf("[%s] %-34s %s\n", mark, name, r.detail)
}

func resolveDNS(host string, timeout time.Duration) result {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return result{false, err.Error()}
	}
	ips := make([]string, 0, len(addrs))
	for _, a := range addrs {
		ips = append(ips, a.IP.String())
	}
	return result{true, strings.Join(ips, ", ")}
}

// probeTCP establishes the TLS/TCP path and reports what the server says about
// itself, including whether it advertises h3 via Alt-Svc.
func probeTCP(target, host, port string, timeout time.Duration, insecure bool, hdrs headerList) (altSvc string, ok bool) {
	dialer := &net.Dialer{Timeout: timeout}
	conn, err := tls.DialWithDialer(dialer, "tcp", net.JoinHostPort(host, port), &tls.Config{
		ServerName:         host,
		InsecureSkipVerify: insecure,
		NextProtos:         []string{"h2", "http/1.1"},
	})
	if err != nil {
		step("TCP + TLS", result{false, err.Error()})
		return "", false
	}
	state := conn.ConnectionState()
	issuer := "unknown"
	expiry := ""
	if len(state.PeerCertificates) > 0 {
		c := state.PeerCertificates[0]
		issuer = c.Issuer.CommonName
		expiry = c.NotAfter.Format("2006-01-02")
	}
	_ = conn.Close()
	step("TCP + TLS", result{true, fmt.Sprintf("alpn=%s issuer=%q expires=%s",
		state.NegotiatedProtocol, issuer, expiry)})

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	applyHeaders(req, hdrs)
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: insecure},
			ForceAttemptHTTP2: true,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	resp, err := client.Do(req)
	if err != nil {
		step("HTTP over TCP", result{false, err.Error()})
		return "", false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))

	altSvc = resp.Header.Get("Alt-Svc")
	fronting := "origin"
	if resp.Header.Get("cf-ray") != "" {
		fronting = "Cloudflare (cf-ray present)"
	} else if s := resp.Header.Get("server"); s != "" {
		fronting = s
	}
	step("HTTP over TCP", result{true, fmt.Sprintf("%s %s  via=%s", resp.Proto, resp.Status, fronting)})

	r := result{altSvc != "", altSvc}
	if altSvc == "" {
		r.detail = "no Alt-Svc header: server is not advertising HTTP/3"
	}
	mark := pass
	if !r.ok {
		mark = warn
	}
	fmt.Printf("[%s] %-34s %s\n", mark, "Alt-Svc (h3 advertised?)", r.detail)
	return altSvc, true
}

func probeH3(target string, timeout time.Duration, insecure bool, qlogDir string, hdrs headerList) (bool, string) {
	quicConf := &quic.Config{HandshakeIdleTimeout: timeout, MaxIdleTimeout: timeout}
	if err := qlogtrace.Configure(quicConf, qlogDir); err != nil {
		return false, err.Error()
	}
	tr := &http3.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure},
		QUICConfig:      quicConf,
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return false, err.Error()
	}
	applyHeaders(req, hdrs)
	start := time.Now()
	resp, err := tr.RoundTrip(req)
	if err != nil {
		kind, detail := quicerr.Classify(err)
		return false, fmt.Sprintf("%s: %s (after %s)", kind, detail, time.Since(start).Round(time.Millisecond))
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return true, fmt.Sprintf("%s %s in %s", resp.Proto, resp.Status, time.Since(start).Round(time.Millisecond))
}

func verdict(tcpOK, h3OK bool, altSvc string, controlTested, controlOK bool) {
	switch {
	case h3OK:
		fmt.Println("VERDICT: HTTP/3 works end to end. Safe to run `quicshot blast` against this URL.")
	case controlTested && !controlOK:
		fmt.Println("VERDICT: HTTP/3 failed here AND to a known-good public endpoint.")
		fmt.Println("         UDP/443 is being blocked on your side - corporate firewall, VPN, or")
		fmt.Println("         a network that does not pass QUIC. This says nothing about the target.")
		fmt.Println("         Retest off-VPN, or from a host inside the same network as the tunnel.")
	case !tcpOK:
		fmt.Println("VERDICT: could not reach the host over TCP either. Check DNS, VPN and access.")
	case altSvc == "":
		fmt.Println("VERDICT: the server never advertised HTTP/3 (no Alt-Svc) and the QUIC handshake")
		fmt.Println("         failed. HTTP/3 is most likely not enabled for this hostname.")
	default:
		fmt.Printf("VERDICT: the server advertises h3 (%s) but the QUIC handshake failed,\n", altSvc)
		fmt.Println("         while the control endpoint succeeded. The block is specific to this")
		fmt.Println("         path - a middlebox, or the edge is not actually serving h3.")
	}
	fmt.Println()
}
