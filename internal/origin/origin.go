// Package origin is the "your app" end of the tunnel: a plain HTTP/1.1 server
// whose latency and failure modes are driven entirely by the request URL.
package origin

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"
)

func Main(args []string) error {
	fs := flag.NewFlagSet("origin", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("role", "origin")
	srv := &http.Server{
		Addr:              *addr,
		Handler:           newMux(log),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Info("listening", "addr", *addr)
	return srv.ListenAndServe()
}

func newMux(log *slog.Logger) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", handleHealthz)
	mux.HandleFunc("/fast", handleFast)
	mux.HandleFunc("/slow", handleSlow(log))
	mux.HandleFunc("/hang", handleHang(log))
	mux.HandleFunc("/drip", handleDrip(log))
	mux.HandleFunc("/bytes", handleBytes)
	mux.HandleFunc("/reset", handleReset)
	mux.HandleFunc("/status", handleStatus)
	mux.HandleFunc("/flaky", handleFlaky)
	mux.HandleFunc("/echo", handleEcho)
	mux.HandleFunc("/headers", handleHeaders)
	mux.HandleFunc("/stall", handleStall)
	return mux
}

func intParam(r *http.Request, name string, def int) int {
	if v, err := strconv.Atoi(r.URL.Query().Get(name)); err == nil {
		return v
	}
	return def
}

func durationParam(r *http.Request, name string, def time.Duration) time.Duration {
	if ms := intParam(r, name, -1); ms >= 0 {
		return time.Duration(ms) * time.Millisecond
	}
	return def
}
