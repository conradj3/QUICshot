package origin

import (
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func writeString(w http.ResponseWriter, s string) {
	_, _ = w.Write([]byte(s))
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeString(w, "ok\n")
}

func handleFast(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("x-origin", "fast")
	writeString(w, "fast\n")
}

// /slow?ms=N — the 524 lever. Exceed the edge's origin timeout and the edge
// synthesises a 524 exactly like Cloudflare does.
func handleSlow(log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		d := durationParam(r, "ms", 5*time.Second)
		select {
		case <-time.After(d):
		case <-r.Context().Done():
			log.Warn("client went away while sleeping", "path", r.URL.Path, "waited", d.String())
			return
		}
		w.Header().Set("x-origin-delay", d.String())
		_, _ = fmt.Fprintf(w, "slept %s\n", d)
	}
}

// /hang never responds. Models a wedged worker/thread pool, the most common
// real cause of a 524.
func handleHang(log *slog.Logger) http.HandlerFunc {
	return func(_ http.ResponseWriter, r *http.Request) {
		log.Warn("hanging request", "remote", r.RemoteAddr)
		<-r.Context().Done()
		log.Warn("hung request released", "remote", r.RemoteAddr)
	}
}

// /drip streams chunks with a delay between them. Exercises HTTP/3 stream
// flow control and reveals mid-body disconnects rather than 524s.
func handleDrip(log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		gap := durationParam(r, "ms", 200*time.Millisecond)
		chunks := intParam(r, "chunks", 10)
		size := intParam(r, "size", 1024)
		w.Header().Set("content-type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		buf := make([]byte, size)
		for i := 0; i < chunks; i++ {
			if _, err := w.Write(buf); err != nil {
				log.Warn("drip write failed", "chunk", i, "err", err.Error())
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			select {
			case <-time.After(gap):
			case <-r.Context().Done():
				return
			}
		}
	}
}

// /bytes?n=N returns a body of N bytes. Use it to push enough volume through
// the tunnel to make UDP receive-buffer drops show up.
func handleBytes(w http.ResponseWriter, r *http.Request) {
	n := intParam(r, "n", 1<<20)
	w.Header().Set("content-type", "application/octet-stream")
	w.Header().Set("content-length", strconv.Itoa(n))
	buf := make([]byte, 32*1024)
	for written := 0; written < n; {
		chunk := min(len(buf), n-written)
		m, err := w.Write(buf[:chunk])
		if err != nil {
			return
		}
		written += m
	}
}

// /reset kills the TCP connection without a response, which the edge should
// surface as a 502 rather than a 524.
func handleReset(w http.ResponseWriter, _ *http.Request) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijack unsupported", http.StatusInternalServerError)
		return
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		return
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0) // send RST instead of FIN
	}
	_ = conn.Close()
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(intParam(r, "code", 200))
}

// /flaky?pct=N fails a percentage of requests, for mixed-signal runs.
func handleFlaky(w http.ResponseWriter, r *http.Request) {
	if rand.Intn(100) < intParam(r, "pct", 10) {
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	writeString(w, "ok\n")
}

// /echo returns the request body. Used to prove POST / request-stream flow
// control through the tunnel.
func handleEcho(w http.ResponseWriter, r *http.Request) {
	b, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("content-type", r.Header.Get("content-type"))
	w.Header().Set("x-echo-bytes", strconv.Itoa(len(b)))
	_, _ = w.Write(b)
}

// /headers?n=N&size=S writes N response headers of S bytes each. This is a
// QPACK / header-block stress lever, not a full encoder fuzzer.
func handleHeaders(w http.ResponseWriter, r *http.Request) {
	n := min(intParam(r, "n", 32), 256)
	size := min(intParam(r, "size", 64), 2048)
	pad := strings.Repeat("x", size)
	for i := 0; i < n; i++ {
		w.Header().Set(fmt.Sprintf("x-stress-%d", i), pad)
	}
	_, _ = fmt.Fprintf(w, "headers=%d size=%d\n", n, size)
}

// /stall?ms=N sleeps before and while reading the body. Useful for holding
// streams open; it is not a substitute for kernel UDP RcvbufErrors.
func handleStall(w http.ResponseWriter, r *http.Request) {
	gap := durationParam(r, "ms", 250*time.Millisecond)
	if r.Body != nil {
		slowReadBody(r, gap)
	} else {
		time.Sleep(gap)
	}
	writeString(w, "stalled\n")
}

func slowReadBody(r *http.Request, gap time.Duration) {
	buf := make([]byte, 1)
	for {
		select {
		case <-r.Context().Done():
			return
		default:
		}
		_, err := r.Body.Read(buf)
		if err != nil {
			return
		}
		time.Sleep(gap)
	}
}
