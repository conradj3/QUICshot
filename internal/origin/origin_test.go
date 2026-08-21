package origin

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestEchoAndHeaders(t *testing.T) {
	h := newMux(slog.New(slog.NewTextHandler(io.Discard, nil)))

	t.Run("echo", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/echo", bytes.NewReader([]byte("hello")))
		req.Header.Set("content-type", "text/plain")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != 200 {
			t.Fatalf("status = %d", rr.Code)
		}
		if got := rr.Body.String(); got != "hello" {
			t.Fatalf("body = %q", got)
		}
		if rr.Header().Get("x-echo-bytes") != "5" {
			t.Fatalf("x-echo-bytes = %q", rr.Header().Get("x-echo-bytes"))
		}
	})

	t.Run("headers-then-hang", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		req := httptest.NewRequest(http.MethodGet, "/headers-then-hang?ms=5000", nil).WithContext(ctx)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != 200 {
			t.Fatalf("status = %d, want 200 (not a 524)", rr.Code)
		}
		if rr.Header().Get("x-origin") != "headers-then-hang" {
			t.Fatalf("missing headers-then-hang marker")
		}
	})

	t.Run("headers", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/headers?n=4&size=8", nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != 200 {
			t.Fatalf("status = %d", rr.Code)
		}
		n := 0
		for k := range rr.Header() {
			if strings.HasPrefix(strings.ToLower(k), "x-stress-") {
				n++
				if len(rr.Header().Get(k)) != 8 {
					t.Fatalf("%s length = %d", k, len(rr.Header().Get(k)))
				}
			}
		}
		if n != 4 {
			t.Fatalf("stress headers = %d, want 4", n)
		}
		if !strings.Contains(rr.Body.String(), "headers=4") {
			t.Fatalf("body = %q", rr.Body.String())
		}
	})
}

func TestIntParamClampViaHeadersCap(t *testing.T) {
	h := newMux(slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodGet, "/headers?n=9999&size=9999", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	n := 0
	for k := range rr.Header() {
		if strings.HasPrefix(strings.ToLower(k), "x-stress-") {
			n++
		}
	}
	if n != 256 {
		t.Fatalf("capped header count = %d, want 256", n)
	}
	if got := len(rr.Header().Get("X-Stress-0")); got != 2048 {
		t.Fatalf("capped size = %d, want 2048 (%s)", got, strconv.Itoa(got))
	}
}
