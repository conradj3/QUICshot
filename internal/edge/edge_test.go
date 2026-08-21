package edge

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return false }

func TestClassifyOriginFailure(t *testing.T) {
	deadlineCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	tests := []struct {
		name   string
		err    error
		ctx    context.Context
		status int
		code   int
	}{
		{name: "context deadline", err: errors.New("read response"), ctx: deadlineCtx, status: statusOriginTimeout, code: 524},
		{name: "deadline error", err: context.DeadlineExceeded, ctx: context.Background(), status: statusOriginTimeout, code: 524},
		{name: "net timeout", err: timeoutErr{}, ctx: context.Background(), status: statusOriginTimeout, code: 524},
		{name: "origin connection error", err: errors.New("connection reset by peer"), ctx: context.Background(), status: http.StatusBadGateway, code: 1014},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, code, label := classifyOriginFailure(tt.err, tt.ctx)
			if status != tt.status || code != tt.code {
				t.Fatalf("classifyOriginFailure() = status %d code %d, want status %d code %d", status, code, tt.status, tt.code)
			}
			if label == "" {
				t.Fatal("classifyOriginFailure() returned an empty label")
			}
		})
	}
}

func TestWriteCFError(t *testing.T) {
	rr := httptest.NewRecorder()
	writeCFError(rr, statusOriginTimeout, 524, "origin did not respond in time")

	if rr.Code != statusOriginTimeout {
		t.Fatalf("status = %d, want %d", rr.Code, statusOriginTimeout)
	}
	if got := rr.Header().Get("cf-error-code"); got != "524" {
		t.Fatalf("cf-error-code = %q, want 524", got)
	}
	if got := rr.Body.String(); got != "error code: 524\norigin did not respond in time\n" {
		t.Fatalf("body = %q", got)
	}
}

func TestConnectorRefLiveRequiresRegister(t *testing.T) {
	now := time.Now()
	c := connectorRef{}
	if c.live(now, time.Second) {
		t.Fatal("unregistered connector must not be live")
	}
	c.registered = true
	c.lastPing = now
	if !c.live(now, 3*time.Second) {
		t.Fatal("fresh register should be live")
	}
	c.lastPing = now.Add(-10 * time.Second)
	if c.live(now, 3*time.Second) {
		t.Fatal("stale ping should not be live")
	}
	if !c.live(now, 0) {
		t.Fatal("staleAfter=0 disables expiry")
	}
}

func TestPickConnectorIndex(t *testing.T) {
	var rr atomic.Uint64
	if got := pickConnectorIndex(3, "/fast", "rr", &rr); got != 0 {
		t.Fatalf("first rr pick = %d, want 0", got)
	}
	if got := pickConnectorIndex(3, "/fast", "rr", &rr); got != 1 {
		t.Fatalf("second rr pick = %d, want 1", got)
	}

	a := pickConnectorIndex(4, "/fast", "hash", &rr)
	b := pickConnectorIndex(4, "/fast", "hash", &rr)
	if a != b {
		t.Fatalf("hash(/fast) was not sticky: %d vs %d", a, b)
	}
	c := pickConnectorIndex(4, "/bytes", "hash", &rr)
	if a == c {
		t.Logf("hash(/fast)=%d hash(/bytes)=%d (collision is allowed)", a, c)
	}
}
