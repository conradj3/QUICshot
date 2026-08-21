package edge

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
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
