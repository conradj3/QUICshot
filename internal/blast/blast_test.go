package blast

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRecorderVerdictFailureRate(t *testing.T) {
	rec := newRecorder(&bytes.Buffer{})
	rec.success(event{Status: http.StatusOK}, time.Millisecond)
	rec.failure(event{Kind: "http_524", Status: statusOriginTimeoutForTest})

	if err := rec.verdict(40, -1); err == nil {
		t.Fatal("verdict() succeeded, want failure rate error")
	}
	if err := rec.verdict(50, -1); err != nil {
		t.Fatalf("verdict() = %v, want nil at threshold", err)
	}
}

func TestRecorderVerdictConnectionDrops(t *testing.T) {
	rec := newRecorder(&bytes.Buffer{})
	rec.connDrops = 2

	if err := rec.verdict(-1, 1); err == nil {
		t.Fatal("verdict() succeeded, want connection drop error")
	}
	if err := rec.verdict(-1, 2); err != nil {
		t.Fatalf("verdict() = %v, want nil at threshold", err)
	}
}

func TestDoRequestRecordsCloudflareErrorCode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("cf-ray", "abc123")
		w.WriteHeader(statusOriginTimeoutForTest)
		w.Write([]byte("error code: 524\norigin did not respond in time\n"))
	}))
	defer srv.Close()

	var out bytes.Buffer
	rec := newRecorder(&out)
	doRequest(t.Context(), rec, srv.Client(), &reqSpec{
		url: srv.URL, method: http.MethodGet, timeout: time.Second, readBody: false,
	}, 0, 0)

	if got := rec.requests.Load(); got != 1 {
		t.Fatalf("requests = %d, want 1", got)
	}
	if got := rec.failures.Load(); got != 1 {
		t.Fatalf("failures = %d, want 1", got)
	}
	if got := rec.cfCodes[524]; got != 1 {
		t.Fatalf("cfCodes[524] = %d, want 1", got)
	}
	if !strings.Contains(out.String(), `"event":"request_5xx"`) || !strings.Contains(out.String(), `cf_error_code=524`) {
		t.Fatalf("event log did not include request_5xx with cf_error_code=524: %s", out.String())
	}
}

func TestPercentile(t *testing.T) {
	latencies := []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 4 * time.Millisecond}
	if got := percentile(latencies, 50); got != 3*time.Millisecond {
		t.Fatalf("p50 = %s, want 3ms", got)
	}
	if got := percentile(latencies, 99.9); got != 4*time.Millisecond {
		t.Fatalf("p99.9 = %s, want 4ms", got)
	}
}

func TestCFMeaning(t *testing.T) {
	if got := cfMeaning(524); !strings.Contains(got, "origin connected") {
		t.Fatalf("cfMeaning(524) = %q", got)
	}
	if got := cfMeaning(999); got != "" {
		t.Fatalf("cfMeaning(999) = %q, want empty", got)
	}
}

const statusOriginTimeoutForTest = 524
