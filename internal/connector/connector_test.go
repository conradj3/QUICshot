package connector

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestResetTunnelOnOriginError(t *testing.T) {
	if !resetTunnelOnOriginError(errors.New("connection reset by peer"), nil) {
		t.Fatal("origin RST should reset the tunnel stream (edge 1014)")
	}
	if resetTunnelOnOriginError(errors.New("context canceled"), context.Canceled) {
		t.Fatal("edge-abandoned origin request should not reset the stream")
	}
	if resetTunnelOnOriginError(nil, nil) {
		t.Fatal("nil origin error should not reset")
	}
}

func TestOriginClientReuseFlag(t *testing.T) {
	pooled := optsOriginClient(time.Second, true, false)
	tr, ok := pooled.Transport.(*http.Transport)
	if !ok || tr.DisableKeepAlives {
		t.Fatal("reuse=true should keep origin connections pooled")
	}
	fresh := optsOriginClient(time.Second, false, true)
	tr, ok = fresh.Transport.(*http.Transport)
	if !ok || !tr.DisableKeepAlives || !tr.ForceAttemptHTTP2 {
		t.Fatal("reuse=false http2=true should disable keep-alives and allow h2")
	}
}

func TestWriteStatus(t *testing.T) {
	var buf bytes.Buffer
	if err := writeStatus(&buf, http.StatusBadRequest); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(&buf), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}
