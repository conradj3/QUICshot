package connector

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net/http"
	"testing"
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
