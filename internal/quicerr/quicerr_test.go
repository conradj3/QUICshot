package quicerr

import (
	"context"
	"errors"
	"net"
	"testing"
)

func TestClassifyCommonErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want Kind
	}{
		{name: "nil", err: nil, want: KindNone},
		{name: "deadline", err: context.DeadlineExceeded, want: KindDeadline},
		{name: "canceled", err: context.Canceled, want: KindCanceled},
		{name: "network timeout", err: &net.DNSError{IsTimeout: true}, want: KindNetworkTimeout},
		{name: "connection refused", err: errors.New("dial udp: connection refused"), want: KindConnRefused},
		{name: "buffer pressure", err: errors.New("write udp: no buffer space available"), want: KindBufferPressure},
		{name: "unknown", err: errors.New("something else"), want: KindUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := Classify(tt.err)
			if got != tt.want {
				t.Fatalf("Classify() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestCloseReasonLocalCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	kind, detail := CloseReason(ctx)
	if kind != KindNone || detail != "closed locally" {
		t.Fatalf("CloseReason() = %s %q, want local close", kind, detail)
	}
}
