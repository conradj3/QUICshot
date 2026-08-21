// Package quicerr turns a QUIC/HTTP3 error into a stable label so that
// disconnect reasons can be counted and compared across runs.
package quicerr

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// Kind is a coarse bucket. Detail carries the precise wire-level reason.
type Kind string

const (
	KindNone           Kind = "none"
	KindIdleTimeout    Kind = "quic_idle_timeout"
	KindHandshake      Kind = "quic_handshake_failure"
	KindStatelessReset Kind = "quic_stateless_reset"
	KindVersionNegot   Kind = "quic_version_negotiation"
	KindTransportError Kind = "quic_transport_error"
	KindApplicationErr Kind = "quic_application_error"
	KindStreamReset    Kind = "quic_stream_reset"
	KindHTTP3          Kind = "http3_error"
	KindDeadline       Kind = "client_deadline"
	KindCanceled       Kind = "client_canceled"
	KindNetworkTimeout Kind = "network_timeout"
	KindConnRefused    Kind = "connection_refused"
	KindBufferPressure Kind = "udp_buffer_pressure"
	KindUnknown        Kind = "unknown"
)

// Classify maps err onto a Kind plus a human-readable detail string.
func Classify(err error) (Kind, string) {
	if err == nil {
		return KindNone, ""
	}

	var h3 *http3.Error
	if errors.As(err, &h3) {
		return KindHTTP3, fmt.Sprintf("code=%s remote=%t msg=%q", h3.ErrorCode.String(), h3.Remote, h3.ErrorMessage)
	}

	var appErr *quic.ApplicationError
	if errors.As(err, &appErr) {
		return KindApplicationErr, fmt.Sprintf("code=0x%x remote=%t reason=%q", uint64(appErr.ErrorCode), appErr.Remote, appErr.ErrorMessage)
	}

	var transErr *quic.TransportError
	if errors.As(err, &transErr) {
		return KindTransportError, fmt.Sprintf("code=%s remote=%t reason=%q", transErr.ErrorCode.String(), transErr.Remote, transErr.ErrorMessage)
	}

	var streamErr *quic.StreamError
	if errors.As(err, &streamErr) {
		return KindStreamReset, fmt.Sprintf("stream=%d code=0x%x remote=%t", int64(streamErr.StreamID), uint64(streamErr.ErrorCode), streamErr.Remote)
	}

	var idle *quic.IdleTimeoutError
	if errors.As(err, &idle) {
		return KindIdleTimeout, "no recent network activity within max_idle_timeout"
	}

	var hs *quic.HandshakeTimeoutError
	if errors.As(err, &hs) {
		return KindHandshake, "handshake did not complete in time"
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return KindDeadline, err.Error()
	}
	if errors.Is(err, context.Canceled) {
		return KindCanceled, err.Error()
	}

	msg := err.Error()
	switch {
	case strings.Contains(msg, "stateless reset"):
		return KindStatelessReset, msg
	case strings.Contains(msg, "no compatible QUIC version"):
		return KindVersionNegot, msg
	case strings.Contains(msg, "connection refused"):
		return KindConnRefused, msg
	case strings.Contains(msg, "no buffer space") || strings.Contains(msg, "buffer size"):
		return KindBufferPressure, msg
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return KindNetworkTimeout, msg
	}
	return KindUnknown, msg
}

// CloseReason describes why a QUIC connection ended, read from its context cause.
func CloseReason(ctx context.Context) (Kind, string) {
	cause := context.Cause(ctx)
	if cause == nil || errors.Is(cause, context.Canceled) {
		return KindNone, "closed locally"
	}
	return Classify(cause)
}
