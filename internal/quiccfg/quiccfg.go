// Package quiccfg is the shared QUIC config for every hop: RFC 9000 + RFC 9369
// version negotiation, datagrams (RFC 9221), and path MTU discovery left on.
package quiccfg

import (
	"time"

	"github.com/quic-go/quic-go"
)

// Client is used by blast and the connector when they dial. http3.Transport
// requires a single QUIC version on the dial path.
func Client(maxIdle, keepAlive time.Duration) *quic.Config {
	c := base(maxIdle, keepAlive)
	c.Versions = []quic.Version{quic.Version1}
	return c
}

// Server is used by the edge on the public HTTP/3 hop (0-RTT allowed).
func Server(maxIdle, keepAlive time.Duration) *quic.Config {
	c := base(maxIdle, keepAlive)
	c.Allow0RTT = true
	return c
}

func base(maxIdle, keepAlive time.Duration) *quic.Config {
	return &quic.Config{
		MaxIdleTimeout:  maxIdle,
		KeepAlivePeriod: keepAlive,
		EnableDatagrams: true,
		Versions:        []quic.Version{quic.Version1, quic.Version2},
	}
}
