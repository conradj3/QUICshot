// Package qlogtrace enables quic-go qlog output for debugging QUIC handshakes,
// loss recovery, idle timeouts and stream shutdowns.
package qlogtrace

import (
	"fmt"
	"os"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
)

// Configure enables qlog tracing on conf when dir is non-empty. quic-go writes
// one .sqlog file per connection under QLOGDIR.
func Configure(conf *quic.Config, dir string) error {
	if dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create qlog dir: %w", err)
	}
	if err := os.Setenv("QLOGDIR", dir); err != nil {
		return fmt.Errorf("set QLOGDIR: %w", err)
	}
	conf.Tracer = qlog.DefaultConnectionTracer
	return nil
}
