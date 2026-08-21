package qlogtrace

import (
	"os"
	"testing"

	"github.com/quic-go/quic-go"
)

func TestConfigureDisabled(t *testing.T) {
	conf := &quic.Config{}
	if err := Configure(conf, ""); err != nil {
		t.Fatalf("Configure() = %v", err)
	}
	if conf.Tracer != nil {
		t.Fatal("Tracer was set for an empty qlog dir")
	}
}

func TestConfigureEnabled(t *testing.T) {
	dir := t.TempDir() + "/qlog"
	conf := &quic.Config{}

	if err := Configure(conf, dir); err != nil {
		t.Fatalf("Configure() = %v", err)
	}
	if conf.Tracer == nil {
		t.Fatal("Tracer was not set")
	}
	if got := os.Getenv("QLOGDIR"); got != dir {
		t.Fatalf("QLOGDIR = %q, want %q", got, dir)
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		t.Fatalf("qlog dir was not created: info=%v err=%v", info, err)
	}
}
