package quiccfg

import (
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

func TestClientAndServerOfferV1V2AndDatagrams(t *testing.T) {
	c := Client(30*time.Second, time.Second)
	if !c.EnableDatagrams {
		t.Fatal("client datagrams disabled")
	}
	if len(c.Versions) != 1 || c.Versions[0] != quic.Version1 {
		t.Fatalf("client versions = %v (http3 dial requires one version)", c.Versions)
	}
	s := Server(30*time.Second, 0)
	if !s.Allow0RTT || !s.EnableDatagrams {
		t.Fatal("server should allow 0-RTT and datagrams")
	}
	if len(s.Versions) != 2 || s.Versions[1] != quic.Version2 {
		t.Fatalf("server versions = %v", s.Versions)
	}
}
