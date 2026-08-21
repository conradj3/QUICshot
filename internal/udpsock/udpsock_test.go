package udpsock

import (
	"strings"
	"testing"
)

func TestParseCounters(t *testing.T) {
	input := `Ip: Forwarding DefaultTTL
Ip: 2 64
Udp: InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors InCsumErrors IgnoredMulti MemErrors
Udp: 220016 1 2 200818 3 79 4 0 5
`
	counters, ok := parseCounters(strings.NewReader(input))
	if !ok {
		t.Fatal("parseCounters() ok = false, want true")
	}

	if counters.InDatagrams != 220016 || counters.OutDatagrams != 200818 || counters.RcvbufErrors != 3 || counters.SndbufErrors != 79 {
		t.Fatalf("unexpected counters: %+v", counters)
	}
}

func TestCountersSub(t *testing.T) {
	cur := Counters{InDatagrams: 10, InErrors: 7, RcvbufErrors: 5, SndbufErrors: 3}
	prev := Counters{InDatagrams: 4, InErrors: 2, RcvbufErrors: 1, SndbufErrors: 1}

	got := cur.Sub(prev)
	if got.InDatagrams != 6 || got.InErrors != 5 || got.RcvbufErrors != 4 || got.SndbufErrors != 2 {
		t.Fatalf("Sub() = %+v", got)
	}
}
