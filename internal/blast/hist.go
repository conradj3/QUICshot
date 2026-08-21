package blast

import (
	"sort"
	"time"
)

type latencyHist struct {
	samples []time.Duration
	seen    int
}

func newLatencyHist(max int) *latencyHist {
	if max < 1 {
		max = 1
	}
	return &latencyHist{samples: make([]time.Duration, 0, max)}
}

func (h *latencyHist) add(d time.Duration) {
	if h == nil {
		return
	}
	h.seen++
	if len(h.samples) < cap(h.samples) {
		h.samples = append(h.samples, d)
		return
	}
	h.samples[h.seen%cap(h.samples)] = d
}

func (h *latencyHist) len() int {
	if h == nil {
		return 0
	}
	return len(h.samples)
}

func (h *latencyHist) sorted() []time.Duration {
	out := append([]time.Duration(nil), h.samples...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
