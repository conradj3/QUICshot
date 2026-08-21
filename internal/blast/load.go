package blast

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/quic-go/quic-go/http3"
)

func runLoad(ctx context.Context, cfg config, transports []*http3.Transport, rec *recorder, spec *reqSpec) {
	if cfg.mode == "open" {
		runOpen(ctx, cfg, transports, rec, spec)
		return
	}
	runClosed(ctx, cfg, transports, rec, spec)
}

func runClosed(ctx context.Context, cfg config, transports []*http3.Transport, rec *recorder, spec *reqSpec) {
	pacer := newTokenPacer(cfg.rps)
	var wg sync.WaitGroup
	for c := 0; c < cfg.conns; c++ {
		client := &http.Client{Transport: transports[c]}
		for w := 0; w < cfg.workers; w++ {
			wg.Add(1)
			go func(connIdx, workerIdx int) {
				defer wg.Done()
				for ctx.Err() == nil {
					if cfg.requests > 0 && rec.requests.Load() >= cfg.requests {
						return
					}
					if !pacer.wait(ctx) {
						return
					}
					doRequest(ctx, rec, client, spec, connIdx, workerIdx)
				}
			}(c, w)
		}
	}
	wg.Wait()
}

func runOpen(ctx context.Context, cfg config, transports []*http3.Transport, rec *recorder, spec *reqSpec) {
	clients := make([]*http.Client, len(transports))
	for i, tr := range transports {
		clients[i] = &http.Client{Transport: tr}
	}
	inflight := make(chan struct{}, cfg.inflight())
	pacer := newTokenPacer(cfg.rps)
	var wg sync.WaitGroup
	i := 0
	for {
		if !pacer.wait(ctx) {
			break
		}
		if cfg.requests > 0 && rec.offered.Load() >= cfg.requests {
			break
		}
		rec.offered.Add(1)
		select {
		case inflight <- struct{}{}:
		default:
			rec.omitted.Add(1)
			continue
		}
		connIdx := i % len(clients)
		i++
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			defer func() { <-inflight }()
			doRequest(ctx, rec, clients[c], spec, c, 0)
		}(connIdx)
	}
	wg.Wait()
}

// tokenPacer spaces starts at 1/rps. Unlike a shared ticker, late waiters
// do not burst; they take the next slot. nil / rps<=0 means "as fast as possible".
type tokenPacer struct {
	interval time.Duration
	mu       sync.Mutex
	next     time.Time
}

func newTokenPacer(rps float64) *tokenPacer {
	if rps <= 0 {
		return nil
	}
	return &tokenPacer{interval: time.Duration(float64(time.Second) / rps)}
}

func (p *tokenPacer) wait(ctx context.Context) bool {
	if p == nil {
		return ctx.Err() == nil
	}
	p.mu.Lock()
	now := time.Now()
	if p.next.IsZero() || !p.next.After(now) {
		p.next = now.Add(p.interval)
		p.mu.Unlock()
		return ctx.Err() == nil
	}
	d := p.next.Sub(now)
	p.next = p.next.Add(p.interval)
	p.mu.Unlock()
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
