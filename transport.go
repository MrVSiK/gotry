package gotry

import (
	"errors"
	"net/http"
)

// Transport is an http.RoundTripper that retries failed attempts. It is safe
// for concurrent use.
type Transport struct {
	base     http.RoundTripper
	cfg      config
	budget   *budget
	breakers *breakers
}

// NewTransport wraps base, which defaults to http.DefaultTransport when nil.
func NewTransport(base http.RoundTripper, opts ...Option) *Transport {
	if base == nil {
		base = http.DefaultTransport
	}
	cfg := defaultConfig()
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	t := &Transport{
		base:   base,
		cfg:    cfg,
		budget: newBudget(cfg.budgetRatio, cfg.budgetMinPerSec),
	}
	if cfg.breaker != nil {
		t.breakers = newBreakers(*cfg.breaker, cfg.hooks.breakerChange)
	}
	return t
}

// RoundTrip implements http.RoundTripper. It never modifies req; each attempt
// is sent as a clone carrying its own context and body.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL == nil {
		closeReqBody(req)
		return nil, errNilURL
	}
	p := t.cfg.policy
	if pp, ok := policyFrom(req.Context()); ok {
		p = p.merge(pp)
	}
	host := req.URL.Host
	probe, ok := t.breakers.admit(host)
	if !ok {
		closeReqBody(req)
		return nil, ErrCircuitOpen
	}
	t.budget.deposit()

	// Settle how many attempts this request can get before touching its body,
	// so a request that gets one attempt is never buffered.
	eligible := isIdempotentMethod(req.Method) ||
		hasIdempotencyKey(req.Header, t.cfg.idemHeaders) ||
		p.allowNonIdempotent
	if !eligible || probe {
		p.maxAttempts = 1
	}
	// Hedging needs both a positive count and a positive delay. Upgrade
	// requests are never hedged: a losing 101 response is a live connection
	// that cannot be drained.
	if p.maxHedges < 0 || p.hedgeDelay <= 0 || req.Header.Get("Upgrade") != "" {
		p.maxHedges = 0
	}
	// No more than maxAttempts attempts can ever be in flight.
	p.maxHedges = min(p.maxHedges, p.maxAttempts-1)

	if p.maxAttempts == 1 {
		return t.run(req, p, singleUseBody(req), probe)
	}
	src, err := newBodySource(req, t.cfg.maxBufferedBody)
	if err != nil {
		if probe {
			t.breakers.report(host, true, outcomeIgnore)
		}
		return nil, err
	}
	// With GetBody every attempt gets a fresh body, so the original is never
	// sent; close it here to honour the RoundTripper contract.
	if req.GetBody != nil && req.Body != nil && req.Body != http.NoBody {
		req.Body.Close()
	}
	if !src.replayable {
		p.maxAttempts = 1
		p.maxHedges = 0
	}
	return t.run(req, p, src, probe)
}

var errNilURL = errors.New("gotry: nil Request.URL")

func closeReqBody(req *http.Request) {
	if req.Body != nil {
		req.Body.Close()
	}
}

// CloseIdleConnections forwards to the base RoundTripper when it supports it.
func (t *Transport) CloseIdleConnections() {
	if c, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}
