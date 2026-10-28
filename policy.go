package gotry

import (
	"context"
	"time"
)

// Policy overrides transport settings for a single request. Zero-valued fields
// inherit the transport's setting. Budget, circuit breaker and hooks are
// transport-wide only.
type Policy struct {
	MaxAttempts        int
	Backoff            Backoff
	PerAttemptTimeout  time.Duration
	Classifier         Classifier
	AllowNonIdempotent *bool         // nil = inherit
	HedgeDelay         time.Duration // hedging needs a positive delay, here or from WithHedging
	MaxHedges          int           // -1 = disable hedging for this request
}

type policyKey struct{}

// WithPolicy returns a context carrying a per-request Policy.
func WithPolicy(ctx context.Context, p Policy) context.Context {
	return context.WithValue(ctx, policyKey{}, p)
}

// NoRetry returns a Policy that disables retries and hedging.
func NoRetry() Policy { return Policy{MaxAttempts: 1, MaxHedges: -1} }

func policyFrom(ctx context.Context) (Policy, bool) {
	p, ok := ctx.Value(policyKey{}).(Policy)
	return p, ok
}

// effectivePolicy is the resolved per-request policy.
type effectivePolicy struct {
	maxAttempts        int
	backoff            Backoff
	perAttemptTimeout  time.Duration
	classifier         Classifier // nil = builtin only
	allowNonIdempotent bool
	hedgeDelay         time.Duration
	maxHedges          int // 0 = hedging off
}

func (e effectivePolicy) merge(p Policy) effectivePolicy {
	if p.MaxAttempts != 0 {
		e.maxAttempts = p.MaxAttempts
	}
	if p.Backoff != nil {
		e.backoff = p.Backoff
	}
	if p.PerAttemptTimeout != 0 {
		e.perAttemptTimeout = p.PerAttemptTimeout
	}
	if p.Classifier != nil {
		e.classifier = p.Classifier
	}
	if p.AllowNonIdempotent != nil {
		e.allowNonIdempotent = *p.AllowNonIdempotent
	}
	if p.HedgeDelay != 0 {
		e.hedgeDelay = p.HedgeDelay
	}
	switch {
	case p.MaxHedges < 0:
		e.maxHedges = 0
	case p.MaxHedges > 0:
		e.maxHedges = p.MaxHedges
	}
	if e.maxAttempts < 1 {
		e.maxAttempts = 1
	}
	return e
}
