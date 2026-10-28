package gotry

import "time"

// Option configures a Transport.
type Option func(*config)

type config struct {
	policy            effectivePolicy
	retryableStatuses map[int]bool
	maxRetryAfter     time.Duration
	idemHeaders       []string
	maxBufferedBody   int64
	budgetRatio       float64
	budgetMinPerSec   int
	breaker           *BreakerConfig // nil = off
	hooks             Hooks
}

func defaultConfig() config {
	return config{
		policy: effectivePolicy{
			maxAttempts: 3,
			backoff:     ExponentialJitter(100*time.Millisecond, 5*time.Second),
		},
		retryableStatuses: map[int]bool{502: true, 503: true, 504: true},
		maxRetryAfter:     30 * time.Second,
		idemHeaders:       []string{"Idempotency-Key", "X-Idempotency-Key"},
		maxBufferedBody:   1 << 20,
		budgetRatio:       0.2,
		budgetMinPerSec:   10,
	}
}

// WithMaxAttempts sets the total attempts including the first; n < 1 means 1.
func WithMaxAttempts(n int) Option {
	return func(c *config) {
		if n < 1 {
			n = 1
		}
		c.policy.maxAttempts = n
	}
}

// WithBackoff sets the backoff strategy.
func WithBackoff(b Backoff) Option { return func(c *config) { c.policy.backoff = b } }

// WithPerAttemptTimeout bounds each attempt; 0 means no limit.
func WithPerAttemptTimeout(d time.Duration) Option {
	return func(c *config) { c.policy.perAttemptTimeout = d }
}

// WithClassifier sets a custom classifier consulted alongside the built-in one.
func WithClassifier(cl Classifier) Option { return func(c *config) { c.policy.classifier = cl } }

// WithRetryableStatuses replaces the default retryable set {502, 503, 504}.
// 429 is handled separately.
func WithRetryableStatuses(codes ...int) Option {
	return func(c *config) {
		m := make(map[int]bool, len(codes))
		for _, code := range codes {
			m[code] = true
		}
		c.retryableStatuses = m
	}
}

// WithMaxRetryAfter sets the largest Retry-After honoured; larger values make
// the transport give up.
func WithMaxRetryAfter(d time.Duration) Option { return func(c *config) { c.maxRetryAfter = d } }

// WithNonIdempotentRetries allows retrying non-idempotent requests.
func WithNonIdempotentRetries(allow bool) Option {
	return func(c *config) { c.policy.allowNonIdempotent = allow }
}

// WithIdempotencyKeyHeaders sets the headers whose presence marks a request
// as safe to retry.
func WithIdempotencyKeyHeaders(names ...string) Option {
	return func(c *config) { c.idemHeaders = append([]string(nil), names...) }
}

// WithMaxBufferedBody sets the largest request body buffered for replay;
// n < 0 means 0.
func WithMaxBufferedBody(n int64) Option { return func(c *config) { c.maxBufferedBody = n } }

// WithBudget configures the retry budget; ratio <= 0 disables it.
func WithBudget(ratio float64, minPerSec int) Option {
	return func(c *config) {
		c.budgetRatio = ratio
		c.budgetMinPerSec = minPerSec
	}
}

// WithCircuitBreaker enables per-host circuit breaking.
func WithCircuitBreaker(cfg BreakerConfig) Option {
	return func(c *config) {
		cp := cfg
		c.breaker = &cp
	}
}

// WithHedging enables hedged requests: after delay with no response, launch up
// to maxHedges extra attempts. Hedging is on only when both delay and
// maxHedges are positive. Requests with an Upgrade header are never hedged.
func WithHedging(delay time.Duration, maxHedges int) Option {
	return func(c *config) {
		c.policy.hedgeDelay = delay
		c.policy.maxHedges = maxHedges
	}
}

// WithHooks installs observability hooks.
func WithHooks(h Hooks) Option { return func(c *config) { c.hooks = h } }
