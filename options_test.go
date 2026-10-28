package gotry

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	c := defaultConfig()
	if c.policy.maxAttempts != 3 {
		t.Errorf("maxAttempts = %d", c.policy.maxAttempts)
	}
	if len(c.retryableStatuses) != 3 || !c.retryableStatuses[502] || !c.retryableStatuses[503] || !c.retryableStatuses[504] {
		t.Errorf("statuses = %v", c.retryableStatuses)
	}
	if c.maxRetryAfter != 30*time.Second {
		t.Errorf("maxRetryAfter = %v", c.maxRetryAfter)
	}
	if len(c.idemHeaders) != 2 || c.idemHeaders[0] != "Idempotency-Key" || c.idemHeaders[1] != "X-Idempotency-Key" {
		t.Errorf("idemHeaders = %v", c.idemHeaders)
	}
	if c.maxBufferedBody != 1<<20 {
		t.Errorf("maxBufferedBody = %d", c.maxBufferedBody)
	}
	if c.budgetRatio != 0.2 || c.budgetMinPerSec != 10 {
		t.Errorf("budget = %v,%v", c.budgetRatio, c.budgetMinPerSec)
	}
	if c.breaker != nil || c.policy.maxHedges != 0 {
		t.Error("breaker/hedging should be off")
	}
	if c.policy.backoff == nil {
		t.Fatal("backoff nil")
	}
	if d := c.policy.backoff.Delay(1, 0); d < 0 || d >= 100*time.Millisecond {
		t.Errorf("Delay(1,0) = %v", d)
	}
}

func TestOptionsApply(t *testing.T) {
	apply := func(o Option) config { c := defaultConfig(); o(&c); return c }
	if c := apply(WithMaxAttempts(0)); c.policy.maxAttempts != 1 {
		t.Errorf("WithMaxAttempts(0) = %d", c.policy.maxAttempts)
	}
	if c := apply(WithMaxAttempts(5)); c.policy.maxAttempts != 5 {
		t.Errorf("WithMaxAttempts(5) = %d", c.policy.maxAttempts)
	}
	b := Constant(time.Second)
	if c := apply(WithBackoff(b)); c.policy.backoff.Delay(1, 0) != time.Second {
		t.Error("WithBackoff")
	}
	if c := apply(WithPerAttemptTimeout(time.Second)); c.policy.perAttemptTimeout != time.Second {
		t.Error("WithPerAttemptTimeout")
	}
	if c := apply(WithClassifier(DefaultClassifier)); c.policy.classifier == nil {
		t.Error("WithClassifier")
	}
	c := apply(WithRetryableStatuses(500))
	if len(c.retryableStatuses) != 1 || !c.retryableStatuses[500] {
		t.Errorf("statuses = %v", c.retryableStatuses)
	}
	if d := defaultConfig(); len(d.retryableStatuses) != 3 {
		t.Error("default map aliased")
	}
	if c := apply(WithMaxRetryAfter(time.Minute)); c.maxRetryAfter != time.Minute {
		t.Error("WithMaxRetryAfter")
	}
	if c := apply(WithNonIdempotentRetries(true)); !c.policy.allowNonIdempotent {
		t.Error("WithNonIdempotentRetries")
	}
	if c := apply(WithIdempotencyKeyHeaders("X-A")); len(c.idemHeaders) != 1 || c.idemHeaders[0] != "X-A" {
		t.Error("WithIdempotencyKeyHeaders")
	}
	if c := apply(WithMaxBufferedBody(5)); c.maxBufferedBody != 5 {
		t.Error("WithMaxBufferedBody")
	}
	if c := apply(WithBudget(0.5, 3)); c.budgetRatio != 0.5 || c.budgetMinPerSec != 3 {
		t.Error("WithBudget")
	}
	if c := apply(WithCircuitBreaker(BreakerConfig{})); c.breaker == nil {
		t.Error("WithCircuitBreaker")
	}
	if c := apply(WithHedging(50*time.Millisecond, 2)); c.policy.hedgeDelay != 50*time.Millisecond || c.policy.maxHedges != 2 {
		t.Error("WithHedging")
	}
	called := false
	if c := apply(WithHooks(Hooks{OnRetry: func(RetryEvent) { called = true }})); c.hooks.OnRetry == nil {
		t.Error("WithHooks")
	} else {
		c.hooks.retry(RetryEvent{})
		if !called {
			t.Error("hook not invoked")
		}
	}
}
