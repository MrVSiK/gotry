package gotry

import (
	"context"
	"testing"
	"time"
)

func TestPolicyMerge(t *testing.T) {
	base := effectivePolicy{maxAttempts: 3, hedgeDelay: 50 * time.Millisecond, maxHedges: 2}
	if got := base.merge(Policy{}); got.maxAttempts != 3 || got.hedgeDelay != 50*time.Millisecond || got.maxHedges != 2 || got.allowNonIdempotent || got.backoff != nil || got.classifier != nil || got.perAttemptTimeout != 0 {
		t.Errorf("merge(zero) = %+v", got)
	}
	got := base.merge(NoRetry())
	if got.maxAttempts != 1 || got.maxHedges != 0 {
		t.Errorf("merge(NoRetry) = %+v", got)
	}
	tr := true
	got = base.merge(Policy{AllowNonIdempotent: &tr, MaxAttempts: 5})
	if !got.allowNonIdempotent || got.maxAttempts != 5 {
		t.Errorf("merge = %+v", got)
	}
	if got := (effectivePolicy{}).merge(Policy{}); got.maxAttempts != 1 {
		t.Errorf("maxAttempts<1 = %d", got.maxAttempts)
	}
	got = base.merge(Policy{HedgeDelay: time.Second, PerAttemptTimeout: time.Second, Backoff: Constant(1), Classifier: DefaultClassifier})
	if got.hedgeDelay != time.Second || got.perAttemptTimeout != time.Second || got.backoff == nil || got.classifier == nil {
		t.Errorf("merge fields = %+v", got)
	}
}

func TestWithPolicyRoundTrip(t *testing.T) {
	p := Policy{MaxAttempts: 7, MaxHedges: -1}
	got, ok := policyFrom(WithPolicy(context.Background(), p))
	if !ok || got.MaxAttempts != 7 || got.MaxHedges != -1 {
		t.Errorf("got %+v, %v", got, ok)
	}
	if _, ok := policyFrom(context.Background()); ok {
		t.Error("expected false")
	}
}

func TestHooksNilSafe(t *testing.T) {
	var h Hooks
	h.retry(RetryEvent{})
	h.hedge(HedgeEvent{})
	h.breakerChange("h", Closed, Open)
}
