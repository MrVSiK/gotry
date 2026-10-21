package gotry

import (
	"math/rand/v2"
	"time"
)

// Backoff computes how long to wait before a retry.
//
// retry is 1 for the first retry. prev is the previous delay, or 0 when there
// was none. Implementations must be safe for concurrent use.
type Backoff interface {
	Delay(retry int, prev time.Duration) time.Duration
}

// BackoffFunc adapts a function to the Backoff interface.
type BackoffFunc func(retry int, prev time.Duration) time.Duration

// Delay calls f.
func (f BackoffFunc) Delay(retry int, prev time.Duration) time.Duration { return f(retry, prev) }

// Constant returns a Backoff that always waits d.
func Constant(d time.Duration) Backoff {
	return BackoffFunc(func(int, time.Duration) time.Duration { return d })
}

// Exponential returns a Backoff that waits base, 2*base, 4*base, ... capped at max.
func Exponential(base, max time.Duration) Backoff {
	return BackoffFunc(func(retry int, _ time.Duration) time.Duration {
		return expDelay(base, max, retry)
	})
}

// ExponentialJitter is like Exponential but picks a uniformly random delay in
// [0, exp) where exp is the capped exponential value ("full jitter").
func ExponentialJitter(base, max time.Duration) Backoff {
	return BackoffFunc(func(retry int, _ time.Duration) time.Duration {
		exp := expDelay(base, max, retry)
		if exp <= 0 {
			return 0
		}
		return rand.N(exp)
	})
}

// DecorrelatedJitter returns a Backoff that waits a random duration in
// [base, min(max, 3*prev)], so consecutive delays are decorrelated.
func DecorrelatedJitter(base, max time.Duration) Backoff {
	return BackoffFunc(func(_ int, prev time.Duration) time.Duration {
		if prev < base {
			prev = base
		}
		hi := max
		if prev <= max/3 { // avoids overflow of prev*3
			hi = prev * 3
		}
		if hi <= base {
			return min(base, max)
		}
		return min(base+rand.N(hi-base+1), max)
	})
}

// expDelay returns base<<(retry-1), capped at max and safe from overflow.
func expDelay(base, max time.Duration, retry int) time.Duration {
	if base >= max {
		return max
	}
	if retry < 1 {
		retry = 1
	}
	if retry-1 >= 62 {
		return max
	}
	d := base
	for i := 1; i < retry; i++ {
		if d >= max-d { // doubling would reach or pass max
			return max
		}
		d *= 2
	}
	return d
}
