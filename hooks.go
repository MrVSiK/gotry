package gotry

import (
	"net/http"
	"time"
)

// Hooks are optional observability callbacks. Every field may be nil.
//
// Hooks run synchronously on the request goroutine, so they must be fast.
// Events carry no response, only the status code; the Request they carry is
// the caller's original request, which hooks must not modify. A panicking
// hook is not recovered: the panic propagates out of RoundTrip.
// Calls to OnBreakerStateChange are not strictly ordered under contention:
// concurrent requests may observe and report transitions out of order.
type Hooks struct {
	OnRetry              func(RetryEvent)
	OnHedge              func(HedgeEvent)
	OnBreakerStateChange func(host string, from, to BreakerState)
}

// RetryEvent describes a retry that is about to happen.
type RetryEvent struct {
	Request    *http.Request
	Attempt    int
	Delay      time.Duration
	StatusCode int
	Err        error
	RetryAfter bool
}

// HedgeEvent describes a hedged request that is about to be launched.
type HedgeEvent struct {
	Request  *http.Request
	Attempt  int
	InFlight int
}

func (h Hooks) retry(e RetryEvent) {
	if h.OnRetry != nil {
		h.OnRetry(e)
	}
}

func (h Hooks) hedge(e HedgeEvent) {
	if h.OnHedge != nil {
		h.OnHedge(e)
	}
}

func (h Hooks) breakerChange(host string, from, to BreakerState) {
	if h.OnBreakerStateChange != nil {
		h.OnBreakerStateChange(host, from, to)
	}
}
