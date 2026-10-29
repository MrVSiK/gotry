package gotry

import (
	"context"
	"errors"
	"net/http"
	"time"
)

var errAttemptTimeout error = attemptTimeoutError{}

var errNilResponse = errors.New("gotry: base RoundTripper returned a nil response and a nil error")

// result is what an attempt goroutine reports back to the loop.
type result struct {
	n      int
	req    *http.Request
	resp   *http.Response
	err    error
	cancel context.CancelCauseFunc // set by the loop on receipt
}

// run executes the attempt loop. All loop state is owned by the calling
// goroutine. Attempt goroutines report through the unbuffered results channel:
// each result is either received by the loop or, once run has returned and
// closed done, drained and closed by the attempt goroutine itself. No send can
// therefore block forever, and no buffer is sized by the policy.
func (t *Transport) run(req *http.Request, p effectivePolicy, src bodySource, probe bool) (*http.Response, error) {
	ctx := req.Context()
	host := req.URL.Host
	results := make(chan result)
	done := make(chan struct{})
	inflight := make(map[int]context.CancelCauseFunc)
	launched := 0

	var (
		retained       *http.Response // most recent retryable response
		retainedCancel context.CancelCauseFunc
		lastErr        error
		prevDelay      time.Duration
		retryNum       int
		pending        *result // received but not yet classified
		probeReported  bool
	)

	report := func(o outcome) {
		if probe {
			probeReported = true
		}
		t.breakers.report(host, probe, o)
	}
	discardRetained := func() {
		if retained != nil {
			retainedCancel(nil)
			drainAndClose(retained.Body)
			retained, retainedCancel = nil, nil
		}
	}
	// The cleanup also runs if a classifier or hook panics: running attempts
	// are cancelled and drain their own results, the result being classified
	// and any retained response are discarded, and a probe slot is released.
	defer func() {
		for _, c := range inflight {
			c(context.Canceled)
		}
		close(done)
		if pending != nil {
			pending.cancel(nil)
			if pending.resp != nil {
				drainAndClose(pending.resp.Body)
			}
		}
		discardRetained()
		if probe && !probeReported {
			t.breakers.report(host, true, outcomeIgnore)
		}
	}()

	deliver := func(res result) {
		select {
		case results <- res:
		case <-done:
			if res.resp != nil {
				drainAndClose(res.resp.Body)
			}
		}
	}

	launch := func() {
		launched++
		n := launched
		actx, cancel := context.WithCancelCause(ctx)
		inflight[n] = cancel
		r := req.Clone(actx)
		body, err := src.get()
		if err != nil {
			// Delivered from a goroutine: the loop cannot send to itself.
			go deliver(result{n: n, req: r, err: err})
			return
		}
		r.Body = body
		r.GetBody = src.getBody
		var timer *time.Timer
		if p.perAttemptTimeout > 0 {
			timer = time.AfterFunc(p.perAttemptTimeout, func() { cancel(errAttemptTimeout) })
		}
		go func() {
			resp, err := t.base.RoundTrip(r)
			if resp != nil && err != nil {
				// Like http.Client: a response returned alongside an error
				// is discarded and the error wins.
				drainAndClose(resp.Body)
				resp = nil
			}
			if resp == nil && err == nil {
				err = errNilResponse
			}
			if resp != nil && resp.Body == nil {
				resp.Body = http.NoBody
			}
			if timer != nil && !timer.Stop() && resp != nil {
				// The per-attempt timer fired as the response arrived, so its
				// body may be cut off: treat the attempt as timed out.
				drainAndClose(resp.Body)
				resp, err = nil, errAttemptTimeout
			}
			if err != nil && context.Cause(actx) == errAttemptTimeout {
				err = errAttemptTimeout
			}
			deliver(result{n: n, req: r, resp: resp, err: err})
		}()
	}

	cancelled := func() (*http.Response, error) {
		if probe {
			report(outcomeIgnore)
		}
		return nil, ctx.Err()
	}
	giveUp := func() (*http.Response, error) {
		if retained != nil {
			resp, c := retained, retainedCancel
			retained, retainedCancel = nil, nil
			resp.Body = wrapBody(resp.Body, func() { c(nil) })
			return resp, nil
		}
		return nil, wrapAttemptsErr(lastErr, launched)
	}

	// canLaunch applies the guards for a launch that is not preceded by a
	// backoff sleep (a hedge, or an immediate re-launch while attempts are in
	// flight). The budget token is withdrawn last, only when the rest pass.
	canLaunch := func() bool {
		return len(inflight) < 1+p.maxHedges && launched < p.maxAttempts &&
			t.breakers.allow(host) && t.budget.withdraw()
	}

	launch()

	// The hedge timer is armed while attempts are in flight and the loop is
	// waiting on results; it is stopped during backoff sleeps and on return.
	var hedgeTimer *time.Timer
	var hedgeC <-chan time.Time
	if p.maxHedges > 0 {
		hedgeTimer = time.NewTimer(p.hedgeDelay)
		defer hedgeTimer.Stop()
		hedgeC = hedgeTimer.C
	}

	for {
		var res result
		select {
		case res = <-results:
		case <-hedgeC:
			if canLaunch() {
				t.cfg.hooks.hedge(HedgeEvent{Request: req, Attempt: launched + 1, InFlight: len(inflight)})
				launch()
				hedgeTimer.Reset(p.hedgeDelay)
			}
			continue
		case <-ctx.Done():
			return cancelled()
		}
		cancel := inflight[res.n]
		delete(inflight, res.n)
		res.cancel = cancel
		pending = &res

		if ctx.Err() != nil {
			return cancelled()
		}

		retry := decide(Attempt{Request: res.req, Response: res.resp, Err: res.err, Number: res.n},
			p.classifier, t.cfg.retryableStatuses)
		switch {
		case retry:
			report(outcomeFailure)
		case res.resp == nil:
			// A non-retryable error says nothing about the host's health.
			report(outcomeIgnore)
		default:
			report(outcomeSuccess)
		}
		pending = nil

		if !retry {
			discardRetained()
			if res.resp != nil {
				res.resp.Body = wrapBody(res.resp.Body, func() { cancel(nil) })
				return res.resp, nil
			}
			cancel(nil)
			return nil, wrapAttemptsErr(res.err, launched)
		}

		if res.resp != nil {
			discardRetained()
			retained, retainedCancel = res.resp, cancel
		} else {
			lastErr = res.err
			cancel(nil)
		}

		if len(inflight) > 0 {
			// Other attempts are still running: replace this one right away,
			// with no backoff, or else keep waiting for them.
			// (Only possible with hedging on, so hedgeTimer is non-nil.)
			if canLaunch() {
				launch()
				hedgeTimer.Reset(p.hedgeDelay)
			}
			continue
		}
		if launched >= p.maxAttempts || !t.breakers.allow(host) {
			return giveUp()
		}

		// Retry-After comes only from the result that triggered this retry;
		// an error result uses backoff even when an older response is retained.
		var delay time.Duration
		usedRetryAfter := false
		if r := res.resp; r != nil && (r.StatusCode == http.StatusTooManyRequests ||
			r.StatusCode == http.StatusServiceUnavailable) {
			if d, ok := parseRetryAfter(r.Header.Get("Retry-After"), time.Now()); ok {
				if d > t.cfg.maxRetryAfter {
					return giveUp()
				}
				delay, usedRetryAfter = d, true
			}
		}
		retryNum++
		if !usedRetryAfter && p.backoff != nil {
			delay = p.backoff.Delay(retryNum, prevDelay)
		}
		delay = max(delay, 0)
		prevDelay = delay
		if dl, ok := ctx.Deadline(); ok && time.Now().Add(delay).After(dl) {
			return giveUp()
		}
		// Spend a token only once the retry is definitely going ahead.
		if !t.budget.withdraw() {
			return giveUp()
		}

		ev := RetryEvent{Request: req, Attempt: res.n, Delay: delay, Err: res.err, RetryAfter: usedRetryAfter}
		if res.resp != nil {
			ev.StatusCode = res.resp.StatusCode
		}
		t.cfg.hooks.retry(ev)

		if hedgeTimer != nil {
			hedgeTimer.Stop()
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return cancelled()
		}
		// Other requests may have opened the breaker while we slept.
		if !t.breakers.allow(host) {
			return giveUp()
		}
		launch()
		if hedgeTimer != nil {
			hedgeTimer.Reset(p.hedgeDelay)
		}
	}
}

// wrapAttemptsErr wraps err in *Error when more than one attempt was made.
func wrapAttemptsErr(err error, attempts int) error {
	if attempts > 1 {
		return &Error{Attempts: attempts, Err: err}
	}
	return err
}
