package gotry

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"
)

const testURL = "http://example.test/"

func newClient(rt http.RoundTripper, opts ...Option) *http.Client {
	opts = append([]Option{WithBackoff(Constant(time.Second))}, opts...)
	return &http.Client{Transport: NewTransport(rt, opts...)}
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	resp.Body.Close()
	return string(b)
}

func TestRetriesThenSucceeds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 503}, {status: 503}, {status: 200, body: "ok"}}}
		c := newClient(rt)
		resp, err := c.Get(testURL)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if got := readBody(t, resp); got != "ok" {
			t.Fatalf("body = %q", got)
		}
		if n := rt.numCalls(); n != 3 {
			t.Fatalf("calls = %d, want 3", n)
		}
		if !rt.allClosed() {
			t.Fatal("not all response bodies closed")
		}
	})
}

func TestGivesUpReturnsLastResponse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 503, body: "busy"}}}
		c := newClient(rt, WithMaxAttempts(3))
		resp, err := c.Get(testURL)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != 503 {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if n := rt.numCalls(); n != 3 {
			t.Fatalf("calls = %d, want 3", n)
		}
		bodies := rt.respBodies()
		if !bodies[0].Closed() || !bodies[1].Closed() {
			t.Fatal("discarded bodies not closed")
		}
		if bodies[2].Closed() {
			t.Fatal("returned body closed early")
		}
		if got := readBody(t, resp); got != "busy" {
			t.Fatalf("body = %q", got)
		}
	})
}

func TestNonRetryableStatusReturnedImmediately(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 404}}}
		resp, err := newClient(rt).Get(testURL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 404 || rt.numCalls() != 1 {
			t.Fatalf("status = %d calls = %d", resp.StatusCode, rt.numCalls())
		}
	})
}

func TestConnectionErrorsWrapped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{err: syscall.ECONNRESET}}}
		_, err := newClient(rt).Get(testURL)
		var ge *Error
		if !errors.As(err, &ge) {
			t.Fatalf("err = %v, want *Error", err)
		}
		if ge.Attempts != 3 {
			t.Fatalf("Attempts = %d", ge.Attempts)
		}
		if !errors.Is(err, syscall.ECONNRESET) {
			t.Fatalf("err = %v, want ECONNRESET", err)
		}
	})
}

func doPOST(t *testing.T, c *http.Client, header http.Header) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, testURL, strings.NewReader("data"))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp
}

func TestPOSTNotRetriedByDefault(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 503}, {status: 200}}}
		resp := doPOST(t, newClient(rt), nil)
		if resp.StatusCode != 503 || rt.numCalls() != 1 {
			t.Fatalf("status = %d calls = %d", resp.StatusCode, rt.numCalls())
		}
	})
}

func TestPOSTRetriedWithIdempotencyKey(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 503}, {status: 200}}}
		resp := doPOST(t, newClient(rt), http.Header{"Idempotency-Key": {"k1"}})
		if resp.StatusCode != 200 || rt.numCalls() != 2 {
			t.Fatalf("status = %d calls = %d", resp.StatusCode, rt.numCalls())
		}
	})
}

func TestPOSTRetriedWithOptIn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 503}, {status: 200}}}
		resp := doPOST(t, newClient(rt, WithNonIdempotentRetries(true)), nil)
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d", resp.StatusCode)
		}
	})
}

func TestRetryReplaysBodyWithoutGetBody(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 503}, {status: 200}}}
		req, _ := http.NewRequest(http.MethodPut, testURL, nil)
		req.Body = io.NopCloser(strings.NewReader("payload"))
		req.GetBody = nil
		resp, err := newClient(rt).Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if got, want := rt.gotBodies(), []string{"payload", "payload"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("bodies = %q, want %q", got, want)
		}
	})
}

func TestOversizeBodyNotRetried(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 503}, {status: 200}}}
		req, _ := http.NewRequest(http.MethodPut, testURL, nil)
		orig := newCloseTracker(strings.NewReader("0123456789"))
		req.Body = orig
		req.GetBody = nil
		resp, err := newClient(rt, WithMaxBufferedBody(4)).Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 503 || rt.numCalls() != 1 {
			t.Fatalf("status = %d calls = %d", resp.StatusCode, rt.numCalls())
		}
		if got := rt.gotBodies()[0]; got != "0123456789" {
			t.Fatalf("server got %q", got)
		}
		if !orig.Closed() {
			t.Fatal("original request body not closed")
		}
	})
}

func TestRetryAfterHonoured(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{
			{status: 503, header: http.Header{"Retry-After": {"7"}}},
			{status: 200},
		}}
		start := time.Now()
		resp, err := newClient(rt).Get(testURL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if d := time.Since(start); d != 7*time.Second {
			t.Fatalf("elapsed = %v, want 7s", d)
		}
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d", resp.StatusCode)
		}
	})
}

func TestRetryAfterTooLargeGivesUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{
			{status: 429, header: http.Header{"Retry-After": {"120"}}},
			{status: 200},
		}}
		resp, err := newClient(rt).Get(testURL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 429 || rt.numCalls() != 1 {
			t.Fatalf("status = %d calls = %d", resp.StatusCode, rt.numCalls())
		}
	})
}

func TestRetryAfterBeyondDeadlineReturnsResponse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{
			{status: 503, header: http.Header{"Retry-After": {"20"}}},
			{status: 200},
		}}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, testURL, nil)
		start := time.Now()
		resp, err := newClient(rt).Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 503 || rt.numCalls() != 1 {
			t.Fatalf("status = %d calls = %d", resp.StatusCode, rt.numCalls())
		}
		if d := time.Since(start); d >= time.Second {
			t.Fatalf("elapsed = %v", d)
		}
	})
}

func TestBackoffBeyondDeadlineGivesUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 503}}}
		ctx, cancel := context.WithTimeout(t.Context(), 1500*time.Millisecond)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, testURL, nil)
		resp, err := newClient(rt).Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 503 || rt.numCalls() != 2 {
			t.Fatalf("status = %d calls = %d", resp.StatusCode, rt.numCalls())
		}
	})
}

func TestCancelDuringBackoffReturnsPromptly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 503}}}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		time.AfterFunc(time.Second, cancel)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, testURL, nil)
		start := time.Now()
		_, err := newClient(rt, WithBackoff(Constant(time.Hour))).Do(req)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
		if d := time.Since(start); d != time.Second {
			t.Fatalf("elapsed = %v, want 1s", d)
		}
		if rt.numCalls() != 1 {
			t.Fatalf("calls = %d", rt.numCalls())
		}
		if !rt.allClosed() {
			t.Fatal("503 body not closed")
		}
	})
}

func TestCancelDuringAttemptReturnsPromptly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 200, delay: time.Hour}}}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		time.AfterFunc(time.Second, cancel)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, testURL, nil)
		_, err := newClient(rt).Do(req)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
		if rt.numCalls() != 1 {
			t.Fatalf("calls = %d", rt.numCalls())
		}
	})
}

func TestPerAttemptTimeoutRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 200, delay: time.Hour}, {status: 200, body: "ok"}}}
		resp, err := newClient(rt, WithPerAttemptTimeout(time.Second)).Get(testURL)
		if err != nil {
			t.Fatal(err)
		}
		if got := readBody(t, resp); got != "ok" || resp.StatusCode != 200 {
			t.Fatalf("status = %d body = %q", resp.StatusCode, got)
		}
		if rt.numCalls() != 2 {
			t.Fatalf("calls = %d", rt.numCalls())
		}
	})
}

func TestPerAttemptTimeoutDoesNotTruncateBody(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 200, body: "slow-body"}}}
		resp, err := newClient(rt, WithPerAttemptTimeout(time.Second)).Get(testURL)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Second)
		if err := resp.Request.Context().Err(); err != nil {
			t.Fatalf("attempt context done before body read: %v", err)
		}
		if got := readBody(t, resp); got != "slow-body" {
			t.Fatalf("body = %q", got)
		}
		if resp.Request.Context().Err() == nil {
			t.Fatal("attempt context not released after Close")
		}
	})
}

func TestNoRetryPolicy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 503}, {status: 200}}}
		req, _ := http.NewRequestWithContext(WithPolicy(t.Context(), NoRetry()), http.MethodGet, testURL, nil)
		resp, err := newClient(rt).Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 503 || rt.numCalls() != 1 {
			t.Fatalf("status = %d calls = %d", resp.StatusCode, rt.numCalls())
		}
	})
}

func TestOnRetryHook(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 503}, {status: 502}, {status: 200}}}
		var events []RetryEvent
		record := func(e RetryEvent) { events = append(events, e) }
		resp, err := newClient(rt, WithHooks(Hooks{OnRetry: record})).Get(testURL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if len(events) != 2 {
			t.Fatalf("events = %d, want 2", len(events))
		}
		for i, want := range []int{503, 502} {
			e := events[i]
			if e.Attempt != i+1 || e.StatusCode != want || e.Delay != time.Second || e.RetryAfter || e.Err != nil {
				t.Fatalf("event %d = %+v", i, e)
			}
		}
	})
}

func TestBudgetLimitsRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 503}}}
		c := newClient(rt, WithBudget(0.1, 0), WithMaxAttempts(5))
		for range 10 {
			resp, err := c.Get(testURL)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
		}
		if n := rt.numCalls(); n != 11 {
			t.Fatalf("calls = %d, want 11", n)
		}
	})
}

func TestRequestNotMutated(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 503}, {status: 200}}}
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodPut, testURL, strings.NewReader("payload"))
		req.Header.Set("X-Test", "1")
		wantHeader := req.Header.Clone()
		wantURL := *req.URL
		resp, err := NewTransport(rt, WithBackoff(Constant(time.Second))).RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if !reflect.DeepEqual(req.Header, wantHeader) {
			t.Fatalf("header mutated: %v", req.Header)
		}
		if *req.URL != wantURL {
			t.Fatalf("URL mutated: %v", req.URL)
		}
		if err := req.Context().Err(); err != nil {
			t.Fatalf("request context done: %v", err)
		}
		if resp.Request == req {
			t.Fatal("base saw the caller's request rather than a clone")
		}
	})
}

func TestGetBodyPathClosesOriginalBody(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 200}}}
		req, _ := http.NewRequest(http.MethodPut, testURL, nil)
		orig := newCloseTracker(strings.NewReader("payload"))
		req.Body = orig
		req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("payload")), nil }
		resp, err := NewTransport(rt).RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if !orig.Closed() {
			t.Fatal("original body not closed")
		}
		if got := rt.gotBodies()[0]; got != "payload" {
			t.Fatalf("server got %q", got)
		}
	})
}

func TestCircuitOpenClosesBody(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 503}}}
		tr := NewTransport(rt, WithMaxAttempts(1), WithCircuitBreaker(BreakerConfig{FailureThreshold: 1}))
		resp, err := tr.RoundTrip(httptestReq(t, nil))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		body := newCloseTracker(strings.NewReader("x"))
		_, err = tr.RoundTrip(httptestReq(t, body))
		if !errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("err = %v, want ErrCircuitOpen", err)
		}
		if !body.Closed() {
			t.Fatal("request body not closed")
		}
		if rt.numCalls() != 1 {
			t.Fatalf("calls = %d", rt.numCalls())
		}
	})
}

func httptestReq(t *testing.T, body io.ReadCloser) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, testURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Body = body
	}
	return req
}

type closeIdleRT struct {
	http.RoundTripper
	n int
}

func (c *closeIdleRT) CloseIdleConnections() { c.n++ }

func TestCloseIdleConnectionsForwarded(t *testing.T) {
	base := &closeIdleRT{}
	NewTransport(base).CloseIdleConnections()
	if base.n != 1 {
		t.Fatalf("count = %d, want 1", base.n)
	}
}

func TestRetryAfterTooLargeDoesNotSpendBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		steps := make([]step, 0, 12)
		for range 9 {
			steps = append(steps, step{status: 200})
		}
		steps = append(steps,
			step{status: 429, header: http.Header{"Retry-After": {"120"}}},
			step{status: 503},
			step{status: 200, body: "ok"},
		)
		rt := &scriptRT{steps: steps}
		c := newClient(rt, WithBudget(0.1, 0))
		for range 10 {
			resp, err := c.Get(testURL)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
		}
		// 10 deposits give one token; the 429 gave up without spending it.
		resp, err := c.Get(testURL)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != 200 || readBody(t, resp) != "ok" {
			t.Fatalf("status = %d, want 200 after a budgeted retry", resp.StatusCode)
		}
		if n := rt.numCalls(); n != 12 {
			t.Fatalf("calls = %d, want 12", n)
		}
	})
}

func TestStaleRetryAfterNotReapplied(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{
			{status: 503, header: http.Header{"Retry-After": {"7"}}},
			{err: syscall.ECONNRESET},
			{status: 200},
		}}
		start := time.Now()
		resp, err := newClient(rt).Get(testURL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if d := time.Since(start); d != 8*time.Second {
			t.Fatalf("elapsed = %v, want 8s (Retry-After 7s, then 1s backoff)", d)
		}
	})
}

func TestBreakerOpenedDuringBackoffBlocksRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 503, body: "busy"}}}
		tr := NewTransport(rt, WithBackoff(Constant(time.Second)),
			WithCircuitBreaker(BreakerConfig{FailureThreshold: 2}))
		// Another request fails mid-sleep and opens the breaker.
		time.AfterFunc(500*time.Millisecond, func() {
			ctx := WithPolicy(context.Background(), Policy{MaxAttempts: 1})
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, testURL, nil)
			resp, err := tr.RoundTrip(req)
			if err != nil {
				t.Error(err)
				return
			}
			resp.Body.Close()
		})
		resp, err := tr.RoundTrip(httptestReq(t, nil))
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != 503 || readBody(t, resp) != "busy" {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if n := rt.numCalls(); n != 2 {
			t.Fatalf("calls = %d, want 2 (no retry once the breaker opened)", n)
		}
	})
}

func TestProbeNonRetryableErrorLeavesBreakerHalfOpen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		boom := errors.New("boom")
		rt := &scriptRT{steps: []step{{status: 503}, {err: boom}}}
		tr := NewTransport(rt, WithMaxAttempts(1),
			WithCircuitBreaker(BreakerConfig{FailureThreshold: 1, OpenDuration: 10 * time.Second}))
		resp, err := tr.RoundTrip(httptestReq(t, nil))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		time.Sleep(10 * time.Second)
		if _, err := tr.RoundTrip(httptestReq(t, nil)); !errors.Is(err, boom) {
			t.Fatalf("err = %v, want boom", err)
		}
		h := tr.breakers.host("example.test")
		h.mu.Lock()
		state, probes := h.state, h.probes
		h.mu.Unlock()
		if state != HalfOpen || probes != 0 {
			t.Fatalf("state = %v probes = %d, want half-open with the slot released", state, probes)
		}
	})
}

func TestHugeMaxAttemptsDoesNotPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 503}, {status: 200, body: "ok"}}}
		c := newClient(rt, WithMaxAttempts(math.MaxInt))
		resp, err := c.Get(testURL)
		if err != nil {
			t.Fatal(err)
		}
		if got := readBody(t, resp); got != "ok" {
			t.Fatalf("body = %q", got)
		}

		rt = &scriptRT{steps: []step{{status: 200, body: "slow", delay: 25 * time.Millisecond}}}
		c = newClient(rt, WithMaxAttempts(math.MaxInt), WithHedging(10*time.Millisecond, math.MaxInt))
		resp, err = c.Get(testURL)
		if err != nil {
			t.Fatal(err)
		}
		if got := readBody(t, resp); got != "slow" {
			t.Fatalf("body = %q", got)
		}
		synctest.Wait()
		if !rt.allClosed() {
			t.Fatal("not all response bodies closed")
		}
	})
}

// funcRT adapts a function to http.RoundTripper.
type funcRT func(*http.Request) (*http.Response, error)

func (f funcRT) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestNilResponseBodyBecomesNoBody(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		rt := funcRT(func(r *http.Request) (*http.Response, error) {
			code := 503
			if calls.Add(1) == 3 {
				code = 200
			}
			return &http.Response{StatusCode: code, Header: http.Header{}, Request: r}, nil
		})
		resp, err := newClient(rt).Get(testURL)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != 200 || resp.Body == nil {
			t.Fatalf("status %d body %v", resp.StatusCode, resp.Body)
		}
		if got := readBody(t, resp); got != "" {
			t.Fatalf("body = %q", got)
		}
	})
}

func TestResponseWithErrorTreatedAsError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var bodies []*closeTracker
		var mu sync.Mutex
		rt := funcRT(func(r *http.Request) (*http.Response, error) {
			ct := newCloseTracker(strings.NewReader("partial"))
			mu.Lock()
			bodies = append(bodies, ct)
			mu.Unlock()
			return &http.Response{StatusCode: 200, Header: http.Header{}, Body: ct, Request: r}, io.ErrUnexpectedEOF
		})
		resp, err := newClient(rt, WithMaxAttempts(2)).Get(testURL)
		if resp != nil {
			t.Fatalf("resp = %v, want nil", resp)
		}
		var ge *Error
		if !errors.As(err, &ge) || ge.Attempts != 2 || !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("err = %v, want *Error{2, ErrUnexpectedEOF}", err)
		}
		mu.Lock()
		defer mu.Unlock()
		for i, b := range bodies {
			if !b.Closed() {
				t.Fatalf("body %d not closed", i)
			}
		}
	})
}

func TestSingleAttemptBodyNotBuffered(t *testing.T) {
	cases := []struct {
		name   string
		method string
		ctx    context.Context
	}{
		{"non-idempotent POST", http.MethodPost, context.Background()},
		{"NoRetry PUT", http.MethodPut, WithPolicy(context.Background(), NoRetry())},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The base reads only the first 5 bytes and answers; the body's
			// writer then blocks until the response has been received.
			rt := funcRT(func(r *http.Request) (*http.Response, error) {
				buf := make([]byte, 5)
				if _, err := io.ReadFull(r.Body, buf); err != nil {
					return nil, err
				}
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(string(buf))), Request: r}, nil
			})
			pr, pw := io.Pipe()
			got := make(chan struct{})
			go func() {
				pw.Write([]byte("hello"))
				<-got
				pw.Close()
			}()
			req, _ := http.NewRequestWithContext(tc.ctx, tc.method, testURL, pr)
			type out struct {
				resp *http.Response
				err  error
			}
			ch := make(chan out, 1)
			go func() {
				resp, err := NewTransport(rt).RoundTrip(req)
				ch <- out{resp, err}
			}()
			select {
			case o := <-ch:
				close(got)
				if o.err != nil {
					t.Fatal(o.err)
				}
				if b := readBody(t, o.resp); b != "hello" {
					t.Fatalf("body = %q", b)
				}
			case <-time.After(2 * time.Second):
				close(got)
				t.Fatal("RoundTrip blocked buffering a body that gets a single attempt")
			}
		})
	}
}

func TestNilURLReturnsError(t *testing.T) {
	ct := newCloseTracker(strings.NewReader("x"))
	resp, err := NewTransport(&scriptRT{steps: []step{{status: 200}}}).RoundTrip(&http.Request{Method: http.MethodGet, Header: http.Header{}, Body: ct})
	if resp != nil || err == nil || err.Error() != "gotry: nil Request.URL" {
		t.Fatalf("resp=%v err=%v, want gotry: nil Request.URL", resp, err)
	}
	if !ct.Closed() {
		t.Fatal("request body not closed")
	}
}

// rwBody is a response body that also implements io.Writer, like the body of
// a 101 Switching Protocols response.
type rwBody struct {
	io.Reader
	wrote strings.Builder
}

func (b *rwBody) Write(p []byte) (int, error) { return b.wrote.Write(p) }
func (b *rwBody) Close() error                { return nil }

func TestSwitchingProtocolsBodyKeepsWriter(t *testing.T) {
	body := &rwBody{Reader: strings.NewReader("")}
	rt := funcRT(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusSwitchingProtocols, Header: http.Header{}, Body: body, Request: r}, nil
	})
	req := httptestReq(t, nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	resp, err := NewTransport(rt).RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	rwc, ok := resp.Body.(io.ReadWriteCloser)
	if !ok {
		t.Fatalf("resp.Body (%T) does not implement io.ReadWriteCloser", resp.Body)
	}
	io.WriteString(rwc, "ping")
	rwc.Close()
	if body.wrote.String() != "ping" {
		t.Fatalf("wrote %q, want ping", body.wrote.String())
	}
}
