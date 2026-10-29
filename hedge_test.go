package gotry

import (
	"context"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"
)

func TestHedgeFiresAfterDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{
			{status: 200, body: "slow", delay: time.Second},
			{status: 200, body: "fast", delay: 10 * time.Millisecond},
		}}
		c := newClient(rt, WithHedging(50*time.Millisecond, 1))
		start := time.Now()
		resp, err := c.Get(testURL)
		if err != nil {
			t.Fatal(err)
		}
		if got := readBody(t, resp); got != "fast" {
			t.Fatalf("body = %q, want fast", got)
		}
		if d := time.Since(start); d != 60*time.Millisecond {
			t.Fatalf("elapsed = %v, want 60ms", d)
		}
		if n := rt.numCalls(); n != 2 {
			t.Fatalf("calls = %d, want 2", n)
		}
		synctest.Wait()
		if !rt.allClosed() {
			t.Fatal("not all response bodies closed")
		}
	})
}

func TestNoHedgeWhenFastEnough(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 200, delay: 10 * time.Millisecond}}}
		resp, err := newClient(rt, WithHedging(50*time.Millisecond, 1)).Get(testURL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		time.Sleep(time.Second) // a leaked hedge timer would fire here
		if n := rt.numCalls(); n != 1 {
			t.Fatalf("calls = %d, want 1", n)
		}
	})
}

func TestHedgeRespectsMaxHedges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 200, delay: time.Second}}}
		c := newClient(rt, WithHedging(10*time.Millisecond, 2), WithMaxAttempts(10))
		resp, err := c.Get(testURL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if n := rt.numCalls(); n != 3 {
			t.Fatalf("calls = %d, want 3", n)
		}
	})
}

func TestHedgeRespectsMaxAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 200, delay: time.Second}}}
		c := newClient(rt, WithHedging(10*time.Millisecond, 5), WithMaxAttempts(2))
		resp, err := c.Get(testURL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if n := rt.numCalls(); n != 2 {
			t.Fatalf("calls = %d, want 2", n)
		}
	})
}

func TestHedgePrefersResponseOverLaterError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{
			{status: 503, body: "busy", delay: 50 * time.Millisecond},
			{err: syscall.ECONNRESET, delay: 100 * time.Millisecond},
		}}
		c := newClient(rt, WithHedging(10*time.Millisecond, 1), WithMaxAttempts(2))
		resp, err := c.Get(testURL)
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if resp.StatusCode != 503 {
			t.Fatalf("status = %d, want 503", resp.StatusCode)
		}
		if got := readBody(t, resp); got != "busy" {
			t.Fatalf("body = %q, want busy", got)
		}
	})
}

func TestRetryableResultLaunchesImmediatelyWhileHedgeInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{
			{status: 200, delay: time.Second},
			{status: 503, delay: 5 * time.Millisecond},
			{status: 200, body: "third", delay: 5 * time.Millisecond},
		}}
		var retries int
		c := newClient(rt, WithHedging(10*time.Millisecond, 1), WithMaxAttempts(3),
			WithBackoff(Constant(time.Hour)),
			WithHooks(Hooks{OnRetry: func(RetryEvent) { retries++ }}))
		start := time.Now()
		resp, err := c.Get(testURL)
		if err != nil {
			t.Fatal(err)
		}
		if got := readBody(t, resp); got != "third" {
			t.Fatalf("body = %q, want third", got)
		}
		if d := time.Since(start); d >= time.Second {
			t.Fatalf("elapsed = %v, want < 1s (no backoff)", d)
		}
		if retries != 0 {
			t.Fatalf("OnRetry fired %d times, want 0", retries)
		}
		synctest.Wait()
		if !rt.allClosed() {
			t.Fatal("not all response bodies closed")
		}
	})
}

func TestHedgingDisabledForPOST(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 200, delay: time.Second}}}
		c := newClient(rt, WithHedging(10*time.Millisecond, 1))
		resp, err := c.Post(testURL, "text/plain", strings.NewReader("x"))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if n := rt.numCalls(); n != 1 {
			t.Fatalf("calls = %d, want 1", n)
		}
	})
}

func TestHedgingDisabledPerRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 200, delay: time.Second}}}
		c := newClient(rt, WithHedging(10*time.Millisecond, 1))
		ctx := WithPolicy(t.Context(), Policy{MaxHedges: -1})
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, testURL, nil)
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if n := rt.numCalls(); n != 1 {
			t.Fatalf("calls = %d, want 1", n)
		}
	})
}

func TestOnHedgeHook(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{
			{status: 200, delay: time.Second},
			{status: 200, delay: 10 * time.Millisecond},
		}}
		var events []HedgeEvent
		tr := NewTransport(rt, WithHedging(50*time.Millisecond, 1),
			WithHooks(Hooks{OnHedge: func(e HedgeEvent) { events = append(events, e) }}))
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, testURL, nil)
		resp, err := tr.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if len(events) != 1 {
			t.Fatalf("events = %d, want 1", len(events))
		}
		if e := events[0]; e.Attempt != 2 || e.InFlight != 1 || e.Request != req {
			t.Fatalf("event = %+v", e)
		}
	})
}

func TestHedgeConsumesBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 200, delay: 100 * time.Millisecond}}}
		c := newClient(rt, WithBudget(0.1, 0), WithHedging(10*time.Millisecond, 1))
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

func TestHedgeTimerPausedDuringBackoffAndRearmed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{
			{status: 503, delay: 5 * time.Millisecond},
			{status: 200, body: "slow", delay: time.Second},
			{status: 200, body: "fast", delay: 10 * time.Millisecond},
		}}
		var hedges []HedgeEvent
		c := newClient(rt, WithHedging(50*time.Millisecond, 1), WithMaxAttempts(4),
			WithHooks(Hooks{OnHedge: func(e HedgeEvent) { hedges = append(hedges, e) }}))
		start := time.Now()
		resp, err := c.Get(testURL)
		if err != nil {
			t.Fatal(err)
		}
		if got := readBody(t, resp); got != "fast" {
			t.Fatalf("body = %q, want fast", got)
		}
		// 5ms attempt, 1s backoff, then a hedge 50ms after the retry.
		want := 5*time.Millisecond + time.Second + 50*time.Millisecond + 10*time.Millisecond
		if d := time.Since(start); d != want {
			t.Fatalf("elapsed = %v, want %v", d, want)
		}
		if len(hedges) != 1 || hedges[0].Attempt != 3 || hedges[0].InFlight != 1 {
			t.Fatalf("hedges = %+v", hedges)
		}
		synctest.Wait()
		if !rt.allClosed() {
			t.Fatal("not all response bodies closed")
		}
	})
}

func TestNoHedgeForUpgradeRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 200, delay: time.Second}}}
		tr := NewTransport(rt, WithHedging(10*time.Millisecond, 2))
		req := httptestReq(t, nil)
		req.Header.Set("Connection", "Upgrade")
		req.Header.Set("Upgrade", "websocket")
		resp, err := tr.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if n := rt.numCalls(); n != 1 {
			t.Fatalf("calls = %d, want 1 (no hedges for Upgrade requests)", n)
		}
	})
}

func TestHedgingNeedsPositiveDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, tc := range []struct {
			name string
			opts []Option
			ctx  context.Context
		}{
			{"policy MaxHedges without delay", nil, WithPolicy(context.Background(), Policy{MaxHedges: 2})},
			{"WithHedging zero delay", []Option{WithHedging(0, 2)}, context.Background()},
		} {
			rt := &scriptRT{steps: []step{{status: 200, delay: 100 * time.Millisecond}}}
			req, _ := http.NewRequestWithContext(tc.ctx, http.MethodGet, testURL, nil)
			resp, err := NewTransport(rt, tc.opts...).RoundTrip(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if n := rt.numCalls(); n != 1 {
				t.Fatalf("%s: calls = %d, want 1", tc.name, n)
			}
		}
	})
}
