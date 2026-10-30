package gotry

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func doGet(t *testing.T, c *http.Client) (int, error) {
	t.Helper()
	resp, err := c.Get(testURL)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

func TestBreakerOpensAndShortCircuits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 503}}}
		c := newClient(rt, WithMaxAttempts(3),
			WithCircuitBreaker(BreakerConfig{FailureThreshold: 3, OpenDuration: 10 * time.Second}))
		code, err := doGet(t, c)
		if err != nil || code != 503 {
			t.Fatalf("request 1: code=%d err=%v", code, err)
		}
		if n := rt.numCalls(); n != 3 {
			t.Fatalf("calls = %d, want 3", n)
		}
		_, err = doGet(t, c)
		if !errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("request 2 err = %v, want ErrCircuitOpen", err)
		}
		if n := rt.numCalls(); n != 3 {
			t.Fatalf("calls = %d, want 3", n)
		}
	})
}

func TestBreakerStopsRetriesMidRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 503}}}
		c := newClient(rt, WithMaxAttempts(5),
			WithCircuitBreaker(BreakerConfig{FailureThreshold: 2, OpenDuration: 10 * time.Second}))
		code, err := doGet(t, c)
		if err != nil || code != 503 {
			t.Fatalf("code=%d err=%v", code, err)
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

func TestBreakerProbeSingleAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 503}, {status: 503}, {status: 503}, {status: 503}, {status: 200}}}
		c := newClient(rt, WithMaxAttempts(3),
			WithCircuitBreaker(BreakerConfig{FailureThreshold: 3, OpenDuration: 10 * time.Second}))
		if code, err := doGet(t, c); err != nil || code != 503 {
			t.Fatalf("opening request: code=%d err=%v", code, err)
		}
		time.Sleep(10 * time.Second)
		code, err := doGet(t, c) // probe: steps 503, then 200 that must not be used
		if err != nil || code != 503 {
			t.Fatalf("probe: code=%d err=%v", code, err)
		}
		if n := rt.numCalls(); n != 4 {
			t.Fatalf("calls = %d, want 4 (probe is a single attempt)", n)
		}
		if _, err := doGet(t, c); !errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("after failed probe err = %v, want ErrCircuitOpen", err)
		}
		if n := rt.numCalls(); n != 4 {
			t.Fatalf("calls = %d, want 4", n)
		}
	})
}

type breakerChange struct {
	host     string
	from, to BreakerState
}

func TestBreakerProbeSuccessCloses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var changes []breakerChange
		hooks := Hooks{OnBreakerStateChange: func(host string, from, to BreakerState) {
			mu.Lock()
			changes = append(changes, breakerChange{host, from, to})
			mu.Unlock()
		}}
		rt := &scriptRT{steps: []step{{status: 503}, {status: 503}, {status: 200}}}
		c := newClient(rt, WithMaxAttempts(2), WithHooks(hooks),
			WithCircuitBreaker(BreakerConfig{FailureThreshold: 2, OpenDuration: 10 * time.Second}))
		if code, err := doGet(t, c); err != nil || code != 503 {
			t.Fatalf("opening request: code=%d err=%v", code, err)
		}
		time.Sleep(10 * time.Second)
		if code, err := doGet(t, c); err != nil || code != 200 {
			t.Fatalf("probe: code=%d err=%v", code, err)
		}
		if code, err := doGet(t, c); err != nil || code != 200 {
			t.Fatalf("after close: code=%d err=%v", code, err)
		}
		if n := rt.numCalls(); n != 4 {
			t.Fatalf("calls = %d, want 4 (follow-up request admitted)", n)
		}
		want := []breakerChange{
			{"example.test", Closed, Open},
			{"example.test", Open, HalfOpen},
			{"example.test", HalfOpen, Closed},
		}
		mu.Lock()
		defer mu.Unlock()
		if len(changes) != len(want) {
			t.Fatalf("changes = %v, want %v", changes, want)
		}
		for i := range want {
			if changes[i] != want[i] {
				t.Fatalf("changes = %v, want %v", changes, want)
			}
		}
	})
}

func TestBreakerIgnoresCallerCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 200, delay: time.Hour}, {status: 200}}}
		c := newClient(rt, WithCircuitBreaker(BreakerConfig{FailureThreshold: 1, OpenDuration: time.Hour}))
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(time.Second, cancel)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, testURL, nil)
		if resp, err := c.Do(req); err == nil {
			resp.Body.Close()
			t.Fatal("expected cancellation error")
		}
		code, err := doGet(t, c)
		if err != nil || code != 200 {
			t.Fatalf("after cancel: code=%d err=%v", code, err)
		}
	})
}

func TestConcurrentRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const workers, perWorker = 50, 20
		const total = workers * perWorker
		// A burst of failures trips the breaker early; afterwards a mix of
		// fast 503s, slow 200s (hedges fire) and fast 200s keeps most
		// requests flowing without consecutive failures.
		s := make([]step, 20000)
		for i := range s {
			switch {
			case i < 30:
				s[i] = step{status: 503, delay: time.Millisecond}
			case i%4 == 0:
				s[i] = step{status: 503, delay: time.Millisecond}
			case i%4 == 1:
				s[i] = step{status: 200, body: "slow", delay: 20 * time.Millisecond}
			default:
				s[i] = step{status: 200, body: "ok", delay: time.Millisecond}
			}
		}
		rt := &scriptRT{steps: s}
		var retries, hedges, changes atomic.Int64
		hooks := Hooks{
			OnRetry: func(RetryEvent) { retries.Add(1) },
			OnHedge: func(HedgeEvent) { hedges.Add(1) },
			OnBreakerStateChange: func(string, BreakerState, BreakerState) {
				changes.Add(1)
			},
		}
		c := newClient(rt, WithMaxAttempts(3), WithBackoff(Constant(2*time.Millisecond)),
			WithHedging(5*time.Millisecond, 1), WithBudget(0.5, 1000), WithHooks(hooks),
			WithCircuitBreaker(BreakerConfig{FailureThreshold: 5, OpenDuration: 20 * time.Millisecond}))

		var wg sync.WaitGroup
		var ok, rejected, bad, unexpected atomic.Int64
		for range workers {
			wg.Go(func() {
				for range perWorker {
					time.Sleep(10 * time.Millisecond) // lets time pass so the breaker can cycle
					resp, err := c.Get(testURL)
					switch {
					case err == nil && resp != nil:
						ok.Add(1)
						resp.Body.Close()
					case errors.Is(err, ErrCircuitOpen):
						rejected.Add(1)
					case err != nil:
						unexpected.Add(1)
					default:
						bad.Add(1)
					}
				}
			})
		}
		wg.Wait()
		synctest.Wait()
		t.Logf("ok=%d rejected=%d calls=%d retries=%d hedges=%d changes=%d",
			ok.Load(), rejected.Load(), rt.numCalls(), retries.Load(), hedges.Load(), changes.Load())
		if bad.Load() != 0 {
			t.Fatalf("%d requests returned neither response nor error", bad.Load())
		}
		if unexpected.Load() != 0 {
			t.Fatalf("%d requests failed with an error other than ErrCircuitOpen", unexpected.Load())
		}
		if ok.Load()+rejected.Load() != total {
			t.Fatalf("ok=%d rejected=%d, want sum %d", ok.Load(), rejected.Load(), total)
		}
		if ok.Load() < total/2 {
			t.Fatalf("only %d of %d requests reached the transport", ok.Load(), total)
		}
		if n := rt.numCalls(); n <= int(ok.Load()) {
			t.Fatalf("calls = %d, want more than %d successful requests", n, ok.Load())
		}
		if retries.Load() == 0 || hedges.Load() == 0 {
			t.Fatalf("retries=%d hedges=%d, want both > 0", retries.Load(), hedges.Load())
		}
		if changes.Load() < 2 {
			t.Fatalf("breaker transitions = %d, want at least 2", changes.Load())
		}
		if !rt.allClosed() {
			t.Fatal("not all response bodies closed")
		}
	})
}

func TestProbeSlotReleasedWhenClassifierPanics(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &scriptRT{steps: []step{{status: 503}, {status: 200}}}
		c := newClient(rt, WithMaxAttempts(1),
			WithCircuitBreaker(BreakerConfig{FailureThreshold: 1, OpenDuration: time.Second}))
		if code, err := doGet(t, c); err != nil || code != 503 {
			t.Fatalf("request 1: code=%d err=%v", code, err)
		}
		time.Sleep(time.Second)

		panicky := Policy{Classifier: func(Attempt) Decision { panic("classifier boom") }}
		func() {
			defer func() {
				if r := recover(); r != "classifier boom" {
					t.Fatalf("recovered %v, want classifier panic", r)
				}
			}()
			req, _ := http.NewRequestWithContext(WithPolicy(context.Background(), panicky), http.MethodGet, testURL, nil)
			c.Do(req)
		}()
		synctest.Wait()
		if !rt.allClosed() {
			t.Fatal("probe response body not closed after classifier panic")
		}

		code, err := doGet(t, c)
		if err != nil || code != 200 {
			t.Fatalf("next request: code=%d err=%v, want a probe that closes the breaker", code, err)
		}
	})
}
