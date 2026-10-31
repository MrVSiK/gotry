package gotry

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestIntegrationRetry503(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		io.WriteString(w, "ok")
	}))
	defer srv.Close()

	c := &http.Client{Transport: NewTransport(nil, WithBackoff(Constant(10*time.Millisecond)))}
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(b) != "ok" {
		t.Fatalf("got %d %q", resp.StatusCode, b)
	}
	if n := hits.Load(); n != 3 {
		t.Fatalf("hits = %d, want 3", n)
	}
}

func TestIntegrationConnectionRefused(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()

	c := &http.Client{Transport: NewTransport(nil, WithMaxAttempts(2), WithBackoff(Constant(10*time.Millisecond)))}
	resp, err := c.Get(url)
	if err == nil {
		resp.Body.Close()
		t.Fatal("expected error")
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("err = %v, want ECONNREFUSED", err)
	}
	var ge *Error
	if !errors.As(err, &ge) || ge.Attempts != 2 {
		t.Fatalf("err = %v, want *Error with 2 attempts", err)
	}
}

func TestIntegrationBodyReplayedToServer(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(b))
		n := len(bodies)
		mu.Unlock()
		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL, bytes.NewReader([]byte("payload")))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Idempotency-Key", "k1")
	c := &http.Client{Transport: NewTransport(nil, WithBackoff(Constant(10*time.Millisecond)))}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	mu.Lock()
	defer mu.Unlock()
	if resp.StatusCode != 200 || len(bodies) != 2 || bodies[0] != "payload" || bodies[1] != bodies[0] {
		t.Fatalf("status %d bodies %q", resp.StatusCode, bodies)
	}
}

func TestIntegrationRetryAfter(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer srv.Close()

	c := &http.Client{Transport: NewTransport(nil, WithBackoff(Constant(time.Millisecond)))}
	start := time.Now()
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	el := time.Since(start)
	if resp.StatusCode != 200 || el < time.Second || el >= 2*time.Second {
		t.Fatalf("status %d elapsed %v", resp.StatusCode, el)
	}
}

// goroutineLeakCheck records the current goroutine count. The returned
// function fails the test unless the count falls back to that baseline within
// two seconds. Call it after closing the server and idle connections.
func goroutineLeakCheck(t *testing.T) func() {
	base := runtime.NumGoroutine()
	return func() {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for {
			n := runtime.NumGoroutine()
			if n <= base {
				return
			}
			if time.Now().After(deadline) {
				buf := make([]byte, 1<<20)
				buf = buf[:runtime.Stack(buf, true)]
				t.Fatalf("goroutines = %d, baseline %d: leak\n%s", n, base, buf)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestIntegrationHedgeCancelsLoser(t *testing.T) {
	checkLeaks := goroutineLeakCheck(t)
	var hits atomic.Int32
	var cancelled atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			<-r.Context().Done()
			cancelled.Store(true)
			return
		}
		io.WriteString(w, "fast")
	}))
	defer srv.Close()

	tr := NewTransport(&http.Transport{}, WithHedging(20*time.Millisecond, 1))
	c := &http.Client{Transport: tr}
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	deadline := time.Now().Add(time.Second)
	for !cancelled.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !cancelled.Load() {
		t.Fatal("slow attempt was not cancelled")
	}
	tr.CloseIdleConnections()
	srv.Close()
	checkLeaks()
}

func TestIntegrationCallerCancelNoLeak(t *testing.T) {
	checkLeaks := goroutineLeakCheck(t)
	var started, cancelled atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started.Add(1)
		<-r.Context().Done()
		cancelled.Add(1)
	}))
	defer srv.Close()

	tr := NewTransport(&http.Transport{}, WithHedging(20*time.Millisecond, 2))
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	resp, err := tr.RoundTrip(req)
	if err == nil {
		resp.Body.Close()
		t.Fatal("expected an error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	deadline := time.Now().Add(time.Second)
	for cancelled.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if s, c := started.Load(), cancelled.Load(); s != 3 || c != 3 {
		t.Fatalf("started %d cancelled %d, want 3 and 3", s, c)
	}
	tr.CloseIdleConnections()
	srv.Close()
	checkLeaks()
}
