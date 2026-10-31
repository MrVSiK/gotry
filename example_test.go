package gotry_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	"github.com/MrVSiK/gotry"
)

func ExampleNewTransport() {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer srv.Close()

	client := &http.Client{Transport: gotry.NewTransport(nil,
		gotry.WithMaxAttempts(3),
		gotry.WithBackoff(gotry.Constant(time.Millisecond)),
	)}
	resp, err := client.Get(srv.URL)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()
	fmt.Println(resp.StatusCode)
	// Output: 200
}

func ExampleWithPolicy() {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	client := &http.Client{Transport: gotry.NewTransport(nil,
		gotry.WithBackoff(gotry.Constant(time.Millisecond)))}
	ctx := gotry.WithPolicy(context.Background(), gotry.NoRetry())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	resp, err := client.Do(req) // one attempt only: the 503 is returned as is
	if err != nil {
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()
	fmt.Println(resp.StatusCode)
	// Output: 503
}

func ExampleWithHedging() {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			<-r.Context().Done() // slow first attempt; cancelled once the hedge wins
		}
	}))
	defer srv.Close()

	client := &http.Client{Transport: gotry.NewTransport(nil,
		gotry.WithHedging(20*time.Millisecond, 1))}
	resp, err := client.Get(srv.URL)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()
	fmt.Println(resp.StatusCode)
	// Output: 200
}

func ExampleWithCircuitBreaker() {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	client := &http.Client{Transport: gotry.NewTransport(nil,
		gotry.WithMaxAttempts(1),
		gotry.WithCircuitBreaker(gotry.BreakerConfig{FailureThreshold: 2, OpenDuration: time.Minute}),
	)}
	for range 2 {
		resp, err := client.Get(srv.URL)
		if err != nil {
			fmt.Println(err)
			return
		}
		resp.Body.Close()
		fmt.Println(resp.StatusCode)
	}
	_, err := client.Get(srv.URL)
	fmt.Println(errors.Is(err, gotry.ErrCircuitOpen))
	// Output:
	// 503
	// 503
	// true
}

func ExampleClassifier() {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.Header().Set("X-Retryable", "true")
			w.WriteHeader(http.StatusConflict)
		}
	}))
	defer srv.Close()

	var retryConflict gotry.Classifier = func(a gotry.Attempt) gotry.Decision {
		if a.Response != nil && a.Response.Header.Get("X-Retryable") == "true" {
			return gotry.Retry
		}
		return gotry.Default // defer to the built-in rules
	}
	client := &http.Client{Transport: gotry.NewTransport(nil,
		gotry.WithClassifier(retryConflict),
		gotry.WithBackoff(gotry.Constant(time.Millisecond)),
	)}
	resp, err := client.Get(srv.URL)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()
	fmt.Println(resp.StatusCode)
	// Output: 200
}
