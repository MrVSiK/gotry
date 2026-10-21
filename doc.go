// Package gotry provides an http.RoundTripper that adds retries with backoff,
// Retry-After handling, retry budgets and request hedging to outbound HTTP
// calls. It has no dependencies beyond the standard library.
//
// Because it is a RoundTripper, existing call sites keep using
// client.Get and client.Do unchanged, and any SDK that accepts an
// *http.Client picks up the behaviour automatically.
//
//	client := &http.Client{
//	    Transport: gotry.NewTransport(
//	        http.DefaultTransport,
//	        gotry.WithMaxAttempts(4),
//	        gotry.WithBackoff(gotry.ExponentialJitter(100*time.Millisecond, 5*time.Second)),
//	        gotry.WithBudget(0.1, 10),
//	        gotry.WithHedging(50*time.Millisecond, 2),
//	    ),
//	}
//
//	resp, err := client.Get("https://api.example.com/users/1") // unchanged call site
//
//	// per-request override via context
//	ctx = gotry.WithPolicy(ctx, gotry.NoRetry())
//	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
//	resp, err = client.Do(req)
package gotry
