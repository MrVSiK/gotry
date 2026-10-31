# gotry

`gotry` is a dependency-free Go package that adds retries with backoff,
`Retry-After` handling, a retry budget, per-host circuit breaking and request
hedging to outbound HTTP calls. It is an `http.RoundTripper`, so existing call
sites keep using `client.Get` and `client.Do`, and any SDK that accepts an
`*http.Client` picks up the behaviour.

## Install

```
go get github.com/MrVSiK/gotry
```

## Quick start

```go
ctx := context.Background()
url := "https://api.example.com/orders"
body := strings.NewReader(`{"item":"book"}`)

client := &http.Client{
    Transport: gotry.NewTransport(
        http.DefaultTransport,
        gotry.WithMaxAttempts(4),
        gotry.WithBackoff(gotry.ExponentialJitter(100*time.Millisecond, 5*time.Second)),
        gotry.WithBudget(0.1, 10),
        gotry.WithHedging(50*time.Millisecond, 2),
    ),
}

resp, err := client.Get("https://api.example.com/users/1") // unchanged call site

// per-request override via context
ctx = gotry.WithPolicy(ctx, gotry.NoRetry())
req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
resp, err = client.Do(req)
_, _ = resp, err
```

`NewTransport(nil)` wraps `http.DefaultTransport`. The caller's `*http.Request`
is never modified: every attempt is sent as a clone with its own context and
body.

## Options

| Option | Default | Meaning |
| --- | --- | --- |
| `WithMaxAttempts(n)` | 3 | Total attempts, including the first and any hedges. `n < 1` means 1. |
| `WithBackoff(b)` | `ExponentialJitter(100ms, 5s)` | Delay between retries. Also `Constant`, `Exponential`, `DecorrelatedJitter`, `BackoffFunc`. |
| `WithPerAttemptTimeout(d)` | 0 (none) | Bounds each attempt. A timed-out attempt counts as a retryable failure. |
| `WithClassifier(c)` | none | Custom `Classifier`, consulted before the built-in rules. |
| `WithRetryableStatuses(codes...)` | 502, 503, 504 | Replaces the retryable status set. 429 is always retryable. |
| `WithMaxRetryAfter(d)` | 30s | Largest `Retry-After` that is honoured. |
| `WithNonIdempotentRetries(bool)` | false | Allow retrying non-idempotent requests without a key. |
| `WithIdempotencyKeyHeaders(names...)` | `Idempotency-Key`, `X-Idempotency-Key` | Headers whose presence marks a request safe to retry. |
| `WithMaxBufferedBody(n)` | 1 MiB | Largest request body buffered for replay. `n < 0` means 0. |
| `WithBudget(ratio, minPerSec)` | 0.2, 10 | Retry budget. `ratio <= 0` disables it. |
| `WithCircuitBreaker(cfg)` | off | Per-host circuit breaker. |
| `WithHedging(delay, maxHedges)` | off | Hedged requests. Hedging is on only when both `delay` and `maxHedges` are positive. |
| `WithHooks(h)` | none | Observability callbacks. |

## Per-request policy

`gotry.WithPolicy(ctx, gotry.Policy{...})` overrides settings for one request.
Zero-valued fields inherit the transport's setting.

| Field | Notes |
| --- | --- |
| `MaxAttempts` | |
| `Backoff` | |
| `PerAttemptTimeout` | |
| `Classifier` | |
| `AllowNonIdempotent` | `*bool`; nil inherits. |
| `HedgeDelay` | Hedging needs a positive delay, from here or the transport. |
| `MaxHedges` | `-1` disables hedging for this request. Without a positive `HedgeDelay` (here or from `WithHedging`) no hedges are sent. |

`gotry.NoRetry()` returns `Policy{MaxAttempts: 1, MaxHedges: -1}`. The budget,
circuit breaker and hooks are transport-wide and cannot be overridden per
request.

## What is retried

An attempt is retried when it is classified as retryable:

- a response with status 502, 503, 504 (configurable) or 429;
- connection errors (reset, refused, EOF, unexpected EOF), network timeouts, DNS
  errors other than "not found", and per-attempt timeouts.

Not retried: TLS certificate errors, DNS "not found", context errors
(`context.Canceled`, `context.DeadlineExceeded`, however wrapped; a
per-attempt timeout is distinct and is retried), and every other status or
error.

A `Classifier` can override this per attempt. It receives an `Attempt`
(request, response, error, attempt number) and returns `Retry`, `DontRetry`
or `Default` (use the built-in rules). `ExampleClassifier` retries a 409
response carrying `X-Retryable: true`.

### Retry-After

On a 429 or 503 response that triggers a retry, a `Retry-After` header
(seconds or HTTP date) from that response replaces the backoff delay. It is
honoured only if it is at most `MaxRetryAfter` (default 30s) and fits within
the request's context deadline; otherwise the transport gives up and returns
that response. Other statuses always use the backoff.

## Idempotency and request bodies

A request is eligible for retries and hedging only if both hold:

1. It is idempotent: GET, HEAD, OPTIONS, TRACE, PUT or DELETE; or it carries
   one of the idempotency-key headers; or non-idempotent retries are enabled
   (transport option or per-request policy).
2. Its body is replayable: no body, `http.NoBody`, a `GetBody` function (set
   automatically by `http.NewRequest` for `bytes.Reader`, `bytes.Buffer` and
   `strings.Reader`), or a body of at most `MaxBufferedBody` bytes, which is
   buffered in memory. A larger body is sent once, without retries or hedges.

Ineligible requests get a single attempt. Eligibility is settled before the
body is touched: a request that will get exactly one attempt (ineligible,
`MaxAttempts` 1 such as `NoRetry()`, or a breaker probe) has its body passed
through to the base transport unread and unbuffered, so streaming bodies work.

## Retries and hedges share MaxAttempts

Retries and hedges are one mechanism with one `MaxAttempts` limit.

- Hedging is on when both the hedge delay and `maxHedges` are positive. A
  hedge launches when the hedge delay passes with no result, up to
  `maxHedges` extra attempts in flight, with no backoff.
- Requests carrying an `Upgrade` header (WebSocket, h2c) are never hedged,
  since a losing `101 Switching Protocols` response is a live connection.
  They can still be retried. A returned `101` response body keeps its
  `io.Writer`, so `resp.Body.(io.ReadWriteCloser)` works as with
  `http.Transport`.
- When an attempt returns a retryable result while another attempt is still in
  flight, the next attempt launches immediately. When nothing else is in flight
  the transport waits for the backoff (or `Retry-After`) first.
- Backoff retries are themselves hedged: the hedge timer pauses during the
  backoff sleep and re-arms after the retry launches.
- The first non-retryable result wins; the other in-flight attempts are
  cancelled and their bodies drained (up to 64 KiB) and closed.
- The winning response's context is cancelled only when its body is closed,
  so per-attempt timeouts and hedge cancellation never truncate a body being
  read.

## Retry budget

One token bucket is shared by the whole `Transport`. Each original request
deposits `ratio` tokens, deposits expire over a 10 second window, and
`minPerSec` retries per second are always allowed so low-traffic clients can
still retry. A token is spent only when a retry or hedge actually launches.
When the budget is empty no new attempts launch; in-flight ones continue and
the last result is returned.

## Circuit breaker

Enabled with `WithCircuitBreaker(BreakerConfig{...})`; state is kept per
`req.URL.Host`. Zero fields default to `FailureThreshold` 5, `OpenDuration`
10s, `HalfOpenProbes` 1.

- Retryable results count as failures. Non-retryable responses (including
  4xx) count as successes. Non-retryable errors with no response, and caller
  cancellation, are ignored.
- Closed becomes Open after `FailureThreshold` consecutive failures. Open
  becomes HalfOpen after `OpenDuration`. In HalfOpen, up to `HalfOpenProbes`
  requests are admitted as probes; one success closes the breaker, one failure
  re-opens it. Other requests get `gotry.ErrCircuitOpen`.
- A probe request gets exactly one attempt: no retries and no hedges. The
  probe slot is held until that attempt completes (or the request is
  cancelled), so give requests a context deadline or set
  `WithPerAttemptTimeout` when the breaker is on; a hung probe otherwise keeps
  the breaker half-open with its slot taken.
- The breaker is checked before every attempt, including after backoff
  sleeps. If it opens mid-request, no further attempts launch and the transport
  gives up as described below: the retained retryable response, or the last
  error (wrapped in `*gotry.Error` when more than one attempt launched).
  `ErrCircuitOpen` is returned only when a request is rejected at admission.

## What is returned on give-up

When attempts, budget, `Retry-After` limits or the breaker stop the transport
without a winner:

- the most recent retryable response is returned with a nil error, so callers
  see the real status and body;
- otherwise the last attempt's error is returned. If more than one attempt was
  launched it is wrapped in `*gotry.Error` (`Attempts`, `Err`; supports
  `errors.Is` and `errors.As`); a single-attempt error is returned unwrapped.

`Error.Attempts` counts every attempt launched, including cancelled hedges.
If the caller's context is cancelled or expires, the context's error is
returned directly.

## Hooks

```go
gotry.WithHooks(gotry.Hooks{
    OnRetry:              func(e gotry.RetryEvent) { /* e.Attempt, e.Delay, e.StatusCode, e.Err, e.RetryAfter */ },
    OnHedge:              func(e gotry.HedgeEvent) { /* e.Attempt, e.InFlight */ },
    OnBreakerStateChange: func(host string, from, to gotry.BreakerState) {},
})
```

All fields are optional. Callbacks run synchronously on the request
goroutine, so keep them fast. The events carry no response, only the status
code; the `Request` they carry is the caller's original request, which hooks
must not modify. A panicking hook is not recovered (the panic propagates out of
`RoundTrip`). `OnBreakerStateChange` calls are not strictly
ordered under contention.
