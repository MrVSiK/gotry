# gotry — HTTP client retries and hedging for Go

Date: 2026-10-21
Module: `github.com/MrVSiK/gotry` (repo cloned at `/home/sri/dev/go-retry`)
Package: `gotry` (single package at the module root)

## 1. Intent

A small, dependency-free Go package that makes outbound HTTP calls resilient
to transient failures and slow tail latency. It plugs in as an
`http.RoundTripper`, so existing code keeps calling `client.Get`/`client.Do`
unchanged and any third-party SDK that accepts an `*http.Client` gets the
behaviour for free.

```go
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
```

## 2. Goals

Everything below is in scope for v1 (decided by the user: "all").

**Retry triggers**
1. Connection failures (dial errors, connection reset/refused, DNS errors, unexpected EOF before response).
2. Per-attempt timeouts (distinct from the caller's overall deadline).
3. 5xx responses — default set `502, 503, 504`, configurable.
4. `429` and `503` honouring `Retry-After` (delta-seconds and HTTP-date forms).
5. Custom classifier — user function deciding retry / don't retry.

**Backoff**
6. Constant.
7. Exponential with cap.
8. Exponential with jitter — full jitter and decorrelated jitter.

**Safety**
9. Idempotency gating — auto-retry only idempotent methods; non-idempotent (POST, PATCH, …) only with an idempotency-key header or explicit opt-in.
10. Max attempts and overall deadline (request context).
11. Retry budget — retries capped at a fraction of traffic.
12. Circuit breaker — per host.

**Hedging** — after a delay with no response, send another copy; first acceptable response wins; the rest are cancelled.

**Observability** — callback hooks (`OnRetry`, `OnHedge`, `OnBreakerStateChange`) the user wires to their own logs/metrics (agreed).

## 3. Non-goals

- No logging, metrics, or tracing library integration (no `slog`, no OpenTelemetry). Hooks only.
- No generic `retry.Do(func)` for non-HTTP work.
- No wrapper `Client` type — `RoundTripper` only (decided by the user).
- No server-side behaviour (no retry-throttling headers emitted, no load shedding).
- No distributed / cross-process budget or breaker state.

## 4. Anti-goals (would be a failure even if it "works")

- Retrying a non-idempotent request without the caller's consent (double charges, duplicate orders).
- Retry storms: unbounded retries amplifying an outage. The budget and breaker exist to prevent this.
- Leaking goroutines, connections, or response bodies (losing hedges and discarded retry responses must be drained/closed).
- Cancelling the winning response's context before the caller has read its body.
- Mutating the caller's `*http.Request` (violates the `RoundTripper` contract).
- Sleeping past the caller's deadline instead of returning promptly.
- Any third-party dependency.

## 5. Constraints

- Go toolchain available: 1.26. `go.mod` declares `go 1.25` *(my call: lets tests use `testing/synctest`, which went GA in 1.25, for deterministic time-based tests)*.
- Standard library only, including tests.
- Must be safe for concurrent use by many goroutines (one `Transport` shared by an `http.Client`).
- Must satisfy the `http.RoundTripper` contract: don't modify the request, always close bodies we don't return, return exactly one of `(resp, nil)` / `(nil, err)`.

## 6. Public API

### 6.1 Transport

```go
type Transport struct { /* unexported */ }

func NewTransport(base http.RoundTripper, opts ...Option) *Transport // base nil → http.DefaultTransport
func (t *Transport) RoundTrip(*http.Request) (*http.Response, error)
func (t *Transport) CloseIdleConnections() // forwards to base if it implements it
```

### 6.2 Options (transport-wide defaults)

```go
type Option func(*config)

WithMaxAttempts(n int)                         // total attempts incl. first; default 3; n<1 → 1
WithBackoff(b Backoff)                         // default ExponentialJitter(100ms, 5s)
WithPerAttemptTimeout(d time.Duration)         // default 0 = none
WithClassifier(c Classifier)                   // default DefaultClassifier
WithRetryableStatuses(codes ...int)            // replaces default {502,503,504}; 429 handled separately
WithMaxRetryAfter(d time.Duration)             // default 30s; larger Retry-After → give up
WithNonIdempotentRetries(allow bool)           // default false
WithIdempotencyKeyHeaders(names ...string)     // default "Idempotency-Key", "X-Idempotency-Key"
WithMaxBufferedBody(n int64)                   // default 1 MiB; see §7.3
WithBudget(ratio float64, minPerSec int)       // default (0.2, 10); ratio<=0 disables
WithCircuitBreaker(cfg BreakerConfig)          // default off
WithHedging(delay time.Duration, maxHedges int) // default off
WithHooks(h Hooks)
```

### 6.3 Per-request policy (context)

```go
type Policy struct {
    MaxAttempts        int
    Backoff            Backoff
    PerAttemptTimeout  time.Duration
    Classifier         Classifier
    AllowNonIdempotent *bool          // nil = inherit
    HedgeDelay         time.Duration
    MaxHedges          int            // -1 = disable hedging for this request
}

func WithPolicy(ctx context.Context, p Policy) context.Context
func NoRetry() Policy     // MaxAttempts: 1, MaxHedges: -1
```

Zero-valued fields inherit the transport's setting *(my call)*. Budget, circuit breaker and hooks are transport-wide only and cannot be overridden per request.

### 6.4 Backoff

```go
type Backoff interface {
    // Delay returns the wait before retry number `retry` (1 = first retry).
    // prev is the previous delay (0 before the first retry).
    Delay(retry int, prev time.Duration) time.Duration
}

func Constant(d time.Duration) Backoff
func Exponential(base, max time.Duration) Backoff          // base * 2^(retry-1), capped
func ExponentialJitter(base, max time.Duration) Backoff    // full jitter: rand[0, exp)
func DecorrelatedJitter(base, max time.Duration) Backoff   // min(max, rand[base, prev*3))
type BackoffFunc func(retry int, prev time.Duration) time.Duration
```

Jitter uses `math/rand/v2` (concurrency-safe global source). Overflow-safe: shifting stops once the cap is reached.

### 6.5 Classifier

```go
type Decision int
const (
    Default Decision = iota // defer to the next classifier / built-in rules
    Retry
    DontRetry
)

type Attempt struct {
    Request  *http.Request
    Response *http.Response // nil on error
    Err      error
    Number   int            // 1-based
}

type Classifier func(Attempt) Decision
func DefaultClassifier(Attempt) Decision
```

A user classifier runs first; `Default` falls through to `DefaultClassifier`. Built-in rules:
- caller context cancelled / deadline exceeded → `DontRetry`
- per-attempt timeout fired → `Retry`
- `net.Error` timeout, `*net.OpError` (dial/read/write), `syscall.ECONNRESET`, `ECONNREFUSED`, `io.ErrUnexpectedEOF`, `*net.DNSError` with `IsTemporary || IsNotFound==false` → `Retry`
- TLS certificate errors (`*tls.CertificateVerificationError`, `x509.*`) → `DontRetry`
- status in retryable set, or `429` → `Retry`
- everything else → `DontRetry`

### 6.6 Hooks

```go
type Hooks struct {
    OnRetry              func(RetryEvent)
    OnHedge              func(HedgeEvent)
    OnBreakerStateChange func(host string, from, to BreakerState)
}
type RetryEvent struct { Request *http.Request; Attempt int; Delay time.Duration; StatusCode int; Err error; RetryAfter bool }
type HedgeEvent struct { Request *http.Request; Attempt int; InFlight int }
```

Hooks are called synchronously on the request goroutine; they must be fast and must not read/close response bodies. A panicking hook is not recovered.

### 6.7 Circuit breaker

```go
type BreakerState int // Closed, Open, HalfOpen
type BreakerConfig struct {
    FailureThreshold int           // consecutive failures to open; default 5
    OpenDuration     time.Duration // default 10s
    HalfOpenProbes   int           // concurrent probes in half-open; default 1
}
var ErrCircuitOpen = errors.New("gotry: circuit open")
```

### 6.8 Errors

```go
var ErrCircuitOpen error
type Error struct {    // returned when we give up after ≥2 attempts with an error
    Attempts int
    Err      error     // last attempt's error
}
func (e *Error) Error() string
func (e *Error) Unwrap() error
```

When the final attempt produced a *response* (e.g. a 503), that response is returned with `nil` error — the caller sees the real status. Single-attempt errors are returned unwrapped.

## 7. Behaviour

### 7.1 Request flow (`RoundTrip`)

1. Resolve effective policy (transport config ⊕ context `Policy`).
2. Breaker check for `req.URL.Host`: open → return `ErrCircuitOpen` without calling base.
3. Deposit into budget (every original request earns `ratio` tokens).
4. Determine retry eligibility: idempotent method (GET, HEAD, OPTIONS, TRACE, PUT, DELETE), or idempotency-key header present, or non-idempotent opt-in; *and* body replayable (§7.3). Ineligible → single attempt, no hedging.
5. Run the attempt loop (§7.2). Each attempt uses `req.Clone(attemptCtx)` with a fresh body.
6. Report each attempt's outcome to the breaker.
7. Return the winning/last response or error.

### 7.2 Attempt loop and hedging (unified)

*My call, following gRPC's hedging model:* retries and hedges share one loop and one `MaxAttempts` budget.

- Start attempt 1.
- A new attempt is launched when either:
  - **hedge timer fires** (hedging on, `inFlight < 1 + maxHedges`, attempts remaining, budget allows) — fires `OnHedge`, no backoff; or
  - **an in-flight attempt returns a retryable result** — if other attempts are still in flight, start the next one immediately (no backoff); if none are in flight, wait `backoff` (or `Retry-After`) and then start — fires `OnRetry`.
- The first **non-retryable** result (success, 4xx, non-retryable error) wins: cancel all others, drain+close their bodies, return it.
- When attempts are exhausted and nothing is in flight, return the last result.
- **"Last result" when stopping without a winner** *(my call)*: prefer a response over an error, and among responses, the most recently completed one. The loop retains at most one retryable response at a time: when a newer response arrives, the older one is drained and closed immediately. Errors never displace a retained response. If no response was ever received, return the most recently completed error wrapped in `*Error`. `Error.Attempts` counts every attempt launched, including cancelled hedge losers.
- Before sleeping: if `delay` would exceed the context deadline, give up now and return the last result.
- `Retry-After` (429/503 only): if present and ≤ `MaxRetryAfter` and within deadline, use it instead of backoff; if larger, give up and return that response.
- Each retry and each hedge withdraws one budget token; if the budget is empty, stop launching new attempts (in-flight ones continue).

### 7.3 Request bodies

- `req.Body == nil` or `http.NoBody` → replayable.
- `req.GetBody != nil` → use it per attempt.
- Otherwise read up to `MaxBufferedBody`+1 bytes: if it fits, buffer and replay from memory; if larger, the request is sent once with the already-read prefix stitched back via `io.MultiReader` and is not retried or hedged.

### 7.4 Contexts and body lifetime

- Each attempt runs under `attemptCtx` derived from the request context (plus per-attempt timeout if set, plus cancel for hedge losers).
- The winning response's `Body` is wrapped so that `attemptCtx`'s cancel func runs on `Close()` (and on EOF) — never before. This keeps per-attempt timeouts and hedge cancellation from truncating the body the caller is reading.
- Discarded responses: read up to 64 KiB into `io.Discard`, then close (allows connection reuse without hanging on huge bodies).

### 7.5 Retry budget

*My call:* a Finagle-style token bucket shared across the whole `Transport` (not per host).
- Each original request deposits `ratio` tokens; each retry/hedge withdraws 1.
- Plus a floor of `minPerSec` retries per second regardless of traffic, so low-volume clients can still retry.
- Deposits expire over a 10 s sliding window (implemented as a ring of 1-second buckets, mutex-protected).
- Default `(0.2, 10)` — on by default because it is the main defence against retry storms.

### 7.6 Circuit breaker

- Per `req.URL.Host`, state stored in a `sync.Map` of small mutex-guarded structs.
- Failure = an attempt the classifier considers retryable (5xx in set, 429, connection error, attempt timeout). Caller cancellation is neither success nor failure. Any other response = success.
- Closed → Open after `FailureThreshold` consecutive failures. Open → HalfOpen after `OpenDuration`. HalfOpen admits `HalfOpenProbes` concurrent requests; one success → Closed, one failure → Open.
- **Half-open probes** *(my call, conservative to avoid hammering a recovering host)*: a probe slot is per *request*. A request admitted as a probe gets exactly one attempt: no retries, no hedges. That attempt's outcome alone decides Closed vs Open, and the slot is released when the outcome is reported. Requests arriving in half-open with no free slot get `ErrCircuitOpen`. The admission API therefore returns a token from the `RoundTrip` check that marks the request as a probe, and the attempt loop forces `MaxAttempts=1` and no hedging when it is set.
- Checked before every attempt, including retries and hedges — an open breaker stops further attempts and returns the last result (or `ErrCircuitOpen` if there was none).
- Off by default.

## 8. File layout

All files in the module root, package `gotry`. Each `.go` file has a matching `_test.go`.

| File | Contents |
|---|---|
| `go.mod` | `module github.com/MrVSiK/gotry`, `go 1.25` |
| `doc.go` | Package documentation with usage overview |
| `transport.go` | `Transport`, `NewTransport`, `RoundTrip`, `CloseIdleConnections`; orchestration of §7.1 |
| `attempt.go` | The unified retry/hedge loop (§7.2): launching, result channel, cancellation, winner selection |
| `options.go` | `Option`, `config`, defaults, all `With*` options |
| `policy.go` | `Policy`, `WithPolicy`, `NoRetry`, merge of context policy over config |
| `backoff.go` | `Backoff`, `BackoffFunc`, `Constant`, `Exponential`, `ExponentialJitter`, `DecorrelatedJitter` |
| `classify.go` | `Decision`, `Attempt`, `Classifier`, `DefaultClassifier`, error inspection helpers |
| `retryafter.go` | `Retry-After` parsing (seconds and HTTP-date) |
| `idempotency.go` | Method set and idempotency-key header check |
| `body.go` | Body replay (`GetBody` / buffering / oversize fallback), cancel-on-close body wrapper, drain-and-close |
| `budget.go` | Retry budget token bucket |
| `breaker.go` | `BreakerState`, `BreakerConfig`, per-host breaker registry, `ErrCircuitOpen` |
| `hooks.go` | `Hooks`, `RetryEvent`, `HedgeEvent`, nil-safe invokers |
| `errors.go` | `Error` type |
| `example_test.go` | Runnable `Example*` functions for godoc |
| `internal/testutil/` *(optional)* | Scripted fake `RoundTripper` and helpers for tests |
| `README.md` | Install, quick start, option reference, semantics notes |

## 9. Testing approach

- TDD per file; unit tests against a scripted fake `RoundTripper` (returns a programmed sequence of responses/errors/delays and records calls).
- Time-dependent behaviour (backoff waits, hedge timers, breaker open duration, budget window) tested with `testing/synctest` so tests are fast and deterministic.
- Integration tests with `httptest.Server` for real connection errors, body replay, and `Retry-After`.
- All tests run under `-race`.
- Leak checks: count goroutines after hedging tests; assert every discarded body was closed.

## 10. Left to the builder

- Internal type and helper names not listed above.
- Exact doc-comment wording and README structure.
- Whether `internal/testutil` exists or helpers live in `_test.go` files.
- Implementation of the budget window (as long as it matches §7.5 semantics and is concurrency-safe).
