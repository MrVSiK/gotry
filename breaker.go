package gotry

import (
	"errors"
	"sync"
	"time"
)

// BreakerState is the state of a per-host circuit breaker.
type BreakerState int

const (
	// Closed lets all requests through.
	Closed BreakerState = iota
	// Open rejects requests until OpenDuration has elapsed.
	Open
	// HalfOpen lets a limited number of probe requests through.
	HalfOpen
)

func (s BreakerState) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	}
	return "unknown"
}

// BreakerConfig configures per-host circuit breakers. Zero fields take the
// defaults: FailureThreshold 5, OpenDuration 10s, HalfOpenProbes 1.
//
// A half-open probe holds its slot until its single attempt completes, so a
// probe that hangs keeps the breaker half-open with that slot taken. Give
// requests a context deadline or set a per-attempt timeout when the breaker
// is enabled.
type BreakerConfig struct {
	FailureThreshold int
	OpenDuration     time.Duration
	HalfOpenProbes   int
}

// ErrCircuitOpen is returned when a host's circuit breaker rejects a request.
var ErrCircuitOpen = errors.New("gotry: circuit open")

type outcome int

const (
	outcomeSuccess outcome = iota
	outcomeFailure
	outcomeIgnore
)

type hostBreaker struct {
	mu       sync.Mutex
	state    BreakerState
	failures int       // consecutive failures while Closed
	openedAt time.Time // when the breaker last opened
	probes   int       // probe slots in use while HalfOpen
}

type breakers struct {
	cfg      BreakerConfig
	onChange func(host string, from, to BreakerState)
	hosts    sync.Map // string -> *hostBreaker
}

func newBreakers(cfg BreakerConfig, onChange func(host string, from, to BreakerState)) *breakers {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.OpenDuration <= 0 {
		cfg.OpenDuration = 10 * time.Second
	}
	if cfg.HalfOpenProbes <= 0 {
		cfg.HalfOpenProbes = 1
	}
	return &breakers{cfg: cfg, onChange: onChange}
}

func (b *breakers) host(host string) *hostBreaker {
	if v, ok := b.hosts.Load(host); ok {
		return v.(*hostBreaker)
	}
	v, _ := b.hosts.LoadOrStore(host, &hostBreaker{})
	return v.(*hostBreaker)
}

func (b *breakers) notify(host string, from, to BreakerState) {
	if b.onChange != nil && from != to {
		b.onChange(host, from, to)
	}
}

// admit is called once per request. probe is true when the request holds a
// half-open probe slot, which must be released through report.
func (b *breakers) admit(host string) (probe, ok bool) {
	if b == nil {
		return false, true
	}
	h := b.host(host)
	var changes [1][2]BreakerState
	n := 0
	h.mu.Lock()
	if h.state == Open && time.Since(h.openedAt) >= b.cfg.OpenDuration {
		changes[0] = [2]BreakerState{Open, HalfOpen}
		n = 1
		h.state = HalfOpen
		h.probes = 0
	}
	switch h.state {
	case Closed:
		probe, ok = false, true
	case HalfOpen:
		if h.probes < b.cfg.HalfOpenProbes {
			h.probes++
			probe, ok = true, true
		}
	}
	h.mu.Unlock()
	if n == 1 {
		b.notify(host, changes[0][0], changes[0][1])
	}
	return probe, ok
}

// allow is used for second and later attempts. It never takes a probe slot.
func (b *breakers) allow(host string) bool {
	if b == nil {
		return true
	}
	h := b.host(host)
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.state == Closed
}

// report records the outcome of one attempt.
func (b *breakers) report(host string, probe bool, o outcome) {
	if b == nil {
		return
	}
	h := b.host(host)
	from, to := Closed, Closed
	h.mu.Lock()
	switch {
	case probe:
		if h.state == HalfOpen {
			if h.probes > 0 {
				h.probes--
			}
			from = HalfOpen
			to = HalfOpen
			switch o {
			case outcomeSuccess:
				h.state, h.failures, to = Closed, 0, Closed
			case outcomeFailure:
				h.state, h.openedAt, to = Open, time.Now(), Open
			}
		}
	case h.state == Closed:
		from, to = Closed, Closed
		switch o {
		case outcomeSuccess:
			h.failures = 0
		case outcomeFailure:
			h.failures++
			if h.failures >= b.cfg.FailureThreshold {
				h.state, h.openedAt, h.failures = Open, time.Now(), 0
				to = Open
			}
		}
	}
	h.mu.Unlock()
	b.notify(host, from, to)
}
