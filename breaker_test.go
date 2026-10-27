package gotry

import (
	"reflect"
	"testing"
	"testing/synctest"
	"time"
)

type change struct {
	host     string
	from, to BreakerState
}

func recorder() (*[]change, func(string, BreakerState, BreakerState)) {
	var calls []change
	return &calls, func(h string, f, to BreakerState) { calls = append(calls, change{h, f, to}) }
}

func fail(b *breakers, host string, n int) {
	for i := 0; i < n; i++ {
		b.report(host, false, outcomeFailure)
	}
}

func TestBreakerOpensAfterThreshold(t *testing.T) {
	calls, cb := recorder()
	b := newBreakers(BreakerConfig{FailureThreshold: 3}, cb)
	fail(b, "h", 2)
	if _, ok := b.admit("h"); !ok {
		t.Fatal("should admit after 2 failures")
	}
	fail(b, "h", 1)
	if _, ok := b.admit("h"); ok {
		t.Fatal("should deny after 3 failures")
	}
	if want := []change{{"h", Closed, Open}}; !reflect.DeepEqual(*calls, want) {
		t.Fatalf("calls = %v, want %v", *calls, want)
	}
}

func TestBreakerSuccessResetsCount(t *testing.T) {
	b := newBreakers(BreakerConfig{FailureThreshold: 3}, nil)
	fail(b, "h", 2)
	b.report("h", false, outcomeSuccess)
	fail(b, "h", 2)
	if _, ok := b.admit("h"); !ok {
		t.Fatal("should still be closed")
	}
}

func TestBreakerPerHost(t *testing.T) {
	b := newBreakers(BreakerConfig{}, nil)
	fail(b, "a", 5)
	if _, ok := b.admit("a"); ok {
		t.Fatal("a should be denied")
	}
	if _, ok := b.admit("b"); !ok {
		t.Fatal("b should be admitted")
	}
}

func TestBreakerHalfOpenProbe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls, cb := recorder()
		b := newBreakers(BreakerConfig{FailureThreshold: 1, OpenDuration: 10 * time.Second}, cb)
		fail(b, "h", 1)
		if _, ok := b.admit("h"); ok {
			t.Fatal("should be open")
		}
		time.Sleep(10 * time.Second)
		if probe, ok := b.admit("h"); !probe || !ok {
			t.Fatalf("admit = %v,%v want true,true", probe, ok)
		}
		if probe, ok := b.admit("h"); probe || ok {
			t.Fatalf("second admit = %v,%v want false,false", probe, ok)
		}
		if b.allow("h") {
			t.Fatal("allow must be false in half-open")
		}
		b.report("h", true, outcomeSuccess)
		if probe, ok := b.admit("h"); probe || !ok {
			t.Fatalf("after success admit = %v,%v want false,true", probe, ok)
		}
		want := []change{{"h", Closed, Open}, {"h", Open, HalfOpen}, {"h", HalfOpen, Closed}}
		if !reflect.DeepEqual(*calls, want) {
			t.Fatalf("calls = %v, want %v", *calls, want)
		}
	})
}

func TestBreakerProbeFailureReopens(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBreakers(BreakerConfig{FailureThreshold: 1, OpenDuration: 10 * time.Second}, nil)
		fail(b, "h", 1)
		time.Sleep(10 * time.Second)
		probe, ok := b.admit("h")
		if !probe || !ok {
			t.Fatal("expected probe")
		}
		b.report("h", true, outcomeFailure)
		if _, ok := b.admit("h"); ok {
			t.Fatal("should be open again")
		}
		time.Sleep(9 * time.Second)
		if _, ok := b.admit("h"); ok {
			t.Fatal("should still be open before 10s")
		}
		time.Sleep(time.Second)
		if probe, ok := b.admit("h"); !probe || !ok {
			t.Fatal("expected probe after another 10s")
		}
	})
}

func TestBreakerProbeIgnoreReleasesSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBreakers(BreakerConfig{FailureThreshold: 1, OpenDuration: 10 * time.Second}, nil)
		fail(b, "h", 1)
		time.Sleep(10 * time.Second)
		if probe, ok := b.admit("h"); !probe || !ok {
			t.Fatal("expected probe")
		}
		b.report("h", true, outcomeIgnore)
		if probe, ok := b.admit("h"); !probe || !ok {
			t.Fatal("slot should be free again")
		}
	})
}

func TestBreakerLateNonProbeReportsIgnored(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls, cb := recorder()
		b := newBreakers(BreakerConfig{FailureThreshold: 1, OpenDuration: 10 * time.Second}, cb)
		fail(b, "h", 1)
		time.Sleep(10 * time.Second)
		b.admit("h") // probe taken, half-open
		n := len(*calls)
		b.report("h", false, outcomeFailure)
		b.report("h", false, outcomeSuccess)
		if len(*calls) != n {
			t.Fatalf("state changed: %v", *calls)
		}
		if _, ok := b.admit("h"); ok {
			t.Fatal("still half-open with slot taken")
		}
	})
}

func TestBreakerNil(t *testing.T) {
	var b *breakers
	if probe, ok := b.admit("h"); probe || !ok {
		t.Fatal("nil admit")
	}
	if !b.allow("h") {
		t.Fatal("nil allow")
	}
	b.report("h", true, outcomeFailure)
}

func TestBreakerStateString(t *testing.T) {
	if Closed.String() != "closed" || Open.String() != "open" || HalfOpen.String() != "half-open" {
		t.Fatal("bad strings")
	}
}

func TestBreakerConcurrent(t *testing.T) {
	b := newBreakers(BreakerConfig{FailureThreshold: 2, OpenDuration: time.Millisecond}, func(string, BreakerState, BreakerState) {})
	done := make(chan struct{})
	for g := 0; g < 20; g++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := 0; i < 200; i++ {
				probe, ok := b.admit("h")
				b.allow("h")
				if ok {
					o := outcomeFailure
					if i%3 == 0 {
						o = outcomeSuccess
					}
					b.report("h", probe, o)
				}
			}
		}()
	}
	for g := 0; g < 20; g++ {
		<-done
	}
}
