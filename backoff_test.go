package gotry

import (
	"testing"
	"time"
)

func TestConstant(t *testing.T) {
	b := Constant(250 * time.Millisecond)
	for r := 1; r <= 5; r++ {
		if got := b.Delay(r, 0); got != 250*time.Millisecond {
			t.Fatalf("retry %d: %v", r, got)
		}
	}
}

func TestExponentialDoublesAndCaps(t *testing.T) {
	b := Exponential(100*time.Millisecond, time.Second)
	want := []time.Duration{100, 200, 400, 800, 1000, 1000} // ms
	for i, w := range want {
		if got := b.Delay(i+1, 0); got != w*time.Millisecond {
			t.Fatalf("retry %d: got %v want %v", i+1, got, w*time.Millisecond)
		}
	}
}

func TestExponentialNoOverflow(t *testing.T) {
	b := Exponential(time.Second, time.Minute)
	for _, r := range []int{63, 64, 100, 1 << 20} {
		if got := b.Delay(r, 0); got != time.Minute {
			t.Fatalf("retry %d: %v", r, got)
		}
	}
}

func TestExponentialJitterWithinRange(t *testing.T) {
	b := ExponentialJitter(100*time.Millisecond, time.Second)
	for i := 0; i < 1000; i++ {
		got := b.Delay(3, 0) // ceiling 400ms
		if got < 0 || got >= 400*time.Millisecond {
			t.Fatalf("out of range: %v", got)
		}
	}
	if got := b.Delay(50, 0); got >= time.Second {
		t.Fatalf("cap exceeded: %v", got)
	}
}

func TestDecorrelatedJitterBounds(t *testing.T) {
	base, max := 100*time.Millisecond, 2*time.Second
	b := DecorrelatedJitter(base, max)
	prev := time.Duration(0)
	for i := 1; i <= 1000; i++ {
		got := b.Delay(i, prev)
		if got < base || got > max {
			t.Fatalf("iter %d: %v outside [%v,%v]", i, got, base, max)
		}
		prev = got
	}
}

func TestBackoffFunc(t *testing.T) {
	var b Backoff = BackoffFunc(func(r int, _ time.Duration) time.Duration { return time.Duration(r) * time.Second })
	if b.Delay(3, 0) != 3*time.Second {
		t.Fatal("BackoffFunc not delegating")
	}
}
