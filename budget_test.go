package gotry

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestBudgetRatio(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBudget(0.1, 0)
		for i := 0; i < 100; i++ {
			b.deposit()
		}
		for i := 0; i < 10; i++ {
			if !b.withdraw() {
				t.Fatalf("withdraw %d denied", i)
			}
		}
		if b.withdraw() {
			t.Fatal("11th withdraw should be denied")
		}
	})
}

func TestBudgetDepositsExpire(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBudget(0.5, 0)
		for i := 0; i < 10; i++ {
			b.deposit()
		}
		time.Sleep(11 * time.Second)
		if b.withdraw() {
			t.Fatal("deposits should have expired")
		}
	})
}

func TestBudgetMinPerSecondFloor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBudget(0.1, 1) // floor: 10 per 10s window, no traffic needed
		for i := 0; i < 10; i++ {
			if !b.withdraw() {
				t.Fatalf("floor withdraw %d denied", i)
			}
		}
		if b.withdraw() {
			t.Fatal("floor exceeded")
		}
		time.Sleep(11 * time.Second)
		if !b.withdraw() {
			t.Fatal("withdrawals should have expired")
		}
	})
}

func TestBudgetNilAlwaysAllows(t *testing.T) {
	var b *budget = newBudget(0, 10)
	if b != nil || !b.withdraw() {
		t.Fatal("disabled budget must allow")
	}
	b.deposit() // must not panic
}

func TestBudgetConcurrent(t *testing.T) {
	b := newBudget(0.2, 10)
	var wg sync.WaitGroup
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				b.deposit()
				b.withdraw()
			}
		}()
	}
	wg.Wait()
}
