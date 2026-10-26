package gotry

import (
	"sync"
	"time"
)

const budgetWindow = 10 // seconds, also the number of ring buckets

type budgetBucket struct {
	sec         int64
	deposits    int
	withdrawals int
}

// budget is a sliding-window retry budget. Each original request deposits
// credit at the configured ratio; each retry or hedge withdraws one unit.
// A nil *budget is disabled and always allows.
type budget struct {
	mu        sync.Mutex
	ratio     float64
	minPerSec int
	buckets   [budgetWindow]budgetBucket
}

// newBudget returns nil (disabled) when ratio <= 0.
func newBudget(ratio float64, minPerSec int) *budget {
	if ratio <= 0 {
		return nil
	}
	return &budget{ratio: ratio, minPerSec: minPerSec}
}

// current returns the bucket for the given second, zeroing it if it was last
// used for a different second. Caller must hold b.mu.
func (b *budget) current(sec int64) *budgetBucket {
	idx := int(((sec % budgetWindow) + budgetWindow) % budgetWindow)
	bk := &b.buckets[idx]
	if bk.sec != sec {
		*bk = budgetBucket{sec: sec}
	}
	return bk
}

// deposit records one original request.
func (b *budget) deposit() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.current(time.Now().Unix()).deposits++
}

// withdraw reports whether a retry or hedge is permitted, recording it if so.
func (b *budget) withdraw() bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now().Unix()
	var deposits, withdrawals int
	for i := range b.buckets {
		bk := &b.buckets[i]
		if bk.sec > now-budgetWindow && bk.sec <= now {
			deposits += bk.deposits
			withdrawals += bk.withdrawals
		}
	}
	avail := b.ratio*float64(deposits) + float64(b.minPerSec*budgetWindow) - float64(withdrawals)
	if avail < 1 {
		return false
	}
	b.current(now).withdrawals++
	return true
}
