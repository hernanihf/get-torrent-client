// Package bandwidth provides token-bucket rate limiting for peer data transfer.
package bandwidth

import (
	"context"
	"sync"
	"time"
)

// minBurst is the smallest bucket size: one standard 16 KiB block, so a limited transfer can
// always make progress block by block.
const minBurst = 16 * 1024

// Limiter is a token bucket shared by any number of goroutines. A disabled limiter never waits.
// Waiters reserve tokens up front (the balance may go negative) and sleep off the deficit, which
// keeps the long-run rate exact without starving large requests.
type Limiter struct {
	mu      sync.Mutex
	enabled bool
	rate    float64 // bytes per second
	burst   float64
	tokens  float64
	last    time.Time
	changed chan struct{} // closed and replaced whenever the limit changes
}

// NewLimiter returns a disabled (unlimited) limiter.
func NewLimiter() *Limiter {
	return &Limiter{changed: make(chan struct{})}
}

// SetLimit enables a limit of bytesPerSec, or disables limiting when enabled is false.
// An enabled limit of 0 blocks all transfer until the limit changes. Waiters blocked under the
// previous limit are released immediately.
func (l *Limiter) SetLimit(bytesPerSec int64, enabled bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if bytesPerSec < 0 {
		bytesPerSec = 0
	}
	if l.enabled == enabled && l.rate == float64(bytesPerSec) {
		return
	}
	l.enabled = enabled
	l.rate = float64(bytesPerSec)
	// Allow about half a second of burst, but never less than one block.
	l.burst = max(l.rate/2, minBurst)
	// A new limit starts with an empty bucket, so it is never exceeded right after being set;
	// outstanding reservations under the old limit are forgiven.
	l.tokens = 0
	l.last = time.Now()
	close(l.changed)
	l.changed = make(chan struct{})
}

// Limit reports the current limit in bytes per second and whether it is enabled.
func (l *Limiter) Limit() (int64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return int64(l.rate), l.enabled
}

// refill adds tokens for the time elapsed since the last update. Caller holds mu.
func (l *Limiter) refill(now time.Time) {
	l.tokens = min(l.tokens+now.Sub(l.last).Seconds()*l.rate, l.burst)
	l.last = now
}

// Wait blocks until n bytes may be transferred, the limit changes, or ctx is done.
// A nil Limiter never waits.
func (l *Limiter) Wait(ctx context.Context, n int) error {
	if l == nil || n <= 0 {
		return ctx.Err()
	}
	for {
		l.mu.Lock()
		if !l.enabled {
			l.mu.Unlock()
			return nil
		}
		changed := l.changed
		var delay time.Duration
		blocked := l.rate == 0
		if !blocked {
			l.refill(time.Now())
			l.tokens -= float64(n)
			if l.tokens < 0 {
				delay = time.Duration(-l.tokens / l.rate * float64(time.Second))
			}
		}
		l.mu.Unlock()

		if !blocked && delay == 0 {
			return nil
		}
		var timer *time.Timer
		var fire <-chan time.Time
		if !blocked {
			timer = time.NewTimer(delay)
			fire = timer.C
		}
		select {
		case <-fire:
			return nil
		case <-changed:
			// The limit changed; our reservation was discarded, so re-evaluate under the new one.
			if timer != nil {
				timer.Stop()
			}
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return ctx.Err()
		}
	}
}

// WaitAll waits on each non-nil limiter in turn, so a transfer honours every applicable limit
// (e.g. per-torrent and session-wide).
func WaitAll(ctx context.Context, n int, limiters ...*Limiter) error {
	for _, l := range limiters {
		if err := l.Wait(ctx, n); err != nil {
			return err
		}
	}
	return nil
}
