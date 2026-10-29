package bandwidth

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestDisabledAndNilNeverWait(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	l := NewLimiter()
	for i := 0; i < 1000; i++ {
		if err := l.Wait(ctx, 1<<20); err != nil {
			t.Fatal(err)
		}
	}
	var nilLimiter *Limiter
	if err := nilLimiter.Wait(ctx, 1<<20); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Errorf("disabled limiter waited %v", time.Since(start))
	}
}

func TestRateIsEnforced(t *testing.T) {
	l := NewLimiter()
	const rate = 1_000_000 // 1 MB/s
	l.SetLimit(rate, true)
	ctx := context.Background()

	// Transfer 1 MB in 16 KiB blocks from 4 goroutines. A new limit starts with an empty bucket,
	// so this must take about 1 s.
	const total, block, workers = 1_000_000, 16 * 1024, 4
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sent := 0; sent < total/workers; sent += block {
				if err := l.Wait(ctx, block); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	if elapsed < 900*time.Millisecond || elapsed > 1400*time.Millisecond {
		t.Errorf("1 MB at 1 MB/s took %v, expected ~1s", elapsed)
	}
}

func TestZeroLimitBlocksUntilChanged(t *testing.T) {
	l := NewLimiter()
	l.SetLimit(0, true)
	done := make(chan error, 1)
	go func() { done <- l.Wait(context.Background(), 100) }()

	select {
	case <-done:
		t.Fatal("an enabled zero limit must block")
	case <-time.After(100 * time.Millisecond):
	}
	l.SetLimit(0, false) // lifting the limit releases waiters immediately
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter not released after the limit was lifted")
	}
}

func TestLimitChangeWakesWaiters(t *testing.T) {
	l := NewLimiter()
	l.SetLimit(1000, true) // 1 kB/s: a 1 MB wait would take ~17 minutes
	done := make(chan struct{})
	go func() {
		_ = l.Wait(context.Background(), 1_000_000)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	l.SetLimit(100_000_000, true)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("raising the limit must re-evaluate blocked waiters")
	}
	if rate, on := l.Limit(); rate != 100_000_000 || !on {
		t.Errorf("Limit() = %d, %v", rate, on)
	}
}

func TestWaitHonoursContext(t *testing.T) {
	l := NewLimiter()
	l.SetLimit(0, true)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := l.Wait(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

func TestWaitAllUsesStrictestLimit(t *testing.T) {
	fast, slow := NewLimiter(), NewLimiter()
	fast.SetLimit(100_000_000, true)
	slow.SetLimit(200_000, true)
	ctx := context.Background()
	start := time.Now()
	// 300 kB through both takes ~1.5 s at the slower 200 kB/s.
	for sent := 0; sent < 300_000; sent += 10_000 {
		if err := WaitAll(ctx, 10_000, fast, nil, slow); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(start); elapsed < 1300*time.Millisecond || elapsed > 2000*time.Millisecond {
		t.Errorf("took %v, expected ~1.5s governed by the slower limiter", elapsed)
	}
}
