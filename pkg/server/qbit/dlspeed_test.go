package qbit

import (
	"sync"
	"testing"
	"time"
)

// newTestQBit returns a minimal *QBit with the speed-cache maps initialised.
// It avoids New() which requires a real *manager.Manager and calls logger.New().
// deriveDlspeed and pruneSpeedCache only touch the cache fields and the mutex,
// so this is sufficient for all dlspeed tests.
func newTestQBit() *QBit {
	return &QBit{
		speedCache:  make(map[string]speedSample),
		lastDerived: make(map[string]int64),
	}
}

// TestDeriveDlspeed exercises deriveDlspeed across the sequence of edge cases
// that guard against divide-by-zero, negative deltas, and first-call zero.
func TestDeriveDlspeed(t *testing.T) {
	const hash = "aabbccdd"
	t0 := time.Date(2026, 5, 17, 0, 0, 0, 0, time.UTC)

	q := newTestQBit()

	// First call: no prior sample -> 0 (no divide, no panic).
	got := q.deriveDlspeed(hash, 0, t0)
	if got != 0 {
		t.Errorf("first call: got %d, want 0", got)
	}

	// Second call: 100 MiB over 10 s -> ~10 MiB/s = 10*1024*1024.
	const tenMiB = int64(10 * 1024 * 1024)
	const hundredMiB = int64(100 * 1024 * 1024)
	got = q.deriveDlspeed(hash, hundredMiB, t0.Add(10*time.Second))
	const want = tenMiB
	wantF := float64(want)
	tolerance := int64(wantF * 0.01) // 1%
	if got < want-tolerance || got > want+tolerance {
		t.Errorf("10s sample: got %d, want ~%d (±1%%)", got, want)
	}

	// Third call: same timestamp as previous (elapsed = 0) -> must return
	// the previously derived value (no divide-by-zero, no panic).
	prev := got
	sameTime := t0.Add(10 * time.Second)
	got = q.deriveDlspeed(hash, hundredMiB+1024, sameTime)
	if got != prev {
		t.Errorf("same timestamp: got %d, want previously derived %d", got, prev)
	}

	// Fourth call: size went backwards -> delta clamped to 0 -> returns 0.
	got = q.deriveDlspeed(hash, 0, t0.Add(20*time.Second))
	if got != 0 {
		t.Errorf("size went backwards: got %d, want 0", got)
	}
}

// TestPruneSpeedCache guards DA-C5: the cache must be bounded to the live
// torrent set. After pruning, only the hashes in the live set survive.
func TestPruneSpeedCache(t *testing.T) {
	q := newTestQBit()
	now := time.Now()

	// Seed A, B, C into both maps.
	for _, h := range []string{"A", "B", "C"} {
		q.speedCache[h] = speedSample{size: 1000, at: now}
		q.lastDerived[h] = 100
	}

	// Prune to only A.
	q.pruneSpeedCache(map[string]struct{}{"A": {}})

	// A must survive in both maps.
	if _, ok := q.speedCache["A"]; !ok {
		t.Error("speedCache: A should have survived pruning")
	}
	if _, ok := q.lastDerived["A"]; !ok {
		t.Error("lastDerived: A should have survived pruning")
	}

	// B and C must be evicted from both maps.
	for _, evicted := range []string{"B", "C"} {
		if _, ok := q.speedCache[evicted]; ok {
			t.Errorf("speedCache: %s should have been pruned", evicted)
		}
		if _, ok := q.lastDerived[evicted]; ok {
			t.Errorf("lastDerived: %s should have been pruned", evicted)
		}
	}
}

// TestDeriveDlspeedConcurrent verifies the mutex prevents data races when N
// goroutines call deriveDlspeed for distinct hashes simultaneously.
func TestDeriveDlspeedConcurrent(t *testing.T) {
	q := newTestQBit()
	t0 := time.Now()

	const N = 50
	var wg sync.WaitGroup
	wg.Add(N)
	for i := range N {
		go func(i int) {
			defer wg.Done()
			hash := string(rune('a' + i%26))
			q.deriveDlspeed(hash, int64(i*1024*1024), t0.Add(time.Duration(i)*time.Second))
		}(i)
	}
	wg.Wait()
}
