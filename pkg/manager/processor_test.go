package manager

import (
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/testutil"
)

// newTestManager builds the minimal Manager surface that
// sweepProcessingEntries needs (clock, processingEntries map, logger).
// Avoids manager.New() which would pull config + storage + debrid clients.
func newTestManager(clk Clock) *Manager {
	return &Manager{
		processingEntries: xsync.NewMap[string, time.Time](),
		clock:             clk,
		logger:            zerolog.Nop(),
	}
}

// TestProcessingEntriesSweepRemovesExpired guards G6: processingEntries
// entries leak forever when worker goroutines panic without cleanup,
// blocking future re-processing of the same hash. A periodic sweep removes
// entries older than the configured TTL.
//
// See: /brain/05-Projects/2026-05-15-decypharr-fork-plan.md (Task G6).
func TestProcessingEntriesSweepRemovesExpired(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())
	clk := testutil.NewFakeClock(time.Now())
	m := newTestManager(clk)

	// hash-a marked at t=0
	m.processingEntries.Store("hash-a", clk.Now())
	clk.Advance(2 * time.Minute)
	// hash-b marked at t=2m
	m.processingEntries.Store("hash-b", clk.Now())
	// Advance so hash-a is 6m old, hash-b is 4m old
	clk.Advance(4 * time.Minute)

	reclaimed := m.sweepProcessingEntries(5 * time.Minute)

	if reclaimed != 1 {
		t.Errorf("expected 1 entry reclaimed, got %d", reclaimed)
	}
	if _, ok := m.processingEntries.Load("hash-a"); ok {
		t.Error("expected hash-a to be swept (6 min old, TTL 5 min)")
	}
	if _, ok := m.processingEntries.Load("hash-b"); !ok {
		t.Error("expected hash-b to remain (4 min old, TTL 5 min)")
	}
}

// TestProcessingEntriesSweepKeepsAllWithinTTL confirms the sweep is a no-op
// when every entry is younger than the TTL.
func TestProcessingEntriesSweepKeepsAllWithinTTL(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())
	clk := testutil.NewFakeClock(time.Now())
	m := newTestManager(clk)

	m.processingEntries.Store("hash-a", clk.Now())
	m.processingEntries.Store("hash-b", clk.Now())
	clk.Advance(1 * time.Minute)

	reclaimed := m.sweepProcessingEntries(5 * time.Minute)

	if reclaimed != 0 {
		t.Errorf("expected 0 entries reclaimed, got %d", reclaimed)
	}
	if _, ok := m.processingEntries.Load("hash-a"); !ok {
		t.Error("expected hash-a to remain (1 min old)")
	}
	if _, ok := m.processingEntries.Load("hash-b"); !ok {
		t.Error("expected hash-b to remain (1 min old)")
	}
}

// TestProcessingEntriesSweepClearsAllWhenAllStale confirms every entry past
// the TTL is removed in a single sweep.
func TestProcessingEntriesSweepClearsAllWhenAllStale(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())
	clk := testutil.NewFakeClock(time.Now())
	m := newTestManager(clk)

	m.processingEntries.Store("hash-a", clk.Now())
	m.processingEntries.Store("hash-b", clk.Now())
	clk.Advance(10 * time.Minute)

	reclaimed := m.sweepProcessingEntries(5 * time.Minute)

	if reclaimed != 2 {
		t.Errorf("expected 2 entries reclaimed, got %d", reclaimed)
	}
	count := 0
	m.processingEntries.Range(func(_ string, _ time.Time) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("expected empty map after sweep, got %d entries", count)
	}
}

// TestProcessingEntriesSweepEmpty confirms the sweep is safe on an empty map.
func TestProcessingEntriesSweepEmpty(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())
	clk := testutil.NewFakeClock(time.Now())
	m := newTestManager(clk)

	if reclaimed := m.sweepProcessingEntries(5 * time.Minute); reclaimed != 0 {
		t.Errorf("expected 0 reclaimed from empty map, got %d", reclaimed)
	}
}
