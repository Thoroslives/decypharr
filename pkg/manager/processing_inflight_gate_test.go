package manager

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/testutil"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// B5 regression suite. The invariant under test is structural, not
// path-shaped: there must never be two concurrent in-flight downloads (no
// live RegisterDownload overwrite of an existing downloadHandle) for one
// infohash. The hardcoded processingEntries TTL (processor.go:27) plus a
// liveness-blind sweep let any local pull longer than the TTL (every
// throttled UHD remux) get its dedup slot reclaimed mid-flight, re-dispatched,
// and writing a second FD onto the same .mkv inode. The gate is in-flight
// awareness via the existing downloadCancels registry; the chokepoint is
// processAction (RegisterDownload Store overwrites).
//
// Exercised through >=2 paths per the spec: (a) the processQueuedEntries
// TTL-reclaim path (sweep + dispatch), and (b) a direct second processAction
// for an already-registered hash. The absolute-ceiling backstop is covered
// too so a downloadCancels leak cannot permanently wedge re-processing.

// recordingJobQueue wires a real JobQueue whose processFunc records every
// dispatched infohash instead of running the heavy per-protocol handlers, so
// "was a second job submitted?" is a deterministic assertion.
func recordingJobQueue(t *testing.T) (*JobQueue, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var dispatched []string
	done := make(chan struct{}, 64)
	jq := NewJobQueue(context.Background(), 1, func(_ context.Context, job *Job) {
		mu.Lock()
		if job != nil {
			dispatched = append(dispatched, job.ID)
		}
		mu.Unlock()
		done <- struct{}{}
	})
	t.Cleanup(jq.Close)
	return jq, func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := make([]string, len(dispatched))
		copy(out, dispatched)
		return out
	}
}

// TestSweepKeepsInFlightEntry is path (a): a hash with a live downloadCancels
// registration whose processingEntries timestamp is older than the TTL must
// NOT be reclaimed by the sweep. Pre-fix the sweep reclaims purely on age
// (ts.Before(cutoff)) with no liveness check, so the slot is freed mid-pull
// and processQueuedEntries re-dispatches a second concurrent writer.
func TestSweepKeepsInFlightEntry(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())
	clk := testutil.NewFakeClock(time.Now())
	m := &Manager{
		processingEntries: xsync.NewMap[string, time.Time](),
		downloadCancels:   xsync.NewMap[string, *downloadHandle](),
		clock:             clk,
		logger:            zerolog.Nop(),
	}

	const hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	// Live local pull in flight for this hash (the throttled-remux case).
	_, release := m.RegisterDownload(hash, context.Background())
	defer release()

	// Its dedup slot was stamped at dispatch and is now well past the TTL
	// because the pull has been running longer than 5 minutes.
	m.processingEntries.Store(hash, clk.Now())
	clk.Advance(processingEntriesTTL + time.Minute)

	reclaimed := m.sweepProcessingEntries(processingEntriesTTL)

	if reclaimed != 0 {
		t.Fatalf("sweep reclaimed an in-flight slot (%d reclaimed); a second concurrent writer would now be dispatched", reclaimed)
	}
	if _, ok := m.processingEntries.Load(hash); !ok {
		t.Fatal("processingEntries slot for an in-flight download was removed by the sweep")
	}
}

// TestSweepReclaimsLeakedEntryWithoutInFlight confirms the gate does not break
// the sweep's actual purpose: a stale slot with NO live registration (a worker
// that exited without cleanup) is still reclaimed.
func TestSweepReclaimsLeakedEntryWithoutInFlight(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())
	clk := testutil.NewFakeClock(time.Now())
	m := &Manager{
		processingEntries: xsync.NewMap[string, time.Time](),
		downloadCancels:   xsync.NewMap[string, *downloadHandle](),
		clock:             clk,
		logger:            zerolog.Nop(),
	}

	const hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	m.processingEntries.Store(hash, clk.Now())
	clk.Advance(processingEntriesTTL + time.Minute)

	if reclaimed := m.sweepProcessingEntries(processingEntriesTTL); reclaimed != 1 {
		t.Fatalf("expected the leaked (no in-flight) slot to be reclaimed, got %d", reclaimed)
	}
	if _, ok := m.processingEntries.Load(hash); ok {
		t.Fatal("leaked slot with no in-flight download should have been swept")
	}
}

// TestSweepAbsoluteCeilingReclaimsEvenInFlight covers the backstop: a slot far
// past the absolute ceiling is reclaimed even while a download is registered,
// so a future downloadCancels leak cannot convert this self-healing bug into a
// silent permanently-wedged one.
func TestSweepAbsoluteCeilingReclaimsEvenInFlight(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())
	clk := testutil.NewFakeClock(time.Now())
	m := &Manager{
		processingEntries: xsync.NewMap[string, time.Time](),
		downloadCancels:   xsync.NewMap[string, *downloadHandle](),
		clock:             clk,
		logger:            zerolog.Nop(),
	}

	const hash = "cccccccccccccccccccccccccccccccccccccccc"
	_, release := m.RegisterDownload(hash, context.Background())
	defer release()

	m.processingEntries.Store(hash, clk.Now())
	// Far beyond any plausible legit pull AND beyond the absolute ceiling.
	clk.Advance(processingEntriesAbsoluteCeiling + time.Hour)

	if reclaimed := m.sweepProcessingEntries(processingEntriesTTL); reclaimed != 1 {
		t.Fatalf("slot past the absolute ceiling must be reclaimed even when in-flight, got %d reclaimed", reclaimed)
	}
	if _, ok := m.processingEntries.Load(hash); ok {
		t.Fatal("slot past the absolute ceiling should have been swept regardless of in-flight state")
	}
}

// TestProcessActionDoesNotOverwriteLiveRegistration is path (b): a second
// processAction for a hash that already has a live downloadHandle must bail
// before RegisterDownload, leaving the original handle (and thus the original
// worker's cancel/done wiring) untouched. Pre-fix processAction unconditionally
// calls RegisterDownload, whose Store OVERWRITES the existing handle, so a
// DELETE can no longer cancel the real in-flight worker and a second writer
// runs on the same inode.
func TestProcessActionDoesNotOverwriteLiveRegistration(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())

	dbPath := filepath.Join(t.TempDir(), "db")
	strg, err := storage.NewStorage(dbPath)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	m := NewForTest(strg, zerolog.Nop())
	// Wire the minimal surface AddOrUpdate's async RefreshEntries callback
	// touches so the PRE-fix path (which does reach AddOrUpdate) can be
	// observed instead of crashing the test binary on a nil EntryCache.
	// Post-fix the gate returns at the top of processAction and never gets
	// here; downloader stays nil precisely so a missed gate would panic
	// loudly rather than silently pass.
	m.config = config.Get()
	m.entry = NewEntryCache(m)

	const hash = "dddddddddddddddddddddddddddddddddddddddd"
	entry := newAddNewTorrentStyleEntry(hash, "Some.Like.It.Hot.1959.2160p", 1)
	entry.Action = "symlink"

	// First/legitimate worker registers the download and is still running.
	_, release := m.RegisterDownload(hash, context.Background())
	defer release()
	original, ok := m.downloadCancels.Load(hash)
	if !ok {
		t.Fatal("precondition: first RegisterDownload did not store a handle")
	}

	// A second processAction fires for the same in-flight hash (the
	// TTL-reclaim re-dispatch, or an AddNewTorrent->JobTypeNew re-grab).
	// PRE-fix this runs past RegisterDownload (overwriting the live handle),
	// then panics on the intentionally-nil downloader and its deferred
	// release() deletes the (now-overwritten) registration. Recover so the
	// invariant below is asserted instead of the binary dying; reaching the
	// downloader at all already means the gate failed.
	func() {
		defer func() { _ = recover() }()
		m.processAction(entry)
	}()

	got, ok := m.downloadCancels.Load(hash)
	if !ok {
		t.Fatal("downloadCancels lost the in-flight registration after a duplicate processAction (the gate did not bail; RegisterDownload overwrote then release() deleted it). The original worker can no longer be cancelled and a second writer is now running on the same inode")
	}
	if got != original {
		t.Fatal("second processAction overwrote the live downloadHandle (RegisterDownload was not gated); the original worker can no longer be cancelled and a second writer is now running on the same inode")
	}
}

// TestProcessQueuedEntriesSkipsInFlight is the dispatch side of path (a): an
// entry whose hash already has a live download must NOT be re-submitted to the
// JobQueue, and the processingEntries slot taken on the skip path must be
// released so a legitimate future re-process is not blocked. Pre-fix
// processQueuedEntries only checks entry.IsDownloading (a persisted flag that
// the TTL-reclaim path does not flip back) and re-submits.
func TestProcessQueuedEntriesSkipsInFlight(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())

	dbPath := filepath.Join(t.TempDir(), "db")
	strg, err := storage.NewStorage(dbPath)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	m := NewForTest(strg, zerolog.Nop())
	jq, dispatched := recordingJobQueue(t)
	m.jobQueue = jq

	const hash = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	entry := newAddNewTorrentStyleEntry(hash, "12.Angry.Men.1957.2160p", 1)
	dt := representativeDebridTorrent(hash, debridTypes.TorrentStatusDownloading)
	m.attachProvider(entry, dt) // gives ActiveProvider so the torrent branch is reachable
	entry.IsDownloading = false // the TTL-reclaim path leaves this false
	if err := m.queue.Add(entry); err != nil {
		t.Fatalf("queue.Add: %v", err)
	}

	// A local pull is already in flight for this hash.
	_, release := m.RegisterDownload(hash, context.Background())
	defer release()

	// Reproduce the exact B5 race: an OLD liveness-blind sweep already
	// reclaimed this hash's dedup slot mid-pull (the heartbeat keeps it
	// otherwise, so we clear it explicitly to model the leaked-slot state).
	// processQueuedEntries' LoadOrStore therefore takes a FRESH slot and,
	// pre-fix, re-dispatches a second concurrent writer. Post-fix the
	// hasInFlightDownload early-out must skip it AND release the slot it just
	// took so a legitimate future re-process is not wedged until the sweep.
	m.processingEntries.Delete(hash)

	m.processQueuedEntries()

	// Give the (correctly: none) dispatched job a chance to be recorded.
	time.Sleep(100 * time.Millisecond)

	if got := dispatched(); len(got) != 0 {
		t.Fatalf("processQueuedEntries re-submitted an in-flight hash to the JobQueue: %v; a second concurrent writer would run", got)
	}
	if _, held := m.processingEntries.Load(hash); held {
		t.Fatal("processingEntries slot was taken and not released on the in-flight skip path; legitimate future re-processing is now blocked until the sweep")
	}
}
