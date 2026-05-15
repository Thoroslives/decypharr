package manager

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/testutil"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// fakeSizeProber is a contentLengthProber test double. It returns a fixed
// length (or error) regardless of the link, standing in for the real RD HEAD
// call so the store round-trip can be exercised without a network.
type fakeSizeProber struct {
	length int64
	err    error
	calls  int
}

func (f *fakeSizeProber) HeadContentLength(_ context.Context, _ string) (int64, error) {
	f.calls++
	if f.err != nil {
		return 0, f.err
	}
	return f.length, nil
}

// completedEntryWithStaleSize builds a COMPLETED multi-file torrent entry whose
// advertised Size is the (wrong) sum of static RD per-file metadata. The
// largest file is the media file that drives Radarr's size-verified move.
func completedEntryWithStaleSize(infohash string, staleLargest, staleSmall int64) *storage.Entry {
	now := time.Now()
	e := &storage.Entry{
		InfoHash:       infohash,
		Name:           "Some.Movie.2024.2160p.BluRay.REMUX",
		Protocol:       config.ProtocolTorrent,
		ActiveProvider: "realdebrid",
		Category:       "radarr",
		State:          storage.EntryStatePausedUP,
		IsComplete:     true,
		Progress:       1.0,
		AddedOn:        now,
		Files: map[string]*storage.File{
			"movie.mkv":  {Name: "movie.mkv", Size: staleLargest, AddedOn: now},
			"sample.mkv": {Name: "sample.mkv", Size: staleSmall, AddedOn: now},
			"poster.jpg": {Name: "poster.jpg", Size: 250_000, AddedOn: now},
		},
	}
	// Seed Entry.Size as the (wrong) sum of static RD per-file metadata, the
	// way Decypharr advertises it pre-reconcile.
	e.Size = staleLargest + staleSmall + 250_000
	return e
}

// TestReconcileEntrySizePersistsToQBitVisibleStore is the BLOCKING Concern 3
// guard.
//
// The qBit-compat /torrents/info read path is:
//
//	handleTorrentsInfo -> Queue().ListFilter() -> Storage.FilterQueued() -> the
//	QUEUE store (s.queue, lowercased key).
//
// A completed Download-action entry is NOT removed from the queue store on
// completion (only DownloadActionNone deletes it), and markAsCompleted persists
// via queue.Update -> Storage.UpdateQueue -> the QUEUE store. Therefore the
// size reconciliation MUST land in the queue store, read back through
// FilterQueued/ListFilter, or Radarr will keep seeing the stale RD-metadata
// size and the size-verified move will keep false-failing.
//
// This test seeds a STALE-size completed entry the way markAsCompleted
// persists it (queue store), runs the reconcile, then reads it back through
// the EXACT qbit-compat path (Queue.ListFilter) and asserts the visible Size
// equals the authoritative HEAD length, not the stale seed.
//
// RED before the fix: reconcileEntrySize / contentLengthProber do not exist
// (compile failure). Stubbed to a no-op, the read-back still shows the stale
// size (the reconcile never persisted to the queue store).
func TestReconcileEntrySizePersistsToQBitVisibleStore(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())
	dir := t.TempDir()

	q, strg := newQueueWithStorage(t, dir)
	defer closeQueueStorage(t, strg)

	const infohash = "00112233445566778899aabbccddeeff00112233"
	const (
		staleLargest = int64(56_000_000_000) // RD f.Bytes metadata (under-reports)
		staleSmall   = int64(120_000_000)
		trueLargest  = int64(57_251_461_400) // authoritative CDN Content-Length
	)
	wantTotal := trueLargest + staleSmall + 250_000

	e := completedEntryWithStaleSize(infohash, staleLargest, staleSmall)

	// Persist exactly the way markAsCompleted does: queue.Update -> UpdateQueue.
	if err := q.Update(e); err != nil {
		t.Fatalf("seed completed entry into queue store: %v", err)
	}

	// Sanity: the stale size is what the qbit-compat path currently sees.
	preList := q.ListFilter("", config.ProtocolAll, "", nil, "added_on", false)
	if len(preList) != 1 {
		t.Fatalf("pre-reconcile ListFilter: got %d entries want 1", len(preList))
	}
	if preList[0].Size != staleLargest+staleSmall+250_000 {
		t.Fatalf("pre-reconcile sanity: queue-visible Size = %d, expected stale seed %d", preList[0].Size, staleLargest+staleSmall+250_000)
	}

	prober := &fakeSizeProber{length: trueLargest}

	// reconcileEntrySize must: probe the LARGEST active file's link only
	// (Concern 2: one HEAD per completion, not per file), set that file's
	// Size and recompute Entry.Size, then persist to the SAME store the
	// qbit-compat path reads (queue store).
	largest := largestActiveFile(e)
	if largest == nil || largest.Name != "movie.mkv" {
		t.Fatalf("largestActiveFile picked %v, want movie.mkv", largest)
	}
	if err := reconcileEntrySize(context.Background(), q, e, prober, "https://rd-cdn.example/movie.mkv"); err != nil {
		t.Fatalf("reconcileEntrySize: %v", err)
	}
	if prober.calls != 1 {
		t.Fatalf("prober called %d times, want exactly 1 (Concern 2: largest file only, one HEAD per completion)", prober.calls)
	}

	// Read back through the EXACT qbit-compat path: Queue.ListFilter ->
	// Storage.FilterQueued -> queue store.
	got := q.ListFilter("", config.ProtocolAll, "", nil, "added_on", false)
	if len(got) != 1 {
		t.Fatalf("post-reconcile ListFilter: got %d entries want 1", len(got))
	}
	gotEntry := got[0]
	if gotEntry.Size != wantTotal {
		t.Errorf("qbit-visible Entry.Size: got %d want %d (reconcile must persist to the queue store the qbit-compat /torrents/info reads, not entries)", gotEntry.Size, wantTotal)
	}
	if f := gotEntry.Files["movie.mkv"]; f == nil || f.Size != trueLargest {
		t.Errorf("largest File.Size after reconcile: got %v want %d", f, trueLargest)
	}
	if f := gotEntry.Files["sample.mkv"]; f == nil || f.Size != staleSmall {
		t.Errorf("non-target File.Size must be untouched: got %v want %d", f, staleSmall)
	}
}

// TestReconcileEntrySizeKeepsPriorSizeOnProbeError guards Concern 2 / B0: when
// the HEAD/link path fails (e.g. RD bytes_limit_reached account cap, or a 451
// DMCA on the CDN), the reconcile must log+continue and keep the prior size,
// never fail completion and never persist a wrong size.
func TestReconcileEntrySizeKeepsPriorSizeOnProbeError(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())
	dir := t.TempDir()

	q, strg := newQueueWithStorage(t, dir)
	defer closeQueueStorage(t, strg)

	const infohash = "ffeeddccbbaa99887766554433221100ffeeddcc"
	const (
		staleLargest = int64(56_000_000_000)
		staleSmall   = int64(120_000_000)
	)
	priorTotal := staleLargest + staleSmall + 250_000

	e := completedEntryWithStaleSize(infohash, staleLargest, staleSmall)
	if err := q.Update(e); err != nil {
		t.Fatalf("seed completed entry: %v", err)
	}

	prober := &fakeSizeProber{err: fmt.Errorf("realdebrid API error: Status: 451")}

	if err := reconcileEntrySize(context.Background(), q, e, prober, "https://rd-cdn.example/movie.mkv"); err != nil {
		t.Fatalf("reconcileEntrySize must NOT fail completion on probe error, got: %v", err)
	}

	got := q.ListFilter("", config.ProtocolAll, "", nil, "added_on", false)
	if len(got) != 1 {
		t.Fatalf("ListFilter: got %d entries want 1", len(got))
	}
	if got[0].Size != priorTotal {
		t.Errorf("prior size must be preserved on probe error: got %d want %d", got[0].Size, priorTotal)
	}
}
