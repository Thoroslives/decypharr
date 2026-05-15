package manager

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/testutil"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// testRemoveStalledAfter is the stalled-reaper window used by the persisted
// requeue tests. It is intentionally short so expireRequeueRecord can backdate
// a record past the TTL (2 * removeStalledAfter) without sleeping.
const testRemoveStalledAfter = 30 * time.Minute

// newQueueWithStorage opens a real bbolt-backed Storage at dir and builds a
// real *Queue rooted on it with a small, deterministic removeStalledAfter.
// Mirrors newQueue() but lets the test control removeStalledAfter (NewForTest
// hardcodes "" which would make the TTL net infinite).
func newQueueWithStorage(t *testing.T, dir string) (*Queue, *storage.Storage) {
	t.Helper()
	strg, err := storage.NewStorage(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	q := newQueue(context.Background(), strg, 1000, testRemoveStalledAfter.String())
	if q.removeStalledAfter != testRemoveStalledAfter {
		t.Fatalf("removeStalledAfter not applied: got %s want %s", q.removeStalledAfter, testRemoveStalledAfter)
	}
	return q, strg
}

// closeQueueStorage closes the underlying Storage (simulates a process exit).
func closeQueueStorage(t *testing.T, strg *storage.Storage) {
	t.Helper()
	if err := strg.Close(); err != nil {
		t.Fatalf("close storage: %v", err)
	}
}

// resolvableArrName is the arr name the positive-path requests carry. The
// drain resolver in these tests resolves exactly this name to a real *arr.Arr.
const resolvableArrName = "sonarr"

// arrResolver returns a resolver that only knows about resolvableArrName,
// matching the production m.arr.Get contract (nil when unresolvable).
func arrResolver() func(name string) *arr.Arr {
	known := arr.New(resolvableArrName, "http://sonarr:8989", "token", false, false, nil, "", "manual")
	return func(name string) *arr.Arr {
		if name == resolvableArrName {
			return known
		}
		return nil
	}
}

// newTestImportRequest builds a minimal ImportRequest with a RESOLVABLE Arr,
// the way AddNewTorrent would hand one to ReQueue.
func newTestImportRequest(infohash, name string) *ImportRequest {
	return &ImportRequest{
		Id:             "req-" + infohash,
		DownloadFolder: "/data/torrents",
		SelectedDebrid: "realdebrid",
		Magnet: &utils.Magnet{
			InfoHash: infohash,
			Name:     name,
			Size:     123,
			Link:     "magnet:?xt=urn:btih:" + infohash + "&dn=" + name,
		},
		Arr:    arr.New(resolvableArrName, "http://sonarr:8989", "token", false, false, nil, "", "manual"),
		Action: config.DownloadAction("symlink"),
		Type:   ImportTypeQBit,
	}
}

// mustCreateLiveEntry persists a storage.Entry for infohash into the queue
// bucket, simulating "this grab is already owned by a live entry".
func mustCreateLiveEntry(t *testing.T, strg *storage.Storage, infohash string) {
	t.Helper()
	e := &storage.Entry{
		InfoHash: infohash,
		Name:     "live-entry",
		Protocol: config.ProtocolTorrent,
		State:    storage.EntryStateDownloading,
	}
	if err := strg.AddQueue(e); err != nil {
		t.Fatalf("seed live entry: %v", err)
	}
}

// expireRequeueRecord backdates the persisted requeue record's PersistedAt so
// it is older than the TTL (2 * removeStalledAfter), forcing the self-healing
// drain to discard it.
func expireRequeueRecord(t *testing.T, strg *storage.Storage, infohash string) {
	t.Helper()
	rec, err := strg.GetRequeue(infohash)
	if err != nil {
		t.Fatalf("GetRequeue for expiry: %v", err)
	}
	rec.PersistedAt = time.Now().Add(-3 * testRemoveStalledAfter)
	if err := strg.PutRequeue(infohash, rec); err != nil {
		t.Fatalf("PutRequeue (backdate): %v", err)
	}
}

// TestPushRequestSurvivesRestart is the P0 guard: a requeued grab must be
// recoverable after a container restart. Before the fix, PushRequest only
// appended to the in-memory slice, so the grab was lost on restart.
//
// RED before the fix: PutRequeue/DrainPersistedRequeue/DeletePersistedRequeue
// do not exist (compile failure), and even stubbed, PushRequest does not
// persist so a post-restart drain returns nothing.
func TestPushRequestSurvivesRestart(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())
	dir := t.TempDir()

	q, strg := newQueueWithStorage(t, dir)
	const infohash = "0123456789abcdef0123456789abcdef01234567"
	req := newTestImportRequest(infohash, "Show.S01E01")
	if err := q.ReQueue(req); err != nil {
		t.Fatalf("ReQueue: %v", err)
	}
	closeQueueStorage(t, strg)

	// Simulate restart: re-open the same dir, in-memory slice is gone.
	q2, strg2 := newQueueWithStorage(t, dir)
	defer closeQueueStorage(t, strg2)

	got, err := q2.DrainPersistedRequeue(arrResolver())
	if err != nil {
		t.Fatalf("DrainPersistedRequeue: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 recovered request, got %d (requeued grab lost across restart)", len(got))
	}
	if got[0].Magnet == nil || got[0].Magnet.InfoHash != infohash {
		t.Fatalf("recovered request infohash mismatch: %+v", got[0].Magnet)
	}
	if got[0].Arr == nil || got[0].Arr.Name != resolvableArrName {
		t.Fatalf("recovered request Arr not reconstructed: %+v", got[0].Arr)
	}

	// No double-replay: delete then drain again -> nothing.
	if err := q2.DeletePersistedRequeue(infohash); err != nil {
		t.Fatalf("DeletePersistedRequeue: %v", err)
	}
	got2, err := q2.DrainPersistedRequeue(arrResolver())
	if err != nil {
		t.Fatalf("second DrainPersistedRequeue: %v", err)
	}
	if len(got2) != 0 {
		t.Fatalf("expected 0 after delete, got %d (double-replay)", len(got2))
	}
}

// TestRequeueNotReplayedAfterEntryExists guards self-healing Concern 4: if a
// live storage.Entry already owns the infohash, the persisted requeue must be
// discarded on drain (not replayed) so we don't spawn a duplicate/zombie grab.
func TestRequeueNotReplayedAfterEntryExists(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())
	dir := t.TempDir()

	q, strg := newQueueWithStorage(t, dir)
	const infohash = "abcdefabcdefabcdefabcdefabcdefabcdefabcd"
	req := newTestImportRequest(infohash, "Show.S01E02")
	if err := q.ReQueue(req); err != nil {
		t.Fatalf("ReQueue: %v", err)
	}
	// A live entry now owns this hash.
	mustCreateLiveEntry(t, strg, infohash)

	got, err := q.DrainPersistedRequeue(arrResolver())
	if err != nil {
		t.Fatalf("DrainPersistedRequeue: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 (entry already owned), got %d", len(got))
	}
	// And the stale record must have been self-healed away.
	if _, err := q.storage.GetRequeue(infohash); err == nil {
		t.Fatal("expected requeue record deleted after drain (already-owned), still present")
	}
	closeQueueStorage(t, strg)
}

// TestRequeueTTLExpiry guards self-healing Concern 4: a record older than
// 2*removeStalledAfter cannot represent a still-valid requeue (it would have
// been reaped by the stalled remover), so the drain must discard + delete it.
func TestRequeueTTLExpiry(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())
	dir := t.TempDir()

	q, strg := newQueueWithStorage(t, dir)
	const infohash = "feedfacefeedfacefeedfacefeedfacefeedface"
	req := newTestImportRequest(infohash, "Show.S01E03")
	if err := q.ReQueue(req); err != nil {
		t.Fatalf("ReQueue: %v", err)
	}
	expireRequeueRecord(t, strg, infohash)

	got, err := q.DrainPersistedRequeue(arrResolver())
	if err != nil {
		t.Fatalf("DrainPersistedRequeue: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 (TTL-expired), got %d", len(got))
	}
	if _, err := q.storage.GetRequeue(infohash); err == nil {
		t.Fatal("expected TTL-expired requeue record deleted after drain, still present")
	}
	closeQueueStorage(t, strg)
}

// TestRequeueDiscardedWhenArrUnresolvable guards Concern 5: if the persisted
// ArrName does not resolve to exactly one configured arr, the drain must
// discard the record (warn, no panic, no error, no wrong-library misfile)
// rather than guess.
func TestRequeueDiscardedWhenArrUnresolvable(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())
	dir := t.TempDir()

	q, strg := newQueueWithStorage(t, dir)
	const infohash = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
	req := newTestImportRequest(infohash, "Show.S01E04")
	// Persisted ArrName will be resolvableArrName, but we hand the drain a
	// resolver that knows NOTHING -> unresolvable.
	if err := q.ReQueue(req); err != nil {
		t.Fatalf("ReQueue: %v", err)
	}

	emptyResolver := func(name string) *arr.Arr { return nil }
	got, err := q.DrainPersistedRequeue(emptyResolver)
	if err != nil {
		t.Fatalf("DrainPersistedRequeue returned error (must discard, not error): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 (arr unresolvable), got %d", len(got))
	}
	if _, err := q.storage.GetRequeue(infohash); err == nil {
		t.Fatal("expected unresolvable-arr requeue record deleted after drain, still present")
	}
	closeQueueStorage(t, strg)
}

// compile-time: ensure zerolog import stays used if helpers change.
var _ = zerolog.Nop
