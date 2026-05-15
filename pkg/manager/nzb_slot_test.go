package manager

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/testutil"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/notifications"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// TestNZBPathRespectsJobQueueCap guards Fix 3 (BIS-2): the NZB JobQueue path
// (JobTypeNZB) must honor max_downloads exactly like the torrent path. With a
// JobQueue sized maxWorkers, a burst of JobTypeNZB jobs must never run more
// than maxWorkers processFunc invocations concurrently. This mirrors
// TestAddNewTorrentRespectsJobQueueCap for the NZB job type and locks the
// dispatcher-side cap contract.
func TestNZBPathRespectsJobQueueCap(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())

	const (
		maxWorkers = 2
		totalJobs  = 6
	)
	var peak, current int32
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(totalJobs)

	processFunc := func(ctx context.Context, job *Job) {
		defer wg.Done()
		c := atomic.AddInt32(&current, 1)
		defer atomic.AddInt32(&current, -1)
		for {
			p := atomic.LoadInt32(&peak)
			if c <= p || atomic.CompareAndSwapInt32(&peak, p, c) {
				break
			}
		}
		<-release
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q := NewJobQueue(ctx, maxWorkers, processFunc)
	defer q.Close()

	for i := 0; i < totalJobs; i++ {
		job := newTestJob(JobTypeNZB, fmt.Sprintf("nzb-%d", i))
		if err := q.Submit(job); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	time.Sleep(100 * time.Millisecond)

	close(release)
	wg.Wait()

	if got := atomic.LoadInt32(&peak); got > maxWorkers {
		t.Errorf("peak concurrency = %d, want <= %d (JobTypeNZB burst not capped)", got, maxWorkers)
	}
	if got := atomic.LoadInt32(&peak); got == 0 {
		t.Errorf("peak concurrency = 0; expected jobs to actually run")
	}
}

// minimalUsenet builds a *usenet.Usenet whose PreCache returns an error fast
// instead of panicking: an empty fs map misses on Load, getFile then consults
// the NZBStorage which has no record for the test id, so PreCache returns a
// "not found" error. This keeps the detached precache goroutines in processNZB
// (usenet.go ~96-102) from crashing the test process.
func minimalUsenet(t *testing.T) *usenet.Usenet {
	t.Helper()
	store, err := usenet.NewNZBStorage()
	if err != nil {
		t.Fatalf("NewNZBStorage: %v", err)
	}
	return usenet.NewForTest(store)
}

// managerForNZBSlotTest wires the minimal Manager surface processNZB ->
// processAction actually touches on the DownloadActionNone fast path, so
// processAction completes cleanly (no panic) and its terminal side effect
// (the entry is removed from the queue) is observable.
//
//   - real config (via IsolateConfig) so EntryCache.Refresh / GetTorrentMountPath
//     do not nil-deref
//   - real EntryCache so the AddOrUpdate refresh callback is a safe no-op
//   - real Downloader; DownloadActionNone resolves to completeEntry +
//     queue.Delete + return without any debrid/NNTP I/O
//   - notifications service (disabled by default config -> Notify is a no-op)
//   - arr storage (GetOrCreate returns an Arr with empty Host so the
//     triggerArrRefresh goroutine returns immediately)
func managerForNZBSlotTest(t *testing.T) *Manager {
	t.Helper()
	strg, err := storage.NewStorage(t.TempDir() + "/db")
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	m := NewForTest(strg, zerolog.Nop())
	m.config = config.Get()
	m.usenet = minimalUsenet(t)
	m.entry = NewEntryCache(m)
	m.arr = arr.NewStorage()
	m.Notifications = notifications.New(&m.config.Notifications, zerolog.Nop())
	m.downloader = NewDownloadManager(m)
	return m
}

func newNZBSlotEntry(infohash string) *storage.Entry {
	entry := &storage.Entry{
		InfoHash:        infohash,
		Name:            "nzb-slot-entry",
		Protocol:        config.ProtocolNZB,
		State:           storage.EntryStateDownloading,
		Status:          debridTypes.TorrentStatusDownloading,
		Action:          config.DownloadActionNone,
		SkipMultiSeason: true, // skip detectMultiSeason so download takes the fast path
		Providers:       make(map[string]*storage.ProviderEntry),
		Files:           make(map[string]*storage.File),
		Tags:            []string{},
		AddedOn:         time.Now(),
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
	}
	_ = entry.AddUsenetProvider(&storage.NZB{ID: infohash, Name: entry.Name})
	entry.ActiveProvider = "usenet"
	return entry
}

func newNZBSlotMetadata(infohash, name string) *storage.NZB {
	// Non-empty Files so processNZB clears its len(entry.Files)==0 guard and
	// reaches the processAction call site (usenet.go:108).
	return &storage.NZB{
		ID:        infohash,
		Name:      name,
		TotalSize: 1,
		Files: []storage.NZBFile{
			{NzbID: infohash, Name: "file-0.bin", Size: 1},
		},
	}
}

// TestNZBPathHoldsWorkerSlot is the Fix 3 (BIS-2) RED->GREEN guard. It proves
// processNZB runs processAction on the *caller's goroutine* rather than
// spawning it with `go`, i.e. the caller is blocked until the local pull
// finishes. This is the exact contract A-bis established for the torrent side;
// the NZB side regressed via `go m.processAction(entry)` at usenet.go:108.
//
// The DownloadActionNone path makes processAction's terminal effect a queue
// delete. processNZB is called on the test goroutine with pre-fetched
// metadata (exactly what processQueuedNZB passes after its own
// m.usenet.GetNZB lookup). There is no goroutine handoff in the assertion
// path, so the check is deterministic:
//
//   - Post-fix (synchronous): processNZB does not return until processAction
//     has run, so the entry is gone from the queue by the time processNZB
//     returns -> GREEN.
//   - Pre-fix (`go m.processAction`): processNZB returns immediately, before
//     the detached processAction touches the queue, so the entry is still
//     present right after processNZB returns -> RED.
func TestNZBPathHoldsWorkerSlot(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())
	m := managerForNZBSlotTest(t)

	const infohash = "00112233445566778899aabbccddeeff00112233"
	entry := newNZBSlotEntry(infohash)
	if err := m.queue.Add(entry); err != nil {
		t.Fatalf("queue.Add: %v", err)
	}
	if _, err := m.queue.GetTorrent(infohash); err != nil {
		t.Fatalf("entry should be queued before processNZB: %v", err)
	}

	if err := m.processNZB(context.Background(), entry, newNZBSlotMetadata(infohash, entry.Name)); err != nil {
		t.Fatalf("processNZB returned error: %v", err)
	}

	// Synchronous processNZB only returns after processAction (and thus the
	// DownloadActionNone queue.Delete) has completed. With the pre-fix `go`,
	// processNZB returns before processAction runs and this entry is still
	// queued.
	if _, err := m.queue.GetTorrent(infohash); err == nil {
		t.Fatal("entry still queued after processNZB returned: processAction did not run on the caller's goroutine (the `go m.processAction` escape is still present)")
	}
}

// TestNZBJobQueuePathHoldsWorkerSlot exercises the production JobQueue
// JobTypeNZB worker path holding its slot for the whole local pull. A single
// worker runs N sequential NZB jobs; each processFunc drives the real
// processNZB (fed pre-fetched metadata, as processQueuedNZB does after its
// m.usenet.GetNZB lookup) and only returns when processNZB returns. With the
// synchronous fix, observed peak concurrency is bounded by the worker count
// and every entry is drained from the queue (processAction ran inside the
// slot). The pre-fix `go` would let processNZB return early; the queue-drain
// assertion is what fails in that case.
func TestNZBJobQueuePathHoldsWorkerSlot(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())
	m := managerForNZBSlotTest(t)

	const (
		maxWorkers = 1
		totalJobs  = 4
	)

	type nzbItem struct {
		infohash string
		meta     *storage.NZB
	}
	items := make([]nzbItem, totalJobs)
	for i := 0; i < totalJobs; i++ {
		ih := fmt.Sprintf("aabbccddeeff00112233445566778899%08d", i)
		e := newNZBSlotEntry(ih)
		if err := m.queue.Add(e); err != nil {
			t.Fatalf("queue.Add %d: %v", i, err)
		}
		items[i] = nzbItem{infohash: ih, meta: newNZBSlotMetadata(ih, e.Name)}
	}

	var peak, current int32
	var wg sync.WaitGroup
	wg.Add(totalJobs)
	idx := make(chan int, totalJobs)
	for i := range items {
		idx <- i
	}

	processFunc := func(ctx context.Context, job *Job) {
		defer wg.Done()
		c := atomic.AddInt32(&current, 1)
		defer atomic.AddInt32(&current, -1)
		for {
			p := atomic.LoadInt32(&peak)
			if c <= p || atomic.CompareAndSwapInt32(&peak, p, c) {
				break
			}
		}
		i := <-idx
		it, err := m.queue.GetTorrent(items[i].infohash)
		if err != nil {
			t.Errorf("entry %d missing before processNZB: %v", i, err)
			return
		}
		_ = m.processNZB(context.Background(), it, items[i].meta)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q := NewJobQueue(ctx, maxWorkers, processFunc)
	defer q.Close()

	for i := range items {
		if err := q.Submit(newTestJob(JobTypeNZB, items[i].infohash)); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	wg.Wait()

	if got := atomic.LoadInt32(&peak); got > maxWorkers {
		t.Errorf("peak concurrency = %d, want <= %d", got, maxWorkers)
	}
	// Every entry must be drained: a synchronous processNZB ran processAction
	// (DownloadActionNone -> queue.Delete) inside the worker slot before
	// returning. The pre-fix `go m.processAction` could leave the worker
	// returning before processAction touched the queue.
	for i := range items {
		if _, err := m.queue.GetTorrent(items[i].infohash); err == nil {
			t.Errorf("entry %d still queued after its NZB job finished: processAction did not complete within the worker slot", i)
		}
	}
}
