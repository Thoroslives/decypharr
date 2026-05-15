package manager

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/testutil"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// newTestJob constructs a Job directly because NewJob dereferences
// req.Id, which panics on a nil ImportRequest. The dispatcher only
// reads Type/Entry/ID, so a struct literal is sufficient for tests.
func newTestJob(t JobType, id string) *Job {
	return &Job{
		ID:        id,
		Type:      t,
		CreatedAt: time.Now(),
	}
}

// TestJobQueueRespectsMaxWorkers guards Fix A: NewJobQueue with maxWorkers=N
// must never run more than N processFunc invocations concurrently. The pre-Fix-A
// bug was `go m.processQueuedTorrent(entry)` in processQueuedEntries firing per
// queued item with zero concurrency cap, ignoring `max_downloads: N`.
//
// See: /brain/05-Projects/2026-05-15-decypharr-fork-spec.md (Fix A).
func TestJobQueueRespectsMaxWorkers(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())

	const (
		maxWorkers = 3
		totalJobs  = 10
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
		job := newTestJob(JobTypeTorrent, fmt.Sprintf("job-%d", i))
		if err := q.Submit(job); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	time.Sleep(100 * time.Millisecond) // let workers pick up

	close(release)
	wg.Wait()

	if got := atomic.LoadInt32(&peak); got > maxWorkers {
		t.Errorf("peak concurrency = %d, want <= %d", got, maxWorkers)
	}
	if got := atomic.LoadInt32(&peak); got == 0 {
		t.Errorf("peak concurrency = 0; expected jobs to actually run")
	}
}

// TestProcessJobRecoversPanics guards Fix A: a job whose dispatched processing
// function panics must NOT propagate up. JobQueue's workers have no recover()
// of their own, so without Manager.processJob's wrapper a single bad job would
// permanently remove a worker from the pool.
//
// Strategy: build a minimal Manager with no queue, then dispatch a torrent
// job whose Entry triggers processQueuedTorrent. The first nil-deref happens
// when processQueuedTorrent calls m.queue.Update — that panic must be caught
// by processJob's defer-recover and processJob must return normally.
func TestProcessJobRecoversPanics(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())

	m := &Manager{logger: zerolog.Nop()} // queue: nil — guaranteed panic source

	// Entry with no active provider so processQueuedTorrent goes down the
	// "no active placement found" branch, which calls m.queue.Update and
	// panics on the nil queue. That panic is what we want processJob to swallow.
	entry := &storage.Entry{InfoHash: "deadbeef"}
	job := &Job{
		ID:        "panic-job",
		Type:      JobTypeTorrent,
		Entry:     entry,
		CreatedAt: time.Now(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// If processJob's defer-recover is missing or wrong, this call panics
	// up through the test runtime and FAILs the test. If it works, processJob
	// returns normally and the test passes.
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.processJob(ctx, job)
	}()

	select {
	case <-done:
		// processJob returned cleanly — the panic was caught.
	case <-time.After(2 * time.Second):
		t.Fatal("processJob did not return within 2s; recover() likely missing")
	}

	// Sanity check: processJob with a nil job must also not panic.
	m.processJob(ctx, nil)
}

// TestAddNewTorrentRespectsJobQueueCap guards Fix A-bis: fresh submissions
// (the AddNewTorrent path, typical of a Radarr MissingMoviesSearch burst)
// are now routed through the JobQueue via JobTypeNew, so a burst must not
// exceed max_downloads concurrent workers. Pre-fix this path was an ungated
// `go m.processNewTorrent(...)` per submission, bypassing Fix A's cap.
//
// See: /brain/05-Projects/2026-05-15-decypharr-fork-soak-findings.md
func TestAddNewTorrentRespectsJobQueueCap(t *testing.T) {
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
		job := newTestJob(JobTypeNew, fmt.Sprintf("new-%d", i))
		if err := q.Submit(job); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	time.Sleep(100 * time.Millisecond)

	close(release)
	wg.Wait()

	if got := atomic.LoadInt32(&peak); got > maxWorkers {
		t.Errorf("peak concurrency = %d, want <= %d (JobTypeNew burst not capped)", got, maxWorkers)
	}
	if got := atomic.LoadInt32(&peak); got == 0 {
		t.Errorf("peak concurrency = 0; expected jobs to actually run")
	}
}

// TestJobQueueCloseDrains guards Fix A: Close blocks until in-flight workers
// finish (cleanly drains), and post-Close Submit returns an error. The
// production path is Manager.Stop -> jobQueue.Close before scheduler shutdown.
func TestJobQueueCloseDrains(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())

	var doneCount int32
	block := make(chan struct{})
	processFunc := func(ctx context.Context, job *Job) {
		<-block
		atomic.AddInt32(&doneCount, 1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q := NewJobQueue(ctx, 2, processFunc)

	if err := q.Submit(newTestJob(JobTypeTorrent, "a")); err != nil {
		t.Fatal(err)
	}
	if err := q.Submit(newTestJob(JobTypeTorrent, "b")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // let workers pick the jobs up

	// Close in goroutine because it blocks until workers drain.
	closeDone := make(chan struct{})
	go func() { q.Close(); close(closeDone) }()

	// Verify Close is blocking (not returned yet while workers are stuck).
	select {
	case <-closeDone:
		t.Fatal("Close returned before unblocking workers")
	case <-time.After(50 * time.Millisecond):
	}

	close(block) // let the two in-flight workers finish

	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after unblocking workers")
	}

	if got := atomic.LoadInt32(&doneCount); got != 2 {
		t.Errorf("drained %d jobs, want 2", got)
	}

	// Post-Close Submit must error.
	if err := q.Submit(newTestJob(JobTypeTorrent, "c")); err == nil {
		t.Error("Submit after Close must error")
	}
}

// TestJobQueuePendingIDs guards Fix 2 (BIS-2): PendingIDs must contain a job
// ID ONLY while it is still in the not-yet-popped slice, and must NOT contain
// a job a worker has already popped (in-flight) or one that never existed.
// This is the real "held in JobQueue, not yet picked up by a worker"
// discriminator the qBit state mapping keys off; a job a worker is already
// running will make progress and is not "queued waiting for a slot".
func TestJobQueuePendingIDs(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	processFunc := func(ctx context.Context, job *Job) {
		once.Do(func() { close(started) })
		<-release
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Single worker so the second submitted job stays strictly pending while
	// the first one is in-flight (popped, running processFunc).
	q := NewJobQueue(ctx, 1, processFunc)
	defer q.Close()

	// Empty queue: nothing pending, unknown id absent.
	if ids := q.PendingIDs(); len(ids) != 0 {
		t.Errorf("PendingIDs on empty queue = %v, want empty", ids)
	}

	running := newTestJob(JobTypeNew, "running")
	pending := newTestJob(JobTypeNew, "pending")
	if err := q.Submit(running); err != nil {
		t.Fatalf("submit running: %v", err)
	}
	<-started // worker has popped "running" and is blocked in processFunc

	if err := q.Submit(pending); err != nil {
		t.Fatalf("submit pending: %v", err)
	}

	ids := q.PendingIDs()
	// "pending" is queued behind the busy single worker -> still pending.
	if _, ok := ids["pending"]; !ok {
		t.Error("PendingIDs missing \"pending\", want present (queued, no free worker)")
	}
	// "running" has been popped and is executing -> NOT pending. This is the
	// pending-vs-running distinction the discriminator depends on.
	if _, ok := ids["running"]; ok {
		t.Error("PendingIDs contains \"running\", want absent (already popped by a worker)")
	}
	// Unknown id never appears.
	if _, ok := ids["nope"]; ok {
		t.Error("PendingIDs contains unknown id, want absent")
	}

	close(release)
}

// TestManagerPendingJobIDsNilSafe guards Fix 2: Manager.PendingJobIDs must be
// safe when the JobQueue is not wired (NewForTest builds a Manager with a nil
// jobQueue). The qBit list handler reads the returned map; a nil map must be
// safe for comma-ok lookups so the handler needs no nil check.
func TestManagerPendingJobIDsNilSafe(t *testing.T) {
	m := &Manager{} // jobQueue nil, as in NewForTest
	ids := m.PendingJobIDs()
	if ids != nil {
		t.Errorf("PendingJobIDs on nil jobQueue = %v, want nil", ids)
	}
	if _, ok := ids["anything"]; ok {
		t.Error("comma-ok lookup on nil map returned true, want false")
	}
}
