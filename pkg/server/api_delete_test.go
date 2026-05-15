package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/testutil"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// mustOpenStorage opens a bbolt-backed storage rooted under dir/db. Mirrors
// the qBit-compat handler's test helper in pkg/server/qbit/http_delete_test.go.
func mustOpenStorage(t *testing.T, dir string) *storage.Storage {
	t.Helper()
	s, err := storage.NewStorage(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// newServerForTest builds a *Server with only the fields the internal DELETE
// handlers touch (manager + logger). The full New() flow parses templates from
// an embedded FS, builds a cookie store, and constructs the qbit/sabnzbd/webdav
// sub-handlers — none of which handleDeleteTorrent / handleDeleteTorrents need.
// The test is in package server, so the unexported fields are accessible. This
// keeps the test a tight handler unit test, mirroring the qBit test's use of a
// minimal NewForTest manager.
func newServerForTest(m *manager.Manager) *Server {
	return &Server{
		manager: m,
		logger:  zerolog.Nop(),
	}
}

// withHashParam injects a chi route context so chi.URLParam(r, "hash") resolves
// inside the handler. handleDeleteTorrent reads the hash from the chi URL param,
// not the query string, so the test must populate the route context the same
// way chi's router would at request time.
func withHashParam(t *testing.T, req *http.Request, hash string) *http.Request {
	t.Helper()
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("hash", hash)
	return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
}

// TestHandleDeleteTorrentCancelsBeforeUnlink guards Fix B-bis: the internal
// API single-delete handler (used by the Decypharr dashboard UI) must call
// CancelDownload and wait for the in-flight local-pull worker to exit BEFORE
// Queue.Delete unlinks files. Pre-fix only the qBit-compat handler had this
// gate (Fix B, PR #12); the internal handler went straight to Queue.Delete
// while the worker was still writing, producing .fuse_hidden orphan inodes
// and "directory not empty" errors observed in the 2026-05-15 soak.
//
// Mirrors TestHandleTorrentsDeleteCancelsBeforeUnlink in the qBit test: a
// registered fake worker records the time it observed cancellation; the queue
// row disappearing marks delete-time; cancel-time must precede delete-time.
func TestHandleDeleteTorrentCancelsBeforeUnlink(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())
	strg := mustOpenStorage(t, t.TempDir())
	m := manager.NewForTest(strg, zerolog.Nop())
	s := newServerForTest(m)

	const hash = "deadbeef00000000000000000000000000000001"

	// Seed a queued entry so Queue.Delete has something to delete.
	if err := strg.AddQueue(&storage.Entry{
		InfoHash: hash,
		Name:     "Test.Entry.For.Delete",
	}); err != nil {
		t.Fatalf("seed queue entry: %v", err)
	}

	// Register a fake worker. The "worker" is a goroutine blocked on ctx.Done(),
	// which records the time it observed the cancel and then calls release.
	var cancelTimeUnixNano atomic.Int64
	workerCtx, release := m.RegisterDownload(hash, context.Background())
	workerExited := make(chan struct{})
	go func() {
		<-workerCtx.Done()
		cancelTimeUnixNano.Store(time.Now().UnixNano())
		release()
		close(workerExited)
	}()

	// Invoke the DELETE handler directly. The handler closes over s, so this
	// is a tight integration test of just the handler logic.
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/", nil)
	req = withHashParam(t, req, hash)

	handlerDone := make(chan struct{})
	go func() {
		s.handleDeleteTorrent(rr, req)
		close(handlerDone)
	}()

	// Wait for the handler to complete (it blocks on WaitForDownloadExit).
	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("DELETE handler did not return within 2s")
	}

	// The fake worker must have observed cancellation.
	select {
	case <-workerExited:
	case <-time.After(time.Second):
		t.Fatal("worker did not observe ctx cancellation — handler did not call CancelDownload")
	}

	if cancelTimeUnixNano.Load() == 0 {
		t.Fatal("worker exited without recording cancel time — order assertion impossible")
	}

	// Capture delete-time AFTER the handler returns. Queue.Delete is
	// synchronous in the handler so the entry must already be gone.
	deleteObservedNano := time.Now().UnixNano()

	// Sanity-check the queue row is gone.
	if _, err := strg.GetQueued(hash); err == nil {
		t.Error("queue entry still present after DELETE handler — Queue.Delete was not invoked")
	}

	// Order check: cancel time must be <= delete-observation time.
	if cancelTimeUnixNano.Load() > deleteObservedNano {
		t.Errorf("cancel happened AFTER delete (cancel=%d delete=%d) — Fix B-bis ordering regression",
			cancelTimeUnixNano.Load(), deleteObservedNano)
	}

	if rr.Code != http.StatusOK {
		t.Errorf("DELETE handler returned status %d, want 200", rr.Code)
	}
}

// TestHandleDeleteTorrentsBatchCancelsBeforeUnlink guards the batch internal
// handler (handleDeleteTorrents, used by the dashboard UI's bulk delete). It
// must apply the same Cancel+Wait gate to every hash before the batch unlink.
// A single hash in the batch is sufficient to prove the gate is wired.
func TestHandleDeleteTorrentsBatchCancelsBeforeUnlink(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())
	strg := mustOpenStorage(t, t.TempDir())
	m := manager.NewForTest(strg, zerolog.Nop())
	s := newServerForTest(m)

	const hash = "deadbeef00000000000000000000000000000002"

	if err := strg.AddQueue(&storage.Entry{
		InfoHash: hash,
		Name:     "Test.Batch.Entry.For.Delete",
	}); err != nil {
		t.Fatalf("seed queue entry: %v", err)
	}

	var cancelTimeUnixNano atomic.Int64
	workerCtx, release := m.RegisterDownload(hash, context.Background())
	workerExited := make(chan struct{})
	go func() {
		<-workerCtx.Done()
		cancelTimeUnixNano.Store(time.Now().UnixNano())
		release()
		close(workerExited)
	}()

	// handleDeleteTorrents reads the hashes from the query string, so no chi
	// route context is needed here.
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/?hashes="+hash, nil)

	handlerDone := make(chan struct{})
	go func() {
		s.handleDeleteTorrents(rr, req)
		close(handlerDone)
	}()

	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("batch DELETE handler did not return within 2s")
	}

	select {
	case <-workerExited:
	case <-time.After(time.Second):
		t.Fatal("worker did not observe ctx cancellation — batch handler did not call CancelDownload")
	}

	if cancelTimeUnixNano.Load() == 0 {
		t.Fatal("worker exited without recording cancel time — order assertion impossible")
	}

	deleteObservedNano := time.Now().UnixNano()

	if _, err := strg.GetQueued(hash); err == nil {
		t.Error("queue entry still present after batch DELETE handler — Queue.DeleteWhere was not invoked")
	}

	if cancelTimeUnixNano.Load() > deleteObservedNano {
		t.Errorf("cancel happened AFTER delete (cancel=%d delete=%d) — Fix B-bis ordering regression",
			cancelTimeUnixNano.Load(), deleteObservedNano)
	}

	if rr.Code != http.StatusOK {
		t.Errorf("batch DELETE handler returned status %d, want 200", rr.Code)
	}
}
