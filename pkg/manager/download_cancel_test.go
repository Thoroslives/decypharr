package manager

import (
	"context"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/internal/testutil"
)

// TestDownloadCancelRegisterCancelWait guards Fix B.1: the per-torrent
// cancellation registry on Manager. RegisterDownload stores a cancel func and
// done channel; CancelDownload signals the ctx; WaitForDownloadExit blocks
// until done closes or timeout.
//
// Pre-fix the downloader uses Manager.ctx (process-wide context.Background())
// for grab.Request, so qBit DELETE has no way to cancel a single in-flight
// worker. The registry plus the localDownloader rewiring in B.2 fixes that.
func TestDownloadCancelRegisterCancelWait(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())
	m := &Manager{downloadCancels: xsync.NewMap[string, *downloadHandle]()}

	ctx, release := m.RegisterDownload("hash-a", context.Background())
	if ctx.Err() != nil {
		t.Fatalf("fresh ctx should not be cancelled, got %v", ctx.Err())
	}

	// Simulate worker: goroutine that exits when ctx cancels, then triggers
	// release. The done channel inside the handle is closed by release.
	done := make(chan struct{})
	go func() {
		<-ctx.Done()
		release()
		close(done)
	}()

	m.CancelDownload("hash-a")
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not exit after CancelDownload")
	}

	if err := m.WaitForDownloadExit("hash-a", 100*time.Millisecond); err != nil {
		t.Errorf("WaitForDownloadExit after release should return nil, got %v", err)
	}
}

// TestDownloadCancelUnknownHashNoOp confirms that CancelDownload and
// WaitForDownloadExit are safe to invoke for hashes that were never
// registered. The qBit DELETE handler relies on this: it fires Cancel/Wait
// for every hash regardless of whether a local download is actually active.
func TestDownloadCancelUnknownHashNoOp(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())
	m := &Manager{downloadCancels: xsync.NewMap[string, *downloadHandle]()}
	m.CancelDownload("never-registered") // must not panic
	if err := m.WaitForDownloadExit("never-registered", 50*time.Millisecond); err != nil {
		t.Errorf("Wait on unregistered must return nil, got %v", err)
	}
}

// TestDownloadCancelWaitTimeout guards the timeout branch of
// WaitForDownloadExit. If a worker fails to exit within the timeout, the
// handler must surface that to the caller rather than blocking forever.
func TestDownloadCancelWaitTimeout(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())
	m := &Manager{downloadCancels: xsync.NewMap[string, *downloadHandle]()}
	_, _ = m.RegisterDownload("hash-a", context.Background())
	// Don't cancel, don't release — Wait should hit timeout.
	err := m.WaitForDownloadExit("hash-a", 50*time.Millisecond)
	if err != context.DeadlineExceeded {
		t.Errorf("Wait without release must return DeadlineExceeded, got %v", err)
	}
}

// TestDownloadCancelDoubleCancel ensures repeated cancels are no-ops and
// release is safe to invoke more than once. Real call sites use defer for
// release; we want to keep the API safe against bugs that double-fire it.
func TestDownloadCancelDoubleCancel(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())
	m := &Manager{downloadCancels: xsync.NewMap[string, *downloadHandle]()}
	_, release := m.RegisterDownload("hash-a", context.Background())
	m.CancelDownload("hash-a")
	m.CancelDownload("hash-a") // must not panic
	release()
	release() // must not panic — release is idempotent via select-default close
}
