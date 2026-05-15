package manager

import (
	"context"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/internal/testutil"
)

// TestCancelAndWait guards the consolidated cancel-before-unlink gate used by
// every DELETE handler. CancelAndWait is CancelDownload followed by
// WaitForDownloadExit, so it must preserve the three observable outcomes the
// handlers depend on:
//
//   - no worker registered for the hash: CancelDownload is a no-op and
//     WaitForDownloadExit returns nil immediately (the common DELETE case).
//   - a registered worker that exits on cancel: CancelAndWait cancels its ctx,
//     the worker observes cancellation, and CancelAndWait returns nil.
//   - a registered worker that ignores cancel: CancelAndWait returns
//     context.DeadlineExceeded once the timeout elapses so a stuck worker
//     cannot block DELETE forever.
//
// Mirrors the fake-worker harness in download_cancel_test.go. Timeouts are
// kept in the millisecond range so the test stays fast.
func TestCancelAndWait(t *testing.T) {
	t.Run("no worker registered returns nil promptly", func(t *testing.T) {
		testutil.IsolateConfig(t, t.TempDir())
		m := &Manager{downloadCancels: xsync.NewMap[string, *downloadHandle]()}

		start := time.Now()
		if err := m.CancelAndWait("never-registered", 50*time.Millisecond); err != nil {
			t.Errorf("CancelAndWait on unregistered hash must return nil, got %v", err)
		}
		if elapsed := time.Since(start); elapsed >= 50*time.Millisecond {
			t.Errorf("CancelAndWait on unregistered hash should return promptly, took %v", elapsed)
		}
	})

	t.Run("registered worker that exits on cancel returns nil", func(t *testing.T) {
		testutil.IsolateConfig(t, t.TempDir())
		m := &Manager{downloadCancels: xsync.NewMap[string, *downloadHandle]()}

		ctx, release := m.RegisterDownload("hash-a", context.Background())
		observedCancel := make(chan struct{})
		go func() {
			<-ctx.Done()
			close(observedCancel)
			release()
		}()

		if err := m.CancelAndWait("hash-a", 50*time.Millisecond); err != nil {
			t.Errorf("CancelAndWait on a worker that exits must return nil, got %v", err)
		}
		select {
		case <-observedCancel:
		case <-time.After(time.Second):
			t.Fatal("worker did not observe ctx cancellation; CancelAndWait did not cancel")
		}
	})

	t.Run("registered worker that ignores cancel returns DeadlineExceeded", func(t *testing.T) {
		testutil.IsolateConfig(t, t.TempDir())
		m := &Manager{downloadCancels: xsync.NewMap[string, *downloadHandle]()}

		// Register but never release: the worker ignores the cancel signal, so
		// WaitForDownloadExit must hit the timeout.
		_, _ = m.RegisterDownload("hash-a", context.Background())
		if err := m.CancelAndWait("hash-a", 50*time.Millisecond); err != context.DeadlineExceeded {
			t.Errorf("CancelAndWait on a stuck worker must return DeadlineExceeded, got %v", err)
		}
	})
}
