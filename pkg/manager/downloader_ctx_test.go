package manager

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/testutil"
)

// TestLocalDownloaderRespectsCtxCancel guards Fix B.2: localDownloader must
// honor the per-torrent ctx passed in (not the process-wide d.manager.ctx).
// When ctx is cancelled, the grab transfer must abort within ~2s, closing
// its file descriptor so the destination directory can be safely removed by
// the qBit DELETE handler.
//
// Pre-fix: localDownloader called req.WithContext(d.manager.ctx), which is
// context.Background() set in manager.New (manager.go:114) and never
// cancelled mid-lifetime. Cancelling a per-torrent ctx had no effect on
// the in-flight transfer, so DELETE unlinked while grab kept writing,
// producing .fuse_hidden orphan inodes.
func TestLocalDownloaderRespectsCtxCancel(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())

	// Slow-drip server: claims a 10MB body but writes 1 byte every 50ms.
	// The transfer will never finish before the test times out unless ctx
	// cancel actually halts it.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		// Don't set Content-Length so grab streams without expecting a fixed size.
		flusher, _ := w.(http.Flusher)
		for {
			select {
			case <-r.Context().Done():
				return
			default:
			}
			if _, err := w.Write([]byte("x")); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(50 * time.Millisecond)
		}
	}))
	defer srv.Close()

	dir := t.TempDir()
	destPath := filepath.Join(dir, "test.bin")

	// Build a minimal Downloader wired to a Manager with just enough to call
	// localDownloader: streamClient (used by grab) + logger.
	mgr := &Manager{
		logger:       zerolog.Nop(),
		streamClient: &http.Client{},
	}
	d := &Downloader{
		manager: mgr,
		logger:  zerolog.Nop(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := d.localDownloader(ctx, srv.URL, destPath, nil, nil)
		done <- err
	}()

	// Give grab a moment to start transferring so we exercise the cancel
	// codepath rather than the "never started" codepath.
	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		// Expected: grab returns a ctx-cancelled error. We don't care about
		// the exact error type, only that it returned promptly.
		_ = err
	case <-time.After(3 * time.Second):
		t.Fatal("localDownloader did not exit within 3s of ctx cancel — grab is not honoring per-torrent ctx")
	}

	// Wait for the goroutine to fully exit so the destination FD is closed
	// before we try to remove the directory.
	wg.Wait()

	// The destination directory must be removable — i.e. no orphan FD.
	// Pre-fix this would fail with "directory not empty" because grab kept
	// writing to a deleted FD that Linux preserved as .fuse_hidden.
	if err := os.RemoveAll(dir); err != nil {
		t.Errorf("destination dir should be removable after cancel, got %v", err)
	}
}

// TestLocalDownloaderUsesPassedCtxNotManagerCtx is a static-shape regression
// for the Fix B.2 plumbing: localDownloader's first arg must be honored
// instead of falling back to d.manager.ctx. We assert this by passing a
// ctx that is ALREADY cancelled — localDownloader must return immediately
// (or shortly thereafter), proving the cancelled ctx propagated to grab.
//
// Pre-fix this test would hang for the full timeout because d.manager.ctx
// was background and never cancelled.
func TestLocalDownloaderUsesPassedCtxNotManagerCtx(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, _ := w.(http.Flusher)
		for {
			select {
			case <-r.Context().Done():
				return
			default:
			}
			if _, err := w.Write([]byte("x")); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(50 * time.Millisecond)
		}
	}))
	defer srv.Close()

	dir := t.TempDir()
	destPath := filepath.Join(dir, "test.bin")

	mgr := &Manager{
		logger:       zerolog.Nop(),
		streamClient: &http.Client{},
	}
	d := &Downloader{
		manager: mgr,
		logger:  zerolog.Nop(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled

	done := make(chan struct{})
	go func() {
		_ = d.localDownloader(ctx, srv.URL, destPath, nil, nil)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("localDownloader did not return for pre-cancelled ctx — passed ctx is being ignored")
	}
}
