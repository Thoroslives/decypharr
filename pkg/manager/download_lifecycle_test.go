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

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/testutil"
)

// TestDeleteMidDownloadCleansFD is the integration regression test for Fix B.
// Pre-fix flow:
//  1. Worker goroutine started, grab.Request bound to d.manager.ctx
//     (process-wide context.Background())
//  2. User fires qBit DELETE → handler unlinks the destination directory
//  3. grab worker keeps writing to its open FD; Linux preserves the inode
//     as .fuse_hidden00b9... in the parent directory
//  4. Parent directory cannot be removed ("directory not empty"), GBs of
//     data lost to the orphan inode
//
// Post-fix flow:
//  1. processAction calls Manager.RegisterDownload, derives per-torrent ctx
//  2. Worker starts grab.Request bound to per-torrent ctx
//  3. qBit DELETE → Manager.CancelDownload signals the ctx
//  4. grab honors WithContext: closes FD, returns
//  5. WaitForDownloadExit blocks until worker exits cleanly
//  6. Parent directory is removable immediately
//
// This test exercises steps 2-6 directly (skipping processAction's higher-
// level flow because that would require a full Manager.init).
func TestDeleteMidDownloadCleansFD(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())

	// Slow-drip server: writes 1 byte every 50ms, never finishes.
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

	const hash = "deadbeef00000000000000000000000000000999"
	parentDir := t.TempDir()
	destPath := filepath.Join(parentDir, "test.bin")

	mgr := &Manager{
		logger:          zerolog.Nop(),
		streamClient:    &http.Client{},
		downloadCancels: xsync.NewMap[string, *downloadHandle](),
	}
	d := &Downloader{
		manager: mgr,
		logger:  zerolog.Nop(),
	}

	// Per-torrent ctx + release matching processAction's pattern.
	ctx, release := mgr.RegisterDownload(hash, context.Background())

	// Start a worker that runs localDownloader and signals exit via release.
	var wg sync.WaitGroup
	wg.Add(1)
	workerExited := make(chan struct{})
	go func() {
		defer wg.Done()
		defer release()
		_ = d.localDownloader(ctx, srv.URL, destPath, nil, nil)
		close(workerExited)
	}()

	// Let grab actually start writing so we exercise the in-flight cancel
	// path. Pre-fix this is where the .fuse_hidden inode would be created.
	time.Sleep(250 * time.Millisecond)

	// Sanity: the worker has started writing the file (or at least opened it).
	// We don't assert anything specific about contents; just that the FD exists.
	if _, statErr := os.Stat(destPath); statErr != nil {
		// Not necessarily a failure; grab may not have flushed yet. Continue.
		t.Logf("dest not yet visible (grab not flushed yet): %v", statErr)
	}

	// Fire the cancel; this is what the qBit DELETE handler does pre-unlink.
	mgr.CancelDownload(hash)

	// Wait for the worker to exit. Should be ~immediate since grab honors ctx.
	if err := mgr.WaitForDownloadExit(hash, 3*time.Second); err != nil {
		t.Fatalf("WaitForDownloadExit timed out; grab is not honoring per-torrent ctx: %v", err)
	}

	select {
	case <-workerExited:
	case <-time.After(time.Second):
		t.Fatal("worker did not exit after WaitForDownloadExit returned (close ordering bug)")
	}
	wg.Wait()

	// The orphan-FD probe: parent directory must be removable. Pre-fix this
	// fails with "directory not empty" because the inode is preserved as
	// .fuse_hidden while grab keeps writing.
	if err := os.RemoveAll(parentDir); err != nil {
		t.Errorf("parent directory should be removable after cancel + wait, got %v", err)
	}
}
