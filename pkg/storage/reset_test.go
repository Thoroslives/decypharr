package storage

import (
	"path/filepath"
	"testing"

	"github.com/sirrobot01/decypharr/internal/testutil"
)

// TestStorageResetsIsDownloadingOnNewStorage guards Fix D: after container
// restart, entries persisted with IsDownloading=true (set by previous-instance
// workers that died with the container) must be reset to false so
// processQueuedEntries' filter (pkg/manager/processor.go:89:
// "if entry.IsDownloading { continue }") doesn't skip them forever.
//
// See: /brain/02-Troubleshooting/2026-05-14-decypharr-shutdown-panic-flatline.md
// See: /brain/05-Projects/2026-05-15-decypharr-fork-spec.md (Fix D)
func TestStorageResetsIsDownloadingOnNewStorage(t *testing.T) {
	root := t.TempDir()
	// NewStorage transitively calls logger.New -> config.Get(), which os.Exit(1)s
	// without an initialised config path. IsolateConfig points the global config
	// singleton at our temp dir so the test never trips that exit.
	testutil.IsolateConfig(t, root)
	dir := filepath.Join(root, "db")

	// Phase 1: seed a "stuck" entry.
	s1, err := NewStorage(dir)
	if err != nil {
		t.Fatalf("open storage (seed phase): %v", err)
	}
	stuck := &Entry{
		InfoHash:      "abc123abc123abc123abc123abc123abc123abcd",
		Name:          "Test.Stuck.Entry",
		IsDownloading: true, // simulates previous-instance worker that died with container
	}
	if err := s1.AddOrUpdate(stuck); err != nil {
		_ = s1.Close()
		t.Fatalf("seed entry: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("close storage (seed phase): %v", err)
	}

	// Phase 2: re-open and check the flag.
	s2, err := NewStorage(dir)
	if err != nil {
		t.Fatalf("open storage (re-open phase): %v", err)
	}
	defer s2.Close()

	got, err := s2.Get("abc123abc123abc123abc123abc123abc123abcd")
	if err != nil {
		t.Fatalf("get seeded entry: %v", err)
	}
	if got == nil {
		t.Fatal("seeded entry not found after re-open")
	}
	if got.IsDownloading {
		t.Errorf("expected IsDownloading=false after NewStorage, got true - stuck-flag bug still present")
	}
}
