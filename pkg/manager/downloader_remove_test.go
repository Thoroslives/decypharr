package manager

import (
	"os"
	"path/filepath"
	"testing"
)

// TestRemoveEntryDirHandlesNonEmpty guards G5: os.Remove was used at the
// entry-deletion path, which fails with "directory not empty" when stray
// files remain (common with hidden inodes from in-flight downloads).
// os.RemoveAll handles both cases.
//
// This is a defensive change. Fix B (cancel-context) should prevent the
// non-empty case from occurring at all, but we want the seatbelt.
func TestRemoveEntryDirHandlesNonEmpty(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "non-empty-target")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "stray.partial"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := removeEntryDir(target); err != nil {
		t.Errorf("expected non-empty delete to succeed, got %v", err)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Errorf("expected target gone, stat returned: %v", err)
	}
}

// TestRemoveEntryDirIsIdempotent confirms the call is a no-op for absent paths.
func TestRemoveEntryDirIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "never-existed")
	if err := removeEntryDir(target); err != nil {
		t.Errorf("expected absent-path delete to no-op, got %v", err)
	}
}

// TestRemoveEntryDirHandlesFile covers the call site at downloader.go where
// the path is a single partial-download file (the original os.Remove usage).
// RemoveAll handles regular files identically.
func TestRemoveEntryDirHandlesFile(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "partial.file")
	if err := os.WriteFile(target, []byte("partial download bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := removeEntryDir(target); err != nil {
		t.Errorf("expected file delete to succeed, got %v", err)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Errorf("expected file gone, stat returned: %v", err)
	}
}
