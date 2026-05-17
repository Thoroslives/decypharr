package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// seedOnDisk creates the on-disk DownloadPath() directory for entry with a
// single dummy file inside, so DeleteSymmetric's os.RemoveAll has something
// real to remove. It returns the directory path. Name is deliberately
// extension-less so utils.RemoveExtension is an identity transform and
// DownloadPath() == filepath.Join(SavePath, Name).
func seedOnDisk(t *testing.T, e *Entry) string {
	t.Helper()
	dir := e.DownloadPath()
	if dir == "" {
		t.Fatalf("DownloadPath() empty for entry %q (SavePath=%q Name=%q)", e.InfoHash, e.SavePath, e.Name)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "payload.bin"), []byte("data"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return dir
}

func TestDeleteSymmetric(t *testing.T) {
	const (
		hashEntries = "AABBCCDDEEFF00112233445566778899AABBCCDD"
		hashQueue   = "1122334455667788990011223344556677889900"
		hashBoth    = "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF0"
		hashNeither = "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"
		hashOrder   = "0F0F0F0F0F0F0F0F0F0F0F0F0F0F0F0F0F0F0F0F"
	)

	t.Run("entries only", func(t *testing.T) {
		s := newTestStorage(t)
		e := &Entry{InfoHash: hashEntries, Name: "EntriesOnlyEntry", SavePath: t.TempDir()}
		if err := s.AddOrUpdate(e); err != nil {
			t.Fatalf("AddOrUpdate: %v", err)
		}
		dir := seedOnDisk(t, e)

		calls := 0
		err := s.DeleteSymmetric(hashEntries, func(*Entry) error { calls++; return nil })
		if err != nil {
			t.Fatalf("DeleteSymmetric returned error: %v", err)
		}
		if _, gErr := s.Get(hashEntries); gErr == nil {
			t.Error("entry still readable from entries store after DeleteSymmetric")
		}
		if _, statErr := os.Stat(dir); !os.IsNotExist(statErr) {
			t.Errorf("download dir still present: stat err = %v", statErr)
		}
		if calls != 1 {
			t.Errorf("cleanup called %d times, want 1", calls)
		}
		if !s.IsTombstoned(hashEntries) {
			t.Error("expected tombstone after DeleteSymmetric")
		}
	})

	t.Run("queue only", func(t *testing.T) {
		s := newTestStorage(t)
		e := &Entry{InfoHash: hashQueue, Name: "QueueOnlyEntry", SavePath: t.TempDir()}
		if err := s.AddQueue(e); err != nil {
			t.Fatalf("AddQueue: %v", err)
		}
		dir := seedOnDisk(t, e)

		calls := 0
		err := s.DeleteSymmetric(hashQueue, func(*Entry) error { calls++; return nil })
		if err != nil {
			t.Fatalf("DeleteSymmetric returned error: %v", err)
		}
		if _, gErr := s.GetQueued(hashQueue); gErr == nil {
			t.Error("entry still readable from queue store after DeleteSymmetric")
		}
		if _, statErr := os.Stat(dir); !os.IsNotExist(statErr) {
			t.Errorf("download dir still present: stat err = %v", statErr)
		}
		if calls != 1 {
			t.Errorf("cleanup called %d times, want 1", calls)
		}
		if !s.IsTombstoned(hashQueue) {
			t.Error("expected tombstone after DeleteSymmetric")
		}
	})

	t.Run("both stores", func(t *testing.T) {
		s := newTestStorage(t)
		e := &Entry{InfoHash: hashBoth, Name: "BothStoresEntry", SavePath: t.TempDir()}
		if err := s.AddOrUpdate(e); err != nil {
			t.Fatalf("AddOrUpdate: %v", err)
		}
		if err := s.AddQueue(e); err != nil {
			t.Fatalf("AddQueue: %v", err)
		}
		dir := seedOnDisk(t, e)

		calls := 0
		err := s.DeleteSymmetric(hashBoth, func(*Entry) error { calls++; return nil })
		if err != nil {
			t.Fatalf("DeleteSymmetric returned error: %v", err)
		}
		if _, gErr := s.Get(hashBoth); gErr == nil {
			t.Error("entry still readable from entries store after DeleteSymmetric")
		}
		if _, gErr := s.GetQueued(hashBoth); gErr == nil {
			t.Error("entry still readable from queue store after DeleteSymmetric")
		}
		if _, statErr := os.Stat(dir); !os.IsNotExist(statErr) {
			t.Errorf("download dir still present: stat err = %v", statErr)
		}
		if calls != 1 {
			t.Errorf("cleanup called %d times, want exactly 1 (not once per store)", calls)
		}
		if !s.IsTombstoned(hashBoth) {
			t.Error("expected tombstone after DeleteSymmetric")
		}
	})

	t.Run("neither store", func(t *testing.T) {
		s := newTestStorage(t)
		calls := 0
		err := s.DeleteSymmetric(hashNeither, func(*Entry) error { calls++; return nil })
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound when entry is in neither store, got %v", err)
		}
		if calls != 0 {
			t.Errorf("cleanup called %d times, want 0 for a no-op delete", calls)
		}
		if s.IsTombstoned(hashNeither) {
			t.Error("must NOT write a tombstone for an entry that was never present")
		}
	})

	t.Run("ordering tombstone before cleanup", func(t *testing.T) {
		s := newTestStorage(t)
		e := &Entry{InfoHash: hashOrder, Name: "OrderingEntry", SavePath: t.TempDir()}
		if err := s.AddOrUpdate(e); err != nil {
			t.Fatalf("AddOrUpdate: %v", err)
		}
		seedOnDisk(t, e)

		sawTombstone := false
		err := s.DeleteSymmetric(hashOrder, func(*Entry) error {
			sawTombstone = s.IsTombstoned(hashOrder)
			return nil
		})
		if err != nil {
			t.Fatalf("DeleteSymmetric returned error: %v", err)
		}
		if !sawTombstone {
			t.Error("tombstone was NOT already written at the instant cleanup ran; ordering invariant violated")
		}
	})
}
