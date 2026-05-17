package storage

import (
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/testutil"
)

func newTestStorage(t *testing.T) *Storage {
	testutil.IsolateConfig(t, t.TempDir())
	s, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestTombstonePutIsDeleteRoundTrip(t *testing.T) {
	s := newTestStorage(t)
	const hash = "ABC123def"
	if s.IsTombstoned(hash) {
		t.Fatal("unexpected tombstone before Put")
	}
	if err := s.PutTombstone(hash); err != nil {
		t.Fatalf("PutTombstone: %v", err)
	}
	if !s.IsTombstoned(hash) {
		t.Fatal("expected tombstone after Put")
	}
	if !s.IsTombstoned("abc123DEF") {
		t.Fatal("expected case-insensitive tombstone hit")
	}
	if err := s.DeleteTombstone(hash); err != nil {
		t.Fatalf("DeleteTombstone: %v", err)
	}
	if s.IsTombstoned(hash) {
		t.Fatal("tombstone should be gone after Delete")
	}
}

func TestTombstoneTTLSelfHeal(t *testing.T) {
	s := newTestStorage(t)
	const hash = "deadbeef"
	old := TombstoneRecord{InfoHash: hash, DeletedAt: time.Now().Add(-(tombstoneTTL + time.Minute))}
	if err := s.putTombstoneRecord(hash, &old); err != nil {
		t.Fatalf("putTombstoneRecord: %v", err)
	}
	if s.IsTombstoned(hash) {
		t.Fatal("expired tombstone must read as absent (self-heal)")
	}
}

func TestForEachTombstoneSkipsCorrupt(t *testing.T) {
	s := newTestStorage(t)
	if err := s.PutTombstone("h1"); err != nil {
		t.Fatal(err)
	}
	if err := s.tombstone.Put("h2", []byte("{not json"), nil); err != nil {
		t.Fatal(err)
	}
	n := 0
	if err := s.ForEachTombstone(func(string, *TombstoneRecord) error { n++; return nil }); err != nil {
		t.Fatalf("ForEachTombstone: %v", err)
	}
	if n != 1 {
		t.Fatalf("want 1 valid record, got %d", n)
	}
}

func TestPruneExpiredTombstones(t *testing.T) {
	s := newTestStorage(t)
	if err := s.PutTombstone("fresh1"); err != nil { t.Fatal(err) }
	if err := s.PutTombstone("fresh2"); err != nil { t.Fatal(err) }
	stale := TombstoneRecord{InfoHash: "stale", DeletedAt: time.Now().Add(-(tombstoneTTL + time.Hour))}
	if err := s.putTombstoneRecord("stale", &stale); err != nil { t.Fatal(err) }
	if n := s.PruneExpiredTombstones(); n != 1 {
		t.Fatalf("PruneExpiredTombstones: want 1 pruned, got %d", n)
	}
	if !s.IsTombstoned("fresh1") || !s.IsTombstoned("fresh2") {
		t.Fatal("fresh tombstones must survive prune")
	}
}
