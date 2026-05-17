package storage

import (
	"fmt"
	"os"
	"strings"
)

// DeleteSymmetric is the canonical "delete everything for this hash" op. It
// resolves the entry from whichever store holds it (queue keyed lowercased,
// entries raw), then in STRICT order (tombstone-first - separate per-store
// hybrid mutexes make a later tombstone a re-adopt race vs a concurrent sync
// tick): 1.PutTombstone 2.os.RemoveAll(DownloadPath) 3.cleanup once, error
// propagated 4.remove store record(s). Not-found in BOTH -> real error, NO
// tombstone, NO file op (nothing was deleted; callers stop returning 200).
func (s *Storage) DeleteSymmetric(infohash string, cleanup func(*Entry) error) error {
	entry, errE := s.Get(infohash)        // entries (raw)
	queued, errQ := s.GetQueued(infohash) // queue   (lower)
	resolved := entry
	if resolved == nil {
		resolved = queued
	}
	if resolved == nil {
		return fmt.Errorf("delete: entry %s not found in queue or entries", infohash)
	}
	// 1. Tombstone FIRST - before the entry is gone, so a sync tick racing in
	//    the gap sees IsTombstoned()==true and does NOT re-adopt. Abort on
	//    failure rather than do a tombstone-less delete (that re-opens the loop).
	if err := s.PutTombstone(infohash); err != nil {
		s.logger.Warn().Err(err).Str("infohash", infohash).Msg("tombstone write failed; aborting delete to avoid a tombstone-less removal")
		return fmt.Errorf("delete %s: tombstone write failed: %w", infohash, err)
	}
	// 2. Local-file delete - replicates Queue.deleteEntryFiles; orphaned
	//    .fuse_hidden files ARE the B1 symptom.
	if p := resolved.DownloadPath(); p != "" {
		if err := os.RemoveAll(p); err != nil {
			s.logger.Error().Err(err).Str("path", p).Msg("DeleteSymmetric: failed to remove downloaded files")
		}
	}
	// 3. cleanup once, error propagated (no _ = swallow).
	var cleanupErr error
	if cleanup != nil {
		cleanupErr = cleanup(resolved)
	}
	// 4. Remove store record(s) from wherever it lived.
	if errQ == nil && queued != nil {
		_ = s.queue.Delete(strings.ToLower(infohash))
	}
	if errE == nil && entry != nil {
		_ = s.Delete(infohash) // entries + removeFromEntryItem
	}
	return cleanupErr
}
