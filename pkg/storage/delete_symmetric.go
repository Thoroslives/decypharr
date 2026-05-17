package storage

import (
	"errors"
	"fmt"
	"os"
)

// ErrNotFound is returned by DeleteSymmetric when the hash exists in neither
// the queue nor the entries store. Callers match it with errors.Is to return
// HTTP 404 instead of silently treating a no-op delete as success.
var ErrNotFound = errors.New("delete: entry not found in queue or entries")

// DeleteSymmetric is the canonical "delete everything for this hash" op.
//
// The ordering is strict and load-bearing: the tombstone is written FIRST so a
// sync tick that races in before the store records are removed sees an active
// tombstone and does not re-adopt the entry (each store has its own mutex, so
// the delete is not atomic across them). A not-found in BOTH stores returns
// ErrNotFound without writing a tombstone or touching the filesystem - nothing
// was deleted, so the caller must surface that rather than report success.
func (s *Storage) DeleteSymmetric(infohash string, cleanup func(*Entry) error) error {
	entry, errE := s.Get(infohash)        // entries (raw key)
	queued, errQ := s.GetQueued(infohash) // queue (lowercased key)
	resolved := entry
	if resolved == nil {
		resolved = queued
	}
	if resolved == nil {
		return fmt.Errorf("%s: %w", infohash, ErrNotFound)
	}

	if err := s.PutTombstone(infohash); err != nil {
		s.logger.Warn().Err(err).Str("infohash", infohash).Msg("tombstone write failed; aborting delete to avoid a tombstone-less removal")
		return fmt.Errorf("delete %s: tombstone write failed: %w", infohash, err)
	}

	// Remove the downloaded files before the store record. Orphaned
	// .fuse_hidden directories are the observable symptom of skipping this.
	if p := resolved.DownloadPath(); p != "" {
		if err := os.RemoveAll(p); err != nil {
			s.logger.Error().Err(err).Str("path", p).Msg("DeleteSymmetric: failed to remove downloaded files")
		}
	}

	var cleanupErr error
	if cleanup != nil {
		cleanupErr = cleanup(resolved)
	}

	// errQ/errE == nil guarantees a non-nil entry: Get/GetQueued return
	// (nil, err) on miss and (entry, nil) otherwise.
	if errQ == nil {
		_ = s.DeleteQueued(infohash, nil)
	}
	if errE == nil {
		_ = s.Delete(infohash) // entries + removeFromEntryItem
	}
	return cleanupErr
}
