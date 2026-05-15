package manager

import (
	"context"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// contentLengthProber is the minimal surface reconcileEntrySize needs from a
// debrid client: a single HEAD against an already-resolved download link
// returning the authoritative CDN Content-Length. *realdebrid.RealDebrid
// satisfies this via HeadContentLength. Kept as a tiny local interface (rather
// than widening debrid.Client) so only the provider with the verified
// size-fidelity bug implements it and the manager has no extra import.
type contentLengthProber interface {
	HeadContentLength(ctx context.Context, downloadLink string) (int64, error)
}

// largestActiveFile returns the single largest non-deleted file in the entry,
// or nil if there are none. This is the media file that drives Radarr's
// size-verified move (Concern 2: reconcile the primary file only, one HEAD per
// completion, never a per-file loop).
func largestActiveFile(entry *storage.Entry) *storage.File {
	var largest *storage.File
	for _, f := range entry.GetActiveFiles() {
		if largest == nil || f.Size > largest.Size {
			largest = f
		}
	}
	return largest
}

// reconcileEntrySize HEADs the resolved RD link of the entry's largest file,
// reads the authoritative CDN Content-Length, and (only on success) sets that
// File.Size and recomputes Entry.Size as the sum of active file sizes, then
// persists through q.Update -> Storage.UpdateQueue -> the QUEUE store.
//
// Concern 3 (BLOCKING): the qBit-compat /torrents/info read path is
// handleTorrentsInfo -> Queue().ListFilter() -> Storage.FilterQueued() -> the
// queue store (lowercased key). A completed Download-action entry stays in the
// queue store (only DownloadActionNone deletes it) and markAsCompleted already
// persists via queue.Update. So this writes to the SAME store the qbit-compat
// path reads. The dual entries/queue store asymmetry (B1/B3) is NOT addressed
// here; this only writes to the correct one of the two.
//
// Concern 2 / B0: on ANY link/HEAD error (notably RD bytes_limit_reached during
// an account cap, or a 451 DMCA on the CDN) this logs Warn, keeps the prior
// size, and returns nil. It never fails completion and never persists a wrong
// size. Under a cap the link does not resolve upstream so this is INERT (the
// caller passes an empty link and the probe is skipped).
//
// Concern 6: this corrects size for COMPLETED local-pull entries only. It does
// not claim to fix the streamed-before-complete normalizeStreamRange path
// universally.
func reconcileEntrySize(ctx context.Context, q *Queue, entry *storage.Entry, prober contentLengthProber, downloadLink string) error {
	if entry == nil || prober == nil {
		return nil
	}

	largest := largestActiveFile(entry)
	if largest == nil {
		// Nothing to reconcile (no active files); leave size as-is.
		return nil
	}

	if downloadLink == "" {
		// Link never resolved (e.g. RD bytes_limit_reached cap). Fix 2 is inert
		// here by design: keep the prior size, do not fail completion.
		q.logger.Warn().
			Str("entry", entry.Name).
			Str("file", largest.Name).
			Msg("size reconcile skipped: no resolved download link (inert under RD cap / link-gen failure)")
		return nil
	}

	trueLen, err := prober.HeadContentLength(ctx, downloadLink)
	if err != nil {
		// Keep prior size; never fail completion on a probe error.
		q.logger.Warn().
			Err(err).
			Str("entry", entry.Name).
			Str("file", largest.Name).
			Int64("prior_size", entry.Size).
			Msg("size reconcile skipped: HEAD probe failed, keeping prior advertised size")
		return nil
	}

	if trueLen <= 0 || trueLen == largest.Size {
		// Nothing to correct (unknown length already errored above, or the
		// advertised size already matched delivered bytes).
		return nil
	}

	oldFileSize := largest.Size
	oldEntrySize := entry.Size
	largest.Size = trueLen

	total := int64(0)
	for _, f := range entry.GetActiveFiles() {
		total += f.Size
	}
	entry.Size = total

	if err := q.Update(entry); err != nil {
		// Persist failure: roll the in-memory mutation back so a later code
		// path does not act on a size we failed to durably record.
		largest.Size = oldFileSize
		entry.Size = oldEntrySize
		q.logger.Warn().
			Err(err).
			Str("entry", entry.Name).
			Msg("size reconcile computed but failed to persist to queue store; reverted in-memory")
		return nil
	}

	q.logger.Info().
		Str("entry", entry.Name).
		Str("file", largest.Name).
		Int64("advertised_file_size", oldFileSize).
		Int64("true_file_size", trueLen).
		Int64("entry_size", entry.Size).
		Msg("reconciled entry/file size to authoritative CDN content length on completion")
	return nil
}
