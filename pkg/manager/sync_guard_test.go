package manager

import (
	"context"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/internal/testutil"
)

// TestShouldSkipSyncForInFlight guards the defensive Fix C: detectTorrentChanges
// must not route an entry through processNewTorrents / placement-update paths if
// a local download is currently in flight for that hash.
//
// The existing NeedsUpdate check at fixes-v2.3 HEAD `93ae749` closes most of
// this race (ID match + status match + Files-non-empty all hold for a
// completed in-flight entry, so NeedsUpdate returns false), but RD-side ID
// recycling, status flap, or freshly-completed RD torrents whose local pull
// hasn't yet hit processAction could still slip through.
//
// This helper plus its caller in detectTorrentChanges adds belt-and-braces
// using the Fix B downloadCancels registry as the source of truth for
// "in-flight": if a hash is registered for active download, sync skips it.
func TestShouldSkipSyncForInFlight(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())
	m := &Manager{downloadCancels: xsync.NewMap[string, *downloadHandle]()}

	if m.shouldSkipSyncForInFlight("never-registered") {
		t.Error("unregistered hash must not be skipped")
	}

	_, release := m.RegisterDownload("hash-a", context.Background())
	defer release()

	if !m.shouldSkipSyncForInFlight("hash-a") {
		t.Error("registered in-flight hash MUST be skipped")
	}

	// Different hash with one registered: must NOT short-circuit unrelated hashes.
	if m.shouldSkipSyncForInFlight("hash-b") {
		t.Error("unrelated hash must not be skipped when another is in flight")
	}

	// After release, the entry is cleared from the registry, so sync must
	// route the hash normally again (avoids permanently-stuck entries if the
	// release somehow runs before the next sync tick).
	release()
	if m.shouldSkipSyncForInFlight("hash-a") {
		t.Error("released hash must not be skipped")
	}
}
