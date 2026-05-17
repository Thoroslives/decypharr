package manager

import (
	"context"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/testutil"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// readoptFakeClient is a complete-on-arrival provider stub for the
// processSyncTorrent re-adoption gate tests. ProviderClient(t.Debrid) must
// resolve to a non-nil client or processSyncTorrent returns (nil,nil) at
// torrent.go:372 before the storage/tombstone branch is ever reached, so the
// test would pass for the wrong reason. UpdateTorrent is a no-op because the
// torrents the tests feed already carry a linked (complete) file, so the
// needsUpdate branch (torrent.go:376) is skipped and control falls straight
// to the m.storage.Get miss branch where the gate lives. The embedded
// debrid.Client is nil: any other method panics if the gate wrongly proceeds
// (strict by design - mirrors rd451FakeClient).
type readoptFakeClient struct {
	debrid.Client
}

func (c *readoptFakeClient) UpdateTorrent(tr *types.Torrent) error { return nil }

// newManagerWithStorageAndFakeRD builds a *Manager backed by a real
// bbolt-rooted Storage (so the tombstone store is live) AND a fake provider
// registered under "realdebrid". This is the surface processSyncTorrent and
// AddNewTorrent need: real storage for IsTombstoned/DeleteTombstone, a
// resolvable client for ProviderClient/FilterDebrid. NewForTest gives the
// storage+queue wiring; the fake client is then layered on top.
func newManagerWithStorageAndFakeRD(t *testing.T, client debrid.Client) (*Manager, *storage.Storage) {
	t.Helper()
	testutil.IsolateConfig(t, t.TempDir())
	strg, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	t.Cleanup(func() { _ = strg.Close() })

	m := NewForTest(strg, zerolog.Nop())
	m.clients = xsync.NewMap[string, debrid.Client]()
	m.clients.Store("realdebrid", client)
	return m, strg
}

// completeRemoteTorrent returns a *types.Torrent that is already complete
// (one file with a non-empty Link, so isComplete(t.Files) is true). This
// makes processSyncTorrent skip the UpdateTorrent API hop and fall through
// to the m.storage.Get(InfoHash) miss branch - the exact spot the
// re-adoption gate guards.
func completeRemoteTorrent(infohash string) *types.Torrent {
	return &types.Torrent{
		Id:       "remote-" + infohash,
		InfoHash: infohash,
		Name:     "deleted.but.still.on.rd",
		Size:     1024,
		Bytes:    1024,
		Status:   types.TorrentStatusDownloaded,
		Debrid:   "realdebrid",
		Files: map[string]types.File{
			"file.mkv": {
				Id:   "f1",
				Name: "file.mkv",
				Size: 1024,
				Link: "https://real-debrid.example/d/abc",
			},
		},
	}
}

// TestTombstonedHashIsNotReAdopted is the core Task 4 invariant: a hash with
// an active deletion tombstone must NOT be re-adopted by the sync path. Pre-
// fix, processSyncTorrent unconditionally built a fresh storage.Entry on a
// Get miss and returned it for batched write, re-spawning the .fuse_hidden
// loop. The gate must short-circuit to (nil,nil) - processNewTorrents:293
// already drops nil, so no entry is ever written.
func TestTombstonedHashIsNotReAdopted(t *testing.T) {
	const hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	m, strg := newManagerWithStorageAndFakeRD(t, &readoptFakeClient{})

	if err := strg.PutTombstone(hash); err != nil {
		t.Fatalf("PutTombstone: %v", err)
	}
	// Precondition: no existing entry, so without the gate processSyncTorrent
	// WOULD reach the fresh-create branch and re-adopt.
	if _, err := strg.Get(hash); err == nil {
		t.Fatal("precondition: expected no stored entry for tombstoned hash")
	}

	mt, err := m.processSyncTorrent(completeRemoteTorrent(hash))
	if err != nil {
		t.Fatalf("processSyncTorrent on tombstoned hash returned error: %v", err)
	}
	if mt != nil {
		t.Fatalf("tombstoned hash must NOT be re-adopted: processSyncTorrent returned a non-nil entry %+v", mt)
	}
	// No entry must have been created as a side effect.
	if _, err := strg.Get(hash); err == nil {
		t.Fatal("tombstoned hash must NOT have a stored entry after processSyncTorrent")
	}
	// Tombstone itself must remain (sync does not clear it; only a deliberate
	// add does).
	if !strg.IsTombstoned(hash) {
		t.Fatal("tombstone must survive a sync-path skip")
	}
}

// TestNonTombstonedHashIsReAdopted is the adjacent fence proving the gate
// only blocks when a tombstone is present. Same setup as the blocked case
// but with NO tombstone -> processSyncTorrent must still re-adopt (return a
// non-nil *storage.Entry for the hash). This guarantees the gate is a
// targeted skip, not a blanket disable of re-adoption, and that the blocked
// test above fails for the right reason. TTL-expiry self-heal is covered by
// the storage package's own TestTombstoneTTLSelfHeal.
func TestNonTombstonedHashIsReAdopted(t *testing.T) {
	const hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	m, strg := newManagerWithStorageAndFakeRD(t, &readoptFakeClient{})

	if strg.IsTombstoned(hash) {
		t.Fatal("precondition: hash must not be tombstoned")
	}

	mt, err := m.processSyncTorrent(completeRemoteTorrent(hash))
	if err != nil {
		t.Fatalf("processSyncTorrent returned error: %v", err)
	}
	if mt == nil {
		t.Fatal("non-tombstoned hash must be re-adopted: processSyncTorrent returned nil")
	}
	if mt.InfoHash != hash {
		t.Fatalf("re-adopted entry InfoHash: got %q want %q", mt.InfoHash, hash)
	}
}

// TestDetectTorrentChangesSkipsTombstonedNew is the belt-and-braces half:
// detectTorrentChanges must NOT classify a tombstoned remote hash as "new"
// (the brand-new branch at torrent.go:227-231), so it never even fans out a
// worker. With an empty cache, a non-tombstoned remote hash IS new; the same
// hash with a tombstone must be excluded.
func TestDetectTorrentChangesSkipsTombstonedNew(t *testing.T) {
	const liveHash = "cccccccccccccccccccccccccccccccccccccccc"
	const deadHash = "dddddddddddddddddddddddddddddddddddddddd"
	m, strg := newManagerWithStorageAndFakeRD(t, &readoptFakeClient{})

	if err := strg.PutTombstone(deadHash); err != nil {
		t.Fatalf("PutTombstone: %v", err)
	}

	remote := map[string]*types.Torrent{
		liveHash: completeRemoteTorrent(liveHash),
		deadHash: completeRemoteTorrent(deadHash),
	}

	newTorrents, _, _, err := m.detectTorrentChanges("realdebrid", remote)
	if err != nil {
		t.Fatalf("detectTorrentChanges: %v", err)
	}

	var sawLive, sawDead bool
	for _, nt := range newTorrents {
		if nt.InfoHash == liveHash {
			sawLive = true
		}
		if nt.InfoHash == deadHash {
			sawDead = true
		}
	}
	if !sawLive {
		t.Error("non-tombstoned brand-new hash must be classified as new")
	}
	if sawDead {
		t.Error("tombstoned hash must NOT be classified as new (no worker fan-out)")
	}
}

// TestDeliberateAddClearsTombstone proves the explicit re-grab path clears
// the tombstone so an intentional re-add always works. AddNewTorrent must
// DeleteTombstone as its first storage interaction (before SendToDebrid).
// SendToDebrid is stubbed to fail (rd451FakeClient with a hard error) so the
// test does not hit the network; the tombstone-clear happens BEFORE that
// failure, so IsTombstoned must be false regardless of the add's outcome.
func TestDeliberateAddClearsTombstone(t *testing.T) {
	const hash = "1111111111111111111111111111111111111111"
	// rd451FakeClient.SubmitMagnet returns submitErr; a hard error makes
	// SendToDebrid fail fast (no network), exercising AddNewTorrent's first
	// statement (the tombstone clear) then the early error return.
	m, strg := newManagerWithStorageAndFakeRD(t, &rd451FakeClient{submitErr: customerror.InfringingFileError})

	if err := strg.PutTombstone(hash); err != nil {
		t.Fatalf("PutTombstone: %v", err)
	}
	if !strg.IsTombstoned(hash) {
		t.Fatal("precondition: hash must be tombstoned before the deliberate add")
	}

	magnet := &utils.Magnet{
		Name:     "deliberate.regrab",
		InfoHash: hash,
		Link:     "magnet:?xt=urn:btih:" + hash,
	}
	a := arr.New("sonarr", "http://sonarr:8989", "token", false, false, nil, "", "manual")
	importReq := NewTorrentRequest("realdebrid", t.TempDir(), magnet, a, config.DownloadActionSymlink, nil, "", ImportTypeQBit, false)

	// The add itself is expected to fail at SendToDebrid (stubbed), which is
	// fine: the contract is that the tombstone is cleared regardless.
	_ = m.AddNewTorrent(context.Background(), importReq)

	if strg.IsTombstoned(hash) {
		t.Fatal("deliberate add must clear the deletion tombstone (intentional re-grab must always work)")
	}
}
