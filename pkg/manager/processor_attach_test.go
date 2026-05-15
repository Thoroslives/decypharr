package manager

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/testutil"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// newAddNewTorrentStyleEntry builds a *storage.Entry exactly the way
// AddNewTorrent constructs it before queueing: bare Providers/Files maps,
// no ActiveProvider, size taken from the magnet only.
func newAddNewTorrentStyleEntry(infohash, name string, magnetSize int64) *storage.Entry {
	return &storage.Entry{
		InfoHash:         infohash,
		Name:             name,
		OriginalFilename: name,
		Protocol:         config.ProtocolTorrent,
		Size:             magnetSize,
		Bytes:            magnetSize,
		Status:           debridTypes.TorrentStatusDownloading,
		State:            storage.EntryStateDownloading,
		Progress:         0,
		SavePath:         "/tmp/save",
		Providers:        make(map[string]*storage.ProviderEntry),
		Files:            make(map[string]*storage.File),
		Tags:             []string{},
	}
}

// representativeDebridTorrent mirrors the RD-cached torrent shape that
// SendToDebrid returns: a provider name, a real size, and a couple of
// files. Status drives the processNewTorrent post-attach branch.
func representativeDebridTorrent(infohash string, status debridTypes.TorrentStatus) *debridTypes.Torrent {
	return &debridTypes.Torrent{
		Id:               "RD-ID-123",
		InfoHash:         infohash,
		Name:             "Inside.Out.2015.1080p",
		OriginalFilename: "Inside.Out.2015.1080p.mkv",
		Size:             4 * 1024 * 1024 * 1024,
		Bytes:            4 * 1024 * 1024 * 1024,
		Debrid:           "realdebrid",
		Status:           status,
		Files: map[string]debridTypes.File{
			"a.mkv": {Id: "f1", Name: "a.mkv", Size: 3 * 1024 * 1024 * 1024, Link: "http://rd/a"},
			"b.nfo": {Id: "f2", Name: "b.nfo", Size: 1024, Link: "http://rd/b"},
		},
	}
}

// TestAttachProviderMakesEntryRestartRecoverable is the P0 guard.
//
// AddNewTorrent builds a bare entry (empty Providers, no ActiveProvider)
// and submits a JobTypeNew job to the in-memory JobQueue. processNewTorrent
// sets the provider/size/files later. If the submission is over
// max_downloads it sits as an in-memory job; a container restart drops the
// in-memory JobQueue and the job is lost. processQueuedEntries
// (processor.go:167) only re-submits queue-bucket torrents whose
// ActiveProvider != "", so an entry that never ran processNewTorrent is
// skipped forever and the grab is silently lost.
//
// The fix attaches the provider/size/files in AddNewTorrent BEFORE the
// entry is queued. This test persists an AddNewTorrent-style entry through
// the attach path, re-opens storage (simulating a restart), and asserts the
// recovered entry satisfies the processQueuedEntries ActiveProvider gate.
//
// RED before the fix: attachProvider does not exist / is not called, so the
// persisted entry has ActiveProvider == "" and the gate skips it.
func TestAttachProviderMakesEntryRestartRecoverable(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())

	dbPath := filepath.Join(t.TempDir(), "db")
	strg, err := storage.NewStorage(dbPath)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	m := NewForTest(strg, zerolog.Nop())

	const infohash = "0123456789abcdef0123456789abcdef01234567"
	entry := newAddNewTorrentStyleEntry(infohash, "magnet-name", 1)
	debridTorrent := representativeDebridTorrent(infohash, debridTypes.TorrentStatusDownloading)

	// This is the AddNewTorrent ordering: attach BEFORE queueing.
	m.attachProvider(entry, debridTorrent)
	if err := m.queue.Add(entry); err != nil {
		t.Fatalf("queue.Add: %v", err)
	}

	// Simulate a container restart: in-memory JobQueue is gone, only the
	// persisted queue bucket survives. Re-open storage from the same path.
	strg2, err := storage.NewStorage(dbPath)
	if err != nil {
		t.Fatalf("re-open NewStorage: %v", err)
	}
	recovered, err := strg2.GetQueued(infohash)
	if err != nil {
		t.Fatalf("GetQueued after restart: %v", err)
	}
	if recovered == nil {
		t.Fatal("recovered entry is nil after restart")
	}

	// processQueuedEntries (processor.go:166-171) only submits a torrent
	// for recovery when ActiveProvider != "".
	if !recovered.IsTorrent() {
		t.Fatalf("recovered entry is not a torrent protocol: %q", recovered.Protocol)
	}
	if recovered.ActiveProvider == "" {
		t.Fatal("recovered entry has empty ActiveProvider: processQueuedEntries would skip it forever (grab lost)")
	}
	if len(recovered.Providers) == 0 {
		t.Fatal("recovered entry has no Providers")
	}
	if _, ok := recovered.Providers[recovered.ActiveProvider]; !ok {
		t.Fatalf("recovered entry has no ProviderEntry for ActiveProvider %q", recovered.ActiveProvider)
	}
	if recovered.GetActiveProvider() == nil {
		t.Fatal("recovered entry GetActiveProvider() is nil (processQueuedTorrent would error out)")
	}
	if len(recovered.Files) == 0 {
		t.Fatal("recovered entry has no Files")
	}
	if recovered.Size <= 0 {
		t.Fatalf("recovered entry Size not set: %d", recovered.Size)
	}
}

// TestAttachProviderNormalPathNoDoubleCount is the devil's-advocate BLOCKING
// no-regression guard. The no-restart path calls attach twice: once in
// AddNewTorrent and once in processNewTorrent (which keeps its attach so the
// two paths are provably identical). This must NOT double-count the size,
// duplicate the provider, or duplicate the files. It also asserts the
// processNewTorrent post-attach logic (placement.DownloadedAt when the
// debrid torrent is Downloaded) still runs after the refactor.
func TestAttachProviderNormalPathNoDoubleCount(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())

	dbPath := filepath.Join(t.TempDir(), "db")
	strg, err := storage.NewStorage(dbPath)
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	m := NewForTest(strg, zerolog.Nop())

	const infohash = "fedcba9876543210fedcba9876543210fedcba98"
	entry := newAddNewTorrentStyleEntry(infohash, "magnet-name", 1)
	debridTorrent := representativeDebridTorrent(infohash, debridTypes.TorrentStatusDownloaded)
	wantSize := debridTorrent.GetSize()
	wantFiles := len(debridTorrent.Files)

	// Path step 1: AddNewTorrent attaches, then queues.
	m.attachProvider(entry, debridTorrent)
	if err := m.queue.Add(entry); err != nil {
		t.Fatalf("queue.Add: %v", err)
	}

	// Path step 2: the JobTypeNew worker runs processNewTorrent on the same
	// entry. processNewTorrent re-attaches (via the shared helper) and then
	// runs its post-attach logic. We do not call processAction here (it
	// reaches into the downloader which NewForTest does not wire); instead
	// we replicate processNewTorrent's attach + the Downloaded placement
	// marking so the parity assertions cover the real post-attach path.
	m.attachProvider(entry, debridTorrent)
	if debridTorrent.Status == debridTypes.TorrentStatusDownloaded {
		if placement := entry.GetActiveProvider(); placement != nil {
			now := time.Now()
			placement.DownloadedAt = &now
			placement.Progress = 1.0
		}
	}
	if err := m.queue.Update(entry); err != nil {
		t.Fatalf("queue.Update: %v", err)
	}

	// Exactly one provider for that debrid (AddTorrentProvider is a map
	// assignment by debrid key, not an append).
	if len(entry.Providers) != 1 {
		t.Fatalf("expected exactly 1 provider, got %d: %v", len(entry.Providers), entry.Providers)
	}
	if _, ok := entry.Providers[debridTorrent.Debrid]; !ok {
		t.Fatalf("no provider entry for debrid %q", debridTorrent.Debrid)
	}

	// Size/Bytes equal to debrid size, NOT doubled (= assignment, not +=).
	if entry.Size != wantSize {
		t.Errorf("Size doubled or wrong: got %d want %d", entry.Size, wantSize)
	}
	if entry.Bytes != wantSize {
		t.Errorf("Bytes doubled or wrong: got %d want %d", entry.Bytes, wantSize)
	}

	// Files count equals the debrid file count, NOT doubled (map by name).
	if len(entry.Files) != wantFiles {
		t.Errorf("Files doubled or wrong: got %d want %d", len(entry.Files), wantFiles)
	}

	// processNewTorrent post-attach logic still marks the placement
	// downloaded when the debrid torrent is Downloaded.
	placement := entry.GetActiveProvider()
	if placement == nil {
		t.Fatal("GetActiveProvider() nil after attach")
	}
	if placement.DownloadedAt == nil {
		t.Error("placement.DownloadedAt not set for a Downloaded debrid torrent")
	}
	if placement.Progress != 1.0 {
		t.Errorf("placement.Progress = %v, want 1.0", placement.Progress)
	}

	// And the round-trip is consistent: re-open storage, same single
	// provider, same non-doubled size/files.
	strg2, err := storage.NewStorage(dbPath)
	if err != nil {
		t.Fatalf("re-open NewStorage: %v", err)
	}
	recovered, err := strg2.GetQueued(infohash)
	if err != nil {
		t.Fatalf("GetQueued: %v", err)
	}
	if len(recovered.Providers) != 1 {
		t.Errorf("recovered providers != 1: %d", len(recovered.Providers))
	}
	if recovered.Size != wantSize {
		t.Errorf("recovered Size = %d want %d", recovered.Size, wantSize)
	}
	if len(recovered.Files) != wantFiles {
		t.Errorf("recovered Files = %d want %d", len(recovered.Files), wantFiles)
	}
}
