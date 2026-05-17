package manager

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// processingEntries TTL/sweep parameters (G6). A worker goroutine that
// panics or exits via an unexpected path never reaches its
// `defer m.processingEntries.Delete(...)`, so the hash stays in the map
// forever and blocks future re-processing. The sweep reclaims those.
//
// TODO(future): make these configurable via config.Config if soak testing
// shows the defaults are wrong.
const (
	processingEntriesTTL        = 5 * time.Minute
	processingEntriesSweepEvery = 1 * time.Minute

	// processingEntriesAbsoluteCeiling is a hard upper bound on how long a
	// processingEntries slot may survive the in-flight gate. The gate
	// (hasInFlightDownload) deliberately refuses to reclaim a slot while a
	// download is registered, which is correct for a legit long pull but
	// would silently wedge re-processing forever if a downloadCancels entry
	// ever leaked. Past this ceiling the sweep reclaims regardless of
	// in-flight state: a noisy self-healing reclaim is preferable to a
	// silent unrecoverable block. Set far above any plausible legit pull.
	processingEntriesAbsoluteCeiling = 12 * time.Hour
)

// applyRDProgress writes an RD-side (upstream debrid) ingestion claim onto
// the entry's RDProgress/RDSpeed fields and mirrors the fraction onto the
// active provider placement.
//
// debridPercent is the provider's 0-100 progress value (RD's own format);
// this function divides by 100 so RDProgress matches the 0-1 contract used
// by the qBit-compat handler.
//
// CRITICAL: this function MUST NOT touch entry.Progress or entry.Speed.
// Those fields are reserved for local-pull truth and are written exclusively
// by downloader.go's progressCallback. The pre-Fix-F bug was conflating
// these two semantic surfaces, which let RD's caching-side progress climb
// to 100% while no local bytes had transferred.
func applyRDProgress(entry *storage.Entry, debridPercent float64, debridSpeed int64) {
	entry.RDProgress = debridPercent / 100.0
	entry.RDSpeed = debridSpeed
	if placement := entry.GetActiveProvider(); placement != nil {
		placement.Progress = entry.RDProgress
	}
}

// absoluteCeiling is the hard upper bound past which a processingEntries
// slot is reclaimed even while a download is registered (B5 backstop). It
// is a multiple of remove_stalled_after when configured (a stalled-removal
// window already bounds how long any single grab is allowed to live), and
// falls back to processingEntriesAbsoluteCeiling otherwise. The multiple
// keeps the ceiling comfortably above a legit long pull while still finite.
func (m *Manager) absoluteCeiling() time.Duration {
	if m.queue != nil && m.queue.removeStalledAfter > 0 {
		if c := 4 * m.queue.removeStalledAfter; c > processingEntriesAbsoluteCeiling {
			return c
		}
	}
	return processingEntriesAbsoluteCeiling
}

// sweepProcessingEntries removes entries from processingEntries whose
// timestamp is older than maxAge relative to the clock. Returns the
// number of entries reclaimed.
//
// A slot is reclaimed only when it is past maxAge AND no local pull is
// registered for it (hasInFlightDownload): the sweep exists to reclaim
// slots leaked by workers that died without their `defer Delete()`, and a
// live downloadCancels registration means the worker is NOT dead, so
// reclaiming mid-flight would re-dispatch a second concurrent writer on the
// same inode (B5). Past absoluteCeiling the slot is reclaimed regardless of
// in-flight state (backstop — see processingEntriesAbsoluteCeiling).
func (m *Manager) sweepProcessingEntries(maxAge time.Duration) int {
	now := m.clock.Now()
	cutoff := now.Add(-maxAge)
	ceilingCutoff := now.Add(-m.absoluteCeiling())
	reclaimed := 0
	m.processingEntries.Range(func(key string, ts time.Time) bool {
		pastCeiling := ts.Before(ceilingCutoff)
		if pastCeiling || (ts.Before(cutoff) && !m.hasInFlightDownload(key)) {
			m.processingEntries.Delete(key)
			reclaimed++
		}
		return true
	})
	if reclaimed > 0 {
		m.logger.Warn().
			Int("reclaimed", reclaimed).
			Dur("ttl", maxAge).
			Msg("Reclaimed leaked processingEntries (no in-flight download; worker exited without cleanup or hit the absolute ceiling)")
	}
	return reclaimed
}

// AddNewTorrent creates a torrent from import request and processes it
func (m *Manager) AddNewTorrent(ctx context.Context, importReq *ImportRequest) error {
	// Deliberate re-grab clears any deletion tombstone for this hash FIRST,
	// before anything else touches storage. An explicit add (user, Radarr,
	// qbit - all funnel here via addMagnet/addTorrent) is an intentional
	// signal that overrides a prior deletion, so the sync re-adoption gate
	// (processSyncTorrent / detectTorrentChanges) must stop blocking it.
	// Best-effort only - the tombstone TTL self-heals, so a missed clear
	// here is reclaimed later and never permanently strands a re-grab.
	if err := m.storage.DeleteTombstone(importReq.Magnet.InfoHash); err != nil {
		m.logger.Warn().Err(err).Str("infohash", importReq.Magnet.InfoHash).Msg("failed to clear deletion tombstone on explicit add")
	}

	var (
		debridTorrent *debridTypes.Torrent
		err           error
	)

	debridTorrent, err = m.SendToDebrid(ctx, importReq)
	if err != nil {
		// Check if too many active downloads
		var customErr *customerror.Error
		if errors.As(err, &customErr) && customErr.Code == "too_many_active_downloads" {
			m.logger.Warn().Msgf("Too many active downloads, marking as queued: %s", importReq.Magnet.Name)
			if err := m.queue.ReQueue(importReq); err != nil {
				return err
			}
			return nil
		}
		return fmt.Errorf("failed to submit torrent to debrid: %w", err)
	}

	// Create managed torrent with InfoHash as primary key
	torrent := &storage.Entry{
		InfoHash:         importReq.Magnet.InfoHash,
		Name:             importReq.Magnet.Name,
		OriginalFilename: importReq.Magnet.Name,
		Protocol:         config.ProtocolTorrent,
		Size:             importReq.Magnet.Size,
		Bytes:            importReq.Magnet.Size,
		Magnet:           importReq.Magnet.Link,
		Category:         importReq.Arr.Name,
		SavePath:         filepath.Join(importReq.DownloadFolder, importReq.Arr.Name),
		Status:           debridTypes.TorrentStatusDownloading,
		State:            storage.EntryStateDownloading,
		Progress:         0,
		Action:           importReq.Action,
		DownloadUncached: debridTorrent.DownloadUncached,
		CallbackURL:      importReq.CallBackUrl,
		SkipMultiSeason:  importReq.SkipMultiSeason,
		CreatedAt:        time.Now(),
		UpdatedAt:        time.Now(),
		AddedOn:          time.Now(),
		Providers:        make(map[string]*storage.ProviderEntry),
		Files:            make(map[string]*storage.File),
		Tags:             []string{},
	}
	torrent.ContentPath = torrent.DownloadPath()

	// Attach the provider/size/files BEFORE the entry is queued. A fresh
	// submission over max_downloads sits as an in-memory JobTypeNew job
	// until a slot frees; processNewTorrent (which used to be the only place
	// that attached the provider) has not run yet. A container restart drops
	// the in-memory JobQueue, and processQueuedEntries only recovers
	// queue-bucket entries whose ActiveProvider is set. Attaching here means
	// the persisted entry carries ActiveProvider, so the existing
	// processQueuedEntries -> processQueuedTorrent recovery path picks it up
	// after a restart instead of skipping it forever (silent grab loss).
	m.attachProvider(torrent, debridTorrent)

	// Add to queue
	if err := m.queue.Add(torrent); err != nil {
		return fmt.Errorf("failed to add torrent to queue: %w", err)
	}

	// Success-path fast cleanup: a real storage.Entry now owns this grab, so
	// any durable requeue record for it (written by a previous
	// too_many_active_downloads -> ReQueue on the same hash) is obsolete.
	// Best-effort only - DrainPersistedRequeue is self-healing, so a missed
	// delete here is reclaimed on the next restart, never re-spawned.
	if err := m.queue.DeletePersistedRequeue(torrent.InfoHash); err != nil {
		m.logger.Warn().Err(err).Str("infohash", torrent.InfoHash).Msg("failed to delete persisted requeue record (will self-heal)")
	}

	// Route fresh-submission processing through the JobQueue so bursts
	// (e.g. Radarr MissingMoviesSearch) honor max_downloads. Pre-fix this
	// was an ungated `go m.processNewTorrent(...)` per submission.
	job := &Job{
		ID:            torrent.InfoHash,
		Type:          JobTypeNew,
		Entry:         torrent,
		DebridTorrent: debridTorrent,
		CreatedAt:     time.Now(),
	}
	if err := m.jobQueue.Submit(job); err != nil {
		m.logger.Warn().Err(err).Str("infohash", torrent.InfoHash).Msg("jobQueue submit failed (new torrent)")
	}

	return nil
}

func (m *Manager) processQueuedEntries() {
	queueEntries := m.queue.ListFilter("", config.ProtocolAll, storage.EntryStateDownloading, nil, "", true)
	if len(queueEntries) == 0 {
		return
	}
	for _, entry := range queueEntries {
		// Parse only active downloading torrents
		if entry.State != storage.EntryStateDownloading {
			continue
		}
		// Skip entries that are actively being downloading
		if entry.IsDownloading {
			continue
		}
		// Skip if a previous tick's goroutine hasn't finished yet for this hash.
		// The timestamp lets sweepProcessingEntries reclaim entries leaked by
		// panicked/crashed worker goroutines (G6).
		if _, loaded := m.processingEntries.LoadOrStore(entry.InfoHash, m.clock.Now()); loaded {
			continue
		}
		// B5 early-out: a live local pull is already registered for this
		// hash; re-submitting would put a second concurrent writer on the
		// same inode. Release the slot just taken so a legitimate future
		// re-process is not blocked. processAction is the authoritative
		// guard; this is only a cheap early-out.
		if m.hasInFlightDownload(entry.InfoHash) {
			m.processingEntries.Delete(entry.InfoHash)
			continue
		}
		if entry.IsTorrent() {
			if entry.ActiveProvider != "" {
				m.submitProcessingJob(entry, JobTypeTorrent)
			} else {
				m.processingEntries.Delete(entry.InfoHash)
			}
		} else if entry.IsNZB() {
			m.submitProcessingJob(entry, JobTypeNZB)
		} else {
			m.processingEntries.Delete(entry.InfoHash)
		}
	}
}

// submitProcessingJob hands an entry to the JobQueue worker pool. The pool
// is sized to config.MaxDownloads (Fix A); before Fix A, processQueuedEntries
// spawned per-entry goroutines and ignored the cap.
//
// On submit failure (queue closed, e.g. during shutdown) we release the
// processingEntries slot we took at the call site so the entry isn't
// permanently blocked from re-processing on the next tick.
func (m *Manager) submitProcessingJob(entry *storage.Entry, jobType JobType) {
	job := &Job{
		ID:        entry.InfoHash,
		Type:      jobType,
		Entry:     entry,
		CreatedAt: time.Now(),
	}
	if err := m.jobQueue.Submit(job); err != nil {
		m.logger.Warn().
			Err(err).
			Str("infohash", entry.InfoHash).
			Str("type", string(jobType)).
			Msg("jobQueue submit failed; releasing processingEntries slot")
		m.processingEntries.Delete(entry.InfoHash)
	}
}

// processJob is the dispatcher passed to JobQueue. It maps a *Job back to
// the existing per-protocol processing entrypoints and recovers from panics
// so a single bad job doesn't kill a worker permanently. JobQueue's own
// workers have no recover(); without this wrapper, a panicked processFunc
// would tear down the worker goroutine and shrink the pool.
func (m *Manager) processJob(ctx context.Context, job *Job) {
	defer func() {
		if r := recover(); r != nil {
			infohash := ""
			if job != nil && job.Entry != nil {
				infohash = job.Entry.InfoHash
			}
			jobID := ""
			jobType := ""
			if job != nil {
				jobID = job.ID
				jobType = string(job.Type)
			}
			m.logger.Error().
				Interface("panic", r).
				Str("job_id", jobID).
				Str("job_type", jobType).
				Str("infohash", infohash).
				Msg("job panicked, worker recovered")
		}
	}()
	if job == nil {
		return
	}
	switch job.Type {
	case JobTypeTorrent:
		if job.Entry != nil {
			m.processQueuedTorrent(job.Entry)
		}
	case JobTypeNZB:
		if job.Entry != nil {
			m.processQueuedNZB(job.Entry)
		}
	case JobTypeNew:
		if job.Entry != nil && job.DebridTorrent != nil {
			m.processNewTorrent(job.Entry, job.DebridTorrent)
		}
	}
}

func (m *Manager) processQueuedNZB(entry *storage.Entry) {
	defer m.processingEntries.Delete(entry.InfoHash)
	// Check if the nzb is already processed
	metadata, err := m.usenet.GetNZB(entry.InfoHash)
	if err != nil {
		m.logger.Error().Err(err).Str("name", entry.Name).Msg("Error getting NZB metadata")
		entry.MarkAsError(err)
		_ = m.queue.Update(entry)
		return
	}
	if metadata == nil {
		m.logger.Error().Str("name", entry.Name).Msg("NZB metadata not found")
		entry.MarkAsError(fmt.Errorf("nzb metadata not found"))
		_ = m.queue.Update(entry)
		return
	}
	switch metadata.Status {
	case usenet.NZBStatusFailed:
		m.logger.Error().Str("name", entry.Name).Msg("NZB processing failed")
		entry.MarkAsError(fmt.Errorf("nzb processing failed"))
		_ = m.queue.Update(entry)
		return
	case usenet.NZBStatusParsing, usenet.NZBStatusDownloading:
		// Still processing, skip for now
		return
	case usenet.NZBStatusCompleted:
		if err := m.processNZB(context.Background(), entry, metadata); err != nil {
			m.logger.Error().Err(err).Str("name", entry.Name).Msg("Error processing queued NZB")
			entry.MarkAsError(err)
			_ = m.queue.Update(entry)
			return
		}
	default:
		m.logger.Error().Str("name", entry.Name).Msgf("Unknown NZB status: %s", metadata.Status)
		entry.MarkAsError(fmt.Errorf("unknown nzb status: %s", metadata.Status))
		_ = m.queue.Update(entry)
		return
	}
}

func (m *Manager) processQueuedTorrent(entry *storage.Entry) {
	defer m.processingEntries.Delete(entry.InfoHash)
	placement := entry.GetActiveProvider()
	if placement == nil {
		m.logger.Error().Str("name", entry.Name).Msg("No active placement found for queued entry")
		entry.MarkAsError(fmt.Errorf("no active placement found"))
		_ = m.queue.Update(entry)
		return
	}

	client := m.ProviderClient(entry.ActiveProvider)
	if client == nil {
		m.logger.Error().Str("debrid", entry.ActiveProvider).Msg("Provider client not found")
		entry.MarkAsError(fmt.Errorf("debrid client not found: %s", entry.ActiveProvider))
		_ = m.queue.Update(entry)
		return
	}

	magnet, err := utils.GetMagnetInfo(entry.Magnet, m.config.AlwaysRmTrackerUrls)
	if err != nil {
		magnet = utils.ConstructMagnet(entry.InfoHash, entry.Name)
	}

	arr := m.arr.GetOrCreate(entry.Category)

	debridTorrent := &debridTypes.Torrent{
		Id:               placement.ID,
		InfoHash:         entry.InfoHash,
		Magnet:           magnet,
		Name:             magnet.Name,
		Arr:              arr,
		Size:             entry.Size,
		Files:            make(map[string]debridTypes.File),
		DownloadUncached: entry.DownloadUncached,
	}

	dbT, err := client.CheckStatus(debridTorrent)
	if err != nil {
		m.logger.Error().Err(err).Str("name", entry.Name).Msg("Error checking status")
		entry.MarkAsError(err)
		_ = m.queue.Update(entry)

		// Delete from debrid on error
		go func() {
			if dbT != nil && dbT.Id != "" {
				_ = client.DeleteTorrent(dbT.Id)
			}
		}()
		return
	}

	debridTorrent = dbT

	if debridTorrent == nil {
		m.logger.Error().Str("name", entry.Name).Msg("Provider entry not found")
		entry.MarkAsError(fmt.Errorf("debrid entry not found"))
		_ = m.queue.Update(entry)
		return
	}

	if debridTorrent.Status == debridTypes.TorrentStatusError {
		m.logger.Error().
			Str("debrid", debridTorrent.Debrid).
			Str("name", debridTorrent.Name).
			Str("status", string(debridTorrent.Status)).
			Msg("Entry in error state")
		entry.MarkAsError(fmt.Errorf("entry in error state on debrid: %s", debridTorrent.Debrid))
		_ = m.queue.Update(entry)
		return
	}

	// Update entry progress with RD-side ingestion claim.
	applyRDProgress(entry, debridTorrent.Progress, debridTorrent.Speed)
	entry.Size = debridTorrent.GetSize()
	entry.Seeders = debridTorrent.Seeders
	entry.UpdatedAt = time.Now()

	_ = m.queue.Update(entry)
	// Check if done or failed
	if debridTorrent.Status == debridTypes.TorrentStatusDownloaded {
		m.processAction(entry)
	}
}

func (m *Manager) processAction(entry *storage.Entry) {
	// Central chokepoint for the B5 dup-writer class: RegisterDownload below
	// does downloadCancels.Store, which OVERWRITES any existing handle. If a
	// live registration already exists a different goroutine is mid-pull on
	// this hash; proceeding would orphan the original worker's cancel/done
	// wiring (a DELETE could no longer cancel it) and start a second
	// concurrent writer on the same inode. Bail before any state mutation.
	if m.hasInFlightDownload(entry.InfoHash) {
		m.logger.Info().
			Str("name", entry.Name).
			Str("infohash", entry.InfoHash).
			Msg("Skipping duplicate processAction: a local pull is already in flight for this hash")
		return
	}

	entry.Status = debridTypes.TorrentStatusDownloaded
	entry.UpdatedAt = time.Now()
	_ = m.queue.Update(entry)
	m.logger.Info().
		Str("name", entry.Name).
		Str("action", string(entry.Action)).
		Msg("Download completed, processing action")

	// Merge with existing entry if same infohash already exists (e.g., same
	// torrent on a different provider). The queue entry only knows about the
	// provider it was queued for, so we need to preserve other placements.
	if existing, err := m.storage.Get(entry.InfoHash); err == nil && existing != nil {
		entry = storage.HandleExistingEntryMerge(existing, entry)
	}

	// Now add entry to the main storage
	if err := m.AddOrUpdate(entry, func(t *storage.Entry) {
		m.RefreshEntries(true)
	}); err != nil {
		return
	}
	// Register a per-torrent cancellation context (Fix B). The qBit DELETE
	// handler cancels this ctx before unlinking files, so the grab worker
	// closes its file descriptor first and Linux doesn't preserve the inode
	// as a .fuse_hidden orphan. defer release() ensures the registry is
	// cleared on normal completion as well as panic.
	ctx, release := m.RegisterDownload(entry.InfoHash, m.ctx)
	defer release()
	err := m.downloader.download(ctx, entry)
	if err != nil {
		m.logger.Error().
			Err(err).
			Str("name", entry.Name).
			Msg("Error running post-download action")
		return
	}
}

// attachProvider copies the debrid-side provider, size and file metadata
// onto the entry. It is called from BOTH AddNewTorrent (before the entry is
// queued, so a JobQueue-held-then-restarted entry is restart-recoverable)
// and processNewTorrent (the normal processing path), so the two paths
// produce provably identical entry state rather than relying on incidental
// idempotence.
//
// Idempotent for the same debrid: AddTorrentProvider assigns
// Providers[debrid] by key (fresh ProviderEntry, no append), Size/Bytes are
// = assignments (not +=), and Files are keyed by name. Calling it twice on
// the same entry (the no-restart path) therefore does not double-count.
func (m *Manager) attachProvider(torrent *storage.Entry, debridTorrent *debridTypes.Torrent) {
	_ = torrent.AddTorrentProvider(debridTorrent)
	torrent.ActiveProvider = debridTorrent.Debrid
	torrent.Bytes = debridTorrent.GetSize()
	torrent.Size = debridTorrent.GetSize()
	torrent.Name = debridTorrent.Name
	torrent.OriginalFilename = debridTorrent.OriginalFilename
	torrent.UpdatedAt = time.Now()
	for _, file := range debridTorrent.Files {
		tFile := &storage.File{
			Name:      file.Name,
			Size:      file.Size,
			ByteRange: file.ByteRange,
			Deleted:   file.Deleted,
			InfoHash:  torrent.InfoHash,
			AddedOn:   torrent.AddedOn,
		}
		torrent.Files[file.Name] = tFile
	}
}

// processTorrent handles the complete torrent lifecycle
func (m *Manager) processNewTorrent(torrent *storage.Entry, debridTorrent *debridTypes.Torrent) {
	// Update status to submitting
	torrent.UpdatedAt = time.Now()
	_ = m.queue.Update(torrent)

	// AddOrUpdate placement. Shared with AddNewTorrent so the no-restart
	// path (AddNewTorrent attach then this attach) and the restart-recovery
	// path produce identical entry state. The block is idempotent for the
	// same debrid: AddTorrentProvider assigns Providers[debrid] by key (no
	// append), Size/Bytes are = assignments, and Files are keyed by name.
	m.attachProvider(torrent, debridTorrent)
	_ = m.queue.Update(torrent)

	if debridTorrent.Status != debridTypes.TorrentStatusDownloaded {
		m.logger.Info().
			Str("debrid", debridTorrent.Debrid).
			Str("name", debridTorrent.Name).
			Msg("Started downloading torrent")
		return
	}

	// Mark placement as downloaded
	if placement := torrent.GetActiveProvider(); placement != nil {
		now := time.Now()
		placement.DownloadedAt = &now
		placement.Progress = 1.0
	}

	// Parse post-download action
	m.processAction(torrent)
}

// SendToDebrid submits a magnet to debrid service(s) - replaces debrid.Parse
func (m *Manager) SendToDebrid(ctx context.Context, importRequest *ImportRequest) (*debridTypes.Torrent, error) {
	debridTorrent := &debridTypes.Torrent{
		InfoHash: importRequest.Magnet.InfoHash,
		Magnet:   importRequest.Magnet,
		Name:     importRequest.Magnet.Name,
		Arr:      importRequest.Arr,
		Size:     importRequest.Magnet.Size,
		Files:    make(map[string]debridTypes.File),
	}

	clients := m.FilterDebrid(func(c common.Client) bool {
		if importRequest.SelectedDebrid != "" && c.Config().Name != importRequest.SelectedDebrid {
			return false
		}
		return true
	})

	if len(clients) == 0 {
		return nil, fmt.Errorf("no debrid clients available")
	}

	errs := make([]error, 0, len(clients))

	for _, db := range clients {
		overrideDownloadUncached := false

		if importRequest.DownloadUncached != nil {
			overrideDownloadUncached = *importRequest.DownloadUncached
		} else {
			overrideDownloadUncached = db.Config().DownloadUncached
		}
		debridTorrent.DownloadUncached = overrideDownloadUncached
		_logger := db.Logger()
		_logger.Info().
			Str("Provider", db.Config().Name).
			Str("Arr", importRequest.Arr.Name).
			Str("Hash", debridTorrent.InfoHash).
			Str("Name", debridTorrent.Name).
			Str("Action", string(importRequest.Action)).
			Msg("Processing torrent")

		dbt, err := db.SubmitMagnet(debridTorrent)
		if err != nil || dbt == nil || dbt.Id == "" {
			errs = append(errs, err)
			continue
		}
		dbt.Arr = importRequest.Arr
		_logger.Info().Str("id", dbt.Id).Msgf("Entry: %s submitted to %s", dbt.Name, db.Config().Name)

		torrent, err := db.CheckStatus(dbt)
		if err != nil && torrent != nil && torrent.Id != "" {
			// Delete the torrent if it was not downloaded
			go func(id string) {
				_ = db.DeleteTorrent(id)
			}(torrent.Id)
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if torrent == nil {
			errs = append(errs, fmt.Errorf("torrent %s returned nil after checking status", dbt.Name))
			continue
		}
		return torrent, nil
	}
	if len(errs) == 0 {
		return nil, fmt.Errorf("failed to process torrent: no clients available")
	}
	joinedErrors := errors.Join(errs...)
	return nil, fmt.Errorf("failed to process torrent: %w", joinedErrors)
}
