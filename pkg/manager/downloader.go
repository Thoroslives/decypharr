package manager

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	grab "github.com/cavaliergopher/grab/v3"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/notifications"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sourcegraph/conc/pool"
)

type Downloader struct {
	manager      *Manager
	strmURL      string
	mountPath    string
	dest         string
	logger       zerolog.Logger
	maxDownloads int
}

const (
	symlinkMountWaitTimeout     = 30 * time.Minute
	symlinkScanInitialInterval  = 100 * time.Millisecond
	symlinkScanMaxInterval      = 2 * time.Second
	symlinkReadyTimeout         = 2 * time.Minute
	symlinkReadyInitialInterval = 200 * time.Millisecond
	symlinkReadyMaxInterval     = 2 * time.Second
	symlinkLogEveryAttempts     = 10
	symlinkLogSampleSize        = 8
)

type downloadLogMeta struct {
	requestHost     string
	finalHost       string
	requestRange    string
	contentRange    string
	responseProto   string
	contentEncoding string
	statusCode      int
	transferMode    string
	parts           int
}

// NewDownloadManager creates a new strm manager
func NewDownloadManager(manager *Manager) *Downloader {
	cfg := config.Get()
	strmURL := cfg.AppURL
	if strmURL == "" {
		bindAddress := cfg.BindAddress
		if bindAddress == "" {
			bindAddress = "localhost"
		}

		strmURL = fmt.Sprintf("http://%s:%s", bindAddress, cfg.Port)
	}
	return &Downloader{
		manager:      manager,
		strmURL:      strmURL,
		mountPath:    cfg.Mount.MountPath,
		logger:       manager.logger.With().Str("component", "downloader").Logger(),
		dest:         cfg.DownloadFolder,
		maxDownloads: cfg.MaxDownloads,
	}
}

// download is the per-torrent entry point. ctx is the per-torrent context
// derived in processAction (Fix B); cancelling it aborts any in-flight grab
// transfer and stops further file iteration.
func (d *Downloader) download(ctx context.Context, torrent *storage.Entry) error {
	// Mark as in-flight up front so the queue scheduler skips this entry while
	// we're iterating seasons / creating symlinks (processSymlink only flips
	// this flag after its own directory scan, which is too late for the parent
	// of a multi-season torrent).
	torrent.IsDownloading = true
	_ = d.manager.queue.Update(torrent)

	var (
		isMultiSeason bool
		seasons       []SeasonInfo
	)
	if !torrent.SkipMultiSeason {
		isMultiSeason, seasons = d.detectMultiSeason(torrent)
	}
	torrentMountPath := d.manager.GetTorrentMountPath(torrent)
	if isMultiSeason {
		seasonResults := convertToMultiSeason(torrent, seasons)
		for _, result := range seasonResults {
			if err := d.manager.queue.Add(result); err != nil {
				d.logger.Error().Err(err).Msgf("Failed to save season torrent")
				continue
			}
			if err := d.process(ctx, result, torrentMountPath); err != nil {
				d.markAsError(result, err)
			}
		}
		// Parent has been fanned out into season entries; mark it complete so
		// it leaves the downloading queue instead of getting re-processed.
		d.completeEntry(torrent)
		return nil
	}
	return d.process(ctx, torrent, torrentMountPath)
}

// process dispatches to per-action handlers. ctx flows through to the
// download paths so grab transfers honor per-torrent cancellation (Fix B).
func (d *Downloader) process(ctx context.Context, entry *storage.Entry, mountPath string) error {
	switch entry.Action {
	case config.DownloadActionDownload:
		return d.processDownload(ctx, entry)
	case config.DownloadActionSymlink:
		return d.processSymlink(entry, mountPath)
	case config.DownloadActionStrm:
		return d.processStrm(entry)
	case config.DownloadActionNone:
		d.completeEntry(entry)
		// Remove entry from queue
		_ = d.manager.queue.Delete(entry.InfoHash, nil)
		return nil
	default:
		return d.processSymlink(entry, mountPath)
	}
}

func (d *Downloader) completeEntry(entry *storage.Entry) {
	d.markAsCompleted(entry)
	d.notifyCompleted(entry)
	d.triggerArrRefresh(entry)
}

func (d *Downloader) markAsCompleted(entry *storage.Entry) {
	// Mark as completed
	entry.MarkAsCompleted(entry.DownloadPath())
	_ = d.manager.queue.Update(entry)
}

func (d *Downloader) notifyCompleted(entry *storage.Entry) {
	// Send notification
	msg := fmt.Sprintf("Download completed: %s [%s] -> %s", entry.Name, entry.Category, entry.DownloadPath())
	d.manager.Notifications.Notify(notifications.Event{
		Type:    config.EventDownloadComplete,
		Status:  "success",
		Entry:   entry,
		Message: msg,
	})
}

func (d *Downloader) triggerArrRefresh(entry *storage.Entry) {
	go func() {
		a := d.manager.arr.GetOrCreate(entry.Category)
		if a == nil || a.Host == "" || a.Token == "" {
			return
		}
		if err := a.Refresh(); err != nil {
			d.logger.Debug().
				Err(err).
				Str("arr", a.Name).
				Str("entry", entry.Name).
				Msg("Failed to trigger Arr refresh")
		}
	}()
}

func (d *Downloader) markAsError(entry *storage.Entry, err error) {
	d.logger.Error().Err(err).Str("name", entry.Name).Msg("Failed to process action")
	entry.MarkAsError(err)
	_ = d.manager.queue.Update(entry)

	// Send error notification
	msg := fmt.Sprintf("Download failed: %s [%s] - %s", entry.Name, entry.Category, err.Error())
	d.manager.Notifications.Notify(notifications.Event{
		Type:    config.EventDownloadFailed,
		Status:  "error",
		Entry:   entry,
		Message: msg,
		Error:   err,
	})
}

// processSymlink creates symlinks for torrent files
func (d *Downloader) processSymlink(entry *storage.Entry, mountPath string) error {
	files := entry.GetActiveFiles()
	torrentSymlinkPath := entry.DownloadPath()
	d.logger.Info().Str("mount_path", mountPath).Msgf("Creating symlinks for %d files in %s", len(files), torrentSymlinkPath)

	// Create symlink directory
	err := os.MkdirAll(torrentSymlinkPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %s: %v", torrentSymlinkPath, err)
	}

	filePaths, err := d.createSymlinksWhenMountFilesAppear(entry, files, mountPath, torrentSymlinkPath)
	if err != nil {
		return err
	}

	entry.IsDownloading = true
	_ = d.manager.queue.Update(entry)

	if err := d.waitForSymlinkFilesReady(filePaths, symlinkReadyTimeout); err != nil {
		return err
	}

	// Run ffprobe on files to warm cache and trigger imports
	if !d.manager.config.SkipPreCache && len(filePaths) > 0 {
		probeFiles := filePaths
		if len(probeFiles) > MaxNZBPreCacheFiles {
			probeFiles = probeFiles[:MaxNZBPreCacheFiles]
		}
		d.logger.Debug().Int("files", len(probeFiles)).Msgf("Running ffprobe on %s", entry.Name)
		if err := d.manager.RunFFprobe(probeFiles); err != nil {
			d.logger.Error().Msgf("Failed to run ffprobe: %s", err)
		} else {
			d.logger.Debug().Str("entry", entry.Name).Msgf("Ran ffprobe on %d/%d files", len(probeFiles), len(filePaths))
		}
	}

	d.completeEntry(entry)

	return nil
}

func (d *Downloader) createSymlinksWhenMountFilesAppear(entry *storage.Entry, files []*storage.File, mountPath string, symlinkDir string) ([]string, error) {
	remainingFiles := make(map[string]*storage.File, len(files))
	for _, file := range files {
		remainingFiles[file.Name] = file
	}

	filePaths := make([]string, 0, len(remainingFiles))
	deadline := time.Now().Add(symlinkMountWaitTimeout)
	delay := symlinkScanInitialInterval
	attempt := 0
	var lastScanErr error
	var scanErr error

	var checkDirectory func(string) error
	checkDirectory = func(dirPath string) error {
		entries, err := os.ReadDir(dirPath)
		if err != nil {
			if scanErr == nil {
				scanErr = err
			}
			return nil
		}

		for _, item := range entries {
			entryName := item.Name()
			fullPath := filepath.Join(dirPath, entryName)

			if file, exists := remainingFiles[entryName]; exists {
				fileSymlinkPath := filepath.Join(symlinkDir, file.Name)
				if err := os.Symlink(fullPath, fileSymlinkPath); err != nil && !os.IsExist(err) {
					return fmt.Errorf("failed to create symlink %s -> %s: %w", fileSymlinkPath, fullPath, err)
				}
				filePaths = append(filePaths, fileSymlinkPath)
				delete(remainingFiles, entryName)
				d.logger.Info().Msgf("File is ready: %s/%s", entry.GetFolder(), file.Name)
				continue
			}

			if item.IsDir() {
				if err := checkDirectory(fullPath); err != nil {
					return err
				}
			}
		}
		return nil
	}

	for len(remainingFiles) > 0 {
		attempt++
		scanErr = nil
		if err := checkDirectory(mountPath); err != nil {
			return nil, err
		}
		lastScanErr = scanErr
		if len(remainingFiles) == 0 {
			break
		}

		if time.Now().After(deadline) {
			pending := pendingMountFileNames(remainingFiles, symlinkLogSampleSize)
			if lastScanErr != nil {
				return nil, fmt.Errorf("timeout waiting for mount files: %d files still pending (%s): last scan error: %w", len(remainingFiles), strings.Join(pending, ", "), lastScanErr)
			}
			return nil, fmt.Errorf("timeout waiting for mount files: %d files still pending (%s)", len(remainingFiles), strings.Join(pending, ", "))
		}

		if shouldLogSymlinkWaitAttempt(attempt) {
			d.logger.Debug().
				Err(lastScanErr).
				Str("entry", entry.Name).
				Str("mount_path", mountPath).
				Int("pending", len(remainingFiles)).
				Strs("sample", pendingMountFileNames(remainingFiles, symlinkLogSampleSize)).
				Msg("Waiting for mount files before creating symlinks")
		}

		if err := d.sleepUntilNextSymlinkAttempt(delay, deadline); err != nil {
			return nil, err
		}
		delay = nextSymlinkBackoff(delay, symlinkScanMaxInterval)
	}

	return filePaths, nil
}

func (d *Downloader) waitForSymlinkFilesReady(filePaths []string, timeout time.Duration) error {
	if len(filePaths) == 0 {
		return nil
	}

	pending := make(map[string]error, len(filePaths))
	for _, path := range filePaths {
		pending[path] = nil
	}

	deadline := time.Now().Add(timeout)
	delay := symlinkReadyInitialInterval
	attempt := 0

	for len(pending) > 0 {
		attempt++
		for path := range pending {
			if err := verifySymlinkFileReady(path); err != nil {
				pending[path] = err
				continue
			}
			delete(pending, path)
		}
		if len(pending) == 0 {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for symlink files to be ready: %d files still pending (%s)", len(pending), strings.Join(pendingSymlinkFileStatuses(pending, symlinkLogSampleSize), ", "))
		}

		if shouldLogSymlinkWaitAttempt(attempt) {
			d.logger.Debug().
				Int("pending", len(pending)).
				Strs("sample", pendingSymlinkFileStatuses(pending, symlinkLogSampleSize)).
				Msg("Waiting for symlink files to resolve")
		}

		if err := d.sleepUntilNextSymlinkAttempt(delay, deadline); err != nil {
			return err
		}
		delay = nextSymlinkBackoff(delay, symlinkReadyMaxInterval)
	}

	return nil
}

func verifySymlinkFileReady(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("symlink not available: %w", err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		return fmt.Errorf("path is not a symlink")
	}

	targetInfo, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("symlink target not available: %w", err)
	}
	if targetInfo.IsDir() {
		return fmt.Errorf("symlink target is a directory")
	}

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("symlink target cannot be opened: %w", err)
	}
	return f.Close()
}

func (d *Downloader) sleepUntilNextSymlinkAttempt(delay time.Duration, deadline time.Time) error {
	if remaining := time.Until(deadline); remaining < delay {
		delay = remaining
	}
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	ctx := d.operationContext()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (d *Downloader) operationContext() context.Context {
	if d.manager != nil && d.manager.ctx != nil {
		return d.manager.ctx
	}
	return context.Background()
}

func nextSymlinkBackoff(current time.Duration, maxDelay time.Duration) time.Duration {
	current *= 2
	if current > maxDelay {
		return maxDelay
	}
	return current
}

func shouldLogSymlinkWaitAttempt(attempt int) bool {
	return attempt == 1 || attempt%symlinkLogEveryAttempts == 0
}

func pendingMountFileNames(files map[string]*storage.File, limit int) []string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	return limitedStringSample(names, limit)
}

func pendingSymlinkFileStatuses(files map[string]error, limit int) []string {
	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	statuses := make([]string, 0, len(paths))
	for _, path := range paths {
		err := files[path]
		status := path
		if err != nil {
			status = fmt.Sprintf("%s: %s", path, err.Error())
		}
		statuses = append(statuses, status)
	}
	return limitedStringSample(statuses, limit)
}

func limitedStringSample(values []string, limit int) []string {
	if limit <= 0 || len(values) <= limit {
		return values
	}

	sample := append([]string(nil), values[:limit]...)
	sample = append(sample, fmt.Sprintf("... %d more", len(values)-limit))
	return sample
}

// processDownload downloads all files for an entry with progress tracking.
// For torrents: uses HTTP download from debrid (ctx threaded through grab).
// For NZBs: uses parallel NNTP segment download (ctx threaded through usenet).
// Cancelling ctx aborts in-flight transfers and exits the goroutine pool
// promptly so the qBit DELETE handler can unlink files safely (Fix B).
func (d *Downloader) processDownload(ctx context.Context, entry *storage.Entry) error {
	// Check if this is a usenet entry
	if entry.IsNZB() {
		return d.processUsenetDownload(ctx, entry)
	}
	return d.processTorrentDownload(ctx, entry)
}

// processTorrentDownload downloads files from debrid via HTTP. ctx is the
// per-torrent context registered in processAction (Fix B); it is passed to
// linkService.GetLink and to each localDownloader invocation so DELETE can
// cancel in-flight transfers and prevent .fuse_hidden orphan inodes.
func (d *Downloader) processTorrentDownload(ctx context.Context, entry *storage.Entry) error {
	files := entry.GetActiveFiles()
	d.logger.Info().Msgf("Downloading %d files...", len(files))

	totalSize := int64(0)
	for _, file := range files {
		totalSize += file.Size
	}
	downloadedFolder := entry.DownloadPath()
	if err := os.MkdirAll(downloadedFolder, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create download directory: %s: %v", downloadedFolder, err)
	}
	entry.SizeDownloaded = 0
	entry.IsDownloading = true
	entry.Progress = 0

	var progressMu sync.Mutex
	progressCallback := func(downloaded int64, speed int64) {
		progressMu.Lock()
		defer progressMu.Unlock()

		entry.SizeDownloaded += downloaded
		entry.Speed = speed
		if totalSize > 0 {
			entry.Progress = float64(entry.SizeDownloaded) / float64(totalSize)
		}
		entry.UpdatedAt = time.Now()
		_ = d.manager.queue.Update(entry)
	}

	// Resolve download links before spawning goroutines. Use the per-torrent
	// ctx (Fix B) so the link resolution itself is also cancellable.
	type downloadTask struct {
		file *storage.File
		link string
	}
	var tasks []downloadTask
	for _, file := range files {
		downloadLink, err := d.manager.linkService.GetLink(ctx, entry, file.Name)
		if err != nil {
			d.logger.Error().Msgf("Failed to get download link for %s: %v", file.Name, err)
			continue
		}
		tasks = append(tasks, downloadTask{file: file, link: downloadLink.DownloadLink})
	}

	// If no valid download links were obtained, return error instead of panic
	if len(tasks) == 0 {
		return fmt.Errorf("no valid download links available for %s", entry.Name)
	}

	p := pool.New().WithErrors().WithFirstError()
	if d.maxDownloads > 0 {
		p = p.WithMaxGoroutines(d.maxDownloads)
	}
	for _, task := range tasks {
		p.Go(func() error {
			if err := d.localDownloader(
				ctx,
				task.link,
				filepath.Join(downloadedFolder, task.file.Name),
				task.file.ByteRange,
				progressCallback,
			); err != nil {
				d.logger.Error().Msgf("Failed to download %s: %v", task.file.Name, err)
				return err
			}
			d.logger.Info().Msgf("Downloaded %s", task.file.Name)
			return nil
		})
	}

	if err := p.Wait(); err != nil {
		return fmt.Errorf("download failed: %w", err)
	}

	// Size-fidelity reconcile (BIS-3 Fix 2). RD advertises STATIC JSON
	// metadata sizes never reconciled to delivered bytes; Radarr's
	// size-verified move then false-fails ("File move incomplete, data loss
	// may have occurred") and re-grabs. HEAD the resolved link of the LARGEST
	// file only (Concern 2: one probe per completion, not per file), set the
	// true CDN Content-Length, and persist to the queue store BEFORE
	// completeEntry so markAsCompleted's queue.Update flushes the corrected
	// size to the same store the qbit-compat /torrents/info reads (Concern 3).
	// Any link/HEAD failure (incl. RD bytes_limit_reached cap) is logged and
	// ignored: prior size kept, completion never blocked (Concern 2 / B0).
	if largest := largestActiveFile(entry); largest != nil {
		var probeLink string
		for _, t := range tasks {
			if t.file == largest {
				probeLink = t.link
				break
			}
		}
		if client, ok := d.manager.clients.Load(entry.ActiveProvider); ok {
			if prober, ok := client.(contentLengthProber); ok {
				_ = reconcileEntrySize(ctx, d.manager.queue, entry, prober, probeLink)
			}
		}
	}

	d.completeEntry(entry)
	d.logger.Info().Msgf("Downloaded all files for %s", entry.Name)
	return nil
}

// processUsenetDownload downloads NZB files via parallel NNTP segment
// fetching. ctx is the per-torrent context registered in processAction
// (Fix B); it replaces the previous d.manager.ctx usage so DELETE can
// cancel an in-flight NZB pull without taking down the whole process.
func (d *Downloader) processUsenetDownload(ctx context.Context, entry *storage.Entry) error {
	if d.manager.usenet == nil {
		return fmt.Errorf("usenet client not configured")
	}

	files := entry.GetActiveFiles()
	d.logger.Info().Msgf("Downloading %d NZB files via usenet...", len(files))

	downloadedFolder := entry.DownloadPath()
	if err := os.MkdirAll(downloadedFolder, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create download directory: %s: %v", downloadedFolder, err)
	}

	totalSize := int64(0)
	for _, file := range files {
		totalSize += file.Size
	}

	entry.SizeDownloaded = 0
	entry.Progress = 0
	entry.IsDownloading = true
	_ = d.manager.queue.Update(entry)

	var progressMu sync.Mutex
	// Track per-file progress so we can compute the global total across all files
	fileProgress := make(map[string]int64)

	p := pool.New().WithErrors().WithFirstError()
	if d.maxDownloads > 0 {
		p = p.WithMaxGoroutines(d.maxDownloads)
	}
	for _, file := range files {
		p.Go(func() error {
			destPath := filepath.Join(downloadedFolder, file.Name)
			destFile, err := os.Create(destPath)
			if err != nil {
				return fmt.Errorf("failed to create file %s: %w", file.Name, err)
			}
			defer destFile.Close()

			progressCallback := func(downloaded int64, speed int64) {
				progressMu.Lock()
				defer progressMu.Unlock()

				prev := fileProgress[file.Name]
				fileProgress[file.Name] = downloaded
				entry.SizeDownloaded += downloaded - prev
				entry.Speed = speed
				if totalSize > 0 {
					entry.Progress = float64(entry.SizeDownloaded) / float64(totalSize)
				}
				entry.UpdatedAt = time.Now()
				_ = d.manager.queue.Update(entry)
			}

			if err := d.manager.usenet.Download(ctx, entry.InfoHash, file.Name, destFile, progressCallback); err != nil {
				_ = removeEntryDir(destPath)
				return fmt.Errorf("failed to download %s: %w", file.Name, err)
			}

			d.logger.Info().Msgf("Downloaded NZB file: %s", file.Name)
			return nil
		})
	}

	err := p.Wait()

	if err != nil {
		entry.MarkAsError(err)
		_ = d.manager.queue.Update(entry)
		return fmt.Errorf("NZB download failed: %w", err)
	}

	d.completeEntry(entry)
	d.logger.Info().Msgf("Downloaded all NZB files for %s", entry.Name)
	return nil
}

// processStrm creates symlinks for torrent files
func (d *Downloader) processStrm(torrent *storage.Entry) error {
	files := torrent.GetActiveFiles()
	d.logger.Info().Msgf("Creating .strm for %d files ...", len(files))

	torrentSymlinkPath := torrent.DownloadPath()

	// Create symlink directory
	err := os.MkdirAll(torrentSymlinkPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %s: %v", torrentSymlinkPath, err)
	}

	for _, file := range files {
		strmFilePath := filepath.Join(torrentSymlinkPath, file.Name+".strm")
		streamURL, err := url.JoinPath(
			d.strmURL,
			"webdav",
			"stream",
			EntryAllFolder,
			url.PathEscape(torrent.GetFolder()),
			url.PathEscape(file.Name),
		)
		if err != nil {
			continue
		}
		if err := os.WriteFile(strmFilePath, []byte(streamURL), 0644); err != nil {
			return fmt.Errorf("failed to create .strm file: %s: %v", strmFilePath, err)
		}
	}
	d.completeEntry(torrent)
	d.logger.Info().Str("destination", torrentSymlinkPath).Msgf("Created .strm files for %s", torrent.Name)
	return nil
}

func (d *Downloader) detectMultiSeason(torrent *storage.Entry) (bool, []SeasonInfo) {
	torrentName := torrent.Name
	files := torrent.GetActiveFiles()

	// Find all seasons present in the files
	seasonsFound := findAllSeasons(files)

	// Check if this is actually a multi-season torrent
	isMultiSeason := len(seasonsFound) > 1 || hasMultiSeasonIndicators(torrentName)

	if !isMultiSeason {
		return false, nil
	}

	d.logger.Info().Msgf("Multi-season torrent detected with seasons: %v", getSortedSeasons(seasonsFound))

	// Group files by season
	seasonGroups := groupFilesBySeason(files, seasonsFound)

	// Create SeasonInfo objects with proper naming
	var seasons []SeasonInfo
	for seasonNum, seasonFiles := range seasonGroups {
		if len(seasonFiles) == 0 {
			continue
		}

		// Generate season-specific name preserving all metadata
		seasonName := replaceMultiSeasonPattern(torrentName, seasonNum)

		seasons = append(seasons, SeasonInfo{
			SeasonNumber: seasonNum,
			Files:        seasonFiles,
			InfoHash:     generateSeasonHash(torrent.InfoHash, seasonNum),
			Name:         seasonName,
		})
	}

	return true, seasons
}

// localDownloader downloads a file with grab so interrupted local downloads
// can resume cleanly. ctx is the per-torrent cancellation context (Fix B);
// grab v3.0.1 honors ctx via req.WithContext, so cancelling ctx aborts the
// transfer in flight, closes the file descriptor, and lets the caller unlink
// the destination directory without orphaning the inode.
//
// Pre-fix this used d.manager.ctx, which is the process-wide
// context.Background() set in manager.New (manager.go:114) and never
// cancelled mid-lifetime, so qBit DELETE had no way to abort the worker.
func (d *Downloader) localDownloader(ctx context.Context, downloadURL, filename string, byterange *[2]int64, progressCallback func(int64, int64)) error {
	startTime := time.Now()
	requestedRange := "full"
	req, err := grab.NewRequest(filename, downloadURL)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	req.BufferSize = 1 << 20
	req.HTTPRequest.Header.Set("User-Agent", "Decypharr[QBitTorrent]")
	req.HTTPRequest.Header.Set("Accept", "*/*")
	req.HTTPRequest.Header.Set("Accept-Encoding", "identity")

	if byterange != nil {
		requestedRange = fmt.Sprintf("bytes=%d-%d", byterange[0], byterange[1])
		req.NoResume = true
		req.HTTPRequest.Header.Set("Range", requestedRange)
	}

	client := grab.NewClient()
	client.BufferSize = 1 << 20
	client.HTTPClient = d.manager.streamClient

	resp := client.Do(req)
	if resp == nil {
		return fmt.Errorf("grab returned nil response for %s", downloadURL)
	}

	var lastReported int64
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	defer func() {
		var downloaded atomic.Int64
		downloaded.Store(resp.BytesComplete())
		meta := d.buildDownloadLogMeta(req.HTTPRequest, resp.HTTPResponse, requestedRange, "grab", 1)
		d.logDownloadCompletion(filename, startTime, &downloaded, meta)
	}()

	for {
		select {
		case <-t.C:
			current := resp.BytesComplete()
			speed := int64(resp.BytesPerSecond())
			if current != lastReported && progressCallback != nil {
				progressCallback(current-lastReported, speed)
				lastReported = current
			}
		case <-resp.Done:
			if progressCallback != nil {
				final := resp.BytesComplete()
				if final != lastReported {
					progressCallback(final-lastReported, int64(resp.BytesPerSecond()))
				}
			}
			if err := resp.Err(); err != nil {
				if grab.IsStatusCodeError(err) && resp.HTTPResponse != nil {
					return fmt.Errorf("unexpected status %d for %s", resp.HTTPResponse.StatusCode, downloadURL)
				}
				return err
			}
			return nil
		}
	}
}

func (d *Downloader) buildDownloadLogMeta(req *http.Request, resp *http.Response, requestedRange, transferMode string, parts int) downloadLogMeta {
	meta := downloadLogMeta{
		requestHost:     req.URL.Host,
		requestRange:    requestedRange,
		contentRange:    "none",
		contentEncoding: "identity",
		responseProto:   "unknown",
		statusCode:      0,
		transferMode:    transferMode,
		parts:           parts,
	}

	if resp == nil {
		return meta
	}

	if resp.Request != nil && resp.Request.URL != nil {
		meta.finalHost = resp.Request.URL.Host
	}
	meta.responseProto = resp.Proto
	if resp.TLS != nil && resp.TLS.NegotiatedProtocol != "" {
		meta.responseProto = fmt.Sprintf("%s (alpn=%s)", resp.Proto, resp.TLS.NegotiatedProtocol)
	}
	if contentRange := resp.Header.Get("Content-Range"); contentRange != "" {
		meta.contentRange = contentRange
	}
	if encoding := resp.Header.Get("Content-Encoding"); encoding != "" {
		meta.contentEncoding = encoding
	}
	meta.statusCode = resp.StatusCode
	return meta
}

func (d *Downloader) logDownloadCompletion(filename string, startTime time.Time, downloaded *atomic.Int64, meta downloadLogMeta) {
	bytesDownloaded := downloaded.Load()
	elapsed := time.Since(startTime)
	speedMBps := float64(0)
	if elapsed > 0 {
		speedMBps = float64(bytesDownloaded) / elapsed.Seconds() / (1024 * 1024)
	}

	d.logger.Info().
		Str("file", filepath.Base(filename)).
		Str("request_host", meta.requestHost).
		Str("final_host", meta.finalHost).
		Str("request_range", meta.requestRange).
		Str("content_range", meta.contentRange).
		Str("response_proto", meta.responseProto).
		Str("content_encoding", meta.contentEncoding).
		Str("transfer_mode", meta.transferMode).
		Int("parts", meta.parts).
		Int64("status", int64(meta.statusCode)).
		Int64("bytes", bytesDownloaded).
		Dur("duration", elapsed).
		Float64("speed_mbps", speedMBps).
		Msg("download transfer completed")
}

// removeEntryDir removes the on-disk path for an entry, including any stray
// files (partial downloads, hidden inodes). Uses RemoveAll for safety; returns
// nil if the path was already gone.
//
// Defensive: callers historically used os.Remove here which fails with
// "directory not empty" when stray files remain (e.g., .fuse_hidden* inodes
// from in-flight downloads). RemoveAll handles both the file and non-empty
// directory cases.
func removeEntryDir(p string) error {
	if err := os.RemoveAll(p); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete entry dir %s: %w", p, err)
	}
	return nil
}
