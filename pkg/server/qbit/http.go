package qbit

import (
	"errors"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// addFailureReason extracts a precise, distinct reason from an add-failure
// error for logging. It is observability-only: it changes the debug log
// line, never the HTTP response or control flow (the handler still returns
// 400 for every add failure). A typed *customerror.Error surfaces as its
// Code (e.g. "infringing_file" for an RD 451/DMCA rejection); anything else
// stays "unknown" so the opaque-string case is still visible verbatim
// alongside it.
func addFailureReason(err error) string {
	var customErr *customerror.Error
	if errors.As(err, &customErr) && customErr.Code != "" {
		return customErr.Code
	}
	return "unknown"
}

func (q *QBit) handleLogin(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	cfg := config.Get()
	username := r.FormValue("username")
	password := r.FormValue("password")
	a, err := q.authenticate(getCategory(ctx), username, password)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	if cfg.UseAuth {
		cookie := &http.Cookie{
			Name:     "SID",
			Value:    createSID(a.Host, a.Token),
			Path:     "/",
			SameSite: http.SameSiteNoneMode,
		}
		http.SetCookie(w, cookie)
	}
	_, _ = w.Write([]byte("Ok."))
}

func (q *QBit) handleVersion(w http.ResponseWriter, r *http.Request) {
	_, _ = w.Write([]byte("v4.3.2"))
}

func (q *QBit) handleWebAPIVersion(w http.ResponseWriter, r *http.Request) {
	_, _ = w.Write([]byte("2.7"))
}

func (q *QBit) handlePreferences(w http.ResponseWriter, r *http.Request) {
	preferences := getAppPreferences()

	preferences.SavePath = q.downloadFolder
	preferences.TempPath = filepath.Join(q.downloadFolder, "temp")

	utils.JSONResponse(w, preferences, http.StatusOK)
}

func (q *QBit) handleBuildInfo(w http.ResponseWriter, r *http.Request) {
	res := BuildInfo{
		Bitness:    64,
		Boost:      "1.75.0",
		Libtorrent: "1.2.11.0",
		Openssl:    "1.1.1i",
		Qt:         "5.15.2",
		Zlib:       "1.2.11",
	}
	utils.JSONResponse(w, res, http.StatusOK)
}

func (q *QBit) handleShutdown(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (q *QBit) handleTorrentsInfo(w http.ResponseWriter, r *http.Request) {
	//log all url params
	ctx := r.Context()
	category := getCategory(ctx)
	state := strings.Trim(r.URL.Query().Get("filter"), "")
	hashes := getHashes(ctx)

	// Pass ProtocolAll to match the internal /api/torrents handler's membership
	// view. Pre-fix the qBit-compat endpoint filtered to torrents-only, which
	// produced asymmetric "93 here vs N there" counts across the two APIs
	// (Radarr was treated to a different list than the Decypharr dashboard).
	// arrs configuring decypharr as a qBittorrent backend still expect all
	// "their" downloads visible, regardless of protocol.
	torrents := q.manager.Queue().ListFilter(category, config.ProtocolAll, storage.TorrentState(state), hashes, "added_on", false)
	// Snapshot the JobQueue's pending set once (one lock acquisition) instead
	// of probing per entry: this handler returns the full tracked library and
	// is polled every few seconds, so a per-entry lock would contend O(N) with
	// the workers that dispatch downloads. An entry whose job is still pending
	// (waiting for a free worker slot) is reported queuedDL, not stalledDL, so
	// Radarr treats it as queued rather than stalled.
	pending := q.manager.PendingJobIDs()
	qbitTorrents := make([]Torrent, len(torrents))
	for i, t := range torrents {
		_, held := pending[t.InfoHash]
		qbitTorrents[i] = convertToQBitTorrentTorrent(t, held)
	}
	utils.JSONResponse(w, qbitTorrents, http.StatusOK)
}

func (q *QBit) handleTorrentsAdd(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Parse form based on content type
	contentType := r.Header.Get("Content-Type")
	if strings.Contains(contentType, "multipart/form-data") {
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			q.logger.Error().Err(err).Msgf("Error parsing multipart form")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	} else if strings.Contains(contentType, "application/x-www-form-urlencoded") {
		if err := r.ParseForm(); err != nil {
			q.logger.Error().Err(err).Msgf("Error parsing form")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	} else {
		http.Error(w, "Invalid content type", http.StatusBadRequest)
		return
	}

	cfg := config.Get()
	action := cfg.DefaultDownloadAction
	if strings.ToLower(r.FormValue("sequentialDownload")) == "true" {
		action = config.DownloadActionDownload
	}

	rmTrackerUrls := strings.ToLower(r.FormValue("firstLastPiecePrio")) == "true"

	// Check config setting - if always remove tracker URLs is enabled, force it to true
	if q.alwaysRemoveTrackerURLS {
		rmTrackerUrls = true
	}

	debridName := r.FormValue("debrid")
	category := r.FormValue("category")
	_arr := getArrFromContext(ctx)
	if _arr == nil {
		// Arr is not in context
		_arr = arr.New(category, "", "", false, false, nil, "", "")
	}
	atleastOne := false

	// Handle magnet URLs
	if urls := r.FormValue("urls"); urls != "" {
		var urlList []string
		for _, u := range strings.Split(urls, "\n") {
			urlList = append(urlList, strings.TrimSpace(u))
		}
		for _, url := range urlList {
			if err := q.addMagnet(ctx, url, _arr, debridName, action, cfg.Notifications.CallbackURL, rmTrackerUrls, cfg.SkipMultiSeason); err != nil {
				q.logger.Debug().
					Str("reason", addFailureReason(err)).
					Msgf("Error adding magnet: %s", err.Error())
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			atleastOne = true
		}
	}

	// Handle torrent files
	if r.MultipartForm != nil && r.MultipartForm.File != nil {
		if files := r.MultipartForm.File["torrents"]; len(files) > 0 {
			for _, fileHeader := range files {
				if err := q.addTorrent(ctx, fileHeader, _arr, debridName, action, cfg.Notifications.CallbackURL, rmTrackerUrls, cfg.SkipMultiSeason); err != nil {
					q.logger.Debug().Err(err).Msgf("Error adding torrent")
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				atleastOne = true
			}
		}
	}

	if !atleastOne {
		http.Error(w, "No valid URLs or torrents provided", http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (q *QBit) handleTorrentsDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	hashes := getHashes(ctx)

	if len(hashes) == 0 {
		http.Error(w, "No hashes provided", http.StatusBadRequest)
		return
	}
	for _, hash := range hashes {
		// Fix B: cancel any in-flight local-pull worker BEFORE unlinking
		// files. Without this, the worker keeps writing to a deleted FD and
		// Linux preserves the inode as .fuse_hidden, blocking the parent dir
		// from being removed and stranding GBs of data. CancelDownload is a
		// no-op for hashes with no registered worker, so it's safe to call
		// unconditionally.
		if err := q.manager.CancelAndWait(hash, manager.DeleteWorkerWaitTimeout); err != nil {
			q.logger.Warn().
				Err(err).
				Str("infohash", hash).
				Msg("download worker did not exit within timeout; unlinking anyway")
		}

		// B1/B2: route through the store-symmetric delete. A completed or
		// sync-adopted entry lives in the entries store, not the queue, so
		// the old queue-only Queue.Delete was a silent no-op that still
		// returned 200, left the local files orphaned, and let the sync
		// loop re-adopt the surviving RD entry forever. DeleteSymmetric
		// resolves the entry from whichever store holds it, writes the
		// tombstone FIRST (so a concurrent sync tick can't re-adopt in the
		// gap), removes the local files, then removes the store record(s).
		// The cleanup closure is RD-removal ONLY: file delete and store
		// delete are owned by DeleteSymmetric, so doing them here too would
		// be a double-delete. Fire-and-forget RemoveTorrentPlacements
		// because RD API calls take 1-2s and the HTTP response shouldn't
		// block on remote provider state; it iterates t.Providers, so an
		// entry with no providers is a no-op.
		err := q.manager.Storage().DeleteSymmetric(hash, func(e *storage.Entry) error {
			go q.manager.RemoveTorrentPlacements(e)
			return nil
		})
		if err != nil {
			// Not-found in BOTH stores is now a real 404 instead of a
			// silent 200 - the caller (Sonarr/Radarr) must know the delete
			// did not happen so it stops treating a no-op as success.
			if strings.Contains(err.Error(), "not found in queue or entries") {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (q *QBit) handleTorrentsPause(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	hashes := getHashes(ctx)
	for _, hash := range hashes {
		torrent, err := q.manager.Queue().GetTorrent(hash)
		if err != nil {
			continue
		}
		go q.PauseTorrent(torrent)
	}

	w.WriteHeader(http.StatusOK)
}

func (q *QBit) handleTorrentsResume(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	hashes := getHashes(ctx)
	for _, hash := range hashes {
		torrent, err := q.manager.Queue().GetTorrent(hash)
		if err != nil {
			continue
		}
		go q.ResumeTorrent(torrent)
	}

	w.WriteHeader(http.StatusOK)
}

func (q *QBit) handleTorrentRecheck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	hashes := getHashes(ctx)
	for _, hash := range hashes {
		torrent, err := q.manager.Queue().GetTorrent(hash)
		if err != nil {
			continue
		}
		go q.RefreshTorrent(torrent)
	}

	w.WriteHeader(http.StatusOK)
}

func (q *QBit) handleCategories(w http.ResponseWriter, r *http.Request) {
	var categories = map[string]TorrentCategory{}
	for _, cat := range q.categories {
		path := filepath.Join(q.downloadFolder, cat)
		categories[cat] = TorrentCategory{
			Name:     cat,
			SavePath: path,
		}
	}
	utils.JSONResponse(w, categories, http.StatusOK)
}

func (q *QBit) handleCreateCategory(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, "Failed to parse form data", http.StatusBadRequest)
		return
	}

	name := r.Form.Get("category")
	if name == "" {
		http.Error(w, "No name provided", http.StatusBadRequest)
		return
	}

	q.categories = append(q.categories, name)

	utils.JSONResponse(w, nil, http.StatusOK)
}

func (q *QBit) handleTorrentProperties(w http.ResponseWriter, r *http.Request) {
	hash := r.URL.Query().Get("hash")
	torrent, err := q.manager.Queue().GetTorrent(hash)
	if err != nil {
		http.Error(w, "Entry not found", http.StatusNotFound)
		return
	}

	properties := q.GetTorrentProperties(torrent)
	utils.JSONResponse(w, properties, http.StatusOK)
}

func (q *QBit) handleTorrentFiles(w http.ResponseWriter, r *http.Request) {
	hash := r.URL.Query().Get("hash")
	torrent, err := q.manager.Queue().GetTorrent(hash)
	if err != nil {
		http.Error(w, "Entry not found", http.StatusNotFound)
		return
	}
	utils.JSONResponse(w, getTorrentFiles(torrent), http.StatusOK)
}

func (q *QBit) handleSetCategory(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	category := getCategory(ctx)
	hashes := getHashes(ctx)
	var filterFunc func(t *storage.Entry) bool

	hashSet := make(map[string]bool)
	if len(hashes) > 0 {
		for _, h := range hashes {
			hashSet[h] = true
		}

	}

	updateFunc := func(t *storage.Entry) bool {
		if t.Category != category {
			t.Category = category
			return true
		}
		return false
	}

	if err := q.manager.Queue().UpdateWhere(filterFunc, updateFunc); err != nil {
		q.logger.Warn().Err(err).Msgf("Error adding torrent")
		http.Error(w, "Failed to update torrents", http.StatusInternalServerError)
		return
	}
	utils.JSONResponse(w, nil, http.StatusOK)
}

func (q *QBit) handleAddTorrentTags(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, "Failed to parse form data", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	hashes := getHashes(ctx)
	tags := strings.Split(r.FormValue("tags"), ",")
	for i, tag := range tags {
		tags[i] = strings.TrimSpace(tag)
	}
	torrents := q.manager.Queue().ListFilter("", config.ProtocolTorrent, "", hashes, "", false)
	for _, t := range torrents {
		q.setTorrentTags(t, tags)
	}
	utils.JSONResponse(w, nil, http.StatusOK)
}

func (q *QBit) handleRemoveTorrentTags(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, "Failed to parse form data", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	hashes := getHashes(ctx)
	tags := strings.Split(r.FormValue("tags"), ",")
	for i, tag := range tags {
		tags[i] = strings.TrimSpace(tag)
	}
	torrents := q.manager.Queue().ListFilter("", config.ProtocolTorrent, "", hashes, "", false)
	for _, torrent := range torrents {
		q.removeTorrentTags(torrent, tags)

	}
	utils.JSONResponse(w, nil, http.StatusOK)
}

func (q *QBit) handleGetTags(w http.ResponseWriter, r *http.Request) {
	utils.JSONResponse(w, q.Tags, http.StatusOK)
}

func (q *QBit) handleCreateTags(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, "Failed to parse form data", http.StatusBadRequest)
		return
	}
	tags := strings.Split(r.FormValue("tags"), ",")
	for i, tag := range tags {
		tags[i] = strings.TrimSpace(tag)
	}
	q.addTags(tags)
	utils.JSONResponse(w, nil, http.StatusOK)
}
