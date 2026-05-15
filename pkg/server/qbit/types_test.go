package qbit

import (
	"math"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// TestConvertToQBitTorrentTorrentHonestProgress guards Fix F (Honest-Progress
// Contract).
//
// Pre-fix, convertToQBitTorrentTorrent synthesized Downloaded as
// int64(float64(t.Size) * t.Progress) and reported Dlspeed as t.Speed
// unconditionally. The Entry.Progress / Entry.Speed fields were polluted by
// processQueuedTorrent with the upstream debrid provider's own ingestion
// progress (RD reporting how much of the torrent it had cached, not how many
// bytes had been pulled to local disk). The result was that Radarr/Sonarr saw
// "downloaded" bytes that did not exist on disk — a 1,800x discrepancy
// observed during the 2026-05-14/15 marathon debug session.
//
// Post-fix, RD-side ingestion progress lives in Entry.RDProgress /
// Entry.RDSpeed. The qBit-compat API reports Entry.SizeDownloaded directly
// when a local-pull worker is actively running (IsDownloading=true); zero
// otherwise. State is derived from lifecycle signals (IsComplete, State,
// IsDownloading), not from the polluted Progress fraction.
//
// See: /brain/05-Projects/2026-05-15-decypharr-fork-spec.md (Fix F, revised)
// See: /brain/01-Sessions/2026-05-15-decypharr-fork-design-brainstorm.md
//
//	"Fix F Discovery Spike (F.0) — RESPEC verdict"
func TestConvertToQBitTorrentTorrentHonestProgress(t *testing.T) {
	const size = int64(10_000_000_000) // 10 GB

	tests := []struct {
		name           string
		entry          storage.Entry
		wantProgress   float64
		wantDlspeed    int64
		wantDownloaded int64
		wantAmountLeft int64
		wantState      storage.TorrentState
	}{
		{
			// THE SMOKING-GUN CASE. RD reports 87% cached, no local-pull worker
			// active. Pre-fix: Downloaded = 8.7 GB, Progress = 0.87, State =
			// (whatever was last written). Post-fix: all zero, stalledDL.
			name: "RD caching, no local-pull worker (anti-fabrication)",
			entry: storage.Entry{
				Size:          size,
				RDProgress:    0.87,
				RDSpeed:       50_000_000,
				IsDownloading: false,
				IsComplete:    false,
				// State left zero-value — entry has not been marked anything yet.
			},
			wantProgress:   0,
			wantDlspeed:    0,
			wantDownloaded: 0,
			wantAmountLeft: size,
			wantState:      storage.TorrentState("stalledDL"),
		},
		{
			// Active local-pull. The truthful path. Progress/Speed/SizeDownloaded
			// are written by downloader.go's progressCallback.
			name: "active local-pull worker (truthful values)",
			entry: storage.Entry{
				Size:           size,
				Progress:       0.5,
				Speed:          100_000,
				SizeDownloaded: 5_000_000_000,
				IsDownloading:  true,
				State:          storage.EntryStateDownloading,
			},
			wantProgress:   0.5,
			wantDlspeed:    100_000,
			wantDownloaded: 5_000_000_000,
			wantAmountLeft: 5_000_000_000,
			wantState:      storage.EntryStateDownloading,
		},
		{
			// Completed. MarkAsCompleted sets State=pausedUP, IsComplete=true,
			// Progress=1.0. qBit-compat should report 100% with speed 0.
			name: "completed",
			entry: storage.Entry{
				Size:          size,
				Progress:      1.0,
				IsDownloading: false,
				IsComplete:    true,
				State:         storage.EntryStatePausedUP,
			},
			wantProgress:   1.0,
			wantDlspeed:    0,
			wantDownloaded: size,
			wantAmountLeft: 0,
			wantState:      storage.EntryStatePausedUP,
		},
		{
			// Errored mid-pull. MarkAsError sets State=error, IsDownloading=false.
			// SizeDownloaded retains whatever made it to disk before the error.
			name: "error mid-pull",
			entry: storage.Entry{
				Size:           size,
				SizeDownloaded: 1_000_000_000,
				IsDownloading:  false,
				State:          storage.EntryStateError,
			},
			wantProgress:   0,
			wantDlspeed:    0,
			wantDownloaded: 1_000_000_000,
			wantAmountLeft: size - 1_000_000_000,
			wantState:      storage.EntryStateError,
		},
		{
			// Sanitize parity: NaN Progress field (e.g. division-by-zero when
			// provider reports size=0 mid-flight) must not leak into JSON.
			// Pre-fix, the qBit handler skipped Sanitize and could emit NaN.
			name: "NaN progress is sanitized",
			entry: storage.Entry{
				Size:          size,
				Progress:      math.NaN(),
				IsDownloading: false,
				IsComplete:    false,
			},
			wantProgress:   0,
			wantDlspeed:    0,
			wantDownloaded: 0,
			wantAmountLeft: size,
			wantState:      storage.TorrentState("stalledDL"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := tt.entry
			got := convertToQBitTorrentTorrent(&entry, false)

			if got.Progress != tt.wantProgress {
				t.Errorf("Progress: got %v want %v", got.Progress, tt.wantProgress)
			}
			if math.IsNaN(got.Progress) || math.IsInf(got.Progress, 0) {
				t.Errorf("Progress is non-finite (%v) — Sanitize must zero NaN/Inf before encoding", got.Progress)
			}
			if got.Dlspeed != tt.wantDlspeed {
				t.Errorf("Dlspeed: got %d want %d", got.Dlspeed, tt.wantDlspeed)
			}
			if got.Downloaded != tt.wantDownloaded {
				t.Errorf("Downloaded: got %d want %d", got.Downloaded, tt.wantDownloaded)
			}
			if got.AmountLeft != tt.wantAmountLeft {
				t.Errorf("AmountLeft: got %d want %d", got.AmountLeft, tt.wantAmountLeft)
			}
			if got.State != tt.wantState {
				t.Errorf("State: got %q want %q", got.State, tt.wantState)
			}
		})
	}
}

// TestQBitState_HeldVsStalledVsActive guards Fix 2 (BIS-2).
//
// A submission waiting for a free JobQueue worker slot (over max_downloads)
// was reported as stalledDL, so Radarr badged it "stalled" and aggressive
// stalled-handling could remove and blocklist it. It must instead be reported
// as queuedDL so Radarr treats it as queued (no stalled-removal churn).
//
// The discriminator is JobQueue membership (the entry's job is still pending
// in the in-memory JobQueue, not yet picked up by a worker), threaded into
// the conversion via the `held` parameter. After Fix 1 a held entry also
// carries ActiveProvider, IsDownloading=false and Progress=0, which is
// indistinguishable by entry fields alone from a torrent genuinely stuck on
// a slow or dead RD-side download. Mapping the RD-stuck case to queuedDL
// would make Radarr never time out a dead grab (a silent permanent stall,
// strictly worse than today). So when held=false the default state MUST
// remain stalledDL; only a genuinely JobQueue-pending entry (held=true) is
// upgraded to queuedDL. The "genuine RD-stuck" row asserts that explicitly.
func TestQBitState_HeldVsStalledVsActive(t *testing.T) {
	const size = int64(10_000_000_000)

	tests := []struct {
		name      string
		entry     storage.Entry
		held      bool
		wantState storage.TorrentState
	}{
		{
			// Job still pending in the JobQueue, no worker yet. Post-Fix-1 the
			// entry carries ActiveProvider but has no local-pull progress.
			name: "held in JobQueue (pending job present)",
			entry: storage.Entry{
				Size:           size,
				ActiveProvider: "realdebrid",
				IsDownloading:  false,
				IsComplete:     false,
			},
			held:      true,
			wantState: queuedDL,
		},
		{
			// Active local-pull worker. IsDownloading wins regardless of held.
			name: "active worker, IsDownloading",
			entry: storage.Entry{
				Size:           size,
				ActiveProvider: "realdebrid",
				Progress:       0.5,
				Speed:          100_000,
				SizeDownloaded: 5_000_000_000,
				IsDownloading:  true,
				State:          storage.EntryStateDownloading,
			},
			held:      true,
			wantState: storage.EntryStateDownloading,
		},
		{
			// THE ANTI-REGRESSION ROW. Not in the JobQueue (held=false), no
			// worker, no local progress: a genuine RD-stuck grab. This MUST
			// stay stalledDL so Radarr can still time the dead grab out.
			// Indistinguishable from the held case by entry fields alone,
			// which is exactly why a pure entry-field heuristic is rejected.
			name: "genuine RD-stuck (not in JobQueue) stays stalledDL",
			entry: storage.Entry{
				Size:           size,
				ActiveProvider: "realdebrid",
				IsDownloading:  false,
				IsComplete:     false,
			},
			held:      false,
			wantState: stalledDL,
		},
		{
			// held must never override a terminal/paused state.
			name: "completed stays pausedUP even if held flag set",
			entry: storage.Entry{
				Size:       size,
				Progress:   1.0,
				IsComplete: true,
				State:      storage.EntryStatePausedUP,
			},
			held:      true,
			wantState: storage.EntryStatePausedUP,
		},
		{
			name: "error stays error even if held flag set",
			entry: storage.Entry{
				Size:           size,
				SizeDownloaded: 1_000_000_000,
				IsDownloading:  false,
				State:          storage.EntryStateError,
			},
			held:      true,
			wantState: storage.EntryStateError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := tt.entry
			got := convertToQBitTorrentTorrent(&entry, tt.held)
			if got.State != tt.wantState {
				t.Errorf("State: got %q want %q", got.State, tt.wantState)
			}
		})
	}
}
