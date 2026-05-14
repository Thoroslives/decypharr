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
			got := convertToQBitTorrentTorrent(&entry)

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
