package qbit

import (
	"math"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// TestGetTorrentPropertiesHonestProgress guards Fix F for /api/v2/torrents/properties.
//
// Pre-fix:
//   - DlSpeed and UpSpeed both returned t.Speed unconditionally (Speed could
//     reflect debrid-side ingestion rather than local pull).
//   - TotalDownloaded returned t.Bytes (mirrors debrid claim, not bytes on disk).
//   - TotalUploaded also returned t.Bytes (we never upload — was a copy-paste).
//
// Post-fix the same state machine as convertToQBitTorrentTorrent applies:
//
//	IsComplete=true                        -> 100% of Size, no live speed
//	State == EntryStateError               -> SizeDownloaded retained, no speed
//	IsDownloading=true                     -> truthful local-pull counters
//	otherwise (queued at RD, no worker)    -> zero across the board
//
// See: /brain/05-Projects/2026-05-15-decypharr-fork-spec.md (Fix F, revised).
func TestGetTorrentPropertiesHonestProgress(t *testing.T) {
	const size = int64(10_000_000_000) // 10 GB

	tests := []struct {
		name                string
		entry               storage.Entry
		wantDlSpeed         int64
		wantUpSpeed         int64
		wantTotalDownloaded int64
		wantTotalUploaded   int64
	}{
		{
			// RD-side ingestion in flight, no local-pull worker started.
			// Pre-fix: DlSpeed = RD's caching speed, TotalDownloaded = t.Bytes.
			// Post-fix: zero — nothing has touched local disk yet.
			name: "RD caching, no local-pull worker (anti-fabrication)",
			entry: storage.Entry{
				Size:          size,
				Bytes:         size, // pre-fix this leaked to TotalDownloaded
				RDProgress:    0.87,
				RDSpeed:       50_000_000,
				IsDownloading: false,
				IsComplete:    false,
			},
			wantDlSpeed:         0,
			wantUpSpeed:         0,
			wantTotalDownloaded: 0,
			wantTotalUploaded:   0,
		},
		{
			name: "active local-pull worker (truthful values)",
			entry: storage.Entry{
				Size:           size,
				Bytes:          size,
				Progress:       0.5,
				Speed:          100_000,
				SizeDownloaded: 5_000_000_000,
				IsDownloading:  true,
				State:          storage.EntryStateDownloading,
			},
			wantDlSpeed:         100_000,
			wantUpSpeed:         0,
			wantTotalDownloaded: 5_000_000_000,
			wantTotalUploaded:   0,
		},
		{
			name: "completed",
			entry: storage.Entry{
				Size:          size,
				Bytes:         size,
				IsDownloading: false,
				IsComplete:    true,
				State:         storage.EntryStatePausedUP,
			},
			wantDlSpeed:         0,
			wantUpSpeed:         0,
			wantTotalDownloaded: size,
			wantTotalUploaded:   0,
		},
		{
			name: "error mid-pull",
			entry: storage.Entry{
				Size:           size,
				Bytes:          size,
				SizeDownloaded: 1_000_000_000,
				IsDownloading:  false,
				State:          storage.EntryStateError,
			},
			wantDlSpeed:         0,
			wantUpSpeed:         0,
			wantTotalDownloaded: 1_000_000_000,
			wantTotalUploaded:   0,
		},
		{
			// NaN Progress must be sanitized; otherwise json.Marshal returns an
			// "unsupported value: NaN" error and the entire response payload
			// fails. Pre-fix the qBit properties handler never called Sanitize.
			name: "NaN progress sanitized",
			entry: storage.Entry{
				Size:          size,
				Progress:      math.NaN(),
				IsDownloading: false,
				IsComplete:    false,
			},
			wantDlSpeed:         0,
			wantUpSpeed:         0,
			wantTotalDownloaded: 0,
			wantTotalUploaded:   0,
		},
	}

	// GetTorrentProperties doesn't read any QBit state, so a zero-value
	// receiver suffices — no IsolateConfig needed for this pure-conversion
	// surface.
	q := &QBit{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := tt.entry
			if entry.AddedOn.IsZero() {
				entry.AddedOn = time.Now()
			}
			got := q.GetTorrentProperties(&entry)

			if got.DlSpeed != tt.wantDlSpeed {
				t.Errorf("DlSpeed: got %d want %d", got.DlSpeed, tt.wantDlSpeed)
			}
			if got.UpSpeed != tt.wantUpSpeed {
				t.Errorf("UpSpeed: got %d want %d", got.UpSpeed, tt.wantUpSpeed)
			}
			if got.TotalDownloaded != tt.wantTotalDownloaded {
				t.Errorf("TotalDownloaded: got %d want %d", got.TotalDownloaded, tt.wantTotalDownloaded)
			}
			if got.TotalUploaded != tt.wantTotalUploaded {
				t.Errorf("TotalUploaded: got %d want %d", got.TotalUploaded, tt.wantTotalUploaded)
			}
		})
	}
}
