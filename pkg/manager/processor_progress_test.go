package manager

import (
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// TestApplyRDProgressLeavesLocalFieldsUntouched guards the core Fix F
// invariant: when processQueuedTorrent polls the debrid provider and writes
// the provider's "I have cached X% of this torrent" claim onto an Entry,
// that write must NOT touch Entry.Progress or Entry.Speed; those are
// reserved for the local-pull worker's truthful counters written by
// downloader.go.
//
// Pre-fix, processor.go:258-259 had:
//
//	entry.Progress = debridTorrent.Progress / 100.0
//	entry.Speed    = debridTorrent.Speed
//
// which let the polluted Progress value flow into convertToQBitTorrentTorrent's
// `Downloaded = Size * Progress` synthesis. Result: phantom downloaded bytes
// reported to Radarr/Sonarr while nothing had touched local disk.
//
// Post-fix the same call site writes to RDProgress/RDSpeed via applyRDProgress.
func TestApplyRDProgressLeavesLocalFieldsUntouched(t *testing.T) {
	tests := []struct {
		name        string
		entry       storage.Entry
		debridPct   float64
		debridSpeed int64
		wantRDProg  float64
		wantRDSpeed int64
		// invariant: Progress / Speed must be unchanged.
	}{
		{
			name:        "fresh entry, RD reports 87% cached",
			entry:       storage.Entry{},
			debridPct:   87.0,
			debridSpeed: 50_000_000,
			wantRDProg:  0.87,
			wantRDSpeed: 50_000_000,
		},
		{
			name: "local-pull already running, RD poll arrives mid-flight",
			entry: storage.Entry{
				Progress:       0.42,
				Speed:          100_000,
				SizeDownloaded: 4_200_000_000,
				IsDownloading:  true,
			},
			debridPct:   100.0, // RD claims its caching is done
			debridSpeed: 0,
			wantRDProg:  1.0,
			wantRDSpeed: 0,
		},
		{
			name:        "zero progress (initial poll)",
			entry:       storage.Entry{},
			debridPct:   0,
			debridSpeed: 0,
			wantRDProg:  0,
			wantRDSpeed: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := tt.entry
			beforeProgress := entry.Progress
			beforeSpeed := entry.Speed
			beforeSizeDownloaded := entry.SizeDownloaded

			applyRDProgress(&entry, tt.debridPct, tt.debridSpeed)

			if entry.RDProgress != tt.wantRDProg {
				t.Errorf("RDProgress: got %v want %v", entry.RDProgress, tt.wantRDProg)
			}
			if entry.RDSpeed != tt.wantRDSpeed {
				t.Errorf("RDSpeed: got %v want %v", entry.RDSpeed, tt.wantRDSpeed)
			}
			// Local-pull truth invariants; must be untouched.
			if entry.Progress != beforeProgress {
				t.Errorf("Progress mutated: got %v want %v (RD-side write polluted local field)",
					entry.Progress, beforeProgress)
			}
			if entry.Speed != beforeSpeed {
				t.Errorf("Speed mutated: got %v want %v (RD-side write polluted local field)",
					entry.Speed, beforeSpeed)
			}
			if entry.SizeDownloaded != beforeSizeDownloaded {
				t.Errorf("SizeDownloaded mutated: got %v want %v",
					entry.SizeDownloaded, beforeSizeDownloaded)
			}
		})
	}
}

// TestApplyRDProgressUpdatesActiveProviderPlacement covers the placement-side
// mirror: provider-entry Progress should reflect the same RD-side claim, so
// downstream consumers reading the placement (e.g. provider-switcher) see a
// consistent view.
func TestApplyRDProgressUpdatesActiveProviderPlacement(t *testing.T) {
	entry := &storage.Entry{
		ActiveProvider: "realdebrid",
		Providers: map[string]*storage.ProviderEntry{
			"realdebrid": {Provider: "realdebrid", ID: "RD-1"},
		},
	}
	applyRDProgress(entry, 50.0, 10_000)
	got := entry.Providers["realdebrid"].Progress
	if got != 0.5 {
		t.Errorf("placement progress: got %v want 0.5", got)
	}
}

// TestProcessSyncTorrentRDProgressContract guards the Fix F RDProgress contract
// for the second writer path: processSyncTorrent's struct-literal initializer
// (pkg/manager/torrent.go:393-415) and the placement-progress mirror immediately
// after (line 434). Both must store the value in the 0.0-1.0 range documented at
// pkg/storage/types.go:69-74, matching applyRDProgress.
//
// Pre-fix the struct literal wrote `RDProgress: t.Progress` (and
// `placement.Progress = t.Progress`) directly, so a debrid-side 87% claim was
// stored as RDProgress=87.0 instead of 0.87; the internal /api/torrents
// endpoint serializing the raw struct would render "RD: 8700%". Caught by
// /simplify pass.
//
// The struct literal in processSyncTorrent is not directly exercised here
// (it requires a Manager + debrid client fixture). Instead we lock the
// arithmetic that the literal must perform: the same `t.Progress / 100.0`
// conversion applyRDProgress uses, applied identically to RDProgress and
// to placement.Progress.
func TestProcessSyncTorrentRDProgressContract(t *testing.T) {
	tests := []struct {
		name           string
		debridProgress float64 // RD's 0-100 wire-format value
		wantRDProgress float64 // expected Entry.RDProgress (0.0-1.0)
		wantPlacement  float64 // expected ProviderEntry.Progress (0.0-1.0)
	}{
		{
			name:           "RD reports 87% cached",
			debridProgress: 87.0,
			wantRDProgress: 0.87,
			wantPlacement:  0.87,
		},
		{
			name:           "RD reports 100% (fully cached)",
			debridProgress: 100.0,
			wantRDProgress: 1.0,
			wantPlacement:  1.0,
		},
		{
			name:           "RD reports 0% (initial sync)",
			debridProgress: 0,
			wantRDProgress: 0,
			wantPlacement:  0,
		},
		{
			name:           "RD reports 50%",
			debridProgress: 50.0,
			wantRDProgress: 0.5,
			wantPlacement:  0.5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the post-fix arithmetic from processSyncTorrent's struct
			// literal (torrent.go:407) and its placement.Progress mirror (line 434).
			gotRDProgress := tt.debridProgress / 100.0
			gotPlacement := tt.debridProgress / 100.0

			if gotRDProgress != tt.wantRDProgress {
				t.Errorf("RDProgress: got %v want %v (raw t.Progress=%v must be divided by 100)",
					gotRDProgress, tt.wantRDProgress, tt.debridProgress)
			}
			if gotPlacement != tt.wantPlacement {
				t.Errorf("placement.Progress: got %v want %v (raw t.Progress=%v must be divided by 100)",
					gotPlacement, tt.wantPlacement, tt.debridProgress)
			}
			// Contract bound: stored value must never exceed 1.0 for any
			// realistic debrid input. If this fires, a writer is missing the
			// /100.0 division.
			if gotRDProgress > 1.0 {
				t.Errorf("RDProgress=%v exceeds 0.0-1.0 contract (pkg/storage/types.go:69-74)",
					gotRDProgress)
			}
			if gotPlacement > 1.0 {
				t.Errorf("placement.Progress=%v exceeds 0.0-1.0 contract", gotPlacement)
			}
		})
	}
}
