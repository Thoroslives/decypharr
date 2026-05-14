package torbox

import (
	"testing"

	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// TestGetTorboxStatusHandlesUnknownStatus guards G3: providers without a
// default clause fall off the end of the if/else-if chain on unexpected
// statuses (the bug PR #270 fixed in RealDebrid's CheckStatus loop).
// TorBox's getTorboxStatus helper must map any unknown status string to
// types.TorrentStatusError so the CheckStatus default branch handles it
// instead of looping forever.
//
// The finished=true short-circuit returns Downloaded regardless of status
// string; this is the authoritative completion signal from the TorBox API.
//
// See: /brain/05-Projects/2026-05-15-decypharr-fork-plan.md (Fix G3)
func TestGetTorboxStatusHandlesUnknownStatus(t *testing.T) {
	tb := &Torbox{}
	tests := []struct {
		name     string
		raw      string
		finished bool
		want     types.TorrentStatus
	}{
		{"finished_overrides_status", "anything", true, types.TorrentStatusDownloaded},
		{"finished_overrides_unknown", "unexpected_zzz_garbage", true, types.TorrentStatusDownloaded},
		{"downloading", "downloading", false, types.TorrentStatusDownloading},
		{"paused", "paused", false, types.TorrentStatusDownloading},
		{"metaDL", "metaDL", false, types.TorrentStatusDownloading},
		{"queuedDL", "queuedDL", false, types.TorrentStatusDownloading},
		{"checkingResumeData", "checkingResumeData", false, types.TorrentStatusDownloading},
		{"moving", "moving", false, types.TorrentStatusDownloading},
		{"completed", "completed", false, types.TorrentStatusDownloaded},
		{"cached", "cached", false, types.TorrentStatusDownloaded},
		{"uploading", "uploading", false, types.TorrentStatusDownloaded},
		{"downloaded", "downloaded", false, types.TorrentStatusDownloaded},
		{"unknown_garbage", "unexpected_zzz_garbage", false, types.TorrentStatusError},
		{"empty_string", "", false, types.TorrentStatusError},
		{"error_status", "error", false, types.TorrentStatusError},
		{"stalled", "stalledDL", false, types.TorrentStatusError},
		// TorBox strips parenthesised suffixes before matching.
		{"strip_parens_downloading", "downloading (45%)", false, types.TorrentStatusDownloading},
		{"strip_parens_completed", "completed (100%)", false, types.TorrentStatusDownloaded},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tb.getTorboxStatus(tc.raw, tc.finished)
			if got != tc.want {
				t.Errorf("getTorboxStatus(%q, %v) = %q, want %q", tc.raw, tc.finished, got, tc.want)
			}
		})
	}
}
