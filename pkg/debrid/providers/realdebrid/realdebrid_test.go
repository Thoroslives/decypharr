package realdebrid

import (
	"testing"

	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// TestGetStatusHandlesUnknownStatus guards G3: providers without a default
// clause fall off the end of the if/else-if chain on unexpected statuses
// (the bug PR #270 fixed for the CheckStatus loop). RealDebrid's getStatus
// helper must map any raw status it doesn't explicitly handle to
// types.TorrentStatusError so the caller's default branch handles it
// instead of looping forever.
func TestGetStatusHandlesUnknownStatus(t *testing.T) {
	tests := []struct {
		raw  string
		want types.TorrentStatus
	}{
		{"downloaded", types.TorrentStatusDownloaded},
		{"downloading", types.TorrentStatusDownloading},
		{"magnet_conversion", types.TorrentStatusDownloading},
		{"queued", types.TorrentStatusDownloading},
		{"compressing", types.TorrentStatusDownloading},
		{"uploading", types.TorrentStatusDownloading},
		{"waiting_files_selection", types.TorrentStatusDownloading},
		{"unexpected_zzz_garbage", types.TorrentStatusError},
		{"error", types.TorrentStatusError},
		{"dead", types.TorrentStatusError},
		{"magnet_error", types.TorrentStatusError},
		{"", types.TorrentStatusError},
	}
	for _, tc := range tests {
		t.Run(tc.raw, func(t *testing.T) {
			got := getStatus(tc.raw)
			if got != tc.want {
				t.Errorf("getStatus(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}
