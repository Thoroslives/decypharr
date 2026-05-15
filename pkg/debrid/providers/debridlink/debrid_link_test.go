package debridlink

import (
	"strconv"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// TestGetDebridLinkStatusBinaryMapping guards G3: providers without a default
// clause fall off the end of the if/else-if chain on unexpected statuses
// (the bug PR #270 fixed in RealDebrid's CheckStatus loop). DebridLink's
// status mapping is binary (100 = downloaded, everything else = downloading)
// and therefore exhaustive — there's no fall-through hole. This test locks
// the contract in so future refactors don't accidentally introduce a third
// path that doesn't terminate.
func TestGetDebridLinkStatusBinaryMapping(t *testing.T) {
	tests := []struct {
		statusCode int
		want       types.TorrentStatus
	}{
		{100, types.TorrentStatusDownloaded},
		{0, types.TorrentStatusDownloading},
		{1, types.TorrentStatusDownloading},
		{6, types.TorrentStatusDownloading},
		{99, types.TorrentStatusDownloading},
		{101, types.TorrentStatusDownloading},
		{-1, types.TorrentStatusDownloading},
	}
	for _, tc := range tests {
		t.Run(strconv.Itoa(tc.statusCode), func(t *testing.T) {
			got := getDebridLinkStatus(tc.statusCode)
			if got != tc.want {
				t.Errorf("getDebridLinkStatus(%d) = %q, want %q", tc.statusCode, got, tc.want)
			}
		})
	}
}
