package alldebrid

import (
	"strconv"
	"testing"

	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// TestGetAlldebridStatusHandlesUnknownStatus guards G3: providers without a
// default clause fall off the end of the if/else-if chain on unexpected
// statuses (the bug PR #270 fixed in RealDebrid's CheckStatus loop).
// AllDebrid's getAlldebridStatus helper must map any unknown statusCode to
// types.TorrentStatusError so the CheckStatus default branch handles it
// instead of looping forever.
//
// Per AllDebrid API: statusCode 0-3 = downloading variants,
// 4 = downloaded/ready, 5+ = errors (deleted, virused, expired, etc).
func TestGetAlldebridStatusHandlesUnknownStatus(t *testing.T) {
	tests := []struct {
		statusCode int
		want       types.TorrentStatus
	}{
		{0, types.TorrentStatusDownloading},
		{1, types.TorrentStatusDownloading},
		{2, types.TorrentStatusDownloading},
		{3, types.TorrentStatusDownloading},
		{4, types.TorrentStatusDownloaded},
		{5, types.TorrentStatusError},
		{6, types.TorrentStatusError},
		{99, types.TorrentStatusError},
		{-1, types.TorrentStatusError},
	}
	for _, tc := range tests {
		t.Run(strconv.Itoa(tc.statusCode), func(t *testing.T) {
			got := getAlldebridStatus(tc.statusCode)
			if got != tc.want {
				t.Errorf("getAlldebridStatus(%d) = %q, want %q", tc.statusCode, got, tc.want)
			}
		})
	}
}
