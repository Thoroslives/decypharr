package realdebrid

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/internal/testutil"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// magnetTorrent builds a magnet-backed (NOT .torrent-file-backed) Torrent so
// SubmitMagnet routes through addMagnet, the code path under test, rather
// than addTorrent. Magnet.File is left nil so IsTorrent() is false.
func magnetTorrent() *types.Torrent {
	return &types.Torrent{
		InfoHash: "0000000000000000000000000000000000000000",
		Magnet: &utils.Magnet{
			Name:     "infringing.release",
			InfoHash: "0000000000000000000000000000000000000000",
			Link:     "magnet:?xt=urn:btih:0000000000000000000000000000000000000000",
		},
	}
}

// TestAddMagnet451ReturnsTypedInfringingError: an RD HTTP 451
// (DMCA/infringing_file) on /torrents/addMagnet surfaces as a typed
// *customerror.Error with the distinct infringing Code, and is
// non-retryable so it can never latch on permanently-unsatisfiable
// content. Control-flow equivalence and the no-ReQueue invariant are
// proven end-to-end in pkg/manager (TestAddNewTorrentRD451*).
func TestAddMagnet451ReturnsTypedInfringingError(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnavailableForLegalReasons)
	}))
	defer srv.Close()

	r := &RealDebrid{client: request.New(request.WithMaxRetries(0)), Host: srv.URL}

	_, err := r.SubmitMagnet(magnetTorrent())
	if err == nil {
		t.Fatal("SubmitMagnet on RD 451 must return an error, got nil")
	}

	var customErr *customerror.Error
	if !errors.As(err, &customErr) {
		t.Fatalf("RD 451 error must be a *customerror.Error (errors.As), got %T: %v", err, err)
	}
	if customErr.Code != "infringing_file" {
		t.Errorf("RD 451 Code: got %q want %q", customErr.Code, "infringing_file")
	}
	if customErr.IsRetryable() {
		t.Error("RD 451 (DMCA) error must NOT be retryable (would latch on unsatisfiable content)")
	}
}

// TestAddMagnetOtherNon2xxReturnsTypedStatusError asserts a non-451 non-2xx
// (e.g. 404) also becomes a typed *customerror.Error that still conveys the
// status in its message, is non-retryable, and is NOT the
// too_many_active_downloads code. Pre-fix this was a plain error string.
func TestAddMagnetOtherNon2xxReturnsTypedStatusError(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	r := &RealDebrid{client: request.New(request.WithMaxRetries(0)), Host: srv.URL}

	_, err := r.SubmitMagnet(magnetTorrent())
	if err == nil {
		t.Fatal("SubmitMagnet on RD 404 must return an error, got nil")
	}

	var customErr *customerror.Error
	if !errors.As(err, &customErr) {
		t.Fatalf("RD 404 error must be a *customerror.Error (errors.As), got %T: %v", err, err)
	}
	if customErr.Code == "too_many_active_downloads" {
		t.Error("RD 404 must NOT carry the too_many_active_downloads Code")
	}
	if customErr.Code == "infringing_file" {
		t.Error("RD 404 must NOT be classified as infringing_file (that is 451-only)")
	}
	if customErr.IsRetryable() {
		t.Error("RD 404 rejection error must NOT be retryable")
	}
	// The status must still be conveyed in the message so logs/.Error()
	// remain as informative as the prior flattened string.
	if got := customErr.Error(); got == "" || !strings.Contains(got, "404") {
		t.Errorf("RD 404 error message must still convey the status, got %q", got)
	}
}

// (The 509 -> TooManyActiveDownloadsError -> ReQueue path is fenced
// end-to-end at the manager layer: pkg/manager TestAddNewTorrent509StillReQueues.
// B4 touches only addMagnet's default arm; the 509/OK cases are untouched.)
