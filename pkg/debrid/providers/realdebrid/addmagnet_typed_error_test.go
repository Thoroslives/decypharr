package realdebrid

import (
	"errors"
	"net/http"
	"net/http/httptest"
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

// TestAddMagnet451ReturnsTypedInfringingError is the B4 seam unit assertion:
// an RD HTTP 451 (DMCA / infringing_file) on /torrents/addMagnet must surface
// as a typed *customerror.Error whose Code is the distinct infringing code,
// errors.As must succeed, it must NOT be retryable, and its Code must NOT be
// "too_many_active_downloads" (so it can never enter the ReQueue branch at
// processor.go AddNewTorrent). Pre-fix this returned a plain
// *errors.errorString ("realdebrid API error: Status: 451") and every
// assertion below fails.
func TestAddMagnet451ReturnsTypedInfringingError(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 451 Unavailable For Legal Reasons is exactly what RD returns for
		// DMCA'd / infringing content on this endpoint.
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
	if customErr.Code == "too_many_active_downloads" {
		t.Error("RD 451 must NOT carry the too_many_active_downloads Code (would route to ReQueue)")
	}
	// The DMCA content is permanently unsatisfiable on this provider:
	// retrying would latch forever. The new error must be non-retryable.
	if customErr.IsRetryable() {
		t.Error("RD 451 (DMCA) error must NOT be retryable")
	}
	if !customErr.IsPermanent() {
		t.Error("RD 451 (DMCA) error must be permanent")
	}
	// IsRetriableError is the function the download/circuit-breaker paths
	// actually consult; it honors the typed permanent/retry flags. It must
	// classify the DMCA error as non-retriable so nothing latches on it.
	// (customerror.IsPermanentError is intentionally NOT asserted: it is a
	// separate string-only heuristic that does not inspect the typed flag
	// and is not on the add path.)
	if customerror.IsRetriableError(err) {
		t.Error("RD 451 (DMCA) error must NOT classify as retriable (IsRetriableError)")
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
	if got := customErr.Error(); got == "" || !contains(got, "404") {
		t.Errorf("RD 404 error message must still convey the status, got %q", got)
	}
}

// TestAddMagnet509SentinelUnchanged is the adjacent-switch regression fence.
// The B4 change touches ONLY addMagnet's default arm; the case 509 arm
// (return nil, customerror.TooManyActiveDownloadsError) must be untouched.
//
// Note: a persistent HTTP 509 cannot reach addMagnet's switch through the
// shared request.Client, because retryablehttp.DefaultRetryPolicy classifies
// any >=500 status (509 included) as retry-then-give-up and surfaces its own
// "giving up" error instead of the 509 response. That is pre-existing
// realdebrid wrapper behaviour, outside B4's scope. The behavioural 509 ->
// TooManyActiveDownloadsError -> ReQueue path is fenced end-to-end at the
// manager layer (TestAddNewTorrent509StillReQueues). Here we fence the
// sentinel itself -- the exact value the 509 arm returns -- so an
// accidental edit to its Code/retryability (which WOULD silently stop RD
// slot-exhaustion being requeued) is caught.
func TestAddMagnet509SentinelUnchanged(t *testing.T) {
	var customErr *customerror.Error
	if !errors.As(error(customerror.TooManyActiveDownloadsError), &customErr) {
		t.Fatalf("TooManyActiveDownloadsError must be a *customerror.Error")
	}
	if customErr.Code != "too_many_active_downloads" {
		t.Errorf("509 sentinel Code: got %q want %q", customErr.Code, "too_many_active_downloads")
	}
	if !customErr.IsRetryable() {
		t.Error("509 sentinel (slot exhaustion) must remain retryable so it is requeued")
	}
	if customErr.IsPermanent() {
		t.Error("509 sentinel must NOT be permanent (it is the retryable ReQueue path)")
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
