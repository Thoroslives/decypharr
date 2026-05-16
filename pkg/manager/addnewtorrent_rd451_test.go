package manager

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/testutil"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/debrid/account"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// rd451FakeClient is a minimal debrid.Client whose SubmitMagnet returns
// exactly the error realdebrid.addMagnet now produces for the given status
// (the typed seam is asserted end-to-end against a real HTTP 451 in
// pkg/debrid/providers/realdebrid/addmagnet_typed_error_test.go). Here it
// isolates the AddNewTorrent / SendToDebrid CONTROL FLOW so the ReQueue
// invariant can be asserted without RD HTTP plumbing. Only SubmitMagnet and
// Config are exercised on the add-failure path; the rest satisfy the
// interface and must never be called by this path.
type rd451FakeClient struct {
	submitErr error
}

func (c *rd451FakeClient) SubmitMagnet(tr *types.Torrent) (*types.Torrent, error) {
	return nil, c.submitErr
}
func (c *rd451FakeClient) Config() config.Debrid  { return config.Debrid{Name: "realdebrid"} }
func (c *rd451FakeClient) Logger() zerolog.Logger { return zerolog.Nop() }

func (c *rd451FakeClient) CheckStatus(tr *types.Torrent) (*types.Torrent, error) {
	panic("CheckStatus must not be reached on the add-failure path")
}
func (c *rd451FakeClient) GetDownloadLink(string, *types.File) (types.DownloadLink, error) {
	panic("GetDownloadLink must not be reached on the add-failure path")
}
func (c *rd451FakeClient) DeleteTorrent(string) error { return nil }
func (c *rd451FakeClient) IsAvailable([]string) map[string]bool {
	return map[string]bool{}
}
func (c *rd451FakeClient) UpdateTorrent(*types.Torrent) error        { return nil }
func (c *rd451FakeClient) GetTorrent(string) (*types.Torrent, error) { return nil, nil }
func (c *rd451FakeClient) GetTorrents() ([]*types.Torrent, error)    { return nil, nil }
func (c *rd451FakeClient) RefreshDownloadLinks() error               { return nil }
func (c *rd451FakeClient) CheckFile(context.Context, string, string) error {
	return nil
}
func (c *rd451FakeClient) AccountManager() *account.Manager    { return nil }
func (c *rd451FakeClient) GetProfile() (*types.Profile, error) { return nil, nil }
func (c *rd451FakeClient) GetAvailableSlots() (int, error)     { return 0, nil }
func (c *rd451FakeClient) SyncAccounts()                       {}
func (c *rd451FakeClient) DeleteLink(types.DownloadLink) error { return nil }
func (c *rd451FakeClient) SpeedTest(context.Context) types.SpeedTestResult {
	return types.SpeedTestResult{}
}
func (c *rd451FakeClient) SupportsCheck() bool { return false }

// newManagerWithFakeRD wires the fake client into a Manager that has a real
// bbolt-backed *Queue. This is the full add-failure control-flow path:
// AddNewTorrent -> SendToDebrid -> client.SubmitMagnet -> submitErr. The
// real *Queue lets the test prove ReQueue was NOT entered.
func newManagerWithFakeRD(t *testing.T, submitErr error) (*Manager, *Queue) {
	t.Helper()
	q, strg := newQueueWithStorage(t, t.TempDir())
	t.Cleanup(func() { _ = strg.Close() })

	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("realdebrid", &rd451FakeClient{submitErr: submitErr})

	m := &Manager{
		clients:           clients,
		queue:             q,
		processingEntries: xsync.NewMap[string, time.Time](),
		downloadCancels:   xsync.NewMap[string, *downloadHandle](),
		logger:            zerolog.Nop(),
	}
	return m, q
}

func rd451ImportRequest(t *testing.T) *ImportRequest {
	t.Helper()
	infohash := "1111111111111111111111111111111111111111"
	magnet := &utils.Magnet{
		Name:     "dmca.release",
		InfoHash: infohash,
		Link:     "magnet:?xt=urn:btih:" + infohash,
	}
	a := arr.New("sonarr", "http://sonarr:8989", "token", false, false, nil, "", "manual")
	return NewTorrentRequest("realdebrid", t.TempDir(), magnet, a, config.DownloadActionSymlink, nil, "", ImportTypeQBit, false)
}

// TestAddNewTorrentRD451DoesNotReQueue is the B4 invariant centerpiece,
// asserted through the FULL AddNewTorrent path (not addMagnet in isolation).
//
// HARD invariant: an RD 451 (DMCA) add failure must behave byte-identically
// to the pre-fix flattened-string error -- AddNewTorrent returns an error
// (the qbit handler then turns it into a 400), and it must NOT be routed
// into the too_many_active_downloads -> ReQueue branch (processor.go:121).
// The only deltas B4 introduces are: the error is now a typed
// *customerror.Error, and it carries the infringing_file Code for logging.
//
// Pre-fix, addMagnet returned a plain error string, so the input here would
// have been that string. The "returns an error" / "not requeued" assertions
// were GREEN-by-construction even pre-fix (a plain-string error never
// matched Code=="too_many_active_downloads"); the assertion that is RED
// pre-fix and GREEN post-fix is the typed-error chain (errors.As + Code +
// non-retryable) reaching through SendToDebrid's errors.Join + %w wrapping.
// Together they prove the seam was added WITHOUT changing the add-failure
// control flow.
func TestAddNewTorrentRD451DoesNotReQueue(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())

	// The error the post-fix realdebrid.addMagnet returns on HTTP 451.
	m, q := newManagerWithFakeRD(t, customerror.InfringingFileError)

	err := m.AddNewTorrent(context.Background(), rd451ImportRequest(t))

	// 1. Control-flow invariant: AddNewTorrent must return an error on a
	//    451 (so the qbit handler returns HTTP 400, exactly as pre-fix).
	if err == nil {
		t.Fatal("AddNewTorrent on RD 451 must return an error (qbit handler -> 400), got nil")
	}

	// 2. Control-flow invariant: it must NOT have entered the
	//    too_many_active_downloads -> ReQueue branch. ReQueue appends to
	//    the in-memory queue slice (and persists a record); it must be empty.
	q.mu.RLock()
	queued := len(q.queue)
	q.mu.RUnlock()
	if queued != 0 {
		t.Errorf("RD 451 must NOT be requeued (would be an infinite DMCA retry latch); queue length = %d", queued)
	}

	// 3. Seam assertion (RED pre-fix): the surfaced error must be a typed
	//    *customerror.Error carrying the infringing Code, reachable via
	//    errors.As through the AddNewTorrent / SendToDebrid wrapping.
	var customErr *customerror.Error
	if !errors.As(err, &customErr) {
		t.Fatalf("RD 451 error must be a *customerror.Error through the full path (errors.As), got %T: %v", err, err)
	}
	if customErr.Code != "infringing_file" {
		t.Errorf("RD 451 Code through AddNewTorrent: got %q want %q", customErr.Code, "infringing_file")
	}

	// 4. The infringing error must be non-retryable and, critically, must
	//    NOT carry the too_many_active_downloads Code -- the single Code
	//    processor.go branches on into ReQueue.
	if customErr.Code == "too_many_active_downloads" {
		t.Error("RD 451 must NOT carry too_many_active_downloads Code (would route to ReQueue)")
	}
	if customErr.IsRetryable() {
		t.Error("RD 451 (DMCA) error must NOT be retryable (would latch forever)")
	}
	if !customErr.IsPermanent() {
		t.Error("RD 451 (DMCA) error must be permanent")
	}
}

// TestAddNewTorrentRD451ControlFlowMatchesPreFixString locks the
// byte-identical-control-flow claim independent of the new typed symbol. It
// feeds AddNewTorrent the EXACT plain error string the pre-fix
// realdebrid.addMagnet default arm produced for a 451
// ("realdebrid API error: Status: 451") and asserts the disposition
// (returns an error, NOT requeued) is identical to what the new typed error
// produces in TestAddNewTorrentRD451DoesNotReQueue. Same input as pre-fix,
// same outcome -> the seam changed the error TYPE, never the control flow.
func TestAddNewTorrentRD451ControlFlowMatchesPreFixString(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())

	preFixErr := errors.New("realdebrid API error: Status: 451")
	m, q := newManagerWithFakeRD(t, preFixErr)

	err := m.AddNewTorrent(context.Background(), rd451ImportRequest(t))
	if err == nil {
		t.Fatal("pre-fix-string 451 must return an error (qbit handler -> 400), got nil")
	}
	q.mu.RLock()
	queued := len(q.queue)
	q.mu.RUnlock()
	if queued != 0 {
		t.Errorf("pre-fix-string 451 must NOT be requeued; queue length = %d", queued)
	}
	// Sanity: the pre-fix path is NOT a *customerror.Error (this is exactly
	// the opaque-string regression B4 fixes); proving the disposition is the
	// same as the typed path despite the type difference.
	var customErr *customerror.Error
	if errors.As(err, &customErr) {
		t.Error("pre-fix-string path must remain a plain error here (control-flow baseline)")
	}
}

// TestAddNewTorrent509StillReQueues is the positive-control / adjacent fence
// for the invariant test: a 509 (slot exhaustion) MUST still flow into the
// too_many_active_downloads -> ReQueue branch unchanged. This proves the B4
// change did not collaterally break the one customerror branch that DOES
// affect add-path control flow, and that the invariant test's "not
// requeued" assertion is meaningful (the same harness DOES requeue on 509).
func TestAddNewTorrent509StillReQueues(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())

	m, q := newManagerWithFakeRD(t, customerror.TooManyActiveDownloadsError)

	err := m.AddNewTorrent(context.Background(), rd451ImportRequest(t))
	if err != nil {
		t.Fatalf("AddNewTorrent on RD 509 must ReQueue and return nil, got error: %v", err)
	}

	q.mu.RLock()
	queued := len(q.queue)
	q.mu.RUnlock()
	if queued != 1 {
		t.Errorf("RD 509 must be requeued (too_many_active_downloads branch); queue length = %d want 1", queued)
	}
}
