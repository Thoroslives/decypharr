package manager

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/testutil"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// rdRemoveFakeClient counts DeleteTorrent calls. It returns errFail for the
// first failN calls, then nil. An errFail of nil means always succeed.
type rdRemoveFakeClient struct {
	debrid.Client
	failN   int32 // number of initial failures (atomic read)
	calls   atomic.Int32
	errFail error
}

func (c *rdRemoveFakeClient) DeleteTorrent(_ string) error {
	n := c.calls.Add(1)
	if n <= atomic.LoadInt32(&c.failN) {
		return c.errFail
	}
	return nil
}

func (c *rdRemoveFakeClient) Config() config.Debrid  { return config.Debrid{Name: "realdebrid"} }
func (c *rdRemoveFakeClient) Logger() zerolog.Logger { return zerolog.Nop() }

const rdRemoveTestProvider = "realdebrid"

// newManagerForRDRemove wires a single fake client into a minimal Manager
// using the same xsync.Map injection seam as addnewtorrent_rd451_test.go.
// No storage needed: RemoveTorrentPlacements only reads t.Providers and calls
// RemoveFromProvider, which only needs m.clients.
func newManagerForRDRemove(t *testing.T, fake *rdRemoveFakeClient) *Manager {
	t.Helper()
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store(rdRemoveTestProvider, fake)
	return &Manager{
		clients:           clients,
		processingEntries: xsync.NewMap[string, time.Time](),
		downloadCancels:   xsync.NewMap[string, *downloadHandle](),
		logger:            zerolog.Nop(),
	}
}

// rdRemoveEntry builds a *storage.Entry with one provider placement pointing
// at rdRemoveTestProvider.
func rdRemoveEntry() *storage.Entry {
	return &storage.Entry{
		InfoHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Providers: map[string]*storage.ProviderEntry{
			rdRemoveTestProvider: {
				Provider: rdRemoveTestProvider,
				ID:       "RDID001",
			},
		},
	}
}

// TestRDRemoveBoundedRetrySucceeds asserts that when DeleteTorrent fails the
// first 2 times and succeeds on the 3rd attempt, RemoveTorrentPlacements
// calls it exactly 3 times and does not log a permanent-failure warning (the
// call counter is the observable proxy for success).
func TestRDRemoveBoundedRetrySucceeds(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())

	// Shrink backoff so the test does not take real seconds.
	orig := rdRemoveBackoffBase
	rdRemoveBackoffBase = 1 * time.Millisecond
	t.Cleanup(func() { rdRemoveBackoffBase = orig })

	fake := &rdRemoveFakeClient{
		failN:   2,
		errFail: errors.New("connection reset"),
	}
	m := newManagerForRDRemove(t, fake)

	m.RemoveTorrentPlacements(rdRemoveEntry())

	got := fake.calls.Load()
	if got != 3 {
		t.Errorf("DeleteTorrent call count: got %d, want 3 (fail x2 then succeed)", got)
	}
}

// TestRDRemoveBoundedOnPermanentFailure asserts that when DeleteTorrent always
// fails, RemoveTorrentPlacements stops after exactly maxRDRemoveAttempts calls
// and returns (no hang, no panic, no infinite loop).
func TestRDRemoveBoundedOnPermanentFailure(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())

	// Shrink backoff so the test does not take real seconds.
	orig := rdRemoveBackoffBase
	rdRemoveBackoffBase = 1 * time.Millisecond
	t.Cleanup(func() { rdRemoveBackoffBase = orig })

	fake := &rdRemoveFakeClient{
		failN:   1000, // always fail
		errFail: errors.New("RD connection refused"),
	}
	m := newManagerForRDRemove(t, fake)

	m.RemoveTorrentPlacements(rdRemoveEntry())

	got := fake.calls.Load()
	if got != maxRDRemoveAttempts {
		t.Errorf("DeleteTorrent call count: got %d, want %d (maxRDRemoveAttempts)", got, maxRDRemoveAttempts)
	}
}
