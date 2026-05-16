package link

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/testutil"
	"github.com/sirrobot01/decypharr/pkg/debrid/account"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"go.uber.org/ratelimit"
)

// R7 invariant (a): a single disabled account hit by a persistent RD cap
// (bytes_limit_reached) must NOT make fetchAndValidate recurse unboundedly.
//
// Pre-fix chain (service.go:128-140): validateLink returns an account error
// -> disableLinkAccount marks the single account Disabled and wipes the
// validation cache -> the code recurses with the SAME attempt. The single
// account has no genuinely-active sibling, so Manager.Current()'s
// all-disabled fallback returns the just-disabled account: the "swap" is a
// no-op and the recursion is a tight loop bounded only by ctx cancellation.
// attempt never increments on this path, so the MaxReinsertionAttempt
// ceiling never trips. Only a process restart breaks it.
//
// Post-fix: the degenerate case (no active account distinct from the one
// just disabled) is detected; fetchAndValidate returns the transient link
// error WITHOUT recursing and WITHOUT reaching markEntryBad, so the entry
// stays retryable (State unchanged, Bad=false) and the cooldown self-heal
// (account package) gives it a real retry on a later tick.

// stubClient is a minimal debrid.Client. The embedded interface makes any
// unimplemented method panic if the fix accidentally routes through it;
// only GetDownloadLink and AccountManager are exercised by fetchAndValidate.
type stubClient struct {
	debrid.Client
	am   *account.Manager
	link string
}

func (s *stubClient) GetDownloadLink(torrentID string, file *types.File) (types.DownloadLink, error) {
	acc := s.am.Current()
	tok := ""
	if acc != nil {
		tok = acc.Token
	}
	return types.DownloadLink{
		Debrid:       "realdebrid",
		Token:        tok,
		Filename:     file.Name,
		Link:         file.Link,
		DownloadLink: s.link,
	}, nil
}

func (s *stubClient) DeleteLink(dl types.DownloadLink) error { return nil }

func (s *stubClient) AccountManager() *account.Manager { return s.am }

func newCappedEntry(infohash, filename string) *storage.Entry {
	return &storage.Entry{
		InfoHash:       infohash,
		Name:           "Some.Movie.2024.2160p",
		ActiveProvider: "realdebrid",
		State:          storage.EntryStateDownloading,
		Files: map[string]*storage.File{
			filename: {Name: filename, Size: 1 << 30},
		},
		Providers: map[string]*storage.ProviderEntry{
			"realdebrid": {
				Provider: "realdebrid",
				ID:       "RDTORRENTID",
				Files: map[string]*storage.ProviderFile{
					filename: {Id: "rdfile1", Link: "https://real-debrid.example/d/RESTRICTED1"},
				},
			},
		},
	}
}

func TestSingleAccountCapDoesNotRecurseUnbounded(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())

	// Every HEAD validation returns the RD cap code, so the account stays
	// capped on every (re-)probe within this call.
	var headHits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headHits.Add(1)
		w.Header().Set("X-Error", "bytes_limit_reached")
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	// Single RD account: the degenerate case (no active sibling to swap to).
	dc := config.Debrid{Provider: "realdebrid", Name: "realdebrid", DownloadAPIKeys: []string{"tok-a"}}
	am := account.NewManager(dc, ratelimit.NewUnlimited(), zerolog.Nop())

	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("realdebrid", &stubClient{am: am, link: srv.URL})

	var savedBad atomic.Bool
	svc := New(
		clients,
		func(string) (*storage.Entry, error) { return nil, nil },
		func(context.Context, *storage.Entry) error { return nil },
		func(e *storage.Entry) error {
			if e.Bad {
				savedBad.Store(true)
			}
			return nil
		},
		srv.Client(),
		0,
		zerolog.Nop(),
	)

	const filename = "movie.mkv"
	entry := newCappedEntry("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", filename)

	// Hard wall-clock bound: pre-fix this is a tight loop that only ends on
	// ctx cancellation, so without a timeout it runs until the test
	// deadline. The fix must return promptly after the first failed
	// validation + degenerate-disable detection.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan struct{})
	var gotLink types.DownloadLink
	var gotErr error
	go func() {
		gotLink, gotErr = svc.GetLink(ctx, entry, filename)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("GetLink did not return: the single-account cap path is an unbounded recursion (R7 latch). Only a process restart would break it in prod")
	}

	if gotErr == nil {
		t.Fatalf("expected a transient link error from the capped single account, got nil (link=%+v)", gotLink)
	}
	// Bounded: a single failed validation + a degenerate-disable detection.
	// Pre-fix headHits would be huge (loop until the 5s ctx timeout); the
	// fix must probe at most a small constant number of times.
	if hits := headHits.Load(); hits > 3 {
		t.Fatalf("validateLink HEAD was hit %d times for a single capped account; fetchAndValidate is recursing unboundedly (R7 tight loop). Expected <=3", hits)
	}
	// The entry must remain RETRYABLE: a transient account cap is not a
	// permanent failure. markEntryBad / EntryStateError here would recreate
	// the restart-class "dead until intervention" outcome (just quieter).
	if entry.Bad {
		t.Fatal("entry was marked Bad on a transient account-cap error; it must stay retryable so the cooldown self-heal can retry it")
	}
	if savedBad.Load() {
		t.Fatal("entry was persisted with Bad=true on a transient account-cap error (markEntryBad reached); it must stay retryable")
	}
	if entry.State == storage.EntryStateError {
		t.Fatalf("entry State was set to %q on a transient account-cap error; it must stay retryable (not Error)", entry.State)
	}
}
