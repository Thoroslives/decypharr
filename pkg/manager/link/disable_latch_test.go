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
// The latch this guards against: validateLink returns an account error
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

// reprobeStubClient counts debrid round-trips (GetDownloadLink) so a test
// can assert the pre-RD short-circuit actually avoids hitting the debrid
// while no account is usable.
type reprobeStubClient struct {
	debrid.Client
	am     *account.Manager
	link   string
	rtHits atomic.Int64 // debrid GetDownloadLink calls (round-trips)
}

func (s *reprobeStubClient) GetDownloadLink(torrentID string, file *types.File) (types.DownloadLink, error) {
	s.rtHits.Add(1)
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

func (s *reprobeStubClient) DeleteLink(dl types.DownloadLink) error { return nil }
func (s *reprobeStubClient) AccountManager() *account.Manager       { return s.am }

// Defect #1 + #2 end-to-end production-interaction regression. Single RD
// account. Models the real lifecycle the isolated account tests cannot:
// initial cap -> degenerate disable -> within-cooldown re-dispatches (must
// NOT hammer RD and must NOT keep pushing the cooldown out) -> after the
// cooldown a re-probe SUCCEEDS -> account re-enabled and the link resolves.
//
// On 6d9696e this never recovers: every within-cooldown re-dispatch hits RD,
// re-disables and re-stamps disabledAtNanos, so the cooldown never elapses
// (Defect #1); and even if it did, the validated account would stay Disabled
// forever (Defect #2). With the fixes: the pre-RD short-circuit returns the
// transient error without a round-trip while no account is usable, the
// original stamp is preserved so the cooldown actually elapses, the
// post-cooldown re-probe hits RD, validates, and re-enables the account.
func TestSingleAccountSelfHealsAfterCooldown(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())

	clk := testutil.NewFakeClock(time.Now())
	const cooldown = 15 * time.Minute

	// RD is capped until the test flips it (modelling the limit window
	// resetting after the cooldown). Capped -> 403 + X-Error; recovered ->
	// 200 OK.
	var recovered atomic.Bool
	var headHits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headHits.Add(1)
		if recovered.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("X-Error", "bytes_limit_reached")
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	dc := config.Debrid{Provider: "realdebrid", Name: "realdebrid", DownloadAPIKeys: []string{"tok-a"}}
	am := account.NewManager(dc, ratelimit.NewUnlimited(), zerolog.Nop())
	am.SetClock(clk)
	am.SetReprobeCooldown(cooldown)

	clients := xsync.NewMap[string, debrid.Client]()
	stub := &reprobeStubClient{am: am, link: srv.URL}
	clients.Store("realdebrid", stub)

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
	ctx := context.Background()

	// 1) Initial GetLink: capped -> degenerate disable -> transient error.
	if _, err := svc.GetLink(ctx, entry, filename); err == nil {
		t.Fatal("initial GetLink on a capped single account must return a transient error")
	}
	acc, _ := am.GetAccount("tok-a")
	if acc == nil || !acc.Disabled.Load() {
		t.Fatal("the single account must be Disabled after the initial cap")
	}
	rtAfterInitial := stub.rtHits.Load()
	if rtAfterInitial == 0 {
		t.Fatal("the initial attempt should have made at least one debrid round-trip (it had to discover the cap)")
	}

	// 2) Within-cooldown re-dispatches (the Step-0 re-dispatch loop). The
	// first iterations (still inside the cooldown) must fail fast via the
	// pre-RD short-circuit: NO new debrid round-trip. Crucially, the
	// re-dispatches must NOT keep pushing the cooldown out: once total
	// elapsed >= cooldown the account must become a usable re-probe
	// candidate (observable via HasUsableAccount, the exported predicate the
	// short-circuit uses). Pre-fix every iteration hits RD and re-stamps, so
	// HasUsableAccount stays false forever and the round-trip count climbs.
	const step = 15 * time.Second
	iterations := int(2 * cooldown / step)
	reprobeCount := 0
	for i := 0; i < iterations; i++ {
		clk.Advance(step)
		usableNow := am.HasUsableAccount()
		rtBefore := stub.rtHits.Load()
		_, err := svc.GetLink(ctx, entry, filename)
		rtDelta := stub.rtHits.Load() - rtBefore
		// RD is still capped throughout this phase, so every GetLink errors.
		if err == nil {
			t.Fatalf("GetLink #%d must error (RD still capped)", i)
		}
		if usableNow {
			// Past the (preserved) cooldown: ONE re-probe round-trip is
			// allowed (proves the self-heal gate opened despite the
			// intervening re-disables). Still capped -> re-disable ->
			// condition (b) restarts the backoff for the next window.
			reprobeCount++
			if rtDelta != 1 {
				t.Fatalf("post-cooldown re-probe #%d should make exactly ONE debrid round-trip, made %d", i, rtDelta)
			}
		} else {
			// Inside a cooldown window: the pre-RD short-circuit must avoid
			// the round-trip entirely (no RD hammering every ~15s). Pre-fix
			// HasUsableAccount is always true so this branch is never taken
			// and RD is hit every iteration.
			if rtDelta != 0 {
				t.Fatalf("within-cooldown re-dispatch #%d hit the debrid (delta=%d); the pre-RD short-circuit must avoid the round-trip while no account is usable (otherwise RD is hammered every ~15s and the cooldown never elapses)", i, rtDelta)
			}
		}
	}
	// The self-heal must have fired PERIODICALLY across 2x the cooldown:
	// roughly once per cooldown window (~2 times here), never zero (latch)
	// and never every tick (tight loop).
	if reprobeCount == 0 {
		t.Fatal("the account NEVER became a usable re-probe candidate across 2x the cooldown of within-cooldown re-dispatches; the disable timestamp is pushed forward every re-dispatch so the cooldown never elapses (R7 latch intact, restart-only)")
	}
	if reprobeCount > iterations/4 {
		t.Fatalf("the account re-probed %d times in %d iterations (~every tick); condition (b) over-fired into a tight loop instead of a periodic ~1/cooldown re-probe", reprobeCount, iterations)
	}

	// 3) RD's limit window resets. The loop ended with the account in a
	// fresh (condition (b)) cooldown window from its last failed re-probe,
	// so advance past one more full cooldown: the next GetLink is then past
	// the cooldown, HasUsableAccount() is true, the re-probe hits RD,
	// validates, and the account is implicitly re-enabled (Defect #2).
	recovered.Store(true)
	clk.Advance(cooldown + step)
	if !am.HasUsableAccount() {
		t.Fatal("precondition: after a full cooldown past the last failed re-probe the account must be a usable re-probe candidate again")
	}
	dl, err := svc.GetLink(ctx, entry, filename)
	if err != nil {
		t.Fatalf("after the cooldown elapsed and RD recovered, GetLink must succeed (self-heal), got error: %v", err)
	}
	if dl.DownloadLink != srv.URL {
		t.Fatalf("expected the resolved link %q, got %q", srv.URL, dl.DownloadLink)
	}
	if acc.Disabled.Load() {
		t.Fatal("after a successful post-cooldown validation the account must be implicitly re-enabled (Disabled=false); it is still flagged, so it never returns to healthy state")
	}
	// Fully healthy: rejoined Active() and selected via the fast/active path
	// (not a lingering cooled fallback). Active()/Current() are the exported
	// observable proxies for "disable timestamp cleared".
	active := am.Active()
	if len(active) != 1 || active[0].Token != acc.Token {
		t.Fatalf("the re-enabled account must rejoin Active() (fully healthy, not a lingering cooled candidate); got %d active", len(active))
	}
	if cur := am.Current(); cur == nil || cur.Token != acc.Token || cur.Disabled.Load() {
		t.Fatal("Current() must return the re-enabled account via the active path after self-heal")
	}
	if savedBad.Load() || entry.Bad {
		t.Fatal("the entry must never have been marked Bad across the whole self-heal lifecycle")
	}
}
