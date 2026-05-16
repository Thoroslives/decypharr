package account

import (
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

type Account struct {
	Debrid      string                                 `json:"debrid"` // The debrid service name, e.g. "realdebrid"
	links       *xsync.Map[string, types.DownloadLink] // key is the sliced file link
	Index       int                                    `json:"index"` // The index of the account in the config
	Disabled    atomic.Bool                            `json:"disabled"`
	Token       string                                 `json:"token"`
	TrafficUsed atomic.Int64                           `json:"traffic_used"` // Traffic used in bytes
	Username    string                                 `json:"username"`     // Username for the account
	httpClient  *request.Client
	Expiration  time.Time `json:"expiration"`

	// Account reactivation tracking
	DisableCount atomic.Int32 `json:"disable_count"`

	// disabledAtNanos is the unix-nanos timestamp of the most recent
	// MarkDisabled, or 0 when the account has never been disabled / was
	// Reset. Stored as an atomic.Int64 (not a bare time.Time) so concurrent
	// link goroutines hitting MarkDisabled and usable() never see a torn or
	// zero read: a torn read would make the cooldown either never elapse
	// (permanent latch) or always elapse (near-full-speed re-probe loop),
	// silently un-fixing the account-disable latch. It pairs with the
	// Disabled atomic.Bool and uses the same lock-free idiom.
	disabledAtNanos atomic.Int64
}

func (a *Account) Equals(other *Account) bool {
	if other == nil {
		return false
	}
	return a.Token == other.Token && a.Debrid == other.Debrid
}

func (a *Account) Client() *request.Client {
	return a.httpClient
}

// slice download link
func (a *Account) sliceFileLink(fileLink string) string {
	if a.Debrid != "realdebrid" {
		return fileLink
	}
	if len(fileLink) < 39 {
		return fileLink
	}
	return fileLink[0:39]
}

func (a *Account) GetDownloadLink(id string, file *types.File, fetcher LinkFetcher) (types.DownloadLink, error) {
	slicedLink := a.sliceFileLink(file.Link)
	dl, ok := a.links.Load(slicedLink)
	if !ok {
		var err error
		dl, err = fetcher(a, id, file)
		if err != nil {
			return dl, err
		}
		a.storeLink(dl)
	}
	if err := dl.Valid(); err != nil {
		return types.DownloadLink{}, err
	}
	return dl, nil
}

func (a *Account) storeLink(dl types.DownloadLink) {
	slicedLink := a.sliceFileLink(dl.Link)
	a.links.Store(slicedLink, dl)
}
func (a *Account) DeleteLink(link types.DownloadLink, deleter LinkDeleter) error {
	slicedLink := a.sliceFileLink(link.Link)
	a.links.Delete(slicedLink)
	if deleter != nil {
		return deleter(a, link)
	}
	return nil
}
func (a *Account) ClearDownloadLinks() {
	a.links.Clear()
}
func (a *Account) DownloadLinksCount() int {
	return a.links.Size()
}

// GetRandomLink returns any cached download link for speed testing
// Returns empty link if no links are cached
func (a *Account) GetRandomLink() (types.DownloadLink, bool) {
	var result types.DownloadLink
	found := false
	a.links.Range(func(_ string, link types.DownloadLink) bool {
		if !link.Empty() {
			result = link
			found = true
			return false // stop iteration
		}
		return true
	})
	return result, found
}

func (a *Account) StoreDownloadLinks(dls map[string]*types.DownloadLink) {
	for _, dl := range dls {
		a.storeLink(*dl)
	}
}

// MarkDisabled marks the account as disabled and increments the disable
// count. now MUST come from the same clock usable() is later evaluated
// against (the Manager's injected clock): mixing a wall-clock stamp with a
// fake-clock usable() check makes the cooldown math incoherent.
//
// The disable timestamp (which drives the re-probe cooldown) is re-stamped
// CONDITIONALLY, not unconditionally. It is rewritten to now ONLY on:
//
//	(a) an enabled->disabled transition (the account was not Disabled), or
//	(b) a genuine post-cooldown re-probe that failed again: the account was
//	    already Disabled but the cooldown HAD fully elapsed (usable() true),
//	    so the backoff window legitimately restarts from now.
//
// It is PRESERVED on a spurious within-cooldown re-disable (already
// Disabled AND still inside the cooldown). This is the critical invariant:
// the Step-0 retryable-entry fix re-dispatches a stuck entry every
// refresh_interval (~15s); each re-dispatch can re-enter the disable path
// long before a 15-min cooldown elapses. Re-stamping unconditionally there
// would push disabledAtNanos forward ~15s every ~15s, so the cooldown would
// NEVER elapse and the time-based self-heal would never fire (the latch
// would persist, just slower). Preserving the original stamp lets the
// cooldown actually expire despite the intervening re-disables.
func (a *Account) MarkDisabled(now time.Time, cooldown time.Duration) {
	wasDisabled := a.Disabled.Load()
	cooldownElapsed := wasDisabled && a.usable(now, cooldown)
	if !wasDisabled || cooldownElapsed {
		a.disabledAtNanos.Store(now.UnixNano())
	}
	a.Disabled.Store(true)
	a.DisableCount.Add(1)
}

// Enable clears the disabled state and the cooldown timestamp so the account
// rejoins Active() immediately. Called on a successful link validation
// (implicit re-enable: the debrid has demonstrably recovered) so a
// self-healed account actually returns to healthy state instead of staying
// flagged forever as a tier-2 "cooled" re-probe candidate. Idempotent and
// cheap; a no-op when the account was not disabled.
func (a *Account) Enable() {
	if !a.Disabled.Load() {
		return
	}
	a.disabledAtNanos.Store(0)
	a.Disabled.Store(false)
}

// Reset is an inert manual/operator force-clear hook: it un-disables the
// account immediately, bypassing the cooldown. It is intentionally NOT wired
// to anything automatic; the cooldown-based re-probe in usable()/Manager is
// the auto-heal path. Kept (cheap, harmless) so an operator who KNOWS the
// debrid recovered can clear the latch without waiting out the cooldown.
func (a *Account) Reset() {
	a.DisableCount.Store(0)
	a.disabledAtNanos.Store(0)
	a.Disabled.Store(false)
}

// usable reports whether the account may be used for a (re-)probe at now.
// An account is usable if it is not disabled, OR it is disabled but the
// configured cooldown has fully elapsed since the last MarkDisabled (so a
// debrid whose limit window has likely reset gets re-probed instead of
// staying latched until a process restart). A non-positive cooldown or an
// unset/zero disable timestamp on a disabled account is treated as "still
// in cooldown" (not yet re-probable) so a mis-set cooldown fails closed
// rather than degrading into a full-speed retry loop.
func (a *Account) usable(now time.Time, cooldown time.Duration) bool {
	if !a.Disabled.Load() {
		return true
	}
	if cooldown <= 0 {
		return false
	}
	disabledAt := a.disabledAtNanos.Load()
	if disabledAt == 0 {
		return false
	}
	return now.UnixNano()-disabledAt >= cooldown.Nanoseconds()
}
