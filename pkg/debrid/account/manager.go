package account

import (
	"fmt"
	"net/http"
	"slices"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sourcegraph/conc/pool"
	"go.uber.org/ratelimit"
)

// Clock is a minimal time source so the disabled-account re-probe cooldown
// can be exercised with a fake clock in tests instead of sleeping. Production
// uses realClock; tests inject testutil.FakeClock via SetClock.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

type LinkFetcher func(account *Account, id string, file *types.File) (types.DownloadLink, error)
type LinkDeleter func(account *Account, dl types.DownloadLink) error
type LinksFetcher func(account *Account) ([]types.DownloadLink, error)
type SyncFunc func(account *Account) error

type Manager struct {
	debrid   string
	current  atomic.Pointer[Account]
	accounts *xsync.Map[string, *Account]
	logger   zerolog.Logger
	clock    Clock
	// reprobeCooldown gates how long a disabled account stays out of
	// selection before usable()/the slow path will re-probe it. Read-once
	// at construction from config; overridable in tests via
	// SetReprobeCooldown.
	reprobeCooldown time.Duration
}

// SetClock overrides the time source (tests only). Not safe for concurrent
// use; call once during setup before the manager is exercised.
func (m *Manager) SetClock(c Clock) {
	if c != nil {
		m.clock = c
	}
}

// SetReprobeCooldown overrides the disabled-account re-probe cooldown (tests
// only). Not safe for concurrent use; call once during setup.
func (m *Manager) SetReprobeCooldown(d time.Duration) {
	m.reprobeCooldown = d
}

func (m *Manager) now() time.Time {
	if m.clock != nil {
		return m.clock.Now()
	}
	return time.Now()
}

func NewManager(debridConf config.Debrid, downloadRL ratelimit.Limiter, logger zerolog.Logger) *Manager {
	m := &Manager{
		debrid:          debridConf.Name,
		accounts:        xsync.NewMap[string, *Account](),
		logger:          logger,
		clock:           realClock{},
		reprobeCooldown: config.DefaultAccountReprobeCooldown,
	}
	cfg := config.Get()
	if cfg.AccountReprobeCooldown != "" {
		if d, err := utils.ParseDuration(cfg.AccountReprobeCooldown); err == nil && d > 0 {
			m.reprobeCooldown = d
		}
	}

	var firstAccount *Account
	for idx, token := range debridConf.DownloadAPIKeys {
		if token == "" {
			continue
		}
		headers := map[string]string{
			"Authorization": fmt.Sprintf("Bearer %s", token),
		}

		// Create request client with equivalent options
		opts := []request.ClientOption{
			request.WithRateLimiter(downloadRL),
			request.WithHeaders(headers),
			request.WithMaxRetries(cfg.Retries),
			request.WithRetryableStatus(http.StatusTooManyRequests, http.StatusBadGateway, 447),
		}
		if debridConf.Proxy != "" {
			opts = append(opts, request.WithProxy(debridConf.Proxy))
		}

		account := &Account{
			Debrid:     debridConf.Name,
			Token:      token,
			Index:      idx,
			links:      xsync.NewMap[string, types.DownloadLink](),
			httpClient: request.New(opts...),
		}
		m.accounts.Store(token, account)
		if firstAccount == nil {
			firstAccount = account
		}
	}
	m.current.Store(firstAccount)
	return m
}

func (m *Manager) Active() []*Account {
	activeAccounts := make([]*Account, 0)
	m.accounts.Range(func(key string, acc *Account) bool {
		if !acc.Disabled.Load() {
			activeAccounts = append(activeAccounts, acc)
		}
		return true
	})

	slices.SortFunc(activeAccounts, func(i, j *Account) int {
		return i.Index - j.Index
	})
	return activeAccounts
}

func (m *Manager) All() []*Account {
	allAccounts := make([]*Account, 0)
	m.accounts.Range(func(key string, acc *Account) bool {
		allAccounts = append(allAccounts, acc)
		return true
	})

	slices.SortFunc(allAccounts, func(i, j *Account) int {
		return i.Index - j.Index
	})
	return allAccounts
}

// cooledDisabledAccounts returns the Index-sorted accounts that are Disabled
// but whose re-probe cooldown has elapsed (usable() true while
// Disabled.Load() is also true). These are re-probe CANDIDATES only: they
// are tried solely when there is no genuinely-active account, so a healthy
// account always outranks a cooled-but-likely-still-capped one (this is what
// keeps a multi-account pipeline from detouring through a dead account every
// cooldown). NOT a redefinition of Active() and NOT flattened with it.
func (m *Manager) cooledDisabledAccounts() []*Account {
	now := m.now()
	cooled := make([]*Account, 0)
	m.accounts.Range(func(key string, acc *Account) bool {
		if acc.Disabled.Load() && acc.usable(now, m.reprobeCooldown) {
			cooled = append(cooled, acc)
		}
		return true
	})

	slices.SortFunc(cooled, func(i, j *Account) int {
		return i.Index - j.Index
	})
	return cooled
}

// selectAccount applies the two-tier selection used by both Current()'s slow
// path and Disable()'s post-disable swap: a genuinely-active account FIRST;
// only if none, a cooled-disabled account as a re-probe candidate; only if
// none of those, the existing all-disabled fallback (returns nil if there
// are no accounts at all). Single-account: once the lone account cools, it
// is the re-probe candidate, so the latch self-heals. Multi-account: a
// healthy account always wins.
func (m *Manager) selectAccount() *Account {
	if active := m.Active(); len(active) > 0 {
		return active[0]
	}
	if cooled := m.cooledDisabledAccounts(); len(cooled) > 0 {
		m.logger.Warn().Str("debrid", m.debrid).Msg("No genuinely-active accounts; re-probing a disabled account whose cooldown has elapsed")
		return cooled[0]
	}
	m.logger.Warn().Str("debrid", m.debrid).Msg("No active accounts available, all accounts are disabled, falling back to disabled accounts")
	allAccounts := m.All()
	if len(allAccounts) == 0 {
		m.logger.Error().Str("debrid", m.debrid).Msg("Cannot set current account, no accounts available")
		return nil
	}
	return allAccounts[0]
}

func (m *Manager) Current() *Account {
	// Fast path - most common case. Reads Disabled.Load() raw (not usable()):
	// while the current account is disabled this intentionally falls through
	// to the slow path on EVERY call. The slow path's two-tier selection
	// (selectAccount) is the single authoritative gate for the re-probe
	// cooldown; the fast path is deliberately left untouched (minimal diff,
	// and the pipeline is stalled anyway while the only account is capped).
	current := m.current.Load()
	if current != nil && !current.Disabled.Load() {
		return current
	}

	// Slow path - active-first, cooled-disabled re-probe fallback, then the
	// existing all-disabled fallback.
	newCurrent := m.selectAccount()
	m.current.Store(newCurrent)
	return newCurrent
}

func (m *Manager) Disable(account *Account) {
	if account == nil {
		return
	}

	account.MarkDisabled(m.now(), m.reprobeCooldown)

	// If the disabled account is currently in use, refresh the current
	// account via the same two-tier selection as Current()'s slow path:
	// prefer a genuinely-active account; only if none, a cooled-disabled
	// re-probe candidate; only if none of those, the all-disabled fallback.
	// In the single-account degenerate case selectAccount returns the
	// just-disabled account (no sibling exists) and the link service's
	// degenerate-disable detection bails the recursion instead of looping.
	m.current.Store(m.selectAccount())
}

func (m *Manager) Reset() {
	m.accounts.Range(func(key string, acc *Account) bool {
		acc.Reset()
		return true
	})

	// Set current to first active account
	activeAccounts := m.Active()
	if len(activeAccounts) > 0 {
		m.current.Store(activeAccounts[0])
	} else {
		m.current.Store(nil)
	}
}

// HasUsableAccount reports whether any account is usable now (genuinely
// active, or disabled with its re-probe cooldown elapsed). The link service
// uses it as a pre-RD short-circuit: when false, hitting the debrid would
// only re-cap and re-enter Disable every refresh_interval without letting
// the cooldown elapse, so it fails fast with the transient error until the
// cooldown actually expires. Single allocation-free pass (no slice, no
// sort) — it is on the GetLink path, including the healthy case.
func (m *Manager) HasUsableAccount() bool {
	now := m.now()
	found := false
	m.accounts.Range(func(_ string, acc *Account) bool {
		if acc.usable(now, m.reprobeCooldown) {
			found = true
			return false // stop at the first usable account
		}
		return true
	})
	return found
}

// EnableAccount clears the disabled state of the account identified by token
// (implicit re-enable on a successful link validation). No-op if the token
// is unknown or the account is not disabled.
func (m *Manager) EnableAccount(token string) {
	acc, err := m.GetAccount(token)
	if err != nil || acc == nil {
		return
	}
	if !acc.Disabled.Load() {
		return
	}
	acc.Enable()
	m.logger.Info().
		Str("debrid", m.debrid).
		Str("account_token", utils.Mask(acc.Token)).
		Msg("Re-enabled account after a successful link validation (debrid recovered)")
	// A freshly re-enabled account is the preferred current; refresh the
	// two-tier selection so it is picked up immediately.
	m.current.Store(m.selectAccount())
}

func (m *Manager) GetAccount(token string) (*Account, error) {
	if token == "" {
		return nil, fmt.Errorf("token cannot be empty")
	}
	acc, ok := m.accounts.Load(token)
	if !ok {
		return nil, fmt.Errorf("account not found for token")
	}
	return acc, nil
}

func (m *Manager) GetDownloadLink(id string, file *types.File, fetcher LinkFetcher) (types.DownloadLink, error) {
	current := m.Current()
	if current == nil {
		return types.DownloadLink{}, fmt.Errorf("no active account for debrid %s", m.debrid)
	}
	dl, err := current.GetDownloadLink(id, file, fetcher)
	if err != nil {
		activeAccounts := m.Active()
		for _, acc := range activeAccounts {
			if acc.Token == current.Token {
				continue
			}
			dl, err = acc.GetDownloadLink(id, file, fetcher)
			if err != nil {
				continue
			} else {
				// Successfully got link from another account. Just return it, no need to switch current account
				return dl, nil
			}
		}
	}
	return dl, nil
}

func (m *Manager) StoreDownloadLink(downloadLink types.DownloadLink) {
	if downloadLink.Link == "" || downloadLink.Token == "" {
		return
	}
	account, err := m.GetAccount(downloadLink.Token)
	if err != nil || account == nil {
		return
	}
	account.storeLink(downloadLink)
}

func (m *Manager) DeleteDownloadLink(downloadLink types.DownloadLink, deleter LinkDeleter) error {
	if downloadLink.Link == "" || downloadLink.Token == "" {
		return fmt.Errorf("invalid download link")
	}
	account, err := m.GetAccount(downloadLink.Token)
	if err != nil || account == nil {
		return fmt.Errorf("account not found for download link")
	}
	return account.DeleteLink(downloadLink, deleter)
}

func (m *Manager) Stats() []map[string]any {
	stats := make([]map[string]any, 0)

	for _, acc := range m.All() {
		maskedToken := utils.Mask(acc.Token)
		accountDetail := map[string]any{
			"in_use":       acc.Equals(m.Current()),
			"order":        acc.Index,
			"disabled":     acc.Disabled.Load(),
			"token_masked": maskedToken,
			"username":     acc.Username,
			"traffic_used": acc.TrafficUsed.Load(),
			"expiration":   acc.Expiration,
			"links_count":  acc.DownloadLinksCount(),
			"debrid":       acc.Debrid,
		}
		stats = append(stats, accountDetail)
	}
	return stats
}

func (m *Manager) RefreshLinks(fetcher LinksFetcher) error {
	wgPool := pool.New().WithMaxGoroutines(max(1, m.accounts.Size())).WithErrors()
	m.accounts.Range(func(key string, acc *Account) bool {
		wgPool.Go(func() error {
			links, err := fetcher(acc)
			if err != nil {
				m.logger.Error().Err(err).Str("debrid", m.debrid).Str("account_token", utils.Mask(acc.Token)).Msg("Failed to fetch download links for account")
				return err
			}
			for _, dl := range links {
				acc.storeLink(dl)
			}
			return nil
		})
		return true
	})
	return wgPool.Wait()
}

func (m *Manager) Sync(syncer SyncFunc) {
	workers := m.accounts.Size()
	if workers == 0 {
		return
	}
	wgPool := pool.New().WithMaxGoroutines(workers)
	m.accounts.Range(func(key string, acc *Account) bool {
		wgPool.Go(func() {
			if err := syncer(acc); err != nil {
				m.logger.Error().Err(err).Str("debrid", m.debrid).Str("account_token", utils.Mask(acc.Token)).Msg("Failed to sync account")
				return
			}
			// Check if account has expired
			if !acc.Expiration.IsZero() && utils.Now().After(acc.Expiration) {
				m.logger.Warn().Str("debrid", m.debrid).Str("account_token", utils.Mask(acc.Token)).Msg("Account has expired, disabling")
				m.Disable(acc)
			}
			m.UpdateAccount(acc)
		})
		return true
	})
	wgPool.Wait()
}

func (m *Manager) UpdateAccount(updatedAccount *Account) {
	if updatedAccount == nil {
		return
	}
	if updatedAccount.Token == "" {
		return
	}
	m.accounts.Store(updatedAccount.Token, updatedAccount)
}
