package account

import (
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/testutil"
	"go.uber.org/ratelimit"
)

// R7 regression suite for the account-disable latch.
//
// A single RD bytes_limit_reached (or bandwidth/quota/daily code) used to
// latch the whole pipeline until a process restart: MarkDisabled set
// Disabled.Store(true) and the ONLY clear path (Account.Reset via
// Manager.Reset) had zero external callers. The fix adds a race-safe
// disable timestamp and a cooldown so a disabled account becomes a re-probe
// candidate again once the cooldown elapses, with genuinely-active accounts
// always preferred over cooled-but-still-disabled ones.
//
// The invariants under test are behavioural, not impl-shaped:
//   (b) a disabled account becomes selectable again via Current() after the
//       cooldown elapses on a fake clock (auto re-probe);
//   (c) still-capped after the cooldown re-disables with a FRESH timestamp
//       (no tight loop: the cooldown window restarts);
//   (d) MULTI-account: a genuinely-active account is selected over a
//       cooled-but-disabled one (regression guard);
//   (e) disabledAt is race-safe under concurrent MarkDisabled + usable reads.

func newTestManager(t *testing.T, tokens []string, clk Clock, cooldown time.Duration) *Manager {
	t.Helper()
	testutil.IsolateConfig(t, t.TempDir())
	dc := config.Debrid{
		Provider:        "realdebrid",
		Name:            "realdebrid",
		DownloadAPIKeys: tokens,
	}
	m := NewManager(dc, ratelimit.NewUnlimited(), zerolog.Nop())
	m.SetClock(clk)
	m.SetReprobeCooldown(cooldown)
	return m
}

// (b) auto re-probe: a disabled single account is unusable while inside the
// cooldown but becomes the Current() candidate once the cooldown elapses.
func TestDisabledAccountReprobableAfterCooldown(t *testing.T) {
	clk := testutil.NewFakeClock(time.Now())
	cooldown := 15 * time.Minute
	m := newTestManager(t, []string{"tok-a"}, clk, cooldown)

	acc := m.Current()
	if acc == nil {
		t.Fatal("precondition: expected an initial current account")
	}

	m.Disable(acc)
	if !acc.Disabled.Load() {
		t.Fatal("precondition: account should be marked Disabled after Disable()")
	}

	// Inside the cooldown the account must NOT be treated as usable: a
	// just-capped account re-probed immediately is the tight-loop bug.
	if acc.usable(clk.Now(), cooldown) {
		t.Fatal("a just-disabled account must not be usable inside the cooldown window (immediate re-probe = tight loop)")
	}

	// Advance past the cooldown: the same account becomes a re-probe
	// candidate again (single-account self-heal). Pre-fix Current() returned
	// the disabled account via the all-disabled fallback at FULL SPEED with
	// no time gate, so the only "recovery" was a process restart.
	clk.Advance(cooldown + time.Second)
	if !acc.usable(clk.Now(), cooldown) {
		t.Fatal("a disabled account must become usable again once the cooldown has elapsed (auto re-probe; otherwise only a restart clears it)")
	}
	got := m.Current()
	if got == nil {
		t.Fatal("Current() returned nil after the cooldown elapsed; the single account should be re-probed")
	}
	if got.Token != acc.Token {
		t.Fatalf("Current() should re-probe the cooled account, got token %q want %q", got.Token, acc.Token)
	}
}

// (c) still-capped after the cooldown: re-disabling stamps a FRESH timestamp
// so the cooldown window restarts (cheap periodic re-probe, never a tight
// loop).
func TestStillCappedAfterCooldownReDisablesWithFreshTimestamp(t *testing.T) {
	clk := testutil.NewFakeClock(time.Now())
	cooldown := 15 * time.Minute
	m := newTestManager(t, []string{"tok-a"}, clk, cooldown)

	acc := m.Current()
	m.Disable(acc)
	firstStamp := acc.disabledAtNanos.Load()
	if firstStamp == 0 {
		t.Fatal("MarkDisabled must record a non-zero disable timestamp")
	}

	clk.Advance(cooldown + time.Minute)
	if !acc.usable(clk.Now(), cooldown) {
		t.Fatal("account should be re-probable after the cooldown")
	}

	// RD is still capped: the re-probe fails and the account is disabled
	// again. The new timestamp must be strictly later so the next re-probe
	// is another full cooldown away (no busy loop).
	m.Disable(acc)
	secondStamp := acc.disabledAtNanos.Load()
	if secondStamp <= firstStamp {
		t.Fatalf("re-disable must record a FRESH (later) timestamp so the cooldown window restarts; first=%d second=%d", firstStamp, secondStamp)
	}
	if acc.usable(clk.Now(), cooldown) {
		t.Fatal("immediately after a re-disable the account must be unusable again (a fresh cooldown window, not an immediate re-probe)")
	}
}

// (d) multi-account regression guard: a genuinely-active account must be
// selected over a cooled-but-still-disabled one. A flat usable() Index-sort
// would let a low-Index cooled account outrank a healthy higher-Index one
// and detour the pipeline through a dead account every cooldown.
func TestActiveAccountPreferredOverCooledDisabled(t *testing.T) {
	clk := testutil.NewFakeClock(time.Now())
	cooldown := 15 * time.Minute
	// tok-a is Index 0, tok-b is Index 1.
	m := newTestManager(t, []string{"tok-a", "tok-b"}, clk, cooldown)

	accA, err := m.GetAccount("tok-a")
	if err != nil {
		t.Fatalf("GetAccount tok-a: %v", err)
	}
	accB, err := m.GetAccount("tok-b")
	if err != nil {
		t.Fatalf("GetAccount tok-b: %v", err)
	}

	// Disable the low-Index account and let it cool down. tok-b stays
	// genuinely active the whole time.
	m.Disable(accA)
	clk.Advance(cooldown + time.Minute)

	if !accA.usable(clk.Now(), cooldown) {
		t.Fatal("precondition: accA should be a cooled re-probe candidate after the cooldown")
	}
	if accB.Disabled.Load() {
		t.Fatal("precondition: accB must remain genuinely active")
	}

	// Even though accA (Index 0) is now "usable" via the cooldown, the
	// genuinely-active accB must win: healthy always outranks
	// cooled-but-likely-still-capped.
	got := m.Current()
	if got == nil {
		t.Fatal("Current() returned nil with a genuinely-active account available")
	}
	if got.Token != accB.Token {
		t.Fatalf("Current() must prefer the genuinely-active account over a cooled-disabled one; got %q want %q (regression: pipeline detours through a dead account every cooldown)", got.Token, accB.Token)
	}
}

// (e) race safety: concurrent MarkDisabled writers and usable readers must
// not produce a torn/zero timestamp read. Run under `go test -race`.
func TestDisabledAtRaceSafe(t *testing.T) {
	clk := testutil.NewFakeClock(time.Now())
	cooldown := 15 * time.Minute
	m := newTestManager(t, []string{"tok-a"}, clk, cooldown)
	acc := m.Current()

	var wg sync.WaitGroup
	const goroutines = 16
	const iterations = 200

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				acc.MarkDisabled(clk.Now())
			}
		}()
	}
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = acc.usable(clk.Now(), cooldown)
			}
		}()
	}
	wg.Wait()

	if acc.disabledAtNanos.Load() == 0 {
		t.Fatal("after concurrent MarkDisabled calls the disable timestamp must be set (non-zero)")
	}
}
