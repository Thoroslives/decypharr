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
				acc.MarkDisabled(clk.Now(), cooldown)
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

// Defect #1 production-interaction regression: the Step-0 retryable-entry
// fix re-dispatches a stuck entry every refresh_interval (~15s). Each
// re-dispatch re-enters the disable path and calls Disable() again, long
// before the (15min) cooldown elapses. If MarkDisabled re-stamps the
// disable timestamp unconditionally (the 6d9696e behaviour), every ~15s
// re-disable pushes disabledAtNanos forward ~15s, so now-disabledAt is
// ALWAYS ~15s, NEVER >= cooldown: usable()/cooledDisabledAccounts() stay
// perpetually empty, the time-based self-heal NEVER fires and the latch is
// intact (just 1/15s instead of 1/s, still restart-only).
//
// The invariant: a sustained storm of within-cooldown re-disables MUST NOT
// push the effective cooldown out. After total elapsed >= cooldown the
// account becomes usable DESPITE the intervening re-disables.
//
// Fails on 6d9696e (unconditional re-stamp); passes with the conditional
// re-stamp (preserve the original stamp while already-disabled AND still
// within cooldown).
func TestWithinCooldownReDisablesDoNotDeferSelfHeal(t *testing.T) {
	clk := testutil.NewFakeClock(time.Now())
	cooldown := 15 * time.Minute
	m := newTestManager(t, []string{"tok-a"}, clk, cooldown)

	acc := m.Current()
	m.Disable(acc) // T0: enabled->disabled transition, stamps T0
	stampAtT0 := acc.disabledAtNanos.Load()
	if stampAtT0 == 0 {
		t.Fatal("precondition: first Disable must stamp a non-zero disable timestamp")
	}

	// Phase 1 - the core Defect #1 invariant. Simulate the Step-0
	// re-dispatch loop entirely WITHIN one cooldown window: every 15s the
	// stuck entry is re-dispatched, the single account is re-selected (still
	// capped) and Disable() is called again. Every one of these is a
	// within-cooldown re-disable and MUST preserve the original T0 stamp.
	// On 6d9696e (unconditional re-stamp) each iteration pushes the stamp
	// forward ~15s, so now-disabledAt is forever ~15s and the cooldown can
	// NEVER elapse (the latch, just slower). Stop one step short of the
	// cooldown so the boundary is crossed in Phase 2 with NO intervening
	// Disable() (this is exactly what the production pre-RD short-circuit
	// guarantees: within-cooldown re-dispatches never reach Disable()).
	const step = 15 * time.Second
	withinCooldownIters := int(cooldown/step) - 2 // stay strictly inside the window
	for i := 0; i < withinCooldownIters; i++ {
		clk.Advance(step)
		m.Disable(acc) // within-cooldown re-disable: MUST preserve the stamp
		if got := acc.disabledAtNanos.Load(); got != stampAtT0 {
			t.Fatalf("within-cooldown re-disable #%d pushed the disable timestamp forward (got %d, want the original %d); now-disabledAt resets to ~15s every re-dispatch so the cooldown NEVER elapses and the self-heal never fires (R7 latch intact, just slower)", i, got, stampAtT0)
		}
		if acc.usable(clk.Now(), cooldown) {
			t.Fatalf("account became usable at within-cooldown iteration #%d (elapsed %v < cooldown %v); a just-capped account must not re-probe before its cooldown", i, time.Duration(i+1)*step, cooldown)
		}
	}

	// Phase 2 - the cooldown now elapses (no further Disable(): the
	// short-circuit gates re-dispatches in prod). The account MUST become a
	// re-probe candidate despite the Phase-1 re-disable storm.
	clk.Advance(2 * step) // crosses the original T0+cooldown boundary
	if !acc.usable(clk.Now(), cooldown) {
		t.Fatal("after the cooldown elapsed the account must be usable() again despite the intervening within-cooldown re-disables; the time-based self-heal is being indefinitely deferred (R7 latch persists, restart-only)")
	}
	cooled := m.cooledDisabledAccounts()
	if len(cooled) != 1 || cooled[0].Token != acc.Token {
		t.Fatalf("the cooled account must appear as a re-probe candidate after the cooldown despite the re-disable storm; cooledDisabledAccounts()=%d", len(cooled))
	}
	if got := m.Current(); got == nil || got.Token != acc.Token {
		t.Fatal("Current() must re-probe the cooled single account after the cooldown elapses despite the intervening within-cooldown re-disables")
	}

	// Phase 3 - the periodic re-fire (condition (b)). The post-cooldown
	// re-probe hits RD, is still capped, Disable() fires: this is a GENUINE
	// failed re-probe so the backoff legitimately restarts. The account must
	// go unusable again (a fresh window, not stuck-usable every tick) AND
	// must self-heal AGAIN one cooldown later (periodic, never a permanent
	// latch and never a permanent tight loop).
	m.Disable(acc) // genuine post-cooldown re-probe failed -> restart backoff
	if acc.usable(clk.Now(), cooldown) {
		t.Fatal("immediately after a genuine post-cooldown re-disable the account must be unusable again (a fresh cooldown window); otherwise it re-probes every tick (a different tight loop)")
	}
	clk.Advance(cooldown + step)
	if !acc.usable(clk.Now(), cooldown) {
		t.Fatal("the self-heal must fire AGAIN one cooldown after a failed re-probe; the re-probe must be periodic, not one-shot")
	}
}

// Defect #1 corollary: a GENUINE post-cooldown re-probe that fails again
// MUST restart the backoff (re-stamp). This is condition (b) of the
// conditional re-stamp and guards against over-correcting Defect #1 into
// "never re-stamp" (which would make a still-capped account re-probe every
// tick forever after the first cooldown -- a different tight loop).
func TestPostCooldownReDisableRestartsBackoff(t *testing.T) {
	clk := testutil.NewFakeClock(time.Now())
	cooldown := 15 * time.Minute
	m := newTestManager(t, []string{"tok-a"}, clk, cooldown)

	acc := m.Current()
	m.Disable(acc)
	firstStamp := acc.disabledAtNanos.Load()

	// Cooldown fully elapses -> genuine re-probe candidate.
	clk.Advance(cooldown + time.Minute)
	if !acc.usable(clk.Now(), cooldown) {
		t.Fatal("precondition: account should be re-probable after the full cooldown")
	}

	// The genuine re-probe hits the debrid, is still capped, Disable()
	// fires again. Because the cooldown HAD elapsed (usable() was true),
	// this is condition (b): the backoff window must restart from now.
	m.Disable(acc)
	secondStamp := acc.disabledAtNanos.Load()
	if secondStamp <= firstStamp {
		t.Fatalf("a post-cooldown re-disable (genuine failed re-probe) must restart the backoff with a FRESH later timestamp; first=%d second=%d", firstStamp, secondStamp)
	}
	if acc.usable(clk.Now(), cooldown) {
		t.Fatal("immediately after a post-cooldown re-disable the account must be unusable again (a fresh full cooldown window, not an immediate re-probe every tick)")
	}
}

// Defect #2: a successful link validation on a Disabled (post-cooldown)
// account must implicitly re-enable it so it returns to healthy state and
// rejoins Active(). Pre-fix Disabled.Store(false) lived ONLY in the inert
// Reset() hook (wired nowhere automatic), so a recovered account stayed
// Disabled=true forever -- permanently excluded from Active(), stuck
// cycling as a tier-2 cooled re-probe instead of returning to normal.
//
// Modelled at the account-manager level (EnableAccount is what the link
// service calls on validationErr==nil). Fails pre-fix (no EnableAccount /
// no implicit re-enable); passes after.
func TestSuccessfulValidationReEnablesDisabledAccount(t *testing.T) {
	clk := testutil.NewFakeClock(time.Now())
	cooldown := 15 * time.Minute
	m := newTestManager(t, []string{"tok-a"}, clk, cooldown)

	acc := m.Current()
	m.Disable(acc)
	if !acc.Disabled.Load() {
		t.Fatal("precondition: account must be Disabled after Disable()")
	}
	if len(m.Active()) != 0 {
		t.Fatal("precondition: a Disabled single account must not be in Active()")
	}

	// Cooldown elapses, the re-probe hits the debrid and SUCCEEDS (RD's
	// limit window reset). The link service calls EnableAccount on the
	// account that just validated.
	clk.Advance(cooldown + time.Minute)
	m.EnableAccount(acc.Token)

	if acc.Disabled.Load() {
		t.Fatal("a successful validation must implicitly re-enable the account; it is still Disabled (a recovered account stays flagged forever, never returns to healthy state)")
	}
	if acc.disabledAtNanos.Load() != 0 {
		t.Fatal("re-enable must clear the disable timestamp so the account is fully healthy, not a lingering cooled candidate")
	}
	active := m.Active()
	if len(active) != 1 || active[0].Token != acc.Token {
		t.Fatalf("the re-enabled account must rejoin Active(); got %d active", len(active))
	}
	if got := m.Current(); got == nil || got.Token != acc.Token || got.Disabled.Load() {
		t.Fatal("Current() must return the re-enabled account via the fast/active path, not as a disabled fallback")
	}
}
