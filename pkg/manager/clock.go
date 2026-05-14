package manager

import "time"

// Clock is a minimal time source so TTL/sweep logic can be exercised in tests
// without sleeping. Production uses realClock; tests inject FakeClock from
// internal/testutil.
type Clock interface {
	Now() time.Time
}

// realClock returns wall-clock time.
type realClock struct{}

// Now implements Clock.
func (realClock) Now() time.Time { return time.Now() }
