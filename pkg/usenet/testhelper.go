package usenet

import (
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

// NewForTest builds a minimal *Usenet suitable for tests in OTHER packages
// (notably pkg/manager) that need a non-nil m.usenet whose PreCache resolves
// quickly without panicking or doing any NNTP I/O.
//
// It wires only the NZB metadata store and an empty fs map. With these,
// PreCache misses on the fs Load fast path and getFile then consults the
// metadata store, which has no record for an unknown id, so PreCache returns
// a "not found" error instead of dereferencing the (absent) NNTP client.
//
// Production code uses New(); this helper exists so tests can exercise code
// that spawns the detached precache goroutines (processNZB) without those
// goroutines crashing the test process. Mirrors manager.NewForTest.
func NewForTest(store *NZBStorage) *Usenet {
	return &Usenet{
		nzbStorage: store,
		logger:     zerolog.Nop(),
		fs:         xsync.NewMap[string, *fsEntry](),
	}
}
