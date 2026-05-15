package manager

import (
	"context"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// NewForTest builds a minimal *Manager backed by the given Storage, suitable
// for tests in OTHER packages (notably pkg/server/qbit) that need a real
// Manager surface for handler integration tests. Production code uses New().
//
// The returned Manager has:
//   - Real storage (bbolt-backed via storage.NewStorage)
//   - Real queue rooted on the supplied storage
//   - downloadCancels registry initialised for Fix B
//   - Empty (but non-nil) clients map so RemoveTorrentPlacements is a safe
//     no-op for placements whose Provider isn't registered (which is the
//     expected case in tests)
//   - No-op logger, no scheduler, no downloader, no mount manager. Tests
//     should only exercise surfaces compatible with that minimal init.
//
// This helper exists so qBit DELETE-handler tests can construct a *Manager
// without reaching into private fields. Avoids the heavy New() flow which
// hydrates debrid clients, the link service, the entry cache, and the
// repair service.
func NewForTest(strg *storage.Storage, log zerolog.Logger) *Manager {
	ctx := context.Background()
	m := &Manager{
		storage:           strg,
		ctx:               ctx,
		logger:            log,
		queue:             newQueue(ctx, strg, 16, ""),
		clients:           xsync.NewMap[string, debrid.Client](),
		processingEntries: xsync.NewMap[string, time.Time](),
		downloadCancels:   xsync.NewMap[string, *downloadHandle](),
		clock:             realClock{},
	}
	return m
}
