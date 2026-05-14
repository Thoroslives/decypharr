package manager

import (
	"context"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
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
//   - No-op logger, no debrid clients, no scheduler, no downloader, no
//     mount manager. Tests should only exercise surfaces compatible with
//     that minimal init.
//
// This helper exists so qBit DELETE-handler tests can construct a *Manager
// without reaching into private fields. Avoids the heavy New() flow which
// hydrates debrid clients, the link service, the entry cache, and the
// repair service.
//
// See: /brain/05-Projects/2026-05-15-decypharr-fork-spec.md (Fix B).
func NewForTest(strg *storage.Storage, log zerolog.Logger) *Manager {
	ctx := context.Background()
	m := &Manager{
		storage:           strg,
		ctx:               ctx,
		logger:            log,
		queue:             newQueue(ctx, strg, 16, ""),
		processingEntries: xsync.NewMap[string, time.Time](),
		downloadCancels:   xsync.NewMap[string, *downloadHandle](),
		clock:             realClock{},
	}
	return m
}
