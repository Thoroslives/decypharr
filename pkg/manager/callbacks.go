package manager

import (
	"time"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

const maxRDRemoveAttempts = 3

// rdRemoveBackoffBase is the base inter-attempt backoff; a package var (not a
// const) solely so tests can shrink it - production keeps the 1s/2s schedule.
var rdRemoveBackoffBase = time.Second

func (m *Manager) RemoveFromProvider(providerEntry *storage.ProviderEntry) error {
	if providerEntry == nil {
		return nil
	}
	if providerEntry.Provider == "usenet" {
		if m.usenet != nil {
			return m.usenet.Delete(providerEntry.ID)
		}
		return nil
	}

	client := m.ProviderClient(providerEntry.Provider)
	if client == nil {
		return nil
	}
	return client.DeleteTorrent(providerEntry.ID)
}

func (m *Manager) RemoveTorrentPlacements(t *storage.Entry) {
	for _, placement := range t.Providers {
		var err error
		for attempt := 1; attempt <= maxRDRemoveAttempts; attempt++ {
			if err = m.RemoveFromProvider(placement); err == nil {
				break
			}
			if attempt < maxRDRemoveAttempts {
				time.Sleep(time.Duration(attempt) * rdRemoveBackoffBase)
			}
		}
		if err != nil {
			m.logger.Warn().Err(err).
				Str("infohash", t.InfoHash).
				Str("provider", placement.Provider).
				Int("attempts", maxRDRemoveAttempts).
				Msg("RD-side removal failed after retries; tombstone prevents re-adoption, leaving as RD-side clutter")
		}
	}
}
