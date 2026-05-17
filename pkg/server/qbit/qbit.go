package qbit

import (
	"sync"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

type QBit struct {
	downloadFolder          string
	categories              []string
	alwaysRemoveTrackerURLS bool
	logger                  zerolog.Logger
	Tags                    []string
	manager                 *manager.Manager

	// Speed-smoothing cache. In-memory only; never persisted.
	// Guards B2: replaces the instantaneous grab meter with a Δ-bytes/Δ-time
	// rate derived from the truthful SizeDownloaded counter between qbit polls.
	// Pruned to the live torrent set each poll to prevent unbounded growth (DA C5).
	speedMu     sync.Mutex
	speedCache  map[string]speedSample
	lastDerived map[string]int64
}

func New(manager *manager.Manager) *QBit {
	cfg := config.Get()
	return &QBit{
		downloadFolder:          cfg.DownloadFolder,
		categories:              cfg.Categories,
		alwaysRemoveTrackerURLS: cfg.AlwaysRmTrackerUrls,
		manager:                 manager,
		logger:                  logger.New("qbit"),
		speedCache:              make(map[string]speedSample),
		lastDerived:             make(map[string]int64),
	}
}
