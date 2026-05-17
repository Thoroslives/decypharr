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

	// Per-hash instantaneous-dlspeed cache: a Δ-bytes/Δ-time rate from the
	// truthful SizeDownloaded counter between qbit polls, replacing grab's
	// optimistic meter. In-memory only; pruned to the live torrent set each
	// poll so it cannot grow unbounded.
	speedMu    sync.Mutex
	speedCache map[string]speedSample
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
	}
}
