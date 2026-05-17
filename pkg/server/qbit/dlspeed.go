package qbit

import (
	"time"
)

type speedSample struct {
	size     int64
	at       time.Time
	lastRate int64 // rate returned for this hash on the previous poll
}

// deriveDlspeed returns the instantaneous transfer rate (bytes/sec) computed
// from the truthful SizeDownloaded counter over the wall-clock gap since the
// last poll, replacing grab's ~30%-optimistic BytesPerSecond meter. In-memory
// only; never persisted (a storage.Entry field would mean a hand-maintained
// protobuf schema change).
func (q *QBit) deriveDlspeed(hash string, sizeDownloaded int64, now time.Time) int64 {
	q.speedMu.Lock()
	defer q.speedMu.Unlock()
	prev, ok := q.speedCache[hash]
	if !ok {
		q.speedCache[hash] = speedSample{size: sizeDownloaded, at: now}
		return 0
	}
	elapsed := now.Sub(prev.at).Seconds()
	if elapsed <= 0 {
		// Same/earlier timestamp: advance the sample but keep the prior rate.
		q.speedCache[hash] = speedSample{size: sizeDownloaded, at: now, lastRate: prev.lastRate}
		return prev.lastRate
	}
	delta := sizeDownloaded - prev.size
	if delta < 0 {
		delta = 0
	}
	v := int64(float64(delta) / elapsed)
	q.speedCache[hash] = speedSample{size: sizeDownloaded, at: now, lastRate: v}
	return v
}

// pruneSpeedCache bounds the in-memory cache to the currently-tracked torrent
// set; without it the map grows unbounded over a long-lived container.
func (q *QBit) pruneSpeedCache(current map[string]struct{}) {
	q.speedMu.Lock()
	defer q.speedMu.Unlock()
	for h := range q.speedCache {
		if _, live := current[h]; !live {
			delete(q.speedCache, h)
		}
	}
}
