package qbit

import (
	"time"
)

type speedSample struct {
	size int64
	at   time.Time
}

// deriveDlspeed returns a smoothed instantaneous rate (bytes/sec) computed from
// the truthful SizeDownloaded counter over the wall-clock gap since the last
// poll, replacing grab's ~30%-optimistic BytesPerSecond meter. In-memory only;
// never persisted (no storage.Entry/proto field - that is the protoc landmine).
func (q *QBit) deriveDlspeed(hash string, sizeDownloaded int64, now time.Time) int64 {
	q.speedMu.Lock()
	defer q.speedMu.Unlock()
	prev, ok := q.speedCache[hash]
	q.speedCache[hash] = speedSample{size: sizeDownloaded, at: now}
	if !ok {
		return 0
	}
	elapsed := now.Sub(prev.at).Seconds()
	if elapsed <= 0 {
		return q.lastDerived[hash]
	}
	delta := sizeDownloaded - prev.size
	if delta < 0 {
		delta = 0
	}
	v := int64(float64(delta) / elapsed)
	q.lastDerived[hash] = v
	return v
}

// pruneSpeedCache bounds the in-memory maps to the currently-tracked torrent
// set (DA C5 - without this they grow unbounded over a long-lived container).
func (q *QBit) pruneSpeedCache(current map[string]struct{}) {
	q.speedMu.Lock()
	defer q.speedMu.Unlock()
	for h := range q.speedCache {
		if _, live := current[h]; !live {
			delete(q.speedCache, h)
			delete(q.lastDerived, h)
		}
	}
}
