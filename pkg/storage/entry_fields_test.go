package storage

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
)

// TestEntryRDProgressFieldsSerialize guards the Fix F field contract:
//   - Entry.Progress and Entry.Speed are reserved for local-pull truth.
//   - Entry.RDProgress and Entry.RDSpeed carry the upstream debrid provider's
//     own ingestion claim (RD/AllDebrid/etc.). The fields are distinct so the
//     qBit-compat API never sees the RD-side number, but the internal API can
//     expose both for the Decypharr dashboard.
//
// See: /brain/05-Projects/2026-05-15-decypharr-fork-spec.md (Fix F, revised).
func TestEntryRDProgressFieldsSerialize(t *testing.T) {
	e := &Entry{
		InfoHash:   "abc",
		Name:       "test",
		Progress:   0.5,
		Speed:      100,
		RDProgress: 0.87,
		RDSpeed:    1000,
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(b)
	// Both surfaces must be JSON-encodable under stable keys; arrs / dashboards
	// rely on these names.
	for _, key := range []string{`"progress":0.5`, `"speed":100`, `"rd_progress":0.87`, `"rd_speed":1000`} {
		if !strings.Contains(s, key) {
			t.Errorf("JSON missing %q in %s", key, s)
		}
	}
}

// TestEntryRDProgressOmittedWhenZero guards the omitempty contract: an entry
// that has never seen an RD-side progress poll should not emit rd_progress /
// rd_speed at all. Keeps response shapes compatible with consumers reading the
// internal API before this field existed.
func TestEntryRDProgressOmittedWhenZero(t *testing.T) {
	e := &Entry{InfoHash: "abc", Name: "test"}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(b)
	for _, key := range []string{"rd_progress", "rd_speed"} {
		if strings.Contains(s, key) {
			t.Errorf("expected %q omitted when zero, got %s", key, s)
		}
	}
}

// TestEntrySanitizesRDProgressNaN guards against non-finite values reaching the
// JSON encoder via the RD-side field. Mirrors the existing Sanitize contract
// for Entry.Progress.
func TestEntrySanitizesRDProgressNaN(t *testing.T) {
	e := &Entry{
		InfoHash:   "abc",
		RDProgress: math.NaN(),
	}
	e.Sanitize()
	if math.IsNaN(e.RDProgress) || math.IsInf(e.RDProgress, 0) {
		t.Errorf("Sanitize did not zero non-finite RDProgress: got %v", e.RDProgress)
	}
	if e.RDProgress != 0 {
		t.Errorf("expected RDProgress=0 after Sanitize, got %v", e.RDProgress)
	}
}
