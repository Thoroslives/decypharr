package manager

import "testing"

// TestNormalizeStreamRangeFullReadNotTruncated is the truncation-fix lock for
// Fix 2 (BIS-3 Mount size-fidelity).
//
// normalizeStreamRange is mathematically correct GIVEN a truthful size: a
// full-file read (start=0, end=-1) over a file of true length N must yield the
// byte range [0, N-1]. The BIS-3 bug was never in this function; it was that
// the size handed to it (Entry.Size / File.Size) came from STATIC Real-Debrid
// JSON metadata that never matched the bytes the RD CDN actually delivered, so
// the last ~1.2 GB of a large media file was silently dropped from the
// advertised range. Fix 2 reconciles File.Size / Entry.Size to the
// authoritative CDN Content-Length on completion; this test pins the contract
// that, once the size is truthful, no truncation occurs.
//
// Scope (Concern 6): this proves the function is correct for a COMPLETED
// local-pull entry whose size has been reconciled. It does NOT claim to fix
// the streamed-before-complete path universally.
func TestNormalizeStreamRangeFullReadNotTruncated(t *testing.T) {
	// 57_251_461_400 is a representative true content length for a large
	// remux (~57 GB) where the RD per-file f.Bytes metadata under-reported by
	// ~1.2 GB in the field-observed regression.
	const trueSize = int64(57_251_461_400)

	start, end, err := normalizeStreamRange(trueSize, 0, -1)
	if err != nil {
		t.Fatalf("normalizeStreamRange(%d, 0, -1) returned error: %v", trueSize, err)
	}
	if start != 0 {
		t.Errorf("start: got %d want 0", start)
	}
	if end != trueSize-1 {
		t.Errorf("end: got %d want %d (full read must cover the last byte; a short end means the advertised range truncates the file)", end, trueSize-1)
	}
}
