package realdebrid

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/internal/testutil"
)

// TestHeadContentLength guards the BIS-3 Fix 2 size-fidelity probe.
//
// Decypharr advertises STATIC Real-Debrid JSON metadata sizes that are never
// reconciled to the bytes the RD CDN actually delivers (the per-file f.Bytes
// field under-reported a ~57 GB remux by ~1.2 GB in the field). The only
// authoritative figure is the live CDN Content-Length. HeadContentLength does
// a single HEAD against the resolved download link and returns that
// Content-Length so the downloader can reconcile File.Size / Entry.Size on
// completion. A non-2xx response must surface as an error (so the caller keeps
// the prior size rather than persisting a wrong one).
func TestHeadContentLength(t *testing.T) {
	testutil.IsolateConfig(t, t.TempDir())

	const trueLen = int64(57_251_461_400)

	t.Run("returns CDN Content-Length on 200", func(t *testing.T) {
		var gotMethod string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotMethod = r.Method
			w.Header().Set("Content-Length", "57251461400")
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		r := &RealDebrid{client: request.New()}
		got, err := r.HeadContentLength(context.Background(), srv.URL)
		if err != nil {
			t.Fatalf("HeadContentLength returned error: %v", err)
		}
		if gotMethod != http.MethodHead {
			t.Errorf("request method: got %q want HEAD (must not download the body)", gotMethod)
		}
		if got != trueLen {
			t.Errorf("content length: got %d want %d", got, trueLen)
		}
	})

	t.Run("non-2xx is an error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// 451 Unavailable For Legal Reasons is the exact RD/DMCA status
			// that motivates a defensive "keep prior size" on failure.
			w.WriteHeader(http.StatusUnavailableForLegalReasons)
		}))
		defer srv.Close()

		r := &RealDebrid{client: request.New()}
		if _, err := r.HeadContentLength(context.Background(), srv.URL); err == nil {
			t.Fatal("expected error on non-2xx HEAD, got nil (caller would persist a wrong size)")
		}
	})

	t.Run("missing/unknown Content-Length is an error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// No Content-Length, chunked-style unknown length.
			w.Header().Set("Transfer-Encoding", "chunked")
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		r := &RealDebrid{client: request.New()}
		if _, err := r.HeadContentLength(context.Background(), srv.URL); err == nil {
			t.Fatal("expected error when Content-Length is unknown (<=0), got nil")
		}
	})
}
