package utils

import (
	"fmt"
	"os"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
)

// TestMain establishes an isolated config path before any test runs.
//
// magnet.go transitively pulls logger.Default(), which calls config.Get().
// When the config singleton path is unset, config.Get() does os.Exit(1)
// (production main() sets it via SetConfigPath; tests must do the same).
// testutil.IsolateConfig wraps SetConfigPath but needs a testing.TB for
// t.Helper(), which is unavailable in TestMain, so we set the path directly
// with the same effect for the whole package test binary.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "decypharr-utils-test-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create temp config dir: %v\n", err)
		os.Exit(1)
	}
	config.SetConfigPath(dir)

	code := m.Run()

	_ = os.RemoveAll(dir)
	os.Exit(code)
}
