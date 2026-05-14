package testutil

import (
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
)

// IsolateConfig points the global config singleton at dir for the duration of
// the test. Without this, packages that transitively call config.Get() (e.g.
// via logger.Default()) crash the process with "mkdir : no such file or
// directory" because the singleton tries to MkdirAll on an empty path.
//
// Call this from TestMain or the start of any test whose imports may pull
// config.Get(). Safe to call multiple times; SetConfigPath is idempotent.
func IsolateConfig(t testing.TB, dir string) {
	t.Helper()
	config.SetConfigPath(dir)
}
