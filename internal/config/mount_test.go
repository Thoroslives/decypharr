package config

import "testing"

// TestMountApplyDefaultsCacheDir guards G2: v2.x migration auto-set
// mount.type=dfs but left mount.dfs.cache_dir empty, causing
// "mkdir : no such file or directory" at startup. Default cache_dir
// to /cache/dfs (matching the v2.3 docs) so the mount initializes cleanly.
func TestMountApplyDefaultsCacheDir(t *testing.T) {
	tests := []struct {
		name    string
		in      Mount
		wantDir string
	}{
		{
			name:    "dfs with empty cache_dir gets default",
			in:      Mount{Type: MountTypeDFS, DFS: DFS{CacheDir: ""}},
			wantDir: "/cache/dfs",
		},
		{
			name:    "dfs with explicit cache_dir is preserved",
			in:      Mount{Type: MountTypeDFS, DFS: DFS{CacheDir: "/custom/cache"}},
			wantDir: "/custom/cache",
		},
		{
			name:    "type=none does not touch dfs subsection",
			in:      Mount{Type: MountTypeNone, DFS: DFS{CacheDir: ""}},
			wantDir: "", // unchanged
		},
		{
			name:    "type=rclone does not touch dfs subsection",
			in:      Mount{Type: MountTypeRclone, DFS: DFS{CacheDir: ""}},
			wantDir: "", // unchanged
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := tc.in
			m.ApplyDefaults()
			if m.DFS.CacheDir != tc.wantDir {
				t.Errorf("DFS.CacheDir = %q, want %q", m.DFS.CacheDir, tc.wantDir)
			}
		})
	}
}
