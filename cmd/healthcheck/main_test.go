package main

import "testing"

// TestOverallStatus locks the contract that WebDAV is not-applicable to the
// overall healthcheck when webdav is disabled in config (sequential mode),
// while a webdav-enabled deployment is unaffected and never gets a
// false-green when a probed service is genuinely down.
func TestOverallStatus(t *testing.T) {
	tests := []struct {
		name          string
		qbitAPI       bool
		webUI         bool
		webDAVService bool
		disableWebDav bool
		want          bool
	}{
		{
			name:          "webdav disabled and probe failed is healthy",
			qbitAPI:       true,
			webUI:         true,
			webDAVService: false,
			disableWebDav: true,
			want:          true,
		},
		{
			name:          "webdav disabled gate holds even if probe ran",
			qbitAPI:       true,
			webUI:         true,
			webDAVService: true,
			disableWebDav: true,
			want:          true,
		},
		{
			name:          "webdav enabled and down is unhealthy",
			qbitAPI:       true,
			webUI:         true,
			webDAVService: false,
			disableWebDav: false,
			want:          false,
		},
		{
			name:          "qbit down is unhealthy even with webdav disabled",
			qbitAPI:       false,
			webUI:         true,
			webDAVService: true,
			disableWebDav: true,
			want:          false,
		},
		{
			name:          "webui down is unhealthy even with webdav disabled",
			qbitAPI:       true,
			webUI:         false,
			webDAVService: true,
			disableWebDav: true,
			want:          false,
		},
		{
			name:          "default all up is healthy",
			qbitAPI:       true,
			webUI:         true,
			webDAVService: true,
			disableWebDav: false,
			want:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := HealthStatus{
				QbitAPI:       tt.qbitAPI,
				WebUI:         tt.webUI,
				WebDAVService: tt.webDAVService,
			}
			got := overallStatus(s, tt.disableWebDav)
			if got != tt.want {
				t.Errorf("overallStatus(%+v, disableWebDav=%v) = %v, want %v",
					s, tt.disableWebDav, got, tt.want)
			}
		})
	}
}
