package config

import (
	"encoding/json"
	"testing"
)

// TestSyncExistingTorrentsResolvedDefaultsTrue guards G4: when the
// sync_existing_torrents field is absent from config.json, the resolver
// must return true so existing installations keep performing the initial
// sync of torrents from debrid clients on startup.
func TestSyncExistingTorrentsResolvedDefaultsTrue(t *testing.T) {
	var c Config
	if !c.SyncExistingTorrentsResolved() {
		t.Error("SyncExistingTorrentsResolved must default to true when field is unset")
	}
}

// TestSyncExistingTorrentsResolvedRespectsExplicitFalse guards G4: when
// the user explicitly sets sync_existing_torrents to false in config.json,
// the resolver must return false so the manager skips the initial sync.
func TestSyncExistingTorrentsResolvedRespectsExplicitFalse(t *testing.T) {
	raw := []byte(`{"sync_existing_torrents": false}`)
	var c Config
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if c.SyncExistingTorrentsResolved() {
		t.Error("SyncExistingTorrentsResolved must return false when explicitly set to false")
	}
}

// TestSyncExistingTorrentsResolvedRespectsExplicitTrue guards G4: explicit
// true must round-trip through the resolver. Belt-and-braces alongside the
// default-true test.
func TestSyncExistingTorrentsResolvedRespectsExplicitTrue(t *testing.T) {
	raw := []byte(`{"sync_existing_torrents": true}`)
	var c Config
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !c.SyncExistingTorrentsResolved() {
		t.Error("SyncExistingTorrentsResolved must return true when explicitly set to true")
	}
}
