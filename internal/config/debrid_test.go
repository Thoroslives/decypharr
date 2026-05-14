package config

import (
	"encoding/json"
	"testing"
)

// TestDebridConfigIgnoresVestigialWorkersField guards G1: the Workers field
// was set during config migration to runtime.NumCPU()*50/num_debrids but
// never consumed anywhere in the v2.3 source. We remove it; existing
// config.json files containing "workers": 1600 must still parse cleanly
// (the field is ignored, no unmarshal error).
func TestDebridConfigIgnoresVestigialWorkersField(t *testing.T) {
	raw := `{
		"name": "realdebrid",
		"api_key": "test",
		"workers": 1600
	}`
	var d Debrid
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		t.Fatalf("expected legacy config to parse cleanly, got %v", err)
	}
}
