package engine

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStateSaveAndLoad(t *testing.T) {
	tempConfigDir, err := os.MkdirTemp("", "get_torrent_state_*")
	if err != nil {
		t.Fatalf("creating temp config dir: %v", err)
	}
	defer os.RemoveAll(tempConfigDir)

	eng1 := NewEngine("/initial/downloads", 6881)
	eng1.ConfigDir = tempConfigDir
	eng1.SpeedLimitDown = 250000
	eng1.SpeedLimitUp = 100000

	// Save state
	if err := eng1.SaveState(); err != nil {
		t.Fatalf("SaveState failed: %v", err)
	}

	stateFile := filepath.Join(tempConfigDir, "state.json")
	if _, err := os.Stat(stateFile); err != nil {
		t.Fatalf("state.json was not created: %v", err)
	}

	// Create a new engine instance and restore
	eng2 := NewEngine("/default/downloads", 6881)
	eng2.ConfigDir = tempConfigDir

	if err := eng2.LoadState(); err != nil {
		t.Fatalf("LoadState failed: %v", err)
	}

	if eng2.DownloadDir != "/initial/downloads" {
		t.Errorf("expected restored download dir '/initial/downloads', got %q", eng2.DownloadDir)
	}
	if eng2.SpeedLimitDown != 250000 {
		t.Errorf("expected restored speed limit down 250000, got %d", eng2.SpeedLimitDown)
	}
	if eng2.SpeedLimitUp != 100000 {
		t.Errorf("expected restored speed limit up 100000, got %d", eng2.SpeedLimitUp)
	}
}
