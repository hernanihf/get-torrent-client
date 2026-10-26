package engine

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// TorrentStateEntry records persistent resume data for a torrent.
type TorrentStateEntry struct {
	ID          int          `json:"id"`
	InfoHashHex string       `json:"info_hash"`
	TorrentPath string       `json:"torrent_path"`
	MagnetURI   string       `json:"magnet_uri,omitempty"`
	State       TorrentState `json:"state"`
	Downloaded  int64        `json:"downloaded"`
	Uploaded    int64        `json:"uploaded"`
}

// EngineState captures global session settings and active torrent states.
type EngineState struct {
	DownloadDir    string              `json:"download_dir"`
	SpeedLimitDown int64               `json:"speed_limit_down"`
	SpeedLimitUp   int64               `json:"speed_limit_up"`
	Torrents       []TorrentStateEntry `json:"torrents"`
}

// SaveState serializes the current session and torrent states to state.json.
func (e *Engine) SaveState() error {
	if err := e.saveDHTState(); err != nil {
		log.Printf("[Engine] Saving DHT state: %v", err)
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.ConfigDir == "" {
		return nil
	}

	if err := os.MkdirAll(e.ConfigDir, 0755); err != nil {
		return fmt.Errorf("creating config dir %s: %w", e.ConfigDir, err)
	}

	state := EngineState{
		DownloadDir:    e.DownloadDir,
		SpeedLimitDown: e.SpeedLimitDown,
		SpeedLimitUp:   e.SpeedLimitUp,
		Torrents:       make([]TorrentStateEntry, 0, len(e.torrents)),
	}

	for _, t := range e.torrents {
		t.mu.RLock()
		state.Torrents = append(state.Torrents, TorrentStateEntry{
			ID:          t.ID,
			InfoHashHex: hex.EncodeToString(t.Meta.InfoHash[:]),
			TorrentPath: t.TorrentPath,
			MagnetURI:   t.MagnetURI,
			State:       t.State,
			Downloaded:  t.Downloaded,
			Uploaded:    t.Uploaded,
		})
		t.mu.RUnlock()
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("serializing engine state: %w", err)
	}

	stateFile := filepath.Join(e.ConfigDir, "state.json")
	tmpFile := stateFile + ".tmp"

	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return fmt.Errorf("writing state tmp file: %w", err)
	}

	if err := os.Rename(tmpFile, stateFile); err != nil {
		return fmt.Errorf("atomic rename of state file: %w", err)
	}

	return nil
}

// LoadState restores saved session settings and re-registers torrents from state.json.
func (e *Engine) LoadState() error {
	if e.ConfigDir == "" {
		return nil
	}

	stateFile := filepath.Join(e.ConfigDir, "state.json")
	data, err := os.ReadFile(stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No prior state to restore
		}
		return fmt.Errorf("reading state file %s: %w", stateFile, err)
	}

	var state EngineState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("decoding state file: %w", err)
	}

	if state.DownloadDir != "" {
		e.DownloadDir = state.DownloadDir
	}
	e.SpeedLimitDown = state.SpeedLimitDown
	e.SpeedLimitUp = state.SpeedLimitUp

	for _, entry := range state.Torrents {
		var t *ManagedTorrent
		switch {
		case entry.TorrentPath != "":
			// Magnets whose metadata was fetched have a saved .torrent and resume like files.
			t, err = e.AddTorrentFile(entry.TorrentPath)
			if err == nil && entry.MagnetURI != "" {
				t.mu.Lock()
				t.MagnetURI = entry.MagnetURI
				t.mu.Unlock()
			}
		case entry.MagnetURI != "":
			t, err = e.AddMagnet(entry.MagnetURI)
		default:
			continue
		}
		if err != nil {
			log.Printf("[Engine] Failed restoring torrent %s: %v", entry.InfoHashHex, err)
			continue
		}

		t.mu.Lock()
		t.Downloaded = entry.Downloaded
		t.Uploaded = entry.Uploaded
		if entry.State == StateDownloading || entry.State == StateSeeding {
			t.mu.Unlock()
			_ = e.StartTorrent(t.ID)
		} else {
			t.State = entry.State
			t.mu.Unlock()
		}
	}

	log.Printf("[Engine] Loaded state: %d torrents restored from %s", len(state.Torrents), stateFile)
	return nil
}
