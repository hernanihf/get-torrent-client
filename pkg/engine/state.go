package engine

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/hernan/get-torrent/pkg/metainfo"
	"github.com/hernan/get-torrent/pkg/storage"
)

// TorrentStateEntry records persistent resume data for a torrent.
type TorrentStateEntry struct {
	ID          int            `json:"id"`
	InfoHashHex string         `json:"info_hash"`
	TorrentPath string         `json:"torrent_path"`
	MagnetURI   string         `json:"magnet_uri,omitempty"`
	State       TorrentState   `json:"state"`
	Downloaded  int64          `json:"downloaded"`
	Uploaded    int64          `json:"uploaded"`
	CorruptEver int64          `json:"corrupt,omitempty"`
	DownloadDir string         `json:"download_dir,omitempty"`
	AddedDate   int64          `json:"added_date,omitempty"`
	DoneDate    int64          `json:"done_date,omitempty"`
	Limits      *TorrentLimits `json:"limits,omitempty"`
	// Per-file selection; absent means every file wanted at normal priority.
	FilesWanted  []bool `json:"files_wanted,omitempty"`
	FilePriority []int8 `json:"file_priority,omitempty"`

	// Pieces is the verified-piece bitfield and Files the stamps of the data files it describes.
	// On load the bitfield is trusted only if every file still matches its stamp; otherwise the
	// data is rehashed. Both are omitted while a check is running, forcing a recheck.
	Pieces []byte              `json:"pieces,omitempty"`
	Files  []storage.FileStamp `json:"files,omitempty"`
}

// EngineState captures global session settings and active torrent states.
type EngineState struct {
	DownloadDir    string `json:"download_dir"`
	SpeedLimitDown int64  `json:"speed_limit_down"`
	SpeedLimitUp   int64  `json:"speed_limit_up"`
	SpeedDownOn    bool   `json:"speed_limit_down_enabled,omitempty"`
	SpeedUpOn      bool   `json:"speed_limit_up_enabled,omitempty"`
	AltSpeedDown   int64  `json:"alt_speed_down"`
	AltSpeedUp     int64  `json:"alt_speed_up"`
	AltSpeedOn     bool   `json:"alt_speed_enabled,omitempty"`
	// Pointers: absent in older state files, which keep the defaults.
	SeedRatio     *float64            `json:"seed_ratio_limit,omitempty"`
	SeedRatioOn   bool                `json:"seed_ratio_limited,omitempty"`
	IdleSeeding   *int                `json:"idle_seeding_limit,omitempty"`
	IdleSeedingOn bool                `json:"idle_seeding_limit_enabled,omitempty"`
	PEXEnabled    *bool               `json:"pex_enabled,omitempty"`
	Stats         CumulativeStats     `json:"cumulative_stats"`
	Torrents      []TorrentStateEntry `json:"torrents"`
}

// SaveState serializes the current session and torrent states to state.json.
func (e *Engine) SaveState() error {
	e.saveMu.Lock()
	defer e.saveMu.Unlock()

	if err := e.saveDHTState(); err != nil {
		log.Printf("[Engine] Saving DHT state: %v", err)
	}

	_, cumulative := e.SessionStats()

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
		SpeedDownOn:    e.SpeedLimitDownEnabled,
		SpeedUpOn:      e.SpeedLimitUpEnabled,
		AltSpeedDown:   e.AltSpeedDown,
		AltSpeedUp:     e.AltSpeedUp,
		AltSpeedOn:     e.AltSpeedEnabled,
		SeedRatio:      &e.SeedRatioLimit,
		SeedRatioOn:    e.SeedRatioLimited,
		IdleSeeding:    &e.IdleSeedingLimit,
		IdleSeedingOn:  e.IdleSeedingLimitEnabled,
		PEXEnabled:     &e.PEXEnabled,
		Stats:          cumulative,
		Torrents:       make([]TorrentStateEntry, 0, len(e.torrents)),
	}

	for _, t := range e.torrents {
		t.mu.RLock()
		limits := t.limits
		pm, store := t.PieceManager, t.Storage
		saved := t.State
		if saved == StateChecking {
			// Persist what the torrent will do once the check finishes; it is rechecked on load.
			saved = StateStopped
			if t.restartAfter {
				saved = StateDownloading
			}
		}
		entry := TorrentStateEntry{
			ID:          t.ID,
			InfoHashHex: hex.EncodeToString(t.Meta.InfoHash[:]),
			TorrentPath: t.TorrentPath,
			MagnetURI:   t.MagnetURI,
			State:       saved,
			Downloaded:  t.Downloaded,
			Uploaded:    t.Uploaded,
			CorruptEver: t.CorruptEver,
			DownloadDir: t.DownloadDir,
			AddedDate:   unix(t.AddedDate),
			DoneDate:    unix(t.DoneDate),
			Limits:      &limits,
		}
		if t.files.valid(len(t.Meta.Files)) {
			entry.FilesWanted, entry.FilePriority = t.files.Wanted, t.files.Priority
		}
		if pm != nil && t.State != StateChecking {
			// Stamp the files after snapshotting the bitfield: a piece is set only once it is
			// written, so any write racing with the save leaves a newer mtime and a recheck.
			entry.Pieces = pm.Snapshot().Have
			if files, err := store.FileStamps(); err == nil {
				entry.Files = files
			} else {
				entry.Pieces = nil
			}
		}
		t.mu.RUnlock()
		state.Torrents = append(state.Torrents, entry)
	}

	// Restore in ID order so torrents keep their relative order across restarts.
	sort.Slice(state.Torrents, func(i, j int) bool { return state.Torrents[i].ID < state.Torrents[j].ID })

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

// StartAutoSave saves state every interval until Close, so a crash loses at most that much
// session history and resume data. A non-positive interval disables it.
func (e *Engine) StartAutoSave(interval time.Duration) {
	if interval <= 0 {
		return
	}
	e.bg.Add(1)
	go func() {
		defer e.bg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-e.ctx.Done():
				return
			case <-ticker.C:
				if e.ctx.Err() != nil {
					return
				}
				if err := e.SaveState(); err != nil {
					log.Printf("[Engine] Periodic state save failed: %v", err)
				}
			}
		}
	}()
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
	e.mu.Lock()
	e.SpeedLimitDown = state.SpeedLimitDown
	e.SpeedLimitUp = state.SpeedLimitUp
	e.SpeedLimitDownEnabled = state.SpeedDownOn
	e.SpeedLimitUpEnabled = state.SpeedUpOn
	if state.AltSpeedDown > 0 || state.AltSpeedUp > 0 {
		e.AltSpeedDown, e.AltSpeedUp = state.AltSpeedDown, state.AltSpeedUp
	}
	e.AltSpeedEnabled = state.AltSpeedOn
	if state.SeedRatio != nil {
		e.SeedRatioLimit = *state.SeedRatio
	}
	if state.IdleSeeding != nil {
		e.IdleSeedingLimit = *state.IdleSeeding
	}
	e.SeedRatioLimited, e.IdleSeedingLimitEnabled = state.SeedRatioOn, state.IdleSeedingOn
	if state.PEXEnabled != nil {
		e.PEXEnabled = *state.PEXEnabled
	}
	e.applyLimitsLocked()
	e.cumulative = state.Stats
	e.mu.Unlock()

	for _, entry := range state.Torrents {
		var t *ManagedTorrent
		switch {
		case entry.TorrentPath != "":
			// Magnets whose metadata was fetched have a saved .torrent and resume like files.
			var meta *metainfo.TorrentMeta
			if meta, err = metainfo.Open(entry.TorrentPath); err == nil {
				t, err = e.addMeta(meta, entry.TorrentPath, entry.DownloadDir)
			}
			if err == nil && entry.MagnetURI != "" {
				t.mu.Lock()
				t.MagnetURI = entry.MagnetURI
				t.mu.Unlock()
			}
		case entry.MagnetURI != "":
			t, err = e.addMagnet(entry.MagnetURI, entry.DownloadDir)
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
		t.CorruptEver = entry.CorruptEver
		if entry.AddedDate > 0 {
			t.AddedDate = time.Unix(entry.AddedDate, 0)
		}
		if entry.DoneDate > 0 {
			t.DoneDate = time.Unix(entry.DoneDate, 0)
		}
		if entry.Limits != nil {
			t.limits = *entry.Limits
			t.applyLimits(t.limits)
		}
		if sel := (FileSelection{Wanted: entry.FilesWanted, Priority: entry.FilePriority}); t.PieceManager != nil && sel.valid(len(t.Meta.Files)) {
			t.files = sel
			t.PieceManager.SetPiecePriorities(piecePriorities(t.Meta, sel))
			t.Left = t.PieceManager.Left()
		}
		run := entry.State == StateDownloading || entry.State == StateSeeding
		if pm, store := t.PieceManager, t.Storage; pm != nil {
			if !resumeValid(entry, len(t.Meta.Pieces), store) {
				// No trustworthy resume data: rehash the data on disk before resuming instead of
				// downloading everything again, like Transmission does.
				t.mu.Unlock()
				e.startVerify(t, run)
				continue
			}
			pm.SetVerified(entry.Pieces)
			t.Left = pm.Left()
		}
		switch {
		case run:
			t.mu.Unlock()
			_ = e.startTorrent(t)
		case entry.State == StateChecking:
			t.State = StateStopped // interrupted verification (older state files)
			t.mu.Unlock()
		default:
			t.State = entry.State
			t.mu.Unlock()
		}
	}

	log.Printf("[Engine] Loaded state: %d torrents restored from %s", len(state.Torrents), stateFile)
	return nil
}

// resumeValid reports whether a saved bitfield can be trusted: it must fit the torrent and every
// data file must still have the size and modification time it had when the state was saved.
func resumeValid(entry TorrentStateEntry, numPieces int, store *storage.Storage) bool {
	if len(entry.Pieces) != (numPieces+7)/8 {
		return false
	}
	if spare := numPieces % 8; spare != 0 && entry.Pieces[len(entry.Pieces)-1]&(0xFF>>spare) != 0 {
		return false // bits set past the last piece
	}
	current, err := store.FileStamps()
	if err != nil || len(current) != len(entry.Files) {
		return false
	}
	for i := range current {
		if current[i] != entry.Files[i] {
			return false
		}
	}
	return true
}

// Settings are the mutable session-wide options exposed through RPC session-get/session-set.
type Settings struct {
	DownloadDir           string
	SpeedLimitDown        int64 // KB/s
	SpeedLimitUp          int64 // KB/s
	SpeedLimitDownEnabled bool
	SpeedLimitUpEnabled   bool
	AltSpeedDown          int64 // KB/s
	AltSpeedUp            int64 // KB/s
	AltSpeedEnabled       bool

	SeedRatioLimit          float64
	SeedRatioLimited        bool
	IdleSeedingLimit        int // minutes
	IdleSeedingLimitEnabled bool

	PEXEnabled bool
}

// Settings returns a consistent copy of the session settings.
func (e *Engine) Settings() Settings {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.settingsLocked()
}

func (e *Engine) settingsLocked() Settings {
	return Settings{
		DownloadDir:           e.DownloadDir,
		SpeedLimitDown:        e.SpeedLimitDown,
		SpeedLimitUp:          e.SpeedLimitUp,
		SpeedLimitDownEnabled: e.SpeedLimitDownEnabled,
		SpeedLimitUpEnabled:   e.SpeedLimitUpEnabled,
		AltSpeedDown:          e.AltSpeedDown,
		AltSpeedUp:            e.AltSpeedUp,
		AltSpeedEnabled:       e.AltSpeedEnabled,

		SeedRatioLimit:          e.SeedRatioLimit,
		SeedRatioLimited:        e.SeedRatioLimited,
		IdleSeedingLimit:        e.IdleSeedingLimit,
		IdleSeedingLimitEnabled: e.IdleSeedingLimitEnabled,

		PEXEnabled: e.PEXEnabled,
	}
}

// UpdateSettings applies fn to the session settings atomically; speed limits apply immediately.
func (e *Engine) UpdateSettings(fn func(*Settings)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	st := e.settingsLocked()
	fn(&st)
	e.DownloadDir = st.DownloadDir
	e.SpeedLimitDown, e.SpeedLimitUp = st.SpeedLimitDown, st.SpeedLimitUp
	e.SpeedLimitDownEnabled, e.SpeedLimitUpEnabled = st.SpeedLimitDownEnabled, st.SpeedLimitUpEnabled
	e.AltSpeedDown, e.AltSpeedUp, e.AltSpeedEnabled = st.AltSpeedDown, st.AltSpeedUp, st.AltSpeedEnabled
	e.SeedRatioLimit, e.SeedRatioLimited = st.SeedRatioLimit, st.SeedRatioLimited
	e.IdleSeedingLimit, e.IdleSeedingLimitEnabled = st.IdleSeedingLimit, st.IdleSeedingLimitEnabled
	e.PEXEnabled = st.PEXEnabled
	e.applyLimitsLocked()
}
