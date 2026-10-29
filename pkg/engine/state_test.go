package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hernan/get-torrent/pkg/metainfo"
	"github.com/hernan/get-torrent/pkg/storage"
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

// restartWithData saves a torrent in the given state, then writes onDisk over its data file
// (changing its mtime, as an external edit would) and restores it into a fresh engine, which
// must distrust the saved bitfield and recheck the data before resuming.
func restartWithData(t *testing.T, state TorrentState, onDisk []byte, payload []byte) *Engine {
	t.Helper()
	configDir, downloadDir := t.TempDir(), t.TempDir()
	// An unreachable tracker keeps a resumed torrent running (retrying) instead of erroring out.
	meta, _ := metainfo.FromInfoBytes(buildTorrent(t, "r.bin", payload, 16384), []string{"udp://127.0.0.1:1/announce"})
	raw, _ := meta.Marshal()

	first := NewEngine(downloadDir, 0)
	first.ConfigDir = configDir
	mt, err := first.AddTorrentBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	mt.mu.Lock()
	mt.State = state
	mt.mu.Unlock()
	if err := first.SaveState(); err != nil {
		t.Fatal(err)
	}
	_ = first.Close()

	path := filepath.Join(downloadDir, "r.bin")
	if err := os.WriteFile(path, onDisk, 0644); err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(time.Hour)
	if err := os.Chtimes(path, later, later); err != nil {
		t.Fatal(err)
	}

	second := NewEngine(t.TempDir(), 0)
	second.ConfigDir = configDir
	t.Cleanup(func() { _ = second.Close() })
	if err := second.LoadState(); err != nil {
		t.Fatal(err)
	}
	return second
}

func TestLoadStateRechecksCompleteData(t *testing.T) {
	payload := make([]byte, 50000) // 4 pieces
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	eng := restartWithData(t, StateSeeding, payload, payload)
	mt := eng.ListTorrents()[0]

	waitFor(t, 5*time.Second, "startup recheck", func() bool { return mt.Status().State == StateSeeding })
	if s := mt.Status(); s.Left != 0 || s.PercentDone != 1 || s.HaveValid != int64(len(payload)) {
		t.Errorf("complete data must verify on startup: %+v", s)
	}
}

func TestLoadStateRechecksPartialData(t *testing.T) {
	payload := make([]byte, 50000)
	for i := range payload {
		payload[i] = byte(i * 13)
	}
	corrupt := append([]byte(nil), payload...)
	corrupt[2*16384+5] ^= 0xFF // third piece fails its hash
	eng := restartWithData(t, StateStopped, corrupt, payload)
	mt := eng.ListTorrents()[0]

	waitFor(t, 5*time.Second, "startup recheck", func() bool { return mt.Status().State == StateStopped })
	if s := mt.Status(); s.Left != 16384 || s.HaveValid != int64(len(payload)-16384) {
		t.Errorf("expected 3 of 4 pieces verified: %+v", s)
	}
	if mt.PieceManager.HasPiece(2) || !mt.PieceManager.HasPiece(3) {
		t.Errorf("unexpected verified pieces")
	}
}

func TestSaveStateDuringCheckKeepsRunIntent(t *testing.T) {
	eng := NewEngine(t.TempDir(), 0)
	eng.ConfigDir = t.TempDir()
	defer eng.Close()
	meta, _ := metainfo.FromInfoBytes(buildTorrent(t, "c.bin", make([]byte, 1000), 16384), nil)
	raw, _ := meta.Marshal()
	mt, err := eng.AddTorrentBytes(raw)
	if err != nil {
		t.Fatal(err)
	}

	for _, restart := range []bool{true, false} {
		mt.mu.Lock()
		mt.State, mt.restartAfter = StateChecking, restart
		mt.mu.Unlock()
		if err := eng.SaveState(); err != nil {
			t.Fatal(err)
		}
		data, _ := os.ReadFile(filepath.Join(eng.ConfigDir, "state.json"))
		var st EngineState
		if err := json.Unmarshal(data, &st); err != nil {
			t.Fatal(err)
		}
		want := StateStopped
		if restart {
			want = StateDownloading
		}
		if got := st.Torrents[0].State; got != want {
			t.Errorf("restartAfter=%v: saved state %q, want %q", restart, got, want)
		}
	}
	mt.mu.Lock()
	mt.State = StateStopped
	mt.mu.Unlock()
}

func TestLoadStateTrustsMatchingResumeData(t *testing.T) {
	configDir, downloadDir := t.TempDir(), t.TempDir()
	payload := make([]byte, 50000) // 4 pieces
	for i := range payload {
		payload[i] = byte(i * 11)
	}
	corrupt := append([]byte(nil), payload...)
	corrupt[2*16384+5] ^= 0xFF
	path := filepath.Join(downloadDir, "m.bin")
	if err := os.WriteFile(path, corrupt, 0644); err != nil {
		t.Fatal(err)
	}
	meta, _ := metainfo.FromInfoBytes(buildTorrent(t, "m.bin", payload, 16384), nil)
	raw, _ := meta.Marshal()

	first := NewEngine(downloadDir, 0)
	first.ConfigDir = configDir
	mt, err := first.AddTorrentBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.VerifyTorrent(mt.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "verification", func() bool { return mt.Status().State == StateStopped })
	if err := first.SaveState(); err != nil {
		t.Fatal(err)
	}
	_ = first.Close()

	// Repair the bad piece but keep size and mtime: a fresh engine must trust the saved bitfield
	// without hashing, so piece 2 stays missing.
	info, _ := os.Stat(path)
	if err := os.WriteFile(path, payload, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}

	second := NewEngine(t.TempDir(), 0)
	second.ConfigDir = configDir
	defer second.Close()
	if err := second.LoadState(); err != nil {
		t.Fatal(err)
	}
	restored := second.ListTorrents()[0]
	s := restored.Status()
	if s.State != StateStopped || s.Left != 16384 || s.HaveValid != int64(len(payload)-16384) {
		t.Errorf("saved bitfield must be restored without a recheck: %+v", s)
	}
	if restored.PieceManager.HasPiece(2) || !restored.PieceManager.HasPiece(3) {
		t.Errorf("unexpected restored pieces")
	}
}

func TestResumeValid(t *testing.T) {
	dir := t.TempDir()
	meta, _ := metainfo.FromInfoBytes(buildTorrent(t, "v.bin", make([]byte, 50000), 16384), nil) // 4 pieces
	store, err := storage.NewStorage(dir, meta)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stamps, err := store.FileStamps()
	if err != nil {
		t.Fatal(err)
	}

	ok := TorrentStateEntry{Pieces: []byte{0xD0}, Files: stamps}
	if !resumeValid(ok, 4, store) {
		t.Errorf("matching resume data must be valid")
	}
	cases := map[string]TorrentStateEntry{
		"no bitfield":    {Files: stamps},
		"wrong length":   {Pieces: []byte{0xD0, 0}, Files: stamps},
		"spare bits set": {Pieces: []byte{0xD8}, Files: stamps},
		"no stamps":      {Pieces: []byte{0xD0}},
		"size changed":   {Pieces: []byte{0xD0}, Files: []storage.FileStamp{{Size: 1, ModTime: stamps[0].ModTime}}},
		"mtime changed":  {Pieces: []byte{0xD0}, Files: []storage.FileStamp{{Size: stamps[0].Size, ModTime: stamps[0].ModTime + 1}}},
	}
	for name, entry := range cases {
		if resumeValid(entry, 4, store) {
			t.Errorf("%s: resume data must be rejected", name)
		}
	}
}

func TestSaveStateOmitsBitfieldWhileChecking(t *testing.T) {
	eng := NewEngine(t.TempDir(), 0)
	eng.ConfigDir = t.TempDir()
	defer eng.Close()
	meta, _ := metainfo.FromInfoBytes(buildTorrent(t, "k.bin", make([]byte, 1000), 16384), nil)
	raw, _ := meta.Marshal()
	mt, err := eng.AddTorrentBytes(raw)
	if err != nil {
		t.Fatal(err)
	}

	saved := func() TorrentStateEntry {
		if err := eng.SaveState(); err != nil {
			t.Fatal(err)
		}
		data, _ := os.ReadFile(filepath.Join(eng.ConfigDir, "state.json"))
		var st EngineState
		if err := json.Unmarshal(data, &st); err != nil {
			t.Fatal(err)
		}
		return st.Torrents[0]
	}
	if e := saved(); len(e.Pieces) != 1 || len(e.Files) != 1 {
		t.Errorf("bitfield and stamps must be saved: %+v", e)
	}
	mt.mu.Lock()
	mt.State = StateChecking
	mt.mu.Unlock()
	if e := saved(); e.Pieces != nil || e.Files != nil {
		t.Errorf("bitfield must be omitted while checking: %+v", e)
	}
	mt.mu.Lock()
	mt.State = StateStopped
	mt.mu.Unlock()
}

func TestAutoSave(t *testing.T) {
	eng := NewEngine(t.TempDir(), 0)
	eng.ConfigDir = t.TempDir()
	stateFile := filepath.Join(eng.ConfigDir, "state.json")
	eng.StartAutoSave(20 * time.Millisecond)

	waitFor(t, 2*time.Second, "periodic save", func() bool {
		_, err := os.Stat(stateFile)
		return err == nil
	})

	// Close waits for the loop, so no save happens afterwards.
	_ = eng.Close()
	if err := os.Remove(stateFile); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if _, err := os.Stat(stateFile); !os.IsNotExist(err) {
		t.Errorf("state saved after Close: %v", err)
	}
}

func TestConcurrentSaveState(t *testing.T) {
	eng := NewEngine(t.TempDir(), 0)
	eng.ConfigDir = t.TempDir()
	defer eng.Close()
	meta, _ := metainfo.FromInfoBytes(buildTorrent(t, "s.bin", make([]byte, 1000), 16384), nil)
	raw, _ := meta.Marshal()
	if _, err := eng.AddTorrentBytes(raw); err != nil {
		t.Fatal(err)
	}

	errs := make(chan error, 8)
	for range 8 {
		go func() { errs <- eng.SaveState() }()
	}
	for range 8 {
		if err := <-errs; err != nil {
			t.Errorf("concurrent save: %v", err)
		}
	}
}
