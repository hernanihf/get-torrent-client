package engine

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hernan/get-torrent/pkg/bencode"
	"github.com/hernan/get-torrent/pkg/metainfo"
	"github.com/hernan/get-torrent/pkg/storage"
)

// buildMultiFileTorrent returns a torrent with the given file sizes (in a directory "multi")
// and the concatenated payload.
func buildMultiFileTorrent(t *testing.T, sizes []int, pieceLen int) (*metainfo.TorrentMeta, []byte) {
	t.Helper()
	var payload []byte
	var files []any
	for i, n := range sizes {
		data := make([]byte, n)
		_, _ = rand.Read(data)
		payload = append(payload, data...)
		files = append(files, map[string]any{"length": n, "path": []any{string(rune('a'+i)) + ".bin"}})
	}
	var pieces []byte
	for off := 0; off < len(payload); off += pieceLen {
		sum := sha1.Sum(payload[off:min(off+pieceLen, len(payload))])
		pieces = append(pieces, sum[:]...)
	}
	info, err := bencode.Encode(map[string]any{"name": "multi", "files": files, "piece length": pieceLen, "pieces": string(pieces)})
	if err != nil {
		t.Fatal(err)
	}
	meta, err := metainfo.FromInfoBytes(info, nil)
	if err != nil {
		t.Fatal(err)
	}
	return meta, payload
}

func TestPiecePriorities(t *testing.T) {
	// Files of 10, 25 and 5 bytes over 8-byte pieces: pieces 0-1 are a, 1-4 b, 4 c.
	meta, _ := buildMultiFileTorrent(t, []int{10, 25, 5}, 8)
	sel := FileSelection{Wanted: []bool{true, false, true}, Priority: []int8{storage.PriorityLow, 0, storage.PriorityHigh}}
	wanted, prio := piecePriorities(meta, sel)
	wantWanted := []bool{true, true, false, false, true}
	wantPrio := []int8{storage.PriorityLow, storage.PriorityLow, 0, 0, storage.PriorityHigh}
	for i := range wantWanted {
		if wanted[i] != wantWanted[i] || prio[i] != wantPrio[i] {
			t.Errorf("piece %d: wanted %v prio %d, want %v %d", i, wanted[i], prio[i], wantWanted[i], wantPrio[i])
		}
	}

	// A piece shared by wanted files takes the highest priority among them.
	sel = FileSelection{Wanted: []bool{true, true, true}, Priority: []int8{storage.PriorityHigh, storage.PriorityLow, storage.PriorityLow}}
	if _, prio := piecePriorities(meta, sel); prio[1] != storage.PriorityHigh || prio[2] != storage.PriorityLow {
		t.Errorf("shared piece priority: %v", prio)
	}

	if (FileSelection{Wanted: []bool{true}, Priority: []int8{5}}).valid(1) {
		t.Errorf("out-of-range priority must be invalid")
	}
}

func TestSetFileSelectionStateTransitions(t *testing.T) {
	meta, payload := buildMultiFileTorrent(t, []int{40000, 40000}, 16384)
	e := newTestEngine(t)
	data, _ := meta.Marshal()
	mt, err := e.AddTorrentBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	// Have only the pieces fully inside the first file (0 and 1).
	_, pm, _ := mt.components()
	for i := 0; i < 2; i++ {
		for b := 0; b < 16384; b += storage.BlockSize {
			off := i*16384 + b
			if _, err := pm.ReceiveBlock(i, b, payload[off:off+storage.BlockSize]); err != nil {
				t.Fatal(err)
			}
		}
	}
	mt.mu.Lock()
	mt.setStateLocked(StateDownloading) // as if running, without network activity
	mt.mu.Unlock()

	// Piece 2 straddles both files, so it stays wanted: not done yet.
	if err := e.SetFileSelection(mt.ID, func(fs *FileSelection) error { fs.Wanted[1] = false; return nil }); err != nil {
		t.Fatal(err)
	}
	st := mt.Status()
	if st.State != StateDownloading || st.SizeWhenDone != 3*16384 || st.Left != 16384 {
		t.Errorf("unexpected status after deselecting: %s size %d left %d", st.State, st.SizeWhenDone, st.Left)
	}

	// Finish the shared piece: the selected data is complete, so the torrent seeds.
	for b := 0; b < 16384; b += storage.BlockSize {
		off := 2*16384 + b
		if _, err := pm.ReceiveBlock(2, b, payload[off:off+storage.BlockSize]); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.SetFileSelection(mt.ID, func(*FileSelection) error { return nil }); err != nil {
		t.Fatal(err)
	}
	st = mt.Status()
	if st.State != StateSeeding || st.PercentDone != 1 || st.PercentComplete >= 1 {
		t.Errorf("expected a partial seed: %s done %.2f complete %.2f", st.State, st.PercentDone, st.PercentComplete)
	}

	// Selecting the second file again resumes downloading.
	if err := e.SetFileSelection(mt.ID, func(fs *FileSelection) error { fs.Wanted[1] = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if st := mt.Status(); st.State != StateDownloading || st.SizeWhenDone != meta.TotalLength {
		t.Errorf("expected downloading the whole torrent again: %s size %d", st.State, st.SizeWhenDone)
	}

	files := mt.Files()
	if !files[0].Wanted || !files[1].Wanted || files[0].BytesCompleted != 40000 {
		t.Errorf("unexpected files: %+v", files)
	}
}

func TestPartialDownloadSkipsUnwantedFiles(t *testing.T) {
	// a.bin spans pieces 0-2 and b.bin pieces 2-4 (piece 2 is shared).
	meta, payload := buildMultiFileTorrent(t, []int{40000, 40000}, 16384)

	seeder := newTestEngine(t)
	if err := seeder.StartListener(); err != nil {
		t.Fatal(err)
	}
	tr := fakeTracker(t, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: int(seeder.Port)})
	defer tr.Close()
	meta.Announce = tr.URL + "/announce"
	data, _ := meta.Marshal()
	st, err := seeder.AddTorrentBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	seedAll(t, st, payload)
	if err := seeder.StartTorrent(st.ID); err != nil {
		t.Fatal(err)
	}

	leecher := newTestEngine(t)
	lt, err := leecher.AddTorrentBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := leecher.SetFileSelection(lt.ID, func(fs *FileSelection) error { fs.Wanted[1] = false; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := leecher.StartTorrent(lt.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "partial download", func() bool { return lt.Status().State == StateSeeding })
	time.Sleep(300 * time.Millisecond) // nothing more may arrive

	_, pm, _ := lt.components()
	for i := 0; i < 5; i++ {
		if want := i <= 2; pm.HasPiece(i) != want {
			t.Errorf("piece %d: have %v, want %v", i, pm.HasPiece(i), want)
		}
	}
	got, err := os.ReadFile(filepath.Join(leecher.DownloadDir, "multi", "a.bin"))
	if err != nil || !bytes.Equal(got, payload[:40000]) {
		t.Fatalf("wanted file mismatch (err=%v)", err)
	}
}

func TestFileSelectionPersists(t *testing.T) {
	meta, _ := buildMultiFileTorrent(t, []int{1000, 1000, 1000}, 16384)
	cfg, dl := t.TempDir(), t.TempDir()
	e := NewEngine(dl, 0)
	e.ConfigDir = cfg
	data, _ := meta.Marshal()
	mt, err := e.AddTorrentBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SetFileSelection(mt.ID, func(fs *FileSelection) error {
		fs.Wanted[0], fs.Priority[2] = false, storage.PriorityHigh
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.SaveState(); err != nil {
		t.Fatal(err)
	}
	_ = e.Close()

	e2 := NewEngine(dl, 0)
	e2.ConfigDir = cfg
	t.Cleanup(func() { _ = e2.Close() })
	if err := e2.LoadState(); err != nil {
		t.Fatal(err)
	}
	sel := e2.GetTorrentByID(mt.ID).FileSelection()
	if len(sel.Wanted) != 3 || sel.Wanted[0] || !sel.Wanted[1] || sel.Priority[2] != storage.PriorityHigh {
		t.Errorf("selection not restored: %+v", sel)
	}
}
