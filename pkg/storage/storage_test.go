package storage

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hernan/get-torrent/pkg/metainfo"
	"github.com/hernan/get-torrent/pkg/peer"
)

func TestPieceBlockAccumulationAndHash(t *testing.T) {
	pieceLen := 32768 // 2 blocks of 16 KiB
	data := make([]byte, pieceLen)
	_, _ = rand.Read(data)
	hash := sha1.Sum(data)

	piece := NewPiece(0, pieceLen, hash)

	// Block 1 (0 to 16384)
	complete, err := piece.AddBlock(0, data[0:16384])
	if err != nil {
		t.Fatalf("unexpected error adding block 1: %v", err)
	}
	if complete {
		t.Fatalf("piece should not be complete after 1 block")
	}

	// Block 2 (16384 to 32768)
	complete, err = piece.AddBlock(16384, data[16384:32768])
	if err != nil {
		t.Fatalf("unexpected error adding block 2: %v", err)
	}
	if !complete {
		t.Fatalf("piece should be complete after receiving all blocks")
	}

	if !piece.IsComplete {
		t.Errorf("expected piece.IsComplete to be true")
	}
}

func TestPieceCorruptedHash(t *testing.T) {
	pieceLen := 16384
	data := make([]byte, pieceLen)
	_, _ = rand.Read(data)
	hash := sha1.Sum(data)

	// Modify data to simulate corrupt download
	corruptData := make([]byte, pieceLen)
	copy(corruptData, data)
	corruptData[0] ^= 0xFF

	piece := NewPiece(0, pieceLen, hash)
	complete, err := piece.AddBlock(0, corruptData)
	if err == nil {
		t.Fatalf("expected error on corrupted piece, got nil")
	}
	if complete {
		t.Fatalf("corrupted piece marked as complete")
	}
	if piece.BlocksReceived != 0 {
		t.Errorf("piece should reset blocks received on hash failure")
	}
}

func TestSingleFileStorage(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "get_torrent_single_*")
	if err != nil {
		t.Fatalf("creating temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	pieceLen := int64(16384)
	totalLen := int64(32768) // 2 pieces

	meta := &metainfo.TorrentMeta{
		Name:        "test_file.bin",
		PieceLength: pieceLen,
		TotalLength: totalLen,
		Pieces:      make([][20]byte, 2),
		Files: []metainfo.FileInfo{
			{Length: totalLen, Path: []string{"test_file.bin"}},
		},
	}

	store, err := NewStorage(tempDir, meta)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}
	defer store.Close()

	p0Data := make([]byte, pieceLen)
	_, _ = rand.Read(p0Data)

	if err := store.WritePiece(0, p0Data); err != nil {
		t.Fatalf("WritePiece 0 failed: %v", err)
	}

	readData, err := store.ReadPiece(0, int(pieceLen))
	if err != nil {
		t.Fatalf("ReadPiece 0 failed: %v", err)
	}

	if !bytes.Equal(p0Data, readData) {
		t.Errorf("data read from disk does not match written piece")
	}
}

func TestMultiFileStorageSpanningBoundary(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "get_torrent_multi_*")
	if err != nil {
		t.Fatalf("creating temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// File 1 is 10000 bytes. File 2 is 20000 bytes.
	// Piece length is 16384 bytes.
	// Piece 0 spans: 10000 bytes in file 1, and 6384 bytes in file 2!
	meta := &metainfo.TorrentMeta{
		Name:        "my_album",
		PieceLength: 16384,
		TotalLength: 30000,
		Pieces:      make([][20]byte, 2),
		Files: []metainfo.FileInfo{
			{Length: 10000, Path: []string{"song1.mp3"}},
			{Length: 20000, Path: []string{"song2.mp3"}},
		},
	}

	store, err := NewStorage(tempDir, meta)
	if err != nil {
		t.Fatalf("NewStorage multi-file failed: %v", err)
	}
	defer store.Close()

	// Verify directory structure created: tempDir/my_album/song1.mp3 and song2.mp3
	path1 := filepath.Join(tempDir, "my_album", "song1.mp3")
	path2 := filepath.Join(tempDir, "my_album", "song2.mp3")

	if _, err := os.Stat(path1); err != nil {
		t.Fatalf("file 1 was not created: %v", err)
	}
	if _, err := os.Stat(path2); err != nil {
		t.Fatalf("file 2 was not created: %v", err)
	}

	p0Data := make([]byte, 16384)
	_, _ = rand.Read(p0Data)

	if err := store.WritePiece(0, p0Data); err != nil {
		t.Fatalf("WritePiece across files failed: %v", err)
	}

	// Verify bytes read back across both files
	readP0, err := store.ReadPiece(0, 16384)
	if err != nil {
		t.Fatalf("ReadPiece across files failed: %v", err)
	}

	if !bytes.Equal(p0Data, readP0) {
		t.Errorf("multi-file piece read mismatch")
	}
}

func TestPieceManagerWorkflow(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "get_torrent_pm_*")
	if err != nil {
		t.Fatalf("creating temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	p0Data := make([]byte, 16384)
	_, _ = rand.Read(p0Data)
	hash0 := sha1.Sum(p0Data)

	meta := &metainfo.TorrentMeta{
		Name:        "single.iso",
		PieceLength: 16384,
		TotalLength: 16384,
		Pieces:      [][20]byte{hash0},
		Files: []metainfo.FileInfo{
			{Length: 16384, Path: []string{"single.iso"}},
		},
	}

	store, err := NewStorage(tempDir, meta)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}
	defer store.Close()

	pm := NewPieceManager(meta, store)

	// Peer has piece 0
	peerBf := make(peer.Bitfield, 1)
	peerBf.SetPiece(0)

	// Request next block
	req, ok := pm.NextRequest(peerBf)
	if !ok || req == nil {
		t.Fatalf("expected block request, got none")
	}

	if req.PieceIndex != 0 || req.Begin != 0 || req.Length != 16384 {
		t.Errorf("unexpected request params: %+v", req)
	}

	// Receive block
	completed, err := pm.ReceiveBlock(req.PieceIndex, req.Begin, p0Data)
	if err != nil {
		t.Fatalf("ReceiveBlock failed: %v", err)
	}
	if !completed {
		t.Errorf("expected piece to be completed")
	}

	if !pm.IsComplete() {
		t.Errorf("expected PieceManager to be complete")
	}

	percent, downloaded := pm.Progress()
	if percent != 100.0 || downloaded != 16384 {
		t.Errorf("unexpected progress: percent %f, downloaded %d", percent, downloaded)
	}
}

func TestRarestFirstOrdering(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "get_torrent_rarest_*")
	if err != nil {
		t.Fatalf("creating temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	meta := &metainfo.TorrentMeta{
		Name:        "test.iso",
		PieceLength: 16384,
		TotalLength: 16384 * 3,
		Pieces:      make([][20]byte, 3),
		Files: []metainfo.FileInfo{
			{Length: 16384 * 3, Path: []string{"test.iso"}},
		},
	}

	store, err := NewStorage(tempDir, meta)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}
	defer store.Close()

	pm := NewPieceManager(meta, store)

	// Simulate peer availability:
	// Piece 0: available on 5 peers
	// Piece 1: available on 1 peer (Rarest!)
	// Piece 2: available on 3 peers
	for i := 0; i < 5; i++ {
		pm.OnPeerHave(0)
	}
	pm.OnPeerHave(1)
	for i := 0; i < 3; i++ {
		pm.OnPeerHave(2)
	}

	// Peer has pieces 0, 1, 2
	peerBf := make(peer.Bitfield, 1)
	peerBf.SetPiece(0)
	peerBf.SetPiece(1)
	peerBf.SetPiece(2)

	order := pm.RarestPieceOrder(peerBf)
	if len(order) != 3 {
		t.Fatalf("expected 3 pieces in order, got %d", len(order))
	}

	// Rarest first order should be: Piece 1 (avail 1), Piece 2 (avail 3), Piece 0 (avail 5)
	if order[0] != 1 || order[1] != 2 || order[2] != 0 {
		t.Errorf("expected rarest-first order [1, 2, 0], got %v", order)
	}
}

func TestEndgameAndCancelTracking(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "get_torrent_endgame_*")
	if err != nil {
		t.Fatalf("creating temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	meta := &metainfo.TorrentMeta{
		Name:        "endgame.iso",
		PieceLength: 16384,
		TotalLength: 16384,
		Pieces:      make([][20]byte, 1),
		Files: []metainfo.FileInfo{
			{Length: 16384, Path: []string{"endgame.iso"}},
		},
	}

	store, err := NewStorage(tempDir, meta)
	if err != nil {
		t.Fatalf("NewStorage failed: %v", err)
	}
	defer store.Close()

	pm := NewPieceManager(meta, store)

	// Since remaining pieces is 1 (<= 20), it should be in Endgame mode
	if !pm.IsEndgame() {
		t.Errorf("expected IsEndgame to be true when 1 piece remains")
	}

	// Register block request from Peer A and Peer B (redundant in endgame)
	pm.RegisterRequest(0, 0, "1.1.1.1:6881")
	pm.RegisterRequest(0, 0, "2.2.2.2:6881")

	// Peer A delivers the block first
	toCancel := pm.GetPeersToCancel(0, 0, "1.1.1.1:6881")
	if len(toCancel) != 1 || toCancel[0] != "2.2.2.2:6881" {
		t.Errorf("expected Peer B to be canceled, got %v", toCancel)
	}
}

func TestReopenKeepsFileStamps(t *testing.T) {
	dir := t.TempDir()
	meta := &metainfo.TorrentMeta{
		Name:        "album",
		PieceLength: 16384,
		TotalLength: 30000,
		Pieces:      make([][20]byte, 2),
		Files: []metainfo.FileInfo{
			{Length: 10000, Path: []string{"a.bin"}},
			{Length: 20000, Path: []string{"b.bin"}},
		},
	}
	first, err := NewStorage(dir, meta)
	if err != nil {
		t.Fatal(err)
	}
	// Backdate the files so a truncate on reopen would visibly move the mtime.
	past := time.Now().Add(-time.Hour)
	for _, f := range first.Files {
		if err := os.Chtimes(f.FullPath, past, past); err != nil {
			t.Fatal(err)
		}
	}
	before, err := first.FileStamps()
	if err != nil {
		t.Fatal(err)
	}
	_ = first.Close()

	second, err := NewStorage(dir, meta)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	after, err := second.FileStamps()
	if err != nil {
		t.Fatal(err)
	}
	for i := range before {
		if before[i] != after[i] || after[i].Size != meta.Files[i].Length {
			t.Errorf("file %d: stamp changed on reopen: %+v -> %+v", i, before[i], after[i])
		}
	}
}
