package storage

import (
	"testing"

	"github.com/hernan/get-torrent/pkg/metainfo"
	"github.com/hernan/get-torrent/pkg/peer"
)

func TestPieceOrderFollowsSelection(t *testing.T) {
	meta := &metainfo.TorrentMeta{
		PieceLength: 16384,
		TotalLength: 4 * 16384,
		Pieces:      make([][20]byte, 4),
		Files:       []metainfo.FileInfo{{Length: 4 * 16384, Path: []string{"x"}}},
	}
	pm := NewPieceManager(meta, nil)
	pm.Availability = []int{1, 5, 2, 9}
	all := peer.Bitfield{0xF0}

	pm.mu.RLock()
	order := pm.RarestPieceOrder(all)
	pm.mu.RUnlock()
	if want := []int{0, 2, 1, 3}; !equalInts(order, want) {
		t.Errorf("rarest-first order = %v, want %v", order, want)
	}

	// Piece 0 unwanted; piece 3 high priority comes first despite being the most common.
	pm.SetPiecePriorities([]bool{false, true, true, true}, []int8{0, PriorityLow, 0, PriorityHigh})
	pm.mu.RLock()
	order = pm.RarestPieceOrder(all)
	pm.mu.RUnlock()
	if want := []int{3, 2, 1}; !equalInts(order, want) {
		t.Errorf("priority order = %v, want %v", order, want)
	}

	have, total := pm.WantedProgress()
	if have != 0 || total != 3*16384 || pm.IsDone() || pm.Left() != 3*16384 {
		t.Errorf("unexpected progress: have %d total %d left %d", have, total, pm.Left())
	}
	pm.SetVerified(peer.Bitfield{0x70}) // pieces 1-3
	if !pm.IsDone() || pm.IsComplete() || pm.Left() != 0 {
		t.Errorf("wanted pieces verified: done %v complete %v left %d", pm.IsDone(), pm.IsComplete(), pm.Left())
	}
	if req, ok := pm.NextRequest(all); ok {
		t.Errorf("unwanted piece must not be requested, got %+v", req)
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestReleaseRequestMakesBlockRequestableAgain(t *testing.T) {
	const n = 40 // well outside endgame, where any unreceived block may be re-requested anyway
	meta := &metainfo.TorrentMeta{
		PieceLength: 16384, TotalLength: n * 16384, Pieces: make([][20]byte, n),
		Files: []metainfo.FileInfo{{Length: n * 16384, Path: []string{"x"}}},
	}
	pm := NewPieceManager(meta, nil)
	all := make(peer.Bitfield, (n+7)/8)
	for i := 0; i < n; i++ {
		all.SetPiece(i)
	}
	req, ok := pm.NextRequest(all)
	if !ok {
		t.Fatal("no request")
	}
	pm.RegisterRequest(req.PieceIndex, req.Begin, "a")
	pm.RegisterRequest(req.PieceIndex, req.Begin, "b")
	if next, _ := pm.NextRequest(all); next.PieceIndex == req.PieceIndex && next.Begin == req.Begin {
		t.Fatal("a requested block must not be handed out again outside endgame")
	}

	// Still outstanding with b: not requestable yet.
	pm.ReleaseRequest(req.PieceIndex, req.Begin, "a")
	if pm.Pieces[req.PieceIndex].RequestedMap[0] != true {
		t.Error("block released while another peer still has it requested")
	}
	pm.ReleaseRequest(req.PieceIndex, req.Begin, "b")
	if pm.Pieces[req.PieceIndex].RequestedMap[0] {
		t.Error("block must be requestable once no peer has it outstanding")
	}
	if pm.BlockReceived(req.PieceIndex, req.Begin) {
		t.Error("released block is not received")
	}
}
