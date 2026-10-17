package storage

import (
	"fmt"
	"sort"
	"sync"

	"github.com/hernan/get-torrent/pkg/metainfo"
	"github.com/hernan/get-torrent/pkg/peer"
)

// PieceManager coordinates piece block downloading, verification, and disk persistence.
type PieceManager struct {
	Pieces          []*Piece
	Storage         *Storage
	Meta            *metainfo.TorrentMeta
	Bitfield        peer.Bitfield
	Availability    []int
	CompletedPieces int
	DownloadedBytes int64
	TotalBytes      int64
	activeRequests  map[string][]string // blockKey -> []peerAddr
	mu              sync.RWMutex
}

// NewPieceManager initializes piece tracking and peer availability for a torrent.
func NewPieceManager(meta *metainfo.TorrentMeta, store *Storage) *PieceManager {
	numPieces := len(meta.Pieces)
	pieces := make([]*Piece, numPieces)
	availability := make([]int, numPieces)

	bfBytes := (numPieces + 7) / 8
	bf := make(peer.Bitfield, bfBytes)

	for i := 0; i < numPieces; i++ {
		length := int(meta.PieceLength)
		// Last piece might be smaller than standard piece length
		if i == numPieces-1 {
			remainder := int(meta.TotalLength % meta.PieceLength)
			if remainder != 0 {
				length = remainder
			}
		}
		pieces[i] = NewPiece(i, length, meta.Pieces[i])
	}

	return &PieceManager{
		Pieces:          pieces,
		Storage:         store,
		Meta:            meta,
		Bitfield:        bf,
		Availability:    availability,
		CompletedPieces: 0,
		DownloadedBytes: 0,
		TotalBytes:      meta.TotalLength,
		activeRequests:  make(map[string][]string),
	}
}

// OnPeerBitfield updates piece availability when a peer sends its initial bitfield.
func (pm *PieceManager) OnPeerBitfield(bf peer.Bitfield) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for i := 0; i < len(pm.Pieces); i++ {
		if bf.HasPiece(i) {
			pm.Availability[i]++
		}
	}
}

// OnPeerHave increments availability when a peer announces a newly acquired piece.
func (pm *PieceManager) OnPeerHave(pieceIndex int) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pieceIndex >= 0 && pieceIndex < len(pm.Availability) {
		pm.Availability[pieceIndex]++
	}
}

// OnPeerDisconnect decrements availability when a peer connection is terminated.
func (pm *PieceManager) OnPeerDisconnect(bf peer.Bitfield) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for i := 0; i < len(pm.Pieces); i++ {
		if bf.HasPiece(i) && pm.Availability[i] > 0 {
			pm.Availability[i]--
		}
	}
}

// IsEndgame returns true when very few pieces remain, triggering redundant requests to finish fast.
func (pm *PieceManager) IsEndgame() bool {
	remaining := len(pm.Pieces) - pm.CompletedPieces
	if remaining <= 20 {
		return true
	}
	progress := float64(pm.CompletedPieces) / float64(len(pm.Pieces))
	return progress >= 0.98
}

// RarestPieceOrder returns piece indices sorted by availability (rarest first).
func (pm *PieceManager) RarestPieceOrder(peerBf peer.Bitfield) []int {
	type pieceAvail struct {
		index int
		avail int
	}

	var candidates []pieceAvail
	for i := 0; i < len(pm.Pieces); i++ {
		if pm.Bitfield.HasPiece(i) {
			continue // Already completed
		}
		if len(peerBf) > 0 && !peerBf.HasPiece(i) {
			continue // Peer does not have this piece
		}
		candidates = append(candidates, pieceAvail{
			index: i,
			avail: pm.Availability[i],
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].avail < candidates[j].avail
	})

	order := make([]int, len(candidates))
	for i, c := range candidates {
		order[i] = c.index
	}
	return order
}

// NextRequest selects the next block to request from a peer using Rarest-First scheduling.
// In Endgame mode, it also re-requests unreceived blocks from multiple peers.
func (pm *PieceManager) NextRequest(peerBf peer.Bitfield) (*BlockRequest, bool) {
	pm.mu.RLock()
	orderedIndices := pm.RarestPieceOrder(peerBf)
	isEndgame := pm.IsEndgame()
	pm.mu.RUnlock()

	// 1. Try standard unrequested block first
	for _, idx := range orderedIndices {
		piece := pm.Pieces[idx]
		if req, ok := piece.NextBlockRequest(); ok {
			return req, true
		}
	}

	// 2. If in Endgame mode, allow re-requesting unreceived blocks
	if isEndgame {
		for _, idx := range orderedIndices {
			piece := pm.Pieces[idx]
			if req, ok := piece.NextBlockRequestEndgame(); ok {
				return req, true
			}
		}
	}

	return nil, false
}

// RegisterRequest tracks that a block was requested from a specific peer endpoint.
func (pm *PieceManager) RegisterRequest(index, begin int, peerAddr string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	key := fmt.Sprintf("%d:%d", index, begin)
	pm.activeRequests[key] = append(pm.activeRequests[key], peerAddr)
}

// GetPeersToCancel returns the list of peers that need a Cancel message for a received block.
func (pm *PieceManager) GetPeersToCancel(index, begin int, winningPeerAddr string) []string {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	key := fmt.Sprintf("%d:%d", index, begin)
	peers := pm.activeRequests[key]
	delete(pm.activeRequests, key)

	var toCancel []string
	for _, addr := range peers {
		if addr != winningPeerAddr {
			toCancel = append(toCancel, addr)
		}
	}
	return toCancel
}

// ReceiveBlock records an incoming block, verifies piece SHA-1 if complete, and writes to disk.
func (pm *PieceManager) ReceiveBlock(index, begin int, data []byte) (bool, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if index < 0 || index >= len(pm.Pieces) {
		return false, fmt.Errorf("piece index %d out of range", index)
	}

	piece := pm.Pieces[index]
	if pm.Bitfield.HasPiece(index) {
		return false, nil // Already written to disk
	}

	completed, err := piece.AddBlock(begin, data)
	if err != nil {
		return false, fmt.Errorf("adding block to piece %d: %w", index, err)
	}

	if completed {
		// Persist verified piece to disk
		if err := pm.Storage.WritePiece(index, piece.Data); err != nil {
			piece.Reset()
			return false, fmt.Errorf("writing piece %d to disk: %w", index, err)
		}

		pm.Bitfield.SetPiece(index)
		pm.CompletedPieces++
		pm.DownloadedBytes += int64(piece.Length)

		// Free memory buffer once saved to disk
		piece.Data = nil

		return true, nil
	}

	return false, nil
}

// IsComplete returns true if all pieces have been downloaded and verified on disk.
func (pm *PieceManager) IsComplete() bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.CompletedPieces == len(pm.Pieces)
}

// Progress returns download progress percentage (0.0 to 100.0) and downloaded bytes.
func (pm *PieceManager) Progress() (percent float64, downloaded int64) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if pm.TotalBytes == 0 {
		return 100.0, 0
	}
	percent = (float64(pm.DownloadedBytes) / float64(pm.TotalBytes)) * 100.0
	return percent, pm.DownloadedBytes
}
