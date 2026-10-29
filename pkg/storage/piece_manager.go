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
	// wanted and priority select what to download (per-file selection mapped onto pieces).
	// Every piece is wanted at normal priority until SetPiecePriorities says otherwise.
	wanted   []bool
	priority []int8
	mu       sync.RWMutex
}

// Download priorities, as in Transmission's TR_PRI_* values.
const (
	PriorityLow    int8 = -1
	PriorityNormal int8 = 0
	PriorityHigh   int8 = 1
)

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
		wanted:          allTrue(numPieces),
		priority:        make([]int8, numPieces),
	}
}

func allTrue(n int) []bool {
	b := make([]bool, n)
	for i := range b {
		b[i] = true
	}
	return b
}

// SetPiecePriorities replaces which pieces are wanted and their priorities. Both slices must
// have one entry per piece.
func (pm *PieceManager) SetPiecePriorities(wanted []bool, priority []int8) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.wanted = append([]bool(nil), wanted...)
	pm.priority = append([]int8(nil), priority...)
}

// isWanted reports whether piece i is to be downloaded. Caller holds pm.mu.
func (pm *PieceManager) isWanted(i int) bool {
	return pm.wanted == nil || pm.wanted[i]
}

// IsDone reports whether every wanted piece is verified on disk: the download is finished,
// even if unwanted files are incomplete (Transmission's "partial seed").
func (pm *PieceManager) IsDone() bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	for i := range pm.Pieces {
		if pm.isWanted(i) && !pm.Bitfield.HasPiece(i) {
			return false
		}
	}
	return true
}

// WantedProgress returns the verified and total bytes of the wanted pieces.
func (pm *PieceManager) WantedProgress() (have, total int64) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	for i, p := range pm.Pieces {
		if !pm.isWanted(i) {
			continue
		}
		total += int64(p.Length)
		if pm.Bitfield.HasPiece(i) {
			have += int64(p.Length)
		}
	}
	return have, total
}

// Left returns the bytes still to download: wanted pieces not yet verified.
func (pm *PieceManager) Left() int64 {
	have, total := pm.WantedProgress()
	return total - have
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
// Only wanted pieces count. Caller holds pm.mu.
func (pm *PieceManager) IsEndgame() bool {
	wanted, remaining := 0, 0
	for i := range pm.Pieces {
		if pm.isWanted(i) {
			wanted++
			if !pm.Bitfield.HasPiece(i) {
				remaining++
			}
		}
	}
	if remaining <= 20 {
		return true
	}
	return float64(wanted-remaining)/float64(wanted) >= 0.98
}

// RarestPieceOrder returns the wanted, missing piece indices the peer has, highest priority
// first and, within a priority, rarest first. Caller holds pm.mu.
func (pm *PieceManager) RarestPieceOrder(peerBf peer.Bitfield) []int {
	type pieceAvail struct {
		index int
		avail int
		prio  int8
	}

	var candidates []pieceAvail
	for i := 0; i < len(pm.Pieces); i++ {
		if pm.Bitfield.HasPiece(i) || !pm.isWanted(i) {
			continue // Already completed, or not selected for download
		}
		if len(peerBf) > 0 && !peerBf.HasPiece(i) {
			continue // Peer does not have this piece
		}
		c := pieceAvail{index: i, avail: pm.Availability[i]}
		if pm.priority != nil {
			c.prio = pm.priority[i]
		}
		candidates = append(candidates, c)
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].prio != candidates[j].prio {
			return candidates[i].prio > candidates[j].prio
		}
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

// Snapshot is a consistent copy of piece-level state for reporting.
type Snapshot struct {
	Have         peer.Bitfield // pieces verified and on disk
	Availability []int         // per-piece count of connected peers having it
	Wanted       []bool        // per-piece: selected for download
}

// Snapshot returns a copy of the bitfield and availability, safe to use without locks.
func (pm *PieceManager) Snapshot() Snapshot {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return Snapshot{
		Have:         append(peer.Bitfield(nil), pm.Bitfield...),
		Availability: append([]int(nil), pm.Availability...),
		Wanted:       append([]bool(nil), pm.wanted...),
	}
}

// HasPiece reports whether a piece is verified and on disk. Safe for concurrent use.
func (pm *PieceManager) HasPiece(index int) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.Bitfield.HasPiece(index)
}

// SetVerified replaces piece completion state with the result of a full hash check of the data
// on disk (have marks the pieces that verified). Outstanding requests are forgotten.
func (pm *PieceManager) SetVerified(have peer.Bitfield) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.CompletedPieces = 0
	pm.DownloadedBytes = 0
	pm.activeRequests = make(map[string][]string)
	for i, p := range pm.Pieces {
		p.mu.Lock()
		if have.HasPiece(i) {
			pm.Bitfield.SetPiece(i)
			p.IsComplete = true
			p.Data = nil
			pm.CompletedPieces++
			pm.DownloadedBytes += int64(p.Length)
		} else {
			pm.Bitfield[i/8] &^= 1 << (7 - uint(i%8))
			p.resetLocked()
			if p.Data == nil {
				p.Data = make([]byte, p.Length)
			}
		}
		p.mu.Unlock()
	}
}

// ReleaseRequest withdraws peerAddr's request for a block that will not arrive (the peer
// rejected it, choked us without the Fast Extension, or disconnected). The block becomes
// requestable again unless another peer still has it outstanding.
func (pm *PieceManager) ReleaseRequest(index, begin int, peerAddr string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if index < 0 || index >= len(pm.Pieces) {
		return
	}
	key := fmt.Sprintf("%d:%d", index, begin)
	peers := pm.activeRequests[key]
	for i, a := range peers {
		if a == peerAddr {
			peers = append(peers[:i], peers[i+1:]...)
			break
		}
	}
	if len(peers) > 0 {
		pm.activeRequests[key] = peers
		return
	}
	delete(pm.activeRequests, key)
	pm.Pieces[index].releaseBlock(begin)
}

// BlockReceived reports whether a block is already held, in its piece buffer or on disk.
func (pm *PieceManager) BlockReceived(index, begin int) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	if index < 0 || index >= len(pm.Pieces) {
		return true
	}
	if pm.Bitfield.HasPiece(index) {
		return true
	}
	return pm.Pieces[index].hasBlock(begin)
}
