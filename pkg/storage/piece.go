package storage

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"sync"
)

// BlockSize is the standard BitTorrent piece block request size (16 KiB).
const BlockSize = 16384

var (
	ErrBlockOutOfBounds = errors.New("block offset and length exceed piece boundaries")
	ErrPieceCorrupted   = errors.New("piece failed SHA-1 hash verification")
)

// BlockRequest represents a block within a piece waiting to be requested from a peer.
type BlockRequest struct {
	PieceIndex int
	Begin      int
	Length     int
}

// Piece manages the blocks, raw data buffer, and hash verification for a single piece.
type Piece struct {
	Index          int
	Length         int
	Hash           [20]byte
	TotalBlocks    int
	BlocksReceived int
	Data           []byte
	ReceivedMap    []bool
	RequestedMap   []bool
	IsComplete     bool
	mu             sync.Mutex
}

// NewPiece constructs a Piece with pre-allocated block tracking.
func NewPiece(index int, length int, hash [20]byte) *Piece {
	totalBlocks := (length + BlockSize - 1) / BlockSize

	return &Piece{
		Index:        index,
		Length:       length,
		Hash:         hash,
		TotalBlocks:  totalBlocks,
		Data:         make([]byte, length),
		ReceivedMap:  make([]bool, totalBlocks),
		RequestedMap: make([]bool, totalBlocks),
	}
}

// NextBlockRequest finds the next unrequested block in this piece.
func (p *Piece) NextBlockRequest() (*BlockRequest, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.IsComplete {
		return nil, false
	}

	for i := 0; i < p.TotalBlocks; i++ {
		if !p.ReceivedMap[i] && !p.RequestedMap[i] {
			p.RequestedMap[i] = true
			begin := i * BlockSize
			length := BlockSize
			if begin+length > p.Length {
				length = p.Length - begin
			}
			return &BlockRequest{
				PieceIndex: p.Index,
				Begin:      begin,
				Length:     length,
			}, true
		}
	}
	return nil, false
}

// NextBlockRequestEndgame returns any unreceived block in this piece, even if previously requested.
func (p *Piece) NextBlockRequestEndgame() (*BlockRequest, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.IsComplete {
		return nil, false
	}

	for i := 0; i < p.TotalBlocks; i++ {
		if !p.ReceivedMap[i] {
			begin := i * BlockSize
			length := BlockSize
			if begin+length > p.Length {
				length = p.Length - begin
			}
			return &BlockRequest{
				PieceIndex: p.Index,
				Begin:      begin,
				Length:     length,
			}, true
		}
	}
	return nil, false
}

// AddBlock inserts block data into the piece buffer and checks if all blocks have arrived.
func (p *Piece) AddBlock(begin int, data []byte) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.IsComplete {
		return true, nil
	}

	if begin < 0 || begin+len(data) > p.Length {
		return false, fmt.Errorf("%w: begin %d + len %d > piece len %d", ErrBlockOutOfBounds, begin, len(data), p.Length)
	}

	blockIdx := begin / BlockSize
	if blockIdx < p.TotalBlocks && !p.ReceivedMap[blockIdx] {
		p.ReceivedMap[blockIdx] = true
		p.BlocksReceived++
		copy(p.Data[begin:], data)
	}

	if p.BlocksReceived == p.TotalBlocks {
		if !p.verifyHashLocked() {
			p.resetLocked()
			return false, ErrPieceCorrupted
		}
		p.IsComplete = true
		return true, nil
	}

	return false, nil
}

func (p *Piece) verifyHashLocked() bool {
	sum := sha1.Sum(p.Data)
	return sum == p.Hash
}

// VerifyHash verifies whether the buffered piece data matches the expected SHA-1 hash.
func (p *Piece) VerifyHash() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.verifyHashLocked()
}

func (p *Piece) resetLocked() {
	p.BlocksReceived = 0
	p.IsComplete = false
	for i := range p.ReceivedMap {
		p.ReceivedMap[i] = false
		p.RequestedMap[i] = false
	}
}

// Reset resets block tracking if the piece data was corrupted or request timed out.
func (p *Piece) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resetLocked()
}

// releaseBlock marks a requested but unreceived block as requestable again.
func (p *Piece) releaseBlock(begin int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if i := begin / BlockSize; begin%BlockSize == 0 && i >= 0 && i < p.TotalBlocks && !p.ReceivedMap[i] {
		p.RequestedMap[i] = false
	}
}

// hasBlock reports whether the block at begin has been received.
func (p *Piece) hasBlock(begin int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	i := begin / BlockSize
	return p.IsComplete || (i >= 0 && i < p.TotalBlocks && p.ReceivedMap[i])
}
