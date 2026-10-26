// Package metadata reassembles a torrent's info dictionary from BEP 0009 ut_metadata pieces.
package metadata

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hernan/get-torrent/pkg/peer"
)

var (
	ErrSizeMismatch = errors.New("metadata size disagrees with previously announced size")
	ErrInvalidSize  = errors.New("invalid metadata size")
	ErrBadPiece     = errors.New("invalid metadata piece")
	ErrHashMismatch = errors.New("assembled metadata does not match info_hash")
)

// RequestTimeout is how long an outstanding piece request is honoured before it may be re-requested.
const RequestTimeout = 20 * time.Second

// Assembler collects metadata pieces from multiple peers and verifies the result against the info_hash.
// It is safe for concurrent use.
type Assembler struct {
	infoHash  [20]byte
	mu        sync.Mutex
	size      int
	pieces    [][]byte
	received  int
	requested []time.Time
	done      []byte
}

// NewAssembler creates an assembler for the given info_hash. The size is learnt later from peers.
func NewAssembler(infoHash [20]byte) *Assembler {
	return &Assembler{infoHash: infoHash}
}

// numPieces returns the piece count for a metadata size.
func numPieces(size int) int {
	return (size + peer.MetadataPieceSize - 1) / peer.MetadataPieceSize
}

// SetSize initialises the piece table from a peer-announced metadata_size.
// Calling it again with the same size is a no-op; a conflicting size is rejected.
func (a *Assembler) SetSize(size int) error {
	if size <= 0 || size > peer.MaxMetadataSize {
		return fmt.Errorf("%w: %d", ErrInvalidSize, size)
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.size != 0 {
		if a.size != size {
			return fmt.Errorf("%w: have %d, peer says %d", ErrSizeMismatch, a.size, size)
		}
		return nil
	}
	a.size = size
	n := numPieces(size)
	a.pieces = make([][]byte, n)
	a.requested = make([]time.Time, n)
	return nil
}

// Size returns the known metadata size, or 0 if not yet known.
func (a *Assembler) Size() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.size
}

// Progress returns the fraction of metadata pieces received, in [0, 1].
func (a *Assembler) Progress() float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.done != nil {
		return 1
	}
	if len(a.pieces) == 0 {
		return 0
	}
	return float64(a.received) / float64(len(a.pieces))
}

// NextRequests returns up to max piece indices that are neither received nor recently requested,
// marking them as requested at now.
func (a *Assembler) NextRequests(max int, now time.Time) []int {
	a.mu.Lock()
	defer a.mu.Unlock()

	var out []int
	for i := range a.pieces {
		if len(out) >= max {
			break
		}
		if a.pieces[i] != nil {
			continue
		}
		if !a.requested[i].IsZero() && now.Sub(a.requested[i]) < RequestTimeout {
			continue
		}
		a.requested[i] = now
		out = append(out, i)
	}
	return out
}

// Rejected clears the outstanding request for a piece so another peer can be asked.
func (a *Assembler) Rejected(piece int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if piece >= 0 && piece < len(a.requested) {
		a.requested[piece] = time.Time{}
	}
}

// AddPiece stores a received piece. When the final piece arrives the metadata is verified:
// on success the complete info dictionary is returned; on hash failure all pieces are discarded
// and ErrHashMismatch is returned so the download restarts.
func (a *Assembler) AddPiece(index, totalSize int, data []byte) ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.done != nil {
		return nil, nil // Already complete; duplicate piece
	}
	if a.size == 0 {
		return nil, fmt.Errorf("%w: size unknown", ErrBadPiece)
	}
	if totalSize != a.size {
		return nil, fmt.Errorf("%w: total_size %d, expected %d", ErrSizeMismatch, totalSize, a.size)
	}
	if index < 0 || index >= len(a.pieces) {
		return nil, fmt.Errorf("%w: index %d out of range", ErrBadPiece, index)
	}

	// Every piece is exactly 16 KiB except the last, which holds the remainder.
	expected := peer.MetadataPieceSize
	if index == len(a.pieces)-1 {
		expected = a.size - index*peer.MetadataPieceSize
	}
	if len(data) != expected {
		return nil, fmt.Errorf("%w: piece %d has %d bytes, expected %d", ErrBadPiece, index, len(data), expected)
	}

	if a.pieces[index] != nil {
		return nil, nil
	}
	a.pieces[index] = append([]byte(nil), data...)
	a.received++
	if a.received < len(a.pieces) {
		return nil, nil
	}

	full := make([]byte, 0, a.size)
	for _, p := range a.pieces {
		full = append(full, p...)
	}
	if sha1.Sum(full) != a.infoHash {
		// A peer sent corrupt data; we cannot tell which, so start over.
		for i := range a.pieces {
			a.pieces[i] = nil
			a.requested[i] = time.Time{}
		}
		a.received = 0
		return nil, ErrHashMismatch
	}
	a.done = full
	a.pieces = nil
	return full, nil
}

// Complete reports whether verified metadata has been assembled.
func (a *Assembler) Complete() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.done != nil
}

// Piece returns piece index of a complete info dictionary, for serving ut_metadata requests.
func Piece(info []byte, index int) ([]byte, bool) {
	start := index * peer.MetadataPieceSize
	if index < 0 || start >= len(info) {
		return nil, false
	}
	end := start + peer.MetadataPieceSize
	if end > len(info) {
		end = len(info)
	}
	return info[start:end], true
}
