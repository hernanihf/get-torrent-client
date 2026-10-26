package peer

// Bitfield represents which pieces a peer has downloaded.
type Bitfield []byte

// HasPiece checks if the bitfield has the piece at the specified index.
func (bf Bitfield) HasPiece(index int) bool {
	byteIndex := index / 8
	offset := index % 8
	if byteIndex < 0 || byteIndex >= len(bf) {
		return false
	}
	return (bf[byteIndex] >> (7 - offset) & 1) != 0
}

// SetPiece marks the piece at the specified index as possessed in the bitfield.
func (bf Bitfield) SetPiece(index int) {
	byteIndex := index / 8
	offset := index % 8
	if byteIndex < 0 || byteIndex >= len(bf) {
		return
	}
	bf[byteIndex] |= 1 << (7 - offset)
}

// HasAnyPiece returns true if any piece bit is set in the bitfield.
func (bf Bitfield) HasAnyPiece() bool {
	for _, b := range bf {
		if b != 0 {
			return true
		}
	}
	return false
}

// Grow returns a bitfield large enough to hold numPieces bits, preserving existing bits.
// Hard-capped to guard against absurd indices sent by malicious peers.
func (bf Bitfield) Grow(numPieces int) Bitfield {
	const maxPieces = 1 << 22
	if numPieces <= 0 || numPieces > maxPieces {
		return bf
	}
	need := (numPieces + 7) / 8
	if need <= len(bf) {
		return bf
	}
	grown := make(Bitfield, need)
	copy(grown, bf)
	return grown
}
