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
