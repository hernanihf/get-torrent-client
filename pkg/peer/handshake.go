package peer

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

const (
	protocolIdentifier = "BitTorrent protocol"
	handshakeLength    = 68
)

var (
	ErrInvalidProtocol = errors.New("peer did not use 'BitTorrent protocol'")
	ErrHandshakeLength  = errors.New("handshake byte count invalid")
)

// Handshake represents the initial 68-byte exchange between BitTorrent peers.
type Handshake struct {
	Pstr     string
	InfoHash [20]byte
	PeerID   [20]byte
}

// NewHandshake creates a new Handshake for a given info_hash and peer_id.
func NewHandshake(infoHash, peerID [20]byte) *Handshake {
	return &Handshake{
		Pstr:     protocolIdentifier,
		InfoHash: infoHash,
		PeerID:   peerID,
	}
}

// Serialize converts the Handshake into its 68-byte binary representation.
func (h *Handshake) Serialize() []byte {
	buf := make([]byte, handshakeLength)
	buf[0] = byte(len(h.Pstr))
	curr := 1
	curr += copy(buf[curr:], []byte(h.Pstr))
	curr += copy(buf[curr:], make([]byte, 8)) // 8 reserved bytes
	curr += copy(buf[curr:], h.InfoHash[:])
	copy(buf[curr:], h.PeerID[:])
	return buf
}

// ReadHandshake reads and parses a 68-byte Handshake from an io.Reader.
func ReadHandshake(r io.Reader) (*Handshake, error) {
	buf := make([]byte, handshakeLength)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("reading handshake: %w", err)
	}

	pstrlen := int(buf[0])
	if pstrlen != len(protocolIdentifier) {
		return nil, fmt.Errorf("%w: invalid pstrlen %d", ErrInvalidProtocol, pstrlen)
	}

	pstr := string(buf[1 : 1+pstrlen])
	if pstr != protocolIdentifier {
		return nil, fmt.Errorf("%w: got %q", ErrInvalidProtocol, pstr)
	}

	var infoHash, peerID [20]byte
	copy(infoHash[:], buf[1+pstrlen+8:1+pstrlen+8+20])
	copy(peerID[:], buf[1+pstrlen+8+20:])

	return &Handshake{
		Pstr:     pstr,
		InfoHash: infoHash,
		PeerID:   peerID,
	}, nil
}
