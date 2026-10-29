package peer

import (
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
	ErrHandshakeLength = errors.New("handshake byte count invalid")
)

// Handshake represents the initial 68-byte exchange between BitTorrent peers.
type Handshake struct {
	Pstr     string
	Reserved [8]byte
	InfoHash [20]byte
	PeerID   [20]byte
}

// NewHandshake creates a new Handshake for a given info_hash and peer_id.
func NewHandshake(infoHash, peerID [20]byte) *Handshake {
	var reserved [8]byte
	reserved[5] |= 0x10 // BEP 0010 Extension Protocol support
	reserved[7] |= 0x01 // BEP 0005 DHT support
	reserved[7] |= 0x04 // BEP 0006 Fast Extension
	return &Handshake{
		Pstr:     protocolIdentifier,
		Reserved: reserved,
		InfoHash: infoHash,
		PeerID:   peerID,
	}
}

// SupportsExtension returns true if the peer indicates support for BEP 0010 extension protocol.
func (h *Handshake) SupportsExtension() bool {
	return (h.Reserved[5] & 0x10) != 0
}

// SupportsFast reports whether the BEP 0006 Fast Extension bit is set.
func (h *Handshake) SupportsFast() bool {
	return (h.Reserved[7] & 0x04) != 0
}

// SupportsDHT returns true if the peer indicates BEP 0005 DHT support (and may send MsgPort).
func (h *Handshake) SupportsDHT() bool {
	return (h.Reserved[7] & 0x01) != 0
}

// Serialize converts the Handshake into its 68-byte binary representation.
func (h *Handshake) Serialize() []byte {
	buf := make([]byte, handshakeLength)
	buf[0] = byte(len(h.Pstr))
	curr := 1
	curr += copy(buf[curr:], []byte(h.Pstr))
	curr += copy(buf[curr:], h.Reserved[:])
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

	var reserved [8]byte
	copy(reserved[:], buf[1+pstrlen:1+pstrlen+8])

	var infoHash, peerID [20]byte
	copy(infoHash[:], buf[1+pstrlen+8:1+pstrlen+8+20])
	copy(peerID[:], buf[1+pstrlen+8+20:])

	return &Handshake{
		Pstr:     pstr,
		Reserved: reserved,
		InfoHash: infoHash,
		PeerID:   peerID,
	}, nil
}
