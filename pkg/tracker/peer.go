package tracker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
)

var (
	ErrInvalidPeerBytes = errors.New("peer bytes length is not a multiple of 6")
)

// Peer represents an individual peer endpoint.
type Peer struct {
	IP   net.IP
	Port uint16
}

func (p Peer) String() string {
	return net.JoinHostPort(p.IP.String(), fmt.Sprintf("%d", p.Port))
}

// UnmarshalPeers parses a compact binary peer list (BEP 0023).
// Each peer is represented by 6 bytes: 4 bytes IPv4 address + 2 bytes port (big-endian).
func UnmarshalPeers(peerBin []byte) ([]Peer, error) {
	const peerSize = 6
	if len(peerBin)%peerSize != 0 {
		return nil, ErrInvalidPeerBytes
	}

	numPeers := len(peerBin) / peerSize
	peers := make([]Peer, numPeers)

	for i := 0; i < numPeers; i++ {
		offset := i * peerSize
		ip := net.IP(peerBin[offset : offset+4])
		port := binary.BigEndian.Uint16(peerBin[offset+4 : offset+6])
		peers[i] = Peer{
			IP:   ip,
			Port: port,
		}
	}

	return peers, nil
}

// UnmarshalPeers6 parses a compact IPv6 peer list (BEP 0007 "peers6"): 16 bytes of address and
// 2 of port per peer.
func UnmarshalPeers6(peerBin []byte) ([]Peer, error) {
	const peerSize = 18
	if len(peerBin)%peerSize != 0 {
		return nil, fmt.Errorf("peers6 length %d is not a multiple of %d", len(peerBin), peerSize)
	}
	peers := make([]Peer, 0, len(peerBin)/peerSize)
	for off := 0; off < len(peerBin); off += peerSize {
		ip := make(net.IP, net.IPv6len)
		copy(ip, peerBin[off:off+16])
		peers = append(peers, Peer{IP: ip, Port: binary.BigEndian.Uint16(peerBin[off+16 : off+18])})
	}
	return peers, nil
}
