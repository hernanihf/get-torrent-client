// Package dht implements the BitTorrent Mainline DHT (BEP 0005): a Kademlia-style distributed
// hash table over UDP used to find peers for an info_hash without a tracker.
package dht

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net"

	"github.com/hernan/get-torrent/pkg/bencode"
	"github.com/hernan/get-torrent/pkg/tracker"
)

const (
	compactNodeLen = 26 // 20-byte node ID + 4-byte IPv4 + 2-byte port
	compactPeerLen = 6  // 4-byte IPv4 + 2-byte port

	// clientVersion is sent in the optional "v" key of every message.
	clientVersion = "GT01"
)

// KRPC error codes (BEP 0005).
const (
	ErrCodeGeneric  = 201
	ErrCodeServer   = 202
	ErrCodeProtocol = 203
	ErrCodeMethod   = 204
)

var ErrMalformed = errors.New("dht: malformed KRPC message")

// NodeID is a 160-bit DHT node identifier, in the same space as info_hashes.
type NodeID [20]byte

// RandomID returns a uniformly random node ID.
func RandomID() NodeID {
	var id NodeID
	_, _ = rand.Read(id[:])
	return id
}

func (id NodeID) String() string { return hex.EncodeToString(id[:]) }

// closer reports whether a is strictly closer to target than b under the XOR metric.
func closer(target, a, b NodeID) bool {
	for i := range target {
		da, db := a[i]^target[i], b[i]^target[i]
		if da != db {
			return da < db
		}
	}
	return false
}

// NodeInfo identifies a DHT node by ID and UDP address.
type NodeInfo struct {
	ID   NodeID
	Addr *net.UDPAddr
}

// encodeNodes serializes IPv4 nodes in compact node info format; other address families are skipped.
func encodeNodes(nodes []NodeInfo) string {
	buf := make([]byte, 0, len(nodes)*compactNodeLen)
	for _, n := range nodes {
		ip4 := n.Addr.IP.To4()
		if ip4 == nil {
			continue
		}
		buf = append(buf, n.ID[:]...)
		buf = append(buf, ip4...)
		buf = binary.BigEndian.AppendUint16(buf, uint16(n.Addr.Port))
	}
	return string(buf)
}

// decodeNodes parses compact node info, skipping entries with a zero port. A trailing partial
// record is ignored.
func decodeNodes(s string) []NodeInfo {
	var nodes []NodeInfo
	for off := 0; off+compactNodeLen <= len(s); off += compactNodeLen {
		rec := s[off : off+compactNodeLen]
		port := binary.BigEndian.Uint16([]byte(rec[24:26]))
		if port == 0 {
			continue
		}
		var n NodeInfo
		copy(n.ID[:], rec[:20])
		n.Addr = &net.UDPAddr{IP: net.IPv4(rec[20], rec[21], rec[22], rec[23]), Port: int(port)}
		nodes = append(nodes, n)
	}
	return nodes
}

// encodePeer serializes an IPv4 peer in compact format, or returns "" for non-IPv4 addresses.
func encodePeer(p tracker.Peer) string {
	ip4 := p.IP.To4()
	if ip4 == nil {
		return ""
	}
	buf := make([]byte, compactPeerLen)
	copy(buf, ip4)
	binary.BigEndian.PutUint16(buf[4:], p.Port)
	return string(buf)
}

// decodePeers parses a get_peers "values" list of compact peers, skipping malformed entries.
func decodePeers(values []any) []tracker.Peer {
	var peers []tracker.Peer
	for _, v := range values {
		s, ok := v.(string)
		if !ok || len(s) != compactPeerLen {
			continue
		}
		port := binary.BigEndian.Uint16([]byte(s[4:6]))
		if port == 0 {
			continue
		}
		peers = append(peers, tracker.Peer{IP: net.IPv4(s[0], s[1], s[2], s[3]), Port: port})
	}
	return peers
}

// KRPCError is an error returned by a remote node ("y": "e").
type KRPCError struct {
	Code int
	Msg  string
}

func (e *KRPCError) Error() string { return fmt.Sprintf("dht: remote error %d: %s", e.Code, e.Msg) }

// message is a decoded KRPC message: a query ("q"), response ("r") or error ("e").
type message struct {
	T string         // transaction ID
	Y string         // "q", "r" or "e"
	Q string         // query method name
	A map[string]any // query arguments
	R map[string]any // response values
	E *KRPCError
}

func (m *message) encode() []byte {
	dict := map[string]any{"t": m.T, "y": m.Y, "v": clientVersion}
	switch m.Y {
	case "q":
		dict["q"] = m.Q
		dict["a"] = m.A
	case "r":
		dict["r"] = m.R
	case "e":
		dict["e"] = []any{m.E.Code, m.E.Msg}
	}
	out, _ := bencode.Encode(dict)
	return out
}

func parseMessage(data []byte) (*message, error) {
	decoded, err := bencode.Decode(data)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	dict, ok := decoded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: not a dictionary", ErrMalformed)
	}
	m := &message{}
	m.T, _ = dict["t"].(string)
	m.Y, _ = dict["y"].(string)
	if m.T == "" || len(m.T) > 32 {
		return nil, fmt.Errorf("%w: missing or oversized transaction id", ErrMalformed)
	}

	switch m.Y {
	case "q":
		m.Q, _ = dict["q"].(string)
		m.A, _ = dict["a"].(map[string]any)
		if m.Q == "" || m.A == nil {
			return nil, fmt.Errorf("%w: query without method or arguments", ErrMalformed)
		}
	case "r":
		m.R, _ = dict["r"].(map[string]any)
		if m.R == nil {
			return nil, fmt.Errorf("%w: response without values", ErrMalformed)
		}
	case "e":
		m.E = &KRPCError{Code: ErrCodeGeneric}
		if list, ok := dict["e"].([]any); ok {
			if len(list) > 0 {
				if code, ok := list[0].(int64); ok {
					m.E.Code = int(code)
				}
			}
			if len(list) > 1 {
				m.E.Msg, _ = list[1].(string)
			}
		}
	default:
		return nil, fmt.Errorf("%w: unknown message type %q", ErrMalformed, m.Y)
	}
	return m, nil
}

// getID extracts a 20-byte ID (node ID, target or info_hash) from a dictionary.
func getID(d map[string]any, key string) (NodeID, bool) {
	var id NodeID
	s, ok := d[key].(string)
	if !ok || len(s) != len(id) {
		return id, false
	}
	copy(id[:], s)
	return id, true
}
