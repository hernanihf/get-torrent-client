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
	compactNodeLen = 26 // IPv4: 20-byte node ID + 4-byte address + 2-byte port

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

// family is an address family's wire format: IPv4 as in BEP 0005, or IPv6 as in BEP 0032,
// which runs a separate DHT with 16-byte addresses and its nodes under "nodes6".
type family struct {
	network  string // "udp4" or "udp6"
	nodesKey string // "nodes" or "nodes6"
	ipLen    int    // 4 or 16
}

var (
	ipv4 = family{network: "udp4", nodesKey: "nodes", ipLen: net.IPv4len}
	ipv6 = family{network: "udp6", nodesKey: "nodes6", ipLen: net.IPv6len}
)

func (f family) nodeLen() int { return 20 + f.ipLen + 2 }
func (f family) peerLen() int { return f.ipLen + 2 }

// ip returns addr in this family's wire form, or nil if it belongs to the other family.
func (f family) ip(addr net.IP) net.IP {
	if f.ipLen == net.IPv4len {
		return addr.To4()
	}
	if addr.To4() != nil {
		return nil
	}
	return addr.To16()
}

// encodeNodes serializes nodes in compact node info format; other address families are skipped.
func (f family) encodeNodes(nodes []NodeInfo) string {
	buf := make([]byte, 0, len(nodes)*f.nodeLen())
	for _, n := range nodes {
		ip := f.ip(n.Addr.IP)
		if ip == nil {
			continue
		}
		buf = append(buf, n.ID[:]...)
		buf = append(buf, ip...)
		buf = binary.BigEndian.AppendUint16(buf, uint16(n.Addr.Port))
	}
	return string(buf)
}

// decodeNodes parses compact node info, skipping entries with a zero port. A trailing partial
// record is ignored.
func (f family) decodeNodes(s string) []NodeInfo {
	var nodes []NodeInfo
	size := f.nodeLen()
	for off := 0; off+size <= len(s); off += size {
		rec := s[off : off+size]
		port := binary.BigEndian.Uint16([]byte(rec[size-2:]))
		if port == 0 {
			continue
		}
		var n NodeInfo
		copy(n.ID[:], rec[:20])
		ip := make(net.IP, f.ipLen)
		copy(ip, rec[20:20+f.ipLen])
		n.Addr = &net.UDPAddr{IP: ip, Port: int(port)}
		nodes = append(nodes, n)
	}
	return nodes
}

// encodePeer serializes a peer in compact format, or returns "" for the other address family.
func (f family) encodePeer(p tracker.Peer) string {
	ip := f.ip(p.IP)
	if ip == nil {
		return ""
	}
	buf := make([]byte, f.peerLen())
	copy(buf, ip)
	binary.BigEndian.PutUint16(buf[f.ipLen:], p.Port)
	return string(buf)
}

// decodePeers parses a get_peers "values" list of compact peers, skipping malformed entries.
func (f family) decodePeers(values []any) []tracker.Peer {
	var peers []tracker.Peer
	for _, v := range values {
		s, ok := v.(string)
		if !ok || len(s) != f.peerLen() {
			continue
		}
		port := binary.BigEndian.Uint16([]byte(s[f.ipLen:]))
		if port == 0 {
			continue
		}
		ip := make(net.IP, f.ipLen)
		copy(ip, s[:f.ipLen])
		peers = append(peers, tracker.Peer{IP: ip, Port: port})
	}
	return peers
}

// IPv4 helpers (BEP 0005).
func encodeNodes(nodes []NodeInfo) string     { return ipv4.encodeNodes(nodes) }
func decodeNodes(s string) []NodeInfo         { return ipv4.decodeNodes(s) }
func encodePeer(p tracker.Peer) string        { return ipv4.encodePeer(p) }
func decodePeers(values []any) []tracker.Peer { return ipv4.decodePeers(values) }

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
