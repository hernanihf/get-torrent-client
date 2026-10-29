package peer

import (
	"encoding/binary"
	"fmt"
	"net"

	"github.com/hernan/get-torrent/pkg/bencode"
	"github.com/hernan/get-torrent/pkg/tracker"
)

// BEP 0011 peer exchange. A ut_pex message lists the peers the sender connected to ("added")
// and disconnected from ("dropped") since its previous message, in compact form: 6 bytes per
// IPv4 peer and 18 per IPv6 peer, with one flag byte per added peer.

// PEX flags for added peers.
const (
	PexPrefersEncryption byte = 0x01
	PexSeed              byte = 0x02 // upload only
	PexReachable         byte = 0x10 // accepts incoming connections
)

// MaxPexPeers bounds the added and dropped lists of one message, as BEP 0011 recommends.
const MaxPexPeers = 50

// PexMsg is a decoded ut_pex message.
type PexMsg struct {
	Added      []tracker.Peer
	AddedFlags []byte // parallel to Added; 0 when the sender gave no flags
	Dropped    []tracker.Peer
}

// FormatPex builds a ut_pex message addressed to the peer's extID. Lists longer than
// MaxPexPeers are truncated; addedFlags may be nil.
func FormatPex(peerExtID uint8, added []tracker.Peer, addedFlags []byte, dropped []tracker.Peer) *Message {
	if len(added) > MaxPexPeers {
		added = added[:MaxPexPeers]
	}
	if len(dropped) > MaxPexPeers {
		dropped = dropped[:MaxPexPeers]
	}
	var a4, a6, f4, f6, d4, d6 []byte
	for i, p := range added {
		var flag byte
		if i < len(addedFlags) {
			flag = addedFlags[i]
		}
		if ip4 := p.IP.To4(); ip4 != nil {
			a4 = appendCompact(a4, ip4, p.Port)
			f4 = append(f4, flag)
		} else if ip6 := p.IP.To16(); ip6 != nil {
			a6 = appendCompact(a6, ip6, p.Port)
			f6 = append(f6, flag)
		}
	}
	for _, p := range dropped {
		if ip4 := p.IP.To4(); ip4 != nil {
			d4 = appendCompact(d4, ip4, p.Port)
		} else if ip6 := p.IP.To16(); ip6 != nil {
			d6 = appendCompact(d6, ip6, p.Port)
		}
	}
	payload, _ := bencode.Encode(map[string]any{
		"added": string(a4), "added.f": string(f4), "dropped": string(d4),
		"added6": string(a6), "added6.f": string(f6), "dropped6": string(d6),
	})
	return FormatExtended(peerExtID, payload)
}

func appendCompact(b []byte, ip net.IP, port uint16) []byte {
	b = append(b, ip...)
	return binary.BigEndian.AppendUint16(b, port)
}

// ParsePex decodes a ut_pex payload (without the extended ID byte). Malformed lists are
// rejected; at most MaxPexPeers added and dropped peers per family are kept.
func ParsePex(payload []byte) (*PexMsg, error) {
	decoded, err := bencode.Decode(payload)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidExtended, err)
	}
	dict, ok := decoded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: ut_pex is not a dictionary", ErrInvalidExtended)
	}
	msg := &PexMsg{}
	for _, fam := range []struct {
		added, flags, dropped string
		size                  int
	}{{"added", "added.f", "dropped", 6}, {"added6", "added6.f", "dropped6", 18}} {
		added, err := compactPeers(dict, fam.added, fam.size)
		if err != nil {
			return nil, err
		}
		dropped, err := compactPeers(dict, fam.dropped, fam.size)
		if err != nil {
			return nil, err
		}
		flags, _ := dict[fam.flags].(string)
		for i, p := range added {
			msg.Added = append(msg.Added, p)
			var f byte
			if i < len(flags) {
				f = flags[i]
			}
			msg.AddedFlags = append(msg.AddedFlags, f)
		}
		msg.Dropped = append(msg.Dropped, dropped...)
	}
	return msg, nil
}

// compactPeers decodes dict[key] as compact peers of size bytes each (6 or 18).
func compactPeers(dict map[string]any, key string, size int) ([]tracker.Peer, error) {
	raw, _ := dict[key].(string)
	if len(raw)%size != 0 {
		return nil, fmt.Errorf("%w: ut_pex %q has a partial entry", ErrInvalidExtended, key)
	}
	var out []tracker.Peer
	for off := 0; off < len(raw) && len(out) < MaxPexPeers; off += size {
		ip := net.IP(append([]byte(nil), raw[off:off+size-2]...))
		port := binary.BigEndian.Uint16([]byte(raw[off+size-2 : off+size]))
		if port == 0 || ip.IsUnspecified() {
			continue
		}
		out = append(out, tracker.Peer{IP: ip, Port: port})
	}
	return out, nil
}
