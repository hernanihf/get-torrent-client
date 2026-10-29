package peer

import (
	"errors"
	"fmt"

	"github.com/hernan/get-torrent/pkg/bencode"
)

// MsgExtended is the BEP 0010 extension protocol message ID.
const MsgExtended MessageID = 20

const (
	// ExtHandshakeID is the extended message ID reserved for the extension handshake.
	ExtHandshakeID uint8 = 0

	// ExtUTMetadata is the extension name for BEP 0009 metadata exchange.
	ExtUTMetadata = "ut_metadata"

	// LocalUTMetadataID is the extended message ID we advertise for ut_metadata.
	// Peers use this ID when sending ut_metadata messages to us.
	LocalUTMetadataID uint8 = 2

	// ExtUTPex is the extension name for BEP 0011 peer exchange.
	ExtUTPex = "ut_pex"

	// LocalUTPexID is the extended message ID we advertise for ut_pex.
	LocalUTPexID uint8 = 1

	// MetadataPieceSize is the fixed size of each metadata piece (BEP 0009).
	MetadataPieceSize = 16 * 1024

	// MaxMetadataSize bounds the metadata size we accept from peers to avoid memory exhaustion.
	MaxMetadataSize = 16 * 1024 * 1024
)

// ut_metadata message types (BEP 0009).
const (
	MetadataRequest = 0
	MetadataData    = 1
	MetadataReject  = 2
)

var ErrInvalidExtended = errors.New("invalid extended message")

// ExtHandshake holds the fields of a BEP 0010 extension handshake we care about.
type ExtHandshake struct {
	// M maps extension names to the message IDs the sender wants to receive them on.
	M            map[string]uint8
	MetadataSize int
	ClientName   string
	// ListenPort is the sender's TCP listen port ("p"); 0 if not given.
	ListenPort uint16
}

// FormatExtended builds a MsgExtended (ID 20) message: <extended id><payload>.
func FormatExtended(extID uint8, payload []byte) *Message {
	buf := make([]byte, 1+len(payload))
	buf[0] = extID
	copy(buf[1:], payload)
	id := MsgExtended
	return &Message{ID: &id, Payload: buf}
}

// MaxRequestQueue is the number of outstanding block requests we accept from one peer,
// advertised as "reqq".
const MaxRequestQueue = 250

// FormatExtHandshake builds our extension handshake advertising ut_metadata.
// metadataSize is omitted when zero (we do not yet have the info dictionary).
func FormatExtHandshake(metadataSize int) *Message {
	return FormatExtHandshakeWith(ExtHandshakeOptions{MetadataSize: metadataSize})
}

// ExtHandshakeOptions selects what our extension handshake advertises.
type ExtHandshakeOptions struct {
	MetadataSize int    // omitted when zero (we do not yet have the info dictionary)
	ListenPort   uint16 // our TCP port ("p"), so peers we dialled can share it via PEX
	PEX          bool   // advertise ut_pex; never for private torrents (BEP 0027)
}

// FormatExtHandshakeWith builds our extension handshake.
func FormatExtHandshakeWith(opts ExtHandshakeOptions) *Message {
	m := map[string]any{ExtUTMetadata: int(LocalUTMetadataID)}
	if opts.PEX {
		m[ExtUTPex] = int(LocalUTPexID)
	}
	dict := map[string]any{
		"m":    m,
		"v":    "get-torrent 0.0.1",
		"reqq": MaxRequestQueue,
	}
	if opts.MetadataSize > 0 {
		dict["metadata_size"] = opts.MetadataSize
	}
	if opts.ListenPort > 0 {
		dict["p"] = int(opts.ListenPort)
	}
	payload, _ := bencode.Encode(dict)
	return FormatExtended(ExtHandshakeID, payload)
}

// ParseExtended splits a MsgExtended message into its extended ID and payload.
func ParseExtended(msg *Message) (uint8, []byte, error) {
	if msg.ID == nil || *msg.ID != MsgExtended {
		return 0, nil, fmt.Errorf("%w: not an extended message", ErrInvalidExtended)
	}
	if len(msg.Payload) < 1 {
		return 0, nil, fmt.Errorf("%w: empty payload", ErrInvalidExtended)
	}
	return msg.Payload[0], msg.Payload[1:], nil
}

// ParseExtHandshake decodes a BEP 0010 handshake dictionary.
func ParseExtHandshake(payload []byte) (*ExtHandshake, error) {
	decoded, err := bencode.Decode(payload)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidExtended, err)
	}
	dict, ok := decoded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: handshake is not a dictionary", ErrInvalidExtended)
	}

	hs := &ExtHandshake{M: make(map[string]uint8)}
	if m, ok := dict["m"].(map[string]any); ok {
		for name, v := range m {
			// An ID of 0 means the extension is disabled; IDs must fit in one byte.
			if id, ok := v.(int64); ok && id > 0 && id <= 255 {
				hs.M[name] = uint8(id)
			}
		}
	}
	if size, ok := dict["metadata_size"].(int64); ok && size > 0 && size <= MaxMetadataSize {
		hs.MetadataSize = int(size)
	}
	hs.ClientName, _ = dict["v"].(string)
	if p, ok := dict["p"].(int64); ok && p > 0 && p <= 65535 {
		hs.ListenPort = uint16(p)
	}
	return hs, nil
}

// MetadataMsg is a decoded BEP 0009 ut_metadata message.
type MetadataMsg struct {
	Type      int
	Piece     int
	TotalSize int
	Data      []byte // only set for MetadataData
}

// FormatMetadataRequest builds a ut_metadata request for one piece, addressed to the peer's extID.
func FormatMetadataRequest(peerExtID uint8, piece int) *Message {
	payload, _ := bencode.Encode(map[string]any{"msg_type": MetadataRequest, "piece": piece})
	return FormatExtended(peerExtID, payload)
}

// FormatMetadataReject builds a ut_metadata reject for one piece.
func FormatMetadataReject(peerExtID uint8, piece int) *Message {
	payload, _ := bencode.Encode(map[string]any{"msg_type": MetadataReject, "piece": piece})
	return FormatExtended(peerExtID, payload)
}

// FormatMetadataData builds a ut_metadata data message: bencoded header followed by raw piece bytes.
func FormatMetadataData(peerExtID uint8, piece, totalSize int, data []byte) *Message {
	header, _ := bencode.Encode(map[string]any{"msg_type": MetadataData, "piece": piece, "total_size": totalSize})
	payload := make([]byte, len(header)+len(data))
	copy(payload, header)
	copy(payload[len(header):], data)
	return FormatExtended(peerExtID, payload)
}

// ParseMetadataMsg decodes a ut_metadata payload (without the extended ID byte).
func ParseMetadataMsg(payload []byte) (*MetadataMsg, error) {
	decoded, n, err := bencode.DecodeLeading(payload)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidExtended, err)
	}
	dict, ok := decoded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: ut_metadata header is not a dictionary", ErrInvalidExtended)
	}
	msgType, ok1 := dict["msg_type"].(int64)
	piece, ok2 := dict["piece"].(int64)
	if !ok1 || !ok2 || piece < 0 || piece > MaxMetadataSize/MetadataPieceSize {
		return nil, fmt.Errorf("%w: missing or invalid msg_type/piece", ErrInvalidExtended)
	}

	m := &MetadataMsg{Type: int(msgType), Piece: int(piece)}
	if m.Type == MetadataData {
		total, _ := dict["total_size"].(int64)
		if total <= 0 || total > MaxMetadataSize {
			return nil, fmt.Errorf("%w: invalid total_size %d", ErrInvalidExtended, total)
		}
		m.TotalSize = int(total)
		m.Data = payload[n:]
	}
	return m, nil
}
