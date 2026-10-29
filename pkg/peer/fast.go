package peer

import (
	"encoding/binary"
	"fmt"
)

// BEP 0006 Fast Extension messages. With it, every request gets exactly one answer: the piece
// or a Reject Request; Have All / Have None may replace the bitfield.

// FormatHaveAll builds a Have All message.
func FormatHaveAll() *Message { return &Message{ID: fastID(MsgHaveAll)} }

// FormatHaveNone builds a Have None message.
func FormatHaveNone() *Message { return &Message{ID: fastID(MsgHaveNone)} }

// FormatReject builds a Reject Request for a block the peer asked for.
func FormatReject(index, begin, length int) *Message {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[0:4], uint32(index))
	binary.BigEndian.PutUint32(payload[4:8], uint32(begin))
	binary.BigEndian.PutUint32(payload[8:12], uint32(length))
	return &Message{ID: fastID(MsgRejectRequest), Payload: payload}
}

// ParseReject extracts index, begin and length from a Reject Request.
func ParseReject(msg *Message) (int, int, int, error) {
	if msg.ID == nil || *msg.ID != MsgRejectRequest || len(msg.Payload) != 12 {
		return 0, 0, 0, fmt.Errorf("malformed reject request")
	}
	return int(binary.BigEndian.Uint32(msg.Payload[0:4])),
		int(binary.BigEndian.Uint32(msg.Payload[4:8])),
		int(binary.BigEndian.Uint32(msg.Payload[8:12])), nil
}

func fastID(id MessageID) *MessageID { return &id }
