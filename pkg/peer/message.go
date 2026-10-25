package peer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// MessageID represents BitTorrent peer wire message type identifiers.
type MessageID uint8

const (
	MsgChoke         MessageID = 0
	MsgUnchoke       MessageID = 1
	MsgInterested    MessageID = 2
	MsgNotInterested MessageID = 3
	MsgHave          MessageID = 4
	MsgBitfield      MessageID = 5
	MsgRequest       MessageID = 6
	MsgPiece         MessageID = 7
	MsgCancel        MessageID = 8
)

// Message represents a framed BitTorrent peer wire message.
// A nil ID indicates a keep-alive message (length 0).
type Message struct {
	ID      *MessageID
	Payload []byte
}

// FormatRequest builds a MsgRequest (ID 6) message to request a 16KB block of a piece.
func FormatRequest(index, begin, length int) *Message {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[0:4], uint32(index))
	binary.BigEndian.PutUint32(payload[4:8], uint32(begin))
	binary.BigEndian.PutUint32(payload[8:12], uint32(length))
	id := MsgRequest
	return &Message{ID: &id, Payload: payload}
}

// FormatHave builds a MsgHave (ID 4) message announcing possession of a piece.
func FormatHave(index int) *Message {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, uint32(index))
	id := MsgHave
	return &Message{ID: &id, Payload: payload}
}

// FormatBitfield builds a MsgBitfield (ID 5) message transmitting possession of pieces.
func FormatBitfield(bf Bitfield) *Message {
	payload := make([]byte, len(bf))
	copy(payload, bf)
	id := MsgBitfield
	return &Message{ID: &id, Payload: payload}
}


// ParseHave extracts the piece index from a MsgHave payload.
func ParseHave(msg *Message) (int, error) {
	if msg.ID == nil || *msg.ID != MsgHave {
		return 0, errors.New("expected have message")
	}
	if len(msg.Payload) != 4 {
		return 0, fmt.Errorf("expected payload length 4, got %d", len(msg.Payload))
	}
	return int(binary.BigEndian.Uint32(msg.Payload)), nil
}

// ParsePiece copies piece data from a MsgPiece message into a target buffer.
func ParsePiece(expectedIndex int, targetBuf []byte, msg *Message) (int, error) {
	if msg.ID == nil || *msg.ID != MsgPiece {
		return 0, errors.New("expected piece message")
	}
	if len(msg.Payload) < 8 {
		return 0, errors.New("piece message payload too short")
	}
	parsedIndex := int(binary.BigEndian.Uint32(msg.Payload[0:4]))
	if parsedIndex != expectedIndex {
		return 0, fmt.Errorf("expected piece %d, got %d", expectedIndex, parsedIndex)
	}

	begin := int(binary.BigEndian.Uint32(msg.Payload[4:8]))
	data := msg.Payload[8:]

	if begin >= len(targetBuf) {
		return 0, fmt.Errorf("begin offset %d exceeds buffer length %d", begin, len(targetBuf))
	}
	if begin+len(data) > len(targetBuf) {
		return 0, fmt.Errorf("data of length %d at offset %d exceeds buffer length %d", len(data), begin, len(targetBuf))
	}

	copy(targetBuf[begin:], data)
	return len(data), nil
}

// FormatPiece builds a MsgPiece (ID 7) message to send block data to a peer.
func FormatPiece(index, begin int, data []byte) *Message {
	payload := make([]byte, 8+len(data))
	binary.BigEndian.PutUint32(payload[0:4], uint32(index))
	binary.BigEndian.PutUint32(payload[4:8], uint32(begin))
	copy(payload[8:], data)
	id := MsgPiece
	return &Message{ID: &id, Payload: payload}
}

// FormatCancel builds a MsgCancel (ID 8) message to cancel a pending block request.
func FormatCancel(index, begin, length int) *Message {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[0:4], uint32(index))
	binary.BigEndian.PutUint32(payload[4:8], uint32(begin))
	binary.BigEndian.PutUint32(payload[8:12], uint32(length))
	id := MsgCancel
	return &Message{ID: &id, Payload: payload}
}

// ParseRequest extracts index, begin, and length from a MsgRequest payload.
func ParseRequest(msg *Message) (int, int, int, error) {
	if msg.ID == nil || *msg.ID != MsgRequest {
		return 0, 0, 0, errors.New("expected request message")
	}
	if len(msg.Payload) != 12 {
		return 0, 0, 0, fmt.Errorf("expected request payload length 12, got %d", len(msg.Payload))
	}
	index := int(binary.BigEndian.Uint32(msg.Payload[0:4]))
	begin := int(binary.BigEndian.Uint32(msg.Payload[4:8]))
	length := int(binary.BigEndian.Uint32(msg.Payload[8:12]))
	return index, begin, length, nil
}

// ParseCancel extracts index, begin, and length from a MsgCancel payload.
func ParseCancel(msg *Message) (int, int, int, error) {
	if msg.ID == nil || *msg.ID != MsgCancel {
		return 0, 0, 0, errors.New("expected cancel message")
	}
	if len(msg.Payload) != 12 {
		return 0, 0, 0, fmt.Errorf("expected cancel payload length 12, got %d", len(msg.Payload))
	}
	index := int(binary.BigEndian.Uint32(msg.Payload[0:4]))
	begin := int(binary.BigEndian.Uint32(msg.Payload[4:8]))
	length := int(binary.BigEndian.Uint32(msg.Payload[8:12]))
	return index, begin, length, nil
}

// Serialize converts a Message into its wire format:
// <4 bytes length><1 byte ID (optional)><payload>
func (m *Message) Serialize() []byte {
	if m == nil || m.ID == nil {
		// Keep-alive: 4 zero bytes
		return make([]byte, 4)
	}

	length := uint32(len(m.Payload) + 1)
	buf := make([]byte, 4+length)
	binary.BigEndian.PutUint32(buf[0:4], length)
	buf[4] = byte(*m.ID)
	copy(buf[5:], m.Payload)
	return buf
}

// ReadMessage parses a single peer wire message from an io.Reader.
func ReadMessage(r io.Reader) (*Message, error) {
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, fmt.Errorf("reading message length: %w", err)
	}

	length := binary.BigEndian.Uint32(lengthBuf)
	if length == 0 {
		// Keep-alive message
		return &Message{ID: nil, Payload: nil}, nil
	}

	msgBuf := make([]byte, length)
	if _, err := io.ReadFull(r, msgBuf); err != nil {
		return nil, fmt.Errorf("reading message body: %w", err)
	}

	id := MessageID(msgBuf[0])
	payload := msgBuf[1:]

	return &Message{
		ID:      &id,
		Payload: payload,
	}, nil
}
