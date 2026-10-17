package peer

import (
	"bytes"
	"net"
	"testing"
	"time"
)

func TestHandshakeRoundtrip(t *testing.T) {
	infoHash := [20]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	peerID := [20]byte{'-', 'G', 'T', '0', '0', '0', '1', '-', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l'}

	hs := NewHandshake(infoHash, peerID)
	serialized := hs.Serialize()

	if len(serialized) != 68 {
		t.Fatalf("expected 68 bytes, got %d", len(serialized))
	}

	parsed, err := ReadHandshake(bytes.NewReader(serialized))
	if err != nil {
		t.Fatalf("failed reading handshake: %v", err)
	}

	if parsed.Pstr != protocolIdentifier {
		t.Errorf("expected %q, got %q", protocolIdentifier, parsed.Pstr)
	}
	if parsed.InfoHash != infoHash {
		t.Errorf("info_hash mismatch: expected %x, got %x", infoHash, parsed.InfoHash)
	}
	if parsed.PeerID != peerID {
		t.Errorf("peer_id mismatch: expected %x, got %x", peerID, parsed.PeerID)
	}
}

func TestMessageSerialization(t *testing.T) {
	// 1. Keep-alive
	keepAlive := &Message{ID: nil}
	serialized := keepAlive.Serialize()
	if !bytes.Equal(serialized, []byte{0, 0, 0, 0}) {
		t.Errorf("unexpected keep-alive serialization: %v", serialized)
	}

	parsed, err := ReadMessage(bytes.NewReader(serialized))
	if err != nil {
		t.Fatalf("failed to read keep-alive: %v", err)
	}
	if parsed.ID != nil {
		t.Errorf("expected nil ID for keep-alive, got %v", *parsed.ID)
	}

	// 2. MsgInterested (ID 2)
	idInterested := MsgInterested
	msgInterested := &Message{ID: &idInterested}
	serialized = msgInterested.Serialize()
	// Length 1 (0,0,0,1) + ID 2
	expected := []byte{0, 0, 0, 1, 2}
	if !bytes.Equal(serialized, expected) {
		t.Errorf("expected %v, got %v", expected, serialized)
	}

	// 3. MsgHave (ID 4, piece 42)
	haveMsg := FormatHave(42)
	serialized = haveMsg.Serialize()
	parsedHave, err := ReadMessage(bytes.NewReader(serialized))
	if err != nil {
		t.Fatalf("failed to read have msg: %v", err)
	}
	idx, err := ParseHave(parsedHave)
	if err != nil {
		t.Fatalf("ParseHave failed: %v", err)
	}
	if idx != 42 {
		t.Errorf("expected piece 42, got %d", idx)
	}

	// 4. MsgRequest (ID 6)
	reqMsg := FormatRequest(5, 16384, 16384)
	if reqMsg.ID == nil || *reqMsg.ID != MsgRequest {
		t.Errorf("expected MsgRequest ID 6")
	}
	if len(reqMsg.Payload) != 12 {
		t.Errorf("expected 12 bytes payload for request, got %d", len(reqMsg.Payload))
	}
}

func TestBitfield(t *testing.T) {
	bf := make(Bitfield, 2) // 16 pieces total (0 to 15)

	if bf.HasPiece(0) {
		t.Errorf("expected piece 0 to be false")
	}

	bf.SetPiece(0)
	bf.SetPiece(7)
	bf.SetPiece(8)

	if !bf.HasPiece(0) {
		t.Errorf("expected piece 0 to be true")
	}
	if !bf.HasPiece(7) {
		t.Errorf("expected piece 7 to be true")
	}
	if !bf.HasPiece(8) {
		t.Errorf("expected piece 8 to be true")
	}
	if bf.HasPiece(1) {
		t.Errorf("expected piece 1 to be false")
	}
	if bf.HasPiece(15) {
		t.Errorf("expected piece 15 to be false")
	}
}

func TestPeerConnectionNetPipe(t *testing.T) {
	infoHash := [20]byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	clientPeerID := [20]byte{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}
	remotePeerID := [20]byte{3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3}

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	// Simulate remote peer server side
	go func() {
		// Read client handshake
		hs, err := ReadHandshake(serverConn)
		if err != nil {
			return
		}
		// Send back server handshake
		serverHS := NewHandshake(hs.InfoHash, remotePeerID)
		_, _ = serverConn.Write(serverHS.Serialize())

		// Send Unchoke message to client
		unchokeID := MsgUnchoke
		unchokeMsg := &Message{ID: &unchokeID}
		_, _ = serverConn.Write(unchokeMsg.Serialize())
	}()

	// Client side simulation
	hs := NewHandshake(infoHash, clientPeerID)
	_, err := clientConn.Write(hs.Serialize())
	if err != nil {
		t.Fatalf("writing client handshake: %v", err)
	}

	remoteHS, err := ReadHandshake(clientConn)
	if err != nil {
		t.Fatalf("reading remote handshake: %v", err)
	}

	if remoteHS.PeerID != remotePeerID {
		t.Errorf("expected remote peer ID %x, got %x", remotePeerID, remoteHS.PeerID)
	}

	// Read unchoke message
	msg, err := ReadMessage(clientConn)
	if err != nil {
		t.Fatalf("reading message: %v", err)
	}
	if msg.ID == nil || *msg.ID != MsgUnchoke {
		t.Fatalf("expected unchoke message, got %v", msg.ID)
	}
}
