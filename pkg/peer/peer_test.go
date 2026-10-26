package peer

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"sync"
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
	if !parsed.SupportsExtension() {
		t.Errorf("expected SupportsExtension to be true")
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
	pIdx, pBegin, pLen, err := ParseRequest(reqMsg)
	if err != nil || pIdx != 5 || pBegin != 16384 || pLen != 16384 {
		t.Errorf("ParseRequest failed: got idx %d, begin %d, len %d, err %v", pIdx, pBegin, pLen, err)
	}

	// 5. MsgCancel (ID 8)
	cancelMsg := FormatCancel(5, 16384, 16384)
	if cancelMsg.ID == nil || *cancelMsg.ID != MsgCancel {
		t.Errorf("expected MsgCancel ID 8")
	}
	cIdx, cBegin, cLen, err := ParseCancel(cancelMsg)
	if err != nil || cIdx != 5 || cBegin != 16384 || cLen != 16384 {
		t.Errorf("ParseCancel failed: got idx %d, begin %d, len %d, err %v", cIdx, cBegin, cLen, err)
	}

	// 6. MsgPiece (ID 7)
	sampleData := []byte("hello-bittorrent-block")
	pieceMsg := FormatPiece(2, 0, sampleData)
	if pieceMsg.ID == nil || *pieceMsg.ID != MsgPiece {
		t.Errorf("expected MsgPiece ID 7")
	}
	targetBuf := make([]byte, len(sampleData))
	n, err := ParsePiece(2, targetBuf, pieceMsg)
	if err != nil || n != len(sampleData) || !bytes.Equal(targetBuf, sampleData) {
		t.Errorf("ParsePiece failed: n=%d, data=%s, err=%v", n, string(targetBuf), err)
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

	emptyBf := make(Bitfield, 3)
	if emptyBf.HasAnyPiece() {
		t.Errorf("expected empty bitfield HasAnyPiece to be false")
	}

	if !bf.HasAnyPiece() {
		t.Errorf("expected populated bitfield HasAnyPiece to be true")
	}
}

func TestAcceptHandshake(t *testing.T) {
	infoHash := [20]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	ourPeerID := [20]byte{'-', 'G', 'T', '0', '0', '0', '1', '-', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1'}
	remotePeerID := [20]byte{'-', 'U', 'T', '3', '5', '3', '0', '-', '2', '2', '2', '2', '2', '2', '2', '2', '2', '2', '2', '2'}

	// 1. Success case
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	go func() {
		remoteHS := NewHandshake(infoHash, remotePeerID)
		_, _ = c1.Write(remoteHS.Serialize())
		_, _ = ReadHandshake(c1)
	}()

	conn, err := AcceptHandshake(c2, ourPeerID, 2*time.Second, func(h [20]byte) bool {
		return h == infoHash
	})
	if err != nil {
		t.Fatalf("AcceptHandshake failed: %v", err)
	}
	if conn.PeerID != remotePeerID {
		t.Errorf("expected remote peer ID %x, got %x", remotePeerID, conn.PeerID)
	}
	if conn.InfoHash != infoHash {
		t.Errorf("expected info hash %x, got %x", infoHash, conn.InfoHash)
	}
	if !conn.Choked || !conn.AmChoking {
		t.Errorf("expected Choked=true and AmChoking=true initially")
	}

	// 2. Self-connection case
	s1, s2 := net.Pipe()
	defer s1.Close()
	defer s2.Close()

	go func() {
		selfHS := NewHandshake(infoHash, ourPeerID)
		_, _ = s1.Write(selfHS.Serialize())
	}()

	_, err = AcceptHandshake(s2, ourPeerID, 2*time.Second, func(h [20]byte) bool { return true })
	if err != ErrSelfConnection {
		t.Errorf("expected ErrSelfConnection, got %v", err)
	}

	// 3. Torrent not found / validate returns false
	v1, v2 := net.Pipe()
	defer v1.Close()
	defer v2.Close()

	go func() {
		otherHS := NewHandshake(infoHash, remotePeerID)
		_, _ = v1.Write(otherHS.Serialize())
	}()

	_, err = AcceptHandshake(v2, ourPeerID, 2*time.Second, func(h [20]byte) bool { return false })
	if err != ErrTorrentNotFound {
		t.Errorf("expected ErrTorrentNotFound, got %v", err)
	}
}

type mockDeadlineConn struct {
	net.Conn
	deadline time.Time
	mu       sync.Mutex
}

func (m *mockDeadlineConn) SetDeadline(t time.Time) error {
	m.mu.Lock()
	m.deadline = t
	m.mu.Unlock()
	return nil
}

func (m *mockDeadlineConn) Read(b []byte) (int, error) {
	m.mu.Lock()
	dl := m.deadline
	m.mu.Unlock()

	if !dl.IsZero() && time.Now().After(dl) {
		return 0, os.ErrDeadlineExceeded
	}

	type readRes struct {
		n   int
		err error
	}
	ch := make(chan readRes, 1)
	go func() {
		n, err := m.Conn.Read(b)
		ch <- readRes{n, err}
	}()

	if dl.IsZero() {
		res := <-ch
		return res.n, res.err
	}

	select {
	case res := <-ch:
		return res.n, res.err
	case <-time.After(time.Until(dl)):
		_ = m.Conn.Close()
		return 0, os.ErrDeadlineExceeded
	}
}

func TestAcceptHandshakeSlowlorisTimeout(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	serverConn := &mockDeadlineConn{Conn: c2}
	ourPeerID := [20]byte{'-', 'G', 'T', '0', '0', '0', '1', '-', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1'}

	errChan := make(chan error, 1)
	go func() {
		_, err := AcceptHandshake(serverConn, ourPeerID, 50*time.Millisecond, func(h [20]byte) bool { return true })
		errChan <- err
	}()

	// Slowloris peer: writes only 10 bytes and stops
	go func() {
		_, _ = c1.Write([]byte{19, 'B', 'i', 't', 'T', 'o', 'r', 'r', 'e', 'n'})
	}()

	select {
	case err := <-errChan:
		if err == nil {
			t.Fatalf("expected timeout error in AcceptHandshake, got nil")
		}
		if !errors.Is(err, os.ErrDeadlineExceeded) && !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Logf("AcceptHandshake failed as expected on slowloris: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for AcceptHandshake to abort on slowloris peer")
	}
}

func TestSendAndReadBitfield(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	peerConn := &Conn{Conn: c1}
	remoteConn := &Conn{Conn: c2}

	bf := make(Bitfield, 2)
	bf.SetPiece(3)
	bf.SetPiece(10)

	go func() {
		_ = peerConn.SendBitfield(bf)
	}()

	msg, err := remoteConn.ReadMsg()
	if err != nil {
		t.Fatalf("ReadMsg failed: %v", err)
	}
	if msg.ID == nil || *msg.ID != MsgBitfield {
		t.Fatalf("expected MsgBitfield, got %v", msg.ID)
	}
	if !remoteConn.Bitfield.HasPiece(3) || !remoteConn.Bitfield.HasPiece(10) {
		t.Errorf("expected pieces 3 and 10 to be set in remoteConn.Bitfield")
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
