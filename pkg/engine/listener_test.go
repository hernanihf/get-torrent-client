package engine

import (
	"net"
	"testing"
	"time"

	"github.com/hernan/get-torrent/pkg/metainfo"
	"github.com/hernan/get-torrent/pkg/peer"
	"github.com/hernan/get-torrent/pkg/storage"
)

func TestListenerLifecycle(t *testing.T) {
	eng := NewEngine("/tmp/downloads", 0) // Port 0 asks OS for free ephemeral port

	if err := eng.StartListener(); err != nil {
		t.Fatalf("StartListener failed: %v", err)
	}

	if eng.Port == 0 {
		t.Fatalf("expected non-zero ephemeral port assigned, got 0")
	}

	// Double start should be idempotent
	if err := eng.StartListener(); err != nil {
		t.Fatalf("second StartListener failed: %v", err)
	}

	if err := eng.StopListener(); err != nil {
		t.Fatalf("StopListener failed: %v", err)
	}

	// Double stop should be safe
	if err := eng.StopListener(); err != nil {
		t.Fatalf("second StopListener failed: %v", err)
	}

	// Close engine should be safe
	if err := eng.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestInboundConnectionWorkflow(t *testing.T) {
	eng := NewEngine("/tmp/downloads", 0)
	defer func() { _ = eng.Close() }()

	infoHash := [20]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	clientPeerID := [20]byte{'-', 'G', 'T', '9', '9', '9', '9', '-', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2'}

	bf := make(peer.Bitfield, 2)
	bf.SetPiece(1)
	pm := &storage.PieceManager{
		Bitfield: bf,
	}

	managed := &ManagedTorrent{
		ID:           1,
		Meta:         &metainfo.TorrentMeta{InfoHash: infoHash, Name: "test_torrent", TotalLength: 32768, PieceLength: 16384},
		PieceManager: pm,
		State:        StateDownloading,
		stopChan:     make(chan struct{}),
	}

	eng.mu.Lock()
	eng.torrents[infoHash] = managed
	eng.mu.Unlock()

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	go eng.handleInboundConn(serverConn)

	// Send handshake
	hs := peer.NewHandshake(infoHash, clientPeerID)
	if _, err := clientConn.Write(hs.Serialize()); err != nil {
		t.Fatalf("writing handshake: %v", err)
	}

	// Read reciprocal handshake from engine
	respHS, err := peer.ReadHandshake(clientConn)
	if err != nil {
		t.Fatalf("reading reciprocal handshake: %v", err)
	}
	if respHS.PeerID != eng.PeerID {
		t.Errorf("expected engine peer ID %x, got %x", eng.PeerID, respHS.PeerID)
	}
	if respHS.InfoHash != infoHash {
		t.Errorf("expected infoHash %x, got %x", infoHash, respHS.InfoHash)
	}

	// Engine should send Bitfield (since HasAnyPiece is true) and Interested (since StateDownloading)
	msg1, err := peer.ReadMessage(clientConn)
	if err != nil {
		t.Fatalf("reading first message: %v", err)
	}
	if msg1.ID == nil || *msg1.ID != peer.MsgBitfield {
		t.Fatalf("expected MsgBitfield, got %v", msg1.ID)
	}

	msg2, err := peer.ReadMessage(clientConn)
	if err != nil {
		t.Fatalf("reading second message: %v", err)
	}
	if msg2.ID == nil || *msg2.ID != peer.MsgInterested {
		t.Fatalf("expected MsgInterested, got %v", msg2.ID)
	}

	// Check active peers registered
	time.Sleep(50 * time.Millisecond)
	managed.mu.RLock()
	peersCount := managed.ActivePeers
	managed.mu.RUnlock()
	if peersCount != 1 {
		t.Errorf("expected ActivePeers=1, got %d", peersCount)
	}

	// Disconnect client
	_ = clientConn.Close()
	time.Sleep(50 * time.Millisecond)

	managed.mu.RLock()
	peersCount = managed.ActivePeers
	managed.mu.RUnlock()
	if peersCount != 0 {
		t.Errorf("expected ActivePeers to decrement to 0 after disconnect, got %d", peersCount)
	}
}

func TestInboundConnectionRejections(t *testing.T) {
	eng := NewEngine("/tmp/downloads", 0)
	defer func() { _ = eng.Close() }()

	unknownHash := [20]byte{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9}
	clientPeerID := [20]byte{'-', 'T', 'E', 'S', 'T', '-', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4'}

	// 1. Unknown torrent rejection
	c1, s1 := net.Pipe()
	defer c1.Close()
	defer s1.Close()

	go eng.handleInboundConn(s1)

	hs1 := peer.NewHandshake(unknownHash, clientPeerID)
	_, _ = c1.Write(hs1.Serialize())

	// Engine should close connection without reciprocal handshake
	_, err := peer.ReadHandshake(c1)
	if err == nil {
		t.Errorf("expected error reading handshake on unknown torrent, got nil")
	}

	// 2. Self connection rejection
	validHash := [20]byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	eng.mu.Lock()
	eng.torrents[validHash] = &ManagedTorrent{
		ID:       2,
		Meta:     &metainfo.TorrentMeta{InfoHash: validHash},
		State:    StateDownloading,
		stopChan: make(chan struct{}),
	}
	eng.mu.Unlock()

	c2, s2 := net.Pipe()
	defer c2.Close()
	defer s2.Close()

	go eng.handleInboundConn(s2)

	// Send handshake with eng.PeerID (self connection)
	hs2 := peer.NewHandshake(validHash, eng.PeerID)
	_, _ = c2.Write(hs2.Serialize())

	_, err = peer.ReadHandshake(c2)
	if err == nil {
		t.Errorf("expected error reading handshake on self-connection, got nil")
	}

	// 3. Stopped torrent rejection
	stoppedHash := [20]byte{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}
	eng.mu.Lock()
	eng.torrents[stoppedHash] = &ManagedTorrent{
		ID:       3,
		Meta:     &metainfo.TorrentMeta{InfoHash: stoppedHash},
		State:    StateStopped,
		stopChan: make(chan struct{}),
	}
	eng.mu.Unlock()

	c3, s3 := net.Pipe()
	defer c3.Close()
	defer s3.Close()

	go eng.handleInboundConn(s3)

	hs3 := peer.NewHandshake(stoppedHash, clientPeerID)
	_, _ = c3.Write(hs3.Serialize())

	_, err = peer.ReadHandshake(c3)
	if err == nil {
		t.Errorf("expected error reading handshake on stopped torrent, got nil")
	}
}

func TestInboundDuplicatePeerRejection(t *testing.T) {
	eng := NewEngine("/tmp/downloads", 0)
	defer func() { _ = eng.Close() }()

	infoHash := [20]byte{3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3}
	clientPeerID := [20]byte{'-', 'D', 'U', 'P', '1', '-', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4'}

	managed := &ManagedTorrent{
		ID:       1,
		Meta:     &metainfo.TorrentMeta{InfoHash: infoHash, Name: "dup_test"},
		State:    StateDownloading,
		stopChan: make(chan struct{}),
	}
	eng.mu.Lock()
	eng.torrents[infoHash] = managed
	eng.mu.Unlock()

	// First connection from clientPeerID
	c1, s1 := net.Pipe()
	defer c1.Close()
	defer s1.Close()

	go eng.handleInboundConn(s1)

	hs1 := peer.NewHandshake(infoHash, clientPeerID)
	if _, err := c1.Write(hs1.Serialize()); err != nil {
		t.Fatalf("writing hs1: %v", err)
	}

	_, err := peer.ReadHandshake(c1)
	if err != nil {
		t.Fatalf("reading reciprocal hs1: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	managed.mu.RLock()
	peers := managed.ActivePeers
	managed.mu.RUnlock()
	if peers != 1 {
		t.Fatalf("expected 1 active peer, got %d", peers)
	}

	// Second connection from SAME clientPeerID
	c2, s2 := net.Pipe()
	defer c2.Close()
	defer s2.Close()

	go eng.handleInboundConn(s2)

	hs2 := peer.NewHandshake(infoHash, clientPeerID)
	_, _ = c2.Write(hs2.Serialize())

	// AcceptHandshake sends reciprocal handshake before deduplication,
	// then handleInboundConn rejects duplicate and closes connection.
	_, _ = peer.ReadHandshake(c2)
	buf := make([]byte, 1024)
	_, readErr := c2.Read(buf)
	if readErr == nil {
		t.Errorf("expected duplicate connection to be closed, but was able to read")
	}

	time.Sleep(50 * time.Millisecond)
	managed.mu.RLock()
	peers = managed.ActivePeers
	managed.mu.RUnlock()
	if peers != 1 {
		t.Errorf("expected ActivePeers to remain 1 after duplicate rejection, got %d", peers)
	}

	// Close c1
	_ = c1.Close()
	time.Sleep(50 * time.Millisecond)

	managed.mu.RLock()
	peers = managed.ActivePeers
	managed.mu.RUnlock()
	if peers != 0 {
		t.Errorf("expected ActivePeers to become 0 after c1 close, got %d", peers)
	}
}

func TestInboundEmptyBitfield(t *testing.T) {
	eng := NewEngine("/tmp/downloads", 0)
	defer func() { _ = eng.Close() }()

	infoHash := [20]byte{4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4}
	clientPeerID := [20]byte{'-', 'E', 'M', 'P', 'T', '-', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4'}

	// PieceManager with empty bitfield (0 pieces owned)
	bf := make(peer.Bitfield, 2)
	pm := &storage.PieceManager{
		Bitfield: bf,
	}

	managed := &ManagedTorrent{
		ID:           1,
		Meta:         &metainfo.TorrentMeta{InfoHash: infoHash, Name: "empty_test"},
		PieceManager: pm,
		State:        StateDownloading,
		stopChan:     make(chan struct{}),
	}
	eng.mu.Lock()
	eng.torrents[infoHash] = managed
	eng.mu.Unlock()

	c, s := net.Pipe()
	defer c.Close()
	defer s.Close()

	go eng.handleInboundConn(s)

	hs := peer.NewHandshake(infoHash, clientPeerID)
	_, _ = c.Write(hs.Serialize())

	_, err := peer.ReadHandshake(c)
	if err != nil {
		t.Fatalf("reading reciprocal handshake: %v", err)
	}

	// Both sides support the Fast Extension, so an empty bitfield is sent as Have None
	// (BEP 0006 requires one of bitfield/Have All/Have None first), then interest.
	msg, err := peer.ReadMessage(c)
	if err != nil {
		t.Fatalf("reading first message: %v", err)
	}
	if msg.ID == nil || *msg.ID != peer.MsgHaveNone {
		t.Fatalf("expected MsgHaveNone first, got %v", msg.ID)
	}
	if msg, err = peer.ReadMessage(c); err != nil {
		t.Fatalf("reading second message: %v", err)
	}
	if msg.ID == nil || *msg.ID != peer.MsgInterested {
		t.Errorf("expected MsgInterested (ID 2), got %v", msg.ID)
	}
}

func TestInboundSeedingBehavior(t *testing.T) {
	eng := NewEngine("/tmp/downloads", 0)
	defer func() { _ = eng.Close() }()

	infoHash := [20]byte{5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5}
	clientPeerID := [20]byte{'-', 'S', 'E', 'E', 'D', '-', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4'}

	bf := make(peer.Bitfield, 2)
	bf.SetPiece(0)
	pm := &storage.PieceManager{
		Bitfield: bf,
	}

	managed := &ManagedTorrent{
		ID:           1,
		Meta:         &metainfo.TorrentMeta{InfoHash: infoHash, Name: "seeding_test"},
		PieceManager: pm,
		State:        StateSeeding,
		stopChan:     make(chan struct{}),
	}
	eng.mu.Lock()
	eng.torrents[infoHash] = managed
	eng.mu.Unlock()

	c, s := net.Pipe()
	defer c.Close()
	defer s.Close()

	go eng.handleInboundConn(s)

	hs := peer.NewHandshake(infoHash, clientPeerID)
	_, _ = c.Write(hs.Serialize())

	_, err := peer.ReadHandshake(c)
	if err != nil {
		t.Fatalf("reading reciprocal handshake: %v", err)
	}

	// First message should be MsgBitfield
	msg1, err := peer.ReadMessage(c)
	if err != nil {
		t.Fatalf("reading bitfield message: %v", err)
	}
	if msg1.ID == nil || *msg1.ID != peer.MsgBitfield {
		t.Errorf("expected MsgBitfield, got %v", msg1.ID)
	}

	// When seeding, engine should not express interest
	readDone := make(chan *peer.Message, 1)
	go func() {
		m, _ := peer.ReadMessage(c)
		readDone <- m
	}()

	select {
	case m := <-readDone:
		if m != nil && m.ID != nil && *m.ID == peer.MsgInterested {
			t.Errorf("did not expect MsgInterested when torrent is in seeding state")
		}
	case <-time.After(100 * time.Millisecond):
		// Expected: no MsgInterested sent
	}
}

func TestInboundMaxPeersLimit(t *testing.T) {
	eng := NewEngine("/tmp/downloads", 0)
	defer func() { _ = eng.Close() }()

	infoHash := [20]byte{6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6}
	clientPeerID := [20]byte{'-', 'M', 'A', 'X', 'P', '-', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4'}

	managed := &ManagedTorrent{
		ID:          1,
		Meta:        &metainfo.TorrentMeta{InfoHash: infoHash, Name: "max_peers_test"},
		State:       StateDownloading,
		ActivePeers: MaxPeersPerTorrent,
		stopChan:    make(chan struct{}),
	}
	eng.mu.Lock()
	eng.torrents[infoHash] = managed
	eng.mu.Unlock()

	c, s := net.Pipe()
	defer c.Close()
	defer s.Close()

	go eng.handleInboundConn(s)

	hs := peer.NewHandshake(infoHash, clientPeerID)
	_, _ = c.Write(hs.Serialize())

	_, err := peer.ReadHandshake(c)
	if err != nil {
		t.Fatalf("reading reciprocal handshake: %v", err)
	}

	buf := make([]byte, 1024)
	_, readErr := c.Read(buf)
	if readErr == nil {
		t.Errorf("expected connection to be closed due to max peers limit")
	}

	managed.mu.RLock()
	peers := managed.ActivePeers
	managed.mu.RUnlock()
	if peers != MaxPeersPerTorrent {
		t.Errorf("expected ActivePeers to remain %d, got %d", MaxPeersPerTorrent, peers)
	}
}
