package engine

import (
	"context"
	"encoding/binary"
	"errors"
	"log"
	"time"

	"github.com/hernan/get-torrent/pkg/peer"
	"github.com/hernan/get-torrent/pkg/storage"
)

const (
	// PeerIdleTimeout drops peers that send nothing (not even keep-alives) for this long.
	PeerIdleTimeout = 3 * time.Minute

	// KeepAliveInterval is how often we send a keep-alive to a peer we have not written to.
	KeepAliveInterval = 90 * time.Second

	// peerTickInterval drives periodic per-peer work (metadata re-requests, keep-alives).
	peerTickInterval = 10 * time.Second

	// maxInflightBlocks caps pipelined block requests outstanding to one peer.
	maxInflightBlocks = 8

	// maxMetadataRequests caps ut_metadata piece requests issued to one peer at a time.
	maxMetadataRequests = 8
)

// peerSession holds the per-connection state of the message loop. Only the goroutine running
// handlePeerMessages touches it (and the Conn's state fields), so no locking is needed; the
// exception is uploads, which the uploader goroutine shares and which locks itself.
type peerSession struct {
	e        *Engine
	t        *ManagedTorrent
	conn     *peer.Conn
	info     *peerInfo       // reporting record; nil for unregistered connections (tests)
	ctx      context.Context // cancelled when the session ends or the torrent stops
	addr     string
	pm       *storage.PieceManager // set once this peer's pieces are counted in availability
	inflight int
	extDone  bool         // peer's extension handshake has been processed
	uploads  *uploadQueue // shared with the uploader goroutine
	lastSent time.Time
	now      func() time.Time
}

// handlePeerMessages runs the message loop for an established, registered connection until the
// peer disconnects, goes idle, or the torrent is stopped. The caller closes the connection.
func (e *Engine) handlePeerMessages(t *ManagedTorrent, conn *peer.Conn) {
	s := &peerSession{
		e:        e,
		t:        t,
		conn:     conn,
		addr:     conn.RemotePeer.String(),
		lastSent: time.Now(),
		now:      time.Now,
		info:     t.peerInfoFor(conn),
		uploads:  newUploadQueue(),
	}
	defer s.detach()

	t.mu.RLock()
	stop := t.stopChan
	var metaReady <-chan struct{} // nil (never ready) once metadata is known
	if t.PieceManager == nil {
		metaReady = t.metaReady
	}
	t.mu.RUnlock()

	// Bandwidth throttling may block; it must end promptly on stop or disconnect.
	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()
	go func() {
		select {
		case <-stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	s.ctx = ctx
	go s.runUploader(ctx)

	// A dedicated reader goroutine lets the loop below react to stop signals and timers
	// without abandoning a partially read message.
	msgs := make(chan *peer.Message)
	done := make(chan struct{})
	defer close(done)
	go func() {
		defer close(msgs)
		for {
			_ = conn.Conn.SetReadDeadline(time.Now().Add(PeerIdleTimeout))
			msg, err := peer.ReadMessage(conn.Conn)
			if err != nil {
				return
			}
			select {
			case msgs <- msg:
			case <-done:
				return
			}
			// Download limits: pausing before the next read lets TCP flow control slow the peer.
			if msg.ID != nil && *msg.ID == peer.MsgPiece && len(msg.Payload) > 8 {
				if e.throttle(ctx, t, false, len(msg.Payload)-8) != nil {
					return
				}
			}
		}
	}()

	if conn.SupportsExtensions {
		s.send(peer.FormatExtHandshake(t.metadataSize()))
	}
	if server := e.dhtServer(); server != nil && conn.SupportsDHT {
		s.send(peer.FormatPort(uint16(server.Addr().Port)))
	}
	s.attach()
	s.onExtHandshake() // the handshake may have arrived during Dial
	s.requestBlocks()

	ticker := time.NewTicker(peerTickInterval)
	defer ticker.Stop()

	var chokeReq <-chan bool // nil (never ready) for unregistered connections
	if s.info != nil {
		chokeReq = s.info.chokeReq
	}

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			s.tick()
		case choke := <-chokeReq:
			s.applyChoke(choke)
		case <-metaReady:
			metaReady = nil
			if s.attach() {
				s.requestBlocks()
			}
		case msg, ok := <-msgs:
			if !ok {
				return
			}
			conn.UpdateState(msg)
			s.handle(msg)
			s.publish()
			if msg.ID != nil && (*msg.ID == peer.MsgInterested || *msg.ID == peer.MsgNotInterested) {
				t.requestRechoke() // after publish, so the choker sees the new interest
			}
		}
	}
}

// publish exposes the connection's state for reporting (torrent-get "peers").
func (s *peerSession) publish() {
	if s.info != nil {
		s.info.publish(s.conn)
	}
}

func (s *peerSession) send(msg *peer.Message) {
	_ = s.conn.SendMessage(msg)
	s.lastSent = s.now()
}

// attach counts this peer's pieces in the rarest-first availability map once metadata is known.
// It reports whether attachment happened on this call.
func (s *peerSession) attach() bool {
	if s.pm != nil {
		return false
	}
	_, pm, _ := s.t.components()
	if pm == nil {
		return false
	}
	pm.OnPeerBitfield(s.conn.Bitfield)
	s.pm = pm
	return true
}

func (s *peerSession) detach() {
	if s.pm != nil {
		s.pm.OnPeerDisconnect(s.conn.Bitfield)
	}
}

func (s *peerSession) onExtHandshake() {
	if s.extDone || s.conn.PeerExt == nil {
		return
	}
	s.extDone = true
	s.requestMetadata()
}

func (s *peerSession) tick() {
	if s.attach() {
		s.requestBlocks()
	}
	s.requestMetadata() // re-issues timed-out metadata requests
	if s.now().Sub(s.lastSent) >= KeepAliveInterval {
		s.send(&peer.Message{})
	}
}

// requestBlocks pipelines block requests to the peer using rarest-first scheduling.
func (s *peerSession) requestBlocks() {
	if s.pm == nil || s.conn.Choked || s.pm.IsComplete() {
		return
	}
	for s.inflight < maxInflightBlocks {
		req, ok := s.pm.NextRequest(s.conn.Bitfield)
		if !ok {
			break
		}
		s.pm.RegisterRequest(req.PieceIndex, req.Begin, s.addr)
		s.send(peer.FormatRequest(req.PieceIndex, req.Begin, req.Length))
		s.inflight++
	}
}

func (s *peerSession) handle(msg *peer.Message) {
	// Metadata may have become available since the last message (magnet links); if this peer is
	// attached now, its whole bitfield (including this message's effect) has just been counted.
	justAttached := s.attach()
	if msg.ID == nil {
		return // Keep-alive
	}

	switch *msg.ID {
	case peer.MsgUnchoke:
		log.Printf("[Engine] Peer %s unchoked us! Requesting blocks (Rarest-First)...", s.addr)
		s.requestBlocks()

	case peer.MsgChoke:
		// A choking peer discards our pending requests.
		s.inflight = 0

	case peer.MsgHave:
		if s.pm != nil && !justAttached {
			if idx, err := peer.ParseHave(msg); err == nil {
				s.pm.OnPeerHave(idx)
			}
		}
		s.requestBlocks()

	case peer.MsgBitfield:
		if s.pm != nil && !justAttached {
			s.pm.OnPeerBitfield(s.conn.Bitfield)
		}
		s.requestBlocks()

	case peer.MsgPiece:
		s.handlePiece(msg)

	case peer.MsgRequest:
		s.handleRequest(msg)

	case peer.MsgCancel:
		s.handleCancel(msg)

	case peer.MsgPort:
		if port, err := peer.ParsePort(msg); err == nil {
			s.e.addDHTNode(s.conn.RemotePeer.IP, port)
		}

	case peer.MsgExtended:
		extID, payload, err := peer.ParseExtended(msg)
		if err != nil {
			return
		}
		switch extID {
		case peer.ExtHandshakeID:
			s.onExtHandshake()
		case peer.LocalUTMetadataID:
			s.handleMetadataMsg(payload)
		}
	}
}

func (s *peerSession) handlePiece(msg *peer.Message) {
	if s.pm == nil || len(msg.Payload) < 8 {
		return
	}
	index := int(binary.BigEndian.Uint32(msg.Payload[0:4]))
	begin := int(binary.BigEndian.Uint32(msg.Payload[4:8]))
	data := msg.Payload[8:]
	if s.inflight > 0 {
		s.inflight--
	}
	s.e.recordDownload(s.t, s.info, len(data))

	// Endgame Mode optimization: cancel redundant requests sent to other peers
	if peersToCancel := s.pm.GetPeersToCancel(index, begin, s.addr); len(peersToCancel) > 0 {
		s.e.broadcastCancel(s.t, index, begin, len(data), peersToCancel)
	}

	completed, err := s.pm.ReceiveBlock(index, begin, data)
	if err != nil {
		log.Printf("[Engine] Error processing block: %v", err)
		if errors.Is(err, storage.ErrPieceCorrupted) && index >= 0 && index < len(s.pm.Pieces) {
			s.t.mu.Lock()
			s.t.CorruptEver += int64(s.pm.Pieces[index].Length)
			s.t.mu.Unlock()
		}
	}

	if completed {
		meta, _, _ := s.t.components()
		pct, dl := s.pm.Progress()
		s.t.mu.Lock()
		s.t.Left = meta.TotalLength - dl
		s.t.mu.Unlock()

		log.Printf("[Engine] Piece %d verified (SHA-1 OK). Progress: %.1f%% (%d/%d bytes)",
			index, pct, dl, meta.TotalLength)

		// Broadcast MsgHave to all active peers
		s.e.broadcastHave(s.t, index)

		if s.pm.IsComplete() {
			s.t.mu.Lock()
			if s.t.State == StateDownloading {
				s.t.setStateLocked(StateSeeding)
				s.t.downloadFinishedLocked()
				log.Printf("[Engine] 🎉 Torrent %q download complete! Seeding now.", meta.Name)
			}
			s.t.mu.Unlock()
		}
	}

	s.requestBlocks()
}

// handleRequest queues a block request from a peer we have unchoked; the uploader serves it.
func (s *peerSession) handleRequest(msg *peer.Message) {
	reqIdx, reqBegin, reqLen, err := peer.ParseRequest(msg)
	if err != nil || s.conn.AmChoking {
		return
	}
	// Requests larger than 128 KiB are a protocol violation and a memory-abuse vector.
	if reqLen <= 0 || reqLen > 128*1024 || reqBegin < 0 || reqIdx < 0 {
		return
	}
	s.uploads.push(blockRequest{index: reqIdx, begin: reqBegin, length: reqLen})
}

// handleCancel withdraws a request the peer no longer needs (typically in its endgame).
func (s *peerSession) handleCancel(msg *peer.Message) {
	if idx, begin, length, err := peer.ParseCancel(msg); err == nil {
		s.uploads.cancel(blockRequest{index: idx, begin: begin, length: length})
	}
}

func idPtr(id peer.MessageID) *peer.MessageID { return &id }
