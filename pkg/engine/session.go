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

	// maxInflightBlocks caps pipelined block requests outstanding to one peer.
	maxInflightBlocks = 8

	// maxMetadataRequests caps ut_metadata piece requests issued to one peer at a time.
	maxMetadataRequests = 8
)

// peerTickInterval drives periodic per-peer work (metadata re-requests, keep-alives, PEX).
// Each engine copies it when created (Engine.timing), so tests can speed up their own engines
// without racing with sessions of other engines.
var peerTickInterval = 10 * time.Second

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
	pending  map[blockKey]struct{} // blocks requested from this peer and not yet answered
	extDone  bool                  // peer's extension handshake has been processed
	uploads  *uploadQueue          // shared with the uploader goroutine
	pex      pexState
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
		pending:  make(map[blockKey]struct{}),
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
		s.send(peer.FormatExtHandshakeWith(peer.ExtHandshakeOptions{
			MetadataSize: t.metadataSize(),
			ListenPort:   e.Port,
			PEX:          e.pexAllowed(t),
		}))
	}
	if server := e.dhtServer(); server != nil && conn.SupportsDHT {
		s.send(peer.FormatPort(uint16(server.Addr().Port)))
	}
	s.attach()
	s.onExtHandshake() // the handshake may have arrived during Dial
	s.requestBlocks()

	ticker := time.NewTicker(e.timing.peerTick)
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
	if s.conn.PeerHasAll {
		s.conn.Bitfield = fullBitfield(len(pm.Pieces))
	}
	pm.OnPeerBitfield(s.conn.Bitfield)
	s.pm = pm
	return true
}

func (s *peerSession) detach() {
	if s.pm != nil {
		s.releasePending()
		s.pm.OnPeerDisconnect(s.conn.Bitfield)
	}
}

// fullBitfield returns a bitfield with all n pieces set (a Have All peer).
func fullBitfield(n int) peer.Bitfield {
	bf := make(peer.Bitfield, (n+7)/8)
	for i := 0; i < n; i++ {
		bf.SetPiece(i)
	}
	return bf
}

// blockKey identifies a requested block.
type blockKey struct{ index, begin int }

// releasePending gives back every outstanding request, so other peers can be asked.
func (s *peerSession) releasePending() {
	for k := range s.pending {
		s.pm.ReleaseRequest(k.index, k.begin, s.addr)
	}
	clear(s.pending)
}

// prunePending forgets requests whose block arrived from another peer (endgame).
func (s *peerSession) prunePending() {
	for k := range s.pending {
		if s.pm.BlockReceived(k.index, k.begin) {
			delete(s.pending, k)
		}
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
	s.attach()
	s.requestBlocks()   // also picks up files selected since the last peer event
	s.requestMetadata() // re-issues timed-out metadata requests
	s.sendPex()
	if s.now().Sub(s.lastSent) >= KeepAliveInterval {
		s.send(&peer.Message{})
	}
}

// requestBlocks pipelines block requests to the peer using rarest-first scheduling.
func (s *peerSession) requestBlocks() {
	if s.pm == nil || s.conn.Choked || s.pm.IsDone() {
		return
	}
	s.prunePending()
	for len(s.pending) < maxInflightBlocks {
		req, ok := s.pm.NextRequest(s.conn.Bitfield)
		if !ok {
			break
		}
		key := blockKey{req.PieceIndex, req.Begin}
		if _, dup := s.pending[key]; dup {
			break // endgame offers a block already requested from this peer
		}
		s.pm.RegisterRequest(req.PieceIndex, req.Begin, s.addr)
		s.send(peer.FormatRequest(req.PieceIndex, req.Begin, req.Length))
		s.pending[key] = struct{}{}
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
		// Without the Fast Extension a choking peer silently discards our requests; with it,
		// each one is answered by a piece or a reject.
		if s.pm != nil && !s.conn.SupportsFast {
			s.releasePending()
		}

	case peer.MsgRejectRequest:
		if idx, begin, _, err := peer.ParseReject(msg); err == nil && s.pm != nil {
			if _, ok := s.pending[blockKey{idx, begin}]; ok {
				delete(s.pending, blockKey{idx, begin})
				s.pm.ReleaseRequest(idx, begin, s.addr)
			}
		}
		s.requestBlocks()

	case peer.MsgHaveAll:
		if s.pm != nil && !justAttached {
			s.conn.Bitfield = fullBitfield(len(s.pm.Pieces))
			s.pm.OnPeerBitfield(s.conn.Bitfield)
		}
		s.requestBlocks()

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
		case peer.LocalUTPexID:
			s.handlePex(payload)
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
	delete(s.pending, blockKey{index, begin})
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
		s.t.Left = s.pm.Left()
		s.t.mu.Unlock()

		log.Printf("[Engine] Piece %d verified (SHA-1 OK). Progress: %.1f%% (%d/%d bytes)",
			index, pct, dl, meta.TotalLength)

		// Broadcast MsgHave to all active peers
		s.e.broadcastHave(s.t, index)

		if s.pm.IsDone() {
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
// Fast Extension peers get a reject for any request we will not serve.
func (s *peerSession) handleRequest(msg *peer.Message) {
	reqIdx, reqBegin, reqLen, err := peer.ParseRequest(msg)
	// Requests larger than 128 KiB are a protocol violation and a memory-abuse vector.
	if err != nil || reqLen <= 0 || reqLen > 128*1024 || reqBegin < 0 || reqIdx < 0 {
		return
	}
	r := blockRequest{index: reqIdx, begin: reqBegin, length: reqLen}
	if s.conn.AmChoking || s.uploads.push(r) == pushFull {
		s.reject(r)
	}
}

// handleCancel withdraws a request the peer no longer needs (typically in its endgame). With the
// Fast Extension the withdrawn request is answered by a reject.
func (s *peerSession) handleCancel(msg *peer.Message) {
	if idx, begin, length, err := peer.ParseCancel(msg); err == nil {
		r := blockRequest{index: idx, begin: begin, length: length}
		if s.uploads.cancel(r) {
			s.reject(r)
		}
	}
}

// reject tells a Fast Extension peer that a request will not be served; others get nothing.
// Safe from the uploader goroutine: SupportsFast is fixed after the handshake.
func (s *peerSession) reject(r blockRequest) {
	if s.conn.SupportsFast {
		_ = s.conn.SendMessage(peer.FormatReject(r.index, r.begin, r.length))
	}
}

func idPtr(id peer.MessageID) *peer.MessageID { return &id }

// sendHaveState tells a new peer which pieces we have; it must be the first message after the
// handshake. Fast Extension peers get Have All or Have None when they fit, and must get one of
// the three; others get a bitfield only if we have something.
func sendHaveState(t *ManagedTorrent, conn *peer.Conn) {
	_, pm, _ := t.components()
	var have peer.Bitfield
	complete := false
	if pm != nil {
		have = pm.Snapshot().Have
		complete = len(pm.Pieces) > 0 && pm.IsComplete()
	}
	switch {
	case conn.SupportsFast && complete:
		_ = conn.SendMessage(peer.FormatHaveAll())
	case conn.SupportsFast && !have.HasAnyPiece():
		_ = conn.SendMessage(peer.FormatHaveNone())
	case have.HasAnyPiece():
		_ = conn.SendBitfield(have)
	}
}
