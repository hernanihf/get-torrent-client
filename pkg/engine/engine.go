package engine

import (
	"crypto/rand"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hernan/get-torrent/pkg/metainfo"
	"github.com/hernan/get-torrent/pkg/peer"
	"github.com/hernan/get-torrent/pkg/storage"
	"github.com/hernan/get-torrent/pkg/tracker"
)

// TorrentState represents the current state of a managed torrent.
type TorrentState string

const (
	StateStopped     TorrentState = "stopped"
	StateQueued      TorrentState = "queued"
	StateDownloading TorrentState = "downloading"
	StateSeeding     TorrentState = "seeding"
	StateError       TorrentState = "error"
)

// ManagedTorrent wraps metadata, disk storage, and live connection state for a torrent.
type ManagedTorrent struct {
	ID           int
	Meta         *metainfo.TorrentMeta
	TorrentPath  string
	Storage      *storage.Storage
	PieceManager *storage.PieceManager
	State        TorrentState
	Downloaded   int64
	Uploaded     int64
	Left         int64
	Peers        []tracker.Peer
	ActivePeers  int
	activeConns  []*peer.Conn
	stopChan     chan struct{}
	mu           sync.RWMutex
}

// Engine orchestrates torrent downloads, peer connections, tracker announces, and session state.
type Engine struct {
	PeerID           [20]byte
	Port             uint16
	DownloadDir      string
	ConfigDir        string
	SpeedLimitDown   int64
	SpeedLimitUp     int64
	torrents         map[[20]byte]*ManagedTorrent
	mu               sync.RWMutex
	nextID           int
	listener         net.Listener
	listenerStopChan chan struct{}
}

// NewEngine creates and initializes a BitTorrent engine.
func NewEngine(downloadDir string, port uint16) *Engine {
	var peerID [20]byte
	copy(peerID[:8], []byte("-GT0001-")) // Client prefix: Get-Torrent 0.0.1
	_, _ = rand.Read(peerID[8:])

	return &Engine{
		PeerID:           peerID,
		Port:             port,
		DownloadDir:      downloadDir,
		ConfigDir:        "./config",
		torrents:         make(map[[20]byte]*ManagedTorrent),
		nextID:           1,
		listenerStopChan: make(chan struct{}),
	}
}

// AddTorrentFile loads a .torrent file, allocates disk storage, and registers it.
func (e *Engine) AddTorrentFile(path string) (*ManagedTorrent, error) {
	meta, err := metainfo.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening torrent file: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if t, exists := e.torrents[meta.InfoHash]; exists {
		return t, nil
	}

	store, err := storage.NewStorage(e.DownloadDir, meta)
	if err != nil {
		return nil, fmt.Errorf("allocating storage: %w", err)
	}

	pm := storage.NewPieceManager(meta, store)

	t := &ManagedTorrent{
		ID:           e.nextID,
		Meta:         meta,
		TorrentPath:  path,
		Storage:      store,
		PieceManager: pm,
		State:        StateQueued,
		Downloaded:   0,
		Uploaded:     0,
		Left:         meta.TotalLength,
		stopChan:     make(chan struct{}),
	}
	e.nextID++
	e.torrents[meta.InfoHash] = t

	log.Printf("[Engine] Added torrent: %q (%d MB, %d pieces)",
		meta.Name, meta.TotalLength/(1024*1024), len(meta.Pieces))

	return t, nil
}

// GetTorrentByID returns a managed torrent by its unique integer ID.
func (e *Engine) GetTorrentByID(id int) *ManagedTorrent {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, t := range e.torrents {
		if t.ID == id {
			return t
		}
	}
	return nil
}

// StartTorrent begins tracker announcing and peer connections for a given torrent ID.
func (e *Engine) StartTorrent(id int) error {
	t := e.GetTorrentByID(id)
	if t == nil {
		return fmt.Errorf("torrent with id %d not found", id)
	}

	t.mu.Lock()
	if t.State == StateDownloading || t.State == StateSeeding {
		t.mu.Unlock()
		return nil // Already running
	}

	t.State = StateDownloading
	t.stopChan = make(chan struct{})
	t.mu.Unlock()

	go e.runTorrent(t)
	return nil
}

// StopTorrent pauses/stops a torrent and terminates all active peer connections.
func (e *Engine) StopTorrent(id int) error {
	t := e.GetTorrentByID(id)
	if t == nil {
		return fmt.Errorf("torrent with id %d not found", id)
	}

	t.mu.Lock()
	if t.State == StateStopped {
		t.mu.Unlock()
		return nil
	}

	t.State = StateStopped
	select {
	case <-t.stopChan:
		// already closed
	default:
		close(t.stopChan)
	}

	// Close all active peer connections
	for _, c := range t.activeConns {
		_ = c.Close()
	}
	t.activeConns = nil
	t.ActivePeers = 0
	t.mu.Unlock()

	log.Printf("[Engine] Stopped torrent: %q (ID: %d)", t.Meta.Name, t.ID)
	return nil
}

// RemoveTorrent removes a torrent from the engine, optionally deleting files from disk.
func (e *Engine) RemoveTorrent(id int, deleteFiles bool) error {
	t := e.GetTorrentByID(id)
	if t == nil {
		return fmt.Errorf("torrent with id %d not found", id)
	}

	// 1. Stop first
	_ = e.StopTorrent(id)

	// 2. Remove from active map
	e.mu.Lock()
	delete(e.torrents, t.Meta.InfoHash)
	e.mu.Unlock()

	// 3. Close storage file handles
	if t.Storage != nil {
		_ = t.Storage.Close()
	}

	// 4. Optionally delete files from disk
	if deleteFiles && t.Storage != nil {
		for _, f := range t.Storage.Files {
			_ = os.Remove(f.FullPath)
		}
	}

	log.Printf("[Engine] Removed torrent: %q (ID: %d, deleteFiles: %v)", t.Meta.Name, t.ID, deleteFiles)
	return nil
}

func (e *Engine) runTorrent(t *ManagedTorrent) {
	log.Printf("[Engine] Starting download loop for %q", t.Meta.Name)

	// 1. Announce to Tracker
	resp, err := tracker.Announce(tracker.AnnounceParams{
		AnnounceURL: t.Meta.Announce,
		InfoHash:    t.Meta.InfoHash,
		PeerID:      e.PeerID,
		Port:        e.Port,
		Uploaded:    t.Uploaded,
		Downloaded:  t.Downloaded,
		Left:        t.Left,
		Compact:     true,
	})

	if err != nil {
		log.Printf("[Engine] Tracker announce failed for %q: %v", t.Meta.Name, err)
		t.mu.Lock()
		t.State = StateError
		t.mu.Unlock()
		return
	}

	t.mu.Lock()
	t.Peers = resp.Peers
	log.Printf("[Engine] Tracker returned %d peers for %q", len(resp.Peers), t.Meta.Name)
	t.mu.Unlock()

	// 2. Connect to available peers concurrently
	var wg sync.WaitGroup
	for _, p := range resp.Peers {
		select {
		case <-t.stopChan:
			return // Torrent was stopped
		default:
		}

		wg.Add(1)
		go func(peerEndpoint tracker.Peer) {
			defer wg.Done()
			conn, err := peer.Dial(peerEndpoint, t.Meta.InfoHash, e.PeerID, 8*time.Second)
			if err != nil {
				return
			}
			defer conn.Close()

			// Register peer's initial bitfield into Rarest-First availability map
			if len(conn.Bitfield) > 0 {
				t.PieceManager.OnPeerBitfield(conn.Bitfield)
			}

			t.mu.Lock()
			select {
			case <-t.stopChan:
				t.mu.Unlock()
				return
			default:
			}
			t.ActivePeers++
			t.activeConns = append(t.activeConns, conn)
			t.mu.Unlock()

			defer func() {
				// Deregister peer's pieces from availability map
				t.PieceManager.OnPeerDisconnect(conn.Bitfield)

				t.mu.Lock()
				if t.ActivePeers > 0 {
					t.ActivePeers--
				}
				for i, c := range t.activeConns {
					if c == conn {
						t.activeConns = append(t.activeConns[:i], t.activeConns[i+1:]...)
						break
					}
				}
				t.mu.Unlock()
			}()

			log.Printf("[Engine] Connected to peer %s (PeerID: %x)", peerEndpoint.String(), conn.PeerID[:8])

			// Express interest in downloading
			_ = conn.SendInterested()

			// Start block request, message loop, and serving upload requests
			e.handlePeerMessages(t, conn)
		}(p)
	}
}

func (e *Engine) handlePeerMessages(t *ManagedTorrent, conn *peer.Conn) {
	peerAddr := conn.RemotePeer.String()

	requestMoreBlocks := func() {
		if conn.Choked || t.PieceManager.IsComplete() {
			return
		}
		// Request up to 4 pipelined blocks using Rarest-First scheduling
		for i := 0; i < 4; i++ {
			req, ok := t.PieceManager.NextRequest(conn.Bitfield)
			if !ok {
				break
			}
			t.PieceManager.RegisterRequest(req.PieceIndex, req.Begin, peerAddr)
			_ = conn.SendRequest(req.PieceIndex, req.Begin, req.Length)
		}
	}

	for {
		select {
		case <-t.stopChan:
			return // Torrent stopped by user/RPC
		default:
		}

		msg, err := conn.ReadMsg()
		if err != nil {
			break
		}
		if msg.ID == nil {
			continue // Keep-alive
		}

		switch *msg.ID {
		case peer.MsgUnchoke:
			log.Printf("[Engine] Peer %s unchoked us! Requesting blocks (Rarest-First)...", peerAddr)
			requestMoreBlocks()

		case peer.MsgChoke:
			log.Printf("[Engine] Peer %s choked us.", peerAddr)

		case peer.MsgInterested:
			// Unchoke interested peer so we seed/upload to them
			_ = conn.SendUnchoke()

		case peer.MsgHave:
			if idx, err := peer.ParseHave(msg); err == nil {
				t.PieceManager.OnPeerHave(idx)
			}
			requestMoreBlocks()

		case peer.MsgBitfield:
			t.PieceManager.OnPeerBitfield(conn.Bitfield)
			requestMoreBlocks()

		case peer.MsgPiece:
			if len(msg.Payload) < 8 {
				continue
			}
			index := int(uint32(msg.Payload[0])<<24 | uint32(msg.Payload[1])<<16 | uint32(msg.Payload[2])<<8 | uint32(msg.Payload[3]))
			begin := int(uint32(msg.Payload[4])<<24 | uint32(msg.Payload[5])<<16 | uint32(msg.Payload[6])<<8 | uint32(msg.Payload[7]))
			data := msg.Payload[8:]

			// Endgame Mode optimization: cancel redundant requests sent to other peers
			peersToCancel := t.PieceManager.GetPeersToCancel(index, begin, peerAddr)
			if len(peersToCancel) > 0 {
				e.broadcastCancel(t, index, begin, len(data), peersToCancel)
			}

			completed, err := t.PieceManager.ReceiveBlock(index, begin, data)
			if err != nil {
				log.Printf("[Engine] Error processing block: %v", err)
			}

			if completed {
				pct, dl := t.PieceManager.Progress()
				t.mu.Lock()
				t.Downloaded = dl
				t.Left = t.Meta.TotalLength - dl
				t.mu.Unlock()

				log.Printf("[Engine] Piece %d verified (SHA-1 OK). Progress: %.1f%% (%d/%d bytes)",
					index, pct, dl, t.Meta.TotalLength)

				// Broadcast MsgHave to all active peers
				e.broadcastHave(t, index)

				if t.PieceManager.IsComplete() {
					log.Printf("[Engine] 🎉 Torrent %q download complete! Seeding now.", t.Meta.Name)
					t.mu.Lock()
					t.State = StateSeeding
					t.mu.Unlock()
					return
				}
			}

			// Pipeline next blocks
			requestMoreBlocks()

		case peer.MsgRequest:
			// Seeding: remote peer is requesting a block from us
			reqIdx, reqBegin, reqLen, err := peer.ParseRequest(msg)
			if err != nil {
				continue
			}

			// Check if we have this piece and we are willing to upload
			if !conn.AmChoking && t.PieceManager.Bitfield.HasPiece(reqIdx) {
				pieceLen := int(t.Meta.PieceLength)
				if reqIdx == len(t.Meta.Pieces)-1 && t.Meta.TotalLength%t.Meta.PieceLength != 0 {
					pieceLen = int(t.Meta.TotalLength % t.Meta.PieceLength)
				}

				pieceData, err := t.Storage.ReadPiece(reqIdx, pieceLen)
				if err == nil && reqBegin+reqLen <= len(pieceData) {
					block := pieceData[reqBegin : reqBegin+reqLen]
					_ = conn.SendPiece(reqIdx, reqBegin, block)

					t.mu.Lock()
					t.Uploaded += int64(len(block))
					t.mu.Unlock()
				}
			}
		}
	}
}

func (e *Engine) broadcastHave(t *ManagedTorrent, pieceIndex int) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	haveMsg := peer.FormatHave(pieceIndex)
	for _, c := range t.activeConns {
		_ = c.SendMessage(haveMsg)
	}
}

func (e *Engine) broadcastCancel(t *ManagedTorrent, index, begin, length int, peerAddrs []string) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	addrMap := make(map[string]bool, len(peerAddrs))
	for _, a := range peerAddrs {
		addrMap[a] = true
	}

	cancelMsg := peer.FormatCancel(index, begin, length)
	for _, c := range t.activeConns {
		if addrMap[c.RemotePeer.String()] {
			_ = c.SendMessage(cancelMsg)
		}
	}
}

// ListTorrents returns a snapshot list of all managed torrents.
func (e *Engine) ListTorrents() []*ManagedTorrent {
	e.mu.RLock()
	defer e.mu.RUnlock()

	list := make([]*ManagedTorrent, 0, len(e.torrents))
	for _, t := range e.torrents {
		list = append(list, t)
	}
	return list
}

// Close stops the peer listener and gracefully stops all managed torrents.
func (e *Engine) Close() error {
	var errs []error
	if err := e.StopListener(); err != nil {
		errs = append(errs, err)
	}

	e.mu.RLock()
	ids := make([]int, 0, len(e.torrents))
	for _, t := range e.torrents {
		ids = append(ids, t.ID)
	}
	e.mu.RUnlock()

	for _, id := range ids {
		if err := e.StopTorrent(id); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing engine: %v", errs)
	}
	return nil
}

