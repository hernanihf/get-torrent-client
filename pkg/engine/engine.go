package engine

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hernan/get-torrent/pkg/metadata"
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
//
// For torrents added from a magnet link, Meta initially holds only the info_hash and display name;
// Storage and PieceManager stay nil until the info dictionary is fetched from peers (BEP 0009).
// Fields that change after creation must be read under mu (see Status and components).
type ManagedTorrent struct {
	ID           int
	Meta         *metainfo.TorrentMeta
	TorrentPath  string
	MagnetURI    string
	Storage      *storage.Storage
	PieceManager *storage.PieceManager
	State        TorrentState
	Downloaded   int64
	Uploaded     int64
	Left         int64
	Peers        []tracker.Peer
	ActivePeers  int
	LastError    string
	activeConns  []*peer.Conn
	stopChan     chan struct{}
	trackers     []string            // magnet trackers, used until metadata provides its own
	metadata     *metadata.Assembler // non-nil while metadata is being fetched
	mu           sync.RWMutex
}

// components returns the metadata-dependent parts of a torrent; pm and store are nil for a magnet
// that has not yet fetched its metadata.
func (t *ManagedTorrent) components() (*metainfo.TorrentMeta, *storage.PieceManager, *storage.Storage) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Meta, t.PieceManager, t.Storage
}

// TorrentStatus is a consistent snapshot of a torrent for reporting (e.g. RPC torrent-get).
type TorrentStatus struct {
	ID                      int
	Name                    string
	InfoHash                [20]byte
	State                   TorrentState
	TotalSize               int64
	PercentDone             float64 // 0..1
	MetadataPercentComplete float64 // 0..1
	Left                    int64
	Downloaded              int64
	Uploaded                int64
	ActivePeers             int
	MagnetURI               string
	Error                   string
}

// Status returns a race-free snapshot of the torrent's state.
func (t *ManagedTorrent) Status() TorrentStatus {
	t.mu.RLock()
	st := TorrentStatus{
		ID:          t.ID,
		Name:        t.Meta.Name,
		InfoHash:    t.Meta.InfoHash,
		State:       t.State,
		TotalSize:   t.Meta.TotalLength,
		Left:        t.Left,
		Downloaded:  t.Downloaded,
		Uploaded:    t.Uploaded,
		ActivePeers: t.ActivePeers,
		MagnetURI:   t.MagnetURI,
		Error:       t.LastError,
	}
	pm, asm := t.PieceManager, t.metadata
	t.mu.RUnlock()

	if st.Name == "" {
		st.Name = hex.EncodeToString(st.InfoHash[:])
	}
	switch {
	case pm != nil:
		pct, _ := pm.Progress()
		st.PercentDone = pct / 100.0
		st.MetadataPercentComplete = 1
	case asm != nil:
		st.MetadataPercentComplete = asm.Progress()
	}
	return st
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
	return e.addMeta(meta, path)
}

// AddTorrentBytes registers a torrent from raw .torrent contents (e.g. RPC "metainfo").
// A copy is saved under ConfigDir so the torrent survives restarts.
func (e *Engine) AddTorrentBytes(data []byte) (*ManagedTorrent, error) {
	meta, err := metainfo.Parse(data)
	if err != nil {
		return nil, fmt.Errorf("parsing torrent: %w", err)
	}
	path, err := e.saveTorrentFile(meta)
	if err != nil {
		return nil, err
	}
	return e.addMeta(meta, path)
}

func (e *Engine) addMeta(meta *metainfo.TorrentMeta, path string) (*ManagedTorrent, error) {
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

	if t.PieceManager != nil && t.PieceManager.IsComplete() {
		t.State = StateSeeding
	} else {
		t.State = StateDownloading
	}
	t.LastError = ""
	t.stopChan = make(chan struct{})
	stop := t.stopChan
	t.mu.Unlock()

	go e.runTorrent(t, stop)
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
	name := t.Meta.Name
	t.mu.Unlock()

	log.Printf("[Engine] Stopped torrent: %q (ID: %d)", name, t.ID)
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

	meta, _, store := t.components()

	// 2. Remove from active map
	e.mu.Lock()
	delete(e.torrents, meta.InfoHash)
	e.mu.Unlock()

	// 3. Close storage file handles
	if store != nil {
		_ = store.Close()
	}

	// 4. Optionally delete files from disk
	if deleteFiles && store != nil {
		for _, f := range store.Files {
			_ = os.Remove(f.FullPath)
		}
	}

	log.Printf("[Engine] Removed torrent: %q (ID: %d, deleteFiles: %v)", meta.Name, t.ID, deleteFiles)
	return nil
}

func (e *Engine) setError(t *ManagedTorrent, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Only flag an error if the torrent has not been stopped meanwhile.
	if t.State == StateDownloading || t.State == StateSeeding {
		t.State = StateError
		t.LastError = err.Error()
	}
}

func (e *Engine) runTorrent(t *ManagedTorrent, stop <-chan struct{}) {
	meta, _, _ := t.components()
	log.Printf("[Engine] Starting download loop for %q", meta.Name)

	// 1. Announce to trackers
	peers, err := e.announce(t)
	if err != nil {
		log.Printf("[Engine] Tracker announce failed for %q: %v", meta.Name, err)
		e.setError(t, err)
		return
	}

	t.mu.Lock()
	t.Peers = peers
	t.mu.Unlock()
	log.Printf("[Engine] Trackers returned %d peers for %q", len(peers), meta.Name)

	// 2. Connect to available peers concurrently
	for _, p := range peers {
		select {
		case <-stop:
			return // Torrent was stopped
		default:
		}

		go func(peerEndpoint tracker.Peer) {
			conn, err := peer.Dial(peerEndpoint, meta.InfoHash, e.PeerID, 8*time.Second)
			if err != nil {
				return
			}
			defer conn.Close()

			if !e.registerConn(t, conn) {
				return
			}
			defer e.unregisterConn(t, conn)

			log.Printf("[Engine] Connected to peer %s (PeerID: %x)", peerEndpoint.String(), conn.PeerID[:8])

			// Express interest in downloading (or in metadata, for magnets)
			_ = conn.SendInterested()

			e.handlePeerMessages(t, conn)
		}(p)
	}
}

// registerConn adds a connection to the torrent's active set, enforcing the peer limit and
// de-duplicating by peer ID. It returns false if the connection must be dropped.
func (e *Engine) registerConn(t *ManagedTorrent, conn *peer.Conn) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.State != StateDownloading && t.State != StateSeeding {
		return false
	}
	select {
	case <-t.stopChan:
		return false
	default:
	}
	for _, c := range t.activeConns {
		if c.PeerID == conn.PeerID {
			return false
		}
	}
	if t.ActivePeers >= MaxPeersPerTorrent {
		return false
	}
	t.ActivePeers++
	t.activeConns = append(t.activeConns, conn)
	return true
}

func (e *Engine) unregisterConn(t *ManagedTorrent, conn *peer.Conn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, c := range t.activeConns {
		if c == conn {
			t.activeConns = append(t.activeConns[:i], t.activeConns[i+1:]...)
			if t.ActivePeers > 0 {
				t.ActivePeers--
			}
			return
		}
	}
}

// announce queries every known tracker concurrently and merges the de-duplicated peer lists.
// It fails only if no tracker could be reached.
func (e *Engine) announce(t *ManagedTorrent) ([]tracker.Peer, error) {
	t.mu.RLock()
	meta := t.Meta
	trackers := meta.Trackers()
	if len(trackers) == 0 {
		trackers = t.trackers
	}
	params := tracker.AnnounceParams{
		InfoHash:   meta.InfoHash,
		PeerID:     e.PeerID,
		Port:       e.Port,
		Uploaded:   t.Uploaded,
		Downloaded: t.Downloaded,
		Left:       t.Left,
		Compact:    true,
	}
	if t.PieceManager == nil {
		// Size unknown until metadata arrives; a non-zero value marks us as a leecher.
		params.Left = 1
	}
	t.mu.RUnlock()

	if len(trackers) == 0 {
		return nil, fmt.Errorf("no trackers available (DHT is not supported yet)")
	}

	type result struct {
		peers []tracker.Peer
		err   error
	}
	results := make(chan result, len(trackers))
	for _, u := range trackers {
		p := params
		p.AnnounceURL = u
		go func() {
			resp, err := tracker.Announce(p)
			if err != nil {
				results <- result{err: fmt.Errorf("%s: %w", p.AnnounceURL, err)}
				return
			}
			results <- result{peers: resp.Peers}
		}()
	}

	seen := make(map[string]bool)
	var peers []tracker.Peer
	var lastErr error
	succeeded := 0
	for range trackers {
		r := <-results
		if r.err != nil {
			lastErr = r.err
			continue
		}
		succeeded++
		for _, p := range r.peers {
			if key := p.String(); !seen[key] {
				seen[key] = true
				peers = append(peers, p)
			}
		}
	}
	if succeeded == 0 {
		return nil, fmt.Errorf("all %d trackers failed, last error: %w", len(trackers), lastErr)
	}
	return peers, nil
}

func (e *Engine) broadcastHave(t *ManagedTorrent, pieceIndex int) {
	haveMsg := peer.FormatHave(pieceIndex)
	for _, c := range t.conns() {
		_ = c.SendMessage(haveMsg)
	}
}

func (e *Engine) broadcastCancel(t *ManagedTorrent, index, begin, length int, peerAddrs []string) {
	addrMap := make(map[string]bool, len(peerAddrs))
	for _, a := range peerAddrs {
		addrMap[a] = true
	}

	cancelMsg := peer.FormatCancel(index, begin, length)
	for _, c := range t.conns() {
		if addrMap[c.RemotePeer.String()] {
			_ = c.SendMessage(cancelMsg)
		}
	}
}

// conns returns a copy of the active connection list so callers can write without holding mu.
func (t *ManagedTorrent) conns() []*peer.Conn {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return append([]*peer.Conn(nil), t.activeConns...)
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
