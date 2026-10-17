package engine

import (
	"crypto/rand"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/hernan/get-torrent/pkg/metainfo"
	"github.com/hernan/get-torrent/pkg/peer"
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

// ManagedTorrent wraps metadata and live connection state for a torrent.
type ManagedTorrent struct {
	ID          int
	Meta        *metainfo.TorrentMeta
	State       TorrentState
	Downloaded  int64
	Uploaded    int64
	Left        int64
	Peers       []tracker.Peer
	ActivePeers int
	mu          sync.RWMutex
}

// Engine orchestrates torrent downloads, peer connections, and tracker announces.
type Engine struct {
	PeerID     [20]byte
	Port       uint16
	torrents   map[[20]byte]*ManagedTorrent
	mu         sync.RWMutex
	nextID     int
	downloadDir string
}

// NewEngine creates and initializes a BitTorrent engine.
func NewEngine(downloadDir string, port uint16) *Engine {
	var peerID [20]byte
	copy(peerID[:8], []byte("-GT0001-")) // Client prefix: Get-Torrent 0.0.1
	_, _ = rand.Read(peerID[8:])

	return &Engine{
		PeerID:      peerID,
		Port:        port,
		torrents:    make(map[[20]byte]*ManagedTorrent),
		nextID:      1,
		downloadDir: downloadDir,
	}
}

// AddTorrentFile loads a .torrent file and registers it in the engine.
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

	t := &ManagedTorrent{
		ID:         e.nextID,
		Meta:       meta,
		State:      StateQueued,
		Downloaded: 0,
		Uploaded:   0,
		Left:       meta.TotalLength,
	}
	e.nextID++
	e.torrents[meta.InfoHash] = t

	log.Printf("[Engine] Added torrent: %q (%d MB, %d pieces)",
		meta.Name, meta.TotalLength/(1024*1024), len(meta.Pieces))

	return t, nil
}

// Start begins tracker announcing and peer connections for a given torrent.
func (e *Engine) Start(infoHash [20]byte) error {
	e.mu.RLock()
	t, exists := e.torrents[infoHash]
	e.mu.RUnlock()

	if !exists {
		return fmt.Errorf("torrent with hash %x not found", infoHash)
	}

	t.mu.Lock()
	t.State = StateDownloading
	t.mu.Unlock()

	go e.runTorrent(t)
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
		wg.Add(1)
		go func(peerEndpoint tracker.Peer) {
			defer wg.Done()
			conn, err := peer.Dial(peerEndpoint, t.Meta.InfoHash, e.PeerID, 8*time.Second)
			if err != nil {
				return
			}
			defer conn.Close()

			t.mu.Lock()
			t.ActivePeers++
			t.mu.Unlock()

			log.Printf("[Engine] Connected to peer %s (PeerID: %x)", peerEndpoint.String(), conn.PeerID[:8])

			// Express interest
			_ = conn.SendInterested()

			// Listen for messages from this peer
			for {
				msg, err := conn.ReadMsg()
				if err != nil {
					break
				}
				if msg.ID != nil && *msg.ID == peer.MsgUnchoke {
					log.Printf("[Engine] Peer %s unchoked us! Ready to request pieces.", peerEndpoint.String())
				}
			}

			t.mu.Lock()
			t.ActivePeers--
			t.mu.Unlock()
		}(p)
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
