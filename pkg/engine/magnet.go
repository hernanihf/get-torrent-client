package engine

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/hernan/get-torrent/pkg/bandwidth"
	"github.com/hernan/get-torrent/pkg/magnet"
	"github.com/hernan/get-torrent/pkg/metadata"
	"github.com/hernan/get-torrent/pkg/metainfo"
	"github.com/hernan/get-torrent/pkg/peer"
	"github.com/hernan/get-torrent/pkg/storage"
)

// AddMagnet registers a torrent from a magnet URI. Its metadata is fetched from peers
// via BEP 0009 once the torrent is started.
func (e *Engine) AddMagnet(uri string) (*ManagedTorrent, error) {
	return e.addMagnet(uri, "")
}

// addMagnet registers a magnet torrent; dir overrides the session download dir.
func (e *Engine) addMagnet(uri, dir string) (*ManagedTorrent, error) {
	m, err := magnet.Parse(uri)
	if err != nil {
		return nil, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if t, exists := e.torrents[m.InfoHash]; exists {
		return t, nil
	}

	name := m.DisplayName
	if name == "" {
		name = hex.EncodeToString(m.InfoHash[:])
	}

	if dir == "" {
		dir = e.DownloadDir
	}
	t := &ManagedTorrent{
		ID:          e.nextID,
		Meta:        &metainfo.TorrentMeta{InfoHash: m.InfoHash, Name: name},
		MagnetURI:   uri,
		State:       StateQueued,
		DownloadDir: dir,
		AddedDate:   time.Now(),
		stopChan:    make(chan struct{}),
		reannounce:  make(chan struct{}, 1),
		limits:      defaultTorrentLimits(),
		downLimiter: bandwidth.NewLimiter(),
		upLimiter:   bandwidth.NewLimiter(),
		trackers:    m.Trackers,
		metadata:    metadata.NewAssembler(m.InfoHash),
	}
	e.nextID++
	e.torrents[m.InfoHash] = t

	log.Printf("[Engine] Added magnet: %q (info_hash %x, %d trackers)", name, m.InfoHash, len(m.Trackers))
	return t, nil
}

// saveTorrentFile persists a torrent's metainfo under ConfigDir/torrents/<info_hash>.torrent.
func (e *Engine) saveTorrentFile(meta *metainfo.TorrentMeta) (string, error) {
	data, err := meta.Marshal()
	if err != nil {
		return "", fmt.Errorf("encoding torrent file: %w", err)
	}

	e.mu.RLock()
	configDir := e.ConfigDir
	e.mu.RUnlock()
	if configDir == "" {
		configDir = os.TempDir()
	}

	dir := filepath.Join(configDir, "torrents")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", fmt.Errorf("creating torrents dir: %w", err)
	}
	path := filepath.Join(dir, hex.EncodeToString(meta.InfoHash[:])+".torrent")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return "", fmt.Errorf("writing torrent file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return "", fmt.Errorf("renaming torrent file: %w", err)
	}
	return path, nil
}

// onMetadata turns a magnet torrent into a regular one once its verified info dictionary is known:
// it persists a .torrent file, allocates storage, and asks connected peers for pieces.
func (e *Engine) onMetadata(t *ManagedTorrent, info []byte) error {
	t.mu.RLock()
	infoHash := t.Meta.InfoHash
	trackers := t.trackers
	already := t.PieceManager != nil
	t.mu.RUnlock()
	if already {
		return nil
	}

	meta, err := metainfo.FromInfoBytes(info, trackers)
	if err != nil {
		return fmt.Errorf("parsing fetched metadata: %w", err)
	}
	if meta.InfoHash != infoHash {
		return fmt.Errorf("fetched metadata hash %x does not match %x", meta.InfoHash, infoHash)
	}

	path, err := e.saveTorrentFile(meta)
	if err != nil {
		return err
	}

	t.mu.RLock()
	downloadDir := t.DownloadDir
	t.mu.RUnlock()
	store, err := storage.NewStorage(downloadDir, meta)
	if err != nil {
		return fmt.Errorf("allocating storage: %w", err)
	}
	pm := storage.NewPieceManager(meta, store)

	t.mu.Lock()
	if t.PieceManager != nil {
		// Another peer session completed the metadata first.
		t.mu.Unlock()
		_ = store.Close()
		return nil
	}
	t.Meta = meta
	t.TorrentPath = path
	t.Storage = store
	t.PieceManager = pm
	t.Left = meta.TotalLength
	t.metadata = nil
	t.mu.Unlock()
	e.stats.filesAdded.Add(int64(len(meta.Files)))

	log.Printf("[Engine] Metadata received for %q (%d MB, %d pieces); starting download",
		meta.Name, meta.TotalLength/(1024*1024), len(meta.Pieces))

	if err := e.SaveState(); err != nil {
		log.Printf("[Engine] Saving state after metadata: %v", err)
	}

	// Peers that only had metadata to offer may have ignored our earlier interest;
	// re-announce it now that we can actually request pieces.
	for _, c := range t.conns() {
		_ = c.SendInterested()
	}
	return nil
}

// metadataPiece returns a piece of our info dictionary for serving ut_metadata requests.
func (t *ManagedTorrent) metadataPiece(index int) ([]byte, int, bool) {
	t.mu.RLock()
	info := t.Meta.InfoBytes
	t.mu.RUnlock()
	if len(info) == 0 {
		return nil, 0, false
	}
	data, ok := metadata.Piece(info, index)
	return data, len(info), ok
}

// metadataSize returns the size of our info dictionary, or 0 if unknown.
func (t *ManagedTorrent) metadataSize() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.Meta.InfoBytes)
}

// assembler returns the in-progress metadata assembler, or nil if metadata is already known.
func (t *ManagedTorrent) assembler() *metadata.Assembler {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.metadata
}

// requestMetadata asks a peer for missing metadata pieces, if it supports ut_metadata.
func (s *peerSession) requestMetadata() {
	asm := s.t.assembler()
	if asm == nil {
		return
	}
	extID, ok := s.conn.PeerExtID(peer.ExtUTMetadata)
	if !ok {
		return
	}
	if asm.Size() == 0 {
		if s.conn.PeerExt.MetadataSize == 0 {
			return
		}
		if err := asm.SetSize(s.conn.PeerExt.MetadataSize); err != nil {
			log.Printf("[Engine] Peer %s: %v", s.addr, err)
			return
		}
	}
	for _, piece := range asm.NextRequests(maxMetadataRequests, s.now()) {
		_ = s.conn.SendMessage(peer.FormatMetadataRequest(extID, piece))
	}
}

// handleMetadataMsg processes an incoming ut_metadata message.
func (s *peerSession) handleMetadataMsg(payload []byte) {
	m, err := peer.ParseMetadataMsg(payload)
	if err != nil {
		return
	}

	switch m.Type {
	case peer.MetadataRequest:
		extID, ok := s.conn.PeerExtID(peer.ExtUTMetadata)
		if !ok {
			return
		}
		if data, total, ok := s.t.metadataPiece(m.Piece); ok {
			_ = s.conn.SendMessage(peer.FormatMetadataData(extID, m.Piece, total, data))
		} else {
			_ = s.conn.SendMessage(peer.FormatMetadataReject(extID, m.Piece))
		}

	case peer.MetadataData:
		asm := s.t.assembler()
		if asm == nil {
			return
		}
		info, err := asm.AddPiece(m.Piece, m.TotalSize, m.Data)
		if err != nil {
			log.Printf("[Engine] Metadata piece %d from %s: %v", m.Piece, s.addr, err)
		}
		if info != nil {
			if err := s.e.onMetadata(s.t, info); err != nil {
				log.Printf("[Engine] %v", err)
				s.e.setError(s.t, err)
				return
			}
		}
		s.requestMetadata()

	case peer.MetadataReject:
		if asm := s.t.assembler(); asm != nil {
			asm.Rejected(m.Piece)
		}
	}
}
