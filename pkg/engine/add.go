package engine

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/hernan/get-torrent/pkg/magnet"
	"github.com/hernan/get-torrent/pkg/metainfo"
	"github.com/hernan/get-torrent/pkg/storage"
)

// maxTorrentFetchSize bounds .torrent downloads from URLs.
const maxTorrentFetchSize = 10 << 20

// AddRequest describes a torrent to add, mirroring Transmission's torrent-add.
type AddRequest struct {
	// Filename is a local .torrent path, an http(s) URL to one, or a magnet URI.
	Filename string
	// Metainfo is raw .torrent contents, used when Filename is empty.
	Metainfo []byte
	// DownloadDir overrides the session download directory for this torrent.
	DownloadDir string
}

// Add registers a torrent. If it is already present, the existing torrent is returned with
// added=false (Transmission's "torrent-duplicate").
func (e *Engine) Add(req AddRequest) (t *ManagedTorrent, added bool, err error) {
	var (
		meta *metainfo.TorrentMeta
		uri  string
		path string
	)
	lower := strings.ToLower(req.Filename)
	switch {
	case strings.HasPrefix(lower, "magnet:"):
		m, err := magnet.Parse(req.Filename)
		if err != nil {
			return nil, false, err
		}
		if t := e.TorrentByHash(m.InfoHash); t != nil {
			return t, false, nil
		}
		uri = req.Filename
	case strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://"):
		data, err := fetchTorrent(req.Filename)
		if err != nil {
			return nil, false, err
		}
		if meta, err = metainfo.Parse(data); err != nil {
			return nil, false, fmt.Errorf("parsing torrent from %s: %w", req.Filename, err)
		}
	case req.Filename != "":
		if meta, err = metainfo.Open(req.Filename); err != nil {
			return nil, false, fmt.Errorf("opening torrent file: %w", err)
		}
		path = req.Filename
	case len(req.Metainfo) > 0:
		if meta, err = metainfo.Parse(req.Metainfo); err != nil {
			return nil, false, fmt.Errorf("parsing torrent: %w", err)
		}
	default:
		return nil, false, errors.New("filename or metainfo required")
	}

	if meta != nil {
		if t := e.TorrentByHash(meta.InfoHash); t != nil {
			return t, false, nil
		}
		if path == "" {
			// Keep a copy of fetched/uploaded torrents so they survive restarts.
			if path, err = e.saveTorrentFile(meta); err != nil {
				return nil, false, err
			}
		}
		t, err = e.addMeta(meta, path, req.DownloadDir)
	} else {
		t, err = e.addMagnet(uri, req.DownloadDir)
	}
	return t, err == nil, err
}

func fetchTorrent(url string) ([]byte, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("fetching torrent: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetching torrent: HTTP %s", resp.Status)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxTorrentFetchSize+1))
	if err != nil {
		return nil, fmt.Errorf("fetching torrent: %w", err)
	}
	if len(data) > maxTorrentFetchSize {
		return nil, fmt.Errorf("fetching torrent: larger than %d bytes", maxTorrentFetchSize)
	}
	return data, nil
}

// TorrentByHash returns the torrent with the given info_hash, or nil.
func (e *Engine) TorrentByHash(infoHash [20]byte) *ManagedTorrent {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.torrents[infoHash]
}

// TorrentByHashString looks a torrent up by its 40-character hex info_hash.
func (e *Engine) TorrentByHashString(s string) *ManagedTorrent {
	raw, err := hex.DecodeString(s)
	if err != nil || len(raw) != sha1.Size {
		return nil
	}
	var ih [20]byte
	copy(ih[:], raw)
	return e.TorrentByHash(ih)
}

// recentWindow defines "recently active" for torrent-get's ids="recently-active".
const recentWindow = time.Minute

// RecentlyActive returns torrents with transfer or state activity within the last minute, and
// the IDs of torrents removed within that window.
func (e *Engine) RecentlyActive() (active []*ManagedTorrent, removedIDs []int) {
	cutoff := time.Now().Add(-recentWindow)
	for _, t := range e.ListTorrents() {
		t.mu.RLock()
		recent := t.activityDate.After(cutoff) || t.stateSince.After(cutoff) || t.AddedDate.After(cutoff) ||
			t.State == StateDownloading || t.State == StateChecking
		t.mu.RUnlock()
		if recent {
			active = append(active, t)
		}
	}

	e.mu.Lock()
	kept := e.removed[:0]
	for _, r := range e.removed {
		if r.at.After(cutoff) {
			kept = append(kept, r)
			removedIDs = append(removedIDs, r.id)
		}
	}
	e.removed = kept
	e.mu.Unlock()
	return active, removedIDs
}

// Reannounce asks trackers and the DHT for peers now instead of at the next interval.
func (e *Engine) Reannounce(id int) error {
	t := e.GetTorrentByID(id)
	if t == nil {
		return fmt.Errorf("torrent with id %d not found", id)
	}
	select {
	case t.reannounce <- struct{}{}:
	default: // a reannounce is already pending
	}
	return nil
}

// VerifyTorrent re-checks every piece on disk against its SHA-1 hash (Transmission's
// torrent-verify). A running torrent is stopped while checking and restarted afterwards.
func (e *Engine) VerifyTorrent(id int) error {
	t := e.GetTorrentByID(id)
	if t == nil {
		return fmt.Errorf("torrent with id %d not found", id)
	}
	meta, pm, store := t.components()
	if pm == nil {
		return nil // Nothing on disk yet (magnet without metadata)
	}

	t.mu.RLock()
	state := t.State
	t.mu.RUnlock()
	if state == StateChecking {
		return nil
	}
	wasRunning := state == StateDownloading || state == StateSeeding
	if wasRunning {
		_ = e.StopTorrent(id)
	}

	t.mu.Lock()
	t.setStateLocked(StateChecking)
	t.recheck = 0
	t.restartAfter = wasRunning
	t.mu.Unlock()

	go func() {
		have := make([]byte, (len(meta.Pieces)+7)/8)
		for i, hash := range meta.Pieces {
			length := meta.PieceLength
			if i == len(meta.Pieces)-1 {
				length = meta.TotalLength - int64(i)*meta.PieceLength
			}
			if data, err := store.ReadPiece(i, int(length)); err == nil && sha1.Sum(data) == hash {
				have[i/8] |= 1 << (7 - uint(i%8))
			}
			t.mu.Lock()
			t.recheck = float64(i+1) / float64(len(meta.Pieces))
			t.mu.Unlock()
		}
		pm.SetVerified(have)
		_, done := pm.Progress()

		t.mu.Lock()
		t.Left = meta.TotalLength - done
		t.recheck = 0
		restart := t.restartAfter
		t.restartAfter = false
		t.setStateLocked(StateStopped)
		t.mu.Unlock()

		if restart {
			_ = e.StartTorrent(id)
		}
	}()
	return nil
}

// FreeSpace reports free and total bytes on the filesystem holding path.
func FreeSpace(path string) (free, total int64, err error) {
	return storage.DiskSpace(path)
}
