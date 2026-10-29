package engine

import (
	"math/bits"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hernan/get-torrent/pkg/peer"
	"github.com/hernan/get-torrent/pkg/storage"
)

// rateWindow is the averaging window, in seconds, for transfer rates (Transmission uses ~2 s).
const rateWindow = 2

// rateMeter measures a transfer rate over a sliding window of one-second buckets.
type rateMeter struct {
	mu      sync.Mutex
	buckets [rateWindow + 1]int64
	stamps  [rateWindow + 1]int64 // Unix second each bucket belongs to
	first   int64                 // Unix second of the first sample, 0 if none
}

func (m *rateMeter) add(n int, now time.Time) {
	sec := now.Unix()
	i := sec % int64(len(m.buckets))
	m.mu.Lock()
	if m.first == 0 {
		m.first = sec
	}
	if m.stamps[i] != sec {
		m.stamps[i] = sec
		m.buckets[i] = 0
	}
	m.buckets[i] += int64(n)
	m.mu.Unlock()
}

// rate returns bytes per second averaged over the last rateWindow complete seconds, or over
// the complete seconds since the first sample if that is shorter (so a transfer that just
// started is not under-reported).
func (m *rateMeter) rate(now time.Time) int64 {
	sec := now.Unix()
	var sum int64
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.first == 0 {
		return 0
	}
	window := min(int64(rateWindow), sec-m.first)
	if window < 1 {
		return 0 // only the in-progress second so far
	}
	for i, st := range m.stamps {
		if age := sec - st; age >= 1 && age <= window {
			sum += m.buckets[i]
		}
	}
	return sum / window
}

// PeerSource records how we learnt about a peer (Transmission's "peersFrom").
type PeerSource string

const (
	SourceIncoming PeerSource = "incoming"
	SourceTracker  PeerSource = "tracker"
	SourceDHT      PeerSource = "dht"
)

// peerInfo is the reportable state of one connection. The peer session owns the Conn and
// publishes into this struct, which reporting code reads under mu.
type peerInfo struct {
	mu                 sync.Mutex
	addr               string
	ip                 string
	port               int
	source             PeerSource
	incoming           bool
	clientName         string
	clientIsChoked     bool // the peer is choking us
	clientIsInterested bool // we are interested in the peer
	peerIsChoked       bool // we are choking the peer
	peerIsInterested   bool // the peer is interested in us
	piecesHave         int
	down, up           rateMeter
	connected          time.Time // when the connection was registered
	chokeReq           chan bool // choker decisions for the session: true = choke
}

// publish copies the session-owned connection state into the reportable snapshot.
func (pi *peerInfo) publish(c *peer.Conn) {
	have := 0
	for _, b := range c.Bitfield {
		have += bits.OnesCount8(b)
	}
	name := ""
	if c.PeerExt != nil {
		name = c.PeerExt.ClientName
	}
	if name == "" {
		name = clientFromPeerID(c.PeerID)
	}
	pi.mu.Lock()
	pi.clientIsChoked = c.Choked
	pi.clientIsInterested = c.AmInterested
	pi.peerIsChoked = c.AmChoking
	pi.peerIsInterested = c.Interested
	pi.piecesHave = have
	pi.clientName = name
	pi.mu.Unlock()
}

// clientFromPeerID decodes Azureus-style peer IDs ("-TR4000-...") into a readable client name.
func clientFromPeerID(id [20]byte) string {
	if id[0] != '-' || id[7] != '-' {
		return ""
	}
	names := map[string]string{
		"TR": "Transmission", "qB": "qBittorrent", "UT": "µTorrent", "LT": "libtorrent",
		"lt": "libTorrent", "DE": "Deluge", "AZ": "Azureus", "BT": "BitTorrent", "GT": "get-torrent",
	}
	code, ver := string(id[1:3]), strings.TrimLeft(string(id[3:7]), "0")
	if n, ok := names[code]; ok {
		return n + " " + ver
	}
	return code + " " + ver
}

// PeerStatus is a reportable snapshot of one connected peer.
type PeerStatus struct {
	Address            string
	Port               int
	ClientName         string
	ClientIsChoked     bool
	ClientIsInterested bool
	PeerIsChoked       bool
	PeerIsInterested   bool
	IsIncoming         bool
	Progress           float64 // fraction of pieces the peer has, 0..1
	RateToClient       int64   // B/s
	RateToPeer         int64   // B/s
	Source             PeerSource
}

// TrackerStatus is the outcome of the most recent announce to one tracker.
type TrackerStatus struct {
	ID                    int
	Announce              string
	Tier                  int
	HasAnnounced          bool
	LastAnnounceTime      int64
	LastAnnounceSucceeded bool
	LastAnnounceResult    string
	LastAnnouncePeerCount int
	NextAnnounceTime      int64
	SeederCount           int64 // -1 if unknown
	LeecherCount          int64 // -1 if unknown
}

// sessionStats tracks transfer totals for the current daemon session (session-stats).
type sessionStats struct {
	started    time.Time
	downloaded atomic.Int64
	uploaded   atomic.Int64
	filesAdded atomic.Int64
}

// CumulativeStats are totals across all daemon sessions, persisted in state.json.
type CumulativeStats struct {
	UploadedBytes   int64 `json:"uploaded_bytes"`
	DownloadedBytes int64 `json:"downloaded_bytes"`
	FilesAdded      int64 `json:"files_added"`
	SecondsActive   int64 `json:"seconds_active"`
	SessionCount    int64 `json:"session_count"`
}

// SessionStats reports totals for the current session and cumulatively.
func (e *Engine) SessionStats() (current, cumulative CumulativeStats) {
	current = CumulativeStats{
		UploadedBytes:   e.stats.uploaded.Load(),
		DownloadedBytes: e.stats.downloaded.Load(),
		FilesAdded:      e.stats.filesAdded.Load(),
		SecondsActive:   int64(time.Since(e.stats.started).Seconds()),
		SessionCount:    1,
	}
	e.mu.RLock()
	base := e.cumulative
	e.mu.RUnlock()
	cumulative = CumulativeStats{
		UploadedBytes:   base.UploadedBytes + current.UploadedBytes,
		DownloadedBytes: base.DownloadedBytes + current.DownloadedBytes,
		FilesAdded:      base.FilesAdded + current.FilesAdded,
		SecondsActive:   base.SecondsActive + current.SecondsActive,
		SessionCount:    base.SessionCount + 1,
	}
	return current, cumulative
}

// recordDownload accounts payload bytes received from a peer.
func (e *Engine) recordDownload(t *ManagedTorrent, pi *peerInfo, n int) {
	now := time.Now()
	t.down.add(n, now)
	if pi != nil {
		pi.down.add(n, now)
	}
	e.stats.downloaded.Add(int64(n))
	t.mu.Lock()
	t.Downloaded += int64(n)
	t.activityDate = now
	t.mu.Unlock()
}

// recordUpload accounts payload bytes sent to a peer.
func (e *Engine) recordUpload(t *ManagedTorrent, pi *peerInfo, n int) {
	now := time.Now()
	t.up.add(n, now)
	if pi != nil {
		pi.up.add(n, now)
	}
	e.stats.uploaded.Add(int64(n))
	t.mu.Lock()
	t.Uploaded += int64(n)
	t.activityDate = now
	t.mu.Unlock()
}

// Peers returns snapshots of the torrent's connected peers.
func (t *ManagedTorrent) PeerList() []PeerStatus {
	t.mu.RLock()
	infos := make([]*peerInfo, 0, len(t.peerInfos))
	for _, pi := range t.peerInfos {
		infos = append(infos, pi)
	}
	pieceCount := len(t.Meta.Pieces)
	t.mu.RUnlock()

	now := time.Now()
	out := make([]PeerStatus, 0, len(infos))
	for _, pi := range infos {
		pi.mu.Lock()
		ps := PeerStatus{
			Address:            pi.ip,
			Port:               pi.port,
			ClientName:         pi.clientName,
			ClientIsChoked:     pi.clientIsChoked,
			ClientIsInterested: pi.clientIsInterested,
			PeerIsChoked:       pi.peerIsChoked,
			PeerIsInterested:   pi.peerIsInterested,
			IsIncoming:         pi.incoming,
			Source:             pi.source,
		}
		if pieceCount > 0 {
			ps.Progress = min(float64(pi.piecesHave)/float64(pieceCount), 1)
		}
		pi.mu.Unlock()
		ps.RateToClient = pi.down.rate(now)
		ps.RateToPeer = pi.up.rate(now)
		out = append(out, ps)
	}
	return out
}

// TrackerList returns per-tracker announce state, in announce-list order.
func (t *ManagedTorrent) TrackerList() []TrackerStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()

	tiers := t.Meta.AnnounceList
	if len(tiers) == 0 {
		if t.Meta.Announce != "" {
			tiers = [][]string{{t.Meta.Announce}}
		} else {
			for _, u := range t.trackers {
				tiers = append(tiers, []string{u})
			}
		}
	}

	var out []TrackerStatus
	seen := make(map[string]bool)
	for tier, urls := range tiers {
		for _, u := range urls {
			if seen[u] {
				continue
			}
			seen[u] = true
			ts := TrackerStatus{ID: len(out), Announce: u, Tier: tier, LastAnnouncePeerCount: 0}
			if rec := t.trackerRecords[u]; rec != nil {
				ts.HasAnnounced = true
				ts.LastAnnounceTime = rec.time.Unix()
				ts.LastAnnounceSucceeded = rec.succeeded
				ts.LastAnnounceResult = rec.result
				ts.LastAnnouncePeerCount = rec.peers
			}
			ts.SeederCount, ts.LeecherCount = -1, -1
			if rec := t.trackerRecords[u]; rec != nil {
				ts.SeederCount, ts.LeecherCount = rec.seeders, rec.leechers
				if rec.gen == t.runGen && !rec.next.IsZero() {
					ts.NextAnnounceTime = rec.next.Unix()
				}
			}
			out = append(out, ts)
		}
	}
	return out
}

// PieceSnapshot returns verified pieces and availability; empty before metadata is known.
func (t *ManagedTorrent) PieceSnapshot() storage.Snapshot {
	if _, pm, _ := t.components(); pm != nil {
		return pm.Snapshot()
	}
	return storage.Snapshot{}
}

// FileStatus describes one file of a torrent and how much of it is verified.
type FileStatus struct {
	Name           string // path relative to the download dir, "/"-separated
	Length         int64
	BytesCompleted int64
}

// Files returns per-file progress computed from verified pieces.
func (t *ManagedTorrent) Files() []FileStatus {
	meta, pm, _ := t.components()
	if len(meta.Files) == 0 {
		return []FileStatus{}
	}
	var have peer.Bitfield
	if pm != nil {
		have = pm.Snapshot().Have
	}

	multi := len(meta.Files) > 1 || len(meta.Files[0].Path) > 1
	out := make([]FileStatus, 0, len(meta.Files))
	var offset int64
	for _, f := range meta.Files {
		name := meta.Name
		if multi {
			name = meta.Name + "/" + strings.Join(f.Path, "/")
		}
		fs := FileStatus{Name: name, Length: f.Length}
		if f.Length > 0 && have != nil {
			first := offset / meta.PieceLength
			last := (offset + f.Length - 1) / meta.PieceLength
			for p := first; p <= last; p++ {
				if !have.HasPiece(int(p)) {
					continue
				}
				pStart, pEnd := p*meta.PieceLength, (p+1)*meta.PieceLength
				fs.BytesCompleted += min(pEnd, offset+f.Length) - max(pStart, offset)
			}
		}
		offset += f.Length
		out = append(out, fs)
	}
	return out
}
