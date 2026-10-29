package engine

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hernan/get-torrent/pkg/bandwidth"
	"github.com/hernan/get-torrent/pkg/dht"
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
	StateChecking    TorrentState = "checking" // verifying local data (torrent-verify)
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
	DownloadDir  string
	CorruptEver  int64 // bytes discarded because their piece failed SHA-1 verification
	AddedDate    time.Time
	DoneDate     time.Time
	activeConns  []*peer.Conn
	dialing      map[string]bool // peer addresses with an outbound dial in progress
	stopChan     chan struct{}
	trackers     []string            // magnet trackers, used until metadata provides its own
	metadata     *metadata.Assembler // non-nil while metadata is being fetched
	metaReady    chan struct{}       // magnets only: closed once metadata is in place

	startDate      time.Time
	activityDate   time.Time
	stateSince     time.Time // when State last changed, for time accounting
	secondsDown    float64   // accumulated time spent downloading, excluding the current stint
	secondsSeed    float64   // accumulated time spent seeding, excluding the current stint
	down, up       rateMeter
	peerInfos      map[*peer.Conn]*peerInfo
	trackerRecords map[string]*trackerRecord
	dht            dhtSchedule
	runGen         int           // incremented by every start and stop; results from older runs are dropped
	sendCompleted  bool          // the download finished during this run: trackers get "completed"
	reannounce     chan struct{} // wakes the discovery loop early (torrent-reannounce)
	discoveryWake  chan struct{} // an announce finished or became due; reschedule
	rechoke        chan struct{} // wakes the choker early (interest changes, freed slots)
	recheck        float64       // torrent-verify progress, 0..1
	restartAfter   bool          // restart once verification finishes

	files       FileSelection // per-file wanted/priority; zero value means the default (all, normal)
	finished    bool          // stopped by a seeding limit (isFinished)
	limits      TorrentLimits
	downLimiter *bandwidth.Limiter // per-torrent limiters; nil means unlimited
	upLimiter   *bandwidth.Limiter

	mu sync.RWMutex
}

// setStateLocked changes State, accounting time spent downloading/seeding. Caller holds t.mu.
func (t *ManagedTorrent) setStateLocked(s TorrentState) {
	now := time.Now()
	if !t.stateSince.IsZero() {
		switch t.State {
		case StateDownloading:
			t.secondsDown += now.Sub(t.stateSince).Seconds()
		case StateSeeding:
			t.secondsSeed += now.Sub(t.stateSince).Seconds()
		}
	}
	if s == StateSeeding && t.State != StateSeeding && t.DoneDate.IsZero() {
		t.DoneDate = now
	}
	t.State = s
	t.stateSince = now
}

// components returns the metadata-dependent parts of a torrent; pm and store are nil for a magnet
// that has not yet fetched its metadata.
func (t *ManagedTorrent) components() (*metainfo.TorrentMeta, *storage.PieceManager, *storage.Storage) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.Meta, t.PieceManager, t.Storage
}

// Error kinds reported with a torrent (values match Transmission's tr_stat_errtype).
const (
	ErrorNone         = 0
	ErrorTrackerError = 2 // peer discovery failing; the torrent keeps running and retrying
	ErrorLocal        = 3 // the torrent stopped because of a local problem
)

// TorrentStatus is a consistent snapshot of a torrent for reporting (e.g. RPC torrent-get).
type TorrentStatus struct {
	ID                      int
	Name                    string
	InfoHash                [20]byte
	State                   TorrentState
	TotalSize               int64
	PercentDone             float64 // 0..1, of the wanted pieces
	PercentComplete         float64 // 0..1, of all data, wanted or not
	SizeWhenDone            int64   // bytes of the wanted pieces
	MetadataPercentComplete float64 // 0..1
	RecheckProgress         float64 // 0..1
	Left                    int64
	HaveValid               int64 // verified bytes on disk
	Downloaded              int64 // payload bytes received, including discarded ones
	Uploaded                int64
	CorruptEver             int64
	RateDownload            int64 // B/s
	RateUpload              int64 // B/s
	ActivePeers             int
	PeersSendingToUs        int
	PeersGettingFromUs      int
	PeersFrom               map[PeerSource]int
	MagnetURI               string
	Error                   string
	ErrorKind               int
	DownloadDir             string
	TorrentFile             string
	PieceCount              int
	PieceSize               int64
	Private                 bool
	Comment                 string
	Creator                 string
	DateCreated             int64
	AddedDate               int64 // Unix seconds; 0 if unknown
	DoneDate                int64
	StartDate               int64
	ActivityDate            int64
	SecondsDownloading      int64
	SecondsSeeding          int64
}

func unix(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

// Status returns a race-free snapshot of the torrent's state.
func (t *ManagedTorrent) Status() TorrentStatus {
	now := time.Now()
	t.mu.RLock()
	meta := t.Meta
	st := TorrentStatus{
		ID:              t.ID,
		Name:            meta.Name,
		InfoHash:        meta.InfoHash,
		State:           t.State,
		TotalSize:       meta.TotalLength,
		RecheckProgress: t.recheck,
		Left:            t.Left,
		Downloaded:      t.Downloaded,
		Uploaded:        t.Uploaded,
		CorruptEver:     t.CorruptEver,
		ActivePeers:     t.ActivePeers,
		MagnetURI:       t.MagnetURI,
		Error:           t.LastError,
		DownloadDir:     t.DownloadDir,
		TorrentFile:     t.TorrentPath,
		PieceCount:      len(meta.Pieces),
		PieceSize:       meta.PieceLength,
		Private:         meta.Private,
		Comment:         meta.Comment,
		Creator:         meta.CreatedBy,
		DateCreated:     meta.CreationDate,
		AddedDate:       unix(t.AddedDate),
		DoneDate:        unix(t.DoneDate),
		StartDate:       unix(t.startDate),
		ActivityDate:    unix(t.activityDate),
	}
	secDown, secSeed := t.secondsDown, t.secondsSeed
	if !t.stateSince.IsZero() {
		switch t.State {
		case StateDownloading:
			secDown += now.Sub(t.stateSince).Seconds()
		case StateSeeding:
			secSeed += now.Sub(t.stateSince).Seconds()
		}
	}
	st.SecondsDownloading, st.SecondsSeeding = int64(secDown), int64(secSeed)
	pm, asm := t.PieceManager, t.metadata
	t.mu.RUnlock()

	if st.Name == "" {
		st.Name = hex.EncodeToString(st.InfoHash[:])
	}
	switch {
	case st.Error == "":
	case st.State == StateError:
		st.ErrorKind = ErrorLocal
	default:
		st.ErrorKind = ErrorTrackerError
	}
	switch {
	case pm != nil:
		_, have := pm.Progress()
		wantedHave, wantedTotal := pm.WantedProgress()
		st.HaveValid = have
		st.SizeWhenDone = wantedTotal
		st.Left = wantedTotal - wantedHave
		st.PercentDone, st.PercentComplete = 1, 1
		if wantedTotal > 0 {
			st.PercentDone = float64(wantedHave) / float64(wantedTotal)
		}
		if st.TotalSize > 0 {
			st.PercentComplete = float64(have) / float64(st.TotalSize)
		}
		st.MetadataPercentComplete = 1
	case asm != nil:
		st.MetadataPercentComplete = asm.Progress()
	}

	st.RateDownload = t.down.rate(now)
	st.RateUpload = t.up.rate(now)
	st.PeersFrom = make(map[PeerSource]int)
	for _, p := range t.PeerList() {
		st.PeersFrom[p.Source]++
		if p.RateToClient > 0 {
			st.PeersSendingToUs++
		}
		if p.RateToPeer > 0 {
			st.PeersGettingFromUs++
		}
	}
	return st
}

// Engine orchestrates torrent downloads, peer connections, tracker announces, and session state.
type Engine struct {
	PeerID         [20]byte
	Port           uint16
	DownloadDir    string
	ConfigDir      string
	SpeedLimitDown int64 // KB/s, as in Transmission's speed-limit-down (not yet enforced)
	SpeedLimitUp   int64 // KB/s (not yet enforced)

	SpeedLimitDownEnabled bool
	SpeedLimitUpEnabled   bool

	// Alternative ("turtle") speed limits in KB/s, used instead of the above while enabled.
	AltSpeedDown    int64
	AltSpeedUp      int64
	AltSpeedEnabled bool

	// Session seeding limits (Transmission's seedRatioLimit and idle-seeding-limit).
	SeedRatioLimit          float64
	SeedRatioLimited        bool
	IdleSeedingLimit        int // minutes
	IdleSeedingLimitEnabled bool

	// Session-wide limiters, kept in sync with the settings above by applyLimitsLocked.
	sessionDown      *bandwidth.Limiter
	sessionUp        *bandwidth.Limiter
	torrents         map[[20]byte]*ManagedTorrent
	mu               sync.RWMutex
	nextID           int
	listener         net.Listener
	listenerStopChan chan struct{}

	// DHT is the BEP 0005 node used for trackerless peer discovery; nil when disabled.
	DHT *dht.Server

	ctx    context.Context // cancelled by Close
	cancel context.CancelFunc

	stats      sessionStats
	cumulative CumulativeStats // totals from previous sessions (state.json)
	removed    []removedTorrent

	verifySem chan struct{} // held by the one torrent currently hashing its data

	saveMu sync.Mutex     // serializes SaveState (periodic and shutdown saves share the files)
	bg     sync.WaitGroup // background loops that Close waits for before stopping torrents

	announceKey uint32         // tracker "key": identifies this client across IP changes
	stopping    sync.WaitGroup // "stopped" announces that Close waits for
}

// removedTorrent remembers a removal for torrent-get's "recently-active" ids.
type removedTorrent struct {
	id int
	at time.Time
}

// NewEngine creates and initializes a BitTorrent engine.
func NewEngine(downloadDir string, port uint16) *Engine {
	var peerID [20]byte
	copy(peerID[:8], []byte("-GT0001-")) // Client prefix: Get-Torrent 0.0.1
	_, _ = rand.Read(peerID[8:])

	var key [4]byte
	_, _ = rand.Read(key[:])

	ctx, cancel := context.WithCancel(context.Background())
	return &Engine{
		announceKey:      binary.BigEndian.Uint32(key[:]),
		AltSpeedDown:     50,
		AltSpeedUp:       50,
		SeedRatioLimit:   2,
		IdleSeedingLimit: 30,
		sessionDown:      bandwidth.NewLimiter(),
		sessionUp:        bandwidth.NewLimiter(),
		stats:            sessionStats{started: time.Now()},
		ctx:              ctx,
		cancel:           cancel,
		PeerID:           peerID,
		Port:             port,
		DownloadDir:      downloadDir,
		ConfigDir:        "./config",
		torrents:         make(map[[20]byte]*ManagedTorrent),
		nextID:           1,
		listenerStopChan: make(chan struct{}),
		verifySem:        make(chan struct{}, 1),
	}
}

// AddTorrentFile loads a .torrent file, allocates disk storage, and registers it.
func (e *Engine) AddTorrentFile(path string) (*ManagedTorrent, error) {
	meta, err := metainfo.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening torrent file: %w", err)
	}
	return e.addMeta(meta, path, "")
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
	return e.addMeta(meta, path, "")
}

// addMeta registers a torrent with known metadata; dir overrides the session download dir.
func (e *Engine) addMeta(meta *metainfo.TorrentMeta, path, dir string) (*ManagedTorrent, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if t, exists := e.torrents[meta.InfoHash]; exists {
		return t, nil
	}
	if dir == "" {
		dir = e.DownloadDir
	}

	store, err := storage.NewStorage(dir, meta)
	if err != nil {
		return nil, fmt.Errorf("allocating storage: %w", err)
	}

	pm := storage.NewPieceManager(meta, store)

	t := &ManagedTorrent{
		ID:            e.nextID,
		Meta:          meta,
		TorrentPath:   path,
		Storage:       store,
		PieceManager:  pm,
		State:         StateQueued,
		Downloaded:    0,
		Uploaded:      0,
		Left:          meta.TotalLength,
		DownloadDir:   dir,
		AddedDate:     time.Now(),
		stopChan:      make(chan struct{}),
		reannounce:    make(chan struct{}, 1),
		discoveryWake: make(chan struct{}, 1),
		rechoke:       make(chan struct{}, 1),
		limits:        defaultTorrentLimits(),
		downLimiter:   bandwidth.NewLimiter(),
		upLimiter:     bandwidth.NewLimiter(),
	}
	e.nextID++
	e.torrents[meta.InfoHash] = t
	e.stats.filesAdded.Add(int64(len(meta.Files)))

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

// StartTorrent begins tracker announcing and peer connections for a given torrent ID. It is the
// user's start: a torrent that already met its seed ratio gets that limit lifted.
func (e *Engine) StartTorrent(id int) error {
	t := e.GetTorrentByID(id)
	if t == nil {
		return fmt.Errorf("torrent with id %d not found", id)
	}
	if !t.running() {
		e.liftReachedRatio(t)
	}
	return e.startTorrent(t)
}

func (t *ManagedTorrent) running() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.State == StateDownloading || t.State == StateSeeding
}

// startTorrent starts t as it is (restored sessions, finished verifications).
func (e *Engine) startTorrent(t *ManagedTorrent) error {
	t.mu.Lock()
	if t.State == StateDownloading || t.State == StateSeeding {
		t.mu.Unlock()
		return nil // Already running
	}
	if t.State == StateChecking {
		t.restartAfter = true // start once verification finishes
		t.mu.Unlock()
		return nil
	}

	if t.PieceManager != nil && t.PieceManager.IsDone() {
		t.setStateLocked(StateSeeding)
	} else {
		t.setStateLocked(StateDownloading)
	}
	t.startDate = time.Now()
	t.LastError = ""
	t.sendCompleted = false
	t.finished = false
	t.runGen++
	gen := t.runGen
	t.stopChan = make(chan struct{})
	stop := t.stopChan
	t.mu.Unlock()

	go e.runTorrent(t, stop, gen)
	go e.runChoker(t, stop)
	go e.runSeedLimits(t, stop)
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
	if t.State == StateChecking {
		t.restartAfter = false // verification continues, but the torrent stays stopped
		t.mu.Unlock()
		return nil
	}

	t.setStateLocked(StateStopped)
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
	stopped := t.stoppedAnnouncesLocked()
	t.mu.Unlock()

	e.sendStopped(t, stopped)
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
	e.removed = append(e.removed, removedTorrent{id: t.ID, at: time.Now()})
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
		t.setStateLocked(StateError)
		t.LastError = err.Error()
	}
}

// connectPeers dials peers that are not already connected or being dialed, within the peer limit.
func (e *Engine) connectPeers(t *ManagedTorrent, peers []tracker.Peer, source PeerSource) {
	for _, p := range peers {
		addr := p.String()
		if !t.startDial(p) {
			continue
		}
		go func(p tracker.Peer) {
			defer t.endDial(addr)
			e.connectPeer(t, p, source)
		}(p)
	}
}

// startDial reserves a dial slot for p, recording it in the known-peer list.
func (t *ManagedTorrent) startDial(p tracker.Peer) bool {
	addr := p.String()
	t.mu.Lock()
	defer t.mu.Unlock()

	known := false
	for _, kp := range t.Peers {
		if kp.String() == addr {
			known = true
			break
		}
	}
	if !known {
		t.Peers = append(t.Peers, p)
	}

	if t.dialing[addr] || t.ActivePeers+len(t.dialing) >= MaxPeersPerTorrent {
		return false
	}
	for _, c := range t.activeConns {
		if c.RemotePeer.String() == addr {
			return false
		}
	}
	if t.dialing == nil {
		t.dialing = make(map[string]bool)
	}
	t.dialing[addr] = true
	return true
}

func (t *ManagedTorrent) endDial(addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.dialing, addr)
}

// connectPeer dials one peer and runs its session until it disconnects.
func (e *Engine) connectPeer(t *ManagedTorrent, p tracker.Peer, source PeerSource) {
	meta, _, _ := t.components()
	conn, err := peer.Dial(p, meta.InfoHash, e.PeerID, 8*time.Second)
	if err != nil {
		return
	}
	defer conn.Close()

	if !e.registerConn(t, conn, source) {
		return
	}
	defer e.unregisterConn(t, conn)

	log.Printf("[Engine] Connected to peer %s (PeerID: %x)", p.String(), conn.PeerID[:8])

	// Express interest in downloading (or in metadata, for magnets)
	_ = conn.SendInterested()

	e.handlePeerMessages(t, conn)
}

// registerConn adds a connection to the torrent's active set, enforcing the peer limit and
// de-duplicating by peer ID. It returns false if the connection must be dropped.
func (e *Engine) registerConn(t *ManagedTorrent, conn *peer.Conn, source PeerSource) bool {
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

	pi := &peerInfo{
		addr:      conn.RemotePeer.String(),
		port:      int(conn.RemotePeer.Port),
		source:    source,
		incoming:  source == SourceIncoming,
		connected: time.Now(),
		chokeReq:  make(chan bool, 1),
	}
	if conn.RemotePeer.IP != nil {
		pi.ip = conn.RemotePeer.IP.String()
	}
	if t.peerInfos == nil {
		t.peerInfos = make(map[*peer.Conn]*peerInfo)
	}
	t.peerInfos[conn] = pi
	return true
}

// peerInfoFor returns the reporting record of a registered connection.
func (t *ManagedTorrent) peerInfoFor(conn *peer.Conn) *peerInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.peerInfos[conn]
}

func (e *Engine) unregisterConn(t *ManagedTorrent, conn *peer.Conn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.peerInfos, conn)
	for i, c := range t.activeConns {
		if c == conn {
			t.activeConns = append(t.activeConns[:i], t.activeConns[i+1:]...)
			if t.ActivePeers > 0 {
				t.ActivePeers--
			}
			if !conn.AmChoking {
				t.requestRechoke() // an upload slot freed up
			}
			return
		}
	}
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
	e.cancel()
	// Let an in-flight periodic save finish first, so it cannot persist torrents as stopped.
	e.bg.Wait()
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
	// Let trackers know we are gone; each announce is bounded by stoppedAnnounceTimeout.
	e.stopping.Wait()

	if server := e.dhtServer(); server != nil {
		if err := server.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing engine: %v", errs)
	}
	return nil
}
