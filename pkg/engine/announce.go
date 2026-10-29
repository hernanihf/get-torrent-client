package engine

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/hernan/get-torrent/pkg/dht"
	"github.com/hernan/get-torrent/pkg/tracker"
)

// Peer discovery scheduling. Each tracker has its own schedule, as in Transmission: the first
// announce of a run carries "started", the one after the download finishes "completed", and
// stopping the torrent sends "stopped" to every tracker that accepted "started" (BEP 0003,
// BEP 0015). Regular announces follow the interval the tracker asked for. The DHT is queried on
// its own timer.
var (
	// reannounceInterval is how often the DHT is asked again for peers; retryInterval is used
	// instead while no peer is connected, and as the first back-off step after a failure.
	reannounceInterval = 5 * time.Minute
	retryInterval      = time.Minute
)

const (
	// defaultTrackerInterval applies when a tracker response carries no interval.
	defaultTrackerInterval = 30 * time.Minute
	// minTrackerInterval protects trackers from intervals that are absurdly short.
	minTrackerInterval = 30 * time.Second
	// maxRetryBackoff caps the delay between announces to a failing tracker.
	maxRetryBackoff = 30 * time.Minute
	// stoppedAnnounceTimeout bounds a "stopped" announce, so shutdown is not held up by a
	// dead tracker.
	stoppedAnnounceTimeout = 5 * time.Second
	// dhtLookupTimeout bounds one DHT get_peers/announce_peer round.
	dhtLookupTimeout = time.Minute
)

var errNoPeerSources = errors.New("no peer sources: torrent has no trackers and DHT is disabled")

// trackerRecord is the announce state of one tracker URL.
type trackerRecord struct {
	// Outcome of the most recent announce, for reporting.
	time      time.Time
	succeeded bool
	result    string
	peers     int
	seeders   int64 // -1 if unknown
	leechers  int64 // -1 if unknown

	// Scheduling state, valid for run gen only (StartTorrent begins a new run).
	gen         int
	started     bool // the tracker accepted "started"
	completed   bool // "completed" was sent
	inFlight    bool
	next        time.Time // when the next regular announce is due
	minInterval time.Duration
	failures    int    // consecutive failures, for back-off
	trackerID   string // echoed back to HTTP trackers
}

// dhtSchedule is the DHT counterpart of trackerRecord.
type dhtSchedule struct {
	gen      int
	inFlight bool
	next     time.Time
	err      error // outcome of the last lookup
	done     bool  // at least one lookup finished this run
}

// pendingAnnounce is a tracker announce that the discovery loop is about to send.
type pendingAnnounce struct {
	url       string
	event     tracker.Event
	trackerID string
}

// trackerURLsLocked returns the trackers in use: the metainfo's, or the magnet's until the
// metadata provides its own. Caller holds t.mu.
func (t *ManagedTorrent) trackerURLsLocked() []string {
	if urls := t.Meta.Trackers(); len(urls) > 0 {
		return urls
	}
	return t.trackers
}

// recordLocked returns the record for url, resetting its scheduling state if it belongs to an
// earlier run. Caller holds t.mu for writing.
func (t *ManagedTorrent) recordLocked(url string) *trackerRecord {
	if t.trackerRecords == nil {
		t.trackerRecords = make(map[string]*trackerRecord)
	}
	rec := t.trackerRecords[url]
	if rec == nil {
		rec = &trackerRecord{seeders: -1, leechers: -1}
		t.trackerRecords[url] = rec
	}
	if rec.gen != t.runGen {
		*rec = trackerRecord{
			time: rec.time, succeeded: rec.succeeded, result: rec.result, peers: rec.peers,
			seeders: rec.seeders, leechers: rec.leechers, trackerID: rec.trackerID, gen: t.runGen,
		}
	}
	return rec
}

// dueAnnounces marks the trackers whose announce is due as in flight and returns them. force
// (a manual reannounce) makes every tracker due whose minimum interval has passed.
func (t *ManagedTorrent) dueAnnounces(now time.Time, force bool) []pendingAnnounce {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []pendingAnnounce
	for _, url := range t.trackerURLsLocked() {
		rec := t.recordLocked(url)
		if rec.inFlight {
			continue
		}
		ev := tracker.EventNone
		due := !now.Before(rec.next) || (force && !now.Before(rec.time.Add(rec.minInterval)))
		switch {
		case !rec.started:
			ev = tracker.EventStarted
		case t.sendCompleted && !rec.completed:
			ev, due = tracker.EventCompleted, true // sent as soon as the download finishes
		}
		if !due {
			continue
		}
		rec.inFlight = true
		out = append(out, pendingAnnounce{url: url, event: ev, trackerID: rec.trackerID})
	}
	return out
}

// finishAnnounce records the outcome of an announce sent during run gen and schedules the next.
func (t *ManagedTorrent) finishAnnounce(gen int, p pendingAnnounce, resp *tracker.AnnounceResponse, err error) {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	rec := t.trackerRecords[p.url]
	if rec == nil || gen != t.runGen || rec.gen != gen {
		return // the torrent was stopped (and maybe restarted) meanwhile
	}
	rec.inFlight = false
	rec.time = now
	rec.succeeded = err == nil
	if err != nil {
		rec.result = err.Error()
		rec.peers = 0
		rec.failures++
		backoff := maxRetryBackoff
		if shift := rec.failures - 1; shift < 16 {
			backoff = min(retryInterval<<shift, maxRetryBackoff)
		}
		rec.next = now.Add(backoff)
		t.updateDiscoveryErrorLocked()
		return
	}

	rec.result = "Success"
	if resp.Warning != "" {
		rec.result = "Warning: " + resp.Warning
	}
	rec.peers = len(resp.Peers)
	rec.seeders, rec.leechers = resp.Seeders, resp.Leechers
	rec.failures = 0
	if resp.TrackerID != "" {
		rec.trackerID = resp.TrackerID
	}
	switch p.event {
	case tracker.EventStarted:
		rec.started = true
		// Started with the data already complete: there is no download to report as completed.
		rec.completed = t.sendCompleted || t.State == StateSeeding
	case tracker.EventCompleted:
		rec.completed = true
	}

	interval := time.Duration(resp.Interval) * time.Second
	if interval <= 0 {
		interval = defaultTrackerInterval
	}
	interval = max(interval, minTrackerInterval)
	rec.minInterval = time.Duration(resp.MinInterval) * time.Second
	wait := interval
	if t.ActivePeers == 0 {
		// Nobody to talk to: ask again sooner, but never before the tracker's minimum.
		wait = min(interval, max(retryInterval, rec.minInterval, minTrackerInterval))
	}
	rec.next = now.Add(wait)
	t.updateDiscoveryErrorLocked()
}

// updateDiscoveryErrorLocked reports peer discovery as failing while no source (tracker or DHT)
// answered its latest query this run. Caller holds t.mu.
func (t *ManagedTorrent) updateDiscoveryErrorLocked() {
	var errs []string
	for _, url := range t.trackerURLsLocked() {
		rec := t.trackerRecords[url]
		if rec == nil || rec.gen != t.runGen || rec.time.IsZero() {
			continue
		}
		if rec.succeeded {
			t.LastError = ""
			return
		}
		errs = append(errs, fmt.Sprintf("%s: %s", url, rec.result))
	}
	if d := t.dht; d.gen == t.runGen && d.done {
		if d.err == nil {
			t.LastError = ""
			return
		}
		errs = append(errs, fmt.Sprintf("%s: %v", SourceDHT, d.err))
	}
	if len(errs) > 0 {
		t.LastError = "peer discovery failed: " + strings.Join(errs, "; ")
	}
}

// nextDiscovery returns how long the loop may sleep before something is due.
func (t *ManagedTorrent) nextDiscovery(now time.Time, dhtEnabled bool) time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()
	next := now.Add(reannounceInterval)
	for _, url := range t.trackerURLsLocked() {
		rec := t.trackerRecords[url]
		if rec == nil || rec.gen != t.runGen {
			return 0 // never announced this run
		}
		if !rec.inFlight && rec.next.Before(next) {
			next = rec.next
		}
	}
	if dhtEnabled && t.dht.gen == t.runGen && !t.dht.inFlight && t.dht.next.Before(next) {
		next = t.dht.next
	}
	return max(next.Sub(now), 0)
}

// wakeDiscovery makes the discovery loop re-evaluate its schedule.
func (t *ManagedTorrent) wakeDiscovery() {
	select {
	case t.discoveryWake <- struct{}{}:
	default:
	}
}

// announceParams builds the announce parameters common to every tracker of t.
func (e *Engine) announceParams(t *ManagedTorrent) tracker.AnnounceParams {
	t.mu.RLock()
	defer t.mu.RUnlock()
	p := tracker.AnnounceParams{
		InfoHash:   t.Meta.InfoHash,
		PeerID:     e.PeerID,
		Port:       e.Port,
		Uploaded:   t.Uploaded,
		Downloaded: t.Downloaded,
		Left:       t.Left,
		Compact:    true,
		Key:        e.announceKey,
		NumWant:    MaxPeersPerTorrent,
	}
	if t.PieceManager == nil {
		// Size unknown until metadata arrives; a non-zero value marks us as a leecher.
		p.Left = 1
	} else {
		// Verified data is authoritative, as in Status.
		_, have := t.PieceManager.Progress()
		p.Left = t.Meta.TotalLength - have
	}
	return p
}

func (e *Engine) runTorrent(t *ManagedTorrent, stop <-chan struct{}, gen int) {
	meta, _, _ := t.components()
	log.Printf("[Engine] Starting download loop for %q", meta.Name)

	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()
	go func() {
		select {
		case <-stop:
			cancel()
		case <-ctx.Done():
		}
	}()

	force := false
	for {
		if err := e.launchDiscovery(ctx, t, gen, force); err != nil {
			e.setError(t, err)
			return
		}
		force = false

		timer := time.NewTimer(t.nextDiscovery(time.Now(), e.dhtFor(t) != nil))
		select {
		case <-stop:
			timer.Stop()
			return
		case <-timer.C:
		case <-t.reannounce:
			timer.Stop()
			force = true
		case <-t.discoveryWake:
			timer.Stop()
		}
	}
}

// dhtFor returns the DHT node to use for t, or nil: private torrents must not leak to the DHT
// (BEP 0027).
func (e *Engine) dhtFor(t *ManagedTorrent) *dht.Server {
	server := e.dhtServer()
	if server == nil {
		return nil
	}
	meta, _, _ := t.components()
	if meta.Private {
		return nil
	}
	return server
}

// launchDiscovery starts the announces and the DHT lookup that are due. Each one dials the
// peers it finds as soon as it returns, then wakes the loop to reschedule.
func (e *Engine) launchDiscovery(ctx context.Context, t *ManagedTorrent, gen int, force bool) error {
	t.mu.RLock()
	hasTrackers := len(t.trackerURLsLocked()) > 0
	t.mu.RUnlock()
	server := e.dhtFor(t)
	if !hasTrackers && server == nil {
		return errNoPeerSources
	}

	if due := t.dueAnnounces(time.Now(), force); len(due) > 0 {
		params := e.announceParams(t)
		for _, p := range due {
			go func(p pendingAnnounce) {
				ap := params
				ap.AnnounceURL, ap.Event, ap.TrackerID = p.url, p.event, p.trackerID
				resp, err := tracker.AnnounceContext(ctx, ap)
				if ctx.Err() != nil {
					return // stopped: the result belongs to a finished run
				}
				if err != nil {
					log.Printf("[Engine] Announce to %s failed: %v", p.url, err)
				} else {
					log.Printf("[Engine] %s returned %d peers (event %q)", p.url, len(resp.Peers), p.event)
				}
				t.finishAnnounce(gen, p, resp, err)
				if err == nil {
					e.connectPeers(t, resp.Peers, SourceTracker)
				}
				t.wakeDiscovery()
			}(p)
		}
	}

	if server != nil && t.startDHTLookup(time.Now(), force) {
		meta, _, _ := t.components()
		go func() {
			lctx, lcancel := context.WithTimeout(ctx, dhtLookupTimeout)
			defer lcancel()
			peers, err := server.Announce(lctx, meta.InfoHash, e.Port)
			if ctx.Err() != nil {
				return
			}
			if err == nil {
				log.Printf("[Engine] %s returned %d peers for %q", SourceDHT, len(peers), meta.Name)
				e.connectPeers(t, peers, SourceDHT)
			}
			t.finishDHTLookup(gen, err)
			t.wakeDiscovery()
		}()
	}
	return nil
}

// startDHTLookup marks a DHT lookup as in flight if one is due.
func (t *ManagedTorrent) startDHTLookup(now time.Time, force bool) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.dht.gen != t.runGen {
		t.dht = dhtSchedule{gen: t.runGen}
	}
	if t.dht.inFlight || (!force && now.Before(t.dht.next)) {
		return false
	}
	t.dht.inFlight = true
	return true
}

func (t *ManagedTorrent) finishDHTLookup(gen int, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if gen != t.runGen || t.dht.gen != gen {
		return
	}
	wait := reannounceInterval
	if t.ActivePeers == 0 || err != nil {
		wait = retryInterval
	}
	t.dht = dhtSchedule{gen: gen, next: time.Now().Add(wait), err: err, done: true}
	t.updateDiscoveryErrorLocked()
}

// stoppedAnnouncesLocked ends the current run's tracker sessions: it returns the announces
// that must carry "stopped" (trackers that accepted "started") and invalidates in-flight ones.
// Caller holds t.mu for writing.
func (t *ManagedTorrent) stoppedAnnouncesLocked() []pendingAnnounce {
	var out []pendingAnnounce
	for url, rec := range t.trackerRecords {
		if rec.gen == t.runGen && rec.started {
			out = append(out, pendingAnnounce{url: url, event: tracker.EventStopped, trackerID: rec.trackerID})
		}
	}
	t.runGen++ // results still in flight now belong to a finished run
	return out
}

// sendStopped announces "stopped" in the background. Close waits for these, each bounded by
// stoppedAnnounceTimeout.
func (e *Engine) sendStopped(t *ManagedTorrent, announces []pendingAnnounce) {
	if len(announces) == 0 {
		return
	}
	params := e.announceParams(t)
	params.NumWant = 0
	for _, p := range announces {
		e.stopping.Add(1)
		go func(p pendingAnnounce) {
			defer e.stopping.Done()
			ctx, cancel := context.WithTimeout(context.Background(), stoppedAnnounceTimeout)
			defer cancel()
			ap := params
			ap.AnnounceURL, ap.Event, ap.TrackerID = p.url, p.event, p.trackerID
			if _, err := tracker.AnnounceContext(ctx, ap); err != nil {
				log.Printf("[Engine] Stopped announce to %s failed: %v", p.url, err)
			}
		}(p)
	}
}

// downloadFinished queues the "completed" event for trackers. Caller holds t.mu for writing.
func (t *ManagedTorrent) downloadFinishedLocked() {
	t.sendCompleted = true
	t.wakeDiscovery()
}
