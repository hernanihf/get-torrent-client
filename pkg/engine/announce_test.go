package engine

import (
	"crypto/rand"
	"encoding/binary"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hernan/get-torrent/pkg/bencode"
	"github.com/hernan/get-torrent/pkg/metainfo"
)

// announceLog is one request received by recordingTracker.
type announceLog struct {
	peerID, event, left string
	at                  time.Time
}

// recordingTracker logs every announce and answers with a fixed peer, interval and status.
type recordingTracker struct {
	*httptest.Server
	mu   sync.Mutex
	log  []announceLog
	peer *net.TCPAddr // returned in every response; may be nil
	resp map[string]any
	fail bool
}

func newRecordingTracker(t *testing.T, resp map[string]any) *recordingTracker {
	t.Helper()
	rt := &recordingTracker{resp: resp}
	rt.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		rt.mu.Lock()
		rt.log = append(rt.log, announceLog{peerID: q.Get("peer_id"), event: q.Get("event"), left: q.Get("left"), at: time.Now()})
		fail, peer := rt.fail, rt.peer
		body := map[string]any{"peers": ""}
		for k, v := range rt.resp {
			body[k] = v
		}
		rt.mu.Unlock()
		if fail {
			http.Error(w, "down", http.StatusInternalServerError)
			return
		}
		if peer != nil {
			compact := make([]byte, 6)
			copy(compact, peer.IP.To4())
			binary.BigEndian.PutUint16(compact[4:], uint16(peer.Port))
			body["peers"] = string(compact)
		}
		out, _ := bencode.Encode(body)
		_, _ = w.Write(out)
	}))
	t.Cleanup(rt.Close)
	return rt
}

// events returns the events announced by the engine with the given peer id, "" meaning none.
func (rt *recordingTracker) events(e *Engine) []string {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	var out []string
	for _, l := range rt.log {
		if l.peerID == string(e.PeerID[:]) {
			out = append(out, l.event)
		}
	}
	return out
}

func (rt *recordingTracker) count() int {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return len(rt.log)
}

func addWithTracker(t *testing.T, e *Engine, payload []byte, announce string) *ManagedTorrent {
	t.Helper()
	meta, err := metainfo.FromInfoBytes(buildTorrent(t, "announce.bin", payload, 32*1024), nil)
	if err != nil {
		t.Fatal(err)
	}
	meta.Announce = announce
	data, _ := meta.Marshal()
	mt, err := e.AddTorrentBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	return mt
}

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	e := NewEngine(t.TempDir(), 0)
	e.ConfigDir = t.TempDir()
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func equalEvents(got []string, want ...string) bool {
	return strings.Join(got, ",") == strings.Join(want, ",")
}

func TestTrackerEventsLifecycle(t *testing.T) {
	payload := make([]byte, 100_000)
	_, _ = rand.Read(payload)
	rt := newRecordingTracker(t, map[string]any{"interval": 3600})

	seeder := newTestEngine(t)
	if err := seeder.StartListener(); err != nil {
		t.Fatal(err)
	}
	rt.mu.Lock()
	rt.peer = &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: int(seeder.Port)}
	rt.mu.Unlock()
	st := addWithTracker(t, seeder, payload, rt.URL+"/announce")
	seedAll(t, st, payload)
	if err := seeder.StartTorrent(st.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "seeder started announce", func() bool { return len(rt.events(seeder)) == 1 })

	leecher := newTestEngine(t)
	lt := addWithTracker(t, leecher, payload, rt.URL+"/announce")
	if err := leecher.StartTorrent(lt.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "download and completed announce", func() bool {
		return len(rt.events(leecher)) == 2
	})
	if got := rt.events(leecher); !equalEvents(got, "started", "completed") {
		t.Errorf("leecher events = %q, want started,completed", got)
	}
	rt.mu.Lock()
	for _, l := range rt.log {
		if l.peerID == string(leecher.PeerID[:]) && l.event == "completed" && l.left != "0" {
			t.Errorf("completed must report left=0, got %s", l.left)
		}
		if l.peerID == string(seeder.PeerID[:]) && l.left != "0" {
			t.Errorf("a complete torrent must announce left=0, got %s", l.left)
		}
	}
	rt.mu.Unlock()

	if err := leecher.StopTorrent(lt.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "stopped announce", func() bool { return len(rt.events(leecher)) == 3 })
	if got := rt.events(leecher); got[2] != "stopped" {
		t.Errorf("leecher events = %q, want stopped last", got)
	}

	// Close waits for the stopped announce. A seeder that started complete never sends
	// "completed", and the tracker's 1 h interval means no regular announce happened.
	if err := seeder.Close(); err != nil {
		t.Fatal(err)
	}
	if got := rt.events(seeder); !equalEvents(got, "started", "stopped") {
		t.Errorf("seeder events = %q, want started,stopped", got)
	}

	// Restarting begins a new tracker session.
	if err := leecher.StartTorrent(lt.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "restart announce", func() bool { return len(rt.events(leecher)) == 4 })
	if got := rt.events(leecher); got[3] != "started" {
		t.Errorf("restart must announce started, got %q", got)
	}
}

func TestTrackerFailureBackoff(t *testing.T) {
	retry := retryInterval
	retryInterval = 40 * time.Millisecond
	t.Cleanup(func() { retryInterval = retry })

	rt := newRecordingTracker(t, nil)
	rt.fail = true
	e := newTestEngine(t)
	mt := addWithTracker(t, e, make([]byte, 1000), rt.URL+"/announce")
	if err := e.StartTorrent(mt.ID); err != nil {
		t.Fatal(err)
	}
	// Back-off 40, 80, 160, 320 ms: about 4 attempts in 700 ms, not one per 40 ms.
	time.Sleep(700 * time.Millisecond)
	if n := rt.count(); n < 3 || n > 6 {
		t.Errorf("expected exponential back-off (3-6 attempts), got %d", n)
	}
	for _, ev := range rt.events(e) {
		if ev != "started" {
			t.Errorf("failed started announces must be retried as started, got %q", ev)
		}
	}
	st := mt.Status()
	if st.ErrorKind != ErrorTrackerError || !strings.Contains(st.Error, "500") {
		t.Errorf("tracker failure must be reported: kind %d, %q", st.ErrorKind, st.Error)
	}

	// Recovery clears the error.
	rt.mu.Lock()
	rt.fail = false
	rt.mu.Unlock()
	waitFor(t, 3*time.Second, "recovery", func() bool { return mt.Status().Error == "" })
	if st := mt.Status(); st.State != StateDownloading {
		t.Errorf("torrent must keep running, state %s", st.State)
	}
}

func TestReannounceHonorsMinInterval(t *testing.T) {
	rt := newRecordingTracker(t, map[string]any{"interval": 3600, "min interval": 3600})
	e := newTestEngine(t)
	mt := addWithTracker(t, e, make([]byte, 1000), rt.URL+"/announce")
	if err := e.StartTorrent(mt.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "started", func() bool { return rt.count() == 1 })

	_ = e.Reannounce(mt.ID)
	time.Sleep(200 * time.Millisecond)
	if n := rt.count(); n != 1 {
		t.Errorf("manual reannounce must wait for min interval, got %d announces", n)
	}
	tr := mt.TrackerList()[0]
	if wait := time.Until(time.Unix(tr.NextAnnounceTime, 0)); wait < 50*time.Minute {
		t.Errorf("next announce must follow the tracker's interval, due in %v", wait)
	}

	// Without a min interval a manual reannounce goes out at once.
	rt.mu.Lock()
	rt.resp = map[string]any{"interval": 3600}
	rt.mu.Unlock()
	e2 := newTestEngine(t)
	mt2 := addWithTracker(t, e2, make([]byte, 2000), rt.URL+"/announce")
	if err := e2.StartTorrent(mt2.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "started", func() bool { return len(rt.events(e2)) == 1 })
	_ = e2.Reannounce(mt2.ID)
	waitFor(t, 5*time.Second, "manual reannounce", func() bool { return len(rt.events(e2)) == 2 })
	if got := rt.events(e2); got[1] != "" {
		t.Errorf("a regular announce carries no event, got %q", got[1])
	}
}

func TestTrackerStatsReportSwarmCounts(t *testing.T) {
	rt := newRecordingTracker(t, map[string]any{"interval": 3600, "complete": 12, "incomplete": 5})
	e := newTestEngine(t)
	mt := addWithTracker(t, e, make([]byte, 1000), rt.URL+"/announce")
	if tr := mt.TrackerList()[0]; tr.SeederCount != -1 || tr.LeecherCount != -1 {
		t.Errorf("counts must be unknown before announcing: %+v", tr)
	}
	if err := e.StartTorrent(mt.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "announce", func() bool { return mt.TrackerList()[0].HasAnnounced })
	if tr := mt.TrackerList()[0]; tr.SeederCount != 12 || tr.LeecherCount != 5 || !tr.LastAnnounceSucceeded {
		t.Errorf("unexpected tracker stats: %+v", tr)
	}
}

func TestDownloadFromIPv6Peer(t *testing.T) {
	if l, err := net.Listen("tcp6", "[::1]:0"); err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	} else {
		_ = l.Close()
	}
	payload := make([]byte, 100_000)
	_, _ = rand.Read(payload)

	seeder := newTestEngine(t)
	if err := seeder.StartListener(); err != nil {
		t.Fatal(err)
	}
	// The tracker only knows the seeder by its IPv6 address (BEP 0007 peers6).
	v6 := binary.BigEndian.AppendUint16(net.IPv6loopback.To16(), seeder.Port)
	tr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := bencode.Encode(map[string]any{"interval": 3600, "peers": "", "peers6": string(v6)})
		_, _ = w.Write(body)
	}))
	defer tr.Close()

	st := addWithTracker(t, seeder, payload, tr.URL+"/announce")
	seedAll(t, st, payload)
	if err := seeder.StartTorrent(st.ID); err != nil {
		t.Fatal(err)
	}
	leecher := newTestEngine(t)
	lt := addWithTracker(t, leecher, payload, tr.URL+"/announce")
	if err := leecher.StartTorrent(lt.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "download over IPv6", func() bool { return lt.Status().PercentDone == 1 })
	peers := lt.PeerList()
	if len(peers) != 1 || !strings.Contains(peers[0].Address, ":") {
		t.Errorf("expected one IPv6 peer, got %+v", peers)
	}
}
