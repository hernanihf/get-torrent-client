package tracker

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/hernan/get-torrent/pkg/bencode"
)

func TestHTTPAnnounceParamsAndResponseFields(t *testing.T) {
	var got map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		got = map[string]string{"event": q.Get("event"), "key": q.Get("key"), "numwant": q.Get("numwant"), "trackerid": q.Get("trackerid")}
		body, _ := bencode.Encode(map[string]any{
			"interval": 1800, "min interval": 900, "tracker id": "abc", "warning message": "slow down",
			"complete": 7, "incomplete": 3, "peers": "",
		})
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	resp, err := AnnounceContext(context.Background(), AnnounceParams{
		AnnounceURL: srv.URL + "/announce", Event: EventStarted, Key: 0xDEADBEEF, NumWant: 50, TrackerID: "prev",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"event": "started", "key": "DEADBEEF", "numwant": "50", "trackerid": "prev"}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("query %s = %q, want %q", k, got[k], v)
		}
	}
	if resp.Interval != 1800 || resp.MinInterval != 900 || resp.TrackerID != "abc" || resp.Warning != "slow down" ||
		resp.Seeders != 7 || resp.Leechers != 3 {
		t.Errorf("unexpected response: %+v", resp)
	}

	// EventNone and zero optional fields are omitted.
	if _, err := AnnounceContext(context.Background(), AnnounceParams{AnnounceURL: srv.URL}); err != nil {
		t.Fatal(err)
	}
	for k, v := range got {
		if v != "" {
			t.Errorf("%s must be omitted, got %q", k, v)
		}
	}
}

func TestHTTPAnnounceContextCancel(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	defer srv.Close()
	defer close(release)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := AnnounceContext(ctx, AnnounceParams{AnnounceURL: srv.URL}); err == nil {
		t.Fatal("expected an error from a cancelled announce")
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("cancellation took %v", time.Since(start))
	}
}

func TestEventCodes(t *testing.T) {
	// BEP 0015 numeric codes.
	if EventNone != 0 || EventCompleted != 1 || EventStarted != 2 || EventStopped != 3 {
		t.Fatal("event codes do not match BEP 0015")
	}
}

// fakeUDPTracker is a BEP 0015 tracker on loopback. dropFirst packets are ignored, to exercise
// retransmission.
type fakeUDPTracker struct {
	conn      *net.UDPConn
	dropFirst int

	mu        sync.Mutex
	received  int
	connects  int
	announces [][]byte
	errorMsg  string
}

func newFakeUDPTracker(t *testing.T, dropFirst int, errorMsg string) *fakeUDPTracker {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeUDPTracker{conn: conn, dropFirst: dropFirst, errorMsg: errorMsg}
	t.Cleanup(func() { _ = conn.Close() })
	go f.serve()
	return f
}

func (f *fakeUDPTracker) url() string { return "udp://" + f.conn.LocalAddr().String() + "/announce" }

func (f *fakeUDPTracker) serve() {
	const connID = 0x1122334455667788
	buf := make([]byte, 2048)
	for {
		n, addr, err := f.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		f.mu.Lock()
		f.received++
		drop := f.received <= f.dropFirst
		errMsg := f.errorMsg
		f.mu.Unlock()
		if drop || n < 16 {
			continue
		}
		action := binary.BigEndian.Uint32(buf[8:12])
		tx := buf[12:16]
		var out []byte
		switch {
		case action == actionConnect && binary.BigEndian.Uint64(buf[0:8]) == udpProtocolID:
			f.mu.Lock()
			f.connects++
			f.mu.Unlock()
			out = binary.BigEndian.AppendUint32(nil, actionConnect)
			out = append(out, tx...)
			out = binary.BigEndian.AppendUint64(out, connID)
		case action == actionAnnounce && n >= 98 && binary.BigEndian.Uint64(buf[0:8]) == connID:
			f.mu.Lock()
			f.announces = append(f.announces, append([]byte(nil), buf[:n]...))
			f.mu.Unlock()
			if errMsg != "" {
				out = binary.BigEndian.AppendUint32(nil, actionError)
				out = append(out, tx...)
				out = append(out, errMsg...)
				break
			}
			// A stray packet with another transaction id first: the client must skip it.
			_, _ = f.conn.WriteToUDP(append(binary.BigEndian.AppendUint32(nil, actionAnnounce), 0, 0, 0, 0), addr)
			out = binary.BigEndian.AppendUint32(nil, actionAnnounce)
			out = append(out, tx...)
			out = binary.BigEndian.AppendUint32(out, 1800) // interval
			out = binary.BigEndian.AppendUint32(out, 4)    // leechers
			out = binary.BigEndian.AppendUint32(out, 9)    // seeders
			out = append(out, 10, 0, 0, 1, 0x1A, 0xE1)     // 10.0.0.1:6881
		default:
			continue
		}
		_, _ = f.conn.WriteToUDP(out, addr)
	}
}

func shortUDPTimeouts(t *testing.T) {
	t.Helper()
	base, retries := udpBaseTimeout, udpMaxRetries
	udpBaseTimeout, udpMaxRetries = 50*time.Millisecond, 2
	t.Cleanup(func() { udpBaseTimeout, udpMaxRetries = base, retries })
}

func TestUDPAnnounceEventAndConnIDReuse(t *testing.T) {
	shortUDPTimeouts(t)
	f := newFakeUDPTracker(t, 0, "")
	params := AnnounceParams{AnnounceURL: f.url(), Event: EventStarted, Key: 42, NumWant: 30, Port: 6881, Left: 100}

	resp, err := UDPAnnounceContext(context.Background(), params)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Interval != 1800 || resp.Leechers != 4 || resp.Seeders != 9 || len(resp.Peers) != 1 || resp.Peers[0].String() != "10.0.0.1:6881" {
		t.Errorf("unexpected response: %+v", resp)
	}

	params.Event = EventStopped
	if _, err := UDPAnnounceContext(context.Background(), params); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.connects != 1 {
		t.Errorf("connection id must be reused within a minute, got %d connects", f.connects)
	}
	if len(f.announces) != 2 {
		t.Fatalf("expected 2 announces, got %d", len(f.announces))
	}
	first, second := f.announces[0], f.announces[1]
	if ev := binary.BigEndian.Uint32(first[80:84]); ev != uint32(EventStarted) {
		t.Errorf("event = %d, want started", ev)
	}
	if ev := binary.BigEndian.Uint32(second[80:84]); ev != uint32(EventStopped) {
		t.Errorf("event = %d, want stopped", ev)
	}
	if key := binary.BigEndian.Uint32(first[88:92]); key != 42 {
		t.Errorf("key = %d", key)
	}
	if nw := int32(binary.BigEndian.Uint32(first[92:96])); nw != 30 {
		t.Errorf("num_want = %d", nw)
	}
}

func TestUDPAnnounceRetransmits(t *testing.T) {
	shortUDPTimeouts(t)
	f := newFakeUDPTracker(t, 2, "") // lose the first connect and its retransmission
	if _, err := UDPAnnounceContext(context.Background(), AnnounceParams{AnnounceURL: f.url()}); err != nil {
		t.Fatalf("announce must succeed after retransmission: %v", err)
	}
}

func TestUDPAnnounceGivesUp(t *testing.T) {
	shortUDPTimeouts(t)
	f := newFakeUDPTracker(t, 1<<30, "")
	start := time.Now()
	_, err := UDPAnnounceContext(context.Background(), AnnounceParams{AnnounceURL: f.url()})
	if !errors.Is(err, errUDPTimeout) {
		t.Fatalf("expected a timeout error, got %v", err)
	}
	// 50 + 100 + 200 ms.
	if d := time.Since(start); d < 300*time.Millisecond || d > 2*time.Second {
		t.Errorf("unexpected total wait %v", d)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.received != udpMaxRetries+1 {
		t.Errorf("expected %d attempts, got %d", udpMaxRetries+1, f.received)
	}
}

func TestUDPAnnounceCancel(t *testing.T) {
	udpBase := udpBaseTimeout
	udpBaseTimeout = time.Minute
	t.Cleanup(func() { udpBaseTimeout = udpBase })
	f := newFakeUDPTracker(t, 1<<30, "")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := UDPAnnounceContext(ctx, AnnounceParams{AnnounceURL: f.url()}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline error, got %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("cancellation took %v", d)
	}
}

func TestUDPAnnounceTrackerError(t *testing.T) {
	shortUDPTimeouts(t)
	f := newFakeUDPTracker(t, 0, "torrent not registered")
	_, err := UDPAnnounceContext(context.Background(), AnnounceParams{AnnounceURL: f.url()})
	if !errors.Is(err, ErrTrackerFailure) || !containsSubstring(err.Error(), "torrent not registered") {
		t.Errorf("expected tracker failure, got %v", err)
	}
}
