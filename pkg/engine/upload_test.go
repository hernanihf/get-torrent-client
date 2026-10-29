package engine

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/hernan/get-torrent/pkg/metainfo"
	"github.com/hernan/get-torrent/pkg/peer"
)

func TestUploadQueue(t *testing.T) {
	q := newUploadQueue()
	a, b, c := blockRequest{0, 0, 16384}, blockRequest{0, 16384, 16384}, blockRequest{1, 0, 16384}
	q.push(a)
	q.push(a) // duplicate
	q.push(b)
	q.push(c)
	if q.len() != 3 {
		t.Fatalf("expected 3 queued requests, got %d", q.len())
	}

	q.cancel(b)
	ctx := context.Background()
	if r, _ := q.next(ctx); r != a {
		t.Errorf("expected FIFO order, got %+v", r)
	}
	q.push(a) // already being served
	if q.len() != 1 {
		t.Errorf("a request being served must not be queued again, len %d", q.len())
	}
	if !q.finish() {
		t.Errorf("an uncancelled request must be sent")
	}
	if r, _ := q.next(ctx); r != c {
		t.Errorf("cancelled request must be skipped, got %+v", r)
	}
	q.cancel(c) // cancelled while waiting for bandwidth
	if q.finish() {
		t.Errorf("a request cancelled while being served must not be sent")
	}

	// Choking drops everything, including the request being served.
	q.push(a)
	q.push(b)
	q.next(ctx)
	q.clear()
	if q.len() != 0 || q.finish() {
		t.Errorf("clear must drop queued and current requests")
	}

	for i := range maxUploadQueue + 10 {
		q.push(blockRequest{i, 0, 16384})
	}
	if q.len() != maxUploadQueue {
		t.Errorf("queue must be capped at %d, got %d", maxUploadQueue, q.len())
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	q.clear()
	if _, ok := q.next(cctx); ok {
		t.Errorf("next must return when the context is done")
	}
}

func (p *rawPeer) count(id peer.MessageID) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, g := range p.got {
		if g == id {
			n++
		}
	}
	return n
}

func TestCancelledRequestsAreNotServed(t *testing.T) {
	payload := make([]byte, 4*16384)
	info := buildTorrent(t, "cancel.bin", payload, 16384)
	meta, _ := metainfo.FromInfoBytes(info, nil)

	seeder := newTestEngine(t)
	if err := seeder.StartListener(); err != nil {
		t.Fatal(err)
	}
	tracker := fakeTracker(t, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: int(seeder.Port)})
	defer tracker.Close()
	meta.Announce = tracker.URL + "/announce"
	raw, _ := meta.Marshal()
	st, err := seeder.AddTorrentBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	seedAll(t, st, payload)
	// 16 kB/s: the first block comes from the burst, each later one takes about a second.
	if err := seeder.SetTorrentLimits(st.ID, func(l *TorrentLimits) { l.UploadLimit, l.UploadLimited = 16, true }); err != nil {
		t.Fatal(err)
	}
	if err := seeder.StartTorrent(st.ID); err != nil {
		t.Fatal(err)
	}

	p := dialRawPeer(t, seeder.Port, meta.InfoHash, 0)
	p.send(t, &peer.Message{ID: idPtr(peer.MsgInterested)})
	waitFor(t, 3*time.Second, "unchoke", p.unchoked)

	for i := range 4 {
		p.send(t, peer.FormatRequest(i, 0, 16384))
	}
	p.send(t, peer.FormatCancel(2, 0, 16384))
	p.send(t, peer.FormatCancel(3, 0, 16384))

	waitFor(t, 4*time.Second, "two blocks", func() bool { return p.count(peer.MsgPiece) == 2 })
	time.Sleep(2500 * time.Millisecond) // long enough for two more blocks at 16 kB/s
	if n := p.count(peer.MsgPiece); n != 2 {
		t.Errorf("cancelled blocks must not be sent: got %d pieces, want 2", n)
	}
}
