package engine

import (
	"fmt"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hernan/get-torrent/pkg/metainfo"
	"github.com/hernan/get-torrent/pkg/peer"
)

func conns(n int) []*peer.Conn {
	cs := make([]*peer.Conn, n)
	for i := range cs {
		cs[i] = &peer.Conn{}
	}
	return cs
}

func TestSelectUnchokedFewPeers(t *testing.T) {
	cs := conns(3)
	cands := []chokeCandidate{
		{conn: cs[0], interested: true},
		{conn: cs[1], interested: false, rate: 1 << 20}, // fast but not interested
		{conn: cs[2], interested: true},
	}
	unchoke, opt := selectUnchoked(cands, nil, false, rand.New(rand.NewSource(1)))
	if len(unchoke) != 2 || !unchoke[cs[0]] || !unchoke[cs[2]] || opt != nil {
		t.Errorf("all interested peers fit in the regular slots: unchoke=%v opt=%v", unchoke, opt)
	}
}

func TestSelectUnchokedRanksByRate(t *testing.T) {
	cs := conns(7)
	cands := make([]chokeCandidate, len(cs))
	for i, c := range cs {
		cands[i] = chokeCandidate{conn: c, interested: true, rate: int64(i * 1000)}
	}
	unchoke, opt := selectUnchoked(cands, nil, false, rand.New(rand.NewSource(1)))
	if len(unchoke) != uploadSlots+1 {
		t.Fatalf("expected %d unchoked, got %d", uploadSlots+1, len(unchoke))
	}
	for _, c := range cs[3:] { // the four fastest
		if !unchoke[c] {
			t.Errorf("fast peer not unchoked")
		}
	}
	if opt == nil || !unchoke[opt] || (opt != cs[0] && opt != cs[1] && opt != cs[2]) {
		t.Errorf("optimistic unchoke must be one of the slower peers: %v", opt)
	}
}

func TestSelectUnchokedOptimisticRotation(t *testing.T) {
	cs := conns(7)
	cands := make([]chokeCandidate, len(cs))
	for i, c := range cs {
		cands[i] = chokeCandidate{conn: c, interested: true, rate: int64(i * 1000)}
	}
	rng := rand.New(rand.NewSource(1))

	// Kept between rotations while it still qualifies.
	if _, opt := selectUnchoked(cands, cs[1], false, rng); opt != cs[1] {
		t.Errorf("optimistic peer must be kept until rotation, got %v", opt)
	}
	// Replaced once it loses interest, even before rotation.
	cands[1].interested = false
	if unchoke, opt := selectUnchoked(cands, cs[1], false, rng); opt == cs[1] || unchoke[cs[1]] || opt == nil {
		t.Errorf("uninterested optimistic peer must be replaced, got %v", opt)
	}
	// A peer promoted into a regular slot frees the optimistic slot for someone else.
	cands[1].interested = true
	cands[1].rate = 1 << 30
	if unchoke, opt := selectUnchoked(cands, cs[1], false, rng); opt == cs[1] || opt == nil || !unchoke[cs[1]] {
		t.Errorf("promoted peer must leave the optimistic slot, got %v", opt)
	}
}

func TestSelectUnchokedFavoursNewPeers(t *testing.T) {
	cs := conns(uploadSlots + 4)
	cands := make([]chokeCandidate, len(cs))
	for i, c := range cs {
		cands[i] = chokeCandidate{conn: c, interested: true, rate: 1000}
	}
	for i := range uploadSlots { // fill the regular slots with clearly faster peers
		cands[i].rate = 1 << 20
	}
	newPeer := cs[uploadSlots]
	cands[uploadSlots].isNew = true

	rng := rand.New(rand.NewSource(1))
	picked := 0
	const trials = 6000
	for range trials {
		if _, opt := selectUnchoked(cands, nil, true, rng); opt == newPeer {
			picked++
		}
	}
	// Weight 3 against three peers of weight 1: expected share 3/6.
	if share := float64(picked) / trials; share < 0.45 || share > 0.55 {
		t.Errorf("new peer picked %.2f of the time, want about 0.5", share)
	}
}

// rawPeer is a minimal remote peer driven by the test.
type rawPeer struct {
	conn net.Conn
	mu   sync.Mutex
	got  []peer.MessageID
}

func dialRawPeer(t *testing.T, port uint16, infoHash [20]byte, n int) *rawPeer {
	t.Helper()
	c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var id [20]byte
	copy(id[:], fmt.Sprintf("-XX0001-%012d", n))
	if _, err := c.Write(peer.NewHandshake(infoHash, id).Serialize()); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := peer.ReadHandshake(c); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Time{})
	p := &rawPeer{conn: c}
	go func() {
		for {
			msg, err := peer.ReadMessage(c)
			if err != nil {
				return
			}
			if msg.ID != nil {
				p.mu.Lock()
				p.got = append(p.got, *msg.ID)
				p.mu.Unlock()
			}
		}
	}()
	t.Cleanup(func() { _ = c.Close() })
	return p
}

func (p *rawPeer) send(t *testing.T, msg *peer.Message) {
	t.Helper()
	if _, err := p.conn.Write(msg.Serialize()); err != nil {
		t.Fatal(err)
	}
}

// unchoked reports whether the last choke-related message received was an unchoke.
func (p *rawPeer) unchoked() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := len(p.got) - 1; i >= 0; i-- {
		switch p.got[i] {
		case peer.MsgUnchoke:
			return true
		case peer.MsgChoke:
			return false
		}
	}
	return false
}

func (p *rawPeer) received(id peer.MessageID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, g := range p.got {
		if g == id {
			return true
		}
	}
	return false
}

func TestChokerLimitsUnchokedPeers(t *testing.T) {
	payload := make([]byte, 64*1024)
	info := buildTorrent(t, "choke.bin", payload, 16384)
	meta, _ := metainfo.FromInfoBytes(info, nil)

	seeder := NewEngine(t.TempDir(), 0)
	seeder.ConfigDir = t.TempDir()
	defer seeder.Close()
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
	if err := seeder.StartTorrent(st.ID); err != nil {
		t.Fatal(err)
	}

	const n = uploadSlots + 3
	peers := make([]*rawPeer, n)
	for i := range peers {
		peers[i] = dialRawPeer(t, seeder.Port, meta.InfoHash, i)
		peers[i].send(t, &peer.Message{ID: idPtr(peer.MsgInterested)})
	}

	countUnchoked := func() (unchoked, choked []*rawPeer) {
		for _, p := range peers {
			if p.unchoked() {
				unchoked = append(unchoked, p)
			} else {
				choked = append(choked, p)
			}
		}
		return
	}
	waitFor(t, 3*time.Second, "unchokes", func() bool {
		u, _ := countUnchoked()
		return len(u) == uploadSlots+1
	})
	time.Sleep(200 * time.Millisecond) // no further unchokes beyond the slots
	unchoked, choked := countUnchoked()
	if len(unchoked) != uploadSlots+1 {
		t.Fatalf("expected %d unchoked peers, got %d", uploadSlots+1, len(unchoked))
	}

	// A choked peer's requests are ignored; an unchoked peer's are served.
	choked[0].send(t, peer.FormatRequest(0, 0, 16384))
	unchoked[0].send(t, peer.FormatRequest(0, 0, 16384))
	waitFor(t, 2*time.Second, "piece for unchoked peer", func() bool { return unchoked[0].received(peer.MsgPiece) })
	time.Sleep(100 * time.Millisecond)
	if choked[0].received(peer.MsgPiece) {
		t.Errorf("choked peer must not be served")
	}

	// Disconnecting an unchoked peer frees its slot for a waiting one right away.
	_ = unchoked[0].conn.Close()
	waitFor(t, 3*time.Second, "freed slot reassigned", func() bool {
		for _, p := range choked {
			if p.unchoked() {
				return true
			}
		}
		return false
	})
}
