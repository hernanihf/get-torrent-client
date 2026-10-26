package dht

import (
	"context"
	"errors"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/hernan/get-torrent/pkg/bencode"
	"github.com/hernan/get-torrent/pkg/tracker"
)

func newTestServer(t *testing.T, bootstrap ...string) *Server {
	t.Helper()
	s, err := New(Config{Addr: "127.0.0.1:0", Bootstrap: bootstrap, QueryTimeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func idWithPrefix(b ...byte) NodeID {
	var id NodeID
	copy(id[:], b)
	return id
}

func TestCompactNodesRoundTrip(t *testing.T) {
	in := []NodeInfo{
		{ID: idWithPrefix(1), Addr: &net.UDPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 6881}},
		{ID: idWithPrefix(2), Addr: &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}}, // IPv6: skipped
		{ID: idWithPrefix(3), Addr: &net.UDPAddr{IP: net.IPv4(192, 168, 1, 2), Port: 65535}},
	}
	enc := encodeNodes(in)
	if len(enc) != 2*compactNodeLen {
		t.Fatalf("expected 2 encoded nodes, got %d bytes", len(enc))
	}
	// A trailing partial record and a zero-port record must be ignored.
	zeroPort := encodeNodes([]NodeInfo{{ID: idWithPrefix(4), Addr: &net.UDPAddr{IP: net.IPv4(1, 1, 1, 1), Port: 0}}})
	out := decodeNodes(enc + zeroPort + "partial")
	if len(out) != 2 || out[0].ID != in[0].ID || !sameAddr(out[0].Addr, in[0].Addr) || !sameAddr(out[1].Addr, in[2].Addr) {
		t.Fatalf("round trip mismatch: %+v", out)
	}
}

func TestCompactPeers(t *testing.T) {
	p := tracker.Peer{IP: net.IPv4(1, 2, 3, 4), Port: 51413}
	enc := encodePeer(p)
	got := decodePeers([]any{enc, "short", int64(5), "\x01\x02\x03\x04\x00\x00"})
	if len(got) != 1 || !got[0].IP.Equal(p.IP) || got[0].Port != p.Port {
		t.Fatalf("unexpected peers: %v", got)
	}
	if encodePeer(tracker.Peer{IP: net.ParseIP("::1"), Port: 1}) != "" {
		t.Errorf("IPv6 peers must not be encoded in compact IPv4 form")
	}
}

func TestParseMessage(t *testing.T) {
	q := &message{T: "aa", Y: "q", Q: "ping", A: map[string]any{"id": strings.Repeat("x", 20)}}
	m, err := parseMessage(q.encode())
	if err != nil || m.Q != "ping" || m.T != "aa" {
		t.Fatalf("query round trip failed: %+v, %v", m, err)
	}
	e := &message{T: "bb", Y: "e", E: &KRPCError{Code: ErrCodeMethod, Msg: "nope"}}
	m, err = parseMessage(e.encode())
	if err != nil || m.E.Code != ErrCodeMethod || m.E.Msg != "nope" {
		t.Fatalf("error round trip failed: %+v, %v", m, err)
	}

	for _, bad := range []string{
		"", "i1e", "garbage",
		"d1:y1:qe",        // no transaction id
		"d1:t2:aa1:y1:qe", // query without method/args
		"d1:t2:aa1:y1:re", // response without r
		"d1:t2:aa1:y1:ze", // unknown type
		"d1:t40:" + strings.Repeat("t", 40) + "1:y1:re", // oversized tid
	} {
		if _, err := parseMessage([]byte(bad)); err == nil {
			t.Errorf("expected error for %q", bad)
		}
	}
}

func TestBucketIndexAndCloser(t *testing.T) {
	self := NodeID{}
	if got := bucketIndex(self, idWithPrefix(0x80)); got != 0 {
		t.Errorf("top bit differs: expected bucket 0, got %d", got)
	}
	if got := bucketIndex(self, idWithPrefix(0, 0x01)); got != 15 {
		t.Errorf("expected bucket 15, got %d", got)
	}
	if got := bucketIndex(self, self); got != -1 {
		t.Errorf("self must map to -1, got %d", got)
	}
	target := idWithPrefix(0xF0)
	if !closer(target, idWithPrefix(0xF1), idWithPrefix(0x70)) {
		t.Errorf("0xF1 should be closer to 0xF0 than 0x70")
	}
	if closer(target, target, target) {
		t.Errorf("equal distances are not strictly closer")
	}
}

func TestTableBucketsAndEviction(t *testing.T) {
	tbl := NewTable(NodeID{})
	now := time.Now()
	tbl.now = func() time.Time { return now }
	addr := func(i int) *net.UDPAddr { return &net.UDPAddr{IP: net.IPv4(10, 0, 0, byte(i)), Port: 1000 + i} }

	// All share the top bit set → same bucket 0. Fill it beyond K.
	for i := 0; i < K+3; i++ {
		tbl.Seen(NodeInfo{ID: idWithPrefix(0x80, byte(i)), Addr: addr(i)})
	}
	if tbl.Len() != K {
		t.Fatalf("bucket must cap at K=%d, got %d", K, tbl.Len())
	}
	tbl.Seen(NodeInfo{ID: NodeID{}, Addr: addr(99)})
	if tbl.Len() != K {
		t.Errorf("our own ID must never be inserted")
	}

	// A failing node becomes replaceable; a newcomer then takes its slot.
	tbl.Failed(addr(0))
	tbl.Seen(NodeInfo{ID: idWithPrefix(0x80, 0xEE), Addr: addr(50)})
	ids := map[NodeID]bool{}
	for _, n := range tbl.All() {
		ids[n.ID] = true
	}
	if ids[idWithPrefix(0x80, 0)] || !ids[idWithPrefix(0x80, 0xEE)] {
		t.Errorf("failing node should have been replaced by the newcomer")
	}

	// Stale nodes are replaceable too.
	now = now.Add(questionableAfter + time.Minute)
	tbl.Seen(NodeInfo{ID: idWithPrefix(0x80, 0xEF), Addr: addr(51)})
	if !containsID(tbl.All(), idWithPrefix(0x80, 0xEF)) {
		t.Errorf("stale node should have been replaced")
	}

	// Repeated failures evict.
	victim := addr(1)
	for i := 0; i < maxFails; i++ {
		tbl.Failed(victim)
	}
	if containsID(tbl.All(), idWithPrefix(0x80, 1)) {
		t.Errorf("node should be evicted after %d failures", maxFails)
	}

	closest := tbl.Closest(idWithPrefix(0x80, 0xEF), 2)
	if len(closest) != 2 || closest[0].ID != idWithPrefix(0x80, 0xEF) {
		t.Errorf("Closest must return the exact match first: %+v", closest)
	}
}

func containsID(nodes []NodeInfo, id NodeID) bool {
	for _, n := range nodes {
		if n.ID == id {
			return true
		}
	}
	return false
}

func TestPeerStoreBoundsAndExpiry(t *testing.T) {
	ps := newPeerStore()
	now := time.Now()
	ih := idWithPrefix(9)
	for i := 0; i < maxPeersPerInfoHash+10; i++ {
		ps.add(ih, tracker.Peer{IP: net.IPv4(10, 0, byte(i>>8), byte(i)), Port: 1}, now)
	}
	if n := len(ps.m[ih]); n != maxPeersPerInfoHash {
		t.Errorf("expected cap %d, got %d", maxPeersPerInfoHash, n)
	}
	if got := ps.get(ih, maxValues, now); len(got) != maxValues {
		t.Errorf("get must cap at %d, got %d", maxValues, len(got))
	}
	ps.expire(now.Add(peerTTL))
	if len(ps.m) != 0 {
		t.Errorf("expired peers must be removed")
	}
}

func TestServerQueries(t *testing.T) {
	a := newTestServer(t)
	b := newTestServer(t)
	ctx := context.Background()

	if err := a.Ping(ctx, b.Addr()); err != nil {
		t.Fatalf("ping: %v", err)
	}
	// Both sides learn about each other: a from the response, b from the query.
	if a.NumNodes() != 1 || b.NumNodes() != 1 {
		t.Fatalf("expected both tables to hold 1 node, got a=%d b=%d", a.NumNodes(), b.NumNodes())
	}

	ih := [20]byte{0xAB}
	r, err := a.query(ctx, b.Addr(), "get_peers", map[string]any{"info_hash": string(ih[:])})
	if err != nil {
		t.Fatalf("get_peers: %v", err)
	}
	tok, _ := r["token"].(string)
	if tok == "" || r["values"] != nil {
		t.Fatalf("expected a token and no values, got %v", r)
	}

	// A forged token is rejected.
	_, err = a.query(ctx, b.Addr(), "announce_peer", map[string]any{"info_hash": string(ih[:]), "port": 1234, "token": "forged"})
	var kerr *KRPCError
	if !errors.As(err, &kerr) || kerr.Code != ErrCodeProtocol {
		t.Fatalf("expected protocol error for bad token, got %v", err)
	}
	if _, err := a.query(ctx, b.Addr(), "announce_peer", map[string]any{"info_hash": string(ih[:]), "port": 1234, "token": tok}); err != nil {
		t.Fatalf("announce_peer: %v", err)
	}
	// implied_port uses the UDP source port.
	if _, err := a.query(ctx, b.Addr(), "announce_peer", map[string]any{"info_hash": string(ih[:]), "implied_port": 1, "token": tok}); err != nil {
		t.Fatalf("announce_peer implied: %v", err)
	}

	peers := b.LocalPeers(ih)
	ports := map[uint16]bool{}
	for _, p := range peers {
		ports[p.Port] = true
	}
	if len(peers) != 2 || !ports[1234] || !ports[uint16(a.Addr().Port)] {
		t.Fatalf("unexpected stored peers: %v", peers)
	}

	r, err = a.query(ctx, b.Addr(), "get_peers", map[string]any{"info_hash": string(ih[:])})
	if err != nil {
		t.Fatal(err)
	}
	if got := decodePeers(r["values"].([]any)); len(got) != 2 {
		t.Fatalf("expected 2 values, got %v", got)
	}

	_, err = a.query(ctx, b.Addr(), "bogus", map[string]any{})
	if !errors.As(err, &kerr) || kerr.Code != ErrCodeMethod {
		t.Fatalf("expected method-unknown error, got %v", err)
	}
}

func TestServerIgnoresSpoofedAndMalformedPackets(t *testing.T) {
	s := newTestServer(t)
	attacker, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer attacker.Close()

	// A silent "victim" we query; the attacker races a forged response for every possible tid.
	victim, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer victim.Close()

	done := make(chan error, 1)
	go func() { done <- s.Ping(context.Background(), victim.LocalAddr().(*net.UDPAddr)) }()

	buf := make([]byte, 1500)
	_ = victim.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _, err := victim.ReadFromUDP(buf)
	if err != nil {
		t.Fatal(err)
	}
	q, err := parseMessage(buf[:n])
	if err != nil {
		t.Fatal(err)
	}
	forged := &message{T: q.T, Y: "r", R: map[string]any{"id": strings.Repeat("z", 20)}}
	_, _ = attacker.WriteToUDP(forged.encode(), s.Addr())
	for _, junk := range []string{"", "garbage", "d1:t2:xx1:y1:re", "d1:ad2:id3:shorte1:q4:ping1:t2:aa1:y1:qe"} {
		_, _ = attacker.WriteToUDP([]byte(junk), s.Addr())
	}

	if err := <-done; !errors.Is(err, ErrTimeout) {
		t.Fatalf("forged response from another address must be ignored; got %v", err)
	}
	if s.NumNodes() != 0 {
		t.Errorf("no node should have been added, got %d", s.NumNodes())
	}

	// Server must still be healthy.
	other := newTestServer(t)
	if err := other.Ping(context.Background(), s.Addr()); err != nil {
		t.Fatalf("server unhealthy after junk: %v", err)
	}
}

func TestNetworkAnnounceAndGetPeers(t *testing.T) {
	const size = 30
	root := newTestServer(t)
	nodes := []*Server{root}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	for i := 1; i < size; i++ {
		n := newTestServer(t, root.Addr().String())
		if err := n.Bootstrap(ctx); err != nil {
			t.Fatalf("bootstrap node %d: %v", i, err)
		}
		nodes = append(nodes, n)
	}
	// Second pass so early nodes learn about later ones.
	for _, n := range nodes[1:] {
		_ = n.Bootstrap(ctx)
	}
	if root.NumNodes() < K {
		t.Fatalf("root should know at least K nodes, knows %d", root.NumNodes())
	}

	ih := [20]byte{0xDE, 0xAD, 0xBE, 0xEF}
	if _, err := nodes[5].Announce(ctx, ih, 4321); err != nil {
		t.Fatalf("announce: %v", err)
	}

	peers, err := nodes[size-1].GetPeers(ctx, ih)
	if err != nil {
		t.Fatalf("get_peers: %v", err)
	}
	found := false
	for _, p := range peers {
		if p.Port == 4321 && p.IP.Equal(net.IPv4(127, 0, 0, 1)) {
			found = true
		}
	}
	if !found {
		t.Fatalf("announced peer not found; got %v", peers)
	}
}

func TestGetPeersWithoutNodes(t *testing.T) {
	s := newTestServer(t) // no bootstrap routers
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := s.GetPeers(ctx, [20]byte{1}); !errors.Is(err, ErrNoNodes) {
		t.Fatalf("expected ErrNoNodes, got %v", err)
	}
}

func TestBencodeOfQueryArgs(t *testing.T) {
	// Sanity check: our query encoding is valid bencode with sorted keys.
	m := &message{T: "ab", Y: "q", Q: "get_peers", A: map[string]any{"id": strings.Repeat("a", 20), "info_hash": strings.Repeat("b", 20)}}
	if _, err := bencode.Decode(m.encode()); err != nil {
		t.Fatalf("invalid bencode: %v", err)
	}
}

func TestNoGoroutineLeaks(t *testing.T) {
	before := runtime.NumGoroutine()

	root, err := New(Config{Addr: "127.0.0.1:0", QueryTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	var nodes []*Server
	for i := 0; i < 5; i++ {
		n, err := New(Config{Addr: "127.0.0.1:0", Bootstrap: []string{root.Addr().String()}, QueryTimeout: 200 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		_ = n.Bootstrap(context.Background())
		nodes = append(nodes, n)
	}
	_, _ = nodes[0].Announce(context.Background(), [20]byte{1}, 1000)

	// A lookup cancelled mid-flight must not strand its query goroutines.
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(5 * time.Millisecond); cancel() }()
	_, _ = nodes[1].GetPeers(ctx, [20]byte{2})

	for _, n := range append(nodes, root) {
		_ = n.Close()
	}

	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("goroutine leak: %d before, %d after Close", before, after)
	}
}
