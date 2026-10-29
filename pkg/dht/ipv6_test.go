package dht

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/hernan/get-torrent/pkg/tracker"
)

func TestIPv6CompactFormats(t *testing.T) {
	in := []NodeInfo{
		{ID: idWithPrefix(1), Addr: &net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 6881}},
		{ID: idWithPrefix(2), Addr: &net.UDPAddr{IP: net.IPv4(1, 2, 3, 4), Port: 6881}}, // other family
	}
	enc := ipv6.encodeNodes(in)
	if len(enc) != 38 {
		t.Fatalf("expected one 38-byte IPv6 node, got %d bytes", len(enc))
	}
	out := ipv6.decodeNodes(enc + "partial")
	if len(out) != 1 || out[0].ID != in[0].ID || !out[0].Addr.IP.Equal(in[0].Addr.IP) || out[0].Addr.Port != 6881 {
		t.Errorf("round trip: %+v", out)
	}

	p := tracker.Peer{IP: net.ParseIP("2001:db8::2"), Port: 51413}
	c := ipv6.encodePeer(p)
	if len(c) != 18 || ipv6.encodePeer(tracker.Peer{IP: net.IPv4(1, 2, 3, 4), Port: 1}) != "" {
		t.Fatalf("unexpected IPv6 peer encoding: %d bytes", len(c))
	}
	if got := ipv6.decodePeers([]any{c, "short"}); len(got) != 1 || got[0].String() != "[2001:db8::2]:51413" {
		t.Errorf("decoded peers: %v", got)
	}
	if encodePeer(p) != "" {
		t.Errorf("the IPv4 DHT must not carry IPv6 peers")
	}
}

func newTestServer6(t *testing.T, bootstrap ...string) *Server {
	t.Helper()
	s, err := New(Config{Addr: "[::1]:0", Bootstrap: bootstrap, QueryTimeout: 500 * time.Millisecond, IPv6: true})
	if err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestIPv6NetworkAnnounceAndGetPeers(t *testing.T) {
	const size = 12
	root := newTestServer6(t)
	nodes := []*Server{root}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for i := 1; i < size; i++ {
		n := newTestServer6(t, root.Addr().String())
		if err := n.Bootstrap(ctx); err != nil {
			t.Fatalf("bootstrap node %d: %v", i, err)
		}
		nodes = append(nodes, n)
	}
	for _, n := range nodes[1:] {
		_ = n.Bootstrap(ctx)
	}

	ih := [20]byte{0xBE, 0x32}
	if _, err := nodes[3].Announce(ctx, ih, 4321); err != nil {
		t.Fatalf("announce: %v", err)
	}
	peers, err := nodes[size-1].GetPeers(ctx, ih)
	if err != nil {
		t.Fatalf("get_peers: %v", err)
	}
	found := false
	for _, p := range peers {
		if p.Port == 4321 && p.IP.Equal(net.IPv6loopback) {
			found = true
		}
	}
	if !found {
		t.Fatalf("announced IPv6 peer not found; got %v", peers)
	}

	// IPv4 nodes are not part of the IPv6 DHT.
	before := root.NumNodes()
	root.AddNode(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1})
	time.Sleep(50 * time.Millisecond)
	if root.NumNodes() != before {
		t.Errorf("an IPv4 node must not join the IPv6 table")
	}
}
