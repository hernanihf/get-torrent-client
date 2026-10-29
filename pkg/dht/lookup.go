package dht

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/hernan/get-torrent/pkg/tracker"
)

const (
	// alpha is the number of concurrent queries in an iterative lookup.
	alpha = 3

	// softTimeout frees a query's concurrency slot when it is slow to answer, so dead nodes
	// (common on the real network) do not stall the lookup; its late reply is still used.
	softTimeout = 800 * time.Millisecond

	// maxSlowQueries bounds extra in-flight queries opened by soft timeouts.
	maxSlowQueries = 3 * alpha

	// maxLookupQueries bounds the total work of one lookup.
	maxLookupQueries = 256

	// Bounds on what a lookup accepts from (possibly malicious) responders.
	maxNodesPerResponse = 16
	maxLookupCandidates = 512
	maxLookupPeers      = 500
)

type candidate struct {
	info      NodeInfo
	queried   bool
	responded bool
	failed    bool
	token     string
}

// lookupResult holds the outcome of an iterative lookup.
type lookupResult struct {
	peers      []tracker.Peer
	responders []*candidate // nodes that answered, nearest first, at most K
}

// lookup performs an iterative Kademlia lookup for target using method ("find_node" or
// "get_peers"), starting from the routing table plus extra. It converges once the K closest
// live candidates have all been queried.
func (s *Server) lookup(ctx context.Context, target NodeID, method string, extra []NodeInfo) *lookupResult {
	cands := make(map[NodeID]*candidate)
	add := func(n NodeInfo) {
		if n.ID == s.id || len(cands) >= maxLookupCandidates {
			return
		}
		if _, ok := cands[n.ID]; !ok {
			cands[n.ID] = &candidate{info: n}
		}
	}
	for _, n := range s.table.Closest(target, K) {
		add(n)
	}
	for _, n := range extra {
		add(n)
	}

	sorted := func() []*candidate {
		list := make([]*candidate, 0, len(cands))
		for _, c := range cands {
			list = append(list, c)
		}
		sort.Slice(list, func(i, j int) bool { return closer(target, list[i].info.ID, list[j].info.ID) })
		return list
	}

	started := make(map[*candidate]time.Time) // in-flight queries and when they were sent

	// slow reports whether c's query is in flight past the soft timeout; such nodes are treated as
	// dead for scheduling and convergence, though a late reply is still used.
	slow := func(c *candidate, now time.Time) bool {
		t, ok := started[c]
		return ok && now.Sub(t) >= softTimeout
	}

	// next picks an unqueried candidate among the K closest that are not failed or slow.
	next := func() *candidate {
		live := 0
		now := time.Now()
		for _, c := range sorted() {
			if c.failed || slow(c, now) {
				continue
			}
			if live++; live > K {
				return nil
			}
			if !c.queried {
				return c
			}
		}
		return nil
	}

	type reply struct {
		c   *candidate
		r   map[string]any
		err error
	}
	replies := make(chan reply)
	var wg sync.WaitGroup
	defer wg.Wait()
	// Cancelled on return (before wg.Wait runs) so straggling queries are abandoned promptly.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	args := func() map[string]any {
		if method == "get_peers" {
			return map[string]any{"info_hash": string(target[:])}
		}
		return map[string]any{"target": string(target[:])}
	}

	res := &lookupResult{}
	seenPeers := make(map[string]bool)
	queries := 0
	ticker := time.NewTicker(softTimeout / 4)
	defer ticker.Stop()

	// slots allows alpha queries, plus one for each in-flight query past its soft timeout.
	slots := func() int {
		n := 0
		now := time.Now()
		for c := range started {
			if slow(c, now) {
				n++
			}
		}
		return alpha + min(n, maxSlowQueries)
	}

	// converged reports whether the K closest live candidates have all answered.
	converged := func() bool {
		live := 0
		now := time.Now()
		for _, c := range sorted() {
			if c.failed || slow(c, now) {
				continue
			}
			if !c.responded {
				return false
			}
			if live++; live == K {
				return true
			}
		}
		return len(started) == 0 // fewer than K live nodes: done once nothing is pending
	}

	for {
		for len(started) < slots() && queries < maxLookupQueries {
			c := next()
			if c == nil {
				break
			}
			c.queried = true
			started[c] = time.Now()
			queries++
			wg.Add(1)
			go func(c *candidate) {
				defer wg.Done()
				r, err := s.query(ctx, c.info.Addr, method, args())
				select {
				case replies <- reply{c, r, err}:
				case <-ctx.Done():
				}
			}(c)
		}
		if converged() {
			break
		}

		var rep reply
		select {
		case rep = <-replies:
		case <-ticker.C:
			continue // re-evaluate slots as queries age past the soft timeout
		case <-ctx.Done():
			return res.finish(sorted())
		}
		delete(started, rep.c)

		if rep.err != nil {
			rep.c.failed = true
			continue
		}
		rep.c.responded = true
		// The node may report a different ID than the one we were given.
		if id, ok := getID(rep.r, "id"); ok && id != rep.c.info.ID {
			delete(cands, rep.c.info.ID)
			rep.c.info.ID = id
			cands[id] = rep.c
		}
		rep.c.token, _ = rep.r["token"].(string)

		if values, ok := rep.r["values"].([]any); ok {
			for _, p := range s.fam.decodePeers(values) {
				if key := p.String(); !seenPeers[key] && len(res.peers) < maxLookupPeers {
					seenPeers[key] = true
					res.peers = append(res.peers, p)
				}
			}
		}
		if nodes, ok := rep.r[s.fam.nodesKey].(string); ok {
			decoded := s.fam.decodeNodes(nodes)
			if len(decoded) > maxNodesPerResponse {
				decoded = decoded[:maxNodesPerResponse]
			}
			for _, n := range decoded {
				add(n)
			}
		}
	}
	return res.finish(sorted())
}

func (res *lookupResult) finish(sorted []*candidate) *lookupResult {
	for _, c := range sorted {
		if c.responded && len(res.responders) < K {
			res.responders = append(res.responders, c)
		}
	}
	return res
}

// GetPeers finds peers for infoHash without announcing ourselves.
func (s *Server) GetPeers(ctx context.Context, infoHash [20]byte) ([]tracker.Peer, error) {
	return s.Announce(ctx, infoHash, 0)
}

// Announce finds peers for infoHash and, when port is non-zero, announces that we accept peer
// connections on that TCP port to the closest nodes that handed us a token.
func (s *Server) Announce(ctx context.Context, infoHash [20]byte, port uint16) ([]tracker.Peer, error) {
	if s.table.Len() == 0 {
		if err := s.Bootstrap(ctx); err != nil {
			return nil, err
		}
	}
	res := s.lookup(ctx, NodeID(infoHash), "get_peers", nil)
	if len(res.responders) == 0 {
		return nil, ErrNoNodes
	}

	if port != 0 {
		var wg sync.WaitGroup
		for _, c := range res.responders {
			if c.token == "" {
				continue
			}
			wg.Add(1)
			go func(c *candidate) {
				defer wg.Done()
				_, _ = s.query(ctx, c.info.Addr, "announce_peer", map[string]any{
					"info_hash":    string(infoHash[:]),
					"port":         int(port),
					"token":        c.token,
					"implied_port": 0,
				})
			}(c)
		}
		wg.Wait()
	}
	return res.peers, nil
}
