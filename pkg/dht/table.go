package dht

import (
	"math/bits"
	"net"
	"sort"
	"sync"
	"time"
)

const (
	// K is the bucket size and the number of closest nodes a lookup converges on.
	K = 8

	// maxFails is how many consecutive query timeouts remove a node from the table.
	maxFails = 3

	// questionableAfter marks nodes not heard from in this long as replaceable (BEP 0005: 15 min).
	questionableAfter = 15 * time.Minute
)

type entry struct {
	info     NodeInfo
	lastSeen time.Time
	fails    int
}

func (e *entry) replaceable(now time.Time) bool {
	return e.fails > 0 || now.Sub(e.lastSeen) > questionableAfter
}

// Table is a Kademlia routing table with one K-sized bucket per shared-prefix length with our ID.
// Only nodes that have contacted us or answered our queries are inserted. Safe for concurrent use.
type Table struct {
	self    NodeID
	mu      sync.Mutex
	buckets [160][]*entry
	now     func() time.Time
}

// NewTable creates an empty routing table for the given local node ID.
func NewTable(self NodeID) *Table {
	return &Table{self: self, now: time.Now}
}

// bucketIndex returns the length of the common bit prefix of a and b, or -1 if they are equal.
func bucketIndex(a, b NodeID) int {
	for i := range a {
		if x := a[i] ^ b[i]; x != 0 {
			return i*8 + bits.LeadingZeros8(x)
		}
	}
	return -1
}

func sameAddr(a, b *net.UDPAddr) bool {
	return a.Port == b.Port && a.IP.Equal(b.IP)
}

// Seen records that a node is alive. New nodes go into their bucket if it has room or holds a
// replaceable (failing or stale) node; otherwise they are dropped, favouring long-lived nodes.
func (t *Table) Seen(n NodeInfo) {
	idx := bucketIndex(t.self, n.ID)
	if idx < 0 || n.Addr == nil || n.Addr.Port == 0 {
		return
	}
	now := t.now()
	t.mu.Lock()
	defer t.mu.Unlock()

	b := t.buckets[idx]
	for _, e := range b {
		if e.info.ID == n.ID {
			e.info.Addr = n.Addr
			e.lastSeen = now
			e.fails = 0
			return
		}
	}
	if len(b) < K {
		t.buckets[idx] = append(b, &entry{info: n, lastSeen: now})
		return
	}
	// Replace the worst replaceable node, if any.
	worst := -1
	for i, e := range b {
		if e.replaceable(now) && (worst < 0 || e.fails > b[worst].fails ||
			(e.fails == b[worst].fails && e.lastSeen.Before(b[worst].lastSeen))) {
			worst = i
		}
	}
	if worst >= 0 {
		b[worst] = &entry{info: n, lastSeen: now}
	}
}

// Failed records a query timeout for the node at addr, evicting it after maxFails timeouts.
func (t *Table) Failed(addr *net.UDPAddr) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for bi, b := range t.buckets {
		for i, e := range b {
			if sameAddr(e.info.Addr, addr) {
				e.fails++
				if e.fails >= maxFails {
					t.buckets[bi] = append(b[:i], b[i+1:]...)
				}
				return
			}
		}
	}
}

// Closest returns up to n known nodes closest to target, nearest first.
func (t *Table) Closest(target NodeID, n int) []NodeInfo {
	all := t.All()
	sort.Slice(all, func(i, j int) bool { return closer(target, all[i].ID, all[j].ID) })
	if len(all) > n {
		all = all[:n]
	}
	return all
}

// All returns every node in the table.
func (t *Table) All() []NodeInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []NodeInfo
	for _, b := range t.buckets {
		for _, e := range b {
			out = append(out, e.info)
		}
	}
	return out
}

// Len returns the number of nodes in the table.
func (t *Table) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, b := range t.buckets {
		n += len(b)
	}
	return n
}
