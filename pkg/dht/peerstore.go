package dht

import (
	"sync"
	"time"

	"github.com/hernan/get-torrent/pkg/tracker"
)

const (
	// peerTTL is how long an announced peer is kept without being re-announced.
	peerTTL = 30 * time.Minute

	// Bounds on the announce store, which remote nodes can fill at will.
	maxStoredInfoHashes = 4096
	maxPeersPerInfoHash = 256

	// maxValues caps peers returned in one get_peers response, keeping it under a typical MTU.
	maxValues = 50
)

// peerStore holds peers announced to us via announce_peer.
type peerStore struct {
	mu sync.Mutex
	m  map[NodeID]map[string]storedPeer
}

type storedPeer struct {
	peer    tracker.Peer
	expires time.Time
}

func newPeerStore() *peerStore {
	return &peerStore{m: make(map[NodeID]map[string]storedPeer)}
}

func (ps *peerStore) add(ih NodeID, p tracker.Peer, now time.Time) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	peers := ps.m[ih]
	if peers == nil {
		if len(ps.m) >= maxStoredInfoHashes {
			return
		}
		peers = make(map[string]storedPeer)
		ps.m[ih] = peers
	}
	key := p.String()
	if _, exists := peers[key]; !exists && len(peers) >= maxPeersPerInfoHash {
		return
	}
	peers[key] = storedPeer{peer: p, expires: now.Add(peerTTL)}
}

func (ps *peerStore) get(ih NodeID, max int, now time.Time) []tracker.Peer {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	var out []tracker.Peer
	for _, sp := range ps.m[ih] {
		if len(out) >= max {
			break
		}
		if now.Before(sp.expires) {
			out = append(out, sp.peer)
		}
	}
	return out
}

func (ps *peerStore) expire(now time.Time) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	for ih, peers := range ps.m {
		for key, sp := range peers {
			if !now.Before(sp.expires) {
				delete(peers, key)
			}
		}
		if len(peers) == 0 {
			delete(ps.m, ih)
		}
	}
}
