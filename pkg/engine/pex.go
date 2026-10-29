package engine

import (
	"log"
	"net"
	"strconv"
	"time"

	"github.com/hernan/get-torrent/pkg/peer"
	"github.com/hernan/get-torrent/pkg/tracker"
)

// Peer exchange (BEP 0011): every pexInterval each peer that supports ut_pex is told which
// peers we connected to and dropped since the last message, and peers it tells us about are
// dialled. PEX is never used for private torrents (BEP 0027) or when disabled in the session.

// pexInterval is the minimum time between ut_pex messages to one peer (BEP 0011: one minute),
// copied into each engine when created (Engine.pexEvery).
var pexInterval = time.Minute

// pexAllowed reports whether t may use peer exchange.
func (e *Engine) pexAllowed(t *ManagedTorrent) bool {
	if !e.Settings().PEXEnabled {
		return false
	}
	meta, _, _ := t.components()
	return !meta.Private
}

// pexPeers returns the connected peers we can share: their listen address (known for peers we
// dialled, and for incoming peers that sent "p" in their extension handshake) with PEX flags.
func (t *ManagedTorrent) pexPeers() map[string]pexEntry {
	t.mu.RLock()
	infos := make([]*peerInfo, 0, len(t.peerInfos))
	for _, pi := range t.peerInfos {
		infos = append(infos, pi)
	}
	pieceCount := len(t.Meta.Pieces)
	t.mu.RUnlock()

	out := make(map[string]pexEntry, len(infos))
	for _, pi := range infos {
		pi.mu.Lock()
		port, flags := pi.port, byte(0)
		if pi.incoming {
			port = pi.listenPort
		} else {
			flags |= peer.PexReachable // we reached it ourselves
		}
		if pieceCount > 0 && pi.piecesHave >= pieceCount {
			flags |= peer.PexSeed
		}
		ip := net.ParseIP(pi.ip)
		pi.mu.Unlock()
		if ip == nil || port <= 0 || port > 65535 {
			continue
		}
		p := tracker.Peer{IP: ip, Port: uint16(port)}
		out[p.String()] = pexEntry{peer: p, flags: flags}
	}
	return out
}

type pexEntry struct {
	peer  tracker.Peer
	flags byte
}

// pexState is a session's record of what it last told its peer.
type pexState struct {
	sent map[string]tracker.Peer
	last time.Time
}

// sendPex sends this session's peer a ut_pex update if one is due.
func (s *peerSession) sendPex() {
	extID, ok := s.conn.PeerExtID(peer.ExtUTPex)
	if !ok || s.info == nil || !s.e.pexAllowed(s.t) {
		return
	}
	now := s.now()
	if s.pex.sent != nil && now.Sub(s.pex.last) < s.e.pexEvery {
		return
	}

	current := s.t.pexPeers()
	delete(current, s.self())
	var added, dropped []tracker.Peer
	var flags []byte
	for key, ent := range current {
		if _, known := s.pex.sent[key]; !known && len(added) < peer.MaxPexPeers {
			added = append(added, ent.peer)
			flags = append(flags, ent.flags)
		}
	}
	for key, p := range s.pex.sent {
		if _, still := current[key]; !still && len(dropped) < peer.MaxPexPeers {
			dropped = append(dropped, p)
		}
	}
	first := s.pex.sent == nil
	if first {
		s.pex.sent = make(map[string]tracker.Peer)
	}
	if len(added) == 0 && len(dropped) == 0 && !first {
		return
	}
	s.send(peer.FormatPex(extID, added, flags, dropped))
	s.pex.last = now
	for _, p := range added {
		s.pex.sent[p.String()] = p
	}
	for _, p := range dropped {
		delete(s.pex.sent, p.String())
	}
}

// self is the PEX key of this session's own peer, which must not be sent back to it.
func (s *peerSession) self() string {
	s.info.mu.Lock()
	defer s.info.mu.Unlock()
	port := s.info.port
	if s.info.incoming {
		port = s.info.listenPort
	}
	return net.JoinHostPort(s.info.ip, strconv.Itoa(port))
}

// handlePex dials the peers a ut_pex message announces.
func (s *peerSession) handlePex(payload []byte) {
	if !s.e.pexAllowed(s.t) {
		return
	}
	msg, err := peer.ParsePex(payload)
	if err != nil {
		log.Printf("[Engine] Bad ut_pex from %s: %v", s.addr, err)
		return
	}
	if len(msg.Added) > 0 {
		s.e.connectPeers(s.t, msg.Added, SourcePEX)
	}
}
