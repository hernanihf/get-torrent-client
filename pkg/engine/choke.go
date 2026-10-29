package engine

import (
	"math/rand"
	"sort"
	"time"

	"github.com/hernan/get-torrent/pkg/peer"
)

// Choking algorithm (BEP 0003 "Choking and optimistic unchoking"). Every chokeInterval the
// uploadSlots interested peers with the best rate are unchoked: the rate they send to us while
// downloading (tit-for-tat), or the rate we send to them while seeding (so slots go to peers
// that can take the data). One more interested peer is unchoked optimistically and rotated every
// optimisticRounds rounds, giving new peers a chance to prove themselves; recently connected
// peers are newPeerWeight times as likely to be picked, as in the reference client.
const (
	chokeInterval    = 10 * time.Second
	optimisticRounds = 3 // optimistic unchoke rotates every 30 s
	uploadSlots      = 4 // regular unchoke slots per torrent
	newPeerWindow    = time.Minute
	newPeerWeight    = 3
)

// chokeCandidate is one connection as seen by the choker.
type chokeCandidate struct {
	conn       *peer.Conn
	interested bool  // the peer wants data from us
	rate       int64 // B/s that ranks the peer for a regular slot
	isNew      bool  // connected within newPeerWindow
}

// selectUnchoked decides which connections to unchoke. opt is the current optimistic unchoke;
// it is kept unless rotate is set or it no longer qualifies. The returned optimistic peer is nil
// when every interested peer fits in the regular slots.
func selectUnchoked(cands []chokeCandidate, opt *peer.Conn, rotate bool, rng *rand.Rand) (map[*peer.Conn]bool, *peer.Conn) {
	interested := make([]chokeCandidate, 0, len(cands))
	for _, c := range cands {
		if c.interested {
			interested = append(interested, c)
		}
	}
	// Shuffle first so equal rates (e.g. all zero when a torrent starts) do not always favour
	// the same peers.
	rng.Shuffle(len(interested), func(i, j int) { interested[i], interested[j] = interested[j], interested[i] })
	sort.SliceStable(interested, func(i, j int) bool { return interested[i].rate > interested[j].rate })

	unchoke := make(map[*peer.Conn]bool, uploadSlots+1)
	n := min(uploadSlots, len(interested))
	for _, c := range interested[:n] {
		unchoke[c.conn] = true
	}
	rest := interested[n:]
	if len(rest) == 0 {
		return unchoke, nil
	}

	if !rotate && opt != nil {
		for _, c := range rest {
			if c.conn == opt {
				unchoke[opt] = true
				return unchoke, opt
			}
		}
	}

	total := 0
	for _, c := range rest {
		total += candidateWeight(c)
	}
	pick := rng.Intn(total)
	for _, c := range rest {
		if pick -= candidateWeight(c); pick < 0 {
			unchoke[c.conn] = true
			return unchoke, c.conn
		}
	}
	return unchoke, nil // unreachable: pick < total
}

func candidateWeight(c chokeCandidate) int {
	if c.isNew {
		return newPeerWeight
	}
	return 1
}

// requestRechoke asks the choker to re-run soon, e.g. when a peer's interest changes or a slot
// frees up, so an interested peer does not wait for the next round to be served.
func (t *ManagedTorrent) requestRechoke() {
	select {
	case t.rechoke <- struct{}{}:
	default: // a rechoke is already pending
	}
}

// runChoker runs the choking algorithm for a torrent until it is stopped.
func (e *Engine) runChoker(t *ManagedTorrent, stop <-chan struct{}) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	ticker := time.NewTicker(chokeInterval)
	defer ticker.Stop()

	var opt *peer.Conn
	round := 0
	for {
		rotate := false
		select {
		case <-stop:
			return
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			round++
			rotate = round%optimisticRounds == 0
		case <-t.rechoke:
		}
		opt = e.rechoke(t, opt, rotate, rng)
	}
}

// rechoke runs one round of the choking algorithm and hands each decision to the peer's session,
// which owns the connection and sends the choke/unchoke message.
func (e *Engine) rechoke(t *ManagedTorrent, opt *peer.Conn, rotate bool, rng *rand.Rand) *peer.Conn {
	now := time.Now()
	t.mu.RLock()
	seeding := t.State == StateSeeding
	infos := make(map[*peer.Conn]*peerInfo, len(t.peerInfos))
	for conn, pi := range t.peerInfos {
		infos[conn] = pi
	}
	t.mu.RUnlock()

	cands := make([]chokeCandidate, 0, len(infos))
	for conn, pi := range infos {
		pi.mu.Lock()
		c := chokeCandidate{
			conn:       conn,
			interested: pi.peerIsInterested,
			isNew:      now.Sub(pi.connected) < newPeerWindow,
		}
		pi.mu.Unlock()
		if seeding {
			c.rate = pi.up.rate(now)
		} else {
			c.rate = pi.down.rate(now)
		}
		cands = append(cands, c)
	}

	unchoke, opt := selectUnchoked(cands, opt, rotate, rng)
	for conn, pi := range infos {
		pi.setChoke(!unchoke[conn])
	}
	return opt
}

// setChoke queues the choker's latest decision for the peer's session, replacing any decision
// the session has not applied yet. Only the choker goroutine calls it.
func (pi *peerInfo) setChoke(choke bool) {
	select {
	case <-pi.chokeReq:
	default:
	}
	pi.chokeReq <- choke
}

// applyChoke chokes or unchokes the peer as the choker decided. A peer we choke discards the
// requests it has pending with us (BEP 0003), so the upload queue is dropped, and handleRequest
// ignores any that still arrive.
func (s *peerSession) applyChoke(choke bool) {
	if choke == s.conn.AmChoking {
		return
	}
	id := peer.MsgUnchoke
	if choke {
		id = peer.MsgChoke
	}
	s.send(&peer.Message{ID: idPtr(id)})
	s.conn.AmChoking = choke
	if choke && s.uploads != nil {
		// After the choke, so a Fast peer does not re-request while it thinks it is unchoked.
		for _, r := range s.uploads.clear() {
			s.reject(r)
		}
	}
	s.publish()
}
