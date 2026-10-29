package engine

import (
	"context"
	"sync"

	"github.com/hernan/get-torrent/pkg/peer"
)

// maxUploadQueue caps the block requests queued from one peer; we advertise it as "reqq"
// (BEP 0010) and drop requests beyond it.
const maxUploadQueue = peer.MaxRequestQueue

// blockRequest is one block a peer asked us for.
type blockRequest struct {
	index, begin, length int
}

// uploadQueue holds a peer's pending requests. The session loop adds and cancels requests while
// the uploader goroutine serves them, so a "cancel" or a choke takes effect even while the
// uploader is waiting for upload bandwidth (BEP 0003).
type uploadQueue struct {
	mu      sync.Mutex
	reqs    []blockRequest
	current *blockRequest // being served; nil when idle
	dropped bool          // current was cancelled or the peer was choked meanwhile
	wake    chan struct{}
}

func newUploadQueue() *uploadQueue {
	return &uploadQueue{wake: make(chan struct{}, 1)}
}

// push queues r, ignoring duplicates and requests beyond maxUploadQueue.
func (q *uploadQueue) push(r blockRequest) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.reqs) >= maxUploadQueue || (q.current != nil && *q.current == r && !q.dropped) {
		return
	}
	for _, x := range q.reqs {
		if x == r {
			return
		}
	}
	q.reqs = append(q.reqs, r)
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// cancel withdraws r, whether queued or being served.
func (q *uploadQueue) cancel(r blockRequest) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, x := range q.reqs {
		if x == r {
			q.reqs = append(q.reqs[:i], q.reqs[i+1:]...)
			return
		}
	}
	if q.current != nil && *q.current == r {
		q.dropped = true
	}
}

// clear drops every pending request: a choked peer discards them (BEP 0003).
func (q *uploadQueue) clear() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.reqs = nil
	if q.current != nil {
		q.dropped = true
	}
}

// next blocks until a request is available or ctx is done, marking it as being served.
func (q *uploadQueue) next(ctx context.Context) (blockRequest, bool) {
	for {
		q.mu.Lock()
		if len(q.reqs) > 0 {
			r := q.reqs[0]
			q.reqs = q.reqs[1:]
			q.current, q.dropped = &r, false
			q.mu.Unlock()
			return r, true
		}
		q.mu.Unlock()
		select {
		case <-q.wake:
		case <-ctx.Done():
			return blockRequest{}, false
		}
	}
}

// finish ends serving the current request, reporting whether it is still wanted.
func (q *uploadQueue) finish() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	wanted := q.current != nil && !q.dropped
	q.current, q.dropped = nil, false
	return wanted
}

func (q *uploadQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.reqs)
}

// runUploader serves queued requests until ctx is done. It touches only the queue, the
// thread-safe Conn.SendMessage and engine accounting, never the session's loop state.
func (s *peerSession) runUploader(ctx context.Context) {
	for {
		r, ok := s.uploads.next(ctx)
		if !ok {
			return
		}
		block := s.readBlock(r)
		if block == nil {
			s.uploads.finish()
			continue
		}
		if s.e.throttle(ctx, s.t, true, len(block)) != nil {
			return // session ending
		}
		if !s.uploads.finish() {
			continue // cancelled or choked while waiting for bandwidth
		}
		if s.conn.SendMessage(peer.FormatPiece(r.index, r.begin, block)) != nil {
			return
		}
		s.e.recordUpload(s.t, s.info, len(block))
	}
}

// readBlock returns the requested block from disk, or nil if it cannot be served.
func (s *peerSession) readBlock(r blockRequest) []byte {
	meta, pm, store := s.t.components()
	if pm == nil || store == nil || r.index < 0 || r.index >= len(meta.Pieces) || !pm.HasPiece(r.index) {
		return nil
	}
	pieceLen := int(meta.PieceLength)
	if r.index == len(meta.Pieces)-1 && meta.TotalLength%meta.PieceLength != 0 {
		pieceLen = int(meta.TotalLength % meta.PieceLength)
	}
	if r.begin+r.length > pieceLen {
		return nil
	}
	pieceData, err := store.ReadPiece(r.index, pieceLen)
	if err != nil || r.begin+r.length > len(pieceData) {
		return nil
	}
	return pieceData[r.begin : r.begin+r.length]
}
