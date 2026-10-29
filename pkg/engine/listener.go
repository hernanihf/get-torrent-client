package engine

import (
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/hernan/get-torrent/pkg/peer"
)

const (
	// MaxPeersPerTorrent defines the maximum concurrent peer connections per torrent.
	MaxPeersPerTorrent = 50

	// HandshakeTimeout defines the maximum duration allowed to complete the handshake.
	HandshakeTimeout = 5 * time.Second
)

// StartListener starts listening for incoming BitTorrent peer TCP connections.
func (e *Engine) StartListener() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.listener != nil {
		return nil // Already listening
	}

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", e.Port))
	if err != nil {
		return fmt.Errorf("starting peer listener on port %d: %w", e.Port, err)
	}

	e.listener = l
	if tcpAddr, ok := l.Addr().(*net.TCPAddr); ok {
		e.Port = uint16(tcpAddr.Port)
	}
	e.listenerStopChan = make(chan struct{})

	log.Printf("[Engine] Peer listener active on port %d", e.Port)
	go e.listenLoop()
	return nil
}

// StopListener closes the active peer listener and terminates the accept loop.
func (e *Engine) StopListener() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.listener == nil {
		return nil
	}

	select {
	case <-e.listenerStopChan:
	default:
		close(e.listenerStopChan)
	}

	err := e.listener.Close()
	e.listener = nil
	log.Printf("[Engine] Peer listener stopped")
	return err
}

func (e *Engine) listenLoop() {
	e.mu.RLock()
	l := e.listener
	stopChan := e.listenerStopChan
	e.mu.RUnlock()

	if l == nil {
		return
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-stopChan:
				return
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			return
		}

		go e.handleInboundConn(conn)
	}
}

func (e *Engine) handleInboundConn(netConn net.Conn) {
	validate := func(infoHash [20]byte) bool {
		e.mu.RLock()
		t, exists := e.torrents[infoHash]
		e.mu.RUnlock()
		if !exists {
			return false
		}
		t.mu.RLock()
		defer t.mu.RUnlock()
		return t.State == StateDownloading || t.State == StateSeeding
	}

	conn, err := peer.AcceptHandshake(netConn, e.PeerID, HandshakeTimeout, validate)
	if err != nil {
		_ = netConn.Close()
		return
	}
	defer conn.Close()

	e.mu.RLock()
	t, exists := e.torrents[conn.InfoHash]
	e.mu.RUnlock()
	if !exists {
		return
	}

	// Enforces running state, peer-ID de-duplication and the per-torrent peer limit.
	if !e.registerConn(t, conn, SourceIncoming) {
		return
	}
	defer e.unregisterConn(t, conn)

	t.mu.RLock()
	isDownloading := t.State == StateDownloading
	t.mu.RUnlock()

	log.Printf("[Engine] Inbound peer connected: %s (PeerID: %x)", netConn.RemoteAddr().String(), conn.PeerID[:8])

	// Send our bitfield if we have any pieces (must be the first message after the handshake)
	if _, pm, _ := t.components(); pm != nil && pm.Bitfield.HasAnyPiece() {
		_ = conn.SendBitfield(pm.Bitfield)
	}

	// Express interest if downloading
	if isDownloading {
		_ = conn.SendInterested()
	}

	e.handlePeerMessages(t, conn)
}
