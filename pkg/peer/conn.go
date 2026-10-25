package peer

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/hernan/get-torrent/pkg/tracker"
)

var (
	ErrInfoHashMismatch = errors.New("peer returned mismatched info_hash")
	ErrSelfConnection   = errors.New("peer connected to itself")
	ErrTorrentNotFound  = errors.New("torrent info_hash not recognized or not active")
)

// Conn manages the peer wire session over a TCP connection.
type Conn struct {
	Conn        net.Conn
	Choked      bool
	Interested  bool
	AmChoking   bool
	AmInterested bool
	Bitfield    Bitfield
	PeerID      [20]byte
	InfoHash    [20]byte
	RemotePeer  tracker.Peer
}

// Dial connects to a remote peer, executes the BitTorrent handshake, and receives the peer's bitfield.
func Dial(p tracker.Peer, infoHash, peerID [20]byte, timeout time.Duration) (*Conn, error) {
	addr := p.String()
	netConn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, fmt.Errorf("dialing peer %s: %w", addr, err)
	}

	// Set deadline for initial handshake
	_ = netConn.SetDeadline(time.Now().Add(timeout))

	// Send our handshake
	hs := NewHandshake(infoHash, peerID)
	if _, err := netConn.Write(hs.Serialize()); err != nil {
		netConn.Close()
		return nil, fmt.Errorf("sending handshake: %w", err)
	}

	// Read peer's handshake
	remoteHS, err := ReadHandshake(netConn)
	if err != nil {
		netConn.Close()
		return nil, fmt.Errorf("reading remote handshake: %w", err)
	}

	if !bytes.Equal(remoteHS.InfoHash[:], infoHash[:]) {
		netConn.Close()
		return nil, fmt.Errorf("%w: expected %x, got %x", ErrInfoHashMismatch, infoHash, remoteHS.InfoHash)
	}

	// Reset deadline for normal message exchange
	_ = netConn.SetDeadline(time.Time{})

	c := &Conn{
		Conn:         netConn,
		Choked:       true, // Peer starts out choking us
		Interested:   false,
		AmChoking:    true, // We start out choking the peer
		AmInterested: false,
		PeerID:       remoteHS.PeerID,
		InfoHash:     infoHash,
		RemotePeer:   p,
	}

	// Read initial bitfield if peer sends one immediately
	if err := c.initBitfield(); err != nil {
		netConn.Close()
		return nil, fmt.Errorf("initializing bitfield: %w", err)
	}

	return c, nil
}

// AcceptHandshake receives an incoming connection, parses the handshake, validates it,
// sends a reciprocal handshake, and returns an initialized *Conn.
func AcceptHandshake(netConn net.Conn, ourPeerID [20]byte, timeout time.Duration, validate func(infoHash [20]byte) bool) (*Conn, error) {
	if timeout > 0 {
		_ = netConn.SetDeadline(time.Now().Add(timeout))
	}

	remoteHS, err := ReadHandshake(netConn)
	if err != nil {
		return nil, fmt.Errorf("reading handshake: %w", err)
	}

	if remoteHS.PeerID == ourPeerID {
		return nil, ErrSelfConnection
	}

	if validate != nil && !validate(remoteHS.InfoHash) {
		return nil, ErrTorrentNotFound
	}

	// Send reciprocal handshake
	hs := NewHandshake(remoteHS.InfoHash, ourPeerID)
	if _, err := netConn.Write(hs.Serialize()); err != nil {
		return nil, fmt.Errorf("sending handshake: %w", err)
	}

	// Reset deadline for normal message exchange
	_ = netConn.SetDeadline(time.Time{})

	var remotePeer tracker.Peer
	if tcpAddr, ok := netConn.RemoteAddr().(*net.TCPAddr); ok {
		remotePeer = tracker.Peer{
			IP:   tcpAddr.IP,
			Port: uint16(tcpAddr.Port),
		}
	} else if addr := netConn.RemoteAddr(); addr != nil {
		host, portStr, err := net.SplitHostPort(addr.String())
		if err == nil {
			var p uint64
			fmt.Sscanf(portStr, "%d", &p)
			remotePeer = tracker.Peer{
				IP:   net.ParseIP(host),
				Port: uint16(p),
			}
		}
	}

	return &Conn{
		Conn:         netConn,
		Choked:       true,
		Interested:   false,
		AmChoking:    true,
		AmInterested: false,
		PeerID:       remoteHS.PeerID,
		InfoHash:     remoteHS.InfoHash,
		RemotePeer:   remotePeer,
	}, nil
}


func (c *Conn) initBitfield() error {
	_ = c.Conn.SetDeadline(time.Now().Add(5 * time.Second))
	defer func() { _ = c.Conn.SetDeadline(time.Time{}) }()

	msg, err := ReadMessage(c.Conn)
	if err != nil {
		return err
	}

	if msg.ID == nil {
		// Keep-alive received
		return nil
	}

	if *msg.ID == MsgBitfield {
		c.Bitfield = Bitfield(msg.Payload)
	} else if *msg.ID == MsgHave {
		idx, err := ParseHave(msg)
		if err == nil {
			c.Bitfield.SetPiece(idx)
		}
	} else if *msg.ID == MsgUnchoke {
		c.Choked = false
	}

	return nil
}

// SendMessage serializes and writes a message to the peer.
func (c *Conn) SendMessage(msg *Message) error {
	_, err := c.Conn.Write(msg.Serialize())
	return err
}

// SendInterested sends a MsgInterested (ID 2) to the peer.
func (c *Conn) SendInterested() error {
	id := MsgInterested
	return c.SendMessage(&Message{ID: &id})
}

// SendNotInterested sends a MsgNotInterested (ID 3) to the peer.
func (c *Conn) SendNotInterested() error {
	id := MsgNotInterested
	return c.SendMessage(&Message{ID: &id})
}

// SendUnchoke unchokes the remote peer.
func (c *Conn) SendUnchoke() error {
	c.AmChoking = false
	id := MsgUnchoke
	return c.SendMessage(&Message{ID: &id})
}

// SendChoke chokes the remote peer.
func (c *Conn) SendChoke() error {
	c.AmChoking = true
	id := MsgChoke
	return c.SendMessage(&Message{ID: &id})
}

// SendBitfield transmits our bitfield to the remote peer.
func (c *Conn) SendBitfield(bf Bitfield) error {
	return c.SendMessage(FormatBitfield(bf))
}

// SendRequest asks the peer for a block of data (typically 16 KB).
func (c *Conn) SendRequest(index, begin, length int) error {
	return c.SendMessage(FormatRequest(index, begin, length))
}

// SendPiece transmits a block of data to the peer in response to a request.
func (c *Conn) SendPiece(index, begin int, data []byte) error {
	return c.SendMessage(FormatPiece(index, begin, data))
}

// SendCancel transmits a cancel message for a pending block request.
func (c *Conn) SendCancel(index, begin, length int) error {
	return c.SendMessage(FormatCancel(index, begin, length))
}

// ReadMsg reads the next incoming message and updates connection state.
func (c *Conn) ReadMsg() (*Message, error) {
	msg, err := ReadMessage(c.Conn)
	if err != nil {
		return nil, err
	}

	if msg.ID != nil {
		switch *msg.ID {
		case MsgChoke:
			c.Choked = true
		case MsgUnchoke:
			c.Choked = false
		case MsgHave:
			idx, err := ParseHave(msg)
			if err == nil {
				c.Bitfield.SetPiece(idx)
			}
		case MsgBitfield:
			c.Bitfield = Bitfield(msg.Payload)
		}
	}

	return msg, nil
}

// Close closes the underlying network connection.
func (c *Conn) Close() error {
	return c.Conn.Close()
}
