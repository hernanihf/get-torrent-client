package tracker

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"
)

const (
	udpProtocolID  = 0x41727101980
	actionConnect  = 0
	actionAnnounce = 1
	actionError    = 3
)

// BEP 0015 retransmission: wait 15·2ⁿ seconds for a response, then resend. The spec allows n up
// to 8 (over an hour in total); we give up after udpMaxRetries so a dead tracker does not hold
// an announce slot for that long. Variables so tests can shorten them.
var (
	udpBaseTimeout = 15 * time.Second
	udpMaxRetries  = 3 // 15 + 30 + 60 + 120 s
)

// udpConnIDTTL is how long a connection ID may be reused (BEP 0015: one minute for clients).
const udpConnIDTTL = time.Minute

var errUDPTimeout = errors.New("udp tracker did not respond")

// connIDCache remembers connection IDs per tracker so announces within a minute skip the
// connect round trip.
type connIDCache struct {
	mu sync.Mutex
	m  map[string]connIDEntry
}

type connIDEntry struct {
	id      uint64
	expires time.Time
}

var udpConnIDs = &connIDCache{m: make(map[string]connIDEntry)}

func (c *connIDCache) get(host string) (uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[host]
	if !ok || time.Now().After(e.expires) {
		delete(c.m, host)
		return 0, false
	}
	return e.id, true
}

func (c *connIDCache) put(host string, id uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[host] = connIDEntry{id: id, expires: time.Now().Add(udpConnIDTTL)}
}

func (c *connIDCache) drop(host string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, host)
}

// UDPAnnounce announces to a BEP 0015 UDP tracker and returns the list of peers.
func UDPAnnounce(params AnnounceParams) (*AnnounceResponse, error) {
	return UDPAnnounceContext(context.Background(), params)
}

// UDPAnnounceContext announces to a BEP 0015 UDP tracker, retransmitting with exponential
// backoff until it answers, udpMaxRetries is exhausted or ctx is done.
func UDPAnnounceContext(ctx context.Context, params AnnounceParams) (*AnnounceResponse, error) {
	u, err := url.Parse(params.AnnounceURL)
	if err != nil {
		return nil, fmt.Errorf("parsing udp tracker url: %w", err)
	}
	host := u.Host

	var d net.Dialer
	conn, err := d.DialContext(ctx, "udp", host)
	if err != nil {
		return nil, fmt.Errorf("connecting to udp tracker %s: %w", host, err)
	}
	defer conn.Close()
	// Cancellation unblocks a pending read.
	stop := context.AfterFunc(ctx, func() { _ = conn.SetReadDeadline(time.Now()) })
	defer stop()

	for n := 0; n <= udpMaxRetries; n++ {
		timeout := udpBaseTimeout << n
		connID, ok := udpConnIDs.get(host)
		if !ok {
			connID, err = udpConnect(ctx, conn, timeout)
			if err == nil {
				udpConnIDs.put(host, connID)
			}
		}
		var resp *AnnounceResponse
		if err == nil {
			resp, err = udpAnnounceOnce(ctx, conn, connID, params, timeout)
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !errors.Is(err, errUDPTimeout) {
			return resp, err
		}
		// The tracker may be ignoring an expired connection ID; reconnect on the next attempt.
		udpConnIDs.drop(host)
	}
	return nil, fmt.Errorf("%w after %d attempts", errUDPTimeout, udpMaxRetries+1)
}

func udpConnect(ctx context.Context, conn net.Conn, timeout time.Duration) (uint64, error) {
	txID := randomUint32()
	req := make([]byte, 16)
	binary.BigEndian.PutUint64(req[0:8], udpProtocolID)
	binary.BigEndian.PutUint32(req[8:12], actionConnect)
	binary.BigEndian.PutUint32(req[12:16], txID)

	resp, err := udpRoundTrip(ctx, conn, req, txID, actionConnect, timeout)
	if err != nil {
		return 0, err
	}
	if len(resp) < 16 {
		return 0, errors.New("udp connect response too short")
	}
	return binary.BigEndian.Uint64(resp[8:16]), nil
}

func udpAnnounceOnce(ctx context.Context, conn net.Conn, connID uint64, params AnnounceParams, timeout time.Duration) (*AnnounceResponse, error) {
	txID := randomUint32()
	numWant := int32(-1) // tracker default
	if params.NumWant > 0 {
		numWant = int32(params.NumWant)
	}
	req := make([]byte, 98)
	binary.BigEndian.PutUint64(req[0:8], connID)
	binary.BigEndian.PutUint32(req[8:12], actionAnnounce)
	binary.BigEndian.PutUint32(req[12:16], txID)
	copy(req[16:36], params.InfoHash[:])
	copy(req[36:56], params.PeerID[:])
	binary.BigEndian.PutUint64(req[56:64], uint64(params.Downloaded))
	binary.BigEndian.PutUint64(req[64:72], uint64(params.Left))
	binary.BigEndian.PutUint64(req[72:80], uint64(params.Uploaded))
	binary.BigEndian.PutUint32(req[80:84], uint32(params.Event))
	binary.BigEndian.PutUint32(req[84:88], 0) // IP address: use the sender's
	binary.BigEndian.PutUint32(req[88:92], params.Key)
	binary.BigEndian.PutUint32(req[92:96], uint32(numWant))
	binary.BigEndian.PutUint16(req[96:98], params.Port)

	resp, err := udpRoundTrip(ctx, conn, req, txID, actionAnnounce, timeout)
	if err != nil {
		return nil, err
	}
	// 4 action + 4 tx + 4 interval + 4 leechers + 4 seeders, then compact peers.
	if len(resp) < 20 {
		return nil, errors.New("udp announce response too short")
	}
	peersData := resp[20:]
	peersData = peersData[:len(peersData)-len(peersData)%6] // ignore a truncated trailing entry
	peers, err := UnmarshalPeers(peersData)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling udp peers: %w", err)
	}
	return &AnnounceResponse{
		Interval: int64(binary.BigEndian.Uint32(resp[8:12])),
		Leechers: int64(binary.BigEndian.Uint32(resp[12:16])),
		Seeders:  int64(binary.BigEndian.Uint32(resp[16:20])),
		Peers:    peers,
	}, nil
}

// udpRoundTrip sends req and waits up to timeout for the response carrying txID, skipping
// stray packets such as late answers to earlier attempts. It returns errUDPTimeout if none
// arrives, and the tracker's message if it answers with an error action.
func udpRoundTrip(ctx context.Context, conn net.Conn, req []byte, txID, wantAction uint32, timeout time.Duration) ([]byte, error) {
	if _, err := conn.Write(req); err != nil {
		return nil, fmt.Errorf("sending udp tracker request: %w", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	// Checked after setting the deadline: a cancellation that fired earlier would otherwise be
	// overridden by it; one that fires later resets it through the AfterFunc.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return nil, errUDPTimeout
			}
			return nil, fmt.Errorf("reading udp tracker response: %w", err)
		}
		if n < 8 || binary.BigEndian.Uint32(buf[4:8]) != txID {
			continue
		}
		switch action := binary.BigEndian.Uint32(buf[0:4]); action {
		case wantAction:
			return buf[:n], nil
		case actionError:
			return nil, fmt.Errorf("%w: %s", ErrTrackerFailure, buf[8:n])
		default:
			return nil, fmt.Errorf("udp tracker returned unexpected action %d", action)
		}
	}
}

func randomUint32() uint32 {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return binary.BigEndian.Uint32(b[:])
}
