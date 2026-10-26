package tracker

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"time"
)

const (
	udpProtocolID  = 0x41727101980
	actionConnect  = 0
	actionAnnounce = 1
	actionError    = 3
)

// UDPAnnounce announces to a BEP 0015 UDP tracker and returns the list of peers.
func UDPAnnounce(params AnnounceParams) (*AnnounceResponse, error) {
	u, err := url.Parse(params.AnnounceURL)
	if err != nil {
		return nil, fmt.Errorf("parsing udp tracker url: %w", err)
	}

	host := u.Host
	conn, err := net.DialTimeout("udp", host, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("connecting to udp tracker %s: %w", host, err)
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		return nil, err
	}

	// 1. Send Connect Request
	txID := randomUint32()
	connectReq := make([]byte, 16)
	binary.BigEndian.PutUint64(connectReq[0:8], udpProtocolID)
	binary.BigEndian.PutUint32(connectReq[8:12], actionConnect)
	binary.BigEndian.PutUint32(connectReq[12:16], txID)

	if _, err := conn.Write(connectReq); err != nil {
		return nil, fmt.Errorf("sending udp connect request: %w", err)
	}

	// Read Connect Response (16 bytes)
	connectResp := make([]byte, 16)
	n, err := conn.Read(connectResp)
	if err != nil {
		return nil, fmt.Errorf("reading udp connect response: %w", err)
	}
	if n < 16 {
		return nil, errors.New("udp connect response too short")
	}

	action := binary.BigEndian.Uint32(connectResp[0:4])
	respTxID := binary.BigEndian.Uint32(connectResp[4:8])
	if respTxID != txID {
		return nil, errors.New("udp connect transaction id mismatch")
	}
	if action != actionConnect {
		return nil, fmt.Errorf("udp connect returned unexpected action %d", action)
	}

	connID := binary.BigEndian.Uint64(connectResp[8:16])

	// 2. Send Announce Request (98 bytes)
	announceTxID := randomUint32()
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.BigEndian, connID)
	_ = binary.Write(buf, binary.BigEndian, uint32(actionAnnounce))
	_ = binary.Write(buf, binary.BigEndian, announceTxID)
	buf.Write(params.InfoHash[:])
	buf.Write(params.PeerID[:])
	_ = binary.Write(buf, binary.BigEndian, uint64(params.Downloaded))
	_ = binary.Write(buf, binary.BigEndian, uint64(params.Left))
	_ = binary.Write(buf, binary.BigEndian, uint64(params.Uploaded))
	_ = binary.Write(buf, binary.BigEndian, uint32(0))      // event: none
	_ = binary.Write(buf, binary.BigEndian, uint32(0))      // IP address: 0
	_ = binary.Write(buf, binary.BigEndian, randomUint32()) // key
	_ = binary.Write(buf, binary.BigEndian, int32(-1))      // num_want: default
	_ = binary.Write(buf, binary.BigEndian, params.Port)

	if _, err := conn.Write(buf.Bytes()); err != nil {
		return nil, fmt.Errorf("sending udp announce request: %w", err)
	}

	// 3. Read Announce Response (minimum 20 bytes: 4 action + 4 tx + 4 interval + 4 leechers + 4 seeders + peers)
	announceResp := make([]byte, 4096)
	n, err = conn.Read(announceResp)
	if err != nil {
		return nil, fmt.Errorf("reading udp announce response: %w", err)
	}
	if n < 20 {
		return nil, errors.New("udp announce response too short")
	}

	action = binary.BigEndian.Uint32(announceResp[0:4])
	respTxID = binary.BigEndian.Uint32(announceResp[4:8])
	if respTxID != announceTxID {
		return nil, errors.New("udp announce transaction id mismatch")
	}

	if action == actionError {
		errMsg := string(announceResp[8:n])
		return nil, fmt.Errorf("udp tracker returned error: %s", errMsg)
	}
	if action != actionAnnounce {
		return nil, fmt.Errorf("udp announce returned unexpected action %d", action)
	}

	interval := binary.BigEndian.Uint32(announceResp[8:12])
	peersData := announceResp[20:n]
	peers, err := UnmarshalPeers(peersData)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling udp peers: %w", err)
	}

	return &AnnounceResponse{
		Interval: int64(interval),
		Peers:    peers,
	}, nil
}

func randomUint32() uint32 {
	n, err := rand.Int(rand.Reader, big.NewInt(0xFFFFFFFF))
	if err != nil {
		return 12345
	}
	return uint32(n.Int64())
}
