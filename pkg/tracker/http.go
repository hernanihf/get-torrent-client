package tracker

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/hernan/get-torrent/pkg/bencode"
)

var (
	ErrTrackerFailure = errors.New("tracker returned failure reason")
)

// AnnounceParams represents parameters sent in an announce request.
type AnnounceParams struct {
	AnnounceURL string
	InfoHash    [20]byte
	PeerID      [20]byte
	Port        uint16
	Uploaded    int64
	Downloaded  int64
	Left        int64
	Compact     bool
}

// AnnounceResponse represents the parsed response from an announce request.
type AnnounceResponse struct {
	Interval int64
	Peers    []Peer
}

// HTTPAnnounce queries an HTTP/HTTPS tracker for peers using a default HTTP client.
func HTTPAnnounce(params AnnounceParams) (*AnnounceResponse, error) {
	return HTTPAnnounceWithClient(params, &http.Client{Timeout: 15 * time.Second})
}

// HTTPAnnounceWithClient queries an HTTP/HTTPS tracker for peers using the provided HTTP client.
func HTTPAnnounceWithClient(params AnnounceParams, client *http.Client) (*AnnounceResponse, error) {
	u, err := url.Parse(params.AnnounceURL)
	if err != nil {
		return nil, fmt.Errorf("invalid announce url: %w", err)
	}

	// We construct query parameters manually because info_hash and peer_id
	// are raw 20-byte binary values that require strict percent-encoding.
	q := u.Query()
	q.Set("port", strconv.Itoa(int(params.Port)))
	q.Set("uploaded", strconv.FormatInt(params.Uploaded, 10))
	q.Set("downloaded", strconv.FormatInt(params.Downloaded, 10))
	q.Set("left", strconv.FormatInt(params.Left, 10))
	if params.Compact {
		q.Set("compact", "1")
	}

	rawQuery := q.Encode()
	// Append raw percent-encoded info_hash and peer_id
	rawQuery += fmt.Sprintf("&info_hash=%s&peer_id=%s",
		escapeBinary(params.InfoHash[:]),
		escapeBinary(params.PeerID[:]),
	)
	u.RawQuery = rawQuery

	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	resp, err := client.Get(u.String())
	if err != nil {
		return nil, fmt.Errorf("tracker request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tracker returned HTTP status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading tracker response body: %w", err)
	}

	return parseHTTPResponse(body)
}

func parseHTTPResponse(body []byte) (*AnnounceResponse, error) {
	val, err := bencode.Decode(body)
	if err != nil {
		return nil, fmt.Errorf("decoding tracker bencode: %w", err)
	}

	dict, ok := val.(map[string]any)
	if !ok {
		return nil, errors.New("tracker response is not a bencode dictionary")
	}

	if failureReason, ok := dict["failure reason"].(string); ok {
		return nil, fmt.Errorf("%w: %s", ErrTrackerFailure, failureReason)
	}

	interval, _ := dict["interval"].(int64)

	var peers []Peer
	switch p := dict["peers"].(type) {
	case string:
		// Compact format (BEP 0023)
		var err error
		peers, err = UnmarshalPeers([]byte(p))
		if err != nil {
			return nil, fmt.Errorf("failed unmarshaling compact peers: %w", err)
		}
	case []any:
		// Dictionary list format
		for _, item := range p {
			pDict, ok := item.(map[string]any)
			if !ok {
				continue
			}
			ipStr, _ := pDict["ip"].(string)
			portNum, _ := pDict["port"].(int64)
			ip := net.ParseIP(ipStr)
			if ip != nil && portNum > 0 && portNum <= 65535 {
				peers = append(peers, Peer{
					IP:   ip,
					Port: uint16(portNum),
				})
			}
		}
	}

	return &AnnounceResponse{
		Interval: interval,
		Peers:    peers,
	}, nil
}

func escapeBinary(data []byte) string {
	var escaped string
	for _, b := range data {
		escaped += fmt.Sprintf("%%%02X", b)
	}
	return escaped
}
