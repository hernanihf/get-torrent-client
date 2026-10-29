package tracker

import (
	"context"
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

// maxHTTPResponseBytes bounds a tracker response body; peer lists are a few KiB at most.
const maxHTTPResponseBytes = 4 << 20

var defaultHTTPClient = &http.Client{Timeout: 15 * time.Second}

// HTTPAnnounce queries an HTTP/HTTPS tracker for peers using a default HTTP client.
func HTTPAnnounce(params AnnounceParams) (*AnnounceResponse, error) {
	return httpAnnounce(context.Background(), params, defaultHTTPClient)
}

// HTTPAnnounceWithClient queries an HTTP/HTTPS tracker for peers using the provided HTTP client.
func HTTPAnnounceWithClient(params AnnounceParams, client *http.Client) (*AnnounceResponse, error) {
	return httpAnnounce(context.Background(), params, client)
}

func httpAnnounce(ctx context.Context, params AnnounceParams, client *http.Client) (*AnnounceResponse, error) {
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
	if ev := params.Event.String(); ev != "" {
		q.Set("event", ev)
	}
	if params.Key != 0 {
		q.Set("key", fmt.Sprintf("%08X", params.Key))
	}
	if params.NumWant > 0 {
		q.Set("numwant", strconv.Itoa(params.NumWant))
	}
	if params.TrackerID != "" {
		q.Set("trackerid", params.TrackerID)
	}

	rawQuery := q.Encode()
	// Append raw percent-encoded info_hash and peer_id
	rawQuery += fmt.Sprintf("&info_hash=%s&peer_id=%s",
		escapeBinary(params.InfoHash[:]),
		escapeBinary(params.PeerID[:]),
	)
	u.RawQuery = rawQuery

	if client == nil {
		client = defaultHTTPClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("building tracker request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("tracker request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tracker returned HTTP status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxHTTPResponseBytes))
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

	out := &AnnounceResponse{Seeders: -1, Leechers: -1}
	out.Interval, _ = dict["interval"].(int64)
	out.MinInterval, _ = dict["min interval"].(int64)
	out.TrackerID, _ = dict["tracker id"].(string)
	out.Warning, _ = dict["warning message"].(string)
	if n, ok := dict["complete"].(int64); ok {
		out.Seeders = n
	}
	if n, ok := dict["incomplete"].(int64); ok {
		out.Leechers = n
	}

	switch p := dict["peers"].(type) {
	case string:
		// Compact format (BEP 0023)
		peers, err := UnmarshalPeers([]byte(p))
		if err != nil {
			return nil, fmt.Errorf("failed unmarshaling compact peers: %w", err)
		}
		out.Peers = peers
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
				out.Peers = append(out.Peers, Peer{
					IP:   ip,
					Port: uint16(portNum),
				})
			}
		}
	}

	return out, nil
}

func escapeBinary(data []byte) string {
	var escaped string
	for _, b := range data {
		escaped += fmt.Sprintf("%%%02X", b)
	}
	return escaped
}
