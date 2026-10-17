package tracker

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hernan/get-torrent/pkg/bencode"
)

func TestUnmarshalPeers(t *testing.T) {
	// 2 peers: 127.0.0.1:6881 and 192.168.1.50:51413
	// 127.0.0.1 = 0x7F, 0x00, 0x00, 0x01. 6881 = 0x1A, 0xE1
	// 192.168.1.50 = 0xC0, 0xA8, 0x01, 0x32. 51413 = 0xC8, 0xD5
	peerBytes := []byte{
		127, 0, 0, 1, 0x1A, 0xE1,
		192, 168, 1, 50, 0xC8, 0xD5,
	}

	peers, err := UnmarshalPeers(peerBytes)
	if err != nil {
		t.Fatalf("unexpected error unmarshaling peers: %v", err)
	}

	if len(peers) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(peers))
	}

	if peers[0].String() != "127.0.0.1:6881" {
		t.Errorf("expected 127.0.0.1:6881, got %s", peers[0].String())
	}
	if peers[1].String() != "192.168.1.50:51413" {
		t.Errorf("expected 192.168.1.50:51413, got %s", peers[1].String())
	}
}

func TestHTTPAnnounce(t *testing.T) {
	// Mock HTTP tracker server
	peerBytes := []byte{10, 0, 0, 1, 0x1F, 0x90} // 10.0.0.1:8080
	mockResp := map[string]any{
		"interval": int64(1800),
		"peers":    string(peerBytes),
	}
	encodedResp, err := bencode.Encode(mockResp)
	if err != nil {
		t.Fatalf("failed encoding mock bencode: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write(encodedResp)
	}))
	defer server.Close()

	params := AnnounceParams{
		AnnounceURL: server.URL,
		InfoHash:    [20]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
		PeerID:      [20]byte{'-', 'G', 'T', '0', '0', '0', '1', '-', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		Port:        6881,
		Left:        1048576,
		Compact:     true,
	}

	resp, err := HTTPAnnounce(params)
	if err != nil {
		t.Fatalf("HTTPAnnounce failed: %v", err)
	}

	if resp.Interval != 1800 {
		t.Errorf("expected interval 1800, got %d", resp.Interval)
	}

	if len(resp.Peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(resp.Peers))
	}

	expectedPeer := "10.0.0.1:8080"
	if resp.Peers[0].String() != expectedPeer {
		t.Errorf("expected peer %s, got %s", expectedPeer, resp.Peers[0].String())
	}
}

func TestHTTPAnnounceFailure(t *testing.T) {
	mockResp := map[string]any{
		"failure reason": "torrent unregistered",
	}
	encodedResp, _ := bencode.Encode(mockResp)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(encodedResp)
	}))
	defer server.Close()

	params := AnnounceParams{
		AnnounceURL: server.URL,
	}

	_, err := HTTPAnnounce(params)
	if err == nil {
		t.Fatalf("expected error from tracker failure, got nil")
	}
	if !containsSubstring(err.Error(), "torrent unregistered") {
		t.Errorf("expected error message to mention 'torrent unregistered', got %v", err)
	}
}

func containsSubstring(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || fmt.Sprintf("%v", s) != "")
}
