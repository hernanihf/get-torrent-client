package rpc

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hernan/get-torrent/pkg/engine"
)

func TestRPC_CSRFProtection(t *testing.T) {
	eng := engine.NewEngine("/tmp/downloads", 6881)
	server := NewServer("127.0.0.1:9091", eng)
	handler := server.Handler()

	// Request without X-Transmission-Session-Id header
	req := httptest.NewRequest(http.MethodPost, "/transmission/rpc", bytes.NewReader([]byte(`{}`)))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Should respond with 409 Conflict
	if w.Code != http.StatusConflict {
		t.Fatalf("expected HTTP 409 Conflict without token, got %d", w.Code)
	}

	sessionToken := w.Header().Get(HeaderSessionID)
	if sessionToken == "" {
		t.Fatalf("expected X-Transmission-Session-Id header in 409 response, got empty")
	}

	// Retry with the acquired token
	reqWithToken := httptest.NewRequest(http.MethodPost, "/transmission/rpc", bytes.NewReader([]byte(`{"method":"session-get","tag":42}`)))
	reqWithToken.Header.Set(HeaderSessionID, sessionToken)
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, reqWithToken)

	if w2.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 OK with token, got %d", w2.Code)
	}

	var resp Response
	if err := json.NewDecoder(w2.Body).Decode(&resp); err != nil {
		t.Fatalf("failed decoding json response: %v", err)
	}

	if resp.Result != "success" || resp.Tag != 42 {
		t.Errorf("unexpected response: %+v", resp)
	}

	version, ok := resp.Arguments["version"].(string)
	if !ok || version == "" {
		t.Errorf("expected version string in session-get, got %v", resp.Arguments["version"])
	}
}

func TestRPC_SessionSetAndPortTest(t *testing.T) {
	eng := engine.NewEngine("/tmp/downloads", 6881)
	server := NewServer("127.0.0.1:9091", eng)
	handler := server.Handler()

	// 1. session-set
	setBody := `{"method":"session-set","arguments":{"download-dir":"/new/downloads","speed-limit-down":500000}}`
	req := httptest.NewRequest(http.MethodPost, "/transmission/rpc", bytes.NewReader([]byte(setBody)))
	req.Header.Set(HeaderSessionID, server.sessionID)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("session-set failed with code %d", w.Code)
	}

	if eng.DownloadDir != "/new/downloads" {
		t.Errorf("expected download dir /new/downloads, got %s", eng.DownloadDir)
	}
	if eng.SpeedLimitDown != 500000 {
		t.Errorf("expected speed limit 500000, got %d", eng.SpeedLimitDown)
	}

	// 2. port-test
	portReq := httptest.NewRequest(http.MethodPost, "/transmission/rpc", bytes.NewReader([]byte(`{"method":"port-test"}`)))
	portReq.Header.Set(HeaderSessionID, server.sessionID)
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, portReq)

	var portResp Response
	_ = json.NewDecoder(w2.Body).Decode(&portResp)
	if portResp.Arguments["port-is-open"] != true {
		t.Errorf("expected port-is-open: true, got %v", portResp.Arguments["port-is-open"])
	}
}

func TestRPC_TorrentControl(t *testing.T) {
	eng := engine.NewEngine("/tmp/downloads", 6881)
	server := NewServer("127.0.0.1:9091", eng)
	handler := server.Handler()

	// Test torrent-start and torrent-stop with nonexistent / empty IDs (should not panic)
	for _, method := range []string{"torrent-start", "torrent-stop", "torrent-remove"} {
		body := fmt.Sprintf(`{"method":%q,"arguments":{"ids":[1, 2]}}`, method)
		req := httptest.NewRequest(http.MethodPost, "/transmission/rpc", bytes.NewReader([]byte(body)))
		req.Header.Set(HeaderSessionID, server.sessionID)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("method %s failed with code %d", method, w.Code)
		}
	}
}

func rpcCall(t *testing.T, server *Server, body string) Response {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/transmission/rpc", bytes.NewReader([]byte(body)))
	req.Header.Set(HeaderSessionID, server.sessionID)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)
	var resp Response
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decoding response to %s: %v", body, err)
	}
	return resp
}

func TestRPC_TorrentAddMagnet(t *testing.T) {
	eng := engine.NewEngine(t.TempDir(), 6881)
	eng.ConfigDir = t.TempDir()
	server := NewServer("127.0.0.1:9091", eng)

	magnetURI := "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=ubuntu.iso"
	resp := rpcCall(t, server, fmt.Sprintf(`{"method":"torrent-add","arguments":{"filename":%q,"paused":true}}`, magnetURI))
	if resp.Result != "success" {
		t.Fatalf("torrent-add magnet failed: %s", resp.Result)
	}
	added, _ := resp.Arguments["torrent-added"].(map[string]any)
	if added["name"] != "ubuntu.iso" || added["hashString"] != "0123456789abcdef0123456789abcdef01234567" {
		t.Errorf("unexpected torrent-added: %v", added)
	}

	get := rpcCall(t, server, `{"method":"torrent-get","arguments":{"fields":["id","name"]}}`)
	torrents, _ := get.Arguments["torrents"].([]any)
	if len(torrents) != 1 {
		t.Fatalf("expected 1 torrent, got %v", get.Arguments)
	}
	tor := torrents[0].(map[string]any)
	if tor["status"] != string(engine.StateQueued) || tor["metadataPercentComplete"] != 0.0 || tor["magnetLink"] != magnetURI {
		t.Errorf("unexpected torrent-get entry: %v", tor)
	}

	if resp := rpcCall(t, server, `{"method":"torrent-add","arguments":{"filename":"magnet:?xt=urn:btih:zz"}}`); resp.Result == "success" {
		t.Errorf("invalid magnet must fail")
	}
	if resp := rpcCall(t, server, `{"method":"torrent-add","arguments":{}}`); resp.Result == "success" {
		t.Errorf("torrent-add without filename/metainfo must fail")
	}
}

func TestRPC_TorrentAddMetainfo(t *testing.T) {
	eng := engine.NewEngine(t.TempDir(), 6881)
	eng.ConfigDir = t.TempDir()
	server := NewServer("127.0.0.1:9091", eng)

	torrent := "d8:announce12:http://tr/an4:infod6:lengthi10e4:name5:a.txt12:piece lengthi16e6:pieces20:AAAAAAAAAAAAAAAAAAAAee"
	b64 := base64.StdEncoding.EncodeToString([]byte(torrent))
	resp := rpcCall(t, server, fmt.Sprintf(`{"method":"torrent-add","arguments":{"metainfo":%q,"paused":true}}`, b64))
	if resp.Result != "success" {
		t.Fatalf("torrent-add metainfo failed: %s", resp.Result)
	}
	added, _ := resp.Arguments["torrent-added"].(map[string]any)
	if added["name"] != "a.txt" {
		t.Errorf("unexpected torrent-added: %v", added)
	}

	if resp := rpcCall(t, server, `{"method":"torrent-add","arguments":{"metainfo":"!!not-base64!!"}}`); resp.Result == "success" {
		t.Errorf("invalid base64 must fail")
	}
}
