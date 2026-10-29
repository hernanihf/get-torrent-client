package rpc

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const rpcURL = "http://127.0.0.1:9091/transmission/rpc"

func decodeResponse(t *testing.T, w *httptest.ResponseRecorder) Response {
	t.Helper()
	var resp Response
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decoding response %q: %v", w.Body.String(), err)
	}
	return resp
}

func TestNewServerDefaults(t *testing.T) {
	s := NewServer("127.0.0.1:9091", nil)
	if len(s.sessionID) != 32 {
		t.Errorf("session id must be 16 random bytes in hex, got %q", s.sessionID)
	}
	if s.opts.PortChecker == nil {
		t.Errorf("a default port checker must be installed")
	}
	if other := NewServer("127.0.0.1:9091", nil); other.sessionID == s.sessionID {
		t.Errorf("session ids must differ between servers")
	}
}

func TestStaleSessionIDIsRejected(t *testing.T) {
	env := newEnv(t, Options{})
	req := httptest.NewRequest(http.MethodPost, rpcURL, strings.NewReader(`{"method":"session-get"}`))
	req.Header.Set(HeaderSessionID, "stale-session-id")
	w := httptest.NewRecorder()
	env.server.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusConflict || w.Header().Get(HeaderSessionID) != env.server.sessionID {
		t.Errorf("expected 409 with the current session id, got %d %q", w.Code, w.Header().Get(HeaderSessionID))
	}
}

func TestNonPostIsRejected(t *testing.T) {
	env := newEnv(t, Options{})
	req := httptest.NewRequest(http.MethodGet, rpcURL, nil)
	req.Header.Set(HeaderSessionID, env.server.sessionID)
	w := httptest.NewRecorder()
	env.server.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestInvalidJSON(t *testing.T) {
	env := newEnv(t, Options{})
	for _, body := range []string{``, `{`, `not json`, `{"method": 5}`} {
		w := env.post(rpcURL, body)
		if w.Code != http.StatusBadRequest {
			t.Errorf("body %q: expected 400, got %d", body, w.Code)
			continue
		}
		resp := decodeResponse(t, w)
		if resp.Result != "invalid json request" || resp.Arguments == nil {
			t.Errorf("body %q: unexpected response %+v", body, resp)
		}
	}
}

func TestOversizedBodyIsRejected(t *testing.T) {
	env := newEnv(t, Options{})
	body := `{"method":"session-get","arguments":{"pad":"` + strings.Repeat("x", maxRequestBytes) + `"}}`
	if w := env.post(rpcURL, body); w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for a body over %d bytes, got %d", maxRequestBytes, w.Code)
	}
}

func TestNullArgumentsSelectAllTorrents(t *testing.T) {
	env := newEnv(t, Options{})
	env.mustSucceed(t, "torrent-add", map[string]any{"filename": testMagnet, "paused": true})
	w := env.post(rpcURL, `{"method":"torrent-get","arguments":null}`)
	resp := decodeResponse(t, w)
	if resp.Result != "success" || len(resp.Arguments["torrents"].([]any)) != 1 {
		t.Errorf("null arguments must behave like no arguments: %+v", resp)
	}
}

func TestNilEngine(t *testing.T) {
	s := NewServerWithOptions("127.0.0.1:9091", nil, Options{})
	post := func(method string) Response {
		req := httptest.NewRequest(http.MethodPost, rpcURL, strings.NewReader(`{"method":"`+method+`"}`))
		req.Header.Set(HeaderSessionID, s.sessionID)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, req)
		return decodeResponse(t, w)
	}
	if r := post("session-get"); r.Result != "engine unavailable" {
		t.Errorf("expected engine unavailable, got %q", r.Result)
	}
	// Unknown methods are reported as such even without an engine.
	if r := post("no-such-method"); r.Result != "method name not recognized" {
		t.Errorf("expected method name not recognized, got %q", r.Result)
	}
}

func TestInvalidIDsAreRejected(t *testing.T) {
	env := newEnv(t, Options{})
	env.mustSucceed(t, "torrent-add", map[string]any{"filename": testMagnet, "paused": true})
	methods := []string{"torrent-start", "torrent-stop", "torrent-remove", "torrent-set", "queue-move-up"}
	for _, m := range methods {
		for _, ids := range []any{true, []any{1, true}, []any{map[string]any{}}} {
			if r := env.call(t, m, map[string]any{"ids": ids}); r.Result == "success" {
				t.Errorf("%s with ids %v must fail", m, ids)
			}
		}
	}
	// The torrent survives the failed remove.
	if got := env.torrents(t, map[string]any{"fields": []any{"id"}}); len(got) != 1 {
		t.Errorf("torrent must not be removed by an invalid request, got %v", got)
	}
}

func TestDuplicateIDsAreCollapsed(t *testing.T) {
	env := newEnv(t, Options{})
	env.mustSucceed(t, "torrent-add", map[string]any{"filename": testMagnet, "paused": true})
	hash := "0123456789abcdef0123456789abcdef01234567"
	got := env.torrents(t, map[string]any{"ids": []any{1, hash, 1}, "fields": []any{"id"}})
	if len(got) != 1 {
		t.Errorf("the same torrent selected several ways must appear once, got %v", got)
	}
}

func TestIndexPage(t *testing.T) {
	env := newEnv(t, Options{Username: "admin", Password: "s3cret", HostWhitelist: []string{"seedbox.lan"}})
	get := func(host string, auth bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:9091/", nil)
		req.Host = host
		if auth {
			req.SetBasicAuth("admin", "s3cret")
		}
		w := httptest.NewRecorder()
		env.server.Handler().ServeHTTP(w, req)
		return w
	}

	w := get("seedbox.lan:9091", true)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "/transmission/rpc") ||
		!strings.HasPrefix(w.Header().Get("Content-Type"), "text/html") {
		t.Errorf("unexpected index page: %d %q", w.Code, w.Body.String())
	}
	if w := get("seedbox.lan:9091", false); w.Code != http.StatusUnauthorized {
		t.Errorf("index without credentials: expected 401, got %d", w.Code)
	}
	if w := get("evil.example.com", true); w.Code != http.StatusMisdirectedRequest {
		t.Errorf("index from unknown host: expected 421, got %d", w.Code)
	}
}

func TestHostAllowed(t *testing.T) {
	s := &Server{opts: Options{HostWhitelist: []string{"NAS.Lan", "*.example.net"}}}
	cases := map[string]bool{
		"":                     true,
		"LOCALHOST":            true,
		"app.localhost:9091":   true,
		"192.168.1.10":         true,
		"[fe80::1]:9091":       true,
		"nas.lan":              true, // whitelist is case-insensitive
		"a.b.example.net:9091": true,
		"example.net":          false, // "*." only matches subdomains
		"notexample.net":       false,
		"localhost.evil.com":   false,
	}
	for host, want := range cases {
		if got := s.hostAllowed(host); got != want {
			t.Errorf("hostAllowed(%q) = %v, want %v", host, got, want)
		}
	}
}
