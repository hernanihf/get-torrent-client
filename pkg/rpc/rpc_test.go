package rpc

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hernan/get-torrent/pkg/bencode"
	"github.com/hernan/get-torrent/pkg/engine"
)

const testMagnet = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=ubuntu.iso&tr=udp%3A%2F%2Ftracker.example.org%3A1337"

type testEnv struct {
	eng    *engine.Engine
	server *Server
	open   bool // what the fake port checker reports
}

func newEnv(t *testing.T, opts Options) *testEnv {
	t.Helper()
	env := &testEnv{eng: engine.NewEngine(t.TempDir(), 6881)}
	env.eng.ConfigDir = t.TempDir()
	t.Cleanup(func() { _ = env.eng.Close() })
	if opts.PortChecker == nil {
		opts.PortChecker = func(uint16) (bool, error) { return env.open, nil }
	}
	env.server = NewServerWithOptions("127.0.0.1:9091", env.eng, opts)
	return env
}

// post sends a raw request with a valid session id to the given URL.
func (env *testEnv) post(target, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
	req.Header.Set(HeaderSessionID, env.server.sessionID)
	w := httptest.NewRecorder()
	env.server.Handler().ServeHTTP(w, req)
	return w
}

// call performs an RPC method and decodes the response.
func (env *testEnv) call(t *testing.T, method string, args map[string]any) Response {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"method": method, "arguments": args})
	w := env.post("http://127.0.0.1:9091/transmission/rpc", string(body))
	if w.Code != http.StatusOK {
		t.Fatalf("%s: HTTP %d: %s", method, w.Code, w.Body.String())
	}
	var resp Response
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("%s: decoding response: %v", method, err)
	}
	return resp
}

func (env *testEnv) mustSucceed(t *testing.T, method string, args map[string]any) map[string]any {
	t.Helper()
	resp := env.call(t, method, args)
	if resp.Result != "success" {
		t.Fatalf("%s failed: %s", method, resp.Result)
	}
	return resp.Arguments
}

// torrents runs torrent-get and returns the torrent objects.
func (env *testEnv) torrents(t *testing.T, args map[string]any) []map[string]any {
	t.Helper()
	out := env.mustSucceed(t, "torrent-get", args)
	list, _ := out["torrents"].([]any)
	res := make([]map[string]any, 0, len(list))
	for _, item := range list {
		res = append(res, item.(map[string]any))
	}
	return res
}

// makeTorrent builds a single-file .torrent for random data, returning it and the data.
func makeTorrent(t *testing.T, name string, size, pieceLen int) ([]byte, []byte) {
	t.Helper()
	data := make([]byte, size)
	_, _ = rand.Read(data)
	var pieces []byte
	for off := 0; off < size; off += pieceLen {
		sum := sha1.Sum(data[off:min(off+pieceLen, size)])
		pieces = append(pieces, sum[:]...)
	}
	torrent, err := bencode.Encode(map[string]any{
		"announce":   "http://tracker.example.org/announce",
		"comment":    "test torrent",
		"created by": "rpc_test",
		"info": map[string]any{
			"name": name, "length": size, "piece length": pieceLen, "pieces": string(pieces),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return torrent, data
}

func b64(data []byte) string { return base64.StdEncoding.EncodeToString(data) }

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestCSRFAndTrailingSlash(t *testing.T) {
	env := newEnv(t, Options{})

	for _, path := range []string{"/transmission/rpc", "/transmission/rpc/"} {
		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:9091"+path, strings.NewReader(`{}`))
		w := httptest.NewRecorder()
		env.server.Handler().ServeHTTP(w, req)
		if w.Code != http.StatusConflict || w.Header().Get(HeaderSessionID) != env.server.sessionID {
			t.Fatalf("%s: expected 409 with session id, got %d", path, w.Code)
		}
	}

	// transmission-remote posts to the trailing-slash path.
	w := env.post("http://127.0.0.1:9091/transmission/rpc/", `{"method":"session-get","tag":42}`)
	var resp Response
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Result != "success" || resp.Tag == nil || *resp.Tag != 42 {
		t.Errorf("unexpected response: %+v", resp)
	}
}

func TestTagEcho(t *testing.T) {
	env := newEnv(t, Options{})
	w := env.post("http://127.0.0.1:9091/transmission/rpc", `{"method":"session-stats","tag":0}`)
	if !strings.Contains(w.Body.String(), `"tag":0`) {
		t.Errorf("tag 0 must be echoed: %s", w.Body.String())
	}
	w = env.post("http://127.0.0.1:9091/transmission/rpc", `{"method":"session-stats"}`)
	if strings.Contains(w.Body.String(), `"tag"`) {
		t.Errorf("absent tag must not be echoed: %s", w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"arguments":{`) {
		t.Errorf("arguments must always be present: %s", w.Body.String())
	}
}

func TestHostWhitelist(t *testing.T) {
	env := newEnv(t, Options{HostWhitelist: []string{"seedbox.lan", "*.example.net"}})
	cases := map[string]int{
		"127.0.0.1:9091":        http.StatusOK,
		"[::1]:9091":            http.StatusOK,
		"localhost:9091":        http.StatusOK,
		"seedbox.lan:9091":      http.StatusOK,
		"nas.example.net":       http.StatusOK,
		"evil.example.com:9091": http.StatusMisdirectedRequest, // DNS-rebinding attempt
		"example.net.evil.com":  http.StatusMisdirectedRequest,
	}
	for host, want := range cases {
		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:9091/transmission/rpc", strings.NewReader(`{"method":"session-stats"}`))
		req.Host = host
		req.Header.Set(HeaderSessionID, env.server.sessionID)
		w := httptest.NewRecorder()
		env.server.Handler().ServeHTTP(w, req)
		if w.Code != want {
			t.Errorf("Host %q: expected %d, got %d", host, want, w.Code)
		}
	}
}

func TestBasicAuth(t *testing.T) {
	env := newEnv(t, Options{Username: "admin", Password: "s3cret"})
	try := func(user, pass string, withSession bool) int {
		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:9091/transmission/rpc", strings.NewReader(`{"method":"session-stats"}`))
		if user != "" {
			req.SetBasicAuth(user, pass)
		}
		if withSession {
			req.Header.Set(HeaderSessionID, env.server.sessionID)
		}
		w := httptest.NewRecorder()
		env.server.Handler().ServeHTTP(w, req)
		if w.Code == http.StatusUnauthorized && w.Header().Get("WWW-Authenticate") == "" {
			t.Errorf("401 must carry WWW-Authenticate")
		}
		return w.Code
	}
	if got := try("", "", false); got != http.StatusUnauthorized {
		t.Errorf("no credentials: expected 401, got %d", got)
	}
	if got := try("admin", "wrong", true); got != http.StatusUnauthorized {
		t.Errorf("wrong password: expected 401, got %d", got)
	}
	// Authentication is checked before the CSRF token, as in Transmission.
	if got := try("admin", "s3cret", false); got != http.StatusConflict {
		t.Errorf("valid credentials without session: expected 409, got %d", got)
	}
	if got := try("admin", "s3cret", true); got != http.StatusOK {
		t.Errorf("valid credentials: expected 200, got %d", got)
	}
}

func TestSessionGet(t *testing.T) {
	env := newEnv(t, Options{})
	all := env.mustSucceed(t, "session-get", nil)
	if all["rpc-version"] != float64(RPCVersion) || all["session-id"] != env.server.sessionID {
		t.Errorf("unexpected version/session-id: %v / %v", all["rpc-version"], all["session-id"])
	}
	for _, key := range []string{"version", "download-dir", "peer-port", "units", "speed-limit-down", "dht-enabled", "config-dir", "rpc-version-semver"} {
		if _, ok := all[key]; !ok {
			t.Errorf("session-get missing %q", key)
		}
	}
	units, _ := all["units"].(map[string]any)
	if units["speed-bytes"] != float64(1000) || len(units["speed-units"].([]any)) != 4 {
		t.Errorf("unexpected units: %v", units)
	}

	some := env.mustSucceed(t, "session-get", map[string]any{"fields": []any{"version", "peer-port", "bogus"}})
	if len(some) != 2 || some["version"] == nil || some["peer-port"] == nil {
		t.Errorf("fields filter not applied: %v", some)
	}
}

func TestSessionSetAndPortTest(t *testing.T) {
	env := newEnv(t, Options{})
	env.mustSucceed(t, "session-set", map[string]any{
		"download-dir": "/new/downloads", "speed-limit-down": 500, "speed-limit-down-enabled": true, "unknown-key": 1,
	})
	st := env.eng.Settings()
	if st.DownloadDir != "/new/downloads" || st.SpeedLimitDown != 500 || !st.SpeedLimitDownEnabled {
		t.Errorf("settings not applied: %+v", st)
	}
	got := env.mustSucceed(t, "session-get", map[string]any{"fields": []any{"speed-limit-down", "speed-limit-down-enabled"}})
	if got["speed-limit-down"] != float64(500) || got["speed-limit-down-enabled"] != true {
		t.Errorf("session-get does not reflect session-set: %v", got)
	}

	env.open = true
	if r := env.mustSucceed(t, "port-test", nil); r["port-is-open"] != true {
		t.Errorf("expected port open, got %v", r)
	}
	env.open = false
	if r := env.mustSucceed(t, "port-test", nil); r["port-is-open"] != false {
		t.Errorf("expected port closed, got %v", r)
	}
	env.server.opts.PortChecker = func(uint16) (bool, error) { return false, errors.New("offline") }
	if r := env.call(t, "port-test", nil); r.Result == "success" {
		t.Errorf("port-test must fail when the checker fails")
	}
}

func TestTorrentAddMagnetAndDuplicate(t *testing.T) {
	env := newEnv(t, Options{})
	out := env.mustSucceed(t, "torrent-add", map[string]any{"filename": testMagnet, "paused": true})
	added, _ := out["torrent-added"].(map[string]any)
	if added["name"] != "ubuntu.iso" || added["hashString"] != "0123456789abcdef0123456789abcdef01234567" {
		t.Errorf("unexpected torrent-added: %v", out)
	}

	out = env.mustSucceed(t, "torrent-add", map[string]any{"filename": testMagnet})
	if dup, ok := out["torrent-duplicate"].(map[string]any); !ok || dup["id"] != added["id"] {
		t.Errorf("expected torrent-duplicate, got %v", out)
	}

	tor := env.torrents(t, map[string]any{"fields": []any{"status", "metadataPercentComplete", "magnetLink", "totalSize", "eta", "files"}})
	if len(tor) != 1 {
		t.Fatalf("expected 1 torrent, got %d", len(tor))
	}
	if tor[0]["status"] != float64(statusStopped) || tor[0]["metadataPercentComplete"] != 0.0 ||
		tor[0]["magnetLink"] != testMagnet || tor[0]["eta"] != float64(etaUnknown) {
		t.Errorf("unexpected magnet torrent: %v", tor[0])
	}

	for _, args := range []map[string]any{
		{"filename": "magnet:?xt=urn:btih:zz"},
		{},
		{"metainfo": "!!not-base64!!"},
		{"metainfo": b64([]byte("not a torrent"))},
	} {
		if r := env.call(t, "torrent-add", args); r.Result == "success" {
			t.Errorf("torrent-add %v must fail", args)
		}
	}
}

func TestTorrentAddMetainfoWithDownloadDir(t *testing.T) {
	env := newEnv(t, Options{})
	torrent, _ := makeTorrent(t, "a.bin", 40000, 16384)
	dir := t.TempDir()
	out := env.mustSucceed(t, "torrent-add", map[string]any{"metainfo": b64(torrent), "paused": true, "download-dir": dir})
	if out["torrent-added"] == nil {
		t.Fatalf("expected torrent-added: %v", out)
	}
	if _, err := os.Stat(filepath.Join(dir, "a.bin")); err != nil {
		t.Errorf("file must be allocated in the requested download-dir: %v", err)
	}
	tor := env.torrents(t, map[string]any{"fields": []any{"downloadDir", "comment", "creator", "pieceCount", "trackerList", "files"}})
	if tor[0]["downloadDir"] != dir || tor[0]["comment"] != "test torrent" || tor[0]["creator"] != "rpc_test" || tor[0]["pieceCount"] != float64(3) {
		t.Errorf("unexpected fields: %v", tor[0])
	}
	if tor[0]["trackerList"] != "http://tracker.example.org/announce" {
		t.Errorf("unexpected trackerList: %q", tor[0]["trackerList"])
	}
	files := tor[0]["files"].([]any)
	if f := files[0].(map[string]any); f["name"] != "a.bin" || f["length"] != float64(40000) || f["bytesCompleted"] != float64(0) {
		t.Errorf("unexpected files: %v", files)
	}
}

func TestTorrentGetFieldsFormatsAndIDs(t *testing.T) {
	env := newEnv(t, Options{})
	torrent, _ := makeTorrent(t, "b.bin", 1000, 16384)
	env.mustSucceed(t, "torrent-add", map[string]any{"metainfo": b64(torrent), "paused": true})
	env.mustSucceed(t, "torrent-add", map[string]any{"filename": testMagnet, "paused": true})

	// Exactly the requested fields; unknown ones ignored.
	tor := env.torrents(t, map[string]any{"fields": []any{"id", "name", "bogusField"}})
	if len(tor) != 2 || len(tor[0]) != 2 || tor[0]["id"] != float64(1) || tor[1]["id"] != float64(2) {
		t.Fatalf("unexpected filtered result: %v", tor)
	}

	// Every supported field renders (and JSON-encodes) for both a full torrent and a magnet.
	all := env.torrents(t, nil)
	if len(all[0]) != len(torrentFields) || len(all[1]) != len(torrentFields) {
		t.Errorf("default must return all %d fields, got %d / %d", len(torrentFields), len(all[0]), len(all[1]))
	}

	// ids: number, hash string, mixed list.
	hash := "0123456789abcdef0123456789abcdef01234567"
	for _, ids := range []any{2, hash, []any{hash}, []any{2, hash}, []any{strings.ToUpper(hash)}} {
		got := env.torrents(t, map[string]any{"ids": ids, "fields": []any{"hashString"}})
		if len(got) != 1 || got[0]["hashString"] != hash {
			t.Errorf("ids %v: got %v", ids, got)
		}
	}
	if got := env.torrents(t, map[string]any{"ids": []any{99}, "fields": []any{"id"}}); len(got) != 0 {
		t.Errorf("unknown ids must be skipped, got %v", got)
	}
	if r := env.call(t, "torrent-get", map[string]any{"ids": map[string]any{}}); r.Result == "success" {
		t.Errorf("invalid ids must fail")
	}

	// Table format: header row, then one row per torrent.
	out := env.mustSucceed(t, "torrent-get", map[string]any{"format": "table", "fields": []any{"id", "status"}})
	rows := out["torrents"].([]any)
	if len(rows) != 3 || rows[0].([]any)[0] != "id" || rows[0].([]any)[1] != "status" || rows[1].([]any)[0] != float64(1) {
		t.Errorf("unexpected table: %v", rows)
	}
}

func TestRecentlyActiveReportsRemoved(t *testing.T) {
	env := newEnv(t, Options{})
	env.mustSucceed(t, "torrent-add", map[string]any{"filename": testMagnet, "paused": true})
	out := env.mustSucceed(t, "torrent-get", map[string]any{"ids": "recently-active", "fields": []any{"id"}})
	if len(out["torrents"].([]any)) != 1 || len(out["removed"].([]any)) != 0 {
		t.Fatalf("newly added torrent must be recently active: %v", out)
	}
	env.mustSucceed(t, "torrent-remove", map[string]any{"ids": []any{1}})
	out = env.mustSucceed(t, "torrent-get", map[string]any{"ids": "recently-active", "fields": []any{"id"}})
	if len(out["torrents"].([]any)) != 0 || len(out["removed"].([]any)) != 1 || out["removed"].([]any)[0] != float64(1) {
		t.Errorf("expected torrent 1 in removed: %v", out)
	}
	// "removed" only accompanies recently-active requests.
	if out := env.mustSucceed(t, "torrent-get", map[string]any{"fields": []any{"id"}}); out["removed"] != nil {
		t.Errorf("removed must be absent: %v", out)
	}
}

func TestTorrentVerify(t *testing.T) {
	env := newEnv(t, Options{})
	dir := t.TempDir()
	torrent, data := makeTorrent(t, "c.bin", 50000, 16384) // 4 pieces
	// Data already on disk, with the third piece corrupted.
	corrupt := append([]byte(nil), data...)
	corrupt[2*16384+5] ^= 0xFF
	if err := os.WriteFile(filepath.Join(dir, "c.bin"), corrupt, 0644); err != nil {
		t.Fatal(err)
	}
	env.mustSucceed(t, "torrent-add", map[string]any{"metainfo": b64(torrent), "paused": true, "download-dir": dir})

	before := env.torrents(t, map[string]any{"fields": []any{"percentDone"}})
	if before[0]["percentDone"] != 0.0 {
		t.Fatalf("data must not count before verification: %v", before[0])
	}

	env.mustSucceed(t, "torrent-verify", map[string]any{"ids": []any{1}})
	var tor map[string]any
	waitFor(t, "verification", func() bool {
		tor = env.torrents(t, map[string]any{"fields": []any{"status", "haveValid", "leftUntilDone", "pieces", "files"}})[0]
		return tor["status"] == float64(statusStopped)
	})
	wantHave := float64(50000 - 16384)
	if tor["haveValid"] != wantHave || tor["leftUntilDone"] != float64(16384) {
		t.Errorf("expected 3 of 4 pieces verified: %v", tor)
	}
	// Bitfield 1101 0000 -> base64 of 0xD0.
	if tor["pieces"] != b64([]byte{0xD0}) {
		t.Errorf("unexpected pieces bitfield: %v", tor["pieces"])
	}
	f := tor["files"].([]any)[0].(map[string]any)
	if f["bytesCompleted"] != wantHave {
		t.Errorf("file progress must follow verified pieces: %v", f)
	}
}

func TestSessionStatsAndFreeSpace(t *testing.T) {
	env := newEnv(t, Options{})
	env.mustSucceed(t, "torrent-add", map[string]any{"filename": testMagnet, "paused": true})
	stats := env.mustSucceed(t, "session-stats", nil)
	if stats["torrentCount"] != float64(1) || stats["pausedTorrentCount"] != float64(1) || stats["activeTorrentCount"] != float64(0) {
		t.Errorf("unexpected counts: %v", stats)
	}
	current := stats["current-stats"].(map[string]any)
	if current["sessionCount"] != float64(1) {
		t.Errorf("unexpected current-stats: %v", current)
	}
	if _, ok := stats["cumulative-stats"].(map[string]any); !ok {
		t.Errorf("missing cumulative-stats")
	}

	dir := t.TempDir()
	fs := env.mustSucceed(t, "free-space", map[string]any{"path": dir})
	if fs["path"] != dir || fs["size-bytes"].(float64) <= 0 || fs["total_size"].(float64) < fs["size-bytes"].(float64) {
		t.Errorf("unexpected free-space: %v", fs)
	}
	if r := env.call(t, "free-space", nil); r.Result == "success" {
		t.Errorf("free-space without path must fail")
	}
}

func TestLifecycleMethods(t *testing.T) {
	closed := make(chan struct{})
	env := newEnv(t, Options{OnSessionClose: func() { close(closed) }})
	dir := t.TempDir()
	torrent, _ := makeTorrent(t, "d.bin", 1000, 16384)
	env.mustSucceed(t, "torrent-add", map[string]any{"metainfo": b64(torrent), "paused": true, "download-dir": dir})

	for _, m := range []string{"torrent-start", "torrent-reannounce", "torrent-stop", "queue-move-top", "torrent-set"} {
		env.mustSucceed(t, m, map[string]any{"ids": []any{1}})
	}
	// Nonexistent ids are skipped without error, as in Transmission.
	env.mustSucceed(t, "torrent-stop", map[string]any{"ids": []any{1, 2}})

	if r := env.call(t, "torrent-set-location", map[string]any{"ids": []any{1}}); r.Result == "success" {
		t.Errorf("unsupported methods must report failure")
	}
	if r := env.call(t, "no-such-method", nil); r.Result == "success" {
		t.Errorf("unknown methods must report failure")
	}

	env.mustSucceed(t, "torrent-remove", map[string]any{"ids": []any{1}, "delete-local-data": true})
	if _, err := os.Stat(filepath.Join(dir, "d.bin")); !os.IsNotExist(err) {
		t.Errorf("delete-local-data must remove files, stat err=%v", err)
	}

	env.mustSucceed(t, "session-close", nil)
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("session-close did not invoke the shutdown hook")
	}
}

func TestStatusCodes(t *testing.T) {
	cases := map[engine.TorrentState]int{
		engine.StateStopped:     statusStopped,
		engine.StateQueued:      statusStopped,
		engine.StateError:       statusStopped,
		engine.StateChecking:    statusCheck,
		engine.StateDownloading: statusDownload,
		engine.StateSeeding:     statusSeed,
	}
	for state, want := range cases {
		if got := statusCode(state); got != want {
			t.Errorf("%s: expected %d, got %d", state, want, got)
		}
	}
	if s := sitename("udp://tracker.opentrackr.org:1337/announce"); s != "opentrackr" {
		t.Errorf("sitename = %q", s)
	}
}

func TestSpeedLimitSettings(t *testing.T) {
	env := newEnv(t, Options{})
	torrent, _ := makeTorrent(t, "e.bin", 1000, 16384)
	env.mustSucceed(t, "torrent-add", map[string]any{"metainfo": b64(torrent), "paused": true})

	// Defaults match Transmission: per-torrent limits off, 100 KB/s preset, honour session limits.
	tor := env.torrents(t, map[string]any{"fields": []any{"downloadLimit", "downloadLimited", "uploadLimit", "uploadLimited", "honorsSessionLimits"}})[0]
	if tor["downloadLimit"] != float64(100) || tor["downloadLimited"] != false || tor["honorsSessionLimits"] != true {
		t.Errorf("unexpected defaults: %v", tor)
	}

	env.mustSucceed(t, "torrent-set", map[string]any{"ids": []any{1}, "downloadLimit": 250, "downloadLimited": true,
		"uploadLimit": 40, "uploadLimited": true, "honorsSessionLimits": false, "unsupportedKey": 1})
	tor = env.torrents(t, map[string]any{"fields": []any{"downloadLimit", "downloadLimited", "uploadLimit", "uploadLimited", "honorsSessionLimits"}})[0]
	if tor["downloadLimit"] != float64(250) || tor["downloadLimited"] != true || tor["uploadLimit"] != float64(40) ||
		tor["uploadLimited"] != true || tor["honorsSessionLimits"] != false {
		t.Errorf("torrent-set not applied: %v", tor)
	}

	env.mustSucceed(t, "session-set", map[string]any{"alt-speed-down": 20, "alt-speed-up": 10, "alt-speed-enabled": true})
	got := env.mustSucceed(t, "session-get", map[string]any{"fields": []any{"alt-speed-down", "alt-speed-up", "alt-speed-enabled"}})
	if got["alt-speed-down"] != float64(20) || got["alt-speed-up"] != float64(10) || got["alt-speed-enabled"] != true {
		t.Errorf("alt speed not applied: %v", got)
	}
}
