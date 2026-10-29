// Package rpc implements the Transmission RPC protocol (legacy "bespoke" dialect, rpc-version 17
// as in Transmission 4.0.x) on top of the get-torrent engine, so existing Transmission clients
// such as transmission-remote and Transmission Remote GUI can control the daemon.
package rpc

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/hernan/get-torrent/pkg/engine"
	"github.com/hernan/get-torrent/pkg/storage"
)

const (
	HeaderSessionID = "X-Transmission-Session-Id"

	// RPCVersion is the Transmission RPC version we implement (Transmission 4.0.x).
	RPCVersion        = 17
	RPCVersionMinimum = 14
	RPCVersionSemver  = "5.3.0"

	// maxRequestBytes bounds request bodies; torrent-add metainfo is base64 .torrent data.
	maxRequestBytes = 32 << 20
)

// Request represents a Transmission RPC request.
type Request struct {
	Method    string         `json:"method"`
	Arguments map[string]any `json:"arguments,omitempty"`
	Tag       *int64         `json:"tag,omitempty"`
}

// Response represents a Transmission RPC response. Arguments is always present (possibly empty),
// as in Transmission.
type Response struct {
	Result    string         `json:"result"`
	Arguments map[string]any `json:"arguments"`
	Tag       *int64         `json:"tag,omitempty"`
}

// Options configures access control for the RPC server.
type Options struct {
	// Username and Password enable HTTP Basic authentication when Username is non-empty.
	Username string
	Password string
	// HostWhitelist lists extra Host header names allowed besides IP addresses and "localhost".
	// Entries may start with "*." to match subdomains. This blocks DNS-rebinding attacks.
	HostWhitelist []string
	// OnSessionClose is called (asynchronously) when a client sends "session-close".
	OnSessionClose func()
	// PortChecker reports whether the peer port is reachable from the Internet ("port-test").
	PortChecker func(port uint16) (bool, error)
}

// Server provides Transmission-compatible RPC endpoints backed by the engine.
type Server struct {
	mu        sync.RWMutex
	sessionID string
	addr      string
	engine    *engine.Engine
	opts      Options
}

// NewServer creates a new RPC server instance bound to an engine.
func NewServer(addr string, eng *engine.Engine) *Server {
	return NewServerWithOptions(addr, eng, Options{})
}

// NewServerWithOptions creates an RPC server with authentication and host restrictions.
func NewServerWithOptions(addr string, eng *engine.Engine, opts Options) *Server {
	token := make([]byte, 16)
	_, _ = rand.Read(token)
	if opts.PortChecker == nil {
		opts.PortChecker = checkPortOnline
	}
	return &Server{
		sessionID: hex.EncodeToString(token),
		addr:      addr,
		engine:    eng,
		opts:      opts,
	}
}

// Handler returns the HTTP handler for Transmission RPC.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	// transmission-remote posts to "/transmission/rpc/" (trailing slash); other clients omit it.
	mux.HandleFunc("/transmission/rpc", s.handleRPC)
	mux.HandleFunc("/transmission/rpc/", s.handleRPC)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if !s.authorize(w, r) {
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprintf(w, "<h1>get-torrent (Transmission Clone)</h1><p>RPC endpoint active at <code>/transmission/rpc</code></p>")
	})
	return mux
}

// authorize enforces the Host whitelist and Basic authentication, writing an error response
// and returning false when the request is rejected.
func (s *Server) authorize(w http.ResponseWriter, r *http.Request) bool {
	if !s.hostAllowed(r.Host) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusMisdirectedRequest)
		fmt.Fprintf(w, "<h1>421: Misdirected Request</h1><p>Transmission received your request, but the hostname was unrecognized.</p>")
		return false
	}
	if s.opts.Username != "" {
		user, pass, ok := r.BasicAuth()
		userOK := subtle.ConstantTimeCompare([]byte(user), []byte(s.opts.Username)) == 1
		passOK := subtle.ConstantTimeCompare([]byte(pass), []byte(s.opts.Password)) == 1
		if !ok || !userOK || !passOK {
			w.Header().Set("WWW-Authenticate", `Basic realm="Transmission"`)
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprintf(w, "<h1>401: Unauthorized</h1>")
			return false
		}
	}
	return true
}

// hostAllowed accepts IP literals, "localhost", and whitelisted names (DNS-rebinding protection,
// as in Transmission's rpc-host-whitelist).
func (s *Server) hostAllowed(hostport string) bool {
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	host = strings.Trim(strings.ToLower(host), "[]")
	if host == "" || host == "localhost" || strings.HasSuffix(host, ".localhost") || net.ParseIP(host) != nil {
		return true
	}
	for _, allowed := range s.opts.HostWhitelist {
		allowed = strings.ToLower(allowed)
		if allowed == host || (strings.HasPrefix(allowed, "*.") && strings.HasSuffix(host, allowed[1:])) {
			return true
		}
	}
	return false
}

func (s *Server) handleRPC(w http.ResponseWriter, r *http.Request) {
	if !s.authorize(w, r) {
		return
	}

	s.mu.RLock()
	currentSessionID := s.sessionID
	s.mu.RUnlock()

	// CSRF Protection per Transmission RPC Spec:
	clientSessionID := r.Header.Get(HeaderSessionID)
	if clientSessionID == "" || clientSessionID != currentSessionID {
		w.Header().Set(HeaderSessionID, currentSessionID)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusConflict)
		fmt.Fprintf(w, "<h1>409: Conflict</h1><p>Your request is missing the %s header.</p>", HeaderSessionID)
		return
	}

	w.Header().Set(HeaderSessionID, currentSessionID)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var req Request
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBytes)).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(Response{Result: "invalid json request", Arguments: map[string]any{}})
		return
	}
	if req.Arguments == nil {
		req.Arguments = map[string]any{}
	}

	resp := s.dispatch(req)
	resp.Tag = req.Tag
	if resp.Arguments == nil {
		resp.Arguments = map[string]any{}
	}
	_ = json.NewEncoder(w).Encode(resp)
}

// handlerFunc implements one RPC method, returning response arguments or an error whose text
// becomes the "result" string.
type handlerFunc func(s *Server, args map[string]any) (map[string]any, error)

var methods = map[string]handlerFunc{
	"session-get":          (*Server).sessionGet,
	"session-set":          (*Server).sessionSet,
	"session-stats":        (*Server).sessionStats,
	"session-close":        (*Server).sessionClose,
	"free-space":           (*Server).freeSpace,
	"port-test":            (*Server).portTest,
	"torrent-get":          (*Server).torrentGet,
	"torrent-add":          (*Server).torrentAdd,
	"torrent-set":          (*Server).torrentSet,
	"torrent-start":        forEach(func(e *engine.Engine, id int) error { return e.StartTorrent(id) }),
	"torrent-start-now":    forEach(func(e *engine.Engine, id int) error { return e.StartTorrent(id) }),
	"torrent-stop":         forEach(func(e *engine.Engine, id int) error { return e.StopTorrent(id) }),
	"torrent-verify":       forEach(func(e *engine.Engine, id int) error { return e.VerifyTorrent(id) }),
	"torrent-reannounce":   forEach(func(e *engine.Engine, id int) error { return e.Reannounce(id) }),
	"torrent-remove":       (*Server).torrentRemove,
	"queue-move-top":       noQueue,
	"queue-move-up":        noQueue,
	"queue-move-down":      noQueue,
	"queue-move-bottom":    noQueue,
	"blocklist-update":     unsupported,
	"torrent-set-location": unsupported,
	"torrent-rename-path":  unsupported,
	"group-get":            unsupported,
	"group-set":            unsupported,
}

func (s *Server) dispatch(req Request) Response {
	h, ok := methods[req.Method]
	if !ok {
		log.Printf("[RPC] Unknown method: %s", req.Method)
		return Response{Result: "method name not recognized"}
	}
	if s.engine == nil {
		return Response{Result: "engine unavailable"}
	}
	args, err := h(s, req.Arguments)
	if err != nil {
		return Response{Result: err.Error()}
	}
	return Response{Result: "success", Arguments: args}
}

// forEach applies an engine action to every torrent selected by "ids".
func forEach(action func(e *engine.Engine, id int) error) handlerFunc {
	return func(s *Server, args map[string]any) (map[string]any, error) {
		sel, err := s.selectTorrents(args)
		if err != nil {
			return nil, err
		}
		for _, t := range sel.torrents {
			if err := action(s.engine, t.ID); err != nil {
				return nil, err
			}
		}
		_ = s.engine.SaveState()
		return nil, nil
	}
}

// noQueue accepts queue moves: torrents are never queued (all started torrents run).
func noQueue(s *Server, args map[string]any) (map[string]any, error) {
	_, err := s.selectTorrents(args)
	return nil, err
}

func unsupported(*Server, map[string]any) (map[string]any, error) {
	return nil, fmt.Errorf("method not supported by get-torrent")
}

func (s *Server) torrentRemove(args map[string]any) (map[string]any, error) {
	sel, err := s.selectTorrents(args)
	if err != nil {
		return nil, err
	}
	deleteFiles, _ := args["delete-local-data"].(bool)
	for _, t := range sel.torrents {
		if err := s.engine.RemoveTorrent(t.ID, deleteFiles); err != nil {
			return nil, err
		}
	}
	_ = s.engine.SaveState()
	return nil, nil
}

// torrentSet applies per-torrent bandwidth limits and file selection. Like Transmission, keys it
// does not support are ignored, but ids must still be valid.
func (s *Server) torrentSet(args map[string]any) (map[string]any, error) {
	sel, err := s.selectTorrents(args)
	if err != nil {
		return nil, err
	}
	if hasFileArgs(args) {
		for _, t := range sel.torrents {
			if err := s.engine.SetFileSelection(t.ID, func(fs *engine.FileSelection) error {
				return applyFileArgs(args, fs)
			}); err != nil {
				return nil, err
			}
		}
	}
	for _, t := range sel.torrents {
		err := s.engine.SetTorrentLimits(t.ID, func(l *engine.TorrentLimits) {
			if v, ok := args["downloadLimit"].(float64); ok {
				l.DownloadLimit = int64(v)
			}
			if v, ok := args["downloadLimited"].(bool); ok {
				l.DownloadLimited = v
			}
			if v, ok := args["uploadLimit"].(float64); ok {
				l.UploadLimit = int64(v)
			}
			if v, ok := args["uploadLimited"].(bool); ok {
				l.UploadLimited = v
			}
			if v, ok := args["honorsSessionLimits"].(bool); ok {
				l.HonorsSessionLimits = v
			}
		})
		if err != nil {
			return nil, err
		}
	}
	_ = s.engine.SaveState()
	return nil, nil
}

func (s *Server) sessionClose(map[string]any) (map[string]any, error) {
	if s.opts.OnSessionClose != nil {
		go s.opts.OnSessionClose()
	}
	return nil, nil
}

// selection is the set of torrents an "ids" argument refers to.
type selection struct {
	torrents   []*engine.ManagedTorrent
	removed    []int
	recentOnly bool
}

// selectTorrents resolves "ids": absent (all), a number, "recently-active", a hash string, or a
// list of numbers and hash strings. Unknown ids are skipped, as in Transmission.
func (s *Server) selectTorrents(args map[string]any) (selection, error) {
	raw, exists := args["ids"]
	if !exists || raw == nil {
		return selection{torrents: sortedByID(s.engine.ListTorrents())}, nil
	}

	var items []any
	switch v := raw.(type) {
	case string:
		if v == "recently-active" || v == "recently_active" {
			active, removed := s.engine.RecentlyActive()
			return selection{torrents: sortedByID(active), removed: removed, recentOnly: true}, nil
		}
		items = []any{v}
	case float64:
		items = []any{v}
	case []any:
		items = v
	default:
		return selection{}, fmt.Errorf("invalid ids argument")
	}

	seen := make(map[int]bool)
	var out []*engine.ManagedTorrent
	for _, item := range items {
		var t *engine.ManagedTorrent
		switch v := item.(type) {
		case float64:
			t = s.engine.GetTorrentByID(int(v))
		case string:
			t = s.engine.TorrentByHashString(v)
		default:
			return selection{}, fmt.Errorf("invalid id %v", item)
		}
		if t != nil && !seen[t.ID] {
			seen[t.ID] = true
			out = append(out, t)
		}
	}
	return selection{torrents: out}, nil
}

// ListenAndServe runs the HTTP server.
func (s *Server) ListenAndServe() error {
	log.Printf("[RPC] Transmission RPC server listening on http://%s/transmission/rpc", s.addr)
	return http.ListenAndServe(s.addr, s.Handler())
}

// fileArgs maps the torrent-add/torrent-set file arguments to what they set.
var fileArgs = []struct {
	key      string
	wanted   *bool
	priority *int8
}{
	{key: "files-wanted", wanted: ptr(true)},
	{key: "files-unwanted", wanted: ptr(false)},
	{key: "priority-high", priority: ptr(storage.PriorityHigh)},
	{key: "priority-normal", priority: ptr(storage.PriorityNormal)},
	{key: "priority-low", priority: ptr(storage.PriorityLow)},
}

func ptr[T any](v T) *T { return &v }

func hasFileArgs(args map[string]any) bool {
	for _, fa := range fileArgs {
		if _, ok := args[fa.key]; ok {
			return true
		}
	}
	return false
}

// applyFileArgs applies files-wanted, files-unwanted and priority-* to a selection. Each holds
// file indices; an empty list means every file, as in the Transmission spec.
func applyFileArgs(args map[string]any, sel *engine.FileSelection) error {
	n := len(sel.Wanted)
	for _, fa := range fileArgs {
		raw, ok := args[fa.key]
		if !ok {
			continue
		}
		list, ok := raw.([]any)
		if !ok {
			return fmt.Errorf("%s must be a list of file indices", fa.key)
		}
		indices := make([]int, 0, len(list))
		for _, v := range list {
			f, ok := v.(float64)
			if !ok || f != float64(int(f)) || f < 0 || int(f) >= n {
				return fmt.Errorf("%s: invalid file index %v", fa.key, v)
			}
			indices = append(indices, int(f))
		}
		if len(list) == 0 {
			for i := range n {
				indices = append(indices, i)
			}
		}
		for _, i := range indices {
			if fa.wanted != nil {
				sel.Wanted[i] = *fa.wanted
			}
			if fa.priority != nil {
				sel.Priority[i] = *fa.priority
			}
		}
	}
	return nil
}
