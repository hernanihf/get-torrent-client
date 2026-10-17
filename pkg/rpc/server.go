package rpc

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/hernan/get-torrent/pkg/engine"
)

const HeaderSessionID = "X-Transmission-Session-Id"

// Request represents a standard Transmission JSON-RPC request.
type Request struct {
	Method    string         `json:"method"`
	Arguments map[string]any `json:"arguments,omitempty"`
	Tag       int            `json:"tag,omitempty"`
}

// Response represents a standard Transmission JSON-RPC response.
type Response struct {
	Result    string         `json:"result"`
	Arguments map[string]any `json:"arguments,omitempty"`
	Tag       int            `json:"tag,omitempty"`
}

// Server provides Transmission-compatible RPC endpoints backed by the engine.
type Server struct {
	mu        sync.RWMutex
	sessionID string
	addr      string
	engine    *engine.Engine
}

// NewServer creates a new RPC server instance bound to an engine.
func NewServer(addr string, eng *engine.Engine) *Server {
	token := make([]byte, 16)
	_, _ = rand.Read(token)
	return &Server{
		sessionID: hex.EncodeToString(token),
		addr:      addr,
		engine:    eng,
	}
}

// Handler returns the HTTP handler for Transmission RPC.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/transmission/rpc", s.handleRPC)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprintf(w, "<h1>get-torrent (Transmission Clone)</h1><p>RPC endpoint active at <code>/transmission/rpc</code></p>")
	})
	return mux
}

func (s *Server) handleRPC(w http.ResponseWriter, r *http.Request) {
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(Response{Result: "invalid json request"})
		return
	}

	resp := s.dispatch(req)
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) dispatch(req Request) Response {
	switch req.Method {
	case "session-get":
		return Response{
			Result: "success",
			Tag:    req.Tag,
			Arguments: map[string]any{
				"version":             "4.0.0 (get-torrent)",
				"rpc-version":         17,
				"rpc-version-minimum": 14,
				"download-dir":        "/Downloads",
				"speed-limit-down":    0,
				"speed-limit-up":      0,
			},
		}
	case "torrent-get":
		var torrentsList []map[string]any
		if s.engine != nil {
			for _, t := range s.engine.ListTorrents() {
				torrentsList = append(torrentsList, map[string]any{
					"id":             t.ID,
					"name":           t.Meta.Name,
					"status":         t.State,
					"totalSize":      t.Meta.TotalLength,
					"leftUntilDone":  t.Left,
					"peersConnected": t.ActivePeers,
					"rateDownload":   0,
					"rateUpload":     0,
				})
			}
		}
		if torrentsList == nil {
			torrentsList = []map[string]any{}
		}
		return Response{
			Result: "success",
			Tag:    req.Tag,
			Arguments: map[string]any{
				"torrents": torrentsList,
			},
		}
	case "torrent-add":
		filename, _ := req.Arguments["filename"].(string)
		if filename != "" && s.engine != nil {
			t, err := s.engine.AddTorrentFile(filename)
			if err != nil {
				return Response{
					Result: fmt.Sprintf("failed adding torrent: %v", err),
					Tag:    req.Tag,
				}
			}
			return Response{
				Result: "success",
				Tag:    req.Tag,
				Arguments: map[string]any{
					"torrent-added": map[string]any{
						"id":   t.ID,
						"name": t.Meta.Name,
					},
				},
			}
		}
		return Response{
			Result: "filename required",
			Tag:    req.Tag,
		}
	default:
		log.Printf("Unknown or unimplemented RPC method: %s", req.Method)
		return Response{
			Result: fmt.Sprintf("method '%s' not implemented", req.Method),
			Tag:    req.Tag,
		}
	}
}

// ListenAndServe runs the HTTP server.
func (s *Server) ListenAndServe() error {
	log.Printf("[RPC] Transmission RPC server listening on http://%s/transmission/rpc", s.addr)
	return http.ListenAndServe(s.addr, s.Handler())
}
