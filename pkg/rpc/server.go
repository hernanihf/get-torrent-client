package rpc

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
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
		downloadDir := "/Downloads"
		var speedDown, speedUp int64
		var peerPort uint16 = 51413
		if s.engine != nil {
			downloadDir = s.engine.DownloadDir
			speedDown = s.engine.SpeedLimitDown
			speedUp = s.engine.SpeedLimitUp
			peerPort = s.engine.Port
		}

		return Response{
			Result: "success",
			Tag:    req.Tag,
			Arguments: map[string]any{
				"version":                  "4.0.0 (get-torrent)",
				"rpc-version":              17,
				"rpc-version-minimum":      14,
				"download-dir":             downloadDir,
				"download-dir-free-space":  100000000000,
				"speed-limit-down":         speedDown,
				"speed-limit-down-enabled": speedDown > 0,
				"speed-limit-up":           speedUp,
				"speed-limit-up-enabled":   speedUp > 0,
				"peer-port":                peerPort,
				"port-is-open":             true,
			},
		}

	case "session-set":
		if s.engine != nil {
			if newDir, ok := req.Arguments["download-dir"].(string); ok && newDir != "" {
				s.engine.DownloadDir = newDir
			}
			if downLimit, ok := req.Arguments["speed-limit-down"].(float64); ok {
				s.engine.SpeedLimitDown = int64(downLimit)
			}
			if upLimit, ok := req.Arguments["speed-limit-up"].(float64); ok {
				s.engine.SpeedLimitUp = int64(upLimit)
			}
			_ = s.engine.SaveState()
		}
		return Response{
			Result: "success",
			Tag:    req.Tag,
		}

	case "port-test":
		return Response{
			Result: "success",
			Tag:    req.Tag,
			Arguments: map[string]any{
				"port-is-open": true,
			},
		}

	case "torrent-get":
		torrentsList := []map[string]any{}
		if s.engine != nil {
			for _, t := range s.engine.ListTorrents() {
				st := t.Status()
				torrentsList = append(torrentsList, map[string]any{
					"id":                      st.ID,
					"name":                    st.Name,
					"hashString":              hex.EncodeToString(st.InfoHash[:]),
					"status":                  st.State,
					"totalSize":               st.TotalSize,
					"percentDone":             st.PercentDone,
					"metadataPercentComplete": st.MetadataPercentComplete,
					"leftUntilDone":           st.Left,
					"peersConnected":          st.ActivePeers,
					"rateDownload":            0,
					"rateUpload":              0,
					"uploadedEver":            st.Uploaded,
					"downloadedEver":          st.Downloaded,
					"magnetLink":              st.MagnetURI,
					"errorString":             st.Error,
				})
			}
		}
		return Response{
			Result: "success",
			Tag:    req.Tag,
			Arguments: map[string]any{
				"torrents": torrentsList,
			},
		}

	case "torrent-add":
		return s.torrentAdd(req)

	case "torrent-start", "torrent-start-now":
		ids := s.parseIDs(req.Arguments)
		for _, id := range ids {
			_ = s.engine.StartTorrent(id)
		}
		if s.engine != nil {
			_ = s.engine.SaveState()
		}
		return Response{
			Result: "success",
			Tag:    req.Tag,
		}

	case "torrent-stop":
		ids := s.parseIDs(req.Arguments)
		for _, id := range ids {
			_ = s.engine.StopTorrent(id)
		}
		if s.engine != nil {
			_ = s.engine.SaveState()
		}
		return Response{
			Result: "success",
			Tag:    req.Tag,
		}

	case "torrent-remove":
		ids := s.parseIDs(req.Arguments)
		deleteFiles, _ := req.Arguments["delete-local-data"].(bool)
		for _, id := range ids {
			_ = s.engine.RemoveTorrent(id, deleteFiles)
		}
		if s.engine != nil {
			_ = s.engine.SaveState()
		}
		return Response{
			Result: "success",
			Tag:    req.Tag,
		}

	case "torrent-set":
		return Response{
			Result: "success",
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

// torrentAdd implements "torrent-add": "filename" may be a local .torrent path or a magnet URI,
// and "metainfo" a base64-encoded .torrent. Torrents start immediately unless "paused" is true.
func (s *Server) torrentAdd(req Request) Response {
	fail := func(msg string) Response { return Response{Result: msg, Tag: req.Tag} }
	if s.engine == nil {
		return fail("engine unavailable")
	}

	filename, _ := req.Arguments["filename"].(string)
	metainfoB64, _ := req.Arguments["metainfo"].(string)

	var (
		t   *engine.ManagedTorrent
		err error
	)
	switch {
	case strings.HasPrefix(strings.ToLower(filename), "magnet:"):
		t, err = s.engine.AddMagnet(filename)
	case filename != "":
		t, err = s.engine.AddTorrentFile(filename)
	case metainfoB64 != "":
		var data []byte
		data, err = base64.StdEncoding.DecodeString(metainfoB64)
		if err == nil {
			t, err = s.engine.AddTorrentBytes(data)
		}
	default:
		return fail("filename or metainfo required")
	}
	if err != nil {
		return fail(fmt.Sprintf("failed adding torrent: %v", err))
	}

	if paused, _ := req.Arguments["paused"].(bool); !paused {
		_ = s.engine.StartTorrent(t.ID)
	}
	_ = s.engine.SaveState()

	st := t.Status()
	return Response{
		Result: "success",
		Tag:    req.Tag,
		Arguments: map[string]any{
			"torrent-added": map[string]any{
				"id":         st.ID,
				"name":       st.Name,
				"hashString": hex.EncodeToString(st.InfoHash[:]),
			},
		},
	}
}

func (s *Server) parseIDs(args map[string]any) []int {
	if s.engine == nil {
		return nil
	}
	raw, exists := args["ids"]
	if !exists || raw == nil {
		all := s.engine.ListTorrents()
		res := make([]int, len(all))
		for i, t := range all {
			res[i] = t.ID
		}
		return res
	}

	switch v := raw.(type) {
	case float64:
		return []int{int(v)}
	case int:
		return []int{v}
	case []any:
		var res []int
		for _, item := range v {
			if num, ok := item.(float64); ok {
				res = append(res, int(num))
			} else if num, ok := item.(int); ok {
				res = append(res, num)
			}
		}
		return res
	default:
		return nil
	}
}

// ListenAndServe runs the HTTP server.
func (s *Server) ListenAndServe() error {
	log.Printf("[RPC] Transmission RPC server listening on http://%s/transmission/rpc", s.addr)
	return http.ListenAndServe(s.addr, s.Handler())
}
