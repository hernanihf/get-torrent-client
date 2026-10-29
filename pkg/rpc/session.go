package rpc

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/hernan/get-torrent/pkg/engine"
)

// sessionGet implements "session-get", honouring the optional "fields" filter (rpc-version 16).
func (s *Server) sessionGet(args map[string]any) (map[string]any, error) {
	e := s.engine
	settings := e.Settings()
	downloadDir := settings.DownloadDir
	configDir := e.ConfigDir
	if abs, err := filepath.Abs(configDir); err == nil {
		configDir = abs
	}
	freeBytes, _, _ := engine.FreeSpace(downloadDir)

	s.mu.RLock()
	sessionID := s.sessionID
	s.mu.RUnlock()

	all := map[string]any{
		"alt-speed-down":                       settings.AltSpeedDown,
		"alt-speed-enabled":                    settings.AltSpeedEnabled,
		"alt-speed-time-begin":                 540,
		"alt-speed-time-day":                   127,
		"alt-speed-time-enabled":               false,
		"alt-speed-time-end":                   1020,
		"alt-speed-up":                         settings.AltSpeedUp,
		"blocklist-enabled":                    false,
		"blocklist-size":                       0,
		"blocklist-url":                        "",
		"cache-size-mb":                        4,
		"config-dir":                           configDir,
		"default-trackers":                     "",
		"dht-enabled":                          e.DHTEnabled(),
		"download-dir":                         downloadDir,
		"download-dir-free-space":              freeBytes,
		"download-queue-enabled":               false,
		"download-queue-size":                  5,
		"encryption":                           "tolerated", // plaintext only; encryption is not implemented
		"idle-seeding-limit":                   30,
		"idle-seeding-limit-enabled":           false,
		"incomplete-dir":                       downloadDir,
		"incomplete-dir-enabled":               false,
		"lpd-enabled":                          false,
		"peer-limit-global":                    200,
		"peer-limit-per-torrent":               engine.MaxPeersPerTorrent,
		"peer-port":                            e.Port,
		"peer-port-random-on-start":            false,
		"pex-enabled":                          false,
		"port-forwarding-enabled":              false,
		"queue-stalled-enabled":                false,
		"queue-stalled-minutes":                30,
		"rename-partial-files":                 false,
		"rpc-version":                          RPCVersion,
		"rpc-version-minimum":                  RPCVersionMinimum,
		"rpc-version-semver":                   RPCVersionSemver,
		"script-torrent-added-enabled":         false,
		"script-torrent-added-filename":        "",
		"script-torrent-done-enabled":          false,
		"script-torrent-done-filename":         "",
		"script-torrent-done-seeding-enabled":  false,
		"script-torrent-done-seeding-filename": "",
		"seed-queue-enabled":                   false,
		"seed-queue-size":                      10,
		"seedRatioLimit":                       2.0,
		"seedRatioLimited":                     false,
		"session-id":                           sessionID,
		"speed-limit-down":                     settings.SpeedLimitDown,
		"speed-limit-down-enabled":             settings.SpeedLimitDownEnabled,
		"speed-limit-up":                       settings.SpeedLimitUp,
		"speed-limit-up-enabled":               settings.SpeedLimitUpEnabled,
		"start-added-torrents":                 true,
		"trash-original-torrent-files":         false,
		"units": map[string]any{
			"speed-units":  []string{"kB/s", "MB/s", "GB/s", "TB/s"},
			"speed-bytes":  1000,
			"size-units":   []string{"kB", "MB", "GB", "TB"},
			"size-bytes":   1000,
			"memory-units": []string{"KiB", "MiB", "GiB", "TiB"},
			"memory-bytes": 1024,
		},
		"utp-enabled": false,
		"version":     "4.0.6 (get-torrent)",
	}

	raw, ok := args["fields"].([]any)
	if !ok {
		return all, nil
	}
	out := make(map[string]any, len(raw))
	for _, f := range raw {
		if name, ok := f.(string); ok {
			if v, exists := all[name]; exists {
				out[name] = v
			}
		}
	}
	return out, nil
}

// sessionSet implements "session-set". Settings get-torrent does not support are ignored,
// as Transmission ignores unknown keys; session-get keeps reporting the effective values.
func (s *Server) sessionSet(args map[string]any) (map[string]any, error) {
	s.engine.UpdateSettings(func(st *engine.Settings) {
		if newDir, ok := args["download-dir"].(string); ok && newDir != "" {
			st.DownloadDir = newDir
		}
		if v, ok := args["speed-limit-down"].(float64); ok {
			st.SpeedLimitDown = int64(v)
		}
		if v, ok := args["speed-limit-up"].(float64); ok {
			st.SpeedLimitUp = int64(v)
		}
		if v, ok := args["speed-limit-down-enabled"].(bool); ok {
			st.SpeedLimitDownEnabled = v
		}
		if v, ok := args["speed-limit-up-enabled"].(bool); ok {
			st.SpeedLimitUpEnabled = v
		}
		if v, ok := args["alt-speed-down"].(float64); ok {
			st.AltSpeedDown = int64(v)
		}
		if v, ok := args["alt-speed-up"].(float64); ok {
			st.AltSpeedUp = int64(v)
		}
		if v, ok := args["alt-speed-enabled"].(bool); ok {
			st.AltSpeedEnabled = v
		}
	})
	_ = s.engine.SaveState()
	return nil, nil
}

// sessionStats implements "session-stats".
func (s *Server) sessionStats(map[string]any) (map[string]any, error) {
	var active, paused int
	var down, up int64
	list := s.engine.ListTorrents()
	for _, t := range list {
		st := t.Status()
		switch st.State {
		case engine.StateDownloading, engine.StateSeeding, engine.StateChecking:
			active++
		default:
			paused++
		}
		down += st.RateDownload
		up += st.RateUpload
	}
	current, cumulative := s.engine.SessionStats()
	stats := func(c engine.CumulativeStats) map[string]any {
		return map[string]any{
			"uploadedBytes":   c.UploadedBytes,
			"downloadedBytes": c.DownloadedBytes,
			"filesAdded":      c.FilesAdded,
			"sessionCount":    c.SessionCount,
			"secondsActive":   c.SecondsActive,
		}
	}
	return map[string]any{
		"activeTorrentCount": active,
		"downloadSpeed":      down,
		"pausedTorrentCount": paused,
		"torrentCount":       len(list),
		"uploadSpeed":        up,
		"cumulative-stats":   stats(cumulative),
		"current-stats":      stats(current),
	}, nil
}

// freeSpace implements "free-space".
func (s *Server) freeSpace(args map[string]any) (map[string]any, error) {
	path, _ := args["path"].(string)
	if path == "" {
		return nil, errors.New("directory path argument is missing")
	}
	free, total, err := engine.FreeSpace(path)
	if err != nil {
		return nil, fmt.Errorf("free-space: %v", err)
	}
	return map[string]any{"path": path, "size-bytes": free, "total_size": total}, nil
}

// portTest implements "port-test" by asking an external checker whether the peer port is
// reachable from the Internet.
func (s *Server) portTest(map[string]any) (map[string]any, error) {
	open, err := s.opts.PortChecker(s.engine.Port)
	if err != nil {
		return nil, fmt.Errorf("couldn't test port: %v", err)
	}
	return map[string]any{"port-is-open": open}, nil
}

// checkPortOnline uses Transmission's public port checker, which replies "1" or "0".
func checkPortOnline(port uint16) (bool, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Get(fmt.Sprintf("https://portcheck.transmissionbt.com/%d", port))
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16))
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(string(body)) == "1", nil
}
