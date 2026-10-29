package rpc

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/hernan/get-torrent/pkg/engine"
)

// sessionGet implements "session-get", honouring the optional "fields" filter (rpc-version 16).
func (s *Server) sessionGet(args map[string]any) (map[string]any, error) {
	all := s.sessionValues()
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

// sessionValues returns every session-get key with its effective value.
func (s *Server) sessionValues() map[string]any {
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
		"encryption":                           "tolerated", // closest value: we only speak plaintext, and session-set rejects the others
		"idle-seeding-limit":                   settings.IdleSeedingLimit,
		"idle-seeding-limit-enabled":           settings.IdleSeedingLimitEnabled,
		"incomplete-dir":                       downloadDir,
		"incomplete-dir-enabled":               false,
		"lpd-enabled":                          false,
		"peer-limit-global":                    200,
		"peer-limit-per-torrent":               engine.MaxPeersPerTorrent,
		"peer-port":                            e.Port,
		"peer-port-random-on-start":            false,
		"pex-enabled":                          settings.PEXEnabled,
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
		"seedRatioLimit":                       settings.SeedRatioLimit,
		"seedRatioLimited":                     settings.SeedRatioLimited,
		"session-id":                           sessionID,
		"speed-limit-down":                     settings.SpeedLimitDown,
		"speed-limit-down-enabled":             settings.SpeedLimitDownEnabled,
		"speed-limit-up":                       settings.SpeedLimitUp,
		"speed-limit-up-enabled":               settings.SpeedLimitUpEnabled,
		"start-added-torrents":                 settings.StartAddedTorrents,
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
	return all
}

// settableSessionKeys are the session-set keys get-torrent applies.
var settableSessionKeys = map[string]bool{
	"download-dir": true, "speed-limit-down": true, "speed-limit-up": true,
	"speed-limit-down-enabled": true, "speed-limit-up-enabled": true,
	"alt-speed-down": true, "alt-speed-up": true, "alt-speed-enabled": true,
	"seedRatioLimit": true, "seedRatioLimited": true,
	"idle-seeding-limit": true, "idle-seeding-limit-enabled": true,
	"pex-enabled": true, "start-added-torrents": true,
}

// readOnlySessionKeys are session-get keys that clients cannot set; Transmission ignores them.
var readOnlySessionKeys = map[string]bool{
	"blocklist-size": true, "config-dir": true, "download-dir-free-space": true, "rpc-version": true,
	"rpc-version-minimum": true, "rpc-version-semver": true, "session-id": true, "units": true,
	"version": true,
}

// unsupportedSessionKeys lists, sorted, the session-set arguments that would change a setting
// get-torrent does not implement. Such a key is accepted when it repeats the value session-get
// reports, so clients that send their whole preferences dialog keep working.
func (s *Server) unsupportedSessionKeys(args map[string]any) []string {
	current := s.sessionValues()
	var bad []string
	for k, v := range args {
		reported, known := current[k]
		if !known || settableSessionKeys[k] || readOnlySessionKeys[k] {
			continue // unknown keys are ignored, as in Transmission
		}
		if !sameValue(v, reported) {
			bad = append(bad, k)
		}
	}
	sort.Strings(bad)
	return bad
}

// sameValue compares a decoded JSON argument with a reported value, treating all numbers alike.
func sameValue(arg, reported any) bool {
	a, argNum := toFloat(arg)
	b, repNum := toFloat(reported)
	if argNum || repNum {
		return argNum && repNum && a == b
	}
	switch reported.(type) {
	case bool, string:
		return arg == reported
	}
	return fmt.Sprint(arg) == fmt.Sprint(reported)
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint16:
		return float64(n), true
	}
	return 0, false
}

// sessionSet implements "session-set". Nothing is applied if any argument asks for a setting
// get-torrent does not support (see unsupportedSessionKeys); unknown keys are ignored.
func (s *Server) sessionSet(args map[string]any) (map[string]any, error) {
	if bad := s.unsupportedSessionKeys(args); len(bad) > 0 {
		return nil, fmt.Errorf("unsupported session settings: %s", strings.Join(bad, ", "))
	}
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
		if v, ok := args["seedRatioLimit"].(float64); ok {
			st.SeedRatioLimit = v
		}
		if v, ok := args["seedRatioLimited"].(bool); ok {
			st.SeedRatioLimited = v
		}
		if v, ok := args["idle-seeding-limit"].(float64); ok {
			st.IdleSeedingLimit = int(v)
		}
		if v, ok := args["idle-seeding-limit-enabled"].(bool); ok {
			st.IdleSeedingLimitEnabled = v
		}
		if v, ok := args["pex-enabled"].(bool); ok {
			st.PEXEnabled = v
		}
		if v, ok := args["start-added-torrents"].(bool); ok {
			st.StartAddedTorrents = v
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
