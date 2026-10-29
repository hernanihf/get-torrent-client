package rpc

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"mime"
	"net/url"
	"path"
	"sort"
	"strings"

	"github.com/hernan/get-torrent/pkg/engine"
	"github.com/hernan/get-torrent/pkg/storage"
)

// Transmission torrent status codes (tr_torrent_activity). We never queue, so the "wait"
// states (1 check-wait, 3 download-wait, 5 seed-wait) are not used.
const (
	statusStopped  = 0
	statusCheck    = 2
	statusDownload = 4
	statusSeed     = 6
)

// Sentinel values used by Transmission for eta and uploadRatio.
const (
	etaNotAvailable = -1
	etaUnknown      = -2
	ratioNA         = -1.0
	ratioInf        = -2.0
)

func statusCode(s engine.TorrentState) int {
	switch s {
	case engine.StateDownloading:
		return statusDownload
	case engine.StateSeeding:
		return statusSeed
	case engine.StateChecking:
		return statusCheck
	default: // stopped, queued (added paused) and error torrents are not running
		return statusStopped
	}
}

// fieldCtx carries one torrent's snapshot while fields are rendered; expensive parts are
// computed at most once, and only if a requested field needs them.
type fieldCtx struct {
	t        *engine.ManagedTorrent
	st       engine.TorrentStatus
	queuePos int

	files    []engine.FileStatus
	snap     *storage.Snapshot
	trackers []engine.TrackerStatus
}

func (c *fieldCtx) fileList() []engine.FileStatus {
	if c.files == nil {
		c.files = c.t.Files()
	}
	return c.files
}

func (c *fieldCtx) snapshot() *storage.Snapshot {
	if c.snap == nil {
		snap := c.t.PieceSnapshot()
		c.snap = &snap
	}
	return c.snap
}

func (c *fieldCtx) trackerList() []engine.TrackerStatus {
	if c.trackers == nil {
		c.trackers = c.t.TrackerList()
	}
	return c.trackers
}

func (c *fieldCtx) pieceLength(i int) int64 {
	if i == c.st.PieceCount-1 {
		return c.st.TotalSize - int64(i)*c.st.PieceSize
	}
	return c.st.PieceSize
}

// torrentFields renders every supported legacy torrent-get field (Transmission 4.0.x names).
var torrentFields = map[string]func(c *fieldCtx) any{
	"activityDate": func(c *fieldCtx) any { return c.st.ActivityDate },
	"addedDate":    func(c *fieldCtx) any { return c.st.AddedDate },
	"availability": func(c *fieldCtx) any {
		snap := c.snapshot()
		out := make([]int, len(snap.Availability))
		for i, n := range snap.Availability {
			out[i] = n
			if snap.Have.HasPiece(i) {
				out[i] = -1
			}
		}
		return out
	},
	"bandwidthPriority": func(c *fieldCtx) any { return 0 },
	"comment":           func(c *fieldCtx) any { return c.st.Comment },
	"corruptEver":       func(c *fieldCtx) any { return c.st.CorruptEver },
	"creator":           func(c *fieldCtx) any { return c.st.Creator },
	"dateCreated":       func(c *fieldCtx) any { return c.st.DateCreated },
	"desiredAvailable": func(c *fieldCtx) any {
		snap := c.snapshot()
		var n int64
		for i, avail := range snap.Availability {
			if avail > 0 && !snap.Have.HasPiece(i) && (snap.Wanted == nil || snap.Wanted[i]) {
				n += c.pieceLength(i)
			}
		}
		return n
	},
	"doneDate":        func(c *fieldCtx) any { return c.st.DoneDate },
	"downloadDir":     func(c *fieldCtx) any { return c.st.DownloadDir },
	"downloadedEver":  func(c *fieldCtx) any { return c.st.Downloaded },
	"downloadLimit":   func(c *fieldCtx) any { return c.t.Limits().DownloadLimit },
	"downloadLimited": func(c *fieldCtx) any { return c.t.Limits().DownloadLimited },
	"editDate":        func(c *fieldCtx) any { return 0 },
	"error":           func(c *fieldCtx) any { return c.st.ErrorKind },
	"errorString":     func(c *fieldCtx) any { return c.st.Error },
	"eta": func(c *fieldCtx) any {
		if c.st.MetadataPercentComplete < 1 {
			return etaUnknown // size unknown until the magnet's metadata arrives
		}
		if c.st.State != engine.StateDownloading {
			return etaNotAvailable
		}
		if c.st.RateDownload <= 0 {
			return etaUnknown
		}
		return c.st.Left / c.st.RateDownload
	},
	"etaIdle":    func(c *fieldCtx) any { return etaNotAvailable },
	"file-count": func(c *fieldCtx) any { return len(c.fileList()) },
	"files": func(c *fieldCtx) any {
		out := make([]map[string]any, 0, len(c.fileList()))
		for _, f := range c.fileList() {
			out = append(out, map[string]any{"bytesCompleted": f.BytesCompleted, "length": f.Length, "name": f.Name})
		}
		return out
	},
	"fileStats": func(c *fieldCtx) any {
		out := make([]map[string]any, 0, len(c.fileList()))
		for _, f := range c.fileList() {
			out = append(out, map[string]any{"bytesCompleted": f.BytesCompleted, "wanted": f.Wanted, "priority": f.Priority})
		}
		return out
	},
	"group":               func(c *fieldCtx) any { return "" },
	"hashString":          func(c *fieldCtx) any { return hex.EncodeToString(c.st.InfoHash[:]) },
	"haveUnchecked":       func(c *fieldCtx) any { return 0 },
	"haveValid":           func(c *fieldCtx) any { return c.st.HaveValid },
	"honorsSessionLimits": func(c *fieldCtx) any { return c.t.Limits().HonorsSessionLimits },
	"id":                  func(c *fieldCtx) any { return c.st.ID },
	"isFinished":          func(c *fieldCtx) any { return false }, // no seed ratio/idle limits yet
	"isPrivate":           func(c *fieldCtx) any { return c.st.Private },
	"isStalled":           func(c *fieldCtx) any { return false },
	"labels":              func(c *fieldCtx) any { return []string{} },
	"leftUntilDone":       func(c *fieldCtx) any { return c.st.Left },
	"magnetLink": func(c *fieldCtx) any {
		if c.st.MagnetURI != "" {
			return c.st.MagnetURI
		}
		link := "magnet:?xt=urn:btih:" + hex.EncodeToString(c.st.InfoHash[:]) + "&dn=" + url.QueryEscape(c.st.Name)
		for _, tr := range c.trackerList() {
			link += "&tr=" + url.QueryEscape(tr.Announce)
		}
		return link
	},
	"manualAnnounceTime":      func(c *fieldCtx) any { return -1 },
	"maxConnectedPeers":       func(c *fieldCtx) any { return engine.MaxPeersPerTorrent },
	"metadataPercentComplete": func(c *fieldCtx) any { return c.st.MetadataPercentComplete },
	"name":                    func(c *fieldCtx) any { return c.st.Name },
	"peer-limit":              func(c *fieldCtx) any { return engine.MaxPeersPerTorrent },
	"peers": func(c *fieldCtx) any {
		peers := c.t.PeerList()
		out := make([]map[string]any, 0, len(peers))
		for _, p := range peers {
			out = append(out, map[string]any{
				"address":            p.Address,
				"clientName":         p.ClientName,
				"clientIsChoked":     p.ClientIsChoked,
				"clientIsInterested": p.ClientIsInterested,
				"flagStr":            flagStr(p),
				"isDownloadingFrom":  p.RateToClient > 0,
				"isEncrypted":        false,
				"isIncoming":         p.IsIncoming,
				"isUploadingTo":      p.RateToPeer > 0,
				"isUTP":              false,
				"peerIsChoked":       p.PeerIsChoked,
				"peerIsInterested":   p.PeerIsInterested,
				"port":               p.Port,
				"progress":           p.Progress,
				"rateToClient":       p.RateToClient,
				"rateToPeer":         p.RateToPeer,
			})
		}
		return out
	},
	"peersConnected": func(c *fieldCtx) any { return c.st.ActivePeers },
	"peersFrom": func(c *fieldCtx) any {
		return map[string]any{
			"fromCache":    0,
			"fromDht":      c.st.PeersFrom[engine.SourceDHT],
			"fromIncoming": c.st.PeersFrom[engine.SourceIncoming],
			"fromLpd":      0,
			"fromLtep":     0,
			"fromPex":      0,
			"fromTracker":  c.st.PeersFrom[engine.SourceTracker],
		}
	},
	"peersGettingFromUs": func(c *fieldCtx) any { return c.st.PeersGettingFromUs },
	"peersSendingToUs":   func(c *fieldCtx) any { return c.st.PeersSendingToUs },
	"percentComplete":    func(c *fieldCtx) any { return c.st.PercentComplete },
	"percentDone":        func(c *fieldCtx) any { return c.st.PercentDone }, // of the wanted files
	"pieces": func(c *fieldCtx) any {
		return base64.StdEncoding.EncodeToString(c.snapshot().Have)
	},
	"pieceCount": func(c *fieldCtx) any { return c.st.PieceCount },
	"pieceSize":  func(c *fieldCtx) any { return c.st.PieceSize },
	"priorities": func(c *fieldCtx) any {
		out := make([]int, 0, len(c.fileList()))
		for _, f := range c.fileList() {
			out = append(out, int(f.Priority))
		}
		return out
	},
	"primary-mime-type": func(c *fieldCtx) any {
		var largest engine.FileStatus
		for _, f := range c.fileList() {
			if f.Length > largest.Length {
				largest = f
			}
		}
		if t := mime.TypeByExtension(path.Ext(largest.Name)); t != "" {
			return strings.SplitN(t, ";", 2)[0]
		}
		return "application/octet-stream"
	},
	"queuePosition":      func(c *fieldCtx) any { return c.queuePos },
	"rateDownload":       func(c *fieldCtx) any { return c.st.RateDownload },
	"rateUpload":         func(c *fieldCtx) any { return c.st.RateUpload },
	"recheckProgress":    func(c *fieldCtx) any { return c.st.RecheckProgress },
	"secondsDownloading": func(c *fieldCtx) any { return c.st.SecondsDownloading },
	"secondsSeeding":     func(c *fieldCtx) any { return c.st.SecondsSeeding },
	"seedIdleLimit":      func(c *fieldCtx) any { return 30 },
	"seedIdleMode":       func(c *fieldCtx) any { return 0 }, // follow session settings
	"seedRatioLimit":     func(c *fieldCtx) any { return 2.0 },
	"seedRatioMode":      func(c *fieldCtx) any { return 0 },
	"sizeWhenDone":       func(c *fieldCtx) any { return c.st.SizeWhenDone },
	"startDate":          func(c *fieldCtx) any { return c.st.StartDate },
	"status":             func(c *fieldCtx) any { return statusCode(c.st.State) },
	"trackers": func(c *fieldCtx) any {
		out := make([]map[string]any, 0, len(c.trackerList()))
		for _, tr := range c.trackerList() {
			out = append(out, map[string]any{
				"announce": tr.Announce, "id": tr.ID, "scrape": "", "sitename": sitename(tr.Announce), "tier": tr.Tier,
			})
		}
		return out
	},
	"trackerList": func(c *fieldCtx) any {
		// One URL per line; tiers separated by a blank line.
		var b strings.Builder
		for i, tr := range c.trackerList() {
			if i > 0 {
				b.WriteString("\n")
				if tr.Tier != c.trackerList()[i-1].Tier {
					b.WriteString("\n")
				}
			}
			b.WriteString(tr.Announce)
		}
		return b.String()
	},
	"trackerStats": func(c *fieldCtx) any {
		out := make([]map[string]any, 0, len(c.trackerList()))
		for _, tr := range c.trackerList() {
			host := tr.Announce
			if u, err := url.Parse(tr.Announce); err == nil && u.Host != "" {
				host = u.Scheme + "://" + u.Host
			}
			announceState := 0 // inactive
			if c.st.State == engine.StateDownloading || c.st.State == engine.StateSeeding {
				announceState = 1 // waiting
			}
			out = append(out, map[string]any{
				"announce": tr.Announce, "announceState": announceState, "downloadCount": -1,
				"hasAnnounced": tr.HasAnnounced, "hasScraped": false, "host": host, "id": tr.ID,
				"isBackup": false, "lastAnnouncePeerCount": tr.LastAnnouncePeerCount,
				"lastAnnounceResult": tr.LastAnnounceResult, "lastAnnounceStartTime": tr.LastAnnounceTime,
				"lastAnnounceSucceeded": tr.LastAnnounceSucceeded, "lastAnnounceTime": tr.LastAnnounceTime,
				"lastAnnounceTimedOut": strings.Contains(tr.LastAnnounceResult, "timeout"),
				"lastScrapeResult":     "", "lastScrapeStartTime": 0, "lastScrapeSucceeded": false,
				"lastScrapeTime": 0, "lastScrapeTimedOut": false, "leecherCount": tr.LeecherCount,
				"nextAnnounceTime": tr.NextAnnounceTime, "nextScrapeTime": 0, "scrape": "",
				"scrapeState": 0, "seederCount": tr.SeederCount, "sitename": sitename(tr.Announce), "tier": tr.Tier,
			})
		}
		return out
	},
	"totalSize":     func(c *fieldCtx) any { return c.st.TotalSize },
	"torrentFile":   func(c *fieldCtx) any { return c.st.TorrentFile },
	"uploadedEver":  func(c *fieldCtx) any { return c.st.Uploaded },
	"uploadLimit":   func(c *fieldCtx) any { return c.t.Limits().UploadLimit },
	"uploadLimited": func(c *fieldCtx) any { return c.t.Limits().UploadLimited },
	"uploadRatio": func(c *fieldCtx) any {
		// Transmission divides by downloadedEver, falling back to verified data (e.g. seeding
		// a torrent whose data was already present).
		down := c.st.Downloaded
		if down == 0 {
			down = c.st.HaveValid
		}
		switch {
		case down > 0:
			return float64(c.st.Uploaded) / float64(down)
		case c.st.Uploaded > 0:
			return ratioInf
		default:
			return ratioNA
		}
	},
	"wanted": func(c *fieldCtx) any {
		// 0/1 integers rather than booleans: Transmission 4.0.2 restored this for compatibility.
		out := make([]int, 0, len(c.fileList()))
		for _, f := range c.fileList() {
			if f.Wanted {
				out = append(out, 1)
			} else {
				out = append(out, 0)
			}
		}
		return out
	},
	"webseeds":            func(c *fieldCtx) any { return []string{} },
	"webseedsSendingToUs": func(c *fieldCtx) any { return 0 },
}

// allTorrentFields lists every supported field, sorted, for requests without "fields".
var allTorrentFields = func() []string {
	names := make([]string, 0, len(torrentFields))
	for name := range torrentFields {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}()

// flagStr renders Transmission's per-peer status letters.
func flagStr(p engine.PeerStatus) string {
	var b strings.Builder
	switch {
	case p.RateToClient > 0:
		b.WriteByte('D') // downloading from this peer
	case p.ClientIsInterested && !p.ClientIsChoked:
		b.WriteByte('d') // we would download, but nothing is flowing
	}
	switch {
	case p.RateToPeer > 0:
		b.WriteByte('U')
	case p.PeerIsInterested && !p.PeerIsChoked:
		b.WriteByte('u')
	}
	if !p.ClientIsChoked && !p.ClientIsInterested {
		b.WriteByte('K') // peer unchoked us, but we are not interested
	}
	if !p.PeerIsChoked && !p.PeerIsInterested {
		b.WriteByte('?') // we unchoked the peer, but it is not interested
	}
	if p.IsIncoming {
		b.WriteByte('I')
	}
	if p.Source == engine.SourceDHT {
		b.WriteByte('H')
	}
	return b.String()
}

// sitename derives Transmission's short tracker name: the label before the public suffix
// ("tracker.example.org" -> "example").
func sitename(announce string) string {
	u, err := url.Parse(announce)
	if err != nil || u.Hostname() == "" {
		return ""
	}
	labels := strings.Split(u.Hostname(), ".")
	if len(labels) >= 2 {
		return labels[len(labels)-2]
	}
	return labels[0]
}

func sortedByID(list []*engine.ManagedTorrent) []*engine.ManagedTorrent {
	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })
	return list
}

// torrentGet implements "torrent-get", including the "table" response format (rpc-version 16).
func (s *Server) torrentGet(args map[string]any) (map[string]any, error) {
	sel, err := s.selectTorrents(args)
	if err != nil {
		return nil, err
	}

	fields := allTorrentFields
	if raw, ok := args["fields"].([]any); ok {
		fields = make([]string, 0, len(raw))
		for _, f := range raw {
			// Unknown fields are ignored, as in Transmission.
			if name, ok := f.(string); ok && torrentFields[name] != nil {
				fields = append(fields, name)
			}
		}
	}

	// Queue position follows ID order across all torrents.
	queuePos := make(map[int]int)
	for i, t := range sortedByID(s.engine.ListTorrents()) {
		queuePos[t.ID] = i
	}

	table, _ := args["format"].(string)
	var objects []map[string]any
	var rows [][]any
	if table == "table" {
		header := make([]any, len(fields))
		for i, f := range fields {
			header[i] = f
		}
		rows = append(rows, header)
	}

	for _, t := range sel.torrents {
		ctx := &fieldCtx{t: t, st: t.Status(), queuePos: queuePos[t.ID]}
		if table == "table" {
			row := make([]any, len(fields))
			for i, f := range fields {
				row[i] = torrentFields[f](ctx)
			}
			rows = append(rows, row)
		} else {
			obj := make(map[string]any, len(fields))
			for _, f := range fields {
				obj[f] = torrentFields[f](ctx)
			}
			objects = append(objects, obj)
		}
	}

	out := map[string]any{}
	if table == "table" {
		out["torrents"] = rows
	} else {
		if objects == nil {
			objects = []map[string]any{}
		}
		out["torrents"] = objects
	}
	if sel.recentOnly {
		removed := sel.removed
		if removed == nil {
			removed = []int{}
		}
		out["removed"] = removed
	}
	return out, nil
}

// torrentAdd implements "torrent-add": "filename" may be a local .torrent path, an http(s) URL
// or a magnet URI, and "metainfo" a base64-encoded .torrent. Torrents start immediately unless
// "paused" is true. Re-adding an existing torrent reports "torrent-duplicate".
func (s *Server) torrentAdd(args map[string]any) (map[string]any, error) {
	req := engine.AddRequest{}
	req.Filename, _ = args["filename"].(string)
	req.DownloadDir, _ = args["download-dir"].(string)
	if b64, _ := args["metainfo"].(string); b64 != "" && req.Filename == "" {
		data, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return nil, fmt.Errorf("invalid or corrupt torrent file")
		}
		req.Metainfo = data
	}
	if req.Filename == "" && len(req.Metainfo) == 0 {
		return nil, fmt.Errorf("no filename or metainfo specified")
	}

	t, added, err := s.engine.Add(req)
	if err != nil {
		return nil, fmt.Errorf("invalid or corrupt torrent file: %v", err)
	}

	st := t.Status()
	info := map[string]any{"id": st.ID, "name": st.Name, "hashString": hex.EncodeToString(st.InfoHash[:])}
	if !added {
		return map[string]any{"torrent-duplicate": info}, nil
	}

	if hasFileArgs(args) && st.MetadataPercentComplete == 1 {
		// Magnets learn their file list later; Transmission ignores these for them too.
		if err := s.engine.SetFileSelection(t.ID, func(fs *engine.FileSelection) error {
			return applyFileArgs(args, fs)
		}); err != nil {
			_ = s.engine.RemoveTorrent(t.ID, false)
			return nil, err
		}
	}
	if paused, _ := args["paused"].(bool); !paused {
		_ = s.engine.StartTorrent(t.ID)
	}
	_ = s.engine.SaveState()
	return map[string]any{"torrent-added": info}, nil
}
