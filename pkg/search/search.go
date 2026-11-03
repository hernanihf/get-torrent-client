// Package search provides indexer search capabilities and magnet generation.
package search

import (
	"context"
	"fmt"
	"net/url"
)

// DefaultTrackers contains reliable public trackers to bundle into generated magnet links.
var DefaultTrackers = []string{
	"udp://tracker.opentrackr.org:1337/announce",
	"udp://open.stealth.si:80/announce",
	"udp://tracker.torrent.eu.org:451/announce",
	"udp://tracker.bittorrent.com:80/announce",
	"udp://explodie.org:6969/announce",
}

// Result represents an individual torrent search match.
type Result struct {
	Name       string `json:"name"`
	InfoHash   string `json:"infoHash"`
	MagnetLink string `json:"magnetLink"`
	Size       int64  `json:"size"`
	Seeders    int    `json:"seeders"`
	Leechers   int    `json:"leechers"`
	Category   string `json:"category"`
	Provider   string `json:"provider"`
}

// Provider defines the interface for a torrent search indexer or scraper.
type Provider interface {
	Name() string
	Search(ctx context.Context, query string) ([]Result, error)
}

// BuildMagnet constructs a magnet URI from an info hash, title, and tracker list.
func BuildMagnet(infoHash, name string, trackers []string) string {
	if len(trackers) == 0 {
		trackers = DefaultTrackers
	}
	trParams := ""
	for _, tr := range trackers {
		trParams += "&tr=" + url.QueryEscape(tr)
	}
	return fmt.Sprintf("magnet:?xt=urn:btih:%s&dn=%s%s", infoHash, url.QueryEscape(name), trParams)
}

// Manager aggregates multiple search providers and executes searches across them.
type Manager struct {
	providers []Provider
}

// NewManager creates a search manager with the given providers.
func NewManager(providers ...Provider) *Manager {
	if len(providers) == 0 {
		providers = []Provider{NewPirateBayProvider("")}
	}
	return &Manager{providers: providers}
}

// Search queries all registered providers concurrently and aggregates the results.
func (m *Manager) Search(ctx context.Context, query string) ([]Result, error) {
	if len(m.providers) == 1 {
		return m.providers[0].Search(ctx, query)
	}

	type searchOutcome struct {
		results []Result
		err     error
	}

	ch := make(chan searchOutcome, len(m.providers))
	for _, p := range m.providers {
		go func(prov Provider) {
			res, err := prov.Search(ctx, query)
			ch <- searchOutcome{results: res, err: err}
		}(p)
	}

	var all []Result
	var lastErr error
	for i := 0; i < len(m.providers); i++ {
		out := <-ch
		if out.err != nil {
			lastErr = out.err
		} else {
			all = append(all, out.results...)
		}
	}

	if len(all) == 0 && lastErr != nil {
		return nil, lastErr
	}
	return all, nil
}
