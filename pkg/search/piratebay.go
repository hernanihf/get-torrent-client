package search

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const defaultPirateBayURL = "https://apibay.org"

// PirateBayProvider searches The Pirate Bay index using the apibay.org JSON API.
type PirateBayProvider struct {
	baseURL    string
	httpClient *http.Client
}

// NewPirateBayProvider constructs a PirateBayProvider with an optional proxy/mirror base URL.
func NewPirateBayProvider(baseURL string) *PirateBayProvider {
	if baseURL == "" {
		baseURL = defaultPirateBayURL
	}
	baseURL = strings.TrimRight(baseURL, "/")
	return &PirateBayProvider{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// SetHTTPClient configures a custom HTTP client (useful for tests and custom proxies).
func (p *PirateBayProvider) SetHTTPClient(client *http.Client) {
	if client != nil {
		p.httpClient = client
	}
}

// Name returns the provider identifier.
func (p *PirateBayProvider) Name() string {
	return "ThePirateBay"
}

// apibayItem matches the raw JSON object returned by apibay.org /q.php
type apibayItem struct {
	ID       any    `json:"id"`
	Name     string `json:"name"`
	InfoHash string `json:"info_hash"`
	Leechers any    `json:"leechers"`
	Seeders  any    `json:"seeders"`
	NumFiles any    `json:"num_files"`
	Size     any    `json:"size"`
	Username string `json:"username"`
	Added    any    `json:"added"`
	Status   string `json:"status"`
	Category any    `json:"category"`
}

func parseIntField(val any) int {
	switch v := val.(type) {
	case float64:
		return int(v)
	case int:
		return v
	case string:
		n, _ := strconv.Atoi(v)
		return n
	default:
		return 0
	}
}

func parseInt64Field(val any) int64 {
	switch v := val.(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	case string:
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	default:
		return 0
	}
}

func mapCategory(catID int) string {
	switch {
	case catID >= 100 && catID < 200:
		return "Audio"
	case catID >= 200 && catID < 300:
		return "Video"
	case catID >= 300 && catID < 400:
		return "Applications"
	case catID >= 400 && catID < 500:
		return "Games"
	case catID >= 500 && catID < 600:
		return "Porn"
	case catID >= 600:
		return "Other"
	default:
		return "General"
	}
}

// Search queries the apibay backend for torrents matching query.
func (p *PirateBayProvider) Search(ctx context.Context, query string) ([]Result, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return []Result{}, nil
	}

	searchURL := fmt.Sprintf("%s/q.php?q=%s&cat=", p.baseURL, url.QueryEscape(query))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, searchURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating search request: %w", err)
	}

	req.Header.Set("User-Agent", "get-torrent/1.0 (+https://github.com/hernan/get-torrent)")
	req.Header.Set("Accept", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing search: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("indexer returned HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading search response: %w", err)
	}

	var items []apibayItem
	if err := json.Unmarshal(body, &items); err != nil {
		return nil, fmt.Errorf("decoding search response: %w", err)
	}

	results := make([]Result, 0, len(items))
	for _, it := range items {
		hash := strings.ToLower(strings.TrimSpace(it.InfoHash))
		// APIBay returns a dummy item with info_hash 000... if nothing was found
		if hash == "" || hash == "0000000000000000000000000000000000000000" || it.Name == "No results returned" {
			continue
		}

		seeders := parseIntField(it.Seeders)
		leechers := parseIntField(it.Leechers)
		size := parseInt64Field(it.Size)
		catID := parseIntField(it.Category)

		magnet := BuildMagnet(hash, it.Name, DefaultTrackers)

		results = append(results, Result{
			Name:       it.Name,
			InfoHash:   hash,
			MagnetLink: magnet,
			Size:       size,
			Seeders:    seeders,
			Leechers:   leechers,
			Category:   mapCategory(catID),
			Provider:   p.Name(),
		})
	}

	return results, nil
}
