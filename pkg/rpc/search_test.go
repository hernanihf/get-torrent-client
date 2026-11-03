package rpc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hernanihf/get-torrent-client/pkg/search"
)

type mockSearchProvider struct {
	results []search.Result
	err     error
}

func (m *mockSearchProvider) Name() string { return "MockIndexer" }
func (m *mockSearchProvider) Search(ctx context.Context, query string) ([]search.Result, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.results, nil
}

func TestRPCTorrentSearch(t *testing.T) {
	mockResults := []search.Result{
		{
			Name:       "Ubuntu 24.04 Desktop ISO",
			InfoHash:   "0123456789abcdef0123456789abcdef01234567",
			MagnetLink: "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=Ubuntu",
			Size:       4500000000,
			Seeders:    500,
			Leechers:   30,
			Category:   "Applications",
			Provider:   "MockIndexer",
		},
	}

	customMgr := search.NewManager(&mockSearchProvider{results: mockResults})
	env := newEnv(t, Options{
		SearchManager: customMgr,
	})

	// 1. Search with empty query -> returns empty results without calling provider
	reqEmpty := httptest.NewRequest(http.MethodPost, rpcURL, strings.NewReader(`{"method":"torrent-search","arguments":{"query":""}}`))
	reqEmpty.Header.Set(HeaderSessionID, env.server.sessionID)
	wEmpty := httptest.NewRecorder()
	env.server.Handler().ServeHTTP(wEmpty, reqEmpty)

	respEmpty := decodeResponse(t, wEmpty)
	if respEmpty.Result != "success" {
		t.Fatalf("expected success, got %q", respEmpty.Result)
	}
	resEmptyList, ok := respEmpty.Arguments["results"].([]any)
	if !ok || len(resEmptyList) != 0 {
		t.Errorf("expected 0 results for empty query, got %v", resEmptyList)
	}

	// 2. Search with query "ubuntu" -> returns mock results
	reqSearch := httptest.NewRequest(http.MethodPost, rpcURL, strings.NewReader(`{"method":"torrent-search","arguments":{"query":"ubuntu"}}`))
	reqSearch.Header.Set(HeaderSessionID, env.server.sessionID)
	wSearch := httptest.NewRecorder()
	env.server.Handler().ServeHTTP(wSearch, reqSearch)

	respSearch := decodeResponse(t, wSearch)
	if respSearch.Result != "success" {
		t.Fatalf("expected success, got %q", respSearch.Result)
	}
	resultsList, ok := respSearch.Arguments["results"].([]any)
	if !ok || len(resultsList) != 1 {
		t.Fatalf("expected 1 result, got %v", resultsList)
	}

	item := resultsList[0].(map[string]any)
	if item["name"] != "Ubuntu 24.04 Desktop ISO" || item["seeders"] != float64(500) {
		t.Errorf("unexpected search item payload: %+v", item)
	}
}
