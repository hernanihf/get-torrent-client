package search

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newTestClient(fn roundTripFunc) *http.Client {
	return &http.Client{
		Transport: fn,
	}
}

func TestBuildMagnet(t *testing.T) {
	hash := "c1229279831a16a7fc518427da4ea3477b224b0c"
	name := "Ubuntu 22.04 LTS"
	magnet := BuildMagnet(hash, name, []string{"udp://tracker.example.com:1337/announce"})

	if !strings.HasPrefix(magnet, "magnet:?xt=urn:btih:"+hash) {
		t.Errorf("expected magnet to contain info_hash, got %q", magnet)
	}
	if !strings.Contains(magnet, "dn=Ubuntu+22.04+LTS") && !strings.Contains(magnet, "dn=Ubuntu%2022.04%20LTS") {
		t.Errorf("expected magnet to contain encoded dn, got %q", magnet)
	}
	if !strings.Contains(magnet, "tracker.example.com") {
		t.Errorf("expected tracker in magnet, got %q", magnet)
	}
}

func TestPirateBayProviderSearch(t *testing.T) {
	mockResponse := []apibayItem{
		{
			ID:       "1001",
			Name:     "Debian 12 Netinst",
			InfoHash: "0123456789abcdef0123456789abcdef01234567",
			Leechers: "5",
			Seeders:  "120",
			Size:     "654321000",
			Category: "300",
		},
		{
			ID:       "1002",
			Name:     "Arch Linux",
			InfoHash: "abcdef0123456789abcdef0123456789abcdef01",
			Leechers: 2,
			Seeders:  85,
			Size:     float64(900000000),
			Category: 300,
		},
	}

	rawJSON, _ := json.Marshal(mockResponse)
	client := newTestClient(func(req *http.Request) (*http.Response, error) {
		q := req.URL.Query().Get("q")
		if q != "linux" {
			t.Errorf("expected query 'linux', got %q", q)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewReader(rawJSON)),
		}, nil
	})

	prov := NewPirateBayProvider("http://mock.apibay.org")
	prov.SetHTTPClient(client)

	results, err := prov.Search(context.Background(), "linux")
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	if results[0].Name != "Debian 12 Netinst" || results[0].Seeders != 120 || results[0].Category != "Applications" {
		t.Errorf("unexpected first result: %+v", results[0])
	}
	if !strings.HasPrefix(results[0].MagnetLink, "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567") {
		t.Errorf("unexpected magnet link: %s", results[0].MagnetLink)
	}
}

func TestPirateBayProviderNoResults(t *testing.T) {
	mockEmpty := []apibayItem{
		{
			ID:       "0",
			Name:     "No results returned",
			InfoHash: "0000000000000000000000000000000000000000",
		},
	}

	rawJSON, _ := json.Marshal(mockEmpty)
	client := newTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewReader(rawJSON)),
		}, nil
	})

	prov := NewPirateBayProvider("http://mock.apibay.org")
	prov.SetHTTPClient(client)

	results, err := prov.Search(context.Background(), "nonexistentquery123")
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func TestManagerSearch(t *testing.T) {
	mockResp := []apibayItem{
		{
			ID:       "1",
			Name:     "Test Item",
			InfoHash: "1111111111111111111111111111111111111111",
			Seeders:  10,
			Leechers: 1,
			Size:     1024,
		},
	}
	rawJSON, _ := json.Marshal(mockResp)
	client := newTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewReader(rawJSON)),
		}, nil
	})

	p := NewPirateBayProvider("http://mock.apibay.org")
	p.SetHTTPClient(client)

	mgr := NewManager(p)
	results, err := mgr.Search(context.Background(), "test")
	if err != nil {
		t.Fatalf("manager search failed: %v", err)
	}
	if len(results) != 1 || results[0].Name != "Test Item" {
		t.Errorf("unexpected manager results: %+v", results)
	}
}
