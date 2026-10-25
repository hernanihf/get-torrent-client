package magnet

import (
	"encoding/base32"
	"encoding/hex"
	"testing"
)

func TestParseHexMagnet(t *testing.T) {
	uri := "magnet:?xt=urn:btih:c12fe1c06bba254a9dc9f519b335380dc102e940&dn=Test+Torrent&tr=udp%3A%2F%2Ftracker.example.com%3A80&tr=http%3A%2F%2Ftracker.example.com%2Fannounce"

	mag, err := Parse(uri)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	expectedHex := "c12fe1c06bba254a9dc9f519b335380dc102e940"
	gotHex := hex.EncodeToString(mag.InfoHash[:])
	if gotHex != expectedHex {
		t.Errorf("expected hash %s, got %s", expectedHex, gotHex)
	}

	if mag.DisplayName != "Test Torrent" {
		t.Errorf("expected dn 'Test Torrent', got %q", mag.DisplayName)
	}

	if len(mag.Trackers) != 2 {
		t.Fatalf("expected 2 trackers, got %d", len(mag.Trackers))
	}
	if mag.Trackers[0] != "udp://tracker.example.com:80" {
		t.Errorf("unexpected tracker[0]: %s", mag.Trackers[0])
	}
	if mag.Trackers[1] != "http://tracker.example.com/announce" {
		t.Errorf("unexpected tracker[1]: %s", mag.Trackers[1])
	}
}

func TestParseBase32Magnet(t *testing.T) {
	expectedHex := "c12fe1c06bba254a9dc9f519b335380dc102e940"
	hashBytes, _ := hex.DecodeString(expectedHex)
	b32Str := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(hashBytes)

	uri := "magnet:?xt=urn:btih:" + b32Str + "&dn=Base32+Torrent"

	mag, err := Parse(uri)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	gotHex := hex.EncodeToString(mag.InfoHash[:])
	if gotHex != expectedHex {
		t.Errorf("expected hash %s, got %s", expectedHex, gotHex)
	}

	if mag.DisplayName != "Base32 Torrent" {
		t.Errorf("expected dn 'Base32 Torrent', got %q", mag.DisplayName)
	}
}

func TestParseInvalidMagnets(t *testing.T) {
	cases := []string{
		"http://example.com",
		"magnet:?dn=No+XT",
		"magnet:?xt=urn:sha1:c12fe1c06bba254a9dc9f519b335380dc102e940",
		"magnet:?xt=urn:btih:short",
		"magnet:?xt=urn:btih:zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
	}

	for _, c := range cases {
		_, err := Parse(c)
		if err == nil {
			t.Errorf("expected error for magnet %q, got nil", c)
		}
	}
}
