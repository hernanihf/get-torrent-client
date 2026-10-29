package peer

import (
	"net"
	"strings"
	"testing"

	"github.com/hernan/get-torrent/pkg/bencode"
	"github.com/hernan/get-torrent/pkg/tracker"
)

func TestPexRoundTrip(t *testing.T) {
	added := []tracker.Peer{
		{IP: net.IPv4(10, 0, 0, 1), Port: 6881},
		{IP: net.ParseIP("2001:db8::1"), Port: 51413},
	}
	dropped := []tracker.Peer{{IP: net.IPv4(10, 0, 0, 2), Port: 1}}
	msg := FormatPex(7, added, []byte{PexReachable, PexSeed}, dropped)
	extID, payload, err := ParseExtended(msg)
	if err != nil || extID != 7 {
		t.Fatalf("extended id %d, err %v", extID, err)
	}
	got, err := ParsePex(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Added) != 2 || got.Added[0].String() != "10.0.0.1:6881" || got.Added[1].String() != "[2001:db8::1]:51413" {
		t.Errorf("added = %v", got.Added)
	}
	if got.AddedFlags[0] != PexReachable || got.AddedFlags[1] != PexSeed {
		t.Errorf("flags = %v", got.AddedFlags)
	}
	if len(got.Dropped) != 1 || got.Dropped[0].String() != "10.0.0.2:1" {
		t.Errorf("dropped = %v", got.Dropped)
	}
}

func TestPexLimitsAndValidation(t *testing.T) {
	var many []tracker.Peer
	for i := range 80 {
		many = append(many, tracker.Peer{IP: net.IPv4(10, 0, 1, byte(i)), Port: 1000})
	}
	_, payload, _ := ParseExtended(FormatPex(1, many, nil, nil))
	got, err := ParsePex(payload)
	if err != nil || len(got.Added) != MaxPexPeers {
		t.Errorf("sent lists must be capped at %d, got %d (%v)", MaxPexPeers, len(got.Added), err)
	}

	// Received lists are capped too, and zero ports are skipped.
	raw := strings.Repeat("\x0a\x00\x00\x01\x1a\xe1", 70) + "\x0a\x00\x00\x02\x00\x00"
	enc, _ := bencode.Encode(map[string]any{"added": raw})
	if got, err := ParsePex(enc); err != nil || len(got.Added) != MaxPexPeers {
		t.Errorf("received lists must be capped: %d (%v)", len(got.Added), err)
	}
	enc, _ = bencode.Encode(map[string]any{"added": "\x0a\x00\x00\x02\x00\x00"})
	if got, err := ParsePex(enc); err != nil || len(got.Added) != 0 {
		t.Errorf("zero port must be skipped: %v (%v)", got.Added, err)
	}

	for _, bad := range []map[string]any{{"added": "12345"}, {"dropped6": "x"}} {
		enc, _ := bencode.Encode(bad)
		if _, err := ParsePex(enc); err == nil {
			t.Errorf("partial entry %v must be rejected", bad)
		}
	}
	if _, err := ParsePex([]byte("le")); err == nil {
		t.Errorf("non-dictionary must be rejected")
	}
}

func TestExtHandshakeAdvertisesPexAndPort(t *testing.T) {
	_, payload, _ := ParseExtended(FormatExtHandshakeWith(ExtHandshakeOptions{ListenPort: 51413, PEX: true}))
	hs, err := ParseExtHandshake(payload)
	if err != nil {
		t.Fatal(err)
	}
	if hs.M[ExtUTPex] != LocalUTPexID || hs.ListenPort != 51413 {
		t.Errorf("unexpected handshake: %+v", hs)
	}
	_, payload, _ = ParseExtended(FormatExtHandshakeWith(ExtHandshakeOptions{}))
	if hs, _ := ParseExtHandshake(payload); hs.M[ExtUTPex] != 0 || hs.ListenPort != 0 {
		t.Errorf("ut_pex and p must be omitted when off: %+v", hs)
	}
}
