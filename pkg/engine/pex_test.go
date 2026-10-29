package engine

import (
	"crypto/rand"
	"crypto/sha1"
	"net"
	"testing"
	"time"

	"github.com/hernan/get-torrent/pkg/bencode"
	"github.com/hernan/get-torrent/pkg/metainfo"
)

func fastPex(t *testing.T) {
	t.Helper()
	tick, interval := peerTickInterval, pexInterval
	peerTickInterval, pexInterval = 50*time.Millisecond, 100*time.Millisecond
	t.Cleanup(func() { peerTickInterval, pexInterval = tick, interval })
}

// pexSwarm starts a seeder that a fake tracker hands out as the only peer, and returns the
// torrent file for leechers and its payload.
func pexSwarm(t *testing.T, private bool) ([]byte, []byte) {
	t.Helper()
	payload := make([]byte, 64*1024)
	_, _ = rand.Read(payload)
	info := buildTorrent(t, "pex.bin", payload, 16384)
	if private {
		info = buildPrivateTorrent(t, "pex.bin", payload, 16384)
	}
	meta, err := metainfo.FromInfoBytes(info, nil)
	if err != nil {
		t.Fatal(err)
	}
	seeder := newTestEngine(t)
	if err := seeder.StartListener(); err != nil {
		t.Fatal(err)
	}
	tr := fakeTracker(t, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: int(seeder.Port)})
	t.Cleanup(tr.Close)
	meta.Announce = tr.URL + "/announce"
	data, _ := meta.Marshal()
	st, err := seeder.AddTorrentBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	seedAll(t, st, payload)
	if err := seeder.StartTorrent(st.ID); err != nil {
		t.Fatal(err)
	}
	return data, payload
}

func startLeecher(t *testing.T, data []byte) (*Engine, *ManagedTorrent) {
	t.Helper()
	e := newTestEngine(t)
	if err := e.StartListener(); err != nil {
		t.Fatal(err)
	}
	mt, err := e.AddTorrentBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.StartTorrent(mt.ID); err != nil {
		t.Fatal(err)
	}
	return e, mt
}

func TestPexIntroducesLeechers(t *testing.T) {
	fastPex(t)
	data, _ := pexSwarm(t, false)
	_, l1 := startLeecher(t, data)
	waitFor(t, 5*time.Second, "first leecher connected", func() bool { return l1.Status().ActivePeers == 1 })
	_, l2 := startLeecher(t, data)

	// The tracker only knows the seeder: the leechers can only find each other through PEX.
	waitFor(t, 5*time.Second, "leechers connected via PEX", func() bool {
		return l1.Status().PeersFrom[SourcePEX]+l2.Status().PeersFrom[SourcePEX] > 0
	})
}

func TestPexDisabledForPrivateTorrents(t *testing.T) {
	fastPex(t)
	data, _ := pexSwarm(t, true)
	_, l1 := startLeecher(t, data)
	waitFor(t, 5*time.Second, "first leecher connected", func() bool { return l1.Status().ActivePeers == 1 })
	_, l2 := startLeecher(t, data)
	waitFor(t, 5*time.Second, "second leecher connected", func() bool { return l2.Status().ActivePeers >= 1 })
	time.Sleep(500 * time.Millisecond) // several PEX intervals
	if n := l1.Status().PeersFrom[SourcePEX] + l2.Status().PeersFrom[SourcePEX]; n != 0 {
		t.Errorf("private torrents must not use PEX, got %d PEX peers", n)
	}
}

func TestPexSessionSetting(t *testing.T) {
	e := newTestEngine(t)
	mt := addWithTracker(t, e, make([]byte, 1000), "http://127.0.0.1:1/announce")
	if !e.pexAllowed(mt) {
		t.Fatal("PEX must be on by default for public torrents")
	}
	e.UpdateSettings(func(s *Settings) { s.PEXEnabled = false })
	if e.pexAllowed(mt) {
		t.Error("pex-enabled=false must disable PEX")
	}
}

// buildPrivateTorrent is buildTorrent with the BEP 0027 private flag set.
func buildPrivateTorrent(t *testing.T, name string, payload []byte, pieceLen int) []byte {
	t.Helper()
	var pieces []byte
	for off := 0; off < len(payload); off += pieceLen {
		sum := sha1.Sum(payload[off:min(off+pieceLen, len(payload))])
		pieces = append(pieces, sum[:]...)
	}
	info, err := bencode.Encode(map[string]any{
		"name": name, "length": len(payload), "piece length": pieceLen, "pieces": string(pieces), "private": 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return info
}
