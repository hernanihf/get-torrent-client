package engine

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hernan/get-torrent/pkg/bencode"
	"github.com/hernan/get-torrent/pkg/dht"
	"github.com/hernan/get-torrent/pkg/metainfo"
	"github.com/hernan/get-torrent/pkg/peer"
)

// newDHTEngine creates an engine with a peer listener and a DHT node bootstrapped from boot.
func newDHTEngine(t *testing.T, boot *dht.Server) *Engine {
	t.Helper()
	eng := NewEngine(t.TempDir(), 0)
	eng.ConfigDir = t.TempDir()
	t.Cleanup(func() { _ = eng.Close() })
	if err := eng.StartListener(); err != nil {
		t.Fatal(err)
	}
	if err := eng.EnableDHT([]string{boot.Addr().String()}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "DHT bootstrap", func() bool { return eng.DHT.NumNodes() > 0 })
	return eng
}

func newBootNode(t *testing.T) *dht.Server {
	t.Helper()
	boot, err := dht.New(dht.Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = boot.Close() })
	return boot
}

func TestTrackerlessMagnetOverDHT(t *testing.T) {
	payload := make([]byte, 70_000)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	info := buildTorrent(t, "dht.bin", payload, 32768)
	infoHash := sha1.Sum(info)
	meta, _ := metainfo.FromInfoBytes(info, nil) // no trackers at all
	torrentFile, _ := meta.Marshal()

	boot := newBootNode(t)

	// Seeder announces itself to the DHT when started.
	seeder := newDHTEngine(t, boot)
	st, err := seeder.AddTorrentBytes(torrentFile)
	if err != nil {
		t.Fatal(err)
	}
	seedAll(t, st, payload)
	if err := seeder.StartTorrent(st.ID); err != nil {
		t.Fatal(err)
	}
	if st.Status().State != StateSeeding {
		t.Fatalf("a complete torrent must start in seeding state, got %s", st.Status().State)
	}
	waitFor(t, 10*time.Second, "seeder announce to reach the DHT", func() bool {
		return len(boot.LocalPeers(infoHash)) > 0
	})

	// Leecher has only a bare magnet link: no trackers, no display name.
	leecher := newDHTEngine(t, boot)
	lt, err := leecher.AddMagnet(fmt.Sprintf("magnet:?xt=urn:btih:%x", infoHash))
	if err != nil {
		t.Fatal(err)
	}
	if err := leecher.StartTorrent(lt.ID); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 20*time.Second, "DHT magnet download", func() bool {
		s := lt.Status()
		if s.State == StateError {
			t.Fatalf("leecher entered error state: %s", s.Error)
		}
		return s.PercentDone == 1
	})

	got, err := os.ReadFile(filepath.Join(leecher.DownloadDir, "dht.bin"))
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("downloaded data mismatch (err=%v)", err)
	}
	if s := lt.Status(); s.Name != "dht.bin" {
		t.Errorf("name should come from fetched metadata, got %q", s.Name)
	}
}

func TestPrivateTorrentSkipsDHT(t *testing.T) {
	boot := newBootNode(t)
	eng := newDHTEngine(t, boot)

	info, _ := bencode.Encode(map[string]any{
		"name": "private.bin", "length": 10, "piece length": 16384,
		"pieces": strings.Repeat("A", 20), "private": 1,
	})
	meta, err := metainfo.FromInfoBytes(info, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !meta.Private {
		t.Fatal("private flag not parsed")
	}
	data, _ := meta.Marshal()
	mt, err := eng.AddTorrentBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	_ = eng.StartTorrent(mt.ID)

	// With no trackers and DHT forbidden, there is no peer source at all.
	waitFor(t, 3*time.Second, "error state", func() bool { return mt.Status().State == StateError })
	if !strings.Contains(mt.Status().Error, "no peer sources") {
		t.Errorf("unexpected error: %q", mt.Status().Error)
	}
	if peers := boot.LocalPeers(meta.InfoHash); len(peers) != 0 {
		t.Errorf("private torrent leaked to the DHT: %v", peers)
	}
}

func TestTrackerFailureIsNotFatal(t *testing.T) {
	eng := NewEngine(t.TempDir(), 0)
	eng.ConfigDir = t.TempDir()
	defer eng.Close()

	mt, err := eng.AddMagnet("magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&tr=http%3A%2F%2F127.0.0.1%3A1%2Fannounce")
	if err != nil {
		t.Fatal(err)
	}
	_ = eng.StartTorrent(mt.ID)
	waitFor(t, 5*time.Second, "tracker error to be reported", func() bool { return mt.Status().Error != "" })
	if s := mt.Status(); s.State != StateDownloading {
		t.Errorf("an unreachable tracker must not stop the torrent, state=%s", s.State)
	}
}

func TestPortMessageExchange(t *testing.T) {
	boot := newBootNode(t)
	eng := newDHTEngine(t, boot)

	info := buildTorrent(t, "port.bin", make([]byte, 1000), 16384)
	meta, _ := metainfo.FromInfoBytes(info, nil)
	data, _ := meta.Marshal()
	mt, err := eng.AddTorrentBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	mt.mu.Lock()
	mt.State = StateDownloading
	mt.mu.Unlock()

	// Our "remote peer" also runs a DHT node; the engine should ping it after a PORT message.
	remoteDHT := newBootNode(t)

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", eng.Port))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write(peer.NewHandshake(meta.InfoHash, [20]byte{42}).Serialize()); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.ReadHandshake(conn); err != nil {
		t.Fatal(err)
	}

	// The engine advertises its DHT port to DHT-capable peers.
	for {
		msg, err := peer.ReadMessage(conn)
		if err != nil {
			t.Fatalf("waiting for PORT: %v", err)
		}
		if msg.ID != nil && *msg.ID == peer.MsgPort {
			port, err := peer.ParsePort(msg)
			if err != nil || int(port) != eng.DHT.Addr().Port {
				t.Fatalf("PORT advertises %d, DHT listens on %d (err=%v)", port, eng.DHT.Addr().Port, err)
			}
			break
		}
	}

	before := remoteDHT.NumNodes()
	if _, err := conn.Write(peer.FormatPort(uint16(remoteDHT.Addr().Port)).Serialize()); err != nil {
		t.Fatal(err)
	}
	// The engine's ping makes the remote node learn about the engine's DHT node.
	waitFor(t, 5*time.Second, "engine to ping the advertised DHT node", func() bool {
		return remoteDHT.NumNodes() > before
	})
}

func TestDHTIdentityPersists(t *testing.T) {
	boot := newBootNode(t)
	configDir := t.TempDir()

	first := NewEngine(t.TempDir(), 0)
	first.ConfigDir = configDir
	if err := first.StartListener(); err != nil {
		t.Fatal(err)
	}
	if err := first.EnableDHT([]string{boot.Addr().String()}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "bootstrap", func() bool { return first.DHT.NumNodes() > 0 })
	id := first.DHT.ID()
	if err := first.SaveState(); err != nil {
		t.Fatal(err)
	}
	_ = first.Close()

	second := NewEngine(t.TempDir(), 0)
	second.ConfigDir = configDir
	defer second.Close()
	if err := second.StartListener(); err != nil {
		t.Fatal(err)
	}
	// No bootstrap routers: the saved routing table alone must be enough to rejoin.
	if err := second.EnableDHT(nil); err != nil {
		t.Fatal(err)
	}
	if second.DHT.ID() != id {
		t.Errorf("DHT node ID not restored")
	}
	waitFor(t, 5*time.Second, "rejoin from saved nodes", func() bool { return second.DHT.NumNodes() > 0 })
}

func TestTrackerlessMagnetOverIPv6DHT(t *testing.T) {
	boot, err := dht.New(dht.Config{Addr: "[::1]:0", IPv6: true})
	if err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	}
	t.Cleanup(func() { _ = boot.Close() })

	payload := make([]byte, 70_000)
	_, _ = rand.Read(payload)
	info := buildTorrent(t, "dht6.bin", payload, 32768)
	infoHash := sha1.Sum(info)
	meta, _ := metainfo.FromInfoBytes(info, nil)
	torrentFile, _ := meta.Marshal()

	// Only the IPv6 DHT can be joined: the bootstrap node has no IPv4 address.
	newEngine := func() *Engine {
		eng := NewEngine(t.TempDir(), 0)
		eng.ConfigDir = t.TempDir()
		t.Cleanup(func() { _ = eng.Close() })
		if err := eng.StartListener(); err != nil {
			t.Fatal(err)
		}
		if err := eng.EnableDHT([]string{boot.Addr().String()}); err != nil {
			t.Fatal(err)
		}
		if eng.DHT6 == nil {
			t.Skip("IPv6 DHT node could not start")
		}
		waitFor(t, 5*time.Second, "IPv6 DHT bootstrap", func() bool { return eng.DHT6.NumNodes() > 0 })
		return eng
	}

	seeder := newEngine()
	st, err := seeder.AddTorrentBytes(torrentFile)
	if err != nil {
		t.Fatal(err)
	}
	seedAll(t, st, payload)
	if err := seeder.StartTorrent(st.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "seeder announce on the IPv6 DHT", func() bool { return len(boot.LocalPeers(infoHash)) > 0 })

	leecher := newEngine()
	lt, err := leecher.AddMagnet(fmt.Sprintf("magnet:?xt=urn:btih:%x", infoHash))
	if err != nil {
		t.Fatal(err)
	}
	if err := leecher.StartTorrent(lt.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 20*time.Second, "magnet download via the IPv6 DHT", func() bool {
		s := lt.Status()
		if s.State == StateError {
			t.Fatalf("leecher entered error state: %s", s.Error)
		}
		return s.PercentDone == 1
	})
	got, err := os.ReadFile(filepath.Join(leecher.DownloadDir, "dht6.bin"))
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("downloaded data mismatch (err=%v)", err)
	}
	if lt.Status().PeersFrom[SourceDHT] == 0 {
		t.Error("the peer must have come from the DHT")
	}
}

func TestDHTStatePerFamily(t *testing.T) {
	eng := NewEngine(t.TempDir(), 0)
	eng.ConfigDir = t.TempDir()
	t.Cleanup(func() { _ = eng.Close() })
	if err := eng.StartListener(); err != nil {
		t.Fatal(err)
	}
	if err := eng.EnableDHT(nil); err != nil {
		t.Fatal(err)
	}
	if eng.DHT6 == nil {
		t.Skip("IPv6 DHT node could not start")
	}
	if eng.DHT6.ID() != eng.DHT.ID() {
		t.Error("both DHT nodes must share one node ID")
	}
	if err := eng.SaveState(); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"dht.json", "dht6.json"} {
		if _, err := os.Stat(filepath.Join(eng.ConfigDir, f)); err != nil {
			t.Errorf("%s not saved: %v", f, err)
		}
	}
}
