package engine

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hernan/get-torrent/pkg/bencode"
	"github.com/hernan/get-torrent/pkg/metainfo"
	"github.com/hernan/get-torrent/pkg/peer"
	"github.com/hernan/get-torrent/pkg/storage"
)

// buildTorrent creates a single-file torrent for payload. The info dict is padded past 16 KiB so
// that BEP 0009 transfer needs more than one metadata piece.
func buildTorrent(t *testing.T, name string, payload []byte, pieceLen int) []byte {
	t.Helper()
	var pieces []byte
	for off := 0; off < len(payload); off += pieceLen {
		end := min(off+pieceLen, len(payload))
		sum := sha1.Sum(payload[off:end])
		pieces = append(pieces, sum[:]...)
	}
	info, err := bencode.Encode(map[string]any{
		"name":         name,
		"length":       len(payload),
		"piece length": pieceLen,
		"pieces":       string(pieces),
		"x-padding":    strings.Repeat("p", 20000),
	})
	if err != nil {
		t.Fatal(err)
	}
	return info
}

// seedAll feeds every piece of payload into the torrent's piece manager, as if downloaded.
func seedAll(t *testing.T, mt *ManagedTorrent, payload []byte) {
	t.Helper()
	meta, pm, _ := mt.components()
	for i := range meta.Pieces {
		start := i * int(meta.PieceLength)
		end := min(start+int(meta.PieceLength), len(payload))
		for b := start; b < end; b += storage.BlockSize {
			if _, err := pm.ReceiveBlock(i, b-start, payload[b:min(b+storage.BlockSize, end)]); err != nil {
				t.Fatalf("seeding piece %d: %v", i, err)
			}
		}
	}
	if !pm.IsComplete() {
		t.Fatal("seeder is not complete")
	}
}

// fakeTracker answers every announce with a compact peer list containing only addr.
func fakeTracker(t *testing.T, addr *net.TCPAddr) *httptest.Server {
	t.Helper()
	compact := make([]byte, 6)
	copy(compact, addr.IP.To4())
	binary.BigEndian.PutUint16(compact[4:], uint16(addr.Port))
	body, _ := bencode.Encode(map[string]any{"interval": 60, "peers": string(compact)})
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestMagnetEndToEnd(t *testing.T) {
	payload := make([]byte, 100_000) // 4 pieces of 32 KiB, the last one partial
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	info := buildTorrent(t, "payload.bin", payload, 32768)
	infoHash := sha1.Sum(info)
	meta, err := metainfo.FromInfoBytes(info, nil)
	if err != nil {
		t.Fatal(err)
	}
	torrentFile, _ := meta.Marshal()

	// --- Seeder: complete data, accepting inbound peers ---
	seeder := NewEngine(t.TempDir(), 0)
	seeder.ConfigDir = t.TempDir()
	defer seeder.Close()
	st, err := seeder.AddTorrentBytes(torrentFile)
	if err != nil {
		t.Fatalf("seeder add: %v", err)
	}
	seedAll(t, st, payload)
	st.mu.Lock()
	st.State = StateSeeding
	st.mu.Unlock()
	if err := seeder.StartListener(); err != nil {
		t.Fatal(err)
	}

	tracker := fakeTracker(t, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: int(seeder.Port)})
	defer tracker.Close()

	// --- Leecher: only the magnet link ---
	leechDir := t.TempDir()
	leecher := NewEngine(leechDir, 0)
	leecher.ConfigDir = t.TempDir()
	defer leecher.Close()

	uri := fmt.Sprintf("magnet:?xt=urn:btih:%x&dn=%s&tr=%s", infoHash, "payload.bin", url.QueryEscape(tracker.URL+"/announce"))
	lt, err := leecher.AddMagnet(uri)
	if err != nil {
		t.Fatalf("AddMagnet: %v", err)
	}
	if s := lt.Status(); s.Name != "payload.bin" || s.MetadataPercentComplete != 0 || s.InfoHash != infoHash {
		t.Fatalf("unexpected pre-metadata status: %+v", s)
	}
	if again, _ := leecher.AddMagnet(uri); again != lt {
		t.Fatalf("re-adding the same magnet must return the existing torrent")
	}

	if err := leecher.StartTorrent(lt.ID); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 20*time.Second, "magnet download to complete", func() bool {
		s := lt.Status()
		if s.State == StateError {
			t.Fatalf("leecher entered error state: %s", s.Error)
		}
		return s.PercentDone == 1
	})

	s := lt.Status()
	if s.State != StateSeeding || s.MetadataPercentComplete != 1 || s.TotalSize != int64(len(payload)) || s.Left != 0 {
		t.Errorf("unexpected final status: %+v", s)
	}

	// Transfer accounting reached both sides.
	if s.Downloaded < int64(len(payload)) || s.HaveValid != int64(len(payload)) || s.DoneDate == 0 || s.AddedDate == 0 {
		t.Errorf("leecher accounting: downloaded=%d have=%d done=%d added=%d", s.Downloaded, s.HaveValid, s.DoneDate, s.AddedDate)
	}
	if up := st.Status().Uploaded; up < int64(len(payload)) {
		t.Errorf("seeder uploaded %d bytes, expected at least %d", up, len(payload))
	}
	if s.PeersFrom[SourceTracker] != 1 {
		t.Errorf("expected one peer from the tracker, got %v", s.PeersFrom)
	}
	if ss := st.Status(); ss.PeersFrom[SourceIncoming] != 1 {
		t.Errorf("seeder should see one incoming peer, got %v", ss.PeersFrom)
	}
	if trs := lt.TrackerList(); len(trs) != 1 || !trs[0].LastAnnounceSucceeded || trs[0].LastAnnouncePeerCount != 1 {
		t.Errorf("unexpected tracker stats: %+v", trs)
	}
	if peers := lt.PeerList(); len(peers) != 1 || peers[0].Progress != 1 || peers[0].ClientName == "" {
		t.Errorf("unexpected peer list: %+v", peers)
	}

	got, err := os.ReadFile(filepath.Join(leechDir, "payload.bin"))
	if err != nil {
		t.Fatalf("reading downloaded file: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("downloaded data differs from the original")
	}

	// The fetched metadata is persisted as a .torrent with the same info_hash.
	saved, err := metainfo.Open(filepath.Join(leecher.ConfigDir, "torrents", hex.EncodeToString(infoHash[:])+".torrent"))
	if err != nil {
		t.Fatalf("opening persisted torrent: %v", err)
	}
	if saved.InfoHash != infoHash || len(saved.Trackers()) != 1 {
		t.Errorf("persisted torrent mismatch: hash=%x trackers=%v", saved.InfoHash, saved.Trackers())
	}
}

func TestMagnetWithoutTrackersErrors(t *testing.T) {
	eng := NewEngine(t.TempDir(), 0)
	eng.ConfigDir = t.TempDir()
	defer eng.Close()

	mt, err := eng.AddMagnet("magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567")
	if err != nil {
		t.Fatal(err)
	}
	if mt.Status().Name != "0123456789abcdef0123456789abcdef01234567" {
		t.Errorf("magnet without dn should be named by its info_hash, got %q", mt.Status().Name)
	}
	_ = eng.StartTorrent(mt.ID)
	waitFor(t, 2*time.Second, "error state", func() bool { return mt.Status().State == StateError })
	if !strings.Contains(mt.Status().Error, "no trackers") {
		t.Errorf("unexpected error string: %q", mt.Status().Error)
	}

	if _, err := eng.AddMagnet("magnet:?dn=nohash"); err == nil {
		t.Errorf("expected error for magnet without btih")
	}
}

func TestPendingMagnetSurvivesRestart(t *testing.T) {
	configDir := t.TempDir()
	uri := "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=pending&tr=udp%3A%2F%2Ftracker.invalid%3A1"

	eng := NewEngine(t.TempDir(), 0)
	eng.ConfigDir = configDir
	if _, err := eng.AddMagnet(uri); err != nil {
		t.Fatal(err)
	}
	if err := eng.SaveState(); err != nil {
		t.Fatal(err)
	}

	restored := NewEngine(t.TempDir(), 0)
	restored.ConfigDir = configDir
	defer restored.Close()
	if err := restored.LoadState(); err != nil {
		t.Fatal(err)
	}
	list := restored.ListTorrents()
	if len(list) != 1 {
		t.Fatalf("expected 1 restored torrent, got %d", len(list))
	}
	s := list[0].Status()
	if s.MagnetURI != uri || s.Name != "pending" || s.State != StateQueued {
		t.Errorf("unexpected restored status: %+v", s)
	}
}

func TestServesMetadataToPeers(t *testing.T) {
	payload := make([]byte, 40_000)
	info := buildTorrent(t, "srv.bin", payload, 16384)
	meta, _ := metainfo.FromInfoBytes(info, nil)
	torrentFile, _ := meta.Marshal()

	eng := NewEngine(t.TempDir(), 0)
	eng.ConfigDir = t.TempDir()
	defer eng.Close()
	mt, err := eng.AddTorrentBytes(torrentFile)
	if err != nil {
		t.Fatal(err)
	}
	mt.mu.Lock()
	mt.State = StateDownloading
	mt.mu.Unlock()

	client, server := net.Pipe()
	defer client.Close()
	go eng.handleInboundConn(server)

	_ = client.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := client.Write(peer.NewHandshake(meta.InfoHash, [20]byte{7}).Serialize()); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.ReadHandshake(client); err != nil {
		t.Fatal(err)
	}

	// Read until the engine's extension handshake, which must advertise metadata_size.
	var ourID uint8
	for {
		msg, err := peer.ReadMessage(client)
		if err != nil {
			t.Fatalf("reading: %v", err)
		}
		if msg.ID == nil || *msg.ID != peer.MsgExtended {
			continue
		}
		_, payload, _ := peer.ParseExtended(msg)
		hs, err := peer.ParseExtHandshake(payload)
		if err != nil {
			t.Fatal(err)
		}
		if hs.MetadataSize != len(info) {
			t.Fatalf("expected metadata_size %d, got %d", len(info), hs.MetadataSize)
		}
		ourID = hs.M[peer.ExtUTMetadata]
		break
	}

	// Announce our own ut_metadata ID (9), then request piece 1 and an out-of-range piece.
	clientHS, _ := bencode.Encode(map[string]any{"m": map[string]any{peer.ExtUTMetadata: 9}})
	for _, m := range []*peer.Message{
		peer.FormatExtended(peer.ExtHandshakeID, clientHS),
		peer.FormatMetadataRequest(ourID, 1),
		peer.FormatMetadataRequest(ourID, 50),
	} {
		if _, err := client.Write(m.Serialize()); err != nil {
			t.Fatal(err)
		}
	}

	var gotData, gotReject bool
	for !gotData || !gotReject {
		msg, err := peer.ReadMessage(client)
		if err != nil {
			t.Fatalf("reading: %v", err)
		}
		if msg.ID == nil || *msg.ID != peer.MsgExtended {
			continue
		}
		extID, payload, _ := peer.ParseExtended(msg)
		if extID != 9 {
			t.Fatalf("engine must address ut_metadata messages with our ID 9, used %d", extID)
		}
		m, err := peer.ParseMetadataMsg(payload)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case m.Type == peer.MetadataData && m.Piece == 1:
			if !bytes.Equal(m.Data, info[peer.MetadataPieceSize:]) || m.TotalSize != len(info) {
				t.Fatalf("served metadata piece is wrong")
			}
			gotData = true
		case m.Type == peer.MetadataReject && m.Piece == 50:
			gotReject = true
		}
	}
}

func mustMeta(t *testing.T, info []byte) *metainfo.TorrentMeta {
	t.Helper()
	meta, err := metainfo.FromInfoBytes(info, nil)
	if err != nil {
		t.Fatal(err)
	}
	return meta
}
