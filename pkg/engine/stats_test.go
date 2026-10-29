package engine

import (
	"testing"
	"time"

	"github.com/hernan/get-torrent/pkg/peer"
)

func TestRateMeter(t *testing.T) {
	var m rateMeter
	base := time.Unix(1_000_000, 0)
	for s := 0; s < rateWindow; s++ {
		m.add(1000, base.Add(time.Duration(s)*time.Second))
	}
	// The current (partial) second is excluded: 5 full seconds of 1000 B.
	if got := m.rate(base.Add(rateWindow * time.Second)); got != 1000 {
		t.Errorf("expected 1000 B/s, got %d", got)
	}
	m.add(50_000, base.Add(rateWindow*time.Second))
	if got := m.rate(base.Add(rateWindow * time.Second)); got != 1000 {
		t.Errorf("in-progress second must not count yet, got %d", got)
	}
	if got := m.rate(base.Add(time.Hour)); got != 0 {
		t.Errorf("stale buckets must not count, got %d", got)
	}
}

func TestClientFromPeerID(t *testing.T) {
	var id [20]byte
	copy(id[:], "-TR4060-abcdefghijkl")
	if got := clientFromPeerID(id); got != "Transmission 4060" {
		t.Errorf("got %q", got)
	}
	copy(id[:], "M7-2-2--abcdefghijkl")
	if got := clientFromPeerID(id); got != "" {
		t.Errorf("non-Azureus IDs must yield empty name, got %q", got)
	}
}

func TestCumulativeStatsPersist(t *testing.T) {
	configDir := t.TempDir()
	first := NewEngine(t.TempDir(), 0)
	first.ConfigDir = configDir
	first.stats.downloaded.Add(1000)
	first.stats.uploaded.Add(300)
	if err := first.SaveState(); err != nil {
		t.Fatal(err)
	}

	second := NewEngine(t.TempDir(), 0)
	second.ConfigDir = configDir
	if err := second.LoadState(); err != nil {
		t.Fatal(err)
	}
	second.stats.downloaded.Add(10)
	current, cumulative := second.SessionStats()
	if current.DownloadedBytes != 10 || current.SessionCount != 1 {
		t.Errorf("unexpected current stats: %+v", current)
	}
	if cumulative.DownloadedBytes != 1010 || cumulative.UploadedBytes != 300 || cumulative.SessionCount != 2 {
		t.Errorf("unexpected cumulative stats: %+v", cumulative)
	}
}

func TestSetVerifiedAndFiles(t *testing.T) {
	payload := make([]byte, 40_000)
	info := buildTorrent(t, "v.bin", payload, 16384)
	eng := NewEngine(t.TempDir(), 0)
	eng.ConfigDir = t.TempDir()
	defer eng.Close()
	meta := mustMeta(t, info)
	data, _ := meta.Marshal()
	mt, err := eng.AddTorrentBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	_, pm, _ := mt.components()
	have := peer.Bitfield{0xA0} // pieces 0 and 2
	pm.SetVerified(have)
	st := mt.Status()
	if st.HaveValid != 16384+(40_000-2*16384) || st.Left != 16384 {
		t.Errorf("unexpected progress after SetVerified: have=%d left=%d", st.HaveValid, st.Left)
	}
	if files := mt.Files(); files[0].BytesCompleted != st.HaveValid {
		t.Errorf("file progress %d != haveValid %d", files[0].BytesCompleted, st.HaveValid)
	}
	// Un-verifying a piece makes it downloadable again.
	pm.SetVerified(peer.Bitfield{0x80})
	if req, ok := pm.NextRequest(nil); !ok || req.PieceIndex == 0 {
		t.Errorf("expected a request for a non-verified piece, got %+v ok=%v", req, ok)
	}
}
