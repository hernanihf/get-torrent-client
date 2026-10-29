package engine

import (
	"bytes"
	"crypto/rand"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hernan/get-torrent/pkg/metainfo"
)

// transferPair is a seeder with complete data and a leecher that finds it via a fake tracker.
type transferPair struct {
	seeder, leecher *Engine
	st, lt          *ManagedTorrent
	payload         []byte
}

func newTransferPair(t *testing.T, size int) *transferPair {
	t.Helper()
	p := &transferPair{payload: make([]byte, size)}
	_, _ = rand.Read(p.payload)
	info := buildTorrent(t, "limited.bin", p.payload, 64*1024)
	meta, err := metainfo.FromInfoBytes(info, nil)
	if err != nil {
		t.Fatal(err)
	}

	p.seeder = NewEngine(t.TempDir(), 0)
	p.seeder.ConfigDir = t.TempDir()
	t.Cleanup(func() { _ = p.seeder.Close() })
	if err := p.seeder.StartListener(); err != nil {
		t.Fatal(err)
	}
	tracker := fakeTracker(t, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: int(p.seeder.Port)})
	t.Cleanup(tracker.Close)

	meta.Announce = tracker.URL + "/announce"
	torrentFile, _ := meta.Marshal()
	if p.st, err = p.seeder.AddTorrentBytes(torrentFile); err != nil {
		t.Fatal(err)
	}
	seedAll(t, p.st, p.payload)
	if err := p.seeder.StartTorrent(p.st.ID); err != nil { // seeds, with the choker running
		t.Fatal(err)
	}

	p.leecher = NewEngine(t.TempDir(), 0)
	p.leecher.ConfigDir = t.TempDir()
	t.Cleanup(func() { _ = p.leecher.Close() })
	if p.lt, err = p.leecher.AddTorrentBytes(torrentFile); err != nil {
		t.Fatal(err)
	}
	return p
}

// download starts the leecher and waits for completion, returning the elapsed time.
func (p *transferPair) download(t *testing.T) time.Duration {
	t.Helper()
	start := time.Now()
	if err := p.leecher.StartTorrent(p.lt.ID); err != nil {
		t.Fatal(err)
	}
	p.waitDone(t)
	return time.Since(start)
}

func (p *transferPair) waitDone(t *testing.T) {
	t.Helper()
	waitFor(t, 30*time.Second, "limited download", func() bool { return p.lt.Status().PercentDone == 1 })
	got, err := os.ReadFile(filepath.Join(p.leecher.DownloadDir, "limited.bin"))
	if err != nil || !bytes.Equal(got, p.payload) {
		t.Fatalf("downloaded data mismatch (err=%v)", err)
	}
}

// timedTransfer applies configure to a fresh pair and times the download.
func timedTransfer(t *testing.T, size int, configure func(seeder, leecher *Engine, st, lt *ManagedTorrent)) time.Duration {
	t.Helper()
	p := newTransferPair(t, size)
	configure(p.seeder, p.leecher, p.st, p.lt)
	return p.download(t)
}

func TestSpeedLimitsThrottleTransfers(t *testing.T) {
	const size = 600_000 // at 300 kB/s this takes ~2 s
	cases := []struct {
		name      string
		configure func(seeder, leecher *Engine, st, lt *ManagedTorrent)
		min, max  time.Duration
	}{
		{
			name:      "unlimited",
			configure: func(_, _ *Engine, _, _ *ManagedTorrent) {},
			max:       1500 * time.Millisecond,
		},
		{
			name: "session upload limit on the seeder",
			configure: func(seeder, _ *Engine, _, _ *ManagedTorrent) {
				seeder.UpdateSettings(func(s *Settings) { s.SpeedLimitUp, s.SpeedLimitUpEnabled = 300, true })
			},
			min: 1700 * time.Millisecond, max: 3500 * time.Millisecond,
		},
		{
			name: "per-torrent download limit on the leecher",
			configure: func(_, leecher *Engine, _, lt *ManagedTorrent) {
				_ = leecher.SetTorrentLimits(lt.ID, func(l *TorrentLimits) { l.DownloadLimit, l.DownloadLimited = 300, true })
			},
			min: 1700 * time.Millisecond, max: 3500 * time.Millisecond,
		},
		{
			name: "alternative (turtle) speed overrides the normal limit",
			configure: func(_, leecher *Engine, _, _ *ManagedTorrent) {
				leecher.UpdateSettings(func(s *Settings) {
					s.SpeedLimitDown, s.SpeedLimitDownEnabled = 100_000, true // effectively unlimited
					s.AltSpeedDown, s.AltSpeedEnabled = 300, true
				})
			},
			min: 1700 * time.Millisecond, max: 3500 * time.Millisecond,
		},
		{
			name: "torrent ignoring session limits",
			configure: func(_, leecher *Engine, _, lt *ManagedTorrent) {
				leecher.UpdateSettings(func(s *Settings) { s.SpeedLimitDown, s.SpeedLimitDownEnabled = 1, true })
				_ = leecher.SetTorrentLimits(lt.ID, func(l *TorrentLimits) { l.HonorsSessionLimits = false })
			},
			max: 1500 * time.Millisecond,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			elapsed := timedTransfer(t, size, tc.configure)
			if elapsed < tc.min || elapsed > tc.max {
				t.Errorf("transfer took %v, expected between %v and %v", elapsed, tc.min, tc.max)
			}
		})
	}
}

func TestZeroLimitPausesUntilLifted(t *testing.T) {
	p := newTransferPair(t, 200_000)
	p.leecher.UpdateSettings(func(s *Settings) { s.SpeedLimitDown, s.SpeedLimitDownEnabled = 0, true })
	if err := p.leecher.StartTorrent(p.lt.ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond)
	// At most the blocks already in flight when the first read paused can have arrived.
	if pct := p.lt.Status().PercentDone; pct > 0.5 {
		t.Fatalf("an enabled zero download limit must stall the transfer, got %.0f%%", pct*100)
	}
	p.leecher.UpdateSettings(func(s *Settings) { s.SpeedLimitDownEnabled = false })
	p.waitDone(t)
}

func TestLimitsPersist(t *testing.T) {
	configDir := t.TempDir()
	first := NewEngine(t.TempDir(), 0)
	first.ConfigDir = configDir
	info := buildTorrent(t, "p.bin", make([]byte, 1000), 16384)
	meta, _ := metainfo.FromInfoBytes(info, nil)
	data, _ := meta.Marshal()
	mt, err := first.AddTorrentBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	_ = first.SetTorrentLimits(mt.ID, func(l *TorrentLimits) {
		l.UploadLimit, l.UploadLimited, l.HonorsSessionLimits = 42, true, false
	})
	first.UpdateSettings(func(s *Settings) { s.AltSpeedDown, s.AltSpeedUp, s.AltSpeedEnabled = 7, 8, true })
	if err := first.SaveState(); err != nil {
		t.Fatal(err)
	}
	_ = first.Close()

	second := NewEngine(t.TempDir(), 0)
	second.ConfigDir = configDir
	defer second.Close()
	if err := second.LoadState(); err != nil {
		t.Fatal(err)
	}
	if st := second.Settings(); st.AltSpeedDown != 7 || st.AltSpeedUp != 8 || !st.AltSpeedEnabled {
		t.Errorf("alt speed not restored: %+v", st)
	}
	if rate, on := second.sessionDown.Limit(); rate != 7*SpeedUnit || !on {
		t.Errorf("session limiter not applied on load: %d %v", rate, on)
	}
	restored := second.ListTorrents()[0]
	if l := restored.Limits(); l.UploadLimit != 42 || !l.UploadLimited || l.HonorsSessionLimits {
		t.Errorf("torrent limits not restored: %+v", l)
	}
	if rate, on := restored.upLimiter.Limit(); rate != 42*SpeedUnit || !on {
		t.Errorf("torrent limiter not applied on load: %d %v", rate, on)
	}
}
