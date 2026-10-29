package engine

import (
	"encoding/json"
	"testing"
	"time"
)

func fastSeedChecks(t *testing.T) {
	t.Helper()
	old := seedCheckInterval
	seedCheckInterval = 20 * time.Millisecond
	t.Cleanup(func() { seedCheckInterval = old })
}

func TestSeedRatioStopsSeeding(t *testing.T) {
	fastSeedChecks(t)
	p := newTransferPair(t, 200_000)
	// The seeder started complete, so its ratio is measured against the torrent size.
	if err := p.seeder.SetTorrentLimits(p.st.ID, func(l *TorrentLimits) {
		l.SeedRatioMode, l.SeedRatioLimit = SeedLimitSingle, 1
	}); err != nil {
		t.Fatal(err)
	}
	if goal, ok := p.seeder.SeedRatioGoal(p.st); !ok || goal != 200_000 {
		t.Fatalf("goal = %d, %v", goal, ok)
	}
	p.download(t)

	waitFor(t, 5*time.Second, "seeder to stop at ratio 1", func() bool { return p.st.Status().State == StateStopped })
	if !p.st.Finished() {
		t.Errorf("a torrent stopped by its ratio must report isFinished")
	}

	// Starting it by hand lifts the reached ratio limit, so it keeps seeding.
	if err := p.seeder.StartTorrent(p.st.ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if st := p.st.Status(); st.State != StateSeeding || p.st.Finished() {
		t.Errorf("manual start must seed again: %s finished=%v", st.State, p.st.Finished())
	}
	if m := p.st.Limits().SeedRatioMode; m != SeedLimitUnlimited {
		t.Errorf("ratio mode = %d, want unlimited", m)
	}
}

func TestIdleSeedingLimit(t *testing.T) {
	fastSeedChecks(t)
	p := newTransferPair(t, 50_000)
	// Session-wide idle limit of 0 minutes: an idle seed stops at the next check.
	p.seeder.UpdateSettings(func(s *Settings) { s.IdleSeedingLimit, s.IdleSeedingLimitEnabled = 0, true })
	waitFor(t, 5*time.Second, "idle seed to stop", func() bool { return p.st.Status().State == StateStopped })
	if !p.st.Finished() {
		t.Errorf("an idle-stopped torrent must report isFinished")
	}

	// A torrent set to seed forever ignores the session limit.
	if err := p.seeder.SetTorrentLimits(p.st.ID, func(l *TorrentLimits) { l.SeedIdleMode = SeedLimitUnlimited }); err != nil {
		t.Fatal(err)
	}
	if err := p.seeder.StartTorrent(p.st.ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if st := p.st.Status(); st.State != StateSeeding {
		t.Errorf("unlimited idle mode must keep seeding, state %s", st.State)
	}
}

func TestSeedLimitsResolution(t *testing.T) {
	e := newTestEngine(t)
	mt := addWithTracker(t, e, make([]byte, 1000), "http://127.0.0.1:1/announce")
	e.UpdateSettings(func(s *Settings) { s.SeedRatioLimit, s.SeedRatioLimited = 1.5, true })

	if lim := e.seedLimitsFor(mt); !lim.ratioOn || lim.ratio != 1.5 || lim.idleOn {
		t.Errorf("global mode must follow the session: %+v", lim)
	}
	_ = e.SetTorrentLimits(mt.ID, func(l *TorrentLimits) {
		l.SeedRatioMode, l.SeedRatioLimit = SeedLimitSingle, 3
		l.SeedIdleMode, l.SeedIdleLimit = SeedLimitSingle, 10
	})
	if lim := e.seedLimitsFor(mt); lim.ratio != 3 || !lim.idleOn || lim.idle != 10*time.Minute {
		t.Errorf("single mode must use the torrent's limits: %+v", lim)
	}
	_ = e.SetTorrentLimits(mt.ID, func(l *TorrentLimits) { l.SeedRatioMode = SeedLimitUnlimited })
	if lim := e.seedLimitsFor(mt); lim.ratioOn {
		t.Errorf("unlimited mode must disable the ratio: %+v", lim)
	}
}

func TestTorrentLimitsJSONDefaults(t *testing.T) {
	var l TorrentLimits
	if err := json.Unmarshal([]byte(`{"DownloadLimit":5,"HonorsSessionLimits":false}`), &l); err != nil {
		t.Fatal(err)
	}
	if l.DownloadLimit != 5 || l.HonorsSessionLimits || l.SeedRatioLimit != 2 || l.SeedIdleLimit != 30 || l.UploadLimit != 100 {
		t.Errorf("missing fields must keep their defaults: %+v", l)
	}
}

func TestSeedSettingsPersist(t *testing.T) {
	cfg := t.TempDir()
	e := NewEngine(t.TempDir(), 0)
	e.ConfigDir = cfg
	e.UpdateSettings(func(s *Settings) {
		s.SeedRatioLimit, s.SeedRatioLimited = 0.5, true
		s.IdleSeedingLimit, s.IdleSeedingLimitEnabled = 0, true
	})
	if err := e.SaveState(); err != nil {
		t.Fatal(err)
	}
	_ = e.Close()

	e2 := NewEngine(t.TempDir(), 0)
	e2.ConfigDir = cfg
	t.Cleanup(func() { _ = e2.Close() })
	if err := e2.LoadState(); err != nil {
		t.Fatal(err)
	}
	// An idle limit of 0 must survive the round trip, not fall back to the default.
	if st := e2.Settings(); st.SeedRatioLimit != 0.5 || !st.SeedRatioLimited || st.IdleSeedingLimit != 0 || !st.IdleSeedingLimitEnabled {
		t.Errorf("seed settings not restored: %+v", st)
	}
}
