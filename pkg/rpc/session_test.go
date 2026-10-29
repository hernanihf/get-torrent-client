package rpc

import (
	"strings"
	"testing"
)

func TestSessionSetRejectsUnsupportedChanges(t *testing.T) {
	env := newEnv(t, Options{})

	r := env.call(t, "session-set", map[string]any{"lpd-enabled": true, "encryption": "required", "speed-limit-down": 77})
	if r.Result == "success" || !strings.Contains(r.Result, "encryption, lpd-enabled") {
		t.Errorf("expected the unsupported keys to be named, got %q", r.Result)
	}
	if env.eng.Settings().SpeedLimitDown == 77 {
		t.Errorf("nothing may be applied when a key is rejected")
	}

	// Echoing the reported values (a GUI saving its whole preferences dialog) is fine, as are
	// read-only and unknown keys.
	current := env.mustSucceed(t, "session-get", nil)
	current["speed-limit-down"] = 77
	current["no-such-setting"] = "x"
	env.mustSucceed(t, "session-set", current)
	if env.eng.Settings().SpeedLimitDown != 77 {
		t.Errorf("supported settings must be applied")
	}
}

func TestSameValue(t *testing.T) {
	cases := []struct {
		arg, reported any
		want          bool
	}{
		{float64(5), 5, true},
		{float64(5), int64(5), true},
		{float64(6881), uint16(6881), true},
		{float64(2), 2.0, true},
		{float64(5), 6, false},
		{true, true, true},
		{true, false, false},
		{"tolerated", "tolerated", true},
		{"5", 5, false},
	}
	for _, c := range cases {
		if got := sameValue(c.arg, c.reported); got != c.want {
			t.Errorf("sameValue(%#v, %#v) = %v, want %v", c.arg, c.reported, got, c.want)
		}
	}
}

func TestSeedLimitSettings(t *testing.T) {
	env := newEnv(t, Options{})
	env.mustSucceed(t, "session-set", map[string]any{"seedRatioLimit": 1.25, "seedRatioLimited": true, "idle-seeding-limit": 45, "idle-seeding-limit-enabled": true})
	got := env.mustSucceed(t, "session-get", map[string]any{"fields": []any{"seedRatioLimit", "seedRatioLimited", "idle-seeding-limit", "idle-seeding-limit-enabled"}})
	if got["seedRatioLimit"] != 1.25 || got["seedRatioLimited"] != true || got["idle-seeding-limit"] != float64(45) || got["idle-seeding-limit-enabled"] != true {
		t.Errorf("session seed limits not applied: %v", got)
	}

	torrent, _ := makeTorrent(t, "s.bin", 1000, 16384)
	env.mustSucceed(t, "torrent-add", map[string]any{"metainfo": b64(torrent), "paused": true})
	fields := map[string]any{"fields": []any{"seedRatioMode", "seedRatioLimit", "seedIdleMode", "seedIdleLimit", "isFinished", "etaIdle"}}
	tor := env.torrents(t, fields)[0]
	if tor["seedRatioMode"] != float64(0) || tor["seedRatioLimit"] != float64(2) || tor["seedIdleLimit"] != float64(30) || tor["isFinished"] != false {
		t.Errorf("unexpected defaults: %v", tor)
	}

	env.mustSucceed(t, "torrent-set", map[string]any{"ids": []any{1}, "seedRatioMode": 1, "seedRatioLimit": 0.5, "seedIdleMode": 2, "seedIdleLimit": 5})
	tor = env.torrents(t, fields)[0]
	if tor["seedRatioMode"] != float64(1) || tor["seedRatioLimit"] != 0.5 || tor["seedIdleMode"] != float64(2) || tor["seedIdleLimit"] != float64(5) {
		t.Errorf("torrent-set seed limits not applied: %v", tor)
	}

	for _, args := range []map[string]any{{"seedRatioMode": 3}, {"seedIdleMode": 1.5}, {"seedRatioLimit": -1}} {
		args["ids"] = []any{1}
		if r := env.call(t, "torrent-set", args); r.Result == "success" {
			t.Errorf("torrent-set %v must fail", args)
		}
	}
}
