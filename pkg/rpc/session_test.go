package rpc

import (
	"strings"
	"testing"
)

func TestSessionSetRejectsUnsupportedChanges(t *testing.T) {
	env := newEnv(t, Options{})

	r := env.call(t, "session-set", map[string]any{"pex-enabled": true, "encryption": "required", "speed-limit-down": 77})
	if r.Result == "success" || !strings.Contains(r.Result, "encryption, pex-enabled") {
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
