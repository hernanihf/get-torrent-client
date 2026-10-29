package engine

import (
	"log"
	"time"
)

// Seeding limits (Transmission's seedRatio* and seedIdle*): a seeding torrent stops once it has
// uploaded ratio times what it downloaded, or after uploading nothing for the idle limit.
// Each torrent follows the session settings, its own limit, or none.

// Per-torrent seed limit modes (Transmission's TR_RATIOLIMIT_* / TR_IDLELIMIT_*).
const (
	SeedLimitGlobal    = 0 // follow the session settings
	SeedLimitSingle    = 1 // use the torrent's own limit
	SeedLimitUnlimited = 2 // seed forever
)

// seedCheckInterval is how often seeding torrents are checked against their limits.
var seedCheckInterval = 5 * time.Second

// seedLimits are the limits that apply to one torrent; a zero duration or ok=false means none.
type seedLimits struct {
	ratio   float64
	ratioOn bool
	idle    time.Duration
	idleOn  bool
}

// seedLimitsFor resolves t's limits against the session settings.
func (e *Engine) seedLimitsFor(t *ManagedTorrent) seedLimits {
	st := e.Settings()
	l := t.Limits()
	var out seedLimits
	switch l.SeedRatioMode {
	case SeedLimitGlobal:
		out.ratio, out.ratioOn = st.SeedRatioLimit, st.SeedRatioLimited
	case SeedLimitSingle:
		out.ratio, out.ratioOn = l.SeedRatioLimit, true
	}
	switch l.SeedIdleMode {
	case SeedLimitGlobal:
		out.idle, out.idleOn = time.Duration(st.IdleSeedingLimit)*time.Minute, st.IdleSeedingLimitEnabled
	case SeedLimitSingle:
		out.idle, out.idleOn = time.Duration(l.SeedIdleLimit)*time.Minute, true
	}
	return out
}

// ratioBase is what the upload ratio is measured against: the bytes downloaded or, for a torrent
// that was complete from the start, the size of its wanted data, as in Transmission.
func ratioBase(st TorrentStatus) int64 {
	if st.Downloaded > 0 {
		return st.Downloaded
	}
	return st.SizeWhenDone
}

// SeedRatioGoal returns the upload total at which t stops seeding, if a ratio limit applies.
func (e *Engine) SeedRatioGoal(t *ManagedTorrent) (int64, bool) {
	lim := e.seedLimitsFor(t)
	if !lim.ratioOn {
		return 0, false
	}
	return int64(lim.ratio * float64(ratioBase(t.Status()))), true
}

// SeedIdleRemaining returns how long t may stay idle before it stops, if an idle limit applies.
func (e *Engine) SeedIdleRemaining(t *ManagedTorrent, now time.Time) (time.Duration, bool) {
	lim := e.seedLimitsFor(t)
	if !lim.idleOn {
		return 0, false
	}
	return max(lim.idle-now.Sub(t.idleSince()), 0), true
}

// idleSince is when t last uploaded, or started seeding if later.
func (t *ManagedTorrent) idleSince() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	since := t.activityDate
	if t.State == StateSeeding && t.stateSince.After(since) {
		since = t.stateSince
	}
	return since
}

// seedLimitReached reports which limit, if any, a seeding torrent has reached.
func (e *Engine) seedLimitReached(t *ManagedTorrent, now time.Time) string {
	st := t.Status()
	if st.State != StateSeeding {
		return ""
	}
	if goal, ok := e.SeedRatioGoal(t); ok && ratioBase(st) > 0 && st.Uploaded >= goal {
		return "seed ratio"
	}
	if left, ok := e.SeedIdleRemaining(t, now); ok && left == 0 {
		return "idle seeding limit"
	}
	return ""
}

// runSeedLimits stops t once it reaches a seeding limit, marking it finished.
func (e *Engine) runSeedLimits(t *ManagedTorrent, stop <-chan struct{}) {
	ticker := time.NewTicker(seedCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case now := <-ticker.C:
			reason := e.seedLimitReached(t, now)
			if reason == "" {
				continue
			}
			log.Printf("[Engine] %q reached its %s; stopping", t.Status().Name, reason)
			t.mu.Lock()
			t.finished = true
			t.mu.Unlock()
			_ = e.StopTorrent(t.ID)
			return
		}
	}
}

// Finished reports whether t stopped because it reached a seeding limit (isFinished).
func (t *ManagedTorrent) Finished() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.finished
}

// liftReachedRatio lets a torrent that already met its ratio seed again when started by hand:
// its ratio limit is turned off, as Transmission does.
func (e *Engine) liftReachedRatio(t *ManagedTorrent) {
	st := t.Status()
	_, pm, _ := t.components()
	if pm == nil || !pm.IsDone() {
		return
	}
	if goal, ok := e.SeedRatioGoal(t); ok && ratioBase(st) > 0 && st.Uploaded >= goal {
		log.Printf("[Engine] %q restarted manually -- disabling its seed ratio", st.Name)
		t.mu.Lock()
		t.limits.SeedRatioMode = SeedLimitUnlimited
		t.mu.Unlock()
	}
}
