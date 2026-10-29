package engine

import (
	"context"
	"fmt"

	"github.com/hernan/get-torrent/pkg/bandwidth"
)

// SpeedUnit is the number of bytes in Transmission's speed "kB" (its "speed-bytes" unit).
const SpeedUnit = 1000

func errTorrentNotFound(id int) error { return fmt.Errorf("torrent with id %d not found", id) }

// TorrentLimits are per-torrent bandwidth settings (Transmission's torrent-set limits).
type TorrentLimits struct {
	DownloadLimit       int64 // KB/s
	DownloadLimited     bool
	UploadLimit         int64 // KB/s
	UploadLimited       bool
	HonorsSessionLimits bool // also obey the session-wide limits
}

func defaultTorrentLimits() TorrentLimits {
	return TorrentLimits{DownloadLimit: 100, UploadLimit: 100, HonorsSessionLimits: true}
}

// Limits returns the torrent's bandwidth settings.
func (t *ManagedTorrent) Limits() TorrentLimits {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.limits
}

// SetTorrentLimits updates a torrent's bandwidth settings; they take effect immediately.
func (e *Engine) SetTorrentLimits(id int, fn func(*TorrentLimits)) error {
	t := e.GetTorrentByID(id)
	if t == nil {
		return errTorrentNotFound(id)
	}
	t.mu.Lock()
	fn(&t.limits)
	l := t.limits
	t.mu.Unlock()
	t.applyLimits(l)
	return nil
}

func (t *ManagedTorrent) applyLimits(l TorrentLimits) {
	t.downLimiter.SetLimit(l.DownloadLimit*SpeedUnit, l.DownloadLimited)
	t.upLimiter.SetLimit(l.UploadLimit*SpeedUnit, l.UploadLimited)
}

// applyLimitsLocked pushes the session speed settings into the session limiters. The alternative
// speed limits replace the normal ones while enabled. Caller holds e.mu.
func (e *Engine) applyLimitsLocked() {
	if e.AltSpeedEnabled {
		e.sessionDown.SetLimit(e.AltSpeedDown*SpeedUnit, true)
		e.sessionUp.SetLimit(e.AltSpeedUp*SpeedUnit, true)
		return
	}
	e.sessionDown.SetLimit(e.SpeedLimitDown*SpeedUnit, e.SpeedLimitDownEnabled)
	e.sessionUp.SetLimit(e.SpeedLimitUp*SpeedUnit, e.SpeedLimitUpEnabled)
}

// limitersFor returns the limiters a transfer for t must pass, per direction.
func (e *Engine) limitersFor(t *ManagedTorrent, upload bool) []*bandwidth.Limiter {
	t.mu.RLock()
	honors := t.limits.HonorsSessionLimits
	torrentLimiter := t.downLimiter
	if upload {
		torrentLimiter = t.upLimiter
	}
	t.mu.RUnlock()

	if !honors {
		return []*bandwidth.Limiter{torrentLimiter}
	}
	session := e.sessionDown
	if upload {
		session = e.sessionUp
	}
	return []*bandwidth.Limiter{torrentLimiter, session}
}

// throttle waits until n bytes of piece data may be transferred for t in the given direction.
func (e *Engine) throttle(ctx context.Context, t *ManagedTorrent, upload bool, n int) error {
	return bandwidth.WaitAll(ctx, n, e.limitersFor(t, upload)...)
}
