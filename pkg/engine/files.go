package engine

import (
	"fmt"
	"log"

	"github.com/hernan/get-torrent/pkg/metainfo"
	"github.com/hernan/get-torrent/pkg/storage"
)

// FileSelection is the per-file download choice (Transmission's files-wanted and priority-*).
// Both slices have one entry per file in metainfo order.
type FileSelection struct {
	Wanted   []bool
	Priority []int8 // storage.PriorityLow, PriorityNormal or PriorityHigh
}

func defaultSelection(n int) FileSelection {
	sel := FileSelection{Wanted: make([]bool, n), Priority: make([]int8, n)}
	for i := range sel.Wanted {
		sel.Wanted[i] = true
	}
	return sel
}

func (s FileSelection) clone() FileSelection {
	return FileSelection{
		Wanted:   append([]bool(nil), s.Wanted...),
		Priority: append([]int8(nil), s.Priority...),
	}
}

// valid reports whether s fits a torrent with n files.
func (s FileSelection) valid(n int) bool {
	if len(s.Wanted) != n || len(s.Priority) != n {
		return false
	}
	for _, p := range s.Priority {
		if p < storage.PriorityLow || p > storage.PriorityHigh {
			return false
		}
	}
	return true
}

// piecePriorities maps a file selection onto pieces: a piece is wanted if any wanted file
// overlaps it, at the highest priority among those files, as in Transmission.
func piecePriorities(meta *metainfo.TorrentMeta, sel FileSelection) ([]bool, []int8) {
	n := len(meta.Pieces)
	wanted := make([]bool, n)
	prio := make([]int8, n)
	set := make([]bool, n) // prio holds a wanted file's priority
	var offset int64
	for i, f := range meta.Files {
		if f.Length > 0 && sel.Wanted[i] {
			first := offset / meta.PieceLength
			last := (offset + f.Length - 1) / meta.PieceLength
			for p := first; p <= last && p < int64(n); p++ {
				if !set[p] || sel.Priority[i] > prio[p] {
					prio[p] = sel.Priority[i]
				}
				wanted[p], set[p] = true, true
			}
		}
		offset += f.Length
	}
	return wanted, prio
}

// FileSelection returns the torrent's current per-file selection; empty before a magnet has
// its metadata.
func (t *ManagedTorrent) FileSelection() FileSelection {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.PieceManager == nil {
		return FileSelection{}
	}
	return t.selectionLocked().clone()
}

// selectionLocked returns the selection, or the default (every file, normal priority) if none
// was set. Caller holds t.mu.
func (t *ManagedTorrent) selectionLocked() FileSelection {
	if t.files.valid(len(t.Meta.Files)) {
		return t.files
	}
	return defaultSelection(len(t.Meta.Files))
}

// SetFileSelection changes which files are downloaded and their priorities. fn receives a copy
// of the current selection to modify. A seeding torrent that now wants more data resumes
// downloading; a downloading torrent whose remaining wanted data is already present finishes.
func (e *Engine) SetFileSelection(id int, fn func(*FileSelection) error) error {
	t := e.GetTorrentByID(id)
	if t == nil {
		return errTorrentNotFound(id)
	}
	t.mu.Lock()
	pm := t.PieceManager
	if pm == nil {
		t.mu.Unlock()
		return fmt.Errorf("torrent %d has no metadata yet", id)
	}
	sel := t.selectionLocked().clone()
	if err := fn(&sel); err != nil {
		t.mu.Unlock()
		return err
	}
	if !sel.valid(len(t.Meta.Files)) {
		t.mu.Unlock()
		return fmt.Errorf("invalid file selection for torrent %d", id)
	}
	t.files = sel
	pm.SetPiecePriorities(piecePriorities(t.Meta, sel))
	t.Left = pm.Left()
	resume := t.updateDoneStateLocked()
	conns := t.activeConns
	t.mu.Unlock()

	if resume {
		// Peers may have seen no interest from us while we were seeding.
		for _, c := range conns {
			_ = c.SendInterested()
		}
	}
	return nil
}

// updateDoneStateLocked moves a running torrent between downloading and seeding after its
// wanted data changed, reporting whether it went back to downloading. Caller holds t.mu.
func (t *ManagedTorrent) updateDoneStateLocked() bool {
	done := t.PieceManager.IsDone()
	switch {
	case t.State == StateSeeding && !done:
		t.setStateLocked(StateDownloading)
		log.Printf("[Engine] %q: more files selected, downloading again", t.Meta.Name)
		return true
	case t.State == StateDownloading && done:
		t.setStateLocked(StateSeeding)
		t.downloadFinishedLocked()
		log.Printf("[Engine] %q: selected files complete. Seeding now.", t.Meta.Name)
	}
	return false
}
