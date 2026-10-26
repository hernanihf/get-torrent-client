package metadata

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"errors"
	"testing"
	"time"

	"github.com/hernan/get-torrent/pkg/peer"
)

func randomInfo(t *testing.T, size int) ([]byte, [20]byte) {
	t.Helper()
	info := make([]byte, size)
	if _, err := rand.Read(info); err != nil {
		t.Fatal(err)
	}
	return info, sha1.Sum(info)
}

func TestAssemblerOutOfOrderPieces(t *testing.T) {
	size := 2*peer.MetadataPieceSize + 1234 // 3 pieces, last one partial
	info, hash := randomInfo(t, size)
	a := NewAssembler(hash)
	if err := a.SetSize(size); err != nil {
		t.Fatal(err)
	}

	for i, idx := range []int{2, 0, 1} {
		piece, _ := Piece(info, idx)
		got, err := a.AddPiece(idx, size, piece)
		if err != nil {
			t.Fatalf("AddPiece(%d): %v", idx, err)
		}
		if i < 2 && got != nil {
			t.Fatalf("metadata reported complete after %d pieces", i+1)
		}
		if i == 2 && !bytes.Equal(got, info) {
			t.Fatalf("assembled metadata does not match original")
		}
	}
	if !a.Complete() || a.Progress() != 1 {
		t.Errorf("expected complete assembler")
	}
	// Late duplicates are ignored.
	piece, _ := Piece(info, 0)
	if got, err := a.AddPiece(0, size, piece); got != nil || err != nil {
		t.Errorf("duplicate after completion: got=%v err=%v", got != nil, err)
	}
}

func TestAssemblerHashMismatchResets(t *testing.T) {
	size := peer.MetadataPieceSize + 10
	info, hash := randomInfo(t, size)
	a := NewAssembler(hash)
	_ = a.SetSize(size)

	p0, _ := Piece(info, 0)
	p1, _ := Piece(info, 1)
	corrupt := append([]byte(nil), p1...)
	corrupt[0] ^= 0xFF

	if _, err := a.AddPiece(0, size, p0); err != nil {
		t.Fatal(err)
	}
	if _, err := a.AddPiece(1, size, corrupt); !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("expected ErrHashMismatch, got %v", err)
	}
	if a.Progress() != 0 {
		t.Fatalf("expected progress reset to 0, got %v", a.Progress())
	}
	// All pieces must be requestable again.
	if reqs := a.NextRequests(10, time.Now()); len(reqs) != 2 {
		t.Fatalf("expected 2 re-requestable pieces, got %v", reqs)
	}
	// And a clean retry succeeds.
	_, _ = a.AddPiece(0, size, p0)
	got, err := a.AddPiece(1, size, p1)
	if err != nil || !bytes.Equal(got, info) {
		t.Fatalf("retry failed: err=%v", err)
	}
}

func TestAssemblerRejectsBadInput(t *testing.T) {
	size := peer.MetadataPieceSize + 10
	a := NewAssembler([20]byte{})

	if _, err := a.AddPiece(0, size, make([]byte, 10)); err == nil {
		t.Errorf("expected error before size is known")
	}
	for _, bad := range []int{0, -1, peer.MaxMetadataSize + 1} {
		if err := a.SetSize(bad); !errors.Is(err, ErrInvalidSize) {
			t.Errorf("SetSize(%d): expected ErrInvalidSize, got %v", bad, err)
		}
	}
	if err := a.SetSize(size); err != nil {
		t.Fatal(err)
	}
	if err := a.SetSize(size); err != nil {
		t.Errorf("repeating the same size must be a no-op, got %v", err)
	}
	if err := a.SetSize(size + 1); !errors.Is(err, ErrSizeMismatch) {
		t.Errorf("expected ErrSizeMismatch, got %v", err)
	}

	cases := []struct {
		name  string
		index int
		total int
		data  int
		want  error
	}{
		{"wrong total_size", 0, size + 1, peer.MetadataPieceSize, ErrSizeMismatch},
		{"index out of range", 2, size, 10, ErrBadPiece},
		{"negative index", -1, size, 10, ErrBadPiece},
		{"short middle piece", 0, size, 100, ErrBadPiece},
		{"long last piece", 1, size, 11, ErrBadPiece},
	}
	for _, tc := range cases {
		if _, err := a.AddPiece(tc.index, tc.total, make([]byte, tc.data)); !errors.Is(err, tc.want) {
			t.Errorf("%s: expected %v, got %v", tc.name, tc.want, err)
		}
	}
}

func TestAssemblerRequestScheduling(t *testing.T) {
	size := 3 * peer.MetadataPieceSize
	a := NewAssembler([20]byte{})
	_ = a.SetSize(size)
	now := time.Now()

	if got := a.NextRequests(2, now); len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("expected [0 1], got %v", got)
	}
	if got := a.NextRequests(10, now); len(got) != 1 || got[0] != 2 {
		t.Fatalf("expected [2], got %v", got)
	}
	if got := a.NextRequests(10, now.Add(time.Second)); len(got) != 0 {
		t.Fatalf("outstanding requests must not be re-issued early, got %v", got)
	}
	a.Rejected(1)
	if got := a.NextRequests(10, now.Add(time.Second)); len(got) != 1 || got[0] != 1 {
		t.Fatalf("rejected piece must be re-requestable, got %v", got)
	}
	if got := a.NextRequests(10, now.Add(RequestTimeout+time.Second)); len(got) != 3 {
		t.Fatalf("timed-out requests must be re-issued, got %v", got)
	}
}

func TestPieceSlicing(t *testing.T) {
	info := make([]byte, peer.MetadataPieceSize+5)
	if p, ok := Piece(info, 0); !ok || len(p) != peer.MetadataPieceSize {
		t.Errorf("piece 0: ok=%v len=%d", ok, len(p))
	}
	if p, ok := Piece(info, 1); !ok || len(p) != 5 {
		t.Errorf("piece 1: ok=%v len=%d", ok, len(p))
	}
	for _, idx := range []int{-1, 2} {
		if _, ok := Piece(info, idx); ok {
			t.Errorf("piece %d should be out of range", idx)
		}
	}
}
