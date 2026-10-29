package engine

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hernan/get-torrent/pkg/metainfo"
	"github.com/hernan/get-torrent/pkg/peer"
)

// fastSeeder starts a complete seeder for a 4-block single-piece-per-block torrent.
func fastSeeder(t *testing.T) (*Engine, *ManagedTorrent, *metainfo.TorrentMeta) {
	t.Helper()
	payload := make([]byte, 4*16384)
	_, _ = rand.Read(payload)
	meta, err := metainfo.FromInfoBytes(buildTorrent(t, "fast.bin", payload, 16384), nil)
	if err != nil {
		t.Fatal(err)
	}
	seeder := newTestEngine(t)
	if err := seeder.StartListener(); err != nil {
		t.Fatal(err)
	}
	meta.Announce = "http://127.0.0.1:1/announce"
	data, _ := meta.Marshal()
	st, err := seeder.AddTorrentBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	seedAll(t, st, payload)
	if err := seeder.StartTorrent(st.ID); err != nil {
		t.Fatal(err)
	}
	return seeder, st, meta
}

func (p *rawPeer) first() (peer.MessageID, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.got) == 0 {
		return 0, false
	}
	return p.got[0], true
}

func TestFastSeederSendsHaveAllAndRejects(t *testing.T) {
	seeder, _, meta := fastSeeder(t)
	p := dialRawPeer(t, seeder.Port, meta.InfoHash, 0) // NewHandshake sets the Fast bit
	waitFor(t, 2*time.Second, "first message", func() bool { _, ok := p.first(); return ok })
	if id, _ := p.first(); id != peer.MsgHaveAll {
		t.Errorf("a complete seeder must send Have All to a Fast peer, got %d", id)
	}

	// Not interested, so still choked: the request is rejected rather than ignored.
	p.send(t, peer.FormatRequest(0, 0, 16384))
	waitFor(t, 2*time.Second, "reject while choked", func() bool { return p.received(peer.MsgRejectRequest) })
	if p.received(peer.MsgPiece) {
		t.Errorf("a choked peer must not be served")
	}
}

func TestFastCancelIsAnsweredWithReject(t *testing.T) {
	seeder, st, meta := fastSeeder(t)
	if err := seeder.SetTorrentLimits(st.ID, func(l *TorrentLimits) { l.UploadLimit, l.UploadLimited = 16, true }); err != nil {
		t.Fatal(err)
	}
	p := dialRawPeer(t, seeder.Port, meta.InfoHash, 0)
	p.send(t, &peer.Message{ID: idPtr(peer.MsgInterested)})
	waitFor(t, 3*time.Second, "unchoke", p.unchoked)

	for i := range 4 {
		p.send(t, peer.FormatRequest(i, 0, 16384))
	}
	p.send(t, peer.FormatCancel(2, 0, 16384))
	p.send(t, peer.FormatCancel(3, 0, 16384))
	// Every request gets exactly one answer: two pieces and two rejects.
	waitFor(t, 5*time.Second, "answers", func() bool {
		return p.count(peer.MsgPiece) == 2 && p.count(peer.MsgRejectRequest) == 2
	})
	time.Sleep(1500 * time.Millisecond)
	if p.count(peer.MsgPiece) != 2 || p.count(peer.MsgRejectRequest) != 2 {
		t.Errorf("got %d pieces and %d rejects, want 2 and 2", p.count(peer.MsgPiece), p.count(peer.MsgRejectRequest))
	}
}

func TestNonFastPeerGetsBitfieldAndNoRejects(t *testing.T) {
	seeder, _, meta := fastSeeder(t)
	c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", seeder.Port), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	hs := peer.NewHandshake(meta.InfoHash, [20]byte{'-', 'N', 'F', '0', '0', '0', '1', '-'})
	hs.Reserved[7] &^= 0x04 // no Fast Extension
	if _, err := c.Write(hs.Serialize()); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := peer.ReadHandshake(c); err != nil {
		t.Fatal(err)
	}
	msg, err := peer.ReadMessage(c)
	if err != nil || msg.ID == nil || *msg.ID != peer.MsgBitfield {
		t.Fatalf("a non-Fast peer must get a bitfield first, got %v (%v)", msg, err)
	}
	if _, err := c.Write(peer.FormatRequest(0, 0, 16384).Serialize()); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	for {
		msg, err := peer.ReadMessage(c)
		if err != nil {
			break // timeout: nothing more
		}
		if msg.ID != nil && *msg.ID == peer.MsgRejectRequest {
			t.Fatal("non-Fast peers must never receive Reject Request")
		}
	}
}

// scriptedSeeder is a raw remote seeder for one torrent. On the first request it either rejects
// it (fast) or chokes and unchokes again (not fast); either way the leecher must ask again.
func scriptedSeeder(t *testing.T, meta *metainfo.TorrentMeta, payload []byte, fast bool) *net.TCPAddr {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		if _, err := peer.ReadHandshake(c); err != nil {
			return
		}
		hs := peer.NewHandshake(meta.InfoHash, [20]byte{'-', 'S', 'S', '0', '0', '0', '1', '-'})
		if !fast {
			hs.Reserved[7] &^= 0x04
		}
		_, _ = c.Write(hs.Serialize())
		if fast {
			_, _ = c.Write(peer.FormatHaveAll().Serialize())
		} else {
			_, _ = c.Write(peer.FormatBitfield(fullBitfield(len(meta.Pieces))).Serialize())
		}
		_, _ = c.Write((&peer.Message{ID: idPtr(peer.MsgUnchoke)}).Serialize())
		tricked := false
		for {
			msg, err := peer.ReadMessage(c)
			if err != nil {
				return
			}
			if msg.ID == nil || *msg.ID != peer.MsgRequest {
				continue
			}
			idx, begin, length, _ := peer.ParseRequest(msg)
			if !tricked {
				tricked = true
				if fast {
					_, _ = c.Write(peer.FormatReject(idx, begin, length).Serialize())
				} else {
					_, _ = c.Write((&peer.Message{ID: idPtr(peer.MsgChoke)}).Serialize())
					_, _ = c.Write((&peer.Message{ID: idPtr(peer.MsgUnchoke)}).Serialize())
				}
				continue
			}
			off := idx*int(meta.PieceLength) + begin
			_, _ = c.Write(peer.FormatPiece(idx, begin, payload[off:off+length]).Serialize())
		}
	}()
	return ln.Addr().(*net.TCPAddr)
}

func TestLeecherReRequestsRejectedAndDiscardedBlocks(t *testing.T) {
	for _, fast := range []bool{true, false} {
		t.Run(fmt.Sprintf("fast=%v", fast), func(t *testing.T) {
			payload := make([]byte, 16384) // one block: it must be requested again to finish
			_, _ = rand.Read(payload)
			meta, err := metainfo.FromInfoBytes(buildTorrent(t, "retry.bin", payload, 16384), nil)
			if err != nil {
				t.Fatal(err)
			}
			addr := scriptedSeeder(t, meta, payload, fast)
			tr := fakeTracker(t, addr)
			defer tr.Close()
			meta.Announce = tr.URL + "/announce"
			data, _ := meta.Marshal()

			leecher := newTestEngine(t)
			lt, err := leecher.AddTorrentBytes(data)
			if err != nil {
				t.Fatal(err)
			}
			if err := leecher.StartTorrent(lt.ID); err != nil {
				t.Fatal(err)
			}
			waitFor(t, 5*time.Second, "download after the block was refused once", func() bool {
				return lt.Status().PercentDone == 1
			})
			got, err := os.ReadFile(filepath.Join(leecher.DownloadDir, "retry.bin"))
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("data mismatch (err=%v)", err)
			}
		})
	}
}
