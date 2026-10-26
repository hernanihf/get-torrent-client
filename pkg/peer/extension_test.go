package peer

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/hernan/get-torrent/pkg/tracker"
)

func TestExtHandshakeRoundTrip(t *testing.T) {
	msg := FormatExtHandshake(31337)
	if msg.ID == nil || *msg.ID != MsgExtended {
		t.Fatalf("expected MsgExtended, got %v", msg.ID)
	}

	// Round-trip through the wire format.
	parsed, err := ReadMessage(bytes.NewReader(msg.Serialize()))
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	extID, payload, err := ParseExtended(parsed)
	if err != nil || extID != ExtHandshakeID {
		t.Fatalf("ParseExtended: id=%d err=%v", extID, err)
	}
	hs, err := ParseExtHandshake(payload)
	if err != nil {
		t.Fatalf("ParseExtHandshake: %v", err)
	}
	if hs.M[ExtUTMetadata] != LocalUTMetadataID {
		t.Errorf("expected ut_metadata id %d, got %d", LocalUTMetadataID, hs.M[ExtUTMetadata])
	}
	if hs.MetadataSize != 31337 {
		t.Errorf("expected metadata_size 31337, got %d", hs.MetadataSize)
	}
}

func TestExtHandshakeOmitsUnknownMetadataSize(t *testing.T) {
	_, payload, _ := ParseExtended(FormatExtHandshake(0))
	if bytes.Contains(payload, []byte("metadata_size")) {
		t.Errorf("metadata_size must be omitted when unknown: %q", payload)
	}
}

func TestParseExtHandshakeFiltersInvalidValues(t *testing.T) {
	// ut_metadata disabled (0), an out-of-range ID, and an oversized metadata_size.
	payload := []byte("d1:md11:ut_metadatai0e6:ut_pexi300e5:ut_xxi7ee13:metadata_sizei999999999ee")
	hs, err := ParseExtHandshake(payload)
	if err != nil {
		t.Fatalf("ParseExtHandshake: %v", err)
	}
	if _, ok := hs.M["ut_metadata"]; ok {
		t.Errorf("ID 0 means disabled and must not be recorded")
	}
	if _, ok := hs.M["ut_pex"]; ok {
		t.Errorf("IDs > 255 must be rejected")
	}
	if hs.M["ut_xx"] != 7 {
		t.Errorf("expected ut_xx=7, got %d", hs.M["ut_xx"])
	}
	if hs.MetadataSize != 0 {
		t.Errorf("oversized metadata_size must be ignored, got %d", hs.MetadataSize)
	}

	for _, bad := range [][]byte{nil, []byte("i42e"), []byte("d1:m")} {
		if _, err := ParseExtHandshake(bad); err == nil {
			t.Errorf("expected error for %q", bad)
		}
	}
}

func TestMetadataMessages(t *testing.T) {
	data := bytes.Repeat([]byte{0xAB}, 100)

	tests := []struct {
		name string
		msg  *Message
		want MetadataMsg
	}{
		{"request", FormatMetadataRequest(3, 5), MetadataMsg{Type: MetadataRequest, Piece: 5}},
		{"reject", FormatMetadataReject(3, 1), MetadataMsg{Type: MetadataReject, Piece: 1}},
		{"data", FormatMetadataData(3, 2, 40000, data), MetadataMsg{Type: MetadataData, Piece: 2, TotalSize: 40000, Data: data}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			extID, payload, err := ParseExtended(tc.msg)
			if err != nil || extID != 3 {
				t.Fatalf("ParseExtended: id=%d err=%v", extID, err)
			}
			got, err := ParseMetadataMsg(payload)
			if err != nil {
				t.Fatalf("ParseMetadataMsg: %v", err)
			}
			if got.Type != tc.want.Type || got.Piece != tc.want.Piece || got.TotalSize != tc.want.TotalSize || !bytes.Equal(got.Data, tc.want.Data) {
				t.Errorf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestParseMetadataMsgRejectsMalformed(t *testing.T) {
	for _, payload := range []string{
		"",
		"le",
		"d8:msg_typei0ee",            // missing piece
		"d8:msg_typei0e5:piecei-1ee", // negative piece
		"d8:msg_typei1e5:piecei0ee",  // data without total_size
		"d8:msg_typei1e5:piecei0e10:total_sizei-5ee",       // negative total_size
		"d8:msg_typei1e5:piecei0e10:total_sizei99999999ee", // oversized
	} {
		if _, err := ParseMetadataMsg([]byte(payload)); err == nil {
			t.Errorf("expected error for %q", payload)
		}
	}
	if _, _, err := ParseExtended(&Message{ID: idOf(MsgExtended)}); err == nil {
		t.Errorf("expected error for empty extended payload")
	}
	if _, _, err := ParseExtended(&Message{ID: idOf(MsgHave), Payload: []byte{0}}); err == nil {
		t.Errorf("expected error for non-extended message")
	}
}

func TestConnRecordsExtHandshakeDuringDial(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	infoHash := [20]byte{9}
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		if _, err := ReadHandshake(c); err != nil {
			return
		}
		_, _ = c.Write(NewHandshake(infoHash, [20]byte{1}).Serialize())
		// Extension handshake first, instead of a bitfield.
		_, _ = c.Write(FormatExtHandshake(20000).Serialize())
		time.Sleep(100 * time.Millisecond)
	}()

	addr := ln.Addr().(*net.TCPAddr)
	conn, err := Dial(peerAt(addr), infoHash, [20]byte{2}, 2*time.Second)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	if !conn.SupportsExtensions {
		t.Errorf("expected SupportsExtensions")
	}
	id, ok := conn.PeerExtID(ExtUTMetadata)
	if !ok || id != LocalUTMetadataID {
		t.Errorf("expected ut_metadata id %d, got %d (ok=%v)", LocalUTMetadataID, id, ok)
	}
	if conn.PeerExt.MetadataSize != 20000 {
		t.Errorf("expected metadata size 20000, got %d", conn.PeerExt.MetadataSize)
	}
}

func TestUpdateStateGrowsBitfieldOnHave(t *testing.T) {
	c := &Conn{}
	c.UpdateState(FormatHave(17))
	if !c.Bitfield.HasPiece(17) {
		t.Fatalf("expected piece 17 after have on empty bitfield")
	}
	if len(c.Bitfield) != 3 {
		t.Errorf("expected 3-byte bitfield, got %d", len(c.Bitfield))
	}
	// Absurd indices must not allocate.
	c.UpdateState(FormatHave(1 << 30))
	if len(c.Bitfield) != 3 {
		t.Errorf("bitfield grew for absurd index: %d bytes", len(c.Bitfield))
	}
}

func idOf(id MessageID) *MessageID { return &id }

func peerAt(addr *net.TCPAddr) tracker.Peer {
	return tracker.Peer{IP: addr.IP, Port: uint16(addr.Port)}
}
