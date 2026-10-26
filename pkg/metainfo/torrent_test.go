package metainfo

import (
	"crypto/sha1"
	"errors"
	"strings"
	"testing"
)

// A single-file info dict with 2 pieces of 16 KiB covering 20000 bytes.
const sampleInfo = "d6:lengthi20000e4:name8:file.bin12:piece lengthi16384e6:pieces40:" +
	"AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBe"

func TestParseHashesRawInfoBytes(t *testing.T) {
	// Non-canonical: the info dict keys are unsorted, so re-encoding would change the hash.
	info := "d4:name8:file.bin6:lengthi20000e12:piece lengthi16384e6:pieces40:" +
		"AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBe"
	data := "d8:announce12:http://tr/an4:info" + info + "e"

	meta, err := Parse([]byte(data))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if meta.InfoHash != sha1.Sum([]byte(info)) {
		t.Errorf("info_hash must be SHA-1 of the raw info bytes")
	}
	if string(meta.InfoBytes) != info {
		t.Errorf("InfoBytes must be the verbatim info dict")
	}
	if meta.TotalLength != 20000 || len(meta.Pieces) != 2 || meta.Name != "file.bin" {
		t.Errorf("unexpected meta: %+v", meta)
	}
}

func TestParseAnnounceListAndTrackers(t *testing.T) {
	data := "d8:announce9:http://a/13:announce-listll9:http://a/9:http://b/el7:udp://cee4:info" + sampleInfo + "e"
	meta, err := Parse([]byte(data))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(meta.AnnounceList) != 2 || len(meta.AnnounceList[0]) != 2 {
		t.Fatalf("unexpected announce-list: %v", meta.AnnounceList)
	}
	got := strings.Join(meta.Trackers(), ",")
	if got != "http://a/,http://b/,udp://c" {
		t.Errorf("Trackers() = %s", got)
	}
}

func TestMarshalRoundTrip(t *testing.T) {
	meta, err := FromInfoBytes([]byte(sampleInfo), []string{"http://a/", "udp://b"})
	if err != nil {
		t.Fatalf("FromInfoBytes: %v", err)
	}
	if meta.InfoHash != sha1.Sum([]byte(sampleInfo)) {
		t.Fatalf("FromInfoBytes must hash raw bytes")
	}
	data, err := meta.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	back, err := Parse(data)
	if err != nil {
		t.Fatalf("Parse(Marshal()): %v", err)
	}
	if back.InfoHash != meta.InfoHash {
		t.Errorf("info_hash changed across Marshal/Parse")
	}
	if strings.Join(back.Trackers(), ",") != "http://a/,udp://b" {
		t.Errorf("trackers lost: %v", back.Trackers())
	}

	if _, err := (&TorrentMeta{}).Marshal(); err == nil {
		t.Errorf("Marshal without InfoBytes must fail")
	}
}

func TestRejectsUnsafePaths(t *testing.T) {
	pieces := "6:pieces20:AAAAAAAAAAAAAAAAAAAA"
	cases := map[string]string{
		"dotdot name":     "d6:lengthi10e4:name2:..12:piece lengthi16e" + pieces + "e",
		"slash in name":   "d6:lengthi10e4:name5:a/b/c12:piece lengthi16e" + pieces + "e",
		"traversal path":  "d5:filesld6:lengthi10e4:pathl2:..6:passwdeee4:name1:x12:piece lengthi16e" + pieces + "e",
		"absolute path":   "d5:filesld6:lengthi10e4:pathl4:/etceee4:name1:x12:piece lengthi16e" + pieces + "e",
		"empty component": "d5:filesld6:lengthi10e4:pathl0:eee4:name1:x12:piece lengthi16e" + pieces + "e",
		"backslash":       "d5:filesld6:lengthi10e4:pathl5:..\\abeee4:name1:x12:piece lengthi16e" + pieces + "e",
	}
	for name, info := range cases {
		if _, err := FromInfoBytes([]byte(info), nil); !errors.Is(err, ErrUnsafePath) {
			t.Errorf("%s: expected ErrUnsafePath, got %v", name, err)
		}
	}

	ok := "d5:filesld6:lengthi10e4:pathl3:sub8:file.txteee4:name1:x12:piece lengthi16e" + pieces + "e"
	if _, err := FromInfoBytes([]byte(ok), nil); err != nil {
		t.Errorf("valid nested path rejected: %v", err)
	}
}

func TestRejectsInconsistentPieces(t *testing.T) {
	cases := map[string]string{
		"zero piece length": "d6:lengthi10e4:name1:x12:piece lengthi0e6:pieces20:AAAAAAAAAAAAAAAAAAAAe",
		"too few pieces":    "d6:lengthi40e4:name1:x12:piece lengthi16e6:pieces20:AAAAAAAAAAAAAAAAAAAAe",
		"no content":        "d6:lengthi0e4:name1:x12:piece lengthi16e6:pieces0:e",
		"ragged pieces":     "d6:lengthi10e4:name1:x12:piece lengthi16e6:pieces3:abce",
	}
	for name, info := range cases {
		if _, err := FromInfoBytes([]byte(info), nil); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
