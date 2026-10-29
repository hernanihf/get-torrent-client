package rpc

import (
	"crypto/rand"
	"crypto/sha1"
	"strings"
	"testing"

	"github.com/hernan/get-torrent/pkg/bencode"
)

// makeMultiFileTorrent builds a .torrent with three 1000-byte files in one 16 KiB piece.
func makeMultiFileTorrent(t *testing.T) []byte {
	t.Helper()
	data := make([]byte, 3000)
	_, _ = rand.Read(data)
	sum := sha1.Sum(data)
	files := []any{}
	for _, name := range []string{"a.bin", "b.bin", "c.bin"} {
		files = append(files, map[string]any{"length": 1000, "path": []any{name}})
	}
	torrent, err := bencode.Encode(map[string]any{
		"announce": "http://tracker.example.org/announce",
		"info":     map[string]any{"name": "multi", "files": files, "piece length": 16384, "pieces": string(sum[:])},
	})
	if err != nil {
		t.Fatal(err)
	}
	return torrent
}

func intsOf(v any) []int {
	var out []int
	for _, x := range v.([]any) {
		out = append(out, int(x.(float64)))
	}
	return out
}

func TestTorrentSetFileSelection(t *testing.T) {
	env := newEnv(t, Options{})
	env.mustSucceed(t, "torrent-add", map[string]any{"metainfo": b64(makeMultiFileTorrent(t)), "paused": true})

	env.mustSucceed(t, "torrent-set", map[string]any{"ids": []any{1}, "files-unwanted": []any{1}, "priority-high": []any{2}, "priority-low": []any{0}})
	tor := env.torrents(t, map[string]any{"fields": []any{"wanted", "priorities", "fileStats"}})[0]
	if got := intsOf(tor["wanted"]); got[0] != 1 || got[1] != 0 || got[2] != 1 {
		t.Errorf("wanted = %v", got)
	}
	if got := intsOf(tor["priorities"]); got[0] != -1 || got[1] != 0 || got[2] != 1 {
		t.Errorf("priorities = %v", got)
	}
	if fs := tor["fileStats"].([]any)[1].(map[string]any); fs["wanted"] != false {
		t.Errorf("fileStats = %v", fs)
	}

	// An empty list means every file.
	env.mustSucceed(t, "torrent-set", map[string]any{"ids": []any{1}, "files-wanted": []any{}, "priority-normal": []any{}})
	tor = env.torrents(t, map[string]any{"fields": []any{"wanted", "priorities"}})[0]
	if got := intsOf(tor["wanted"]); got[1] != 1 {
		t.Errorf("empty files-wanted must select every file: %v", got)
	}
	if got := intsOf(tor["priorities"]); got[0] != 0 || got[2] != 0 {
		t.Errorf("empty priority-normal must reset every file: %v", got)
	}

	for _, args := range []map[string]any{
		{"files-wanted": []any{3}},
		{"files-wanted": []any{-1}},
		{"files-wanted": []any{0.5}},
		{"files-unwanted": "0"},
	} {
		args["ids"] = []any{1}
		if r := env.call(t, "torrent-set", args); r.Result == "success" || !strings.Contains(r.Result, "files-") {
			t.Errorf("torrent-set %v must fail naming the key, got %q", args, r.Result)
		}
	}
}

func TestTorrentAddWithFileSelection(t *testing.T) {
	env := newEnv(t, Options{})
	env.mustSucceed(t, "torrent-add", map[string]any{"metainfo": b64(makeMultiFileTorrent(t)), "paused": true, "files-unwanted": []any{0, 2}})
	tor := env.torrents(t, map[string]any{"fields": []any{"wanted", "sizeWhenDone", "totalSize", "leftUntilDone"}})[0]
	if got := intsOf(tor["wanted"]); got[0] != 0 || got[1] != 1 || got[2] != 0 {
		t.Errorf("wanted = %v", got)
	}
	// One piece holds all three files, so the wanted size is still the whole piece.
	if tor["sizeWhenDone"] != float64(3000) || tor["leftUntilDone"] != float64(3000) {
		t.Errorf("unexpected sizes: %v", tor)
	}

	if r := env.call(t, "torrent-add", map[string]any{"metainfo": b64(makeMultiFileTorrent(t)), "files-wanted": []any{7}}); r.Result == "success" {
		t.Errorf("invalid file index must fail torrent-add")
	}
	if got := env.torrents(t, map[string]any{"fields": []any{"id"}}); len(got) != 1 {
		t.Errorf("a rejected torrent-add must not leave the torrent behind: %v", got)
	}
}
