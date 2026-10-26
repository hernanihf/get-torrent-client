package metainfo

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/hernan/get-torrent/pkg/bencode"
)

var (
	ErrMissingInfoDict    = errors.New("torrent missing 'info' dictionary")
	ErrInvalidPieces      = errors.New("torrent 'pieces' length is not a multiple of 20")
	ErrInvalidPieceLength = errors.New("torrent 'piece length' must be positive")
	ErrPieceCountMismatch = errors.New("torrent piece count does not match total length")
	ErrUnsafePath         = errors.New("torrent contains an unsafe file path")
)

// FileInfo represents a single file inside a multi-file torrent.
type FileInfo struct {
	Length int64
	Path   []string
}

// TorrentMeta holds decoded metadata from a .torrent file.
type TorrentMeta struct {
	Announce     string
	AnnounceList [][]string
	InfoHash     [20]byte
	Name         string
	PieceLength  int64
	Pieces       [][20]byte
	TotalLength  int64
	Files        []FileInfo
	// Private torrents (BEP 0027) must only get peers from their trackers, never the DHT.
	Private bool
	// InfoBytes is the exact bencoded info dictionary; its SHA-1 is InfoHash.
	InfoBytes []byte
}

// Trackers returns all tracker URLs (announce-list tiers flattened, then announce), deduplicated.
func (m *TorrentMeta) Trackers() []string {
	seen := make(map[string]bool)
	var out []string
	add := func(u string) {
		if u != "" && !seen[u] {
			seen[u] = true
			out = append(out, u)
		}
	}
	for _, tier := range m.AnnounceList {
		for _, u := range tier {
			add(u)
		}
	}
	add(m.Announce)
	return out
}

// Marshal produces a .torrent file for this metadata, embedding the original info bytes verbatim.
func (m *TorrentMeta) Marshal() ([]byte, error) {
	if len(m.InfoBytes) == 0 {
		return nil, errors.New("metainfo has no info dictionary bytes")
	}
	dict := map[string]any{"info": bencode.RawMessage(m.InfoBytes)}
	if m.Announce != "" {
		dict["announce"] = m.Announce
	}
	if len(m.AnnounceList) > 0 {
		tiers := make([]any, 0, len(m.AnnounceList))
		for _, tier := range m.AnnounceList {
			tiers = append(tiers, tier)
		}
		dict["announce-list"] = tiers
	}
	return bencode.Encode(dict)
}

// FromInfoBytes builds metadata from a raw info dictionary (e.g. fetched via BEP 0009)
// and a list of tracker URLs. The info_hash is the SHA-1 of the raw bytes.
func FromInfoBytes(info []byte, trackers []string) (*TorrentMeta, error) {
	decoded, err := bencode.Decode(info)
	if err != nil {
		return nil, fmt.Errorf("bencode decode error: %w", err)
	}
	infoDict, ok := decoded.(map[string]any)
	if !ok {
		return nil, errors.New("'info' is not a dictionary")
	}
	meta, err := parseInfo(infoDict)
	if err != nil {
		return nil, err
	}
	meta.InfoBytes = append([]byte(nil), info...)
	meta.InfoHash = sha1.Sum(meta.InfoBytes)
	if len(trackers) > 0 {
		meta.Announce = trackers[0]
		for _, tr := range trackers {
			meta.AnnounceList = append(meta.AnnounceList, []string{tr})
		}
	}
	return meta, nil
}

// Open parses a .torrent file from the local filesystem.
func Open(path string) (*TorrentMeta, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading torrent file: %w", err)
	}
	return Parse(data)
}

// Parse parses raw bytes of a .torrent file.
func Parse(data []byte) (*TorrentMeta, error) {
	decoded, err := bencode.Decode(data)
	if err != nil {
		return nil, fmt.Errorf("bencode decode error: %w", err)
	}

	dict, ok := decoded.(map[string]any)
	if !ok {
		return nil, errors.New("top-level bencode object is not a dictionary")
	}

	announce, _ := dict["announce"].(string)

	infoVal, exists := dict["info"]
	if !exists {
		return nil, ErrMissingInfoDict
	}
	infoDict, ok := infoVal.(map[string]any)
	if !ok {
		return nil, errors.New("'info' is not a dictionary")
	}

	// The info_hash is the SHA-1 of the info dictionary exactly as it appears in the file;
	// re-encoding would change the hash for files with non-canonical encoding.
	infoBytes, err := bencode.RawDictValue(data, "info")
	if err != nil {
		return nil, fmt.Errorf("locating raw info dict: %w", err)
	}

	meta, err := parseInfo(infoDict)
	if err != nil {
		return nil, err
	}
	meta.Announce = announce
	meta.InfoBytes = infoBytes
	meta.InfoHash = sha1.Sum(infoBytes)

	if tiers, ok := dict["announce-list"].([]any); ok {
		for _, tierVal := range tiers {
			tierList, ok := tierVal.([]any)
			if !ok {
				continue
			}
			var tier []string
			for _, u := range tierList {
				if us, ok := u.(string); ok && us != "" {
					tier = append(tier, us)
				}
			}
			if len(tier) > 0 {
				meta.AnnounceList = append(meta.AnnounceList, tier)
			}
		}
	}

	return meta, nil
}

// parseInfo decodes the fields of an info dictionary (BEP 0003).
func parseInfo(infoDict map[string]any) (*TorrentMeta, error) {
	name, _ := infoDict["name"].(string)
	pieceLength, _ := infoDict["piece length"].(int64)
	if pieceLength <= 0 {
		return nil, ErrInvalidPieceLength
	}

	rawPieces, _ := infoDict["pieces"].(string)
	if len(rawPieces)%20 != 0 {
		return nil, ErrInvalidPieces
	}

	numPieces := len(rawPieces) / 20
	pieces := make([][20]byte, numPieces)
	for i := 0; i < numPieces; i++ {
		copy(pieces[i][:], rawPieces[i*20:(i+1)*20])
	}

	private, _ := infoDict["private"].(int64)
	meta := &TorrentMeta{
		Name:        name,
		PieceLength: pieceLength,
		Pieces:      pieces,
		Private:     private == 1,
	}
	// Single file vs Multi file
	if lengthVal, ok := infoDict["length"].(int64); ok {
		meta.TotalLength = lengthVal
		meta.Files = []FileInfo{{Length: lengthVal, Path: []string{name}}}
	} else if filesVal, ok := infoDict["files"].([]any); ok {
		var total int64
		for _, f := range filesVal {
			fDict, ok := f.(map[string]any)
			if !ok {
				continue
			}
			fLen, _ := fDict["length"].(int64)
			total += fLen

			var pathParts []string
			if pList, ok := fDict["path"].([]any); ok {
				for _, p := range pList {
					if pStr, ok := p.(string); ok {
						pathParts = append(pathParts, pStr)
					}
				}
			}
			meta.Files = append(meta.Files, FileInfo{Length: fLen, Path: pathParts})
		}
		meta.TotalLength = total
	}

	// Paths come from untrusted sources (including peers, for magnet links); reject anything
	// that could escape the download directory once joined.
	if !safePathComponent(name) {
		return nil, fmt.Errorf("%w: name %q", ErrUnsafePath, name)
	}
	for _, f := range meta.Files {
		if f.Length < 0 {
			return nil, fmt.Errorf("negative file length %d", f.Length)
		}
		if len(f.Path) == 0 {
			return nil, fmt.Errorf("%w: empty file path", ErrUnsafePath)
		}
		for _, part := range f.Path {
			if !safePathComponent(part) {
				return nil, fmt.Errorf("%w: path component %q", ErrUnsafePath, part)
			}
		}
	}

	if meta.TotalLength <= 0 {
		return nil, errors.New("torrent has no content (total length is zero)")
	}
	if want := (meta.TotalLength + pieceLength - 1) / pieceLength; int64(numPieces) != want {
		return nil, fmt.Errorf("%w: %d pieces for %d bytes at %d per piece", ErrPieceCountMismatch, numPieces, meta.TotalLength, pieceLength)
	}

	return meta, nil
}

// safePathComponent reports whether s is a single, non-traversing path segment.
func safePathComponent(s string) bool {
	if s == "" || s == "." || s == ".." {
		return false
	}
	return !strings.ContainsAny(s, "/\\\x00")
}
