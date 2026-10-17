package metainfo

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"os"

	"github.com/hernan/get-torrent/pkg/bencode"
)

var (
	ErrMissingInfoDict = errors.New("torrent missing 'info' dictionary")
	ErrInvalidPieces   = errors.New("torrent 'pieces' length is not a multiple of 20")
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

	// Calculate 20-byte SHA-1 info_hash from the raw bencoded 'info' dictionary
	infoEncoded, err := bencode.Encode(infoVal)
	if err != nil {
		return nil, fmt.Errorf("failed re-encoding info dict for hash: %w", err)
	}
	infoHash := sha1.Sum(infoEncoded)

	infoDict, ok := infoVal.(map[string]any)
	if !ok {
		return nil, errors.New("'info' is not a dictionary")
	}

	name, _ := infoDict["name"].(string)
	pieceLength, _ := infoDict["piece length"].(int64)

	rawPieces, _ := infoDict["pieces"].(string)
	if len(rawPieces)%20 != 0 {
		return nil, ErrInvalidPieces
	}

	numPieces := len(rawPieces) / 20
	pieces := make([][20]byte, numPieces)
	for i := 0; i < numPieces; i++ {
		copy(pieces[i][:], rawPieces[i*20:(i+1)*20])
	}

	meta := &TorrentMeta{
		Announce:    announce,
		InfoHash:    infoHash,
		Name:        name,
		PieceLength: pieceLength,
		Pieces:      pieces,
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

	return meta, nil
}
