package magnet

import (
	"encoding/base32"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

var (
	ErrInvalidMagnetURI = errors.New("invalid magnet URI")
	ErrMissingBTIH      = errors.New("missing or invalid btih xt parameter")
	ErrInvalidHash      = errors.New("invalid info hash in magnet URI")
)

// Magnet represents decoded parameters from a BitTorrent magnet link.
type Magnet struct {
	InfoHash    [20]byte
	DisplayName string
	Trackers    []string
	RawURI      string
}

// Parse extracts info_hash, display name, and tracker URLs from a magnet URI (BEP 0009).
func Parse(uri string) (*Magnet, error) {
	if !strings.HasPrefix(strings.ToLower(uri), "magnet:?") {
		return nil, fmt.Errorf("%w: must begin with 'magnet:?'", ErrInvalidMagnetURI)
	}

	rawQuery := uri[len("magnet:?"):]
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return nil, fmt.Errorf("%w: failed parsing query string: %v", ErrInvalidMagnetURI, err)
	}

	xtValues := values["xt"]
	if len(xtValues) == 0 {
		return nil, ErrMissingBTIH
	}

	var hashBytes []byte
	foundBTIH := false

	for _, xt := range xtValues {
		xtLower := strings.ToLower(xt)
		const prefix = "urn:btih:"
		if strings.HasPrefix(xtLower, prefix) {
			rawHash := xt[len(prefix):]
			rawHash = strings.TrimSpace(rawHash)

			switch len(rawHash) {
			case 40:
				// Hex encoded hash
				decoded, err := hex.DecodeString(rawHash)
				if err != nil {
					return nil, fmt.Errorf("%w: invalid hex encoding: %v", ErrInvalidHash, err)
				}
				hashBytes = decoded
				foundBTIH = true
			case 32:
				// Base32 encoded hash (RFC 4648 without padding)
				b32 := base32.StdEncoding.WithPadding(base32.NoPadding)
				decoded, err := b32.DecodeString(strings.ToUpper(rawHash))
				if err != nil {
					return nil, fmt.Errorf("%w: invalid base32 encoding: %v", ErrInvalidHash, err)
				}
				hashBytes = decoded
				foundBTIH = true
			default:
				return nil, fmt.Errorf("%w: expected 40 hex chars or 32 base32 chars, got %d chars", ErrInvalidHash, len(rawHash))
			}
			break
		}
	}

	if !foundBTIH || len(hashBytes) != 20 {
		return nil, ErrMissingBTIH
	}

	var infoHash [20]byte
	copy(infoHash[:], hashBytes)

	displayName := values.Get("dn")
	trackers := values["tr"]

	// Deduplicate trackers while preserving order
	uniqueTrackers := make([]string, 0, len(trackers))
	seen := make(map[string]bool, len(trackers))
	for _, tr := range trackers {
		tr = strings.TrimSpace(tr)
		if tr != "" && !seen[tr] {
			seen[tr] = true
			uniqueTrackers = append(uniqueTrackers, tr)
		}
	}

	return &Magnet{
		InfoHash:    infoHash,
		DisplayName: displayName,
		Trackers:    uniqueTrackers,
		RawURI:      uri,
	}, nil
}
