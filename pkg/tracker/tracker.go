package tracker

import (
	"fmt"
	"strings"
)

// Announce queries the appropriate tracker (HTTP or UDP) based on the URL scheme.
func Announce(params AnnounceParams) (*AnnounceResponse, error) {
	urlLower := strings.ToLower(params.AnnounceURL)
	switch {
	case strings.HasPrefix(urlLower, "http://") || strings.HasPrefix(urlLower, "https://"):
		return HTTPAnnounce(params)
	case strings.HasPrefix(urlLower, "udp://"):
		return UDPAnnounce(params)
	default:
		return nil, fmt.Errorf("unsupported tracker protocol in %q", params.AnnounceURL)
	}
}
