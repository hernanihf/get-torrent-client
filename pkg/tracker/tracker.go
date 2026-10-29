package tracker

import (
	"context"
	"fmt"
	"strings"
)

// Event is the announce event (BEP 0003 "event" key). The numeric values are the BEP 0015 codes.
type Event int32

const (
	EventNone      Event = 0
	EventCompleted Event = 1
	EventStarted   Event = 2
	EventStopped   Event = 3
)

// String returns the HTTP "event" value; empty for EventNone.
func (e Event) String() string {
	switch e {
	case EventCompleted:
		return "completed"
	case EventStarted:
		return "started"
	case EventStopped:
		return "stopped"
	default:
		return ""
	}
}

// AnnounceParams represents parameters sent in an announce request.
type AnnounceParams struct {
	AnnounceURL string
	InfoHash    [20]byte
	PeerID      [20]byte
	Port        uint16
	Uploaded    int64
	Downloaded  int64
	Left        int64
	Compact     bool
	Event       Event
	// Key identifies this client across IP changes; constant for the session.
	Key uint32
	// NumWant is the number of peers requested; 0 lets the tracker choose.
	NumWant int
	// TrackerID echoes the "tracker id" an HTTP tracker returned previously.
	TrackerID string
}

// AnnounceResponse represents the parsed response from an announce request.
type AnnounceResponse struct {
	Interval    int64 // seconds; 0 if the tracker did not say
	MinInterval int64 // seconds; 0 if the tracker did not say
	TrackerID   string
	Warning     string
	Seeders     int64 // -1 if unknown
	Leechers    int64 // -1 if unknown
	Peers       []Peer
}

// Announce queries the appropriate tracker (HTTP or UDP) based on the URL scheme.
func Announce(params AnnounceParams) (*AnnounceResponse, error) {
	return AnnounceContext(context.Background(), params)
}

// AnnounceContext is Announce with cancellation: ctx bounds the whole exchange, retries included.
func AnnounceContext(ctx context.Context, params AnnounceParams) (*AnnounceResponse, error) {
	urlLower := strings.ToLower(params.AnnounceURL)
	switch {
	case strings.HasPrefix(urlLower, "http://") || strings.HasPrefix(urlLower, "https://"):
		return httpAnnounce(ctx, params, defaultHTTPClient)
	case strings.HasPrefix(urlLower, "udp://"):
		return UDPAnnounceContext(ctx, params)
	default:
		return nil, fmt.Errorf("unsupported tracker protocol in %q", params.AnnounceURL)
	}
}
