package security

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

// ClamClient communicates with a ClamAV clamd daemon over TCP using the INSTREAM protocol.
type ClamClient struct {
	addr    string
	timeout time.Duration
}

// NewClamClient creates a client configured to communicate with clamd (e.g. "clamav:3310" or "127.0.0.1:3310").
func NewClamClient(addr string, timeout time.Duration) *ClamClient {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	return &ClamClient{
		addr:    addr,
		timeout: timeout,
	}
}

// Ping checks if the ClamAV daemon is online and responsive.
func (c *ClamClient) Ping() bool {
	conn, err := net.DialTimeout("tcp", c.addr, 2*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Write([]byte("zPING\x00")); err != nil {
		return false
	}

	reply := make([]byte, 8)
	n, err := conn.Read(reply)
	if err != nil {
		return false
	}
	return strings.HasPrefix(string(reply[:n]), "PONG")
}

// ScanFile streams a local file to ClamAV via TCP INSTREAM and returns the verdict.
// Returns (isClean, threatName, error).
func (c *ClamClient) ScanFile(filePath string) (bool, string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return false, "", fmt.Errorf("opening file for scanning: %w", err)
	}
	defer f.Close()

	return c.ScanReader(f)
}

// ScanReader streams bytes from reader to ClamAV daemon and parses the response.
func (c *ClamClient) ScanReader(r io.Reader) (bool, string, error) {
	conn, err := net.DialTimeout("tcp", c.addr, c.timeout)
	if err != nil {
		return false, "", fmt.Errorf("connecting to clamav daemon at %s: %w", c.addr, err)
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(c.timeout))

	// Send INSTREAM command with null-terminator
	if _, err := conn.Write([]byte("zINSTREAM\x00")); err != nil {
		return false, "", fmt.Errorf("initiating INSTREAM: %w", err)
	}

	buf := make([]byte, 32*1024) // 32 KiB chunks
	lenBuf := make([]byte, 4)

	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			binary.BigEndian.PutUint32(lenBuf, uint32(n))
			if _, err := conn.Write(lenBuf); err != nil {
				return false, "", fmt.Errorf("sending chunk length: %w", err)
			}
			if _, err := conn.Write(buf[:n]); err != nil {
				return false, "", fmt.Errorf("sending chunk payload: %w", err)
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return false, "", fmt.Errorf("reading source stream: %w", readErr)
		}
	}

	// Terminate stream with a zero-length chunk
	binary.BigEndian.PutUint32(lenBuf, 0)
	if _, err := conn.Write(lenBuf); err != nil {
		return false, "", fmt.Errorf("sending stream terminator: %w", err)
	}

	// Read response: "stream: OK" or "stream: <ThreatName> FOUND"
	reader := bufio.NewReader(conn)
	resp, err := reader.ReadBytes('\x00')
	if err != nil && !errors.Is(err, io.EOF) {
		return false, "", fmt.Errorf("reading clamav response: %w", err)
	}

	respStr := strings.TrimRight(string(bytes.TrimSpace(resp)), "\x00")
	if strings.HasSuffix(respStr, "OK") {
		return true, "", nil
	}

	if strings.Contains(respStr, "FOUND") {
		// Example: "stream: Win.Trojan.Generic-123 FOUND"
		threat := strings.TrimPrefix(respStr, "stream: ")
		threat = strings.TrimSuffix(threat, " FOUND")
		threat = strings.TrimSpace(threat)
		return false, threat, nil
	}

	return false, "", fmt.Errorf("unexpected clamav response: %q", respStr)
}
