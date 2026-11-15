package security

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHeuristicsCleanFile(t *testing.T) {
	tmpDir := t.TempDir()
	cleanFile := filepath.Join(tmpDir, "video.mp4")
	_ = os.WriteFile(cleanFile, []byte{0x00, 0x00, 0x00, 0x18, 'f', 't', 'y', 'p'}, 0644)

	res, err := InspectFile(cleanFile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Level != RiskClean {
		t.Errorf("expected clean, got %s (%s)", res.Level, res.Threat)
	}
}

func TestHeuristicsCamouflagedPE(t *testing.T) {
	tmpDir := t.TempDir()
	fakeMovie := filepath.Join(tmpDir, "movie.mp4")
	// "MZ" header placed into an .mp4 file
	_ = os.WriteFile(fakeMovie, []byte{'M', 'Z', 0x90, 0x00, 0x03, 0x00, 0x00, 0x00}, 0644)

	res, err := InspectFile(fakeMovie)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Level != RiskCritical || !strings.Contains(res.Threat, "WindowsPE") {
		t.Errorf("expected Critical WindowsPE threat, got %+v", res)
	}
}

func TestHeuristicsCamouflagedELF(t *testing.T) {
	tmpDir := t.TempDir()
	fakeMusic := filepath.Join(tmpDir, "track.mp3")
	// "\x7fELF" header placed into an .mp3 file
	_ = os.WriteFile(fakeMusic, []byte{0x7F, 'E', 'L', 'F', 0x02, 0x01, 0x01, 0x00}, 0644)

	res, err := InspectFile(fakeMusic)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Level != RiskCritical || !strings.Contains(res.Threat, "LinuxELF") {
		t.Errorf("expected Critical LinuxELF threat, got %+v", res)
	}
}

func TestHeuristicsDoubleExtension(t *testing.T) {
	tmpDir := t.TempDir()
	fakeDoc := filepath.Join(tmpDir, "invoice.pdf.exe")
	_ = os.WriteFile(fakeDoc, []byte("harmless content"), 0644)

	res, err := InspectFile(fakeDoc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Level != RiskCritical || !strings.Contains(res.Threat, "DoubleExtension") {
		t.Errorf("expected DoubleExtension threat, got %+v", res)
	}
}

func TestClamAVMockServer(t *testing.T) {
	// Start an in-memory listener to mock ClamAV daemon
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skip("skipping TCP test in restricted sandbox:", err)
		return
	}
	defer ln.Close()

	addr := ln.Addr().String()

	// Handle mock clamd protocol
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				br := bufio.NewReader(c)
				cmdBytes, err := br.ReadBytes('\x00')
				if err != nil {
					return
				}
				cmd := string(cmdBytes)
				if strings.HasPrefix(cmd, "zPING") {
					_, _ = c.Write([]byte("PONG\x00"))
					return
				}
				if strings.HasPrefix(cmd, "zINSTREAM") {
					totalStream := new(bytes.Buffer)
					chunkLenBuf := make([]byte, 4)
					for {
						if _, err := io.ReadFull(br, chunkLenBuf); err != nil {
							break
						}
						length := binary.BigEndian.Uint32(chunkLenBuf)
						if length == 0 {
							break // End of stream
						}
						chunk := make([]byte, length)
						if _, err := io.ReadFull(br, chunk); err != nil {
							break
						}
						totalStream.Write(chunk)
					}

					if strings.Contains(totalStream.String(), "EICAR-STANDARD-ANTIVIRUS-TEST-FILE") {
						_, _ = c.Write([]byte("stream: Eicar-Test-Signature FOUND\x00"))
					} else {
						_, _ = c.Write([]byte("stream: OK\x00"))
					}
				}
			}(conn)
		}
	}()

	client := NewClamClient(addr, 2*time.Second)

	// Test Ping
	if !client.Ping() {
		t.Fatalf("expected Ping to succeed")
	}

	// Test clean stream
	cleanReader := strings.NewReader("Hello, safe data stream!")
	isClean, threat, err := client.ScanReader(cleanReader)
	if err != nil || !isClean || threat != "" {
		t.Fatalf("expected clean scan, got isClean=%v threat=%q err=%v", isClean, threat, err)
	}

	// Test infected stream (EICAR signature)
	eicarReader := strings.NewReader("X5O!P%@AP[4\\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*")
	isClean, threat, err = client.ScanReader(eicarReader)
	if err != nil {
		t.Fatalf("unexpected error scanning eicar: %v", err)
	}
	if isClean || threat != "Eicar-Test-Signature" {
		t.Fatalf("expected Eicar-Test-Signature, got isClean=%v, threat=%s", isClean, threat)
	}
}

func TestScannerWorkflow(t *testing.T) {
	tmpDir := t.TempDir()
	cleanFile := filepath.Join(tmpDir, "safe.txt")
	_ = os.WriteFile(cleanFile, []byte("clean plain text"), 0644)

	badFile := filepath.Join(tmpDir, "trojan.mp4")
	_ = os.WriteFile(badFile, []byte{'M', 'Z', 0x90, 0x00}, 0644)

	scanner := NewScanner(Config{
		Enabled:        true,
		ClamAVAddr:     "", // Disabled ClamAV to test pure heuristic mode
		ActionOnThreat: ActionPause,
	})

	// 1. Scan safe file
	reportSafe := scanner.ScanFiles([]string{cleanFile})
	if reportSafe.Status != StatusClean {
		t.Errorf("expected clean status, got %s", reportSafe.Status)
	}

	// 2. Scan bad file
	reportBad := scanner.ScanFiles([]string{badFile})
	if reportBad.Status != StatusThreatDetected || !strings.Contains(reportBad.ThreatName, "WindowsPE") {
		t.Errorf("expected threat detected, got %+v", reportBad)
	}
}
