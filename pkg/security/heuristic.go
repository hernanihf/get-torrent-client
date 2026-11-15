package security

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// HeuristicRiskLevel indicates the risk assessment of a file.
type HeuristicRiskLevel string

const (
	RiskClean      HeuristicRiskLevel = "clean"
	RiskSuspicious HeuristicRiskLevel = "suspicious"
	RiskCritical   HeuristicRiskLevel = "critical"
)

// HeuristicResult stores findings from static inspection.
type HeuristicResult struct {
	Level       HeuristicRiskLevel
	Threat      string
	Description string
}

// Magic bytes for popular binary executable formats:
var (
	magicPE    = []byte{0x4D, 0x5A}             // Windows PE ("MZ")
	magicELF   = []byte{0x7F, 0x45, 0x4C, 0x46} // Linux ELF ("\x7fELF")
	magicMachO = [][]byte{
		{0xFE, 0xED, 0xFA, 0xCE}, // Mach-O 32-bit (Big Endian)
		{0xCE, 0xFA, 0xED, 0xFE}, // Mach-O 32-bit (Little Endian)
		{0xFE, 0xED, 0xFA, 0xCF}, // Mach-O 64-bit (Big Endian)
		{0xcf, 0xfa, 0xed, 0xfe}, // Mach-O 64-bit (Little Endian)
		{0xCA, 0xFE, 0xBA, 0xBE}, // Mach-O Universal / Fat Binary
	}
)

// Safe media and document extensions that should never be executable binaries:
var mediaDocExtensions = map[string]bool{
	".mp4": true, ".mkv": true, ".avi": true, ".mov": true, ".wmv": true,
	".mp3": true, ".flac": true, ".wav": true, ".aac": true, ".m4a": true,
	".jpg": true, ".jpeg": true, ".png": true, ".gif": true, ".webp": true,
	".pdf": true, ".txt": true, ".epub": true, ".mobi": true,
}

// High risk executable & script extensions frequently used in malicious torrents:
var highRiskExtensions = map[string]string{
	".exe":         "Windows Executable",
	".scr":         "Windows Screensaver Executable",
	".pif":         "Windows Program Information File",
	".vbs":         "VBScript Executable Script",
	".vbe":         "Encoded VBScript",
	".js":          "JavaScript Script Executable",
	".jse":         "Encoded JavaScript",
	".bat":         "Windows Batch Script",
	".cmd":         "Windows Command Script",
	".ps1":         "PowerShell Script",
	".hta":         "HTML Application Executable",
	".cpl":         "Control Panel Extension Executable",
	".reg":         "Registry Modification Script",
	".lnk":         "Windows Shortcut / Stager LNK",
	".command":     "macOS Shell Command Executable",
	".app":         "macOS Application Bundle",
	".scpt":        "macOS AppleScript Executable",
	".applescript": "macOS AppleScript Executable",
	".workflow":    "macOS Automator Workflow",
}

// InspectFile checks a file on disk for disguised binaries and dangerous extension patterns.
func InspectFile(filePath string) (HeuristicResult, error) {
	ext := strings.ToLower(filepath.Ext(filePath))
	baseName := strings.ToLower(filepath.Base(filePath))

	// 1. Check double extension tricks (e.g. "Avatar.mp4.exe", "Invoice.pdf.vbs")
	parts := strings.Split(baseName, ".")
	if len(parts) > 2 {
		secondToLast := "." + parts[len(parts)-2]
		last := "." + parts[len(parts)-1]
		if mediaDocExtensions[secondToLast] && highRiskExtensions[last] != "" {
			return HeuristicResult{
				Level:       RiskCritical,
				Threat:      "Heuristic.DoubleExtension.Trick",
				Description: "Disguised executable hiding behind media/document extension (" + secondToLast + last + ")",
			}, nil
		}
	}

	// 2. Read first 8 bytes for magic bytes header inspection
	f, err := os.Open(filePath)
	if err != nil {
		return HeuristicResult{Level: RiskClean}, err
	}
	defer f.Close()

	header := make([]byte, 8)
	n, _ := io.ReadFull(f, header)
	if n < 2 {
		return HeuristicResult{Level: RiskClean}, nil
	}

	// 3. Check if a media or document file is secretly a binary executable
	if mediaDocExtensions[ext] {
		// Windows PE disguised as media/doc
		if bytes.HasPrefix(header, magicPE) {
			return HeuristicResult{
				Level:       RiskCritical,
				Threat:      "Heuristic.Camouflaged.WindowsPE",
				Description: "File with media extension " + ext + " has Windows PE executable header (MZ)",
			}, nil
		}

		// Linux ELF disguised as media/doc
		if n >= 4 && bytes.HasPrefix(header, magicELF) {
			return HeuristicResult{
				Level:       RiskCritical,
				Threat:      "Heuristic.Camouflaged.LinuxELF",
				Description: "File with media extension " + ext + " has Linux ELF executable header",
			}, nil
		}

		// macOS Mach-O disguised as media/doc
		if n >= 4 {
			for _, m := range magicMachO {
				if bytes.HasPrefix(header, m) {
					return HeuristicResult{
						Level:       RiskCritical,
						Threat:      "Heuristic.Camouflaged.MachOBinary",
						Description: "File with media extension " + ext + " has macOS Mach-O binary header",
					}, nil
				}
			}
		}
	}

	// 4. Standalone dangerous shortcut / stager files
	if ext == ".lnk" {
		return HeuristicResult{
			Level:       RiskSuspicious,
			Threat:      "Heuristic.Suspicious.LNKShortcut",
			Description: "Windows shortcut file (.lnk) commonly used as malware delivery vector",
		}, nil
	}

	return HeuristicResult{Level: RiskClean}, nil
}
