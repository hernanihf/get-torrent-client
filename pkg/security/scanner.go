package security

import (
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Status constants for torrent security state.
const (
	StatusPending        = "pending"
	StatusScanning       = "scanning"
	StatusClean          = "clean"
	StatusThreatDetected = "threat_detected"
	StatusSkipped        = "skipped"
)

// Actions on threat detection:
const (
	ActionPause      = "pause"
	ActionQuarantine = "quarantine"
	ActionWarn       = "warn"
)

// Config controls scanner behavior and connections.
type Config struct {
	Enabled        bool
	ClamAVAddr     string
	ActionOnThreat string
}

// DefaultConfig returns recommended security settings.
func DefaultConfig() Config {
	clamAddr := os.Getenv("ANTIVIRUS_ADDR")
	if clamAddr == "" {
		clamAddr = "clamav:3310" // Default in docker-compose network
	}

	return Config{
		Enabled:        true,
		ClamAVAddr:     clamAddr,
		ActionOnThreat: ActionPause,
	}
}

// Report holds the outcome of a torrent security analysis.
type Report struct {
	Status        string
	ThreatName    string
	AffectedFiles []string
	ScannedAt     time.Time
}

// Scanner orchestrates static heuristics and ClamAV daemon inspection for completed torrents.
type Scanner struct {
	mu   sync.RWMutex
	cfg  Config
	clam *ClamClient
}

// NewScanner constructs a Scanner with the given configuration.
func NewScanner(cfg Config) *Scanner {
	var clam *ClamClient
	if cfg.ClamAVAddr != "" {
		clam = NewClamClient(cfg.ClamAVAddr, 20*time.Second)
	}

	return &Scanner{
		cfg:  cfg,
		clam: clam,
	}
}

// UpdateConfig applies hot changes to the security settings.
func (s *Scanner) UpdateConfig(cfg Config) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cfg = cfg
	if cfg.ClamAVAddr != "" {
		s.clam = NewClamClient(cfg.ClamAVAddr, 20*time.Second)
	} else {
		s.clam = nil
	}
}

// GetConfig returns a copy of current configuration.
func (s *Scanner) GetConfig() Config {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cfg
}

// ScanFiles inspects all target files of a completed torrent.
func (s *Scanner) ScanFiles(filePaths []string) Report {
	s.mu.RLock()
	enabled := s.cfg.Enabled
	clam := s.clam
	s.mu.RUnlock()

	if !enabled {
		return Report{
			Status:    StatusSkipped,
			ScannedAt: time.Now(),
		}
	}

	clamAvailable := clam != nil && clam.Ping()
	if clamAvailable {
		log.Printf("[Security] ClamAV service detected on %s. Starting dual-layer scan...", s.cfg.ClamAVAddr)
	} else {
		log.Printf("[Security] ClamAV not reachable on %s. Operating in native Go heuristic mode.", s.cfg.ClamAVAddr)
	}

	for _, path := range filePaths {
		// 1. Static Heuristic Inspection
		heur, err := InspectFile(path)
		if err == nil && heur.Level == RiskCritical {
			log.Printf("[Security] ⚠️ CRITICAL THREAT detected in %s: %s (%s)", filepath.Base(path), heur.Threat, heur.Description)
			return Report{
				Status:        StatusThreatDetected,
				ThreatName:    heur.Threat,
				AffectedFiles: []string{path},
				ScannedAt:     time.Now(),
			}
		}

		// 2. ClamAV Engine Inspection (if online)
		if clamAvailable {
			clean, threat, err := clam.ScanFile(path)
			if err != nil {
				log.Printf("[Security] Notice: ClamAV file scan error for %s: %v", filepath.Base(path), err)
			} else if !clean && threat != "" {
				log.Printf("[Security] 🚨 MALWARE DETECTED by ClamAV in %s: %s", filepath.Base(path), threat)
				return Report{
					Status:        StatusThreatDetected,
					ThreatName:    threat,
					AffectedFiles: []string{path},
					ScannedAt:     time.Now(),
				}
			}
		}
	}

	return Report{
		Status:    StatusClean,
		ScannedAt: time.Now(),
	}
}
