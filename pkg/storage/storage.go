package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/hernan/get-torrent/pkg/metainfo"
)

var (
	ErrPieceOutOfBounds = errors.New("piece index exceeds total pieces")
)

// FileEntry represents an individual target file within the torrent's global byte stream.
type FileEntry struct {
	RelPath      string
	FullPath     string
	Length       int64
	GlobalOffset int64
	Handle       *os.File
}

// Storage manages disk allocation, writing, and reading across single or multi-file torrents.
type Storage struct {
	DownloadDir string
	PieceLength int64
	TotalLength int64
	NumPieces   int
	Files       []FileEntry
	mu          sync.RWMutex
}

// NewStorage initializes files and directories on disk based on torrent metainfo.
func NewStorage(downloadDir string, meta *metainfo.TorrentMeta) (*Storage, error) {
	if err := os.MkdirAll(downloadDir, 0755); err != nil {
		return nil, fmt.Errorf("creating download directory %s: %w", downloadDir, err)
	}

	files := make([]FileEntry, 0, len(meta.Files))
	var currentOffset int64

	isMultiFile := len(meta.Files) > 1 || (len(meta.Files) == 1 && len(meta.Files[0].Path) > 1)

	for _, f := range meta.Files {
		var relPath string
		if isMultiFile {
			pathSegments := append([]string{meta.Name}, f.Path...)
			relPath = filepath.Join(pathSegments...)
		} else {
			relPath = meta.Name
		}

		fullPath := filepath.Join(downloadDir, relPath)

		// Ensure parent directories exist
		if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
			return nil, fmt.Errorf("creating parent directory for %s: %w", fullPath, err)
		}

		// Open or create file with read/write permissions
		handle, err := os.OpenFile(fullPath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, fmt.Errorf("opening file %s: %w", fullPath, err)
		}

		// Pre-truncate file to required length. Files already the right size are left alone:
		// truncating bumps the modification time, which resume data uses to detect changes.
		info, err := handle.Stat()
		if err != nil {
			_ = handle.Close()
			return nil, fmt.Errorf("stat %s: %w", fullPath, err)
		}
		if info.Size() != f.Length {
			if err := handle.Truncate(f.Length); err != nil {
				_ = handle.Close()
				return nil, fmt.Errorf("pre-allocating file %s to %d bytes: %w", fullPath, f.Length, err)
			}
		}

		files = append(files, FileEntry{
			RelPath:      relPath,
			FullPath:     fullPath,
			Length:       f.Length,
			GlobalOffset: currentOffset,
			Handle:       handle,
		})

		currentOffset += f.Length
	}

	return &Storage{
		DownloadDir: downloadDir,
		PieceLength: meta.PieceLength,
		TotalLength: meta.TotalLength,
		NumPieces:   len(meta.Pieces),
		Files:       files,
	}, nil
}

// WritePiece writes the full piece byte buffer across file boundaries at the appropriate offsets.
func (s *Storage) WritePiece(index int, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if index < 0 || index >= s.NumPieces {
		return ErrPieceOutOfBounds
	}

	pieceStart := int64(index) * s.PieceLength
	pieceEnd := pieceStart + int64(len(data))

	var bytesWritten int
	for _, f := range s.Files {
		fileStart := f.GlobalOffset
		fileEnd := f.GlobalOffset + f.Length

		// Check if this file overlaps with the piece byte range
		if pieceEnd <= fileStart || pieceStart >= fileEnd {
			continue
		}

		// Calculate overlap range
		overlapStart := max(pieceStart, fileStart)
		overlapEnd := min(pieceEnd, fileEnd)
		overlapLen := overlapEnd - overlapStart

		// Offset inside the piece data buffer
		pieceBufferOffset := overlapStart - pieceStart
		// Offset inside the specific file
		fileWriteOffset := overlapStart - fileStart

		chunk := data[pieceBufferOffset : pieceBufferOffset+overlapLen]
		n, err := f.Handle.WriteAt(chunk, fileWriteOffset)
		if err != nil {
			return fmt.Errorf("writing to file %s at offset %d: %w", f.FullPath, fileWriteOffset, err)
		}
		bytesWritten += n
	}

	if bytesWritten != len(data) {
		return fmt.Errorf("partial piece write: expected %d bytes, wrote %d", len(data), bytesWritten)
	}

	return nil
}

// ReadPiece reads a complete piece from disk across file boundaries.
func (s *Storage) ReadPiece(index int, length int) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if index < 0 || index >= s.NumPieces {
		return nil, ErrPieceOutOfBounds
	}

	pieceStart := int64(index) * s.PieceLength
	pieceEnd := pieceStart + int64(length)
	buf := make([]byte, length)

	var bytesRead int
	for _, f := range s.Files {
		fileStart := f.GlobalOffset
		fileEnd := f.GlobalOffset + f.Length

		if pieceEnd <= fileStart || pieceStart >= fileEnd {
			continue
		}

		overlapStart := max(pieceStart, fileStart)
		overlapEnd := min(pieceEnd, fileEnd)
		overlapLen := overlapEnd - overlapStart

		pieceBufferOffset := overlapStart - pieceStart
		fileReadOffset := overlapStart - fileStart

		chunk := buf[pieceBufferOffset : pieceBufferOffset+overlapLen]
		n, err := f.Handle.ReadAt(chunk, fileReadOffset)
		if err != nil {
			return nil, fmt.Errorf("reading from file %s at offset %d: %w", f.FullPath, fileReadOffset, err)
		}
		bytesRead += n
	}

	if bytesRead != length {
		return nil, fmt.Errorf("partial piece read: expected %d bytes, read %d", length, bytesRead)
	}

	return buf, nil
}

// FileStamp identifies the on-disk version of a file for resume data: if size or modification
// time differ from the saved stamp, the file may have changed and must be rechecked.
type FileStamp struct {
	Size    int64 `json:"size"`
	ModTime int64 `json:"mtime"` // Unix nanoseconds
}

// FileStamps returns the current stamp of every file, in torrent order.
func (s *Storage) FileStamps() ([]FileStamp, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stamps := make([]FileStamp, len(s.Files))
	for i, f := range s.Files {
		info, err := os.Stat(f.FullPath)
		if err != nil {
			return nil, fmt.Errorf("stat %s: %w", f.FullPath, err)
		}
		stamps[i] = FileStamp{Size: info.Size(), ModTime: info.ModTime().UnixNano()}
	}
	return stamps, nil
}

// Close closes all open file handles.
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var firstErr error
	for _, f := range s.Files {
		if f.Handle != nil {
			if err := f.Handle.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
