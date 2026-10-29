//go:build !unix

package storage

import "errors"

// DiskSpace is not implemented on this platform.
func DiskSpace(path string) (free, total int64, err error) {
	return 0, 0, errors.New("disk space query not supported on this platform")
}
