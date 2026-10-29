//go:build unix

package storage

import "syscall"

// DiskSpace reports free (available to unprivileged users) and total bytes on the filesystem
// holding path.
func DiskSpace(path string) (free, total int64, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0, err
	}
	bsize := int64(st.Bsize)
	return int64(st.Bavail) * bsize, int64(st.Blocks) * bsize, nil
}
