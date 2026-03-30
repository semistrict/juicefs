//go:build !windows

package chunk

import "syscall"

func pread(fd int, b []byte, off int64) (int, error) {
	return syscall.Pread(fd, b, off)
}
