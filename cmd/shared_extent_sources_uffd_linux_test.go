//go:build linux

package cmd

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/juicedata/juicefs/pkg/vfs"
	"golang.org/x/sys/unix"
)

const (
	uffdAPI        = 0xAA
	uffdioRegister = 0x00
	uffdioCopy     = 0x03
	uffdioAPI      = 0x3F

	uffdioRegisterModeMissing = 1
	uffdEventPagefault        = 0x12
)

type uffdioRange struct {
	start uint64
	len   uint64
}

type uffdioAPIArg struct {
	api      uint64
	features uint64
	ioctls   uint64
}

type uffdioRegisterArg struct {
	rng    uffdioRange
	mode   uint64
	ioctls uint64
}

type uffdioCopyArg struct {
	dst  uint64
	src  uint64
	len  uint64
	mode uint64
	copy int64
}

func TestSharedExtentSourcesWithUserfaultfdPageCacheSharing(t *testing.T) {
	if os.Getenv("JFS_UFFD_PAGE_CACHE_IT") != "1" {
		t.Skip("set JFS_UFFD_PAGE_CACHE_IT=1 to run the userfaultfd shared-extent integration test")
	}
	if runtime.GOARCH != "amd64" && runtime.GOARCH != "arm64" {
		t.Skipf("userfaultfd ioctl constants in this test are covered for amd64/arm64, got %s", runtime.GOARCH)
	}
	probeUserfaultfd(t)

	mountTemp(t, nil, nil, nil)
	defer umountTemp(t)

	pageSize := os.Getpagesize()
	size := 8 * pageSize
	base := filepath.Join(testMountPoint, "base")
	clone1 := filepath.Join(testMountPoint, "clone1")
	target := filepath.Join(testMountPoint, "target")

	writePatternFile(t, base, size, 11)
	copyFileRangeAll(t, base, clone1, size)
	copyFileRangeAll(t, clone1, target, size)
	overwritePage(t, target, 0, pageSize, 55)
	overwritePage(t, target, 2, pageSize, 77)
	overwritePage(t, target, 4, pageSize, 88)
	overwritePage(t, target, 5, pageSize, 99)

	baseIno := fileInode(t, base)
	clone1Ino := fileInode(t, clone1)
	targetIno := fileInode(t, target)
	clone1Spans := sharedExtentSourcesRPC(t, testMountPoint, []vfs.Ino{vfs.Ino(baseIno), vfs.Ino(clone1Ino)}, 0, uint64(size)).Spans
	targetSpans := sharedExtentSourcesRPC(t, testMountPoint, []vfs.Ino{vfs.Ino(baseIno), vfs.Ino(clone1Ino), vfs.Ino(targetIno)}, 0, uint64(size)).Spans

	baseFile := openFile(t, base, os.O_RDONLY)
	defer baseFile.Close()
	clone1File := openFile(t, clone1, os.O_RDONLY)
	defer clone1File.Close()
	targetFile := openFile(t, target, os.O_RDONLY)
	defer targetFile.Close()

	dropFileCache(t, baseFile, clone1File, targetFile)
	readWholeFile(t, baseFile, size)
	readWholeFile(t, clone1File, size)
	readWholeFile(t, targetFile, size)
	naiveBaseResident := residentPages(t, baseFile, size)
	naiveClone1Resident := residentPages(t, clone1File, size)
	naiveTargetResident := residentPages(t, targetFile, size)
	naiveTotalResident := naiveBaseResident + naiveClone1Resident + naiveTargetResident

	dropFileCache(t, baseFile, clone1File, targetFile)
	readWholeFile(t, baseFile, size)
	readViaUserfaultfd(t, clone1Spans, []*os.File{baseFile, clone1File}, size)
	readViaUserfaultfd(t, targetSpans, []*os.File{baseFile, clone1File, targetFile}, size)
	uffdBaseResident := residentPages(t, baseFile, size)
	uffdClone1Resident := residentPages(t, clone1File, size)
	uffdTargetResident := residentPages(t, targetFile, size)
	uffdTotalResident := uffdBaseResident + uffdClone1Resident + uffdTargetResident

	t.Logf("naive direct reads: base=%d clone1=%d target=%d total=%d", naiveBaseResident, naiveClone1Resident, naiveTargetResident, naiveTotalResident)
	t.Logf("RPC+UFFD source reads: base=%d clone1=%d target=%d total=%d", uffdBaseResident, uffdClone1Resident, uffdTargetResident, uffdTotalResident)
	if naiveTotalResident < 20 {
		t.Fatalf("naive direct reads did not populate separate file page caches enough: %d", naiveTotalResident)
	}
	if uffdBaseResident < 8 {
		t.Fatalf("UFFD source reads did not populate shared base page cache enough: %d", uffdBaseResident)
	}
	if uffdTotalResident >= naiveTotalResident {
		t.Fatalf("RPC+UFFD did not reduce total resident pages: naive=%d uffd=%d", naiveTotalResident, uffdTotalResident)
	}
}

func probeUserfaultfd(t *testing.T) {
	t.Helper()
	fd, err := userfaultfd()
	if err != nil {
		if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
			t.Skipf("userfaultfd is not permitted: %s", err)
		}
		t.Fatalf("userfaultfd probe: %s", err)
	}
	_ = unix.Close(fd)
}

func userfaultfd() (int, error) {
	fd, _, errno := unix.Syscall(unix.SYS_USERFAULTFD, uintptr(unix.O_CLOEXEC|unix.O_NONBLOCK), 0, 0)
	if errno != 0 {
		return -1, errno
	}
	api := uffdioAPIArg{api: uffdAPI}
	if err := ioctlPtr(int(fd), uffdIoctl(3, uffdioAPI, unsafe.Sizeof(api)), unsafe.Pointer(&api)); err != nil {
		_ = unix.Close(int(fd))
		return -1, err
	}
	return int(fd), nil
}

func readViaUserfaultfd(t *testing.T, spans []vfs.SharedExtentSourceSpan, files []*os.File, size int) {
	t.Helper()
	mem, err := unix.Mmap(-1, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Munmap(mem)
	fd, err := userfaultfd()
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)

	baseAddr := uintptr(unsafe.Pointer(&mem[0]))
	reg := uffdioRegisterArg{
		rng:  uffdioRange{start: uint64(baseAddr), len: uint64(size)},
		mode: uffdioRegisterModeMissing,
	}
	if err = ioctlPtr(fd, uffdIoctl(3, uffdioRegister, unsafe.Sizeof(reg)), unsafe.Pointer(&reg)); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go handleUserfaults(fd, baseAddr, size, spans, files, done)
	var sum byte
	for i := 0; i < size; i += os.Getpagesize() {
		mem[i] ^= 1
		sum ^= mem[i]
	}
	_ = sum
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for userfaultfd handler")
	}
}

func handleUserfaults(fd int, base uintptr, size int, spans []vfs.SharedExtentSourceSpan, files []*os.File, done chan<- error) {
	pageSize := os.Getpagesize()
	page := make([]byte, pageSize)
	handled := 0
	for handled < size/pageSize {
		var pfd [1]unix.PollFd
		pfd[0].Fd = int32(fd)
		pfd[0].Events = unix.POLLIN
		n, err := unix.Poll(pfd[:], 5000)
		if err != nil {
			done <- err
			return
		}
		if n == 0 {
			done <- syscall.ETIMEDOUT
			return
		}
		var msg [32]byte
		if _, err = unix.Read(fd, msg[:]); err != nil {
			done <- err
			return
		}
		if msg[0] != uffdEventPagefault {
			done <- fmt.Errorf("unexpected userfaultfd event %d", msg[0])
			return
		}
		addr := binary.LittleEndian.Uint64(msg[16:24])
		pageAddr := addr & ^uint64(pageSize-1)
		fileOff := pageAddr - uint64(base)
		clear(page)
		span := spanForOffset(spans, fileOff)
		if span.SourceIndex >= 0 {
			if _, err = files[span.SourceIndex].ReadAt(page, int64(fileOff)); err != nil {
				done <- err
				return
			}
		}
		cp := uffdioCopyArg{
			dst: pageAddr,
			src: uint64(uintptr(unsafe.Pointer(&page[0]))),
			len: uint64(pageSize),
		}
		if err = ioctlPtr(fd, uffdIoctl(3, uffdioCopy, unsafe.Sizeof(cp)), unsafe.Pointer(&cp)); err != nil {
			done <- err
			return
		}
		handled++
	}
	done <- nil
}

func spanForOffset(spans []vfs.SharedExtentSourceSpan, off uint64) vfs.SharedExtentSourceSpan {
	for _, span := range spans {
		if span.Off <= off && off < span.Off+span.Len {
			return span
		}
	}
	return vfs.SharedExtentSourceSpan{SourceIndex: -1}
}

func sharedExtentSourcesRPC(t *testing.T, mountPoint string, files []vfs.Ino, off, length uint64) *vfs.SharedExtentSourcesResponse {
	t.Helper()
	controller, err := openController(mountPoint)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	payload := encodeSharedExtentSourcesRequest(files, []vfs.SharedExtentSourceRange{{Off: off, Len: length}})
	msg := utils.NewBuffer(uint32(8 + len(payload)))
	msg.Put32(meta.SharedExtentSources)
	msg.Put32(uint32(len(payload)))
	msg.Put(payload)
	if _, err = controller.Write(msg.Bytes()); err != nil {
		t.Fatal(err)
	}
	data := readControlDataFromFile(t, controller)
	var resp vfs.SharedExtentSourcesResponse
	if err = json.Unmarshal(data, &resp); err != nil {
		t.Fatal(err)
	}
	return &resp
}

func encodeSharedExtentSourcesRequest(files []vfs.Ino, ranges []vfs.SharedExtentSourceRange) []byte {
	w := utils.NewBuffer(uint32(4 + len(files)*8 + 4 + len(ranges)*16))
	w.Put32(uint32(len(files)))
	for _, ino := range files {
		w.Put64(uint64(ino))
	}
	w.Put32(uint32(len(ranges)))
	for _, rg := range ranges {
		w.Put64(rg.Off)
		w.Put64(rg.Len)
	}
	return w.Bytes()
}

func readControlDataFromFile(t *testing.T, f *os.File) []byte {
	t.Helper()
	buf := make([]byte, 1<<20)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		n, err := f.Read(buf)
		if errors.Is(err, os.ErrClosed) {
			t.Fatal(err)
		}
		if errors.Is(err, syscall.EBADF) {
			t.Fatal(err)
		}
		if errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatal(err)
		}
		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EINTR) || errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, os.ErrNotExist) {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if errors.Is(err, io.EOF) {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if err != nil {
			if err.Error() == "EOF" {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			t.Fatal(err)
		}
		if n == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if n == 1 {
			t.Fatalf("control returned errno %d", buf[0])
		}
		if buf[0] != meta.CDATA || n < 5 {
			t.Fatalf("unexpected control response: %v", buf[:n])
		}
		size := binary.BigEndian.Uint32(buf[1:5])
		if int(size)+5 > n {
			t.Fatalf("short control response: got %d want %d", n, int(size)+5)
		}
		return buf[5 : 5+size]
	}
	t.Fatal("timed out reading control response")
	return nil
}

func writePatternFile(t *testing.T, path string, size int, seed byte) {
	t.Helper()
	data := make([]byte, size)
	for i := range data {
		data[i] = seed + byte(i/os.Getpagesize())
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
}

func copyFileRangeAll(t *testing.T, src, dst string, size int) {
	t.Helper()
	in := openFile(t, src, os.O_RDONLY)
	defer in.Close()
	out := openFile(t, dst, os.O_CREATE|os.O_RDWR)
	defer out.Close()
	var inOff, outOff int64
	for copied := 0; copied < size; {
		n, err := unix.CopyFileRange(int(in.Fd()), &inOff, int(out.Fd()), &outOff, size-copied, 0)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			t.Fatal("copy_file_range copied 0 bytes")
		}
		copied += n
	}
}

func overwritePage(t *testing.T, path string, pageIndex, pageSize int, value byte) {
	t.Helper()
	f := openFile(t, path, os.O_WRONLY)
	defer f.Close()
	buf := make([]byte, pageSize)
	for i := range buf {
		buf[i] = value
	}
	if _, err := f.WriteAt(buf, int64(pageIndex*pageSize)); err != nil {
		t.Fatal(err)
	}
}

func openFile(t *testing.T, path string, flags int) *os.File {
	t.Helper()
	f, err := os.OpenFile(path, flags, 0644)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func fileInode(t *testing.T, path string) uint64 {
	t.Helper()
	ino, err := utils.GetFileInode(path)
	if err != nil {
		t.Fatal(err)
	}
	return ino
}

func dropFileCache(t *testing.T, files ...*os.File) {
	t.Helper()
	for _, f := range files {
		if err := unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED); err != nil {
			t.Fatal(err)
		}
	}
}

func readWholeFile(t *testing.T, f *os.File, size int) {
	t.Helper()
	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
}

func residentPages(t *testing.T, f *os.File, size int) int {
	t.Helper()
	mem, err := unix.Mmap(int(f.Fd()), 0, size, unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Munmap(mem)
	vec := make([]byte, (size+os.Getpagesize()-1)/os.Getpagesize())
	_, _, errno := unix.Syscall(unix.SYS_MINCORE, uintptr(unsafe.Pointer(&mem[0])), uintptr(size), uintptr(unsafe.Pointer(&vec[0])))
	if errno != 0 {
		t.Fatal(errno)
	}
	var resident int
	for _, v := range vec {
		if v&1 != 0 {
			resident++
		}
	}
	return resident
}

func ioctlPtr(fd int, req uintptr, ptr unsafe.Pointer) error {
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), req, uintptr(ptr))
	if errno != 0 {
		return errno
	}
	return nil
}

func uffdIoctl(dir, nr int, size uintptr) uintptr {
	const (
		iocNRShift   = 0
		iocTypeShift = 8
		iocSizeShift = 16
		iocDirShift  = 30
	)
	return uintptr((dir << iocDirShift) | (int(uffdAPI) << iocTypeShift) | (nr << iocNRShift) | (int(size) << iocSizeShift))
}
