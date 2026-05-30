//go:build linux && cgo
// +build linux,cgo

/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package smartmap

import (
	"errors"
	"fmt"
	"io"
	"net"
	"path"
	"sync"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
	"golang.org/x/sys/unix"
)

var uffdPagePool = sync.Pool{New: func() any { return make([]byte, uffdHugePageSize) }}

const uffdRefaultProtectionMultiplier = 2

type uffdSharedCache struct {
	mu                  sync.Mutex
	fd                  int
	mapped              []byte
	next                uint64
	seq                 uint64
	free                []uint64
	resident            int
	maxResidentPages    int
	evictionPolicy      string
	probeCandidatePages int
	probeEvictPages     int
	probeMonitorMillis  int
	clock               uint64
	control             uint64
	probe               uint64
	pages               map[string]*uffdSharedPage
	memories            map[string]*uffdMemory
}

type uffdSharedCacheOptions struct {
	maxResidentPages    int
	evictionPolicy      string
	probeCandidatePages int
	probeEvictPages     int
	probeMonitorMillis  int
}

type uffdSharedPage struct {
	key       string
	shmOff    uint64
	source    uffdPageSource
	refs      int
	populated bool
	loading   bool
	evicting  bool
	probing   bool
	probeSeq  uint64
	probeHit  bool
	probeCold bool
	evictedAt uint64
	protected uint64
	lastUsed  uint64
	err       error
	cond      *sync.Cond
}

type uffdMemory struct {
	id       string
	path     string
	inode    vfs.Ino
	size     uint64
	extents  []UFFDExtent
	pages    map[uint64]*uffdSharedPage
	sessions int
	closing  bool
	active   map[*uffdSharedSession]struct{}
}

type uffdSharedSession struct {
	v       *vfs.VFS
	ctx     vfs.LogContext
	cache   *uffdSharedCache
	memory  *uffdMemory
	mapping []UFFDRegion
	conn    *net.UnixConn
	ctrlMu  sync.Mutex
	uffdFd  int
	connFd  int
	exitR   int
	wakeR   int
	wakeW   int
	done    <-chan struct{}
	retries []uffdPagefault
}

type uffdEvictionTarget struct {
	session *uffdSharedSession
	ranges  []uffdControlRange
}

type uffdProbeConfig struct {
	candidates int
	evict      int
	threshold  int
	monitor    time.Duration
}

func newUFFDSharedCache(options ...uffdSharedCacheOptions) *uffdSharedCache {
	var opt uffdSharedCacheOptions
	if len(options) > 0 {
		opt = options[0]
	}
	return &uffdSharedCache{
		fd:                  -1,
		maxResidentPages:    opt.maxResidentPages,
		evictionPolicy:      opt.evictionPolicy,
		probeCandidatePages: opt.probeCandidatePages,
		probeEvictPages:     opt.probeEvictPages,
		probeMonitorMillis:  opt.probeMonitorMillis,
		pages:               make(map[string]*uffdSharedPage),
		memories:            make(map[string]*uffdMemory),
	}
}

func closeUFFDSharedFDs(fds []int) {
	for _, fd := range fds {
		_ = syscall.Close(fd)
	}
}

func handleSmartmapSession(v *vfs.VFS, conn *net.UnixConn, done <-chan struct{}, cache *uffdSharedCache, frame smartmapFrame, fds []int) {
	if cache == nil {
		closeUFFDSharedFDs(fds)
		writeSmartmapFatal(conn, errors.New("smartmap shared memory cache is unavailable"))
		return
	}
	ctx := vfs.NewLogContext(peerMetaContext(conn))
	if frame.Type != smartmapFrameMap {
		closeUFFDSharedFDs(fds)
		writeSmartmapFatal(conn, fmt.Errorf("smartmap session expected %q frame, got %q", smartmapFrameMap, frame.Type))
		return
	}
	if len(fds) != 0 {
		closeUFFDSharedFDs(fds)
		writeSmartmapFatal(conn, fmt.Errorf("smartmap map expected no fds, got %d", len(fds)))
		return
	}
	memory, err := cache.openMappedFile(v, ctx, frame.Path, frame.Size)
	if err != nil {
		writeSmartmapFatal(conn, err)
		return
	}
	defer func() {
		if err := cache.closeMemory(memory.id); err != nil {
			logger.Warnf("close smartmap memory %s: %s", memory.path, err)
		}
	}()
	clientFD, err := cache.openReadOnlyFD()
	if err != nil {
		writeSmartmapFatal(conn, err)
		return
	}
	mapped := smartmapFrame{Type: smartmapFrameMapped, Size: memory.size, PageSize: uffdHugePageSize, Extents: memory.extents}
	err = writeSmartmapFrameWithFD(conn, mapped, clientFD)
	_ = unix.Close(clientFD)
	if err != nil {
		logger.Warnf("write smartmap mapped response for %s: %s", frame.Path, err)
		return
	}

	attach, attachFDs, err := readSmartmapFrame(conn)
	if err != nil {
		writeSmartmapFatal(conn, err)
		return
	}
	if attach.Type != smartmapFrameAttach {
		closeUFFDSharedFDs(attachFDs)
		writeSmartmapFatal(conn, fmt.Errorf("smartmap session expected %q frame, got %q", smartmapFrameAttach, attach.Type))
		return
	}
	if len(attachFDs) != 1 || attach.FD != smartmapFDUFFD {
		closeUFFDSharedFDs(attachFDs)
		writeSmartmapFatal(conn, fmt.Errorf("smartmap attach expected exactly one uffd fd, got purpose %q count %d", attach.FD, len(attachFDs)))
		return
	}
	mapping := []UFFDRegion{{
		BaseHostVirtAddr: attach.BaseHostVirtAddr,
		Size:             uintptr(attach.Size),
		Offset:           0,
		PageSize:         attach.PageSize,
	}}
	if attach.Size != memory.size {
		closeUFFDSharedFDs(attachFDs)
		writeSmartmapFatal(conn, fmt.Errorf("smartmap attach size %d does not match mapped size %d", attach.Size, memory.size))
		return
	}
	if err := validateUFFDSharedMappings(mapping); err != nil {
		closeUFFDSharedFDs(attachFDs)
		writeSmartmapFatal(conn, err)
		return
	}
	connFd, err := unixConnFD(conn)
	if err != nil {
		closeUFFDSharedFDs(attachFDs)
		writeSmartmapFatal(conn, err)
		return
	}
	s := &uffdSharedSession{
		v: v, ctx: ctx, cache: cache, memory: memory, mapping: mapping,
		conn: conn, uffdFd: attachFDs[0], connFd: connFd, done: done,
	}
	if err := cache.beginMemorySession(memory, s); err != nil {
		closeUFFDSharedFDs(attachFDs)
		writeSmartmapFatal(conn, err)
		return
	}
	if err := writeSmartmapFrame(conn, smartmapFrame{Type: smartmapFrameAttached}); err != nil {
		cache.endMemorySession(memory, s)
		closeUFFDSharedFDs(attachFDs)
		return
	}
	err = s.serve()
	cache.endMemorySession(memory, s)
	if err != nil {
		logger.Warnf("smartmap session for %s ended with error: %s", memory.path, err)
		writeSmartmapFatal(conn, err)
	}
}

func validateUFFDSharedMappings(mappings []UFFDRegion) error {
	if len(mappings) == 0 {
		return errors.New("no uffd shared mappings")
	}
	for i, r := range mappings {
		if r.PageSize != uffdHugePageSize {
			return fmt.Errorf("mapping %d page_size must be %d, got %d", i, uffdHugePageSize, r.PageSize)
		}
		if r.BaseHostVirtAddr == 0 || r.Size == 0 {
			return fmt.Errorf("mapping %d has invalid address range", i)
		}
		if r.BaseHostVirtAddr%uffdHugePageSize != 0 || r.Size%uffdHugePageSize != 0 || r.Offset%uffdHugePageSize != 0 {
			return fmt.Errorf("mapping %d is not 2 MiB aligned", i)
		}
		if r.endHostVirtAddr() < r.BaseHostVirtAddr {
			return fmt.Errorf("mapping %d overflows address space", i)
		}
	}
	return nil
}

func (c *uffdSharedCache) openMappedFile(v *vfs.VFS, ctx vfs.LogContext, reqPath string, size uint64) (*uffdMemory, error) {
	if reqPath == "" {
		return nil, errors.New("smartmap map path is empty")
	}
	if size == 0 || size%uffdHugePageSize != 0 {
		return nil, fmt.Errorf("smartmap map size must be a non-zero %d-byte multiple", uffdHugePageSize)
	}
	p := path.Clean(reqPath)
	if !path.IsAbs(p) {
		return nil, fmt.Errorf("smartmap map path must be absolute: %s", reqPath)
	}
	var ino vfs.Ino
	attr := &vfs.Attr{}
	if st := v.Meta.Resolve(ctx, meta.RootInode, p, &ino, attr, true); st != 0 {
		return nil, fmt.Errorf("resolve %s: %w", p, st)
	}
	if attr.Typ != meta.TypeFile {
		return nil, fmt.Errorf("smartmap map path is not a file: %s", p)
	}
	if attr.Length < size {
		return nil, fmt.Errorf("smartmap map size %d exceeds file length %d", size, attr.Length)
	}
	entry, fh, st := v.Open(ctx, ino, syscall.O_RDONLY)
	if st != 0 {
		return nil, fmt.Errorf("open %s: %w", p, st)
	}
	defer v.Release(ctx, entry.Inode, fh)

	pageCount := int(size / uffdHugePageSize)
	memory := &uffdMemory{
		inode:  entry.Inode,
		path:   p,
		size:   size,
		pages:  make(map[uint64]*uffdSharedPage, pageCount),
		active: make(map[*uffdSharedSession]struct{}),
	}
	memory.extents = make([]UFFDExtent, 0, pageCount)
	for off := uint64(0); off < size; off += uffdHugePageSize {
		source, err := captureUFFDPageSource(v, ctx, entry.Inode, attr.Length, off)
		if err != nil {
			c.releaseMemoryPages(memory)
			return nil, err
		}
		page, err := c.pageForKey(source.key, source)
		if err != nil {
			c.releaseMemoryPages(memory)
			return nil, err
		}
		memory.pages[off] = page
		memory.extents = appendUFFDExtent(memory.extents, UFFDExtent{FileOffset: off, Length: uffdHugePageSize, ShmOffset: page.shmOff})
	}

	c.mu.Lock()
	c.seq++
	memory.id = fmt.Sprintf("memory-%d", c.seq)
	c.memories[memory.id] = memory
	c.mu.Unlock()
	return memory, nil
}

func appendUFFDExtent(extents []UFFDExtent, extent UFFDExtent) []UFFDExtent {
	if len(extents) == 0 {
		return append(extents, extent)
	}
	last := &extents[len(extents)-1]
	if last.FileOffset+last.Length == extent.FileOffset && last.ShmOffset+last.Length == extent.ShmOffset {
		last.Length += extent.Length
		return extents
	}
	return append(extents, extent)
}

func (c *uffdSharedCache) pageForKey(key string, source uffdPageSource) (*uffdSharedPage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if page := c.pages[key]; page != nil {
		page.refs++
		return page, nil
	}
	return c.allocatePageLocked(key, source, 1)
}

func (c *uffdSharedCache) allocatePageLocked(key string, source uffdPageSource, refs int) (*uffdSharedPage, error) {
	if c.fd < 0 {
		fd, err := unix.MemfdCreate("juicefs-uffd-hugetlb", unix.MFD_CLOEXEC|unix.MFD_HUGETLB|unix.MFD_HUGE_2MB)
		if err != nil {
			return nil, fmt.Errorf("create hugetlb memfd: %w", err)
		}
		c.fd = fd
	}
	var shmOff uint64
	if n := len(c.free); n > 0 {
		shmOff = c.free[n-1]
		c.free = c.free[:n-1]
	} else {
		shmOff = c.next
		if err := unix.Ftruncate(c.fd, int64(shmOff+uffdHugePageSize)); err != nil {
			return nil, fmt.Errorf("grow hugetlb memfd: %w", err)
		}
		c.next += uffdHugePageSize
	}
	page := &uffdSharedPage{key: key, shmOff: shmOff, source: source, refs: refs}
	page.cond = sync.NewCond(&c.mu)
	c.pages[key] = page
	return page, nil
}

func (c *uffdSharedCache) openReadOnlyFD() (int, error) {
	c.mu.Lock()
	fd := c.fd
	c.mu.Unlock()
	if fd < 0 {
		return -1, errors.New("shared hugetlb memfd is closed")
	}
	roFD, err := unix.Open(fmt.Sprintf("/proc/self/fd/%d", fd), unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("open read-only shared hugetlb memfd: %w", err)
	}
	return roFD, nil
}

func (c *uffdSharedCache) beginMemorySession(memory *uffdMemory, session *uffdSharedSession) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if memory.closing || c.memories[memory.id] != memory {
		return fmt.Errorf("smartmap memory %s is closing", memory.id)
	}
	memory.sessions++
	memory.active[session] = struct{}{}
	return nil
}

func (c *uffdSharedCache) endMemorySession(memory *uffdMemory, session *uffdSharedSession) {
	c.mu.Lock()
	if _, ok := memory.active[session]; ok {
		delete(memory.active, session)
		if memory.sessions > 0 {
			memory.sessions--
		}
	}
	c.mu.Unlock()
}

func (c *uffdSharedCache) closeMemory(id string) error {
	if id == "" {
		return errors.New("smartmap memory handle is empty")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	memory := c.memories[id]
	if memory == nil {
		return fmt.Errorf("unknown smartmap memory handle: %s", id)
	}
	if memory.closing {
		return fmt.Errorf("smartmap memory %s is closing", id)
	}
	if memory.sessions > 0 {
		return fmt.Errorf("smartmap memory %s has %d active uffd sessions", id, memory.sessions)
	}
	memory.closing = true
	for _, page := range memory.pages {
		if err := c.releasePageLocked(page); err != nil {
			memory.closing = false
			return err
		}
	}
	delete(c.memories, id)
	return nil
}

func (c *uffdSharedCache) releaseMemoryPages(memory *uffdMemory) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, page := range memory.pages {
		_ = c.releasePageLocked(page)
	}
}

func (c *uffdSharedCache) releasePageLocked(page *uffdSharedPage) error {
	if page.refs <= 0 {
		return nil
	}
	page.refs--
	if page.refs > 0 {
		return nil
	}
	if err := c.discardSlotLocked(page.shmOff); err != nil {
		page.refs++
		return err
	}
	delete(c.pages, page.key)
	if page.populated && c.resident > 0 {
		c.resident--
	}
	page.populated = false
	page.loading = false
	page.evicting = false
	c.clearProbeLocked(page)
	page.evictedAt = 0
	page.protected = 0
	page.err = nil
	c.free = append(c.free, page.shmOff)
	return nil
}

func (c *uffdSharedCache) discardSlotLocked(shmOff uint64) error {
	if c.fd < 0 {
		return nil
	}
	err := unix.Fallocate(c.fd, unix.FALLOC_FL_KEEP_SIZE|unix.FALLOC_FL_PUNCH_HOLE, int64(shmOff), int64(uffdHugePageSize))
	if err != nil {
		return fmt.Errorf("discard hugetlb slot at %d: %w", shmOff, err)
	}
	return nil
}

func (c *uffdSharedCache) writePage(page *uffdSharedPage, buf []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	required := page.shmOff + uint64(len(buf))
	if required > c.next {
		return fmt.Errorf("shared page at %d exceeds arena size %d", page.shmOff, c.next)
	}
	if err := c.ensureMappedLocked(); err != nil {
		return err
	}
	copy(c.mapped[page.shmOff:required], buf)
	return nil
}

func (c *uffdSharedCache) copyPageAt(buf []byte, shmOff uint64) error {
	if len(buf) != uffdHugePageSize {
		return fmt.Errorf("copy shared page buffer has %d bytes, want %d", len(buf), uffdHugePageSize)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fd < 0 {
		return errors.New("shared hugetlb memfd is closed")
	}
	if shmOff+uint64(len(buf)) > c.next {
		return fmt.Errorf("shared page at %d exceeds arena size %d", shmOff, c.next)
	}
	if err := c.ensureMappedLocked(); err != nil {
		return err
	}
	copy(buf, c.mapped[shmOff:shmOff+uint64(len(buf))])
	return nil
}

func (c *uffdSharedCache) ensureMappedLocked() error {
	if uint64(len(c.mapped)) >= c.next {
		return nil
	}
	if len(c.mapped) > 0 {
		if err := unix.Munmap(c.mapped); err != nil {
			return fmt.Errorf("unmap hugetlb arena: %w", err)
		}
		c.mapped = nil
	}
	mapped, err := unix.Mmap(c.fd, 0, int(c.next), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("map hugetlb arena: %w", err)
	}
	c.mapped = mapped
	return nil
}

func (c *uffdSharedCache) close() {
	c.mu.Lock()
	fd := c.fd
	c.fd = -1
	mapped := c.mapped
	c.mapped = nil
	c.memories = make(map[string]*uffdMemory)
	c.pages = make(map[string]*uffdSharedPage)
	c.free = nil
	c.resident = 0
	c.mu.Unlock()
	if len(mapped) > 0 {
		_ = unix.Munmap(mapped)
	}
	if fd >= 0 {
		_ = unix.Close(fd)
	}
}

func (s *uffdSharedSession) serve() error {
	defer syscall.Close(s.uffdFd)

	exitPipe := [2]int{}
	if err := syscall.Pipe2(exitPipe[:], syscall.O_NONBLOCK|syscall.O_CLOEXEC); err != nil {
		return fmt.Errorf("create shared uffd exit pipe: %w", err)
	}
	defer syscall.Close(exitPipe[0])
	defer syscall.Close(exitPipe[1])
	s.exitR = exitPipe[0]
	exitW := exitPipe[1]

	wakePipe := [2]int{}
	if err := syscall.Pipe2(wakePipe[:], syscall.O_NONBLOCK|syscall.O_CLOEXEC); err != nil {
		return fmt.Errorf("create shared uffd wake pipe: %w", err)
	}
	defer syscall.Close(wakePipe[0])
	defer syscall.Close(wakePipe[1])
	s.wakeR, s.wakeW = wakePipe[0], wakePipe[1]

	if s.done != nil {
		sessionDone := make(chan struct{})
		defer close(sessionDone)
		go func() {
			select {
			case <-s.done:
				_, _ = syscall.Write(exitW, []byte{1})
			case <-sessionDone:
			}
		}()
	}

	return s.poll()
}

func (s *uffdSharedSession) poll() error {
	pollFds := []unix.PollFd{
		{Fd: int32(s.uffdFd), Events: unix.POLLIN},
		{Fd: int32(s.exitR), Events: unix.POLLIN},
		{Fd: int32(s.wakeR), Events: unix.POLLIN},
		{Fd: int32(s.connFd), Events: unix.POLLIN},
	}
	for {
		if _, err := unix.Poll(pollFds, -1); err != nil {
			if err == unix.EINTR || err == unix.EAGAIN {
				continue
			}
			return fmt.Errorf("poll shared uffd: %w", err)
		}
		if hasPollEvent(pollFds[1].Revents, unix.POLLIN) {
			return nil
		}
		if hasPollEvent(pollFds[2].Revents, unix.POLLIN) {
			drainFd(s.wakeR)
		}
		if hasPollEvent(pollFds[3].Revents, unix.POLLHUP) {
			return nil
		}
		if hasPollEvent(pollFds[3].Revents, unix.POLLERR|unix.POLLNVAL) {
			return fmt.Errorf("shared uffd request socket poll error: revents=%#x", pollFds[3].Revents)
		}
		if hasPollEvent(pollFds[0].Revents, unix.POLLHUP) {
			return nil
		}
		if hasPollEvent(pollFds[0].Revents, unix.POLLERR|unix.POLLNVAL) {
			return fmt.Errorf("shared uffd poll error: revents=%#x", pollFds[0].Revents)
		}
		var faults []uffdPagefault
		if hasPollEvent(pollFds[0].Revents, unix.POLLIN) {
			readFaults, err := readUFFDFaults(s.uffdFd)
			if err != nil {
				return err
			}
			faults = append(faults, readFaults...)
		}
		if len(s.retries) > 0 {
			faults = append(s.retries, faults...)
			s.retries = nil
		}
		for i := range faults {
			if err := s.handleFault(&faults[i]); err != nil {
				return err
			}
		}
	}
}

func (s *uffdSharedSession) handleFault(pf *uffdPagefault) error {
	addr := uintptr(pf.address)
	region, err := findUFFDRegion(s.mapping, addr)
	if err != nil {
		return err
	}
	pageAddr := addr & ^(uintptr(uffdHugePageSize) - 1)
	fileOff := region.fileOffset(pageAddr)
	if fileOff%uffdHugePageSize != 0 {
		return fmt.Errorf("shared fault file offset %d is not 2 MiB aligned", fileOff)
	}
	page := s.memory.pages[fileOff]
	if page == nil {
		return fmt.Errorf("fault file offset %d is outside shared memory layout", fileOff)
	}
	if pf.flags&uffdPagefaultFlagWP != 0 {
		ranges := []uffdControlRange{{
			FileOffset: fileOff,
			Length:     uffdHugePageSize,
			ShmOffset:  page.shmOff,
		}}
		if err := s.requestWriteFault(ranges); err != nil {
			return err
		}
		if err := uffdWriteProtect(s.uffdFd, pageAddr, uintptr(uffdHugePageSize), false); err != nil {
			return err
		}
		return uffdWake(s.uffdFd, pageAddr, uintptr(uffdHugePageSize))
	}
	if pf.flags&uffdPagefaultFlagMinor != 0 {
		s.cache.touchPage(page)
		if err := s.continueCleanReadFault(pageAddr, pf); err != nil {
			return err
		}
		return s.maybeStartProbeRound(page)
	}
	if err := s.populatePage(page); err != nil {
		return err
	}
	if err := s.copyFaultFromPage(pageAddr, page, pf); err != nil {
		return err
	}
	return s.maybeStartProbeRound(page)
}

func (s *uffdSharedSession) continueCleanReadFault(pageAddr uintptr, pf *uffdPagefault) error {
	err := uffdContinueMode(s.uffdFd, pageAddr, uintptr(uffdHugePageSize), uint64(uffdioContinueModeDontWake))
	if errors.Is(err, unix.EEXIST) || errors.Is(err, unix.ESRCH) {
		return nil
	}
	if errors.Is(err, unix.EAGAIN) {
		s.retries = append(s.retries, *pf)
		s.signalWake()
		return nil
	}
	if err != nil {
		return err
	}
	if err = uffdWriteProtect(s.uffdFd, pageAddr, uintptr(uffdHugePageSize), true); err != nil {
		_ = uffdWake(s.uffdFd, pageAddr, uintptr(uffdHugePageSize))
		if errors.Is(err, unix.ENOENT) || errors.Is(err, unix.EEXIST) || errors.Is(err, unix.ESRCH) {
			return nil
		}
		return err
	}
	return uffdWake(s.uffdFd, pageAddr, uintptr(uffdHugePageSize))
}

func (s *uffdSharedSession) copyFaultFromPage(pageAddr uintptr, page *uffdSharedPage, pf *uffdPagefault) error {
	buf := uffdPagePool.Get().([]byte)
	if cap(buf) < uffdHugePageSize {
		buf = make([]byte, uffdHugePageSize)
	}
	buf = buf[:uffdHugePageSize]
	defer uffdPagePool.Put(buf)
	if err := s.cache.copyPageAt(buf, page.shmOff); err != nil {
		return err
	}
	mode := uint64(uffdioCopyModeDontWake | uffdioCopyModeWP)
	err := uffdCopyMode(s.uffdFd, pageAddr, uintptr(uffdHugePageSize), buf, mode)
	if errors.Is(err, unix.EAGAIN) {
		s.retries = append(s.retries, *pf)
		s.signalWake()
		return nil
	}
	if errors.Is(err, unix.EEXIST) {
		return s.continueCleanReadFault(pageAddr, pf)
	}
	if errors.Is(err, unix.ESRCH) {
		return nil
	}
	if err != nil {
		_ = uffdWake(s.uffdFd, pageAddr, uintptr(uffdHugePageSize))
		return err
	}
	return uffdWake(s.uffdFd, pageAddr, uintptr(uffdHugePageSize))
}

func (s *uffdSharedSession) populatePage(page *uffdSharedPage) error {
	s.cache.mu.Lock()
	for page.loading || page.evicting {
		page.cond.Wait()
	}
	if page.populated {
		err := page.err
		if err == nil {
			s.cache.touchPageLocked(page)
		}
		s.cache.mu.Unlock()
		return err
	}
	page.loading = true
	s.cache.mu.Unlock()

	if err := s.ensureResidentCapacity(page); err != nil {
		s.finishPageLoad(page, err)
		return err
	}

	buf := uffdPagePool.Get().([]byte)
	if cap(buf) < uffdHugePageSize {
		buf = make([]byte, uffdHugePageSize)
	}
	buf = buf[:uffdHugePageSize]
	defer uffdPagePool.Put(buf)
	err := s.readSharedPage(buf, page.source)
	if err == nil {
		err = s.cache.writePage(page, buf)
	}

	s.cache.mu.Lock()
	page.err = err
	if err == nil {
		if !page.populated {
			s.cache.resident++
		}
		page.populated = true
		s.markRefaultProtectionLocked(page)
		s.cache.touchPageLocked(page)
	} else {
		page.populated = false
	}
	page.loading = false
	page.cond.Broadcast()
	s.cache.mu.Unlock()
	return err
}

func (s *uffdSharedSession) markRefaultProtectionLocked(page *uffdSharedPage) {
	limit := s.cache.maxResidentPages
	if limit <= 0 || page.evictedAt == 0 {
		return
	}
	refaultWindow := uint64(limit)
	if s.cache.clock < page.evictedAt || s.cache.clock-page.evictedAt > refaultWindow {
		page.evictedAt = 0
		return
	}
	protectFor := uint64(limit * uffdRefaultProtectionMultiplier)
	if protectFor == 0 {
		protectFor = 1
	}
	protected := s.cache.clock + protectFor
	if protected > page.protected {
		page.protected = protected
	}
	page.evictedAt = 0
}

func (s *uffdSharedSession) finishPageLoad(page *uffdSharedPage, err error) {
	s.cache.mu.Lock()
	page.err = err
	page.loading = false
	page.cond.Broadcast()
	s.cache.mu.Unlock()
}

func (s *uffdSharedSession) ensureResidentCapacity(skip *uffdSharedPage) error {
	limit := s.cache.maxResidentPages
	if limit <= 0 {
		return nil
	}
	for {
		if s.probeEvictionEnabled() {
			victim, targets := s.cache.reserveProbeColdVictim(skip, limit)
			if victim != nil {
				if err := s.evictReservedPage(victim, targets); err != nil {
					return err
				}
				continue
			}
		}
		victim, targets := s.cache.reserveEvictionVictim(skip, limit)
		if victim == nil {
			return nil
		}
		if err := s.evictReservedPage(victim, targets); err != nil {
			return err
		}
	}
}

func (c *uffdSharedCache) reserveProbeColdVictim(skip *uffdSharedPage, limit int) (*uffdSharedPage, []uffdEvictionTarget) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if limit <= 0 || c.resident < limit {
		return nil, nil
	}
	var victim *uffdSharedPage
	for _, page := range c.pages {
		if page == skip || !page.populated || page.loading || page.evicting || page.probing || !page.probeCold || page.probeHit || c.refaultProtectedLocked(page) {
			continue
		}
		if c.betterEvictionVictimLocked(page, victim) {
			victim = page
		}
	}
	if victim == nil {
		return nil, nil
	}
	victim.evicting = true
	return victim, c.evictionTargetsLocked(victim)
}

func (c *uffdSharedCache) reserveEvictionVictim(skip *uffdSharedPage, limit int) (*uffdSharedPage, []uffdEvictionTarget) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if limit <= 0 || c.resident < limit {
		return nil, nil
	}
	var victim *uffdSharedPage
	for _, page := range c.pages {
		if page == skip || !page.populated || page.loading || page.evicting || page.probing || c.refaultProtectedLocked(page) {
			continue
		}
		if c.betterEvictionVictimLocked(page, victim) {
			victim = page
		}
	}
	if victim == nil {
		for _, page := range c.pages {
			if page == skip || !page.populated || page.loading || page.evicting {
				continue
			}
			if c.betterEvictionVictimLocked(page, victim) {
				victim = page
			}
		}
	}
	if victim == nil {
		return nil, nil
	}
	victim.evicting = true
	return victim, c.evictionTargetsLocked(victim)
}

func (c *uffdSharedCache) betterEvictionVictimLocked(page, victim *uffdSharedPage) bool {
	if victim == nil {
		return true
	}
	pageMappings := c.activeMappingCountLocked(page)
	victimMappings := c.activeMappingCountLocked(victim)
	if pageMappings != victimMappings {
		return pageMappings < victimMappings
	}
	return page.lastUsed < victim.lastUsed
}

func (c *uffdSharedCache) activeMappingCountLocked(page *uffdSharedPage) int {
	mappings := 0
	for _, memory := range c.memories {
		for _, memoryPage := range memory.pages {
			if memoryPage != page {
				continue
			}
			mappings += len(memory.active)
		}
	}
	return mappings
}

func (c *uffdSharedCache) evictionTargetsLocked(page *uffdSharedPage) []uffdEvictionTarget {
	return c.controlTargetsLocked([]*uffdSharedPage{page})
}

func (c *uffdSharedCache) controlTargetsLocked(pages []*uffdSharedPage) []uffdEvictionTarget {
	pageSet := make(map[*uffdSharedPage]struct{}, len(pages))
	for _, page := range pages {
		pageSet[page] = struct{}{}
	}
	bySession := make(map[*uffdSharedSession][]uffdControlRange)
	for _, memory := range c.memories {
		for fileOff, memoryPage := range memory.pages {
			if _, ok := pageSet[memoryPage]; !ok {
				continue
			}
			for session := range memory.active {
				bySession[session] = append(bySession[session], uffdControlRange{
					FileOffset: fileOff,
					Length:     uffdHugePageSize,
					ShmOffset:  memoryPage.shmOff,
				})
			}
		}
	}
	targets := make([]uffdEvictionTarget, 0, len(bySession))
	for session, ranges := range bySession {
		targets = append(targets, uffdEvictionTarget{session: session, ranges: ranges})
	}
	return targets
}

func (s *uffdSharedSession) maybeStartProbeRound(skip *uffdSharedPage) error {
	cfg, ok := s.probeConfig()
	if !ok {
		return nil
	}
	pages, targets, seq := s.cache.reserveProbeCandidates(skip, cfg)
	if len(pages) == 0 {
		return nil
	}
	for _, target := range targets {
		released, err := target.session.requestProbe(target.ranges)
		if err != nil {
			s.cache.cancelProbeRound(seq, pages)
			if isUFFDControlClosedErr(err) {
				return nil
			}
			return err
		}
		if !controlRangesCover(target.ranges, released) {
			s.cache.cancelProbeRound(seq, pages)
			return nil
		}
	}
	s.cache.finishProbeRoundAfter(seq, pages, cfg.monitor)
	return nil
}

func (s *uffdSharedSession) probeEvictionEnabled() bool {
	return s.cache.evictionPolicy == uffdEvictionPolicyProbe
}

func (s *uffdSharedSession) probeConfig() (uffdProbeConfig, bool) {
	limit := s.cache.maxResidentPages
	if !s.probeEvictionEnabled() || limit <= 0 {
		return uffdProbeConfig{}, false
	}
	candidates := s.cache.probeCandidatePages
	if candidates <= 0 {
		candidates = limit / 2
	}
	if candidates <= 0 {
		candidates = 1
	}
	if candidates > limit {
		candidates = limit
	}
	evict := s.cache.probeEvictPages
	if evict <= 0 {
		evict = candidates / 2
	}
	if evict <= 0 {
		evict = 1
	}
	if evict > candidates {
		evict = candidates
	}
	monitorMillis := s.cache.probeMonitorMillis
	if monitorMillis <= 0 {
		monitorMillis = 10
	}
	threshold := limit - evict
	if threshold < 1 {
		threshold = 1
	}
	return uffdProbeConfig{
		candidates: candidates,
		evict:      evict,
		threshold:  threshold,
		monitor:    time.Duration(monitorMillis) * time.Millisecond,
	}, true
}

func (c *uffdSharedCache) reserveProbeCandidates(skip *uffdSharedPage, cfg uffdProbeConfig) ([]*uffdSharedPage, []uffdEvictionTarget, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.resident < cfg.threshold || c.probeOutstandingLocked() >= cfg.evict {
		return nil, nil, 0
	}
	candidates := c.oldestProbeCandidatesLocked(skip, cfg.candidates)
	if len(candidates) == 0 {
		return nil, nil, 0
	}
	c.probe++
	seq := c.probe
	for _, page := range candidates {
		page.probing = true
		page.probeSeq = seq
		page.probeHit = false
		page.probeCold = false
	}
	return candidates, c.controlTargetsLocked(candidates), seq
}

func (c *uffdSharedCache) probeOutstandingLocked() int {
	outstanding := 0
	for _, page := range c.pages {
		if page.populated && (page.probing || page.probeCold) {
			outstanding++
		}
	}
	return outstanding
}

func (c *uffdSharedCache) oldestProbeCandidatesLocked(skip *uffdSharedPage, limit int) []*uffdSharedPage {
	candidates := make([]*uffdSharedPage, 0, limit)
	for len(candidates) < limit {
		var next *uffdSharedPage
		for _, page := range c.pages {
			if page == skip || !page.populated || page.loading || page.evicting || page.probing || page.probeCold || c.refaultProtectedLocked(page) || containsUFFDSharedPage(candidates, page) {
				continue
			}
			if next == nil || page.lastUsed < next.lastUsed {
				next = page
			}
		}
		if next == nil {
			break
		}
		candidates = append(candidates, next)
	}
	return candidates
}

func containsUFFDSharedPage(pages []*uffdSharedPage, page *uffdSharedPage) bool {
	for _, candidate := range pages {
		if candidate == page {
			return true
		}
	}
	return false
}

func (c *uffdSharedCache) cancelProbeRound(seq uint64, pages []*uffdSharedPage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, page := range pages {
		if page.probeSeq == seq && page.probing {
			c.clearProbeLocked(page)
		}
	}
}

func (c *uffdSharedCache) finishProbeRoundAfter(seq uint64, pages []*uffdSharedPage, delay time.Duration) {
	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		<-timer.C
		c.finishProbeRound(seq, pages)
	}()
}

func (c *uffdSharedCache) finishProbeRound(seq uint64, pages []*uffdSharedPage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, page := range pages {
		if page.probeSeq != seq || !page.probing || page.refs <= 0 || c.pages[page.key] != page {
			continue
		}
		page.probing = false
		if !page.probeHit && page.populated {
			page.probeCold = true
		}
	}
}

func (s *uffdSharedSession) evictReservedPage(page *uffdSharedPage, targets []uffdEvictionTarget) error {
	for _, target := range targets {
		released, err := target.session.requestRelease(target.ranges)
		if err != nil {
			if isUFFDControlClosedErr(err) {
				continue
			}
			s.cache.cancelEviction(page, err)
			return err
		}
		if !controlRangesCover(target.ranges, released) {
			s.cache.cancelEviction(page, nil)
			return nil
		}
	}
	s.cache.mu.Lock()
	defer s.cache.mu.Unlock()
	if page.refs <= 0 || s.cache.pages[page.key] != page {
		s.cache.finishEvictionLocked(page, nil)
		return nil
	}
	err := s.cache.discardSlotLocked(page.shmOff)
	if err != nil {
		s.cache.finishEvictionLocked(page, err)
		return err
	}
	page.populated = false
	if s.cache.resident > 0 {
		s.cache.resident--
	}
	page.evictedAt = s.cache.clock
	page.protected = 0
	s.cache.clearProbeLocked(page)
	s.cache.finishEvictionLocked(page, nil)
	return nil
}

func controlRangesCover(want, got []uffdControlRange) bool {
	for _, w := range want {
		found := false
		for _, g := range got {
			if g.FileOffset == w.FileOffset && g.Length == w.Length {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (c *uffdSharedCache) cancelEviction(page *uffdSharedPage, err error) {
	c.mu.Lock()
	c.finishEvictionLocked(page, err)
	c.mu.Unlock()
}

func (c *uffdSharedCache) finishEvictionLocked(page *uffdSharedPage, err error) {
	if err == nil || !page.populated {
		page.err = err
	} else {
		page.err = nil
	}
	page.evicting = false
	page.cond.Broadcast()
}

func isUFFDControlClosedErr(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, net.ErrClosed)
}

func (c *uffdSharedCache) touchPage(page *uffdSharedPage) {
	c.mu.Lock()
	c.touchPageLocked(page)
	c.mu.Unlock()
}

func (c *uffdSharedCache) refaultProtectedLocked(page *uffdSharedPage) bool {
	return page.protected > c.clock
}

func (c *uffdSharedCache) touchPageLocked(page *uffdSharedPage) {
	c.clock++
	page.lastUsed = c.clock
	if page.probing || page.probeCold {
		page.probeHit = true
		page.probeCold = false
	}
}

func (c *uffdSharedCache) clearProbeLocked(page *uffdSharedPage) {
	page.probing = false
	page.probeSeq = 0
	page.probeHit = false
	page.probeCold = false
}

func (s *uffdSharedSession) requestRelease(ranges []uffdControlRange) ([]uffdControlRange, error) {
	ack, err := s.requestControl(smartmapFrameRelease, ranges)
	return ack.Released, err
}

func (s *uffdSharedSession) requestProbe(ranges []uffdControlRange) ([]uffdControlRange, error) {
	ack, err := s.requestControl(smartmapFrameProbe, ranges)
	return ack.Released, err
}

func (s *uffdSharedSession) requestWriteFault(ranges []uffdControlRange) error {
	_, err := s.requestControl(smartmapFrameWriteFault, ranges)
	return err
}

func (s *uffdSharedSession) requestControl(messageType string, ranges []uffdControlRange) (smartmapFrame, error) {
	s.ctrlMu.Lock()
	defer s.ctrlMu.Unlock()

	if err := writeSmartmapFrame(s.conn, smartmapFrame{Type: messageType, Ranges: ranges}); err != nil {
		return smartmapFrame{}, fmt.Errorf("send smartmap %s request: %w", messageType, err)
	}
	ack, fds, err := readSmartmapFrame(s.conn)
	if err != nil {
		return smartmapFrame{}, fmt.Errorf("read smartmap %s ack: %w", messageType, err)
	}
	if len(fds) != 0 {
		closeUFFDSharedFDs(fds)
		return smartmapFrame{}, fmt.Errorf("unexpected smartmap %s ack fds: %d", messageType, len(fds))
	}
	if ack.Type != smartmapFrameAck {
		return smartmapFrame{}, fmt.Errorf("unexpected smartmap %s ack type=%s", messageType, ack.Type)
	}
	if ack.OK == nil || !*ack.OK {
		if ack.Error == "" {
			ack.Error = fmt.Sprintf("client rejected smartmap %s", messageType)
		}
		return smartmapFrame{}, errors.New(ack.Error)
	}
	return ack, nil
}

func (s *uffdSharedSession) refreshMemoryRanges(ranges []uffdControlRange) error {
	ctx := vfs.NewLogContext(meta.Background())
	attr := &vfs.Attr{}
	if st := s.v.Meta.GetAttr(ctx, s.memory.inode, attr); st != 0 {
		return fmt.Errorf("getattr inode %d after client writeback: %w", s.memory.inode, st)
	}
	if attr.Typ != meta.TypeFile {
		return fmt.Errorf("memory inode %d is no longer a file", s.memory.inode)
	}
	for _, r := range ranges {
		if r.FileOffset%uffdHugePageSize != 0 || r.Length != uffdHugePageSize {
			return fmt.Errorf("refresh range at %d has invalid length %d", r.FileOffset, r.Length)
		}
		old := s.memory.pages[r.FileOffset]
		if old == nil {
			return fmt.Errorf("refresh range at %d is outside memory %s", r.FileOffset, s.memory.id)
		}
		source, err := captureUFFDPageSource(s.v, ctx, s.memory.inode, attr.Length, r.FileOffset)
		if err != nil {
			return err
		}
		if old.key == source.key {
			continue
		}
		page, err := s.cache.pageForKey(source.key, source)
		if err != nil {
			return err
		}
		if err := s.cache.replaceMemoryPage(s.memory, r.FileOffset, page); err != nil {
			return err
		}
	}
	return nil
}

func (c *uffdSharedCache) replaceMemoryPage(memory *uffdMemory, fileOff uint64, page *uffdSharedPage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	old := memory.pages[fileOff]
	if old == nil {
		c.releasePageLocked(page)
		return fmt.Errorf("page offset %d is outside memory %s", fileOff, memory.id)
	}
	if old == page {
		return nil
	}
	memory.pages[fileOff] = page
	memory.extents = rebuildUFFDMemoryExtents(memory)
	if err := c.releasePageLocked(old); err != nil {
		return err
	}
	return nil
}

func rebuildUFFDMemoryExtents(memory *uffdMemory) []UFFDExtent {
	extents := make([]UFFDExtent, 0, len(memory.pages))
	for off := uint64(0); off < memory.size; off += uffdHugePageSize {
		page := memory.pages[off]
		if page == nil {
			continue
		}
		extents = appendUFFDExtent(extents, UFFDExtent{FileOffset: off, Length: uffdHugePageSize, ShmOffset: page.shmOff})
	}
	return extents
}

func (s *uffdSharedSession) readSharedPage(buf []byte, source uffdPageSource) error {
	written := 0
	for _, slice := range source.slices {
		end := written + int(slice.Len)
		if end > len(buf) {
			return fmt.Errorf("shared page source inode %d chunk %d offset %d overflows page: %d > %d",
				source.inode, source.chunkIndex, source.offset, end, len(buf))
		}
		if slice.Id == 0 {
			clear(buf[written:end])
			written = end
			continue
		}
		reader := s.v.Store.NewReader(slice.Id, int(slice.Size))
		read := 0
		for read < int(slice.Len) {
			page := chunk.NewPage(buf[written+read : end])
			n, err := reader.ReadAt(s.ctx, page, int(slice.Off)+read)
			page.Release()
			if n == 0 {
				if err != nil {
					return fmt.Errorf("read shared page source inode %d slice %d at %d: %w", source.inode, slice.Id, int(slice.Off)+read, err)
				}
				return fmt.Errorf("read shared page source inode %d slice %d at %d: short read", source.inode, slice.Id, int(slice.Off)+read)
			}
			read += n
		}
		written = end
	}
	if written != len(buf) {
		clear(buf[written:])
		return fmt.Errorf("shared page source inode %d chunk %d offset %d produced %d bytes, want %d",
			source.inode, source.chunkIndex, source.offset, written, len(buf))
	}
	return nil
}

func (s *uffdSharedSession) signalWake() {
	_, _ = syscall.Write(s.wakeW, []byte{1})
}
