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

package uffd

/*
#include <linux/userfaultfd.h>
#include <sys/syscall.h>
#include <sys/ioctl.h>

#ifndef UFFD_FEATURE_WP_ASYNC
#define UFFD_FEATURE_WP_ASYNC (1 << 15)
#endif

#ifndef UFFDIO_COPY_MODE_WP
#define UFFDIO_COPY_MODE_WP ((__u64)1 << 1)
#endif

struct jfs_uffd_pagefault {
	__u64 flags;
	__u64 address;
	__u32 ptid;
};
*/
import "C"

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path"
	"runtime"
	"sync"
	"syscall"
	"unsafe"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/juicedata/juicefs/pkg/vfs"
	"golang.org/x/sys/unix"
)

var logger = utils.GetLogger("juicefs")

const (
	uffdSocketPayloadSize = 64 << 10
	uffdHugePageSize      = 2 << 20

	uffdOpServeFileCopy    = "serve_file_copy"
	uffdOpOpenMemoryFile   = "open_memory_file"
	uffdOpCloseMemoryFile  = "close_memory_file"
	uffdOpServeMemoryFault = "serve_memory_faults"

	nrUserfaultfd = C.__NR_userfaultfd
	uffdAPI       = C.UFFD_API

	uffdEventPagefault = C.UFFD_EVENT_PAGEFAULT

	uffdFeatureMissingHugetlbfs = C.UFFD_FEATURE_MISSING_HUGETLBFS
	uffdFeatureMinorHugetlbfs   = C.UFFD_FEATURE_MINOR_HUGETLBFS
	uffdFeaturePagefaultFlagWP  = C.UFFD_FEATURE_PAGEFAULT_FLAG_WP
	uffdFeatureWPHugetlbfsShmem = C.UFFD_FEATURE_WP_HUGETLBFS_SHMEM
	uffdFeatureWPAsync          = C.UFFD_FEATURE_WP_ASYNC

	uffdPagefaultFlagWrite = C.UFFD_PAGEFAULT_FLAG_WRITE
	uffdPagefaultFlagMinor = C.UFFD_PAGEFAULT_FLAG_MINOR
	uffdPagefaultFlagWP    = C.UFFD_PAGEFAULT_FLAG_WP

	uffdioAPIIoctl             = C.UFFDIO_API
	uffdioContinueIoctl        = C.UFFDIO_CONTINUE
	uffdioCopyIoctl            = C.UFFDIO_COPY
	uffdioRegisterIoctl        = C.UFFDIO_REGISTER
	uffdioWakeIoctl            = C.UFFDIO_WAKE
	uffdioWriteProtectIoctl    = C.UFFDIO_WRITEPROTECT
	uffdioRegisterModeMissing  = C.UFFDIO_REGISTER_MODE_MISSING
	uffdioRegisterModeMinor    = C.UFFDIO_REGISTER_MODE_MINOR
	uffdioRegisterModeWP       = C.UFFDIO_REGISTER_MODE_WP
	uffdioCopyModeDontWake     = C.UFFDIO_COPY_MODE_DONTWAKE
	uffdioCopyModeWP           = C.UFFDIO_COPY_MODE_WP
	uffdioContinueModeDontWake = C.UFFDIO_CONTINUE_MODE_DONTWAKE
	uffdioWriteProtectModeWP   = C.UFFDIO_WRITEPROTECT_MODE_WP
)

var uffdFdSize = 4

type uffdMsg = C.struct_uffd_msg
type uffdPagefault = C.struct_jfs_uffd_pagefault
type uffdioAPI = C.struct_uffdio_api
type uffdioContinue = C.struct_uffdio_continue
type uffdioCopy = C.struct_uffdio_copy
type uffdioRange = C.struct_uffdio_range
type uffdioRegister = C.struct_uffdio_register
type uffdioWriteProtect = C.struct_uffdio_writeprotect

func newUFFDIOAPI(api, features uint64) uffdioAPI {
	return uffdioAPI{
		api:      C.ulonglong(api),
		features: C.ulonglong(features),
	}
}

func newUFFDIORange(start, length uintptr) uffdioRange {
	return uffdioRange{
		start: C.ulonglong(start),
		len:   C.ulonglong(length),
	}
}

func newUFFDIORegister(start, length uintptr, mode uint64) uffdioRegister {
	return uffdioRegister{
		_range: newUFFDIORange(start, length),
		mode:   C.ulonglong(mode),
	}
}

type UFFDRegion struct {
	BaseHostVirtAddr uintptr `json:"base_host_virt_addr"`
	Size             uintptr `json:"size"`
	Offset           uint64  `json:"offset"`
	PageSize         uintptr `json:"page_size"`
}

type UFFDRequest struct {
	Op                  string       `json:"op,omitempty"`
	Path                string       `json:"path,omitempty"`
	Size                uint64       `json:"size,omitempty"`
	MemoryID            string       `json:"memory_id,omitempty"`
	MaxResidentPages    int          `json:"max_resident_pages,omitempty"`
	EvictionPolicy      string       `json:"eviction_policy,omitempty"`
	ProbeCandidatePages int          `json:"probe_candidate_pages,omitempty"`
	ProbeEvictPages     int          `json:"probe_evict_pages,omitempty"`
	ProbeMonitorMillis  int          `json:"probe_monitor_millis,omitempty"`
	Regions             []UFFDRegion `json:"regions,omitempty"`
	Mappings            []UFFDRegion `json:"mappings,omitempty"`
}

type UFFDExtent struct {
	FileOffset uint64 `json:"file_offset"`
	Length     uint64 `json:"length"`
	ShmOffset  uint64 `json:"shm_offset"`
}

type uffdResponse struct {
	OK       bool         `json:"ok"`
	Error    string       `json:"error,omitempty"`
	MemoryID string       `json:"memory_id,omitempty"`
	Extents  []UFFDExtent `json:"extents,omitempty"`
}

func (r UFFDRegion) endHostVirtAddr() uintptr {
	return r.BaseHostVirtAddr + r.Size
}

func (r UFFDRegion) contains(addr uintptr) bool {
	return addr >= r.BaseHostVirtAddr && addr < r.endHostVirtAddr()
}

func (r UFFDRegion) fileOffset(addr uintptr) uint64 {
	return r.Offset + uint64(addr-r.BaseHostVirtAddr)
}

func validateUFFDRegions(regions []UFFDRegion) error {
	if len(regions) == 0 {
		return errors.New("no uffd regions")
	}
	for i, r := range regions {
		if r.BaseHostVirtAddr == 0 {
			return fmt.Errorf("region %d has zero base_host_virt_addr", i)
		}
		if r.Size == 0 {
			return fmt.Errorf("region %d has zero size", i)
		}
		if r.PageSize == 0 || r.PageSize&(r.PageSize-1) != 0 {
			return fmt.Errorf("region %d has invalid page_size %d", i, r.PageSize)
		}
		if r.BaseHostVirtAddr%r.PageSize != 0 || r.Size%r.PageSize != 0 {
			return fmt.Errorf("region %d is not page aligned", i)
		}
		if r.endHostVirtAddr() < r.BaseHostVirtAddr {
			return fmt.Errorf("region %d overflows address space", i)
		}
	}
	return nil
}

func findUFFDRegion(regions []UFFDRegion, addr uintptr) (*UFFDRegion, error) {
	for i := range regions {
		if regions[i].contains(addr) {
			return &regions[i], nil
		}
	}
	return nil, fmt.Errorf("fault address %d is outside uffd regions", addr)
}

type uffdSession struct {
	v       *vfs.VFS
	ctx     vfs.LogContext
	req     UFFDRequest
	ino     vfs.Ino
	fh      uint64
	uffdFd  int
	connFd  int
	exitR   int
	wakeR   int
	wakeW   int
	done    <-chan struct{}
	retries []uffdPagefault
}

// Start serves UFFD requests for a mounted VFS instance on socketPath.
func Start(v *vfs.VFS, socketPath string) (func(), error) {
	if socketPath == "" {
		return func() {}, nil
	}
	if st, err := os.Lstat(socketPath); err == nil {
		if st.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("refusing to remove non-socket uffd path: %s", socketPath)
		}
		if err := os.Remove(socketPath); err != nil {
			return nil, fmt.Errorf("remove stale uffd socket: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("inspect uffd socket path: %w", err)
	}
	lis, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("listen on uffd socket: %w", err)
	}
	if err := os.Chmod(socketPath, 0600); err != nil {
		_ = lis.Close()
		_ = os.Remove(socketPath)
		return nil, fmt.Errorf("chmod uffd socket: %w", err)
	}

	done := make(chan struct{})
	cache := newUFFDSharedCache()
	var once sync.Once
	var sessions sync.WaitGroup
	var sessionsMu sync.Mutex
	conns := make(map[*net.UnixConn]struct{})
	stop := func() {
		once.Do(func() {
			close(done)
			_ = lis.Close()
			sessionsMu.Lock()
			for conn := range conns {
				_ = conn.Close()
			}
			sessionsMu.Unlock()
			sessions.Wait()
			cache.close()
			_ = os.Remove(socketPath)
		})
	}

	go func() {
		for {
			conn, err := lis.AcceptUnix()
			if err != nil {
				select {
				case <-done:
				default:
					logger.Warnf("accept uffd socket %s: %s", socketPath, err)
				}
				return
			}
			sessionsMu.Lock()
			select {
			case <-done:
				sessionsMu.Unlock()
				_ = conn.Close()
				return
			default:
			}
			conns[conn] = struct{}{}
			sessions.Add(1)
			sessionsMu.Unlock()
			go func() {
				defer func() {
					sessionsMu.Lock()
					delete(conns, conn)
					sessionsMu.Unlock()
					sessions.Done()
				}()
				handleUFFDConn(v, conn, done, cache)
			}()
		}
	}()

	logger.Infof("serving userfaultfd requests on %s", socketPath)
	return stop, nil
}

func handleUFFDConn(v *vfs.VFS, conn *net.UnixConn, done <-chan struct{}, cache *uffdSharedCache) {
	defer conn.Close()
	req, fds, err := readUFFDRequest(conn)
	if err == nil && (req.Op == "" || req.Op == uffdOpServeFileCopy) {
		err = validateUFFDRegions(req.Regions)
	}
	if err != nil {
		for _, fd := range fds {
			_ = syscall.Close(fd)
		}
		writeUFFDResponse(conn, err)
		return
	}
	if req.Op != "" && req.Op != uffdOpServeFileCopy {
		handleUFFDSharedOp(v, conn, done, cache, req, fds)
		return
	}
	if len(fds) != 1 {
		for _, fd := range fds {
			_ = syscall.Close(fd)
		}
		writeUFFDResponse(conn, fmt.Errorf("expected exactly one uffd fd, got %d", len(fds)))
		return
	}
	connFd, err := unixConnFD(conn)
	if err != nil {
		_ = syscall.Close(fds[0])
		writeUFFDResponse(conn, err)
		return
	}

	ctx := vfs.NewLogContext(peerMetaContext(conn))
	s := &uffdSession{v: v, ctx: ctx, req: req, uffdFd: fds[0], connFd: connFd, done: done}
	err = s.serve()
	if err != nil {
		logger.Warnf("uffd session for %s ended with error: %s", req.Path, err)
	}
	writeUFFDResponse(conn, err)
}

func readUFFDRequest(conn *net.UnixConn) (UFFDRequest, []int, error) {
	payload := make([]byte, uffdSocketPayloadSize)
	oob := make([]byte, syscall.CmsgSpace(uffdFdSize))
	n, oobn, flags, _, err := conn.ReadMsgUnix(payload, oob)
	if err != nil {
		return UFFDRequest{}, nil, fmt.Errorf("read uffd socket message: %w", err)
	}
	if flags&unix.MSG_CTRUNC != 0 {
		return UFFDRequest{}, nil, errors.New("uffd socket control message truncated")
	}
	if flags&unix.MSG_TRUNC != 0 {
		return UFFDRequest{}, nil, errors.New("uffd socket payload truncated")
	}
	var req UFFDRequest
	if err := json.Unmarshal(payload[:n], &req); err != nil {
		return UFFDRequest{}, nil, fmt.Errorf("parse uffd request: %w", err)
	}
	msgs, err := syscall.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return UFFDRequest{}, nil, fmt.Errorf("parse uffd control message: %w", err)
	}
	var fds []int
	for i := range msgs {
		rights, err := syscall.ParseUnixRights(&msgs[i])
		if err != nil {
			for _, fd := range fds {
				_ = syscall.Close(fd)
			}
			return UFFDRequest{}, nil, fmt.Errorf("parse uffd unix rights: %w", err)
		}
		fds = append(fds, rights...)
	}
	return req, fds, nil
}

func writeUFFDResponse(conn *net.UnixConn, err error) {
	resp := uffdResponse{OK: err == nil}
	if err != nil {
		resp.Error = err.Error()
	}
	_ = json.NewEncoder(conn).Encode(resp)
}

func writeUFFDResponseWithFD(conn *net.UnixConn, resp uffdResponse, fd int) error {
	payload, err := json.Marshal(resp)
	if err != nil {
		writeUFFDResponse(conn, err)
		return err
	}
	_, _, err = conn.WriteMsgUnix(payload, syscall.UnixRights(fd), nil)
	return err
}

func peerMetaContext(conn *net.UnixConn) meta.Context {
	var cred *unix.Ucred
	raw, err := conn.SyscallConn()
	if err == nil {
		_ = raw.Control(func(fd uintptr) {
			cred, err = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
		})
	}
	if err != nil || cred == nil {
		return meta.Background()
	}
	return meta.NewContext(uint32(cred.Pid), cred.Uid, []uint32{cred.Gid})
}

func unixConnFD(conn *net.UnixConn) (int, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return -1, fmt.Errorf("get uffd socket fd: %w", err)
	}
	connFd := -1
	if err := raw.Control(func(fd uintptr) {
		connFd = int(fd)
	}); err != nil {
		return -1, fmt.Errorf("inspect uffd socket fd: %w", err)
	}
	return connFd, nil
}

func (s *uffdSession) serve() error {
	if err := s.openFile(); err != nil {
		_ = syscall.Close(s.uffdFd)
		return err
	}
	defer s.v.Release(s.ctx, s.ino, s.fh)
	defer syscall.Close(s.uffdFd)

	exitPipe := [2]int{}
	if err := syscall.Pipe2(exitPipe[:], syscall.O_NONBLOCK|syscall.O_CLOEXEC); err != nil {
		return fmt.Errorf("create uffd exit pipe: %w", err)
	}
	defer syscall.Close(exitPipe[0])
	defer syscall.Close(exitPipe[1])
	s.exitR = exitPipe[0]
	exitW := exitPipe[1]

	wakePipe := [2]int{}
	if err := syscall.Pipe2(wakePipe[:], syscall.O_NONBLOCK|syscall.O_CLOEXEC); err != nil {
		return fmt.Errorf("create uffd wake pipe: %w", err)
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

func (s *uffdSession) openFile() error {
	if s.req.Path == "" {
		return errors.New("uffd request path is empty")
	}
	var ino vfs.Ino
	attr := &vfs.Attr{}
	p := path.Clean(s.req.Path)
	if !path.IsAbs(p) {
		return fmt.Errorf("uffd request path must be absolute: %s", s.req.Path)
	}
	if st := s.v.Meta.Resolve(s.ctx, meta.RootInode, p, &ino, attr, true); st != 0 {
		return fmt.Errorf("resolve %s: %w", p, st)
	}
	if attr.Typ != meta.TypeFile {
		return fmt.Errorf("uffd request path is not a file: %s", p)
	}
	entry, fh, st := s.v.Open(s.ctx, ino, syscall.O_RDONLY)
	if st != 0 {
		return fmt.Errorf("open %s: %w", p, st)
	}
	s.ino, s.fh = entry.Inode, fh
	return nil
}

func (s *uffdSession) poll() error {
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
			return fmt.Errorf("poll uffd: %w", err)
		}
		if hasPollEvent(pollFds[1].Revents, unix.POLLIN) {
			return nil
		}
		if hasPollEvent(pollFds[2].Revents, unix.POLLIN) {
			drainFd(s.wakeR)
		}
		if hasPollEvent(pollFds[3].Revents, unix.POLLERR|unix.POLLNVAL) {
			return fmt.Errorf("uffd request socket poll error: revents=%#x", pollFds[3].Revents)
		}
		if hasPollEvent(pollFds[3].Revents, unix.POLLHUP) {
			return nil
		}
		if hasPollEvent(pollFds[3].Revents, unix.POLLIN) {
			closed, err := socketClosed(s.connFd)
			if err != nil {
				return err
			}
			if closed {
				return nil
			}
		}
		if hasPollEvent(pollFds[0].Revents, unix.POLLHUP) {
			return nil
		}
		if hasPollEvent(pollFds[0].Revents, unix.POLLERR|unix.POLLNVAL) {
			return fmt.Errorf("uffd poll error: revents=%#x", pollFds[0].Revents)
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

func readUFFDFaults(fd int) ([]uffdPagefault, error) {
	msgSize := int(unsafe.Sizeof(uffdMsg{}))
	buf := make([]byte, msgSize)
	var faults []uffdPagefault
	for {
		n, err := syscall.Read(fd, buf)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if errors.Is(err, syscall.EAGAIN) || n == 0 {
			return faults, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read uffd: %w", err)
		}
		if n != msgSize {
			return nil, fmt.Errorf("short uffd message: got %d, want %d", n, msgSize)
		}
		msg := (*uffdMsg)(unsafe.Pointer(&buf[0]))
		if msg.event != uffdEventPagefault {
			return nil, fmt.Errorf("unsupported uffd event: %d", msg.event)
		}
		arg := msg.arg
		faults = append(faults, *(*uffdPagefault)(unsafe.Pointer(&arg[0])))
	}
}

func (s *uffdSession) handleFault(pf *uffdPagefault) error {
	if pf.flags&uffdPagefaultFlagMinor != 0 {
		return errors.New("unsupported uffd MINOR page fault")
	}
	if pf.flags&uffdPagefaultFlagWP != 0 {
		return errors.New("unsupported uffd WP page fault")
	}
	addr := uintptr(pf.address)
	region, err := findUFFDRegion(s.req.Regions, addr)
	if err != nil {
		return err
	}
	pageAddr := addr & ^(region.PageSize - 1)
	off := region.fileOffset(pageAddr)
	buf := make([]byte, region.PageSize)
	if err := s.readPage(buf, off); err != nil {
		return err
	}
	err = uffdCopy(s.uffdFd, pageAddr, region.PageSize, buf)
	if errors.Is(err, unix.EEXIST) || errors.Is(err, unix.ESRCH) {
		return nil
	}
	if errors.Is(err, unix.EAGAIN) {
		s.retries = append(s.retries, *pf)
		s.signalWake()
		return nil
	}
	return err
}

func (s *uffdSession) readPage(buf []byte, off uint64) error {
	for read := 0; read < len(buf); {
		n, st := s.v.Read(s.ctx, s.ino, buf[read:], off+uint64(read), s.fh)
		if st != 0 {
			return fmt.Errorf("read page at %d: %w", off+uint64(read), st)
		}
		if n == 0 {
			break
		}
		read += n
	}
	return nil
}

func uffdCopy(fd int, addr, pageSize uintptr, data []byte) error {
	return uffdCopyMode(fd, addr, pageSize, data, 0)
}

func uffdCopyMode(fd int, addr, pageSize uintptr, data []byte, mode uint64) error {
	if len(data) == 0 {
		return errors.New("cannot uffd copy empty page")
	}
	var pinner runtime.Pinner
	pinner.Pin(&data[0])
	defer pinner.Unpin()

	cpy := uffdioCopy{
		dst:  C.ulonglong(addr & ^(pageSize - 1)),
		src:  C.ulonglong(uintptr(unsafe.Pointer(&data[0]))),
		len:  C.ulonglong(pageSize),
		mode: C.ulonglong(mode),
		copy: 0,
	}
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), uffdioCopyIoctl, uintptr(unsafe.Pointer(&cpy))); errno != 0 {
		return errno
	}
	return classifyUFFDCopyResult(int64(cpy.copy), int64(pageSize))
}

func uffdContinue(fd int, addr, pageSize uintptr) error {
	return uffdContinueMode(fd, addr, pageSize, 0)
}

func uffdContinueMode(fd int, addr, pageSize uintptr, mode uint64) error {
	cont := uffdioContinue{
		_range: newUFFDIORange(addr & ^(pageSize-1), pageSize),
		mode:   C.ulonglong(mode),
		mapped: 0,
	}
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), uffdioContinueIoctl, uintptr(unsafe.Pointer(&cont))); errno != 0 {
		return errno
	}
	return classifyUFFDContinueResult(int64(cont.mapped), int64(pageSize))
}

func uffdWake(fd int, addr, pageSize uintptr) error {
	rng := newUFFDIORange(addr & ^(pageSize-1), pageSize)
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), uffdioWakeIoctl, uintptr(unsafe.Pointer(&rng))); errno != 0 {
		return errno
	}
	return nil
}

func uffdWriteProtect(fd int, addr, pageSize uintptr, protect bool) error {
	mode := uint64(0)
	if protect {
		mode = uint64(uffdioWriteProtectModeWP)
	}
	wp := uffdioWriteProtect{
		_range: newUFFDIORange(addr & ^(pageSize-1), pageSize),
		mode:   C.ulonglong(mode),
	}
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), uffdioWriteProtectIoctl, uintptr(unsafe.Pointer(&wp))); errno != 0 {
		return errno
	}
	return nil
}

func classifyUFFDCopyResult(copied, pageSize int64) error {
	if copied < 0 {
		return syscall.Errno(-copied)
	}
	if copied != pageSize {
		return syscall.EAGAIN
	}
	return nil
}

func classifyUFFDContinueResult(mapped, pageSize int64) error {
	if mapped < 0 {
		return syscall.Errno(-mapped)
	}
	if mapped != pageSize {
		return syscall.EAGAIN
	}
	return nil
}

func hasPollEvent(revents int16, event int16) bool {
	return revents&event != 0
}

func drainFd(fd int) {
	var buf [64]byte
	for {
		if _, err := syscall.Read(fd, buf[:]); err != nil {
			return
		}
	}
}

func socketClosed(fd int) (bool, error) {
	var buf [1]byte
	n, _, _, _, err := unix.Recvmsg(fd, buf[:], nil, unix.MSG_PEEK|unix.MSG_DONTWAIT)
	if err == nil {
		if n == 0 {
			return true, nil
		}
		return false, errors.New("unexpected data on uffd request socket")
	}
	if err == unix.ECONNRESET || err == unix.ENOTCONN {
		return true, nil
	}
	if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
		return false, nil
	}
	return false, fmt.Errorf("peek uffd request socket: %w", err)
}

func (s *uffdSession) signalWake() {
	_, _ = syscall.Write(s.wakeW, []byte{1})
}
