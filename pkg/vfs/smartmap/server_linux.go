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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
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
	BaseHostVirtAddr uintptr
	Size             uintptr
	Offset           uint64
	PageSize         uintptr
}

type UFFDExtent struct {
	FileOffset uint64 `json:"file_offset"`
	Length     uint64 `json:"length"`
	ShmOffset  uint64 `json:"shm_offset"`
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

func findUFFDRegion(regions []UFFDRegion, addr uintptr) (*UFFDRegion, error) {
	for i := range regions {
		if regions[i].contains(addr) {
			return &regions[i], nil
		}
	}
	return nil, fmt.Errorf("fault address %d is outside smartmap mappings", addr)
}

// Start serves Smartmap UFFD memory requests for a mounted VFS instance on socketPath.
func Start(v *vfs.VFS, socketPath string) (func(), error) {
	return StartWithOptions(v, socketPath, Options{})
}

func StartWithOptions(v *vfs.VFS, socketPath string, options Options) (func(), error) {
	if socketPath == "" {
		return func() {}, nil
	}
	maxResidentPages, err := validateOptions(options)
	if err != nil {
		return nil, err
	}
	if st, err := os.Lstat(socketPath); err == nil {
		if st.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("refusing to remove non-socket smartmap path: %s", socketPath)
		}
		if err := os.Remove(socketPath); err != nil {
			return nil, fmt.Errorf("remove stale smartmap socket: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("inspect smartmap socket path: %w", err)
	}
	lis, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("listen on smartmap socket: %w", err)
	}
	if err := os.Chmod(socketPath, 0600); err != nil {
		_ = lis.Close()
		_ = os.Remove(socketPath)
		return nil, fmt.Errorf("chmod smartmap socket: %w", err)
	}

	done := make(chan struct{})
	cache := newUFFDSharedCache(uffdSharedCacheOptions{
		maxResidentPages:    maxResidentPages,
		evictionPolicy:      options.EvictionPolicy,
		probeCandidatePages: options.ProbeCandidatePages,
		probeEvictPages:     options.ProbeEvictPages,
		probeMonitorMillis:  options.ProbeMonitorMillis,
	})
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
					logger.Warnf("accept smartmap socket %s: %s", socketPath, err)
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

	logger.Infof("serving Smartmap userfaultfd requests on %s", socketPath)
	return stop, nil
}

func validateOptions(options Options) (int, error) {
	switch options.EvictionPolicy {
	case "", "lru", uffdEvictionPolicyProbe:
	default:
		return 0, fmt.Errorf("unsupported smartmap eviction policy: %s", options.EvictionPolicy)
	}
	if options.ProbeCandidatePages < 0 {
		return 0, fmt.Errorf("probe candidate pages must be non-negative, got %d", options.ProbeCandidatePages)
	}
	if options.ProbeEvictPages < 0 {
		return 0, fmt.Errorf("probe evict pages must be non-negative, got %d", options.ProbeEvictPages)
	}
	if options.ProbeMonitorMillis < 0 {
		return 0, fmt.Errorf("probe monitor milliseconds must be non-negative, got %d", options.ProbeMonitorMillis)
	}
	if options.ResidentSize == 0 {
		if options.EvictionPolicy == uffdEvictionPolicyProbe {
			return 0, errors.New("probe eviction requires smartmap resident size")
		}
		return 0, nil
	}
	if options.ResidentSize%uffdHugePageSize != 0 {
		return 0, fmt.Errorf("smartmap resident size must be a %d-byte multiple", uffdHugePageSize)
	}
	pages := options.ResidentSize / uffdHugePageSize
	if pages > uint64(int(^uint(0)>>1)) {
		return 0, fmt.Errorf("smartmap resident size is too large: %d", options.ResidentSize)
	}
	return int(pages), nil
}

func handleUFFDConn(v *vfs.VFS, conn *net.UnixConn, done <-chan struct{}, cache *uffdSharedCache) {
	defer conn.Close()
	sock := smartmapConn{conn: conn}
	frame, fds, err := sock.readFrame()
	if err != nil {
		for _, fd := range fds {
			_ = syscall.Close(fd)
		}
		sock.writeFatal(err)
		return
	}
	handleSmartmapSession(v, sock, done, cache, frame, fds)
}

type smartmapConn struct {
	conn *net.UnixConn
}

func (c smartmapConn) readFrame() (smartmapFrame, []int, error) {
	payload := make([]byte, uffdSocketPayloadSize)
	oob := make([]byte, syscall.CmsgSpace(uffdFdSize))
	n, oobn, flags, _, err := c.conn.ReadMsgUnix(payload, oob)
	if err != nil {
		return smartmapFrame{}, nil, fmt.Errorf("read smartmap socket message: %w", err)
	}
	fds, err := parseSmartmapFrameFDs(oob[:oobn])
	if err != nil {
		return smartmapFrame{}, nil, err
	}
	if flags&unix.MSG_CTRUNC != 0 {
		closeUFFDSharedFDs(fds)
		return smartmapFrame{}, nil, errors.New("smartmap socket control message truncated")
	}
	if flags&unix.MSG_TRUNC != 0 {
		closeUFFDSharedFDs(fds)
		return smartmapFrame{}, nil, errors.New("smartmap socket payload truncated")
	}
	var frame smartmapFrame
	dec := json.NewDecoder(bytes.NewReader(payload[:n]))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&frame); err != nil {
		closeUFFDSharedFDs(fds)
		return smartmapFrame{}, nil, fmt.Errorf("parse smartmap frame: %w", err)
	}
	if err := validateSmartmapFrameFDs(frame, fds); err != nil {
		closeUFFDSharedFDs(fds)
		return smartmapFrame{}, nil, err
	}
	return frame, fds, nil
}

func parseSmartmapFrameFDs(oob []byte) ([]int, error) {
	msgs, err := syscall.ParseSocketControlMessage(oob)
	if err != nil {
		return nil, fmt.Errorf("parse smartmap control message: %w", err)
	}
	var fds []int
	for i := range msgs {
		rights, err := syscall.ParseUnixRights(&msgs[i])
		if err != nil {
			for _, fd := range fds {
				_ = syscall.Close(fd)
			}
			return nil, fmt.Errorf("parse smartmap unix rights: %w", err)
		}
		fds = append(fds, rights...)
	}
	return fds, nil
}

func validateSmartmapFrameFDs(frame smartmapFrame, fds []int) error {
	switch frame.FD {
	case "":
		if len(fds) != 0 {
			return fmt.Errorf("smartmap frame %s expected no fds, got %d", frame.Type, len(fds))
		}
	case smartmapFDMemory, smartmapFDUFFD:
		if len(fds) != 1 {
			return fmt.Errorf("smartmap frame %s expected exactly one %s fd, got %d", frame.Type, frame.FD, len(fds))
		}
	default:
		return fmt.Errorf("smartmap frame %s has unsupported fd purpose %q", frame.Type, frame.FD)
	}
	return nil
}

func (c smartmapConn) writeFrame(frame smartmapFrame) error {
	if frame.FD != "" {
		return fmt.Errorf("smartmap frame %s must use writeFrameWithFD for fd purpose %q", frame.Type, frame.FD)
	}
	payload, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	_, _, err = c.conn.WriteMsgUnix(payload, nil, nil)
	return err
}

func (c smartmapConn) writeFrameWithFD(frame smartmapFrame, purpose string, fd int) error {
	switch purpose {
	case smartmapFDMemory, smartmapFDUFFD:
	default:
		return fmt.Errorf("unsupported smartmap fd purpose %q", purpose)
	}
	if fd < 0 {
		return fmt.Errorf("invalid smartmap %s fd: %d", purpose, fd)
	}
	frame.FD = purpose
	payload, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	_, _, err = c.conn.WriteMsgUnix(payload, syscall.UnixRights(fd), nil)
	return err
}

func (c smartmapConn) writeFatal(err error) {
	if err == nil {
		return
	}
	_ = c.writeFrame(smartmapFrame{Type: smartmapFrameFatal, Error: err.Error()})
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
		return -1, fmt.Errorf("get smartmap socket fd: %w", err)
	}
	connFd := -1
	if err := raw.Control(func(fd uintptr) {
		connFd = int(fd)
	}); err != nil {
		return -1, fmt.Errorf("inspect smartmap socket fd: %w", err)
	}
	return connFd, nil
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
