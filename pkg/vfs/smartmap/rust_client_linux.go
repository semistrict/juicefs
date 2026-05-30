//go:build linux && cgo
// +build linux,cgo

package smartmap

/*
#cgo CFLAGS: -I${SRCDIR}/../../../sdk/rust/smartmap/include
#cgo LDFLAGS: ${SRCDIR}/../../../sdk/rust/smartmap/target/debug/libjuicefs_smartmap.a -ldl -lpthread -lm
#include <stdint.h>
#include <stdlib.h>
#include "juicefs_smartmap.h"

extern int goSmartmapEvict(void *userdata, jfs_smartmap_range *ranges, size_t len);
extern int goSmartmapProbe(void *userdata, jfs_smartmap_range *ranges, size_t len);
*/
import "C"

import (
	"errors"
	"fmt"
	"runtime/cgo"
	"strings"
	"time"
	"unsafe"
)

type RustControlRange struct {
	FileOffset uint64
	Length     uint64
	ShmOffset  uint64
}

type RustControlHandler interface {
	Evict([]RustControlRange) error
	Probe([]RustControlRange) error
}

type RustClient struct {
	client          *C.jfs_smartmap_client
	memory          *C.jfs_smartmap_memory
	mapping         *C.jfs_smartmap_mapping
	uffd            *C.jfs_smartmap_uffd
	session         *C.jfs_smartmap_session
	handle          *cgo.Handle
	mapped          []byte
	memoryID        string
	uffdFD          int
	base            uintptr
	controlsStarted bool
	controlsDone    chan struct{}
}

func OpenRustClient(sock, path string, size, regionSize uint64, handler RustControlHandler) (*RustClient, error) {
	rc, err := OpenRustMemory(sock, path, size)
	if err != nil {
		return nil, err
	}
	cleanupOnError := func() {
		rc.Close()
	}

	var errC *C.char
	if C.jfs_smartmap_map_private(rc.memory, &rc.mapping, &errC) != 0 {
		msg := peekRustSmartmapError(errC)
		cleanupOnError()
		C.jfs_smartmap_string_free(errC)
		return nil, errors.New(msg)
	}
	ptr := C.jfs_smartmap_mapping_ptr(rc.mapping)
	length := int(C.jfs_smartmap_mapping_len(rc.mapping))
	if ptr == nil || length <= 0 {
		cleanupOnError()
		return nil, errors.New("rust smartmap mapping returned empty range")
	}
	rc.mapped = unsafe.Slice((*byte)(ptr), length)
	rc.base = uintptr(ptr)
	if C.jfs_smartmap_create_uffd(rc.memory, rc.mapping, &rc.uffd, &errC) != 0 {
		msg := peekRustSmartmapError(errC)
		cleanupOnError()
		C.jfs_smartmap_string_free(errC)
		return nil, errors.New(msg)
	}
	if regionSize == 0 {
		regionSize = uint64(length)
	}
	if regionSize > uint64(length) {
		cleanupOnError()
		return nil, fmt.Errorf("region_size %d exceeds mapping size %d", regionSize, length)
	}
	if C.jfs_smartmap_serve_faults_len(rc.memory, rc.uffd, rc.mapping, C.size_t(regionSize), &rc.session, &errC) != 0 {
		cleanupOnError()
		return nil, takeRustSmartmapError(errC)
	}
	rc.uffdFD = int(C.jfs_smartmap_uffd_raw_fd(rc.uffd))
	if handler != nil {
		handle := cgo.NewHandle(handler)
		rc.handle = &handle
		rc.controlsDone = make(chan struct{})
	}
	return rc, nil
}

func OpenRustMemory(sock, path string, size uint64) (*RustClient, error) {
	if path == "" {
		return nil, errors.New("rust smartmap client requires path")
	}
	sockC := C.CString(sock)
	defer C.free(unsafe.Pointer(sockC))
	pathC := C.CString(path)
	defer C.free(unsafe.Pointer(pathC))

	var err *C.char
	var client *C.jfs_smartmap_client
	if C.jfs_smartmap_client_new(sockC, &client, &err) != 0 {
		return nil, takeRustSmartmapError(err)
	}
	rc := &RustClient{client: client}
	cleanupOnError := func() {
		rc.Close()
	}

	if C.jfs_smartmap_open_memory(client, pathC, C.uint64_t(size), &rc.memory, &err) != 0 {
		msg := peekRustSmartmapError(err)
		cleanupOnError()
		C.jfs_smartmap_string_free(err)
		return nil, errors.New(msg)
	}
	memoryIDC := C.jfs_smartmap_memory_id(rc.memory)
	if memoryIDC == nil {
		cleanupOnError()
		return nil, errors.New("rust smartmap client returned empty memory id")
	}
	rc.memoryID = C.GoString(memoryIDC)
	C.jfs_smartmap_string_free((*C.char)(memoryIDC))
	return rc, nil
}

func (c *RustClient) Close() {
	c.CloseFaults()
	if c.mapping != nil {
		C.jfs_smartmap_mapping_free(c.mapping)
		c.mapping = nil
	}
	if c.memory != nil {
		deadline := time.Now().Add(5 * time.Second)
		for {
			err := c.CloseMemory()
			if err == nil || !strings.Contains(err.Error(), "active") || time.Now().After(deadline) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if c.client != nil {
		C.jfs_smartmap_client_free(c.client)
		c.client = nil
	}
}

func (c *RustClient) CloseMemory() error {
	if c.memory == nil {
		return nil
	}
	var err *C.char
	if C.jfs_smartmap_memory_close(c.memory, &err) != 0 {
		return takeRustSmartmapError(err)
	}
	C.jfs_smartmap_memory_free(c.memory)
	c.memory = nil
	return nil
}

func (c *RustClient) CloseFaults() {
	if c.session != nil && c.controlsStarted {
		var err *C.char
		_ = C.jfs_smartmap_session_shutdown(c.session, &err)
		C.jfs_smartmap_string_free(err)
		select {
		case <-c.controlsDone:
		case <-time.After(5 * time.Second):
		}
	}
	if c.session != nil {
		C.jfs_smartmap_session_free(c.session)
		c.session = nil
	}
	if c.uffd != nil {
		C.jfs_smartmap_uffd_free(c.uffd)
		c.uffd = nil
	}
	if c.handle != nil {
		c.handle.Delete()
		c.handle = nil
	}
}

func (c *RustClient) ServeControls() {
	if c.handle == nil {
		return
	}
	c.controlsStarted = true
	defer close(c.controlsDone)
	callbacks := C.jfs_smartmap_callbacks{
		userdata: unsafe.Pointer(uintptr(*c.handle)),
		evict:    (C.jfs_smartmap_control_cb)(C.goSmartmapEvict),
		probe:    (C.jfs_smartmap_control_cb)(C.goSmartmapProbe),
	}
	for {
		var handled C.int
		var err *C.char
		if C.jfs_smartmap_handle_next_control(c.session, &callbacks, &handled, &err) != 0 {
			C.jfs_smartmap_string_free(err)
			return
		}
		if handled == 0 {
			return
		}
	}
}

func takeRustSmartmapError(err *C.char) error {
	msg := peekRustSmartmapError(err)
	C.jfs_smartmap_string_free(err)
	return errors.New(msg)
}

func (c *RustClient) Mapped() []byte {
	return c.mapped
}

func (c *RustClient) MemoryID() string {
	return c.memoryID
}

func (c *RustClient) SharedFD() int {
	if c.memory == nil {
		return -1
	}
	return int(C.jfs_smartmap_memory_raw_fd(c.memory))
}

func (c *RustClient) UffdFD() int {
	return c.uffdFD
}

func (c *RustClient) BaseAddr() uintptr {
	return c.base
}

func (c *RustClient) Extents() []UFFDExtent {
	if c.memory == nil {
		return nil
	}
	count := int(C.jfs_smartmap_memory_extent_count(c.memory))
	extents := make([]UFFDExtent, 0, count)
	for i := 0; i < count; i++ {
		var extent C.jfs_smartmap_extent
		if C.jfs_smartmap_memory_extent_at(c.memory, C.size_t(i), &extent) != 0 {
			continue
		}
		extents = append(extents, UFFDExtent{
			FileOffset: uint64(extent.file_offset),
			Length:     uint64(extent.length),
			ShmOffset:  uint64(extent.shm_offset),
		})
	}
	return extents
}

func peekRustSmartmapError(err *C.char) string {
	if err == nil {
		return "rust smartmap client error"
	}
	return C.GoString(err)
}

//export goSmartmapEvict
func goSmartmapEvict(userdata unsafe.Pointer, ranges *C.jfs_smartmap_range, length C.size_t) C.int {
	return goSmartmapControl(userdata, ranges, length, true)
}

//export goSmartmapProbe
func goSmartmapProbe(userdata unsafe.Pointer, ranges *C.jfs_smartmap_range, length C.size_t) C.int {
	return goSmartmapControl(userdata, ranges, length, false)
}

func goSmartmapControl(userdata unsafe.Pointer, ranges *C.jfs_smartmap_range, length C.size_t, evict bool) C.int {
	if userdata == nil {
		return -1
	}
	handler, ok := cgo.Handle(uintptr(userdata)).Value().(RustControlHandler)
	if !ok || handler == nil {
		return -1
	}
	cRanges := unsafe.Slice(ranges, int(length))
	goRanges := make([]RustControlRange, 0, len(cRanges))
	for _, r := range cRanges {
		goRanges = append(goRanges, RustControlRange{
			FileOffset: uint64(r.file_offset),
			Length:     uint64(r.length),
			ShmOffset:  uint64(r.shm_offset),
		})
	}
	var err error
	if evict {
		err = handler.Evict(goRanges)
	} else {
		err = handler.Probe(goRanges)
	}
	if err != nil {
		return -1
	}
	return 0
}
