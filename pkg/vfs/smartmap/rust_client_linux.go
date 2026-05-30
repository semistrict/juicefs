//go:build linux && cgo
// +build linux,cgo

package smartmap

/*
#cgo CFLAGS: -I${SRCDIR}/../../../sdk/rust/smartmap/include
#cgo LDFLAGS: ${SRCDIR}/../../../sdk/rust/smartmap/target/debug/libjuicefs_smartmap.a -ldl -lpthread -lm
#include <stdint.h>
#include <stdlib.h>
#include "juicefs_smartmap.h"

extern int goSmartmapRelease(void *userdata, jfs_smartmap_range *ranges, size_t len);
extern int goSmartmapProbe(void *userdata, jfs_smartmap_range *ranges, size_t len);
extern int goSmartmapWriteFault(void *userdata, jfs_smartmap_range *ranges, size_t len);
extern int goSmartmapPauseMutator(void *userdata);
extern int goSmartmapResumeMutator(void *userdata);
extern int goSmartmapPageSynced(void *userdata, size_t offset);
*/
import "C"

import (
	"errors"
	"fmt"
	"runtime/cgo"
	"time"
	"unsafe"
)

type RustControlRange struct {
	FileOffset uint64
	Length     uint64
	ShmOffset  uint64
}

type RustControlHandler interface {
	Release([]RustControlRange) error
	Probe([]RustControlRange) error
	WriteFault([]RustControlRange) error
}

type RustSyncHandler interface {
	PauseMutator() error
	ResumeMutator() error
	PageSynced(offset uint64) error
}

type RustClient struct {
	client          *C.jfs_smartmap_client
	mapping         *C.jfs_smartmap_mapping
	handle          *cgo.Handle
	mapped          []byte
	base            uintptr
	controlsStarted bool
	controlsDone    chan struct{}
}

func OpenRustClient(sock, path string, size, regionSize uint64, handler RustControlHandler) (*RustClient, error) {
	if path == "" {
		return nil, errors.New("rust smartmap client requires path")
	}
	if regionSize != 0 && regionSize != size {
		return nil, fmt.Errorf("region_size %d is unsupported; smartmap maps the full %d-byte file", regionSize, size)
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
	if C.jfs_smartmap_mapping_open(client, pathC, C.uint64_t(size), &rc.mapping, &err) != 0 {
		msg := peekRustSmartmapError(err)
		cleanupOnError()
		C.jfs_smartmap_string_free(err)
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
	if handler != nil {
		handle := cgo.NewHandle(handler)
		rc.handle = &handle
		rc.controlsDone = make(chan struct{})
	}
	return rc, nil
}

func (c *RustClient) Close() {
	c.CloseFaults()
	if c.mapping != nil {
		C.jfs_smartmap_mapping_free(c.mapping)
		c.mapping = nil
	}
	if c.client != nil {
		C.jfs_smartmap_client_free(c.client)
		c.client = nil
	}
}

func (c *RustClient) CloseFaults() {
	if c.mapping != nil && c.controlsStarted {
		var err *C.char
		_ = C.jfs_smartmap_mapping_shutdown(c.mapping, &err)
		C.jfs_smartmap_string_free(err)
		select {
		case <-c.controlsDone:
		case <-time.After(5 * time.Second):
		}
	}
	if c.handle != nil {
		c.handle.Delete()
		c.handle = nil
	}
}

func (c *RustClient) Sync(writebackPath string, handler RustSyncHandler) error {
	if c.mapping == nil {
		return errors.New("rust smartmap sync requires an active mapping")
	}
	if writebackPath == "" {
		return errors.New("rust smartmap sync requires writeback path")
	}
	pathC := C.CString(writebackPath)
	defer C.free(unsafe.Pointer(pathC))

	var handle *cgo.Handle
	if handler != nil {
		h := cgo.NewHandle(handler)
		handle = &h
		defer h.Delete()
	}
	var userdata unsafe.Pointer
	if handle != nil {
		userdata = unsafe.Pointer(uintptr(*handle))
	}
	callbacks := C.jfs_smartmap_sync_callbacks{
		userdata:    userdata,
		pause:       (C.jfs_smartmap_mutator_cb)(C.goSmartmapPauseMutator),
		resume:      (C.jfs_smartmap_mutator_cb)(C.goSmartmapResumeMutator),
		page_synced: (C.jfs_smartmap_page_synced_cb)(C.goSmartmapPageSynced),
	}
	var err *C.char
	if C.jfs_smartmap_mapping_sync(c.mapping, pathC, &callbacks, &err) != 0 {
		return takeRustSmartmapError(err)
	}
	return nil
}

func (c *RustClient) ServeControls() {
	if c.handle == nil {
		return
	}
	c.controlsStarted = true
	defer close(c.controlsDone)
	callbacks := C.jfs_smartmap_callbacks{
		userdata:    unsafe.Pointer(uintptr(*c.handle)),
		release:     (C.jfs_smartmap_control_cb)(C.goSmartmapRelease),
		probe:       (C.jfs_smartmap_control_cb)(C.goSmartmapProbe),
		write_fault: (C.jfs_smartmap_control_cb)(C.goSmartmapWriteFault),
	}
	for {
		var handled C.int
		var err *C.char
		if C.jfs_smartmap_handle_next_control(c.mapping, &callbacks, &handled, &err) != 0 {
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

func (c *RustClient) SharedFD() int {
	if c.mapping == nil {
		return -1
	}
	return int(C.jfs_smartmap_mapping_raw_fd(c.mapping))
}

func (c *RustClient) BaseAddr() uintptr {
	return c.base
}

func (c *RustClient) Extents() []UFFDExtent {
	if c.mapping == nil {
		return nil
	}
	count := int(C.jfs_smartmap_mapping_extent_count(c.mapping))
	extents := make([]UFFDExtent, 0, count)
	for i := 0; i < count; i++ {
		var extent C.jfs_smartmap_extent
		if C.jfs_smartmap_mapping_extent_at(c.mapping, C.size_t(i), &extent) != 0 {
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

//export goSmartmapRelease
func goSmartmapRelease(userdata unsafe.Pointer, ranges *C.jfs_smartmap_range, length C.size_t) C.int {
	return goSmartmapControl(userdata, ranges, length, "release")
}

//export goSmartmapProbe
func goSmartmapProbe(userdata unsafe.Pointer, ranges *C.jfs_smartmap_range, length C.size_t) C.int {
	return goSmartmapControl(userdata, ranges, length, "probe")
}

//export goSmartmapWriteFault
func goSmartmapWriteFault(userdata unsafe.Pointer, ranges *C.jfs_smartmap_range, length C.size_t) C.int {
	return goSmartmapControl(userdata, ranges, length, "write_fault")
}

func goSmartmapControl(userdata unsafe.Pointer, ranges *C.jfs_smartmap_range, length C.size_t, kind string) C.int {
	if userdata == nil {
		return 0
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
	switch kind {
	case "release":
		err = handler.Release(goRanges)
	case "probe":
		err = handler.Probe(goRanges)
	case "write_fault":
		err = handler.WriteFault(goRanges)
	default:
		return -1
	}
	if err != nil {
		return -1
	}
	return 0
}

//export goSmartmapPauseMutator
func goSmartmapPauseMutator(userdata unsafe.Pointer) C.int {
	return goSmartmapMutator(userdata, true)
}

//export goSmartmapResumeMutator
func goSmartmapResumeMutator(userdata unsafe.Pointer) C.int {
	return goSmartmapMutator(userdata, false)
}

//export goSmartmapPageSynced
func goSmartmapPageSynced(userdata unsafe.Pointer, offset C.size_t) C.int {
	if userdata == nil {
		return 0
	}
	handler, ok := cgo.Handle(uintptr(userdata)).Value().(RustSyncHandler)
	if !ok || handler == nil {
		return -1
	}
	if err := handler.PageSynced(uint64(offset)); err != nil {
		return -1
	}
	return 0
}

func goSmartmapMutator(userdata unsafe.Pointer, pause bool) C.int {
	if userdata == nil {
		return 0
	}
	handler, ok := cgo.Handle(uintptr(userdata)).Value().(RustSyncHandler)
	if !ok || handler == nil {
		return -1
	}
	var err error
	if pause {
		err = handler.PauseMutator()
	} else {
		err = handler.ResumeMutator()
	}
	if err != nil {
		return -1
	}
	return 0
}
