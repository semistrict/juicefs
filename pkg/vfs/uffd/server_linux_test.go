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

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func createTestVFS(applyMetaConfOption func(metaConfig *meta.Config), metaURI string) (*vfs.VFS, object.ObjectStorage) {
	mp := "/jfs"
	metaConf := meta.DefaultConf()
	metaConf.MountPoint = mp
	if applyMetaConfOption != nil {
		applyMetaConfOption(metaConf)
	}
	if metaURI == "" {
		metaURI = "memkv://"
	}
	m := meta.NewClient(metaURI, metaConf)
	format := &meta.Format{
		Name:        "test",
		UUID:        uuid.New().String(),
		Storage:     "mem",
		BlockSize:   4096,
		Compression: "lz4",
		DirStats:    true,
	}
	if err := m.Init(format, true); err != nil {
		log.Fatalf("setting: %s", err)
	}
	conf := &vfs.Config{
		Meta:    metaConf,
		Format:  *format,
		Version: "Juicefs",
		Chunk: &chunk.Config{
			BlockSize:   format.BlockSize * 1024,
			Compress:    format.Compression,
			MaxUpload:   2,
			MaxDownload: 200,
			BufferSize:  30 << 20,
			CacheSize:   10 << 20,
			CacheDir:    "memory",
		},
		FuseOpts: &vfs.FuseOptions{},
	}
	blob, _ := object.CreateStorage("mem", "", "", "", "")
	registry := prometheus.NewRegistry()
	registerer := prometheus.WrapRegistererWithPrefix("juicefs_",
		prometheus.WrapRegistererWith(prometheus.Labels{"mp": mp, "vol_name": format.Name}, registry))
	store := chunk.NewCachedStore(blob, *conf.Chunk, registry)
	return vfs.NewVFS(conf, m, store, registerer, registry), blob
}

func TestUFFDRegionMapping(t *testing.T) {
	pageSize := uintptr(4096)
	regions := []UFFDRegion{
		{BaseHostVirtAddr: 0x100000, Size: 2 * pageSize, Offset: 8192, PageSize: pageSize},
		{BaseHostVirtAddr: 0x200000, Size: pageSize, Offset: 65536, PageSize: pageSize},
	}
	require.NoError(t, validateUFFDRegions(regions))

	r, err := findUFFDRegion(regions, 0x101234)
	require.NoError(t, err)
	require.Equal(t, uint64(8192+0x1234), r.fileOffset(0x101234))

	r, err = findUFFDRegion(regions, 0x200100)
	require.NoError(t, err)
	require.Equal(t, uint64(65536+0x100), r.fileOffset(0x200100))

	_, err = findUFFDRegion(regions, 0x300000)
	require.Error(t, err)
}

func TestUFFDRegionValidation(t *testing.T) {
	pageSize := uintptr(4096)
	for name, regions := range map[string][]UFFDRegion{
		"empty":         nil,
		"zero base":     {{Size: pageSize, PageSize: pageSize}},
		"zero size":     {{BaseHostVirtAddr: pageSize, PageSize: pageSize}},
		"bad page size": {{BaseHostVirtAddr: pageSize, Size: pageSize, PageSize: 3000}},
		"unaligned":     {{BaseHostVirtAddr: pageSize + 1, Size: pageSize, PageSize: pageSize}},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, validateUFFDRegions(regions))
		})
	}
}

func TestClassifyUFFDCopyResult(t *testing.T) {
	require.NoError(t, classifyUFFDCopyResult(4096, 4096))
	require.ErrorIs(t, classifyUFFDCopyResult(1024, 4096), syscall.EAGAIN)
	require.ErrorIs(t, classifyUFFDCopyResult(-int64(syscall.EEXIST), 4096), syscall.EEXIST)
	require.ErrorIs(t, classifyUFFDCopyResult(-int64(syscall.ESRCH), 4096), syscall.ESRCH)
}

func TestClassifyUFFDContinueResult(t *testing.T) {
	require.NoError(t, classifyUFFDContinueResult(uffdHugePageSize, uffdHugePageSize))
	require.ErrorIs(t, classifyUFFDContinueResult(1024, uffdHugePageSize), syscall.EAGAIN)
	require.ErrorIs(t, classifyUFFDContinueResult(-int64(syscall.EEXIST), uffdHugePageSize), syscall.EEXIST)
	require.ErrorIs(t, classifyUFFDContinueResult(-int64(syscall.ESRCH), uffdHugePageSize), syscall.ESRCH)
}

func TestAppendUFFDExtentCoalescesContiguousMappings(t *testing.T) {
	extents := appendUFFDExtent(nil, UFFDExtent{FileOffset: 0, Length: uffdHugePageSize, ShmOffset: 0})
	extents = appendUFFDExtent(extents, UFFDExtent{FileOffset: uffdHugePageSize, Length: uffdHugePageSize, ShmOffset: uffdHugePageSize})
	extents = appendUFFDExtent(extents, UFFDExtent{FileOffset: 2 * uffdHugePageSize, Length: uffdHugePageSize, ShmOffset: 8 * uffdHugePageSize})

	require.Equal(t, []UFFDExtent{
		{FileOffset: 0, Length: 2 * uffdHugePageSize, ShmOffset: 0},
		{FileOffset: 2 * uffdHugePageSize, Length: uffdHugePageSize, ShmOffset: 8 * uffdHugePageSize},
	}, extents)
}

func TestUFFDControlAckIsMetadataOnly(t *testing.T) {
	payload, err := json.Marshal(uffdControlAck{
		Type:      uffdControlEvictAck,
		RequestID: 7,
		OK:        true,
	})
	require.NoError(t, err)
	require.NotContains(t, string(payload), "dirty")
	require.NotContains(t, string(payload), "data")
}

func TestUFFDRequestPayloadValidation(t *testing.T) {
	server, client := unixConnPair(t)
	defer client.Close()

	done := make(chan struct{})
	go func() {
		var v vfs.VFS
		handleUFFDConn(&v, server, make(chan struct{}), newUFFDSharedCache())
		close(done)
	}()

	devNull, err := os.Open("/dev/null")
	require.NoError(t, err)
	defer devNull.Close()

	req := UFFDRequest{
		Path: "/file",
		Regions: []UFFDRegion{{
			BaseHostVirtAddr: 0x100000,
			Size:             4096,
			Offset:           0,
			PageSize:         4096,
		}},
	}
	payload, err := json.Marshal(req)
	require.NoError(t, err)
	_, _, err = client.WriteMsgUnix(payload, syscall.UnixRights(int(devNull.Fd()), int(devNull.Fd())), nil)
	require.NoError(t, err)

	var resp uffdResponse
	require.NoError(t, json.NewDecoder(client).Decode(&resp))
	require.False(t, resp.OK)
	require.Contains(t, resp.Error, "expected exactly one uffd fd")

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("uffd request handler did not return")
	}
}

func TestReadUFFDRequestRejectsMalformedPayload(t *testing.T) {
	server, client := unixConnPair(t)
	defer server.Close()
	defer client.Close()

	_, err := client.Write([]byte("{"))
	require.NoError(t, err)

	_, fds, err := readUFFDRequest(server)
	require.Error(t, err)
	require.Empty(t, fds)
}

func TestReadUFFDRequestRejectsTruncatedControlMessage(t *testing.T) {
	server, client := unixConnPair(t)
	defer server.Close()
	defer client.Close()

	devNull, err := os.Open("/dev/null")
	require.NoError(t, err)
	defer devNull.Close()

	req := UFFDRequest{
		Path: "/file",
		Regions: []UFFDRegion{{
			BaseHostVirtAddr: 0x100000,
			Size:             4096,
			Offset:           0,
			PageSize:         4096,
		}},
	}
	payload, err := json.Marshal(req)
	require.NoError(t, err)

	rights := syscall.UnixRights(int(devNull.Fd()), int(devNull.Fd()))
	_, _, err = client.WriteMsgUnix(payload, rights, nil)
	require.NoError(t, err)

	oldFdSize := uffdFdSize
	t.Cleanup(func() { uffdFdSize = oldFdSize })
	uffdFdSize = 0

	_, fds, err := readUFFDRequest(server)
	require.Error(t, err)
	require.Contains(t, err.Error(), "control message truncated")
	require.Empty(t, fds)
}

func TestUFFDServerStopClosesIdleConnections(t *testing.T) {
	var v vfs.VFS
	sock := filepath.Join(t.TempDir(), "uffd.sock")
	stop, err := Start(&v, sock)
	require.NoError(t, err)

	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: sock, Net: "unix"})
	require.NoError(t, err)
	defer conn.Close()

	done := make(chan struct{})
	go func() {
		stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("uffd server stop did not close idle client session")
	}
}

func TestUFFDServerRefusesToRemoveNonSocketPath(t *testing.T) {
	var v vfs.VFS
	p := filepath.Join(t.TempDir(), "uffd.sock")
	require.NoError(t, os.WriteFile(p, []byte("not a socket"), 0600))

	stop, err := Start(&v, p)
	require.Error(t, err)
	require.Nil(t, stop)

	got, readErr := os.ReadFile(p)
	require.NoError(t, readErr)
	require.Equal(t, []byte("not a socket"), got)
}

func TestUFFDSharedEvictionCancelKeepsPopulatedPageUsable(t *testing.T) {
	cache := newUFFDSharedCache()
	page := &uffdSharedPage{key: "page", refs: 1, populated: true, evicting: true}
	page.cond = sync.NewCond(&cache.mu)
	cache.pages[page.key] = page

	cache.cancelEviction(page, syscall.EPIPE)

	cache.mu.Lock()
	defer cache.mu.Unlock()
	require.True(t, page.populated)
	require.False(t, page.evicting)
	require.NoError(t, page.err)
}

func TestUFFDSharedBeginSessionRejectsClosingMemory(t *testing.T) {
	cache := newUFFDSharedCache()
	memory := &uffdMemory{id: "memory-1", closing: true, active: make(map[*uffdSharedSession]struct{})}
	cache.memories[memory.id] = memory

	err := cache.beginMemorySession(memory, &uffdSharedSession{})
	require.ErrorContains(t, err, "closing")
	require.Zero(t, memory.sessions)
}

func TestUFFDDemandPagingIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real userfaultfd integration test in short mode")
	}

	v, _ := createTestVFS(nil, "")
	ctx := vfs.NewLogContext(meta.Background())
	pageSize := os.Getpagesize()
	data := deterministicUFFDData(2*pageSize + 123)
	writeVFSFile(t, v, ctx, "uffd-file", data)

	sock := filepath.Join(t.TempDir(), "uffd.sock")
	stop, err := Start(v, sock)
	require.NoError(t, err)
	defer stop()

	runAnonUFFDClient(t, sock, "/uffd-file", uint64(3*pageSize), 0, []uffdClientAction{
		{Op: "read", Offset: 0, Want: data[0]},
		{Op: "read", Offset: uint64(pageSize + 17), Want: data[pageSize+17]},
		{Op: "read", Offset: uint64(len(data) - 1), Want: data[len(data)-1]},
		{Op: "read", Offset: uint64(len(data)), Want: 0},
	})
	stop()
}

func TestUFFDDemandPagingRejectsOutsideRegion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real userfaultfd integration test in short mode")
	}
	v, _ := createTestVFS(nil, "")
	ctx := vfs.NewLogContext(meta.Background())
	pageSize := os.Getpagesize()
	writeVFSFile(t, v, ctx, "uffd-file", deterministicUFFDData(pageSize))

	sock := filepath.Join(t.TempDir(), "uffd.sock")
	stop, err := Start(v, sock)
	require.NoError(t, err)
	defer stop()

	runAnonUFFDClient(t, sock, "/uffd-file", uint64(2*pageSize), uint64(pageSize), []uffdClientAction{
		{Op: "outside_error", Offset: uint64(pageSize), ErrorContains: "outside uffd regions"},
	})
}

func TestUFFDSharedHugeTLBMemoryIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real userfaultfd hugetlb integration test in short mode")
	}
	oldGCPercent := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(oldGCPercent)

	v, _ := createTestVFS(nil, "")
	ctx := vfs.NewLogContext(meta.Background())
	const memorySize = 1024 << 20
	writeLargeVFSFile(t, v, ctx, "base-memory", memorySize)
	cloneVFSFile(t, v, ctx, "base-memory", "clone-memory")

	sock := filepath.Join(t.TempDir(), "uffd.sock")
	stop, err := Start(v, sock)
	require.NoError(t, err)
	defer stop()

	baseOpen, baseErr := openSharedMemoryFile(t, sock, "/base-memory", memorySize)
	skipUnavailableHugeTLB(t, baseErr)
	require.NoError(t, baseErr)
	defer unix.Close(baseOpen.fd)

	cloneOpen, cloneErr := openSharedMemoryFile(t, sock, "/clone-memory", memorySize)
	skipUnavailableHugeTLB(t, cloneErr)
	require.NoError(t, cloneErr)
	defer unix.Close(cloneOpen.fd)
	_, err = unix.Pwrite(cloneOpen.fd, []byte{0xff}, int64(shmOffsetForFileOffset(t, cloneOpen.extents, 0)))
	require.ErrorIs(t, err, syscall.EBADF, "clients should receive a read-only shared memory fd")
	require.Equal(t, baseOpen.extents, cloneOpen.extents, "cloned clean pages should reuse the same shared hugetlb offsets")
	require.Len(t, cloneOpen.extents, 1, "contiguous shared pages should be returned as one extent")

	runSharedUFFDClient(t, sock, cloneOpen, memorySize, 0, []uffdClientAction{
		{Op: "read", Offset: 17, Want: deterministicUFFDByte(17)},
		{Op: "read", Offset: memorySize/2 + 33, Want: deterministicUFFDByte(memorySize/2 + 33)},
		{Op: "read", Offset: memorySize - 1, Want: deterministicUFFDByte(memorySize - 1)},
		{Op: "write", Offset: 0, Value: deterministicUFFDByte(0) ^ 0x7f},
		{Op: "read", Offset: 0, Want: deterministicUFFDByte(0) ^ 0x7f},
	})

	shared := make([]byte, 1)
	_, err = unix.Pread(baseOpen.fd, shared, int64(shmOffsetForFileOffset(t, baseOpen.extents, 0)))
	require.NoError(t, err)
	require.Equal(t, deterministicUFFDByte(0), shared[0], "write-protected CoW must not alter the shared clean page")

	requireCloseSharedMemoryFile(t, sock, cloneOpen.memoryID)
	refreshed, refreshErr := openSharedMemoryFile(t, sock, "/clone-memory", memorySize)
	skipUnavailableHugeTLB(t, refreshErr)
	require.NoError(t, refreshErr)
	defer unix.Close(refreshed.fd)
	require.Equal(t, shmOffsetForFileOffset(t, cloneOpen.extents, 0), shmOffsetForFileOffset(t, refreshed.extents, 0), "private writes without FUSE writeback must not change the file backing")
	require.Equal(t, shmOffsetForFileOffset(t, cloneOpen.extents, uffdHugePageSize), shmOffsetForFileOffset(t, refreshed.extents, uffdHugePageSize), "unchanged cloned page should still share backing")
	require.Equal(t, shmOffsetForFileOffset(t, cloneOpen.extents, memorySize-uffdHugePageSize), shmOffsetForFileOffset(t, refreshed.extents, memorySize-uffdHugePageSize), "untouched last page should still share backing")

	stop()
}

func TestUFFDSharedRefreshUsesOpenedInodeAfterRename(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping hugetlb refresh identity test in short mode")
	}

	v, _ := createTestVFS(nil, "")
	ctx := vfs.NewLogContext(meta.Background())
	const memorySize = uffdHugePageSize
	writePatternVFSFile(t, v, ctx, "base-memory", memorySize, 0x52)

	cache := newUFFDSharedCache()
	defer cache.close()
	memory, err := cache.openMemoryFile(v, ctx, "/base-memory", memorySize)
	skipUnavailableHugeTLB(t, err)
	require.NoError(t, err)

	require.Equal(t, syscall.Errno(0), v.Rename(ctx, meta.RootInode, "base-memory", meta.RootInode, "renamed-memory", 0))
	session := &uffdSharedSession{v: v, ctx: ctx, cache: cache, memory: memory}
	require.NoError(t, session.refreshMemoryRanges([]uffdControlRange{{
		FileOffset: 0,
		Length:     uffdHugePageSize,
		ShmOffset:  memory.extents[0].ShmOffset,
	}}))
}

func TestUFFDSharedExtentsDedupeAcrossMultipleClones(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping hugetlb clone dedupe test in short mode")
	}

	v, _ := createTestVFS(nil, "")
	ctx := vfs.NewLogContext(meta.Background())
	const memorySize = 64 << 20
	writeLargeVFSFile(t, v, ctx, "base-memory", memorySize)
	cloneVFSFile(t, v, ctx, "base-memory", "clone-a")
	cloneVFSFile(t, v, ctx, "clone-a", "clone-b")
	cloneVFSFile(t, v, ctx, "base-memory", "clone-c")

	sock := filepath.Join(t.TempDir(), "uffd.sock")
	stop, err := Start(v, sock)
	require.NoError(t, err)
	defer stop()

	baseOpen, baseErr := openSharedMemoryFile(t, sock, "/base-memory", memorySize)
	skipUnavailableHugeTLB(t, baseErr)
	require.NoError(t, baseErr)
	defer unix.Close(baseOpen.fd)

	cloneA, err := openSharedMemoryFile(t, sock, "/clone-a", memorySize)
	skipUnavailableHugeTLB(t, err)
	require.NoError(t, err)
	defer unix.Close(cloneA.fd)
	cloneB, err := openSharedMemoryFile(t, sock, "/clone-b", memorySize)
	skipUnavailableHugeTLB(t, err)
	require.NoError(t, err)
	defer unix.Close(cloneB.fd)
	cloneC, err := openSharedMemoryFile(t, sock, "/clone-c", memorySize)
	skipUnavailableHugeTLB(t, err)
	require.NoError(t, err)
	defer unix.Close(cloneC.fd)

	require.Equal(t, baseOpen.extents, cloneA.extents)
	require.Equal(t, baseOpen.extents, cloneB.extents)
	require.Equal(t, baseOpen.extents, cloneC.extents)
	require.Len(t, baseOpen.extents, 1, "contiguous shared pages should be returned as one extent")

	dirty := deterministicUFFDData(uffdHugePageSize)
	for i := range dirty {
		dirty[i] ^= 0xa5
	}
	const dirtyOffset = 4 * uffdHugePageSize
	overwriteVFSFileRange(t, v, ctx, "clone-b", dirty, dirtyOffset)

	refreshedB, err := openSharedMemoryFile(t, sock, "/clone-b", memorySize)
	skipUnavailableHugeTLB(t, err)
	require.NoError(t, err)
	defer unix.Close(refreshedB.fd)
	refreshedA, err := openSharedMemoryFile(t, sock, "/clone-a", memorySize)
	skipUnavailableHugeTLB(t, err)
	require.NoError(t, err)
	defer unix.Close(refreshedA.fd)
	refreshedC, err := openSharedMemoryFile(t, sock, "/clone-c", memorySize)
	skipUnavailableHugeTLB(t, err)
	require.NoError(t, err)
	defer unix.Close(refreshedC.fd)

	require.Len(t, refreshedB.extents, 3, "one dirty page in the middle should split the layout into before/dirty/after extents")
	require.NotEqual(t, shmOffsetForFileOffset(t, cloneB.extents, dirtyOffset), shmOffsetForFileOffset(t, refreshedB.extents, dirtyOffset))
	for off := uint64(0); off < memorySize; off += uffdHugePageSize {
		if off == dirtyOffset {
			continue
		}
		require.Equal(t, shmOffsetForFileOffset(t, cloneB.extents, off), shmOffsetForFileOffset(t, refreshedB.extents, off), "unchanged clone-b page at %d should remain deduped", off)
	}
	require.Equal(t, baseOpen.extents, refreshedA.extents)
	require.Equal(t, baseOpen.extents, refreshedC.extents)
}

func TestUFFDSharedDelayedFaultUsesOpenedSource(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real userfaultfd hugetlb integration test in short mode")
	}
	oldGCPercent := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(oldGCPercent)

	v, _ := createTestVFS(nil, "")
	ctx := vfs.NewLogContext(meta.Background())
	const memorySize = 64 << 20
	writeLargeVFSFile(t, v, ctx, "base-memory", memorySize)

	sock := filepath.Join(t.TempDir(), "uffd.sock")
	stop, err := Start(v, sock)
	require.NoError(t, err)
	defer stop()

	opened, openErr := openSharedMemoryFile(t, sock, "/base-memory", memorySize)
	skipUnavailableHugeTLB(t, openErr)
	require.NoError(t, openErr)
	defer unix.Close(opened.fd)

	dirty := deterministicUFFDData(uffdHugePageSize)
	for i := range dirty {
		dirty[i] ^= 0x7f
	}
	overwriteVFSFileRange(t, v, ctx, "base-memory", dirty, 0)

	runSharedUFFDClient(t, sock, opened, memorySize, 0, []uffdClientAction{
		{Op: "read", Offset: 17, Want: deterministicUFFDByte(17)},
	})
	stop()
}

func TestUFFDSharedWriteFirstFaultCOW(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real userfaultfd hugetlb integration test in short mode")
	}
	oldGCPercent := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(oldGCPercent)

	v, _ := createTestVFS(nil, "")
	ctx := vfs.NewLogContext(meta.Background())
	const memorySize = 64 << 20
	writeLargeVFSFile(t, v, ctx, "base-memory", memorySize)
	cloneVFSFile(t, v, ctx, "base-memory", "clone-memory")

	sock := filepath.Join(t.TempDir(), "uffd.sock")
	stop, err := Start(v, sock)
	require.NoError(t, err)
	defer stop()

	baseOpen, baseErr := openSharedMemoryFile(t, sock, "/base-memory", memorySize)
	skipUnavailableHugeTLB(t, baseErr)
	require.NoError(t, baseErr)
	defer unix.Close(baseOpen.fd)

	opened, openErr := openSharedMemoryFile(t, sock, "/clone-memory", memorySize)
	skipUnavailableHugeTLB(t, openErr)
	require.NoError(t, openErr)
	defer unix.Close(opened.fd)
	require.Equal(t, baseOpen.extents, opened.extents)

	const privateByte = 0x6d
	runSharedUFFDClient(t, sock, opened, memorySize, 0, []uffdClientAction{
		{Op: "write", Offset: 0, Value: privateByte},
		{Op: "read", Offset: 0, Want: privateByte},
		{Op: "read", Offset: 17, Want: deterministicUFFDByte(17)},
	})

	shared := make([]byte, 1)
	_, err = unix.Pread(baseOpen.fd, shared, int64(shmOffsetForFileOffset(t, baseOpen.extents, 0)))
	require.NoError(t, err)
	require.Equal(t, deterministicUFFDByte(0), shared[0], "write-protected CoW must not mutate shared clean backing")

	requireCloseSharedMemoryFile(t, sock, opened.memoryID)
	refreshed, refreshErr := openSharedMemoryFile(t, sock, "/clone-memory", memorySize)
	skipUnavailableHugeTLB(t, refreshErr)
	require.NoError(t, refreshErr)
	defer unix.Close(refreshed.fd)
	require.Equal(t, shmOffsetForFileOffset(t, opened.extents, 0), shmOffsetForFileOffset(t, refreshed.extents, 0), "private writes without FUSE writeback must not change the file backing")
	require.Equal(t, shmOffsetForFileOffset(t, opened.extents, uffdHugePageSize), shmOffsetForFileOffset(t, refreshed.extents, uffdHugePageSize), "clean neighboring page should remain deduped")
}

func TestUFFDSharedConcurrentSamePageFaults(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real userfaultfd hugetlb integration test in short mode")
	}
	oldGCPercent := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(oldGCPercent)

	v, _ := createTestVFS(nil, "")
	ctx := vfs.NewLogContext(meta.Background())
	const memorySize = 64 << 20
	writeLargeVFSFile(t, v, ctx, "base-memory", memorySize)

	sock := filepath.Join(t.TempDir(), "uffd.sock")
	stop, err := Start(v, sock)
	require.NoError(t, err)
	defer stop()

	opened, openErr := openSharedMemoryFile(t, sock, "/base-memory", memorySize)
	skipUnavailableHugeTLB(t, openErr)
	require.NoError(t, openErr)
	defer unix.Close(opened.fd)

	const clients = 4
	releaseFile := filepath.Join(t.TempDir(), "release")
	procs := make([]*uffdClientProcess, 0, clients)
	for i := 0; i < clients; i++ {
		readyFile := filepath.Join(t.TempDir(), fmt.Sprintf("ready-%d", i))
		script := newSharedUFFDClientScript(sock, opened, memorySize, 0, []uffdClientAction{
			{Op: "mark_ready", ReadyFile: readyFile},
			{Op: "wait_file", ReadyFile: releaseFile},
			{Op: "read", Offset: 17, Want: deterministicUFFDByte(17)},
		})
		procs = append(procs, startUFFDClientProcess(t, script, opened.fd, uffdClientTimeout(script)))
		require.Eventually(t, func() bool {
			_, err := os.Stat(readyFile)
			return err == nil
		}, 5*time.Second, 10*time.Millisecond)
	}
	require.NoError(t, os.WriteFile(releaseFile, []byte("go\n"), 0600))
	for _, proc := range procs {
		requireUFFDClientResult(t, proc.wait(t))
	}

	shared := make([]byte, 1)
	_, err = unix.Pread(opened.fd, shared, int64(shmOffsetForFileOffset(t, opened.extents, 0)+17))
	require.NoError(t, err)
	require.Equal(t, deterministicUFFDByte(17), shared[0])
}

func TestUFFDSharedTwoClientsShareClonedFile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real userfaultfd hugetlb integration test in short mode")
	}
	oldGCPercent := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(oldGCPercent)

	v, _ := createTestVFS(nil, "")
	ctx := vfs.NewLogContext(meta.Background())
	const memorySize = 64 << 20
	writeLargeVFSFile(t, v, ctx, "base-memory", memorySize)
	cloneVFSFile(t, v, ctx, "base-memory", "clone-memory")

	sock := filepath.Join(t.TempDir(), "uffd.sock")
	stop, err := Start(v, sock)
	require.NoError(t, err)
	defer stop()

	firstOpen, firstErr := openSharedMemoryFile(t, sock, "/clone-memory", memorySize)
	skipUnavailableHugeTLB(t, firstErr)
	require.NoError(t, firstErr)
	defer unix.Close(firstOpen.fd)
	secondOpen, secondErr := openSharedMemoryFile(t, sock, "/clone-memory", memorySize)
	skipUnavailableHugeTLB(t, secondErr)
	require.NoError(t, secondErr)
	defer unix.Close(secondOpen.fd)
	require.Equal(t, firstOpen.extents, secondOpen.extents)

	const privateByte = 0x41
	runSharedUFFDClient(t, sock, firstOpen, memorySize, 0, []uffdClientAction{
		{Op: "read", Offset: 17, Want: deterministicUFFDByte(17)},
		{Op: "write", Offset: 0, Value: privateByte},
		{Op: "read", Offset: 0, Want: privateByte},
	})
	runSharedUFFDClient(t, sock, secondOpen, memorySize, 0, []uffdClientAction{
		{Op: "read", Offset: 0, Want: deterministicUFFDByte(0)},
		{Op: "read", Offset: memorySize/2 + 9, Want: deterministicUFFDByte(memorySize/2 + 9)},
	})

	shared := make([]byte, 1)
	_, err = unix.Pread(firstOpen.fd, shared, int64(shmOffsetForFileOffset(t, firstOpen.extents, 0)))
	require.NoError(t, err)
	require.Equal(t, deterministicUFFDByte(0), shared[0])
}

func TestUFFDSharedReleasedMemoryReusesSlotWithFreshData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real userfaultfd hugetlb integration test in short mode")
	}
	oldGCPercent := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(oldGCPercent)

	v, _ := createTestVFS(nil, "")
	ctx := vfs.NewLogContext(meta.Background())
	const memorySize = uffdHugePageSize
	writePatternVFSFile(t, v, ctx, "old-memory", memorySize, 0x11)
	writePatternVFSFile(t, v, ctx, "new-memory", memorySize, 0x82)

	sock := filepath.Join(t.TempDir(), "uffd.sock")
	stop, err := Start(v, sock)
	require.NoError(t, err)
	defer stop()

	oldOpen, openErr := openSharedMemoryFile(t, sock, "/old-memory", memorySize)
	skipUnavailableHugeTLB(t, openErr)
	require.NoError(t, openErr)
	defer unix.Close(oldOpen.fd)

	runSharedUFFDClient(t, sock, oldOpen, memorySize, 0, []uffdClientAction{
		{Op: "read", Offset: 0, Want: patternedUFFDByte(0x11, 0)},
		{Op: "read", Offset: 12345, Want: patternedUFFDByte(0x11, 12345)},
	})
	requireCloseSharedMemoryFile(t, sock, oldOpen.memoryID)

	newOpen, openErr := openSharedMemoryFile(t, sock, "/new-memory", memorySize)
	skipUnavailableHugeTLB(t, openErr)
	require.NoError(t, openErr)
	defer unix.Close(newOpen.fd)
	require.Equal(t, shmOffsetForFileOffset(t, oldOpen.extents, 0), shmOffsetForFileOffset(t, newOpen.extents, 0), "released slot should be reused instead of growing the shared file")

	runSharedUFFDClient(t, sock, newOpen, memorySize, 0, []uffdClientAction{
		{Op: "read", Offset: 0, Want: patternedUFFDByte(0x82, 0)},
		{Op: "read", Offset: 12345, Want: patternedUFFDByte(0x82, 12345)},
	})
}

func TestUFFDSharedCloseMemoryKeepsCloneReference(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real userfaultfd hugetlb integration test in short mode")
	}

	v, _ := createTestVFS(nil, "")
	ctx := vfs.NewLogContext(meta.Background())
	const memorySize = uffdHugePageSize
	writePatternVFSFile(t, v, ctx, "base-memory", memorySize, 0x22)
	cloneVFSFile(t, v, ctx, "base-memory", "clone-memory")
	writePatternVFSFile(t, v, ctx, "other-memory", memorySize, 0x44)
	writePatternVFSFile(t, v, ctx, "after-memory", memorySize, 0x66)

	sock := filepath.Join(t.TempDir(), "uffd.sock")
	stop, err := Start(v, sock)
	require.NoError(t, err)
	defer stop()

	baseOpen, baseErr := openSharedMemoryFile(t, sock, "/base-memory", memorySize)
	skipUnavailableHugeTLB(t, baseErr)
	require.NoError(t, baseErr)
	defer unix.Close(baseOpen.fd)
	cloneOpen, cloneErr := openSharedMemoryFile(t, sock, "/clone-memory", memorySize)
	skipUnavailableHugeTLB(t, cloneErr)
	require.NoError(t, cloneErr)
	defer unix.Close(cloneOpen.fd)
	require.Equal(t, baseOpen.extents, cloneOpen.extents)

	baseOffset := shmOffsetForFileOffset(t, baseOpen.extents, 0)
	require.NoError(t, closeSharedMemoryFile(t, sock, baseOpen.memoryID))

	otherOpen, otherErr := openSharedMemoryFile(t, sock, "/other-memory", memorySize)
	skipUnavailableHugeTLB(t, otherErr)
	require.NoError(t, otherErr)
	defer unix.Close(otherOpen.fd)
	require.NotEqual(t, baseOffset, shmOffsetForFileOffset(t, otherOpen.extents, 0), "closing one clone reference must not free a page still used by another open memory")

	require.NoError(t, closeSharedMemoryFile(t, sock, cloneOpen.memoryID))
	afterOpen, afterErr := openSharedMemoryFile(t, sock, "/after-memory", memorySize)
	skipUnavailableHugeTLB(t, afterErr)
	require.NoError(t, afterErr)
	defer unix.Close(afterOpen.fd)
	require.Equal(t, baseOffset, shmOffsetForFileOffset(t, afterOpen.extents, 0), "slot should become reusable after the last clone reference is closed")
}

func TestUFFDSharedCloseMemoryRejectsActiveSession(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real userfaultfd hugetlb integration test in short mode")
	}

	v, _ := createTestVFS(nil, "")
	ctx := vfs.NewLogContext(meta.Background())
	const memorySize = uffdHugePageSize
	writePatternVFSFile(t, v, ctx, "base-memory", memorySize, 0x33)

	sock := filepath.Join(t.TempDir(), "uffd.sock")
	stop, err := Start(v, sock)
	require.NoError(t, err)
	defer stop()

	opened, openErr := openSharedMemoryFile(t, sock, "/base-memory", memorySize)
	skipUnavailableHugeTLB(t, openErr)
	require.NoError(t, openErr)
	defer unix.Close(opened.fd)

	readyFile := filepath.Join(t.TempDir(), "ready")
	releaseFile := filepath.Join(t.TempDir(), "release")
	script := newSharedUFFDClientScript(sock, opened, memorySize, 0, []uffdClientAction{
		{Op: "read", Offset: 0, Want: patternedUFFDByte(0x33, 0)},
		{Op: "mark_ready", ReadyFile: readyFile},
		{Op: "wait_file", ReadyFile: releaseFile},
		{Op: "read", Offset: 12345, Want: patternedUFFDByte(0x33, 12345)},
	})
	proc := startUFFDClientProcess(t, script, opened.fd, uffdClientTimeout(script))
	require.Eventually(t, func() bool {
		_, err := os.Stat(readyFile)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)

	closeErr := closeSharedMemoryFile(t, sock, opened.memoryID)
	require.Error(t, closeErr)
	require.Contains(t, closeErr.Error(), "active")
	require.NoError(t, os.WriteFile(releaseFile, []byte("go\n"), 0600))
	requireUFFDClientResult(t, proc.wait(t))

	requireCloseSharedMemoryFile(t, sock, opened.memoryID)
}

func TestUFFDSharedResidentBudgetEvictsAndRefaultsCleanPages(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real userfaultfd hugetlb integration test in short mode")
	}
	oldGCPercent := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(oldGCPercent)

	v, _ := createTestVFS(nil, "")
	ctx := vfs.NewLogContext(meta.Background())
	const memorySize = 4 * uffdHugePageSize
	writeLargeVFSFile(t, v, ctx, "base-memory", memorySize)

	sock := filepath.Join(t.TempDir(), "uffd.sock")
	stop, err := Start(v, sock)
	require.NoError(t, err)
	defer stop()

	opened, openErr := openSharedMemoryFile(t, sock, "/base-memory", memorySize)
	skipUnavailableHugeTLB(t, openErr)
	require.NoError(t, openErr)
	defer unix.Close(opened.fd)

	runSharedUFFDClient(t, sock, opened, memorySize, 1, []uffdClientAction{
		{Op: "read", Offset: 17, Want: deterministicUFFDByte(17)},
		{Op: "read", Offset: uffdHugePageSize + 23, Want: deterministicUFFDByte(uffdHugePageSize + 23)},
		{Op: "expect_eviction", Offset: 0},
		{Op: "read", Offset: 2*uffdHugePageSize + 31, Want: deterministicUFFDByte(2*uffdHugePageSize + 31)},
		{Op: "expect_eviction", Offset: uffdHugePageSize},
		{Op: "read", Offset: 17, Want: deterministicUFFDByte(17)},
		{Op: "expect_eviction", Offset: 2 * uffdHugePageSize},
		{Op: "assert_base_stable"},
	})
}

func TestUFFDSharedProbeEvictionComparesAgainstFaultOnlyLRU(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real userfaultfd hugetlb integration test in short mode")
	}
	oldGCPercent := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(oldGCPercent)

	const memorySize = 5 * uffdHugePageSize
	t.Run("fault-only-lru-evicts-mapped-hot-page", func(t *testing.T) {
		v, _ := createTestVFS(nil, "")
		ctx := vfs.NewLogContext(meta.Background())
		writeLargeVFSFile(t, v, ctx, "base-memory", memorySize)

		sock := filepath.Join(t.TempDir(), "uffd.sock")
		stop, err := Start(v, sock)
		require.NoError(t, err)
		defer stop()

		opened, openErr := openSharedMemoryFile(t, sock, "/base-memory", memorySize)
		skipUnavailableHugeTLB(t, openErr)
		require.NoError(t, openErr)
		defer unix.Close(opened.fd)

		runSharedUFFDClient(t, sock, opened, memorySize, 4, []uffdClientAction{
			{Op: "read", Offset: 0, Want: deterministicUFFDByte(0)},
			{Op: "read", Offset: uffdHugePageSize, Want: deterministicUFFDByte(uffdHugePageSize)},
			{Op: "read", Offset: 2 * uffdHugePageSize, Want: deterministicUFFDByte(2 * uffdHugePageSize)},
			{Op: "read", Offset: 0, Want: deterministicUFFDByte(0)},
			{Op: "read", Offset: 3 * uffdHugePageSize, Want: deterministicUFFDByte(3 * uffdHugePageSize)},
			{Op: "read", Offset: 4 * uffdHugePageSize, Want: deterministicUFFDByte(4 * uffdHugePageSize)},
			{Op: "expect_eviction", Offset: 0},
		})
	})

	t.Run("probe-eviction-keeps-refaulted-hot-page", func(t *testing.T) {
		v, _ := createTestVFS(nil, "")
		ctx := vfs.NewLogContext(meta.Background())
		writeLargeVFSFile(t, v, ctx, "base-memory", memorySize)

		sock := filepath.Join(t.TempDir(), "uffd.sock")
		stop, err := Start(v, sock)
		require.NoError(t, err)
		defer stop()

		opened, openErr := openSharedMemoryFile(t, sock, "/base-memory", memorySize)
		skipUnavailableHugeTLB(t, openErr)
		require.NoError(t, openErr)
		defer unix.Close(opened.fd)

		script := newSharedUFFDClientScript(sock, opened, memorySize, 4, []uffdClientAction{
			{Op: "read", Offset: 0, Want: deterministicUFFDByte(0)},
			{Op: "read", Offset: uffdHugePageSize, Want: deterministicUFFDByte(uffdHugePageSize)},
			{Op: "read", Offset: 2 * uffdHugePageSize, Want: deterministicUFFDByte(2 * uffdHugePageSize)},
			{Op: "expect_probe", Offset: 0},
			{Op: "read", Offset: 0, Want: deterministicUFFDByte(0)},
			{Op: "sleep", Milliseconds: 80},
			{Op: "read", Offset: 3 * uffdHugePageSize, Want: deterministicUFFDByte(3 * uffdHugePageSize)},
			{Op: "read", Offset: 4 * uffdHugePageSize, Want: deterministicUFFDByte(4 * uffdHugePageSize)},
			{Op: "expect_eviction", Offset: uffdHugePageSize},
		})
		script.EvictionPolicy = uffdEvictionPolicyProbe
		script.ProbeCandidates = 2
		script.ProbeEvict = 1
		script.ProbeMonitorMS = 40
		runUFFDClientProcess(t, script, opened.fd)
	})
}

func TestUFFDSharedRefaultProtectionKeepsQuickRefaultsByDefault(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real userfaultfd hugetlb integration test in short mode")
	}
	oldGCPercent := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(oldGCPercent)

	v, _ := createTestVFS(nil, "")
	ctx := vfs.NewLogContext(meta.Background())
	const memorySize = 9 * uffdHugePageSize
	writeLargeVFSFile(t, v, ctx, "base-memory", memorySize)

	sock := filepath.Join(t.TempDir(), "uffd.sock")
	stop, err := Start(v, sock)
	require.NoError(t, err)
	defer stop()

	opened, openErr := openSharedMemoryFile(t, sock, "/base-memory", memorySize)
	skipUnavailableHugeTLB(t, openErr)
	require.NoError(t, openErr)
	defer unix.Close(opened.fd)

	runSharedUFFDClient(t, sock, opened, memorySize, 4, []uffdClientAction{
		{Op: "read", Offset: 0, Want: deterministicUFFDByte(0)},
		{Op: "read", Offset: uffdHugePageSize, Want: deterministicUFFDByte(uffdHugePageSize)},
		{Op: "read", Offset: 2 * uffdHugePageSize, Want: deterministicUFFDByte(2 * uffdHugePageSize)},
		{Op: "read", Offset: 3 * uffdHugePageSize, Want: deterministicUFFDByte(3 * uffdHugePageSize)},
		{Op: "read", Offset: 4 * uffdHugePageSize, Want: deterministicUFFDByte(4 * uffdHugePageSize)},
		{Op: "expect_eviction", Offset: 0},
		{Op: "read", Offset: 0, Want: deterministicUFFDByte(0)},
		{Op: "expect_eviction", Offset: uffdHugePageSize},
		{Op: "read", Offset: 5 * uffdHugePageSize, Want: deterministicUFFDByte(5 * uffdHugePageSize)},
		{Op: "expect_eviction", Offset: 2 * uffdHugePageSize},
		{Op: "read", Offset: 6 * uffdHugePageSize, Want: deterministicUFFDByte(6 * uffdHugePageSize)},
		{Op: "expect_eviction", Offset: 3 * uffdHugePageSize},
		{Op: "read", Offset: 7 * uffdHugePageSize, Want: deterministicUFFDByte(7 * uffdHugePageSize)},
		{Op: "expect_eviction", Offset: 4 * uffdHugePageSize},
		{Op: "read", Offset: 8 * uffdHugePageSize, Want: deterministicUFFDByte(8 * uffdHugePageSize)},
		{Op: "expect_eviction", Offset: 5 * uffdHugePageSize},
	})
}

func BenchmarkUFFDSharedOpenMemoryFile1GiB(b *testing.B) {
	v, _ := createTestVFS(nil, "")
	ctx := vfs.NewLogContext(meta.Background())
	const memorySize = 1024 << 20
	writeLargeVFSFile(b, v, ctx, "base-memory", memorySize)
	cache := newUFFDSharedCache()
	defer cache.close()

	if _, err := cache.openMemoryFile(v, ctx, "/base-memory", memorySize); err != nil {
		skipUnavailableHugeTLB(b, err)
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(memorySize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		memory, err := cache.openMemoryFile(v, ctx, "/base-memory", memorySize)
		if err != nil {
			b.Fatal(err)
		}
		if len(memory.extents) != 1 {
			b.Fatalf("got %d extents", len(memory.extents))
		}
	}
}

func BenchmarkUFFDSharedFirstFault2MiB(b *testing.B) {
	oldGCPercent := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(oldGCPercent)

	v, _ := createTestVFS(nil, "")
	ctx := vfs.NewLogContext(meta.Background())
	const memorySize = 64 << 20
	writeLargeVFSFile(b, v, ctx, "base-memory", memorySize)

	b.StopTimer()
	for i := 0; i < b.N; i++ {
		func() {
			sock := filepath.Join(b.TempDir(), "uffd.sock")
			stop, err := Start(v, sock)
			if err != nil {
				b.Fatal(err)
			}
			defer stop()

			opened, err := openSharedMemoryFile(b, sock, "/base-memory", memorySize)
			skipUnavailableHugeTLB(b, err)
			if err != nil {
				b.Fatal(err)
			}
			defer unix.Close(opened.fd)

			mapped, cleanupMapping := mapSharedMemoryExtents(b, opened.fd, memorySize, opened.extents)
			defer cleanupMapping()
			uffdFD, closeUFFD := registerHugeTLBUFFDRange(b, mapped)
			defer closeUFFD()

			serveConn := sendUFFDRequest(b, sock, uffdFD, UFFDRequest{
				Op:       uffdOpServeMemoryFault,
				MemoryID: opened.memoryID,
				Mappings: []UFFDRegion{{
					BaseHostVirtAddr: uintptr(unsafe.Pointer(&mapped[0])),
					Size:             uintptr(len(mapped)),
					Offset:           0,
					PageSize:         uffdHugePageSize,
				}},
			})
			defer serveConn.Close()

			b.StartTimer()
			if got := readMappedByte(b, mapped, 17); got != deterministicUFFDByte(17) {
				b.Fatalf("got byte %d", got)
			}
			b.StopTimer()
		}()
	}
	b.ReportAllocs()
	b.SetBytes(uffdHugePageSize)
}

func unixConnPair(t *testing.T) (*net.UnixConn, *net.UnixConn) {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	require.NoError(t, err)

	leftFile := os.NewFile(uintptr(fds[0]), "uffd-test-left")
	rightFile := os.NewFile(uintptr(fds[1]), "uffd-test-right")
	defer leftFile.Close()
	defer rightFile.Close()

	left, err := net.FileConn(leftFile)
	require.NoError(t, err)
	right, err := net.FileConn(rightFile)
	require.NoError(t, err)

	return left.(*net.UnixConn), right.(*net.UnixConn)
}

func writeVFSFile(t testing.TB, v *vfs.VFS, ctx vfs.LogContext, name string, data []byte) {
	t.Helper()
	entry, fh, st := v.Create(ctx, 1, name, 0644, 0, syscall.O_RDWR)
	require.Equal(t, syscall.Errno(0), st)
	require.Equal(t, syscall.Errno(0), v.Write(ctx, entry.Inode, data, 0, fh))
	require.Equal(t, syscall.Errno(0), v.Flush(ctx, entry.Inode, fh, 0))
	v.Release(ctx, entry.Inode, fh)
}

func writeLargeVFSFile(t testing.TB, v *vfs.VFS, ctx vfs.LogContext, name string, size uint64) {
	t.Helper()
	entry, fh, st := v.Create(ctx, 1, name, 0644, 0, syscall.O_RDWR)
	require.Equal(t, syscall.Errno(0), st)
	page := make([]byte, uffdHugePageSize)
	for off := uint64(0); off < size; off += uint64(len(page)) {
		for i := range page {
			page[i] = deterministicUFFDByte(int(off) + i)
		}
		require.Equal(t, syscall.Errno(0), v.Write(ctx, entry.Inode, page, off, fh))
	}
	require.Equal(t, syscall.Errno(0), v.Flush(ctx, entry.Inode, fh, 0))
	v.Release(ctx, entry.Inode, fh)
}

func writePatternVFSFile(t testing.TB, v *vfs.VFS, ctx vfs.LogContext, name string, size uint64, seed byte) {
	t.Helper()
	entry, fh, st := v.Create(ctx, 1, name, 0644, 0, syscall.O_RDWR)
	require.Equal(t, syscall.Errno(0), st)
	page := make([]byte, uffdHugePageSize)
	for off := uint64(0); off < size; off += uint64(len(page)) {
		for i := range page {
			page[i] = patternedUFFDByte(seed, int(off)+i)
		}
		require.Equal(t, syscall.Errno(0), v.Write(ctx, entry.Inode, page, off, fh))
	}
	require.Equal(t, syscall.Errno(0), v.Flush(ctx, entry.Inode, fh, 0))
	v.Release(ctx, entry.Inode, fh)
}

func overwriteVFSFileRange(t testing.TB, v *vfs.VFS, ctx vfs.LogContext, name string, data []byte, off uint64) {
	t.Helper()
	var ino vfs.Ino
	attr := &vfs.Attr{}
	require.Equal(t, syscall.Errno(0), v.Meta.Resolve(ctx, meta.RootInode, "/"+name, &ino, attr, true))
	entry, fh, st := v.Open(ctx, ino, syscall.O_RDWR)
	require.Equal(t, syscall.Errno(0), st)
	require.Equal(t, syscall.Errno(0), v.Write(ctx, entry.Inode, data, off, fh))
	require.Equal(t, syscall.Errno(0), v.Flush(ctx, entry.Inode, fh, 0))
	v.Release(ctx, entry.Inode, fh)
}

func cloneVFSFile(t testing.TB, v *vfs.VFS, ctx vfs.LogContext, srcName, dstName string) {
	t.Helper()
	var srcIno vfs.Ino
	attr := &vfs.Attr{}
	require.Equal(t, syscall.Errno(0), v.Meta.Resolve(ctx, meta.RootInode, "/"+srcName, &srcIno, attr, true))
	var count, total uint64
	require.Equal(t, syscall.Errno(0), v.Meta.Clone(ctx, meta.RootInode, srcIno, meta.RootInode, dstName, 0, 022, 4, &count, &total))
}

func deterministicUFFDData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = deterministicUFFDByte(i)
	}
	return data
}

func deterministicUFFDByte(i int) byte {
	return byte((i*31 + 7) % 251)
}

func patternedUFFDByte(seed byte, i int) byte {
	return byte((i*31 + int(seed)*17 + 7) % 251)
}

const (
	uffdClientHelperEnv      = "JUICEFS_UFFD_CLIENT_HELPER"
	uffdClientSharedMemoryFD = 3
	pagemapEntrySize         = 8
	pagemapPresentBit        = uint64(1) << 63
	pagemapUFFDWPBit         = uint64(1) << 57
)

type uffdClientScript struct {
	Kind              string             `json:"kind"`
	Sock              string             `json:"sock"`
	Path              string             `json:"path"`
	Size              uint64             `json:"size"`
	RegionSize        uint64             `json:"region_size,omitempty"`
	MemoryID          string             `json:"memory_id,omitempty"`
	SharedMemoryFD    int                `json:"shared_memory_fd,omitempty"`
	WritebackPath     string             `json:"writeback_path,omitempty"`
	Extents           []UFFDExtent       `json:"extents,omitempty"`
	MaxResidentPages  int                `json:"max_resident_pages,omitempty"`
	EvictionPolicy    string             `json:"eviction_policy,omitempty"`
	ProbeCandidates   int                `json:"probe_candidates,omitempty"`
	ProbeEvict        int                `json:"probe_evict,omitempty"`
	ProbeMonitorMS    int                `json:"probe_monitor_ms,omitempty"`
	Actions           []uffdClientAction `json:"actions,omitempty"`
	TimeoutMultiplier int                `json:"timeout_multiplier,omitempty"`
}

type uffdClientAction struct {
	Op            string `json:"op"`
	Offset        uint64 `json:"offset,omitempty"`
	Value         byte   `json:"value,omitempty"`
	Want          byte   `json:"want,omitempty"`
	ErrorContains string `json:"error_contains,omitempty"`
	ReadyFile     string `json:"ready_file,omitempty"`
	Milliseconds  int    `json:"milliseconds,omitempty"`
}

type uffdClientResult struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
	Skip  string `json:"skip,omitempty"`
}

type uffdClientActionState struct {
	conn       *net.UnixConn
	closeUFFD  func() error
	evictions  <-chan uffdControlMessage
	probes     <-chan uffdControlMessage
	uffdFD     int
	baseAddr   uintptr
	writeback  string
	mappedName string
}

type uffdClientProcess struct {
	cmd    *exec.Cmd
	ctx    context.Context
	cancel context.CancelFunc
	stdout bytes.Buffer
	stderr bytes.Buffer
	done   chan error
}

func TestUFFDClientHelperProcess(t *testing.T) {
	if os.Getenv(uffdClientHelperEnv) != "1" {
		return
	}
	var script uffdClientScript
	if err := json.NewDecoder(os.Stdin).Decode(&script); err != nil {
		_ = json.NewEncoder(os.Stdout).Encode(uffdClientResult{Error: fmt.Sprintf("decode helper script: %s", err)})
		return
	}
	_ = json.NewEncoder(os.Stdout).Encode(runUFFDClientScript(script))
}

func runUFFDClientScript(script uffdClientScript) (result uffdClientResult) {
	defer func() {
		if r := recover(); r != nil {
			result = uffdClientResult{Error: fmt.Sprintf("helper panic: %v", r)}
		}
	}()
	switch script.Kind {
	case "anon":
		return runAnonUFFDClientScript(script)
	case "shared":
		return runSharedUFFDClientScript(script)
	default:
		return uffdClientResult{Error: fmt.Sprintf("unknown helper kind %q", script.Kind)}
	}
}

func runAnonUFFDClientScript(script uffdClientScript) uffdClientResult {
	mapped, uffdFD, closeUFFD, cleanup, skip, err := registerUFFDRangeClient(int(script.Size))
	if skip != "" {
		return uffdClientResult{Skip: skip}
	}
	if err != nil {
		return uffdClientResult{Error: err.Error()}
	}
	defer cleanup()

	regionSize := script.RegionSize
	if regionSize == 0 {
		regionSize = script.Size
	}
	conn, err := sendUFFDRequestClient(script.Sock, uffdFD, UFFDRequest{
		Path: script.Path,
		Regions: []UFFDRegion{{
			BaseHostVirtAddr: uintptr(unsafe.Pointer(&mapped[0])),
			Size:             uintptr(regionSize),
			Offset:           0,
			PageSize:         uintptr(os.Getpagesize()),
		}},
	})
	if err != nil {
		return uffdClientResult{Error: err.Error()}
	}
	defer conn.Close()

	state := uffdClientActionState{conn: conn, closeUFFD: closeUFFD, baseAddr: uintptr(unsafe.Pointer(&mapped[0])), mappedName: "anonymous uffd range"}
	if err := runUFFDClientActions(mapped, script.Actions, state); err != nil {
		return uffdClientResult{Error: err.Error()}
	}
	return uffdClientResult{OK: true}
}

func runSharedUFFDClientScript(script uffdClientScript) uffdClientResult {
	if script.SharedMemoryFD <= 0 {
		return uffdClientResult{Error: "missing shared_memory_fd"}
	}
	defer unix.Close(script.SharedMemoryFD)

	mapped, cleanupMapping, skip, err := mapSharedMemoryExtentsClient(script.SharedMemoryFD, script.Size, script.Extents)
	if skip != "" {
		return uffdClientResult{Skip: skip}
	}
	if err != nil {
		return uffdClientResult{Error: err.Error()}
	}
	defer cleanupMapping()

	uffdFD, closeUFFD, skip, err := registerHugeTLBUFFDRangeClient(mapped)
	if skip != "" {
		return uffdClientResult{Skip: skip}
	}
	if err != nil {
		return uffdClientResult{Error: err.Error()}
	}
	defer closeUFFD()

	conn, err := sendUFFDRequestClient(script.Sock, uffdFD, UFFDRequest{
		Op:                  uffdOpServeMemoryFault,
		MemoryID:            script.MemoryID,
		MaxResidentPages:    script.MaxResidentPages,
		EvictionPolicy:      script.EvictionPolicy,
		ProbeCandidatePages: script.ProbeCandidates,
		ProbeEvictPages:     script.ProbeEvict,
		ProbeMonitorMillis:  script.ProbeMonitorMS,
		Mappings: []UFFDRegion{{
			BaseHostVirtAddr: uintptr(unsafe.Pointer(&mapped[0])),
			Size:             uintptr(len(mapped)),
			Offset:           0,
			PageSize:         uffdHugePageSize,
		}},
	})
	if err != nil {
		return uffdClientResult{Error: err.Error()}
	}
	defer conn.Close()

	session := &cooperativeSharedFaultSession{
		mapped:    mapped,
		conn:      conn,
		uffdFD:    uffdFD,
		baseAddr:  uintptr(unsafe.Pointer(&mapped[0])),
		writeback: script.WritebackPath,
		evictions: make(chan uffdControlMessage, 16),
		probes:    make(chan uffdControlMessage, 16),
	}
	go session.serveControls()

	state := uffdClientActionState{
		conn:       conn,
		closeUFFD:  closeUFFD,
		evictions:  session.evictions,
		probes:     session.probes,
		uffdFD:     uffdFD,
		baseAddr:   uintptr(unsafe.Pointer(&mapped[0])),
		writeback:  script.WritebackPath,
		mappedName: "shared memory range",
	}
	if err := runUFFDClientActions(mapped, script.Actions, state); err != nil {
		return uffdClientResult{Error: err.Error()}
	}
	return uffdClientResult{OK: true}
}

func runUFFDClientActions(mapped []byte, actions []uffdClientAction, state uffdClientActionState) error {
	for _, action := range actions {
		switch action.Op {
		case "read":
			idx, err := checkedMappedIndex(mapped, action.Offset)
			if err != nil {
				return err
			}
			if got := mapped[idx]; got != action.Want {
				return fmt.Errorf("read %s byte at %d: got %d want %d", state.mappedName, action.Offset, got, action.Want)
			}
		case "write":
			idx, err := checkedMappedIndex(mapped, action.Offset)
			if err != nil {
				return err
			}
			mapped[idx] = action.Value
		case "outside_error":
			if err := runOutsideRegionFaultAction(mapped, action, state); err != nil {
				return err
			}
		case "expect_eviction":
			if err := expectClientEviction(action, state.evictions); err != nil {
				return err
			}
		case "expect_probe":
			if err := expectClientProbe(action, state.probes); err != nil {
				return err
			}
		case "flush_dirty":
			if err := flushDirtyPrivatePages(mapped, state, 0, uint64(len(mapped)), false); err != nil {
				return err
			}
		case "assert_base_stable":
			if got := uintptr(unsafe.Pointer(&mapped[0])); got != state.baseAddr {
				return fmt.Errorf("client virtual mapping moved: got %#x want %#x", got, state.baseAddr)
			}
		case "mark_ready":
			if action.ReadyFile == "" {
				return errors.New("mark_ready action requires ready_file")
			}
			if err := os.WriteFile(action.ReadyFile, []byte("ready\n"), 0600); err != nil {
				return fmt.Errorf("mark helper ready: %w", err)
			}
		case "wait_file":
			if action.ReadyFile == "" {
				return errors.New("wait_file action requires ready_file")
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				if _, err := os.Stat(action.ReadyFile); err == nil {
					break
				} else if !errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("wait for helper file %s: %w", action.ReadyFile, err)
				}
				if time.Now().After(deadline) {
					return fmt.Errorf("timed out waiting for helper file %s", action.ReadyFile)
				}
				time.Sleep(10 * time.Millisecond)
			}
		case "sleep":
			time.Sleep(time.Duration(action.Milliseconds) * time.Millisecond)
		default:
			return fmt.Errorf("unknown helper action %q", action.Op)
		}
	}
	return nil
}

func checkedMappedIndex(mapped []byte, off uint64) (int, error) {
	if off >= uint64(len(mapped)) {
		return 0, fmt.Errorf("mapped offset %d is outside range length %d", off, len(mapped))
	}
	return int(off), nil
}

func runOutsideRegionFaultAction(mapped []byte, action uffdClientAction, state uffdClientActionState) error {
	if state.conn == nil || state.closeUFFD == nil {
		return errors.New("outside_error action requires a uffd connection")
	}
	idx, err := checkedMappedIndex(mapped, action.Offset)
	if err != nil {
		return err
	}
	touchDone := make(chan byte, 1)
	go func() {
		touchDone <- mapped[idx]
	}()
	if err := state.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	var resp uffdResponse
	if err := json.NewDecoder(state.conn).Decode(&resp); err != nil {
		return fmt.Errorf("read outside-region response: %w", err)
	}
	if resp.OK {
		return errors.New("outside-region fault unexpectedly succeeded")
	}
	if action.ErrorContains != "" && !bytes.Contains([]byte(resp.Error), []byte(action.ErrorContains)) {
		return fmt.Errorf("outside-region error %q does not contain %q", resp.Error, action.ErrorContains)
	}
	if err := state.closeUFFD(); err != nil {
		return fmt.Errorf("close uffd after outside-region fault: %w", err)
	}
	select {
	case <-touchDone:
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("outside-region fault did not unblock after closing userfaultfd")
	}
}

func expectClientEviction(action uffdClientAction, evictions <-chan uffdControlMessage) error {
	if evictions == nil {
		return errors.New("expect_eviction action requires cooperative control handling")
	}
	select {
	case msg := <-evictions:
		if !controlMessageContainsOffset(msg, action.Offset) {
			return fmt.Errorf("eviction ranges %#v do not include file offset %d", msg.Ranges, action.Offset)
		}
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timed out waiting for eviction at file offset %d", action.Offset)
	}
}

func expectClientProbe(action uffdClientAction, probes <-chan uffdControlMessage) error {
	if probes == nil {
		return errors.New("expect_probe action requires cooperative control handling")
	}
	select {
	case msg := <-probes:
		if !controlMessageContainsOffset(msg, action.Offset) {
			return fmt.Errorf("probe ranges %#v do not include file offset %d", msg.Ranges, action.Offset)
		}
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timed out waiting for probe at file offset %d", action.Offset)
	}
}

func controlMessageContainsOffset(msg uffdControlMessage, off uint64) bool {
	for _, r := range msg.Ranges {
		if r.Length == uint64(uffdHugePageSize) && r.FileOffset == off {
			return true
		}
	}
	return false
}

func flushDirtyPrivatePages(mapped []byte, state uffdClientActionState, start, end uint64, drop bool) error {
	if state.uffdFD <= 0 {
		return errors.New("dirty writeback requires a uffd fd")
	}
	if start%uffdHugePageSize != 0 || end%uffdHugePageSize != 0 || end > uint64(len(mapped)) || start > end {
		return fmt.Errorf("invalid dirty writeback range [%d, %d) for mapping length %d", start, end, len(mapped))
	}
	pagemap, err := os.Open("/proc/self/pagemap")
	if err != nil {
		return fmt.Errorf("open /proc/self/pagemap: %w", err)
	}
	defer pagemap.Close()

	var file *os.File
	defer func() {
		if file != nil {
			_ = file.Close()
		}
	}()
	for off := start; off < end; off += uffdHugePageSize {
		dirty, err := isPrivatePageDirty(pagemap, state.baseAddr+uintptr(off))
		if err != nil {
			return err
		}
		if !dirty {
			continue
		}
		if state.writeback == "" {
			return errors.New("dirty writeback path is empty")
		}
		if file == nil {
			file, err = os.OpenFile(state.writeback, os.O_RDWR, 0)
			if err != nil {
				return fmt.Errorf("open dirty writeback file %s: %w", state.writeback, err)
			}
		}
		page := mapped[int(off):int(off+uffdHugePageSize)]
		if _, err := file.WriteAt(page, int64(off)); err != nil {
			return fmt.Errorf("write dirty page at %d: %w", off, err)
		}
		if err := uffdWriteProtect(state.uffdFD, state.baseAddr+uintptr(off), uintptr(uffdHugePageSize), true); err != nil {
			return fmt.Errorf("re-protect dirty page at %d: %w", off, err)
		}
	}
	if file != nil {
		if err := file.Sync(); err != nil {
			return fmt.Errorf("sync dirty writeback file %s: %w", state.writeback, err)
		}
	}
	if drop && end > start {
		if err := unix.Madvise(mapped[int(start):int(end)], unix.MADV_DONTNEED); err != nil {
			return fmt.Errorf("madvise dropped written range [%d, %d): %w", start, end, err)
		}
	}
	return nil
}

func isPrivatePageDirty(pagemap *os.File, addr uintptr) (bool, error) {
	var buf [pagemapEntrySize]byte
	vpn := uint64(addr) / uint64(os.Getpagesize())
	if _, err := pagemap.ReadAt(buf[:], int64(vpn*pagemapEntrySize)); err != nil {
		return false, fmt.Errorf("read pagemap at %#x: %w", addr, err)
	}
	entry := binary.LittleEndian.Uint64(buf[:])
	if entry&pagemapPresentBit == 0 {
		return false, nil
	}
	return entry&pagemapUFFDWPBit == 0, nil
}

func runAnonUFFDClient(t testing.TB, sock, filePath string, mappingSize, regionSize uint64, actions []uffdClientAction) {
	t.Helper()
	runUFFDClientProcess(t, uffdClientScript{
		Kind:       "anon",
		Sock:       sock,
		Path:       filePath,
		Size:       mappingSize,
		RegionSize: regionSize,
		Actions:    actions,
	}, -1)
}

func runSharedUFFDClient(t testing.TB, sock string, opened sharedMemoryOpen, size uint64, maxResidentPages int, actions []uffdClientAction) {
	t.Helper()
	runUFFDClientProcess(t, newSharedUFFDClientScript(sock, opened, size, maxResidentPages, actions), opened.fd)
}

func newSharedUFFDClientScript(sock string, opened sharedMemoryOpen, size uint64, maxResidentPages int, actions []uffdClientAction) uffdClientScript {
	return uffdClientScript{
		Kind:             "shared",
		Sock:             sock,
		Size:             size,
		MemoryID:         opened.memoryID,
		Extents:          opened.extents,
		MaxResidentPages: maxResidentPages,
		Actions:          actions,
	}
}

func runUFFDClientProcess(t testing.TB, script uffdClientScript, sharedFD int) uffdClientResult {
	t.Helper()
	proc := startUFFDClientProcess(t, script, sharedFD, uffdClientTimeout(script))
	result := proc.wait(t)
	requireUFFDClientResult(t, result)
	return result
}

func startUFFDClientProcess(t testing.TB, script uffdClientScript, sharedFD int, timeout time.Duration) *uffdClientProcess {
	t.Helper()
	if sharedFD >= 0 {
		dup, err := unix.Dup(sharedFD)
		require.NoError(t, err)
		sharedFile := os.NewFile(uintptr(dup), "uffd-shared-memory")
		defer sharedFile.Close()
		script.SharedMemoryFD = uffdClientSharedMemoryFD
		return startUFFDClientProcessWithFiles(t, script, timeout, sharedFile)
	}
	return startUFFDClientProcessWithFiles(t, script, timeout)
}

func startUFFDClientProcessWithFiles(t testing.TB, script uffdClientScript, timeout time.Duration, extraFiles ...*os.File) *uffdClientProcess {
	t.Helper()
	payload, err := json.Marshal(script)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestUFFDClientHelperProcess$")
	cmd.Env = append(os.Environ(), uffdClientHelperEnv+"=1")
	cmd.Stdin = bytes.NewReader(payload)
	cmd.ExtraFiles = extraFiles

	proc := &uffdClientProcess{cmd: cmd, ctx: ctx, cancel: cancel, done: make(chan error, 1)}
	cmd.Stdout = &proc.stdout
	cmd.Stderr = &proc.stderr
	require.NoError(t, cmd.Start())
	go func() {
		proc.done <- cmd.Wait()
	}()
	return proc
}

func (p *uffdClientProcess) wait(t testing.TB) uffdClientResult {
	t.Helper()
	defer p.cancel()

	err := <-p.done
	if errors.Is(p.ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("uffd client helper timed out\nstdout:\n%s\nstderr:\n%s", p.stdout.String(), p.stderr.String())
	}
	if err != nil {
		t.Fatalf("uffd client helper failed: %s\nstdout:\n%s\nstderr:\n%s", err, p.stdout.String(), p.stderr.String())
	}
	var result uffdClientResult
	if decErr := json.NewDecoder(bytes.NewReader(p.stdout.Bytes())).Decode(&result); decErr != nil {
		t.Fatalf("decode uffd client helper result: %s\nstdout:\n%s\nstderr:\n%s", decErr, p.stdout.String(), p.stderr.String())
	}
	return result
}

func requireUFFDClientResult(t testing.TB, result uffdClientResult) {
	t.Helper()
	if result.Skip != "" {
		t.Skip(result.Skip)
	}
	require.True(t, result.OK, "uffd client helper error: %s", result.Error)
}

func uffdClientTimeout(script uffdClientScript) time.Duration {
	timeout := 45 * time.Second
	if script.Size >= 1024<<20 {
		timeout = 2 * time.Minute
	}
	if script.TimeoutMultiplier > 1 {
		timeout *= time.Duration(script.TimeoutMultiplier)
	}
	return timeout
}

func registerUFFDRangeClient(size int) ([]byte, int, func() error, func(), string, error) {
	mapped, err := unix.Mmap(-1, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS)
	if err != nil {
		return nil, -1, nil, nil, "", err
	}

	fd, _, errno := syscall.Syscall(uintptr(nrUserfaultfd), uintptr(unix.O_CLOEXEC|unix.O_NONBLOCK), 0, 0)
	if errno != 0 {
		_ = unix.Munmap(mapped)
		return nil, -1, nil, nil, uffdUnavailableReason(errno), fmt.Errorf("userfaultfd syscall: %w", errno)
	}

	api := newUFFDIOAPI(uint64(uffdAPI), 0)
	if _, _, errno = syscall.Syscall(syscall.SYS_IOCTL, fd, uintptr(uffdioAPIIoctl), uintptr(unsafe.Pointer(&api))); errno != 0 {
		_ = unix.Close(int(fd))
		_ = unix.Munmap(mapped)
		return nil, -1, nil, nil, uffdUnavailableReason(errno), fmt.Errorf("UFFDIO_API: %w", errno)
	}

	reg := newUFFDIORegister(uintptr(unsafe.Pointer(&mapped[0])), uintptr(len(mapped)), uint64(uffdioRegisterModeMissing))
	if _, _, errno = syscall.Syscall(syscall.SYS_IOCTL, fd, uintptr(uffdioRegisterIoctl), uintptr(unsafe.Pointer(&reg))); errno != 0 {
		_ = unix.Close(int(fd))
		_ = unix.Munmap(mapped)
		return nil, -1, nil, nil, uffdUnavailableReason(errno), fmt.Errorf("UFFDIO_REGISTER: %w", errno)
	}

	var once sync.Once
	var fdOnce sync.Once
	var closeErr error
	closeFD := func() error {
		fdOnce.Do(func() {
			closeErr = unix.Close(int(fd))
		})
		return closeErr
	}
	cleanup := func() {
		once.Do(func() {
			_ = closeFD()
			_ = unix.Munmap(mapped)
		})
	}
	return mapped, int(fd), closeFD, cleanup, "", nil
}

func mapSharedMemoryExtentsClient(fd int, size uint64, extents []UFFDExtent) ([]byte, func(), string, error) {
	reserve, err := unix.Mmap(-1, 0, int(size+uffdHugePageSize), unix.PROT_NONE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS)
	if err != nil {
		return nil, nil, hugeTLBUnavailableReason(err), err
	}
	reserveBase := uintptr(unsafe.Pointer(&reserve[0]))
	base := (reserveBase + uffdHugePageSize - 1) & ^(uintptr(uffdHugePageSize) - 1)
	for _, extent := range extents {
		if extent.FileOffset%uffdHugePageSize != 0 || extent.Length%uffdHugePageSize != 0 || extent.ShmOffset%uffdHugePageSize != 0 {
			_ = unix.Munmap(reserve)
			return nil, nil, "", fmt.Errorf("unaligned shared memory extent: %#v", extent)
		}
		addr := base + uintptr(extent.FileOffset)
		_, _, errno := syscall.Syscall6(syscall.SYS_MMAP, addr, uintptr(extent.Length), uintptr(unix.PROT_READ|unix.PROT_WRITE), uintptr(unix.MAP_PRIVATE|unix.MAP_FIXED), uintptr(fd), uintptr(extent.ShmOffset))
		if errno != 0 {
			_ = unix.Munmap(reserve)
			return nil, nil, hugeTLBUnavailableReason(errno), fmt.Errorf("mmap hugetlb shared extent: %w", errno)
		}
	}
	mapped := unsafe.Slice((*byte)(unsafe.Pointer(base)), int(size))
	cleanup := func() {
		_ = unix.Munmap(reserve)
	}
	return mapped, cleanup, "", nil
}

func registerHugeTLBUFFDRangeClient(mapped []byte) (int, func() error, string, error) {
	fd, _, errno := syscall.Syscall(uintptr(nrUserfaultfd), uintptr(unix.O_CLOEXEC|unix.O_NONBLOCK), 0, 0)
	if errno != 0 {
		return -1, nil, uffdUnavailableReason(errno), fmt.Errorf("userfaultfd syscall: %w", errno)
	}
	api := newUFFDIOAPI(uint64(uffdAPI), uint64(uffdFeatureMissingHugetlbfs|uffdFeatureMinorHugetlbfs|uffdFeatureWPAsync))
	if _, _, errno = syscall.Syscall(syscall.SYS_IOCTL, fd, uintptr(uffdioAPIIoctl), uintptr(unsafe.Pointer(&api))); errno != 0 {
		_ = unix.Close(int(fd))
		return -1, nil, uffdUnavailableReason(errno), fmt.Errorf("UFFDIO_API hugetlb: %w", errno)
	}
	reg := newUFFDIORegister(uintptr(unsafe.Pointer(&mapped[0])), uintptr(len(mapped)), uint64(uffdioRegisterModeMissing|uffdioRegisterModeMinor|uffdioRegisterModeWP))
	if _, _, errno = syscall.Syscall(syscall.SYS_IOCTL, fd, uintptr(uffdioRegisterIoctl), uintptr(unsafe.Pointer(&reg))); errno != 0 {
		_ = unix.Close(int(fd))
		return -1, nil, uffdUnavailableReason(errno), fmt.Errorf("UFFDIO_REGISTER hugetlb: %w", errno)
	}
	var once sync.Once
	var closeErr error
	closeFD := func() error {
		once.Do(func() {
			closeErr = unix.Close(int(fd))
		})
		return closeErr
	}
	return int(fd), closeFD, "", nil
}

func sendUFFDRequestClient(sock string, fd int, req UFFDRequest) (*net.UnixConn, error) {
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: sock, Net: "unix"})
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(req)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if _, _, err = conn.WriteMsgUnix(payload, syscall.UnixRights(fd), nil); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

type sharedMemoryOpen struct {
	fd       int
	memoryID string
	extents  []UFFDExtent
}

type cooperativeSharedFaultSession struct {
	mapped    []byte
	conn      *net.UnixConn
	uffdFD    int
	baseAddr  uintptr
	writeback string
	evictions chan uffdControlMessage
	probes    chan uffdControlMessage
}

func (s *cooperativeSharedFaultSession) serveControls() {
	dec := json.NewDecoder(s.conn)
	enc := json.NewEncoder(s.conn)
	for {
		var msg uffdControlMessage
		if err := dec.Decode(&msg); err != nil {
			return
		}
		if msg.Type == "" {
			return
		}
		var ack uffdControlAck
		switch msg.Type {
		case uffdControlEvict:
			ack = s.handleEvictControl(msg)
		case uffdControlProbe:
			ack = s.handleProbeControl(msg)
		default:
			ack = uffdControlAck{Type: msg.Type + "_ack", RequestID: msg.RequestID, Error: "unexpected control message"}
		}
		if encodeErr := enc.Encode(ack); encodeErr != nil {
			return
		}
		if ack.OK && msg.Type == uffdControlEvict {
			select {
			case s.evictions <- msg:
			default:
			}
		}
		if ack.OK && msg.Type == uffdControlProbe {
			select {
			case s.probes <- msg:
			default:
			}
		}
	}
}

func (s *cooperativeSharedFaultSession) handleEvictControl(msg uffdControlMessage) uffdControlAck {
	ack := uffdControlAck{Type: uffdControlEvictAck, RequestID: msg.RequestID, OK: true}
	state := uffdClientActionState{
		uffdFD:    s.uffdFD,
		baseAddr:  s.baseAddr,
		writeback: s.writeback,
	}
	if err := flushCooperativeEvictionRanges(s.mapped, state, msg.Ranges); err != nil {
		ack.OK = false
		ack.Error = err.Error()
	}
	return ack
}

func (s *cooperativeSharedFaultSession) handleProbeControl(msg uffdControlMessage) uffdControlAck {
	ack := uffdControlAck{Type: uffdControlProbeAck, RequestID: msg.RequestID, OK: true}
	state := uffdClientActionState{
		uffdFD:    s.uffdFD,
		baseAddr:  s.baseAddr,
		writeback: s.writeback,
	}
	if err := dropCooperativeRanges(s.mapped, state, msg.Ranges); err != nil {
		ack.OK = false
		ack.Error = err.Error()
	}
	return ack
}

func flushCooperativeEvictionRanges(mapped []byte, state uffdClientActionState, ranges []uffdControlRange) error {
	return dropCooperativeRanges(mapped, state, ranges)
}

func dropCooperativeRanges(mapped []byte, state uffdClientActionState, ranges []uffdControlRange) error {
	for _, r := range ranges {
		if r.Length != uffdHugePageSize {
			return fmt.Errorf("control range length must be %d, got %d", uffdHugePageSize, r.Length)
		}
		end := r.FileOffset + r.Length
		if end > uint64(len(mapped)) {
			return fmt.Errorf("control range [%d, %d) exceeds mapping length %d", r.FileOffset, end, len(mapped))
		}
		if err := flushDirtyPrivatePages(mapped, state, r.FileOffset, end, true); err != nil {
			return err
		}
	}
	return nil
}

func shmOffsetForFileOffset(t testing.TB, extents []UFFDExtent, off uint64) uint64 {
	t.Helper()
	for _, extent := range extents {
		if off >= extent.FileOffset && off < extent.FileOffset+extent.Length {
			return extent.ShmOffset + off - extent.FileOffset
		}
	}
	t.Fatalf("file offset %d is outside extents %#v", off, extents)
	return 0
}

func openSharedMemoryFile(t testing.TB, sock, filePath string, size uint64) (sharedMemoryOpen, error) {
	t.Helper()
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: sock, Net: "unix"})
	if err != nil {
		return sharedMemoryOpen{}, err
	}
	defer conn.Close()

	payload, err := json.Marshal(UFFDRequest{Op: uffdOpOpenMemoryFile, Path: filePath, Size: size})
	if err != nil {
		return sharedMemoryOpen{}, err
	}
	if _, _, err = conn.WriteMsgUnix(payload, nil, nil); err != nil {
		return sharedMemoryOpen{}, err
	}

	msg := make([]byte, uffdSocketPayloadSize)
	oob := make([]byte, syscall.CmsgSpace(uffdFdSize))
	n, oobn, flags, _, err := conn.ReadMsgUnix(msg, oob)
	if err != nil {
		return sharedMemoryOpen{}, err
	}
	if flags&unix.MSG_CTRUNC != 0 || flags&unix.MSG_TRUNC != 0 {
		return sharedMemoryOpen{}, errors.New("truncated shared memory response")
	}
	var resp uffdResponse
	if err := json.Unmarshal(msg[:n], &resp); err != nil {
		return sharedMemoryOpen{}, err
	}
	if !resp.OK {
		return sharedMemoryOpen{}, errors.New(resp.Error)
	}
	msgs, err := syscall.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return sharedMemoryOpen{}, err
	}
	var sharedMemoryFD = -1
	for i := range msgs {
		rights, err := syscall.ParseUnixRights(&msgs[i])
		if err != nil {
			return sharedMemoryOpen{}, err
		}
		if len(rights) != 1 {
			for _, fd := range rights {
				_ = unix.Close(fd)
			}
			return sharedMemoryOpen{}, errors.New("expected exactly one shared_memory_fd")
		}
		if sharedMemoryFD >= 0 {
			_ = unix.Close(rights[0])
			return sharedMemoryOpen{}, errors.New("received duplicate shared_memory_fd")
		}
		sharedMemoryFD = rights[0]
	}
	if sharedMemoryFD < 0 {
		return sharedMemoryOpen{}, errors.New("missing shared_memory_fd")
	}
	return sharedMemoryOpen{fd: sharedMemoryFD, memoryID: resp.MemoryID, extents: resp.Extents}, nil
}

func closeSharedMemoryFile(t testing.TB, sock, memoryID string) error {
	t.Helper()
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: sock, Net: "unix"})
	if err != nil {
		return err
	}
	defer conn.Close()

	payload, err := json.Marshal(UFFDRequest{Op: uffdOpCloseMemoryFile, MemoryID: memoryID})
	if err != nil {
		return err
	}
	if _, _, err = conn.WriteMsgUnix(payload, nil, nil); err != nil {
		return err
	}
	var resp uffdResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}
	return nil
}

func requireCloseSharedMemoryFile(t testing.TB, sock, memoryID string) {
	t.Helper()
	var err error
	require.Eventually(t, func() bool {
		err = closeSharedMemoryFile(t, sock, memoryID)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond, "last close_memory_file error: %v", err)
}

func mapSharedMemoryExtents(t testing.TB, fd int, size uint64, extents []UFFDExtent) ([]byte, func()) {
	t.Helper()
	reserve, err := unix.Mmap(-1, 0, int(size+uffdHugePageSize), unix.PROT_NONE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS)
	require.NoError(t, err)
	reserveBase := uintptr(unsafe.Pointer(&reserve[0]))
	base := (reserveBase + uffdHugePageSize - 1) & ^(uintptr(uffdHugePageSize) - 1)
	for _, extent := range extents {
		require.Equal(t, uint64(0), extent.FileOffset%uffdHugePageSize)
		require.Equal(t, uint64(0), extent.Length%uffdHugePageSize)
		require.Equal(t, uint64(0), extent.ShmOffset%uffdHugePageSize)
		addr := base + uintptr(extent.FileOffset)
		_, _, errno := syscall.Syscall6(syscall.SYS_MMAP, addr, uintptr(extent.Length), uintptr(unix.PROT_READ|unix.PROT_WRITE), uintptr(unix.MAP_PRIVATE|unix.MAP_FIXED), uintptr(fd), uintptr(extent.ShmOffset))
		if errno != 0 {
			_ = unix.Munmap(reserve)
			skipUnavailableHugeTLB(t, errno)
			t.Fatalf("mmap hugetlb shared extent: %s", errno)
		}
	}
	mapped := unsafe.Slice((*byte)(unsafe.Pointer(base)), int(size))
	cleanup := func() {
		_ = unix.Munmap(reserve)
	}
	return mapped, cleanup
}

func registerHugeTLBUFFDRange(t testing.TB, mapped []byte) (int, func() error) {
	t.Helper()
	ensureUFFDTestSchedulerRoom(8)
	fd, _, errno := syscall.Syscall(uintptr(nrUserfaultfd), uintptr(unix.O_CLOEXEC|unix.O_NONBLOCK), 0, 0)
	if errno != 0 {
		skipUnavailableUFFD(t, errno)
		t.Fatalf("userfaultfd syscall: %s", errno)
	}
	api := newUFFDIOAPI(uint64(uffdAPI), uint64(uffdFeatureMissingHugetlbfs|uffdFeatureMinorHugetlbfs|uffdFeatureWPAsync))
	if _, _, errno = syscall.Syscall(syscall.SYS_IOCTL, fd, uintptr(uffdioAPIIoctl), uintptr(unsafe.Pointer(&api))); errno != 0 {
		_ = unix.Close(int(fd))
		skipUnavailableUFFD(t, errno)
		t.Fatalf("UFFDIO_API hugetlb: %s", errno)
	}
	reg := newUFFDIORegister(uintptr(unsafe.Pointer(&mapped[0])), uintptr(len(mapped)), uint64(uffdioRegisterModeMissing|uffdioRegisterModeMinor|uffdioRegisterModeWP))
	if _, _, errno = syscall.Syscall(syscall.SYS_IOCTL, fd, uintptr(uffdioRegisterIoctl), uintptr(unsafe.Pointer(&reg))); errno != 0 {
		_ = unix.Close(int(fd))
		skipUnavailableUFFD(t, errno)
		t.Fatalf("UFFDIO_REGISTER hugetlb: %s", errno)
	}
	var once sync.Once
	var closeErr error
	closeFD := func() error {
		once.Do(func() {
			closeErr = unix.Close(int(fd))
		})
		return closeErr
	}
	return int(fd), closeFD
}

func ensureUFFDTestSchedulerRoom(minProcs int) {
	if runtime.GOMAXPROCS(0) < minProcs {
		runtime.GOMAXPROCS(minProcs)
	}
}

func uffdUnavailableReason(errno syscall.Errno) string {
	if errors.Is(errno, syscall.ENOSYS) || errors.Is(errno, syscall.EPERM) || errors.Is(errno, syscall.EACCES) || errors.Is(errno, syscall.EINVAL) {
		return fmt.Sprintf("userfaultfd unavailable under this kernel policy or feature set: %s", errno)
	}
	return ""
}

func skipUnavailableUFFD(t testing.TB, errno syscall.Errno) {
	t.Helper()
	if reason := uffdUnavailableReason(errno); reason != "" {
		t.Skip(reason)
	}
}

func hugeTLBUnavailableReason(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, syscall.ENOMEM) || errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EINVAL) {
		return fmt.Sprintf("hugetlb shared memory unavailable: %s", err)
	}
	if bytes.Contains([]byte(err.Error()), []byte("hugetlb")) ||
		bytes.Contains([]byte(err.Error()), []byte("cannot allocate memory")) ||
		bytes.Contains([]byte(err.Error()), []byte("invalid argument")) ||
		bytes.Contains([]byte(err.Error()), []byte("operation not permitted")) {
		return fmt.Sprintf("hugetlb shared memory unavailable: %s", err)
	}
	return ""
}

func skipUnavailableHugeTLB(t testing.TB, err error) {
	t.Helper()
	if reason := hugeTLBUnavailableReason(err); reason != "" {
		t.Skip(reason)
	}
}

func sendUFFDRequest(t testing.TB, sock string, fd int, req UFFDRequest) *net.UnixConn {
	t.Helper()
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: sock, Net: "unix"})
	require.NoError(t, err)

	payload, err := json.Marshal(req)
	require.NoError(t, err)
	_, _, err = conn.WriteMsgUnix(payload, syscall.UnixRights(fd), nil)
	require.NoError(t, err)
	return conn
}

func readMappedByte(t testing.TB, mapped []byte, idx int) byte {
	t.Helper()
	ch := make(chan byte, 1)
	go func() {
		ch <- mapped[idx]
	}()
	select {
	case b := <-ch:
		return b
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out reading mapped byte at %d", idx)
		return 0
	}
}

func TestUFFDEvictionProbeWorkloadAvoidsMappedHotSet(t *testing.T) {
	cfg := uffdEvictionVMHotSetConfig()
	legacy := runUFFDFaultOnlyLRUWorkload(cfg)
	refault := runUFFDRefaultFeedbackWorkload(cfg)
	probed := runUFFDProbeEvictionWorkload(cfg)

	require.Greater(t, legacy.HotEvictions, 0)
	require.Greater(t, legacy.HotEvictions, refault.HotEvictions)
	require.Greater(t, legacy.DiskLoads, refault.DiskLoads)
	require.Greater(t, refault.RefaultProtections, 0)
	require.Zero(t, probed.HotEvictions)
	require.GreaterOrEqual(t, refault.DiskLoads, probed.DiskLoads)
	require.Greater(t, legacy.SimulatedLoadTime, refault.SimulatedLoadTime)
	t.Logf("legacy LRU: disk_loads=%d load_time=%s hot_evictions=%d evictions=%d resident=%d", legacy.DiskLoads, legacy.SimulatedLoadTime, legacy.HotEvictions, legacy.Evictions, legacy.ResidentPages)
	t.Logf("refault feedback: disk_loads=%d load_time=%s refault_protections=%d hot_evictions=%d evictions=%d resident=%d", refault.DiskLoads, refault.SimulatedLoadTime, refault.RefaultProtections, refault.HotEvictions, refault.Evictions, refault.ResidentPages)
	t.Logf("refault+probe: disk_loads=%d load_time=%s probe_faults=%d refault_protections=%d hot_evictions=%d evictions=%d resident=%d", probed.DiskLoads, probed.SimulatedLoadTime, probed.ProbeFaults, probed.RefaultProtections, probed.HotEvictions, probed.Evictions, probed.ResidentPages)
}

func BenchmarkUFFDEvictionPoliciesVMHotSet(b *testing.B) {
	cfg := uffdEvictionVMHotSetConfig()
	b.Run("fault-only-lru", func(b *testing.B) {
		var stats uffdEvictionWorkloadStats
		for i := 0; i < b.N; i++ {
			stats = runUFFDFaultOnlyLRUWorkload(cfg)
		}
		reportUFFDEvictionWorkloadStats(b, stats)
	})
	b.Run("refault-feedback", func(b *testing.B) {
		var stats uffdEvictionWorkloadStats
		for i := 0; i < b.N; i++ {
			stats = runUFFDRefaultFeedbackWorkload(cfg)
		}
		reportUFFDEvictionWorkloadStats(b, stats)
	})
	b.Run("refault+probe", func(b *testing.B) {
		var stats uffdEvictionWorkloadStats
		for i := 0; i < b.N; i++ {
			stats = runUFFDProbeEvictionWorkload(cfg)
		}
		reportUFFDEvictionWorkloadStats(b, stats)
	})
}

func uffdEvictionVMHotSetConfig() uffdEvictionWorkloadConfig {
	return uffdEvictionWorkloadConfig{
		ResidentLimit:   128,
		HotPages:        32,
		InitialCold:     96,
		StreamCold:      512,
		ProbeCandidates: 64,
		ProbeEvict:      32,
		LoadLatency:     500 * time.Microsecond,
	}
}

func reportUFFDEvictionWorkloadStats(b *testing.B, stats uffdEvictionWorkloadStats) {
	b.Helper()
	b.ReportMetric(float64(stats.DiskLoads), "disk_loads/op")
	b.ReportMetric(float64(stats.ProbeFaults), "probe_faults/op")
	b.ReportMetric(float64(stats.RefaultProtections), "refault_protections/op")
	b.ReportMetric(float64(stats.HotEvictions), "hot_evictions/op")
	b.ReportMetric(float64(stats.Evictions), "evictions/op")
	b.ReportMetric(float64(stats.SimulatedLoadTime)/float64(time.Millisecond), "sim_load_ms/op")
}

type uffdEvictionWorkloadConfig struct {
	ResidentLimit   int
	HotPages        int
	InitialCold     int
	StreamCold      int
	ProbeCandidates int
	ProbeEvict      int
	LoadLatency     time.Duration
}

type uffdEvictionWorkloadStats struct {
	DiskLoads          int
	ProbeFaults        int
	RefaultProtections int
	Evictions          int
	HotEvictions       int
	ResidentPages      int
	SimulatedLoadTime  time.Duration
}

type uffdEvictionPolicySim struct {
	cfg            uffdEvictionWorkloadConfig
	refaultEnabled bool
	clock          uint64
	resident       map[int]uint64
	evictedAt      map[int]uint64
	protectedUntil map[int]uint64
	stats          uffdEvictionWorkloadStats
}

func runUFFDFaultOnlyLRUWorkload(cfg uffdEvictionWorkloadConfig) uffdEvictionWorkloadStats {
	sim := newUFFDEvictionPolicySim(cfg, false)
	runUFFDEvictionWorkload(cfg, sim.accessFaultOnly)
	return sim.finish()
}

func runUFFDRefaultFeedbackWorkload(cfg uffdEvictionWorkloadConfig) uffdEvictionWorkloadStats {
	sim := newUFFDEvictionPolicySim(cfg, true)
	runUFFDEvictionWorkload(cfg, sim.accessWithRefault)
	return sim.finish()
}

func runUFFDProbeEvictionWorkload(cfg uffdEvictionWorkloadConfig) uffdEvictionWorkloadStats {
	sim := newUFFDEvictionPolicySim(cfg, true)
	runUFFDEvictionWorkload(cfg, sim.accessWithProbe)
	return sim.finish()
}

func newUFFDEvictionPolicySim(cfg uffdEvictionWorkloadConfig, refaultEnabled bool) *uffdEvictionPolicySim {
	return &uffdEvictionPolicySim{
		cfg:            cfg,
		refaultEnabled: refaultEnabled,
		resident:       make(map[int]uint64, cfg.ResidentLimit),
		evictedAt:      make(map[int]uint64, cfg.ResidentLimit),
		protectedUntil: make(map[int]uint64, cfg.ResidentLimit),
	}
}

func runUFFDEvictionWorkload(cfg uffdEvictionWorkloadConfig, access func(int)) {
	const coldBase = 1 << 20
	for page := 0; page < cfg.HotPages; page++ {
		access(page)
	}
	for page := 0; page < cfg.InitialCold; page++ {
		access(coldBase + page)
	}
	for page := 0; page < cfg.StreamCold; page++ {
		for hot := 0; hot < cfg.HotPages; hot++ {
			access(hot)
		}
		access(coldBase + cfg.InitialCold + page)
	}
}

func (s *uffdEvictionPolicySim) accessFaultOnly(page int) {
	if _, ok := s.resident[page]; ok {
		return
	}
	s.ensureFaultOnlyCapacity()
	s.loadPage(page)
}

func (s *uffdEvictionPolicySim) accessWithRefault(page int) {
	if _, ok := s.resident[page]; ok {
		return
	}
	s.ensureRefaultCapacity()
	s.loadPage(page)
}

func (s *uffdEvictionPolicySim) accessWithProbe(page int) {
	if _, ok := s.resident[page]; ok {
		return
	}
	s.ensureProbedCapacity()
	s.loadPage(page)
}

func (s *uffdEvictionPolicySim) ensureFaultOnlyCapacity() {
	for len(s.resident) >= s.cfg.ResidentLimit {
		s.evictPage(s.oldestPage())
	}
}

func (s *uffdEvictionPolicySim) ensureRefaultCapacity() {
	for len(s.resident) >= s.cfg.ResidentLimit {
		s.evictPage(s.oldestEvictablePage())
	}
}

func (s *uffdEvictionPolicySim) ensureProbedCapacity() {
	for len(s.resident) >= s.cfg.ResidentLimit {
		candidates := s.oldestPages(s.cfg.ProbeCandidates)
		for _, page := range candidates {
			if s.hot(page) {
				s.stats.ProbeFaults++
				s.touch(page)
			}
		}
		evicted := 0
		for _, page := range candidates {
			if evicted >= s.cfg.ProbeEvict {
				break
			}
			if !s.hot(page) {
				s.evictPage(page)
				evicted++
			}
		}
		if evicted == 0 {
			s.evictPage(s.oldestEvictablePage())
		}
	}
}

func (s *uffdEvictionPolicySim) loadPage(page int) {
	s.stats.DiskLoads++
	s.stats.SimulatedLoadTime += s.cfg.LoadLatency
	if s.quickRefault(page) {
		protectUntil := s.clock + s.refaultProtectFor()
		if protectUntil > s.protectedUntil[page] {
			s.protectedUntil[page] = protectUntil
		}
		s.stats.RefaultProtections++
	}
	delete(s.evictedAt, page)
	s.touch(page)
}

func (s *uffdEvictionPolicySim) touch(page int) {
	s.clock++
	s.resident[page] = s.clock
}

func (s *uffdEvictionPolicySim) evictPage(page int) {
	if page < 0 {
		return
	}
	delete(s.resident, page)
	s.evictedAt[page] = s.clock
	delete(s.protectedUntil, page)
	s.stats.Evictions++
	if s.hot(page) {
		s.stats.HotEvictions++
	}
}

func (s *uffdEvictionPolicySim) quickRefault(page int) bool {
	if !s.refaultEnabled {
		return false
	}
	evictedAt, ok := s.evictedAt[page]
	return ok && s.clock >= evictedAt && s.clock-evictedAt <= s.refaultWindow()
}

func (s *uffdEvictionPolicySim) refaultWindow() uint64 {
	if s.cfg.ResidentLimit <= 0 {
		return 1
	}
	return uint64(s.cfg.ResidentLimit)
}

func (s *uffdEvictionPolicySim) refaultProtectFor() uint64 {
	if s.cfg.ResidentLimit <= 0 {
		return 1
	}
	return uint64(s.cfg.ResidentLimit * uffdRefaultProtectionMultiplier)
}

func (s *uffdEvictionPolicySim) protected(page int) bool {
	return s.refaultEnabled && s.protectedUntil[page] > s.clock
}

func (s *uffdEvictionPolicySim) oldestEvictablePage() int {
	if page := s.oldestPageIf(func(page int) bool { return !s.protected(page) }); page >= 0 {
		return page
	}
	return s.oldestPage()
}

func (s *uffdEvictionPolicySim) oldestPage() int {
	return s.oldestPageIf(func(int) bool { return true })
}

func (s *uffdEvictionPolicySim) oldestPageIf(eligible func(int) bool) int {
	oldest := -1
	var oldestClock uint64
	for page, lastUsed := range s.resident {
		if !eligible(page) {
			continue
		}
		if oldest < 0 || lastUsed < oldestClock {
			oldest = page
			oldestClock = lastUsed
		}
	}
	return oldest
}

func (s *uffdEvictionPolicySim) oldestPages(limit int) []int {
	if limit <= 0 || limit > len(s.resident) {
		limit = len(s.resident)
	}
	pages := make([]int, 0, limit)
	for len(pages) < limit {
		next := -1
		var nextClock uint64
		for page, lastUsed := range s.resident {
			if s.protected(page) || containsUFFDEvictionCandidate(pages, page) {
				continue
			}
			if next < 0 || lastUsed < nextClock {
				next = page
				nextClock = lastUsed
			}
		}
		if next < 0 {
			break
		}
		pages = append(pages, next)
	}
	return pages
}

func containsUFFDEvictionCandidate(candidates []int, page int) bool {
	for _, candidate := range candidates {
		if candidate == page {
			return true
		}
	}
	return false
}

func (s *uffdEvictionPolicySim) hot(page int) bool {
	return page >= 0 && page < s.cfg.HotPages
}

func (s *uffdEvictionPolicySim) finish() uffdEvictionWorkloadStats {
	s.stats.ResidentPages = len(s.resident)
	return s.stats
}
