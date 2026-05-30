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
	"bytes"
	"context"
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

func TestUFFDMappingAddressToFileOffset(t *testing.T) {
	mappings := []UFFDRegion{
		{BaseHostVirtAddr: 0x200000, Size: 2 * uffdHugePageSize, Offset: 2 * uffdHugePageSize, PageSize: uffdHugePageSize},
		{BaseHostVirtAddr: 0x800000, Size: uffdHugePageSize, Offset: 32 * uffdHugePageSize, PageSize: uffdHugePageSize},
	}
	require.NoError(t, validateUFFDSharedMappings(mappings))

	r, err := findUFFDRegion(mappings, 0x200123)
	require.NoError(t, err)
	require.Equal(t, uint64(2*uffdHugePageSize+0x123), r.fileOffset(0x200123))

	r, err = findUFFDRegion(mappings, 0x800100)
	require.NoError(t, err)
	require.Equal(t, uint64(32*uffdHugePageSize+0x100), r.fileOffset(0x800100))

	_, err = findUFFDRegion(mappings, 0x600000)
	require.Error(t, err)
}

func TestUFFDSharedMappingValidation(t *testing.T) {
	for name, mappings := range map[string][]UFFDRegion{
		"empty":         nil,
		"zero base":     {{Size: uffdHugePageSize, PageSize: uffdHugePageSize}},
		"zero size":     {{BaseHostVirtAddr: uffdHugePageSize, PageSize: uffdHugePageSize}},
		"bad page size": {{BaseHostVirtAddr: uffdHugePageSize, Size: uffdHugePageSize, PageSize: 4096}},
		"unaligned":     {{BaseHostVirtAddr: uffdHugePageSize + 1, Size: uffdHugePageSize, PageSize: uffdHugePageSize}},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, validateUFFDSharedMappings(mappings))
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

func TestSmartmapOptionsResidentSize(t *testing.T) {
	pages, err := validateOptions(Options{ResidentSize: 4 * uffdHugePageSize})
	require.NoError(t, err)
	require.Equal(t, 4, pages)

	_, err = validateOptions(Options{ResidentSize: uffdHugePageSize + 1})
	require.Error(t, err)
	require.Contains(t, err.Error(), "smartmap resident size")
}

func TestUFFDControlAckIsMetadataOnly(t *testing.T) {
	ok := true
	payload, err := json.Marshal(smartmapFrame{
		Type: smartmapFrameAck,
		OK:   &ok,
	})
	require.NoError(t, err)
	require.NotContains(t, string(payload), "memory_id")
	require.NotContains(t, string(payload), "request_id")
	require.NotContains(t, string(payload), "dirty")
	require.NotContains(t, string(payload), "data")
}

func TestSmartmapFrameRejectsFDMismatch(t *testing.T) {
	server, client := unixConnPair(t)
	defer server.Close()
	defer client.Close()

	devNull, err := os.Open("/dev/null")
	require.NoError(t, err)
	defer devNull.Close()

	frame := smartmapFrame{
		Type:             smartmapFrameAttach,
		FD:               smartmapFDUFFD,
		BaseHostVirtAddr: uffdHugePageSize,
		Size:             uffdHugePageSize,
		PageSize:         uffdHugePageSize,
	}
	payload, err := json.Marshal(frame)
	require.NoError(t, err)
	_, _, err = client.WriteMsgUnix(payload, syscall.UnixRights(int(devNull.Fd()), int(devNull.Fd())), nil)
	require.NoError(t, err)

	_, fds, err := readSmartmapFrame(server)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected exactly one uffd fd")
	require.Empty(t, fds)
}

func TestReadSmartmapFrameRejectsMalformedPayload(t *testing.T) {
	server, client := unixConnPair(t)
	defer server.Close()
	defer client.Close()

	devNull, err := os.Open("/dev/null")
	require.NoError(t, err)
	defer devNull.Close()
	openFDs := countOpenFDs(t)

	_, _, err = client.WriteMsgUnix([]byte("{"), syscall.UnixRights(int(devNull.Fd())), nil)
	require.NoError(t, err)

	_, fds, err := readSmartmapFrame(server)
	require.Error(t, err)
	require.Empty(t, fds)
	require.Equal(t, openFDs, countOpenFDs(t))
}

func TestReadSmartmapFrameRejectsUnknownField(t *testing.T) {
	server, client := unixConnPair(t)
	defer server.Close()
	defer client.Close()

	_, err := client.Write([]byte(`{"type":"map","path":"/file","size":2097152,"memory_id":"memory-1"}`))
	require.NoError(t, err)

	_, fds, err := readSmartmapFrame(server)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown field")
	require.Empty(t, fds)
}

func TestReadSmartmapFrameRejectsTruncatedControlMessage(t *testing.T) {
	server, client := unixConnPair(t)
	defer server.Close()
	defer client.Close()

	devNull, err := os.Open("/dev/null")
	require.NoError(t, err)
	defer devNull.Close()

	frame := smartmapFrame{
		Type: smartmapFrameMap,
		Path: "/file",
		Size: uffdHugePageSize,
	}
	payload, err := json.Marshal(frame)
	require.NoError(t, err)

	rights := syscall.UnixRights(int(devNull.Fd()), int(devNull.Fd()))
	_, _, err = client.WriteMsgUnix(payload, rights, nil)
	require.NoError(t, err)

	oldFdSize := uffdFdSize
	t.Cleanup(func() { uffdFdSize = oldFdSize })
	uffdFdSize = 0

	_, fds, err := readSmartmapFrame(server)
	require.Error(t, err)
	require.Contains(t, err.Error(), "control message truncated")
	require.Empty(t, fds)
}

func TestUFFDServerStopClosesIdleConnections(t *testing.T) {
	var v vfs.VFS
	sock := filepath.Join(t.TempDir(), "smartmap.sock")
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
	p := filepath.Join(t.TempDir(), "smartmap.sock")
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

func TestUFFDSharedEvictionPrefersLessMappedPage(t *testing.T) {
	cache := newUFFDSharedCache()
	shared := &uffdSharedPage{key: "shared", populated: true, lastUsed: 1}
	private := &uffdSharedPage{key: "private", populated: true, lastUsed: 2}
	cache.pages[shared.key] = shared
	cache.pages[private.key] = private
	cache.resident = 2

	sessionA := &uffdSharedSession{}
	sessionB := &uffdSharedSession{}
	cache.memories["shared-a"] = &uffdMemory{
		pages:  map[uint64]*uffdSharedPage{0: shared},
		active: map[*uffdSharedSession]struct{}{sessionA: {}, sessionB: {}},
	}
	cache.memories["shared-b"] = &uffdMemory{
		pages:  map[uint64]*uffdSharedPage{0: shared},
		active: map[*uffdSharedSession]struct{}{sessionA: {}},
	}
	cache.memories["private"] = &uffdMemory{
		pages:  map[uint64]*uffdSharedPage{0: private},
		active: map[*uffdSharedSession]struct{}{sessionA: {}},
	}

	victim, targets := cache.reserveEvictionVictim(nil, 2)
	require.Same(t, private, victim)
	require.Len(t, targets, 1)
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

	sock := filepath.Join(t.TempDir(), "smartmap.sock")
	stop, err := Start(v, sock)
	require.NoError(t, err)
	defer stop()

	baseOpen, baseErr := openSharedMemoryFile(t, sock, "/base-memory", memorySize)
	skipUnavailableHugeTLB(t, baseErr)
	require.NoError(t, baseErr)
	defer baseOpen.Close()

	cloneOpen, cloneErr := openSharedMemoryFile(t, sock, "/clone-memory", memorySize)
	skipUnavailableHugeTLB(t, cloneErr)
	require.NoError(t, cloneErr)
	defer cloneOpen.Close()
	_, err = unix.Pwrite(cloneOpen.fd, []byte{0xff}, int64(shmOffsetForFileOffset(t, cloneOpen.extents, 0)))
	require.ErrorIs(t, err, syscall.EBADF, "clients should receive a read-only shared memory fd")
	require.Equal(t, baseOpen.extents, cloneOpen.extents, "cloned clean pages should reuse the same shared hugetlb offsets")
	require.Len(t, cloneOpen.extents, 1, "contiguous shared pages should be returned as one extent")
	requireCloseSharedMemoryFile(t, cloneOpen)
	cloneOpen.client = nil
	requireCloseSharedMemoryFile(t, baseOpen)
	baseOpen.client = nil

	runSharedUFFDClient(t, sock, "/clone-memory", memorySize, []uffdClientAction{
		{Op: "read", Offset: 17, Want: deterministicUFFDByte(17)},
		{Op: "read", Offset: memorySize/2 + 33, Want: deterministicUFFDByte(memorySize/2 + 33)},
		{Op: "read", Offset: memorySize - 1, Want: deterministicUFFDByte(memorySize - 1)},
		{Op: "write", Offset: 0, Value: deterministicUFFDByte(0) ^ 0x7f},
		{Op: "read", Offset: 0, Want: deterministicUFFDByte(0) ^ 0x7f},
	})

	baseOpen, baseErr = openSharedMemoryFile(t, sock, "/base-memory", memorySize)
	skipUnavailableHugeTLB(t, baseErr)
	require.NoError(t, baseErr)
	defer baseOpen.Close()
	require.Equal(t, deterministicUFFDByte(0), readMappedByte(t, baseOpen.client.Mapped(), 0), "write-protected CoW must not alter the source file")

	stop()
}

func TestSmartmapSessionRejectsAttachBeforeMap(t *testing.T) {
	var v vfs.VFS
	sock := filepath.Join(t.TempDir(), "smartmap.sock")
	stop, err := Start(&v, sock)
	require.NoError(t, err)
	defer stop()

	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: sock, Net: "unix"})
	require.NoError(t, err)
	defer conn.Close()

	frame := smartmapFrame{Type: smartmapFrameAttach, BaseHostVirtAddr: uffdHugePageSize, Size: uffdHugePageSize, PageSize: uffdHugePageSize}
	payload, err := json.Marshal(frame)
	require.NoError(t, err)
	_, _, err = conn.WriteMsgUnix(payload, nil, nil)
	require.NoError(t, err)

	resp, fds, err := readSmartmapFrame(conn)
	require.NoError(t, err)
	require.Empty(t, fds)
	require.Equal(t, smartmapFrameFatal, resp.Type)
	require.Contains(t, resp.Error, "expected \"map\"")
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
	memory, err := cache.openMappedFile(v, ctx, "/base-memory", memorySize)
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

	sock := filepath.Join(t.TempDir(), "smartmap.sock")
	stop, err := Start(v, sock)
	require.NoError(t, err)
	defer stop()

	baseOpen, baseErr := openSharedMemoryFile(t, sock, "/base-memory", memorySize)
	skipUnavailableHugeTLB(t, baseErr)
	require.NoError(t, baseErr)
	defer baseOpen.Close()

	cloneA, err := openSharedMemoryFile(t, sock, "/clone-a", memorySize)
	skipUnavailableHugeTLB(t, err)
	require.NoError(t, err)
	defer cloneA.Close()
	cloneB, err := openSharedMemoryFile(t, sock, "/clone-b", memorySize)
	skipUnavailableHugeTLB(t, err)
	require.NoError(t, err)
	defer cloneB.Close()
	cloneC, err := openSharedMemoryFile(t, sock, "/clone-c", memorySize)
	skipUnavailableHugeTLB(t, err)
	require.NoError(t, err)
	defer cloneC.Close()

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
	defer refreshedB.Close()
	refreshedA, err := openSharedMemoryFile(t, sock, "/clone-a", memorySize)
	skipUnavailableHugeTLB(t, err)
	require.NoError(t, err)
	defer refreshedA.Close()
	refreshedC, err := openSharedMemoryFile(t, sock, "/clone-c", memorySize)
	skipUnavailableHugeTLB(t, err)
	require.NoError(t, err)
	defer refreshedC.Close()

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

	sock := filepath.Join(t.TempDir(), "smartmap.sock")
	stop, err := Start(v, sock)
	require.NoError(t, err)
	defer stop()

	opened, openErr := openSharedMemoryFile(t, sock, "/base-memory", memorySize)
	skipUnavailableHugeTLB(t, openErr)
	require.NoError(t, openErr)
	defer opened.Close()

	dirty := deterministicUFFDData(uffdHugePageSize)
	for i := range dirty {
		dirty[i] ^= 0x7f
	}
	overwriteVFSFileRange(t, v, ctx, "base-memory", dirty, 0)

	require.Equal(t, deterministicUFFDByte(17), readMappedByte(t, opened.client.Mapped(), 17))
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

	sock := filepath.Join(t.TempDir(), "smartmap.sock")
	stop, err := Start(v, sock)
	require.NoError(t, err)
	defer stop()

	baseOpen, baseErr := openSharedMemoryFile(t, sock, "/base-memory", memorySize)
	skipUnavailableHugeTLB(t, baseErr)
	require.NoError(t, baseErr)
	defer baseOpen.Close()

	opened, openErr := openSharedMemoryFile(t, sock, "/clone-memory", memorySize)
	skipUnavailableHugeTLB(t, openErr)
	require.NoError(t, openErr)
	defer opened.Close()
	require.Equal(t, baseOpen.extents, opened.extents)

	const privateByte = 0x6d
	runSharedUFFDClient(t, sock, opened.path, memorySize, []uffdClientAction{
		{Op: "write", Offset: 0, Value: privateByte},
		{Op: "read", Offset: 0, Want: privateByte},
		{Op: "read", Offset: 17, Want: deterministicUFFDByte(17)},
	})

	shared := make([]byte, 1)
	_, err = unix.Pread(baseOpen.fd, shared, int64(shmOffsetForFileOffset(t, baseOpen.extents, 0)))
	require.NoError(t, err)
	require.Equal(t, deterministicUFFDByte(0), shared[0], "write-protected CoW must not mutate shared clean backing")

	requireCloseSharedMemoryFile(t, opened)
	refreshed, refreshErr := openSharedMemoryFile(t, sock, "/clone-memory", memorySize)
	skipUnavailableHugeTLB(t, refreshErr)
	require.NoError(t, refreshErr)
	defer refreshed.Close()
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

	sock := filepath.Join(t.TempDir(), "smartmap.sock")
	stop, err := Start(v, sock)
	require.NoError(t, err)
	defer stop()

	opened, openErr := openSharedMemoryFile(t, sock, "/base-memory", memorySize)
	skipUnavailableHugeTLB(t, openErr)
	require.NoError(t, openErr)
	defer opened.Close()

	const clients = 4
	releaseFile := filepath.Join(t.TempDir(), "release")
	procs := make([]*uffdClientProcess, 0, clients)
	for i := 0; i < clients; i++ {
		readyFile := filepath.Join(t.TempDir(), fmt.Sprintf("ready-%d", i))
		script := newSharedUFFDClientScript(sock, opened.path, memorySize, []uffdClientAction{
			{Op: "mark_ready", ReadyFile: readyFile},
			{Op: "wait_file", ReadyFile: releaseFile},
			{Op: "read", Offset: 17, Want: deterministicUFFDByte(17)},
		})
		procs = append(procs, startUFFDClientProcess(t, script, uffdClientTimeout(script)))
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

	sock := filepath.Join(t.TempDir(), "smartmap.sock")
	stop, err := Start(v, sock)
	require.NoError(t, err)
	defer stop()

	firstOpen, firstErr := openSharedMemoryFile(t, sock, "/clone-memory", memorySize)
	skipUnavailableHugeTLB(t, firstErr)
	require.NoError(t, firstErr)
	defer firstOpen.Close()
	secondOpen, secondErr := openSharedMemoryFile(t, sock, "/clone-memory", memorySize)
	skipUnavailableHugeTLB(t, secondErr)
	require.NoError(t, secondErr)
	defer secondOpen.Close()
	require.Equal(t, firstOpen.extents, secondOpen.extents)

	const privateByte = 0x41
	runSharedUFFDClient(t, sock, firstOpen.path, memorySize, []uffdClientAction{
		{Op: "read", Offset: 17, Want: deterministicUFFDByte(17)},
		{Op: "write", Offset: 0, Value: privateByte},
		{Op: "read", Offset: 0, Want: privateByte},
	})
	runSharedUFFDClient(t, sock, secondOpen.path, memorySize, []uffdClientAction{
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

	sock := filepath.Join(t.TempDir(), "smartmap.sock")
	stop, err := Start(v, sock)
	require.NoError(t, err)
	defer stop()

	oldOpen, openErr := openSharedMemoryFile(t, sock, "/old-memory", memorySize)
	skipUnavailableHugeTLB(t, openErr)
	require.NoError(t, openErr)
	defer oldOpen.Close()

	runSharedUFFDClient(t, sock, oldOpen.path, memorySize, []uffdClientAction{
		{Op: "read", Offset: 0, Want: patternedUFFDByte(0x11, 0)},
		{Op: "read", Offset: 12345, Want: patternedUFFDByte(0x11, 12345)},
	})
	requireCloseSharedMemoryFile(t, oldOpen)

	newOpen, openErr := openSharedMemoryFile(t, sock, "/new-memory", memorySize)
	skipUnavailableHugeTLB(t, openErr)
	require.NoError(t, openErr)
	defer newOpen.Close()
	require.Equal(t, shmOffsetForFileOffset(t, oldOpen.extents, 0), shmOffsetForFileOffset(t, newOpen.extents, 0), "released slot should be reused instead of growing the shared file")

	runSharedUFFDClient(t, sock, newOpen.path, memorySize, []uffdClientAction{
		{Op: "read", Offset: 0, Want: patternedUFFDByte(0x82, 0)},
		{Op: "read", Offset: 12345, Want: patternedUFFDByte(0x82, 12345)},
	})
}

func TestUFFDSharedSessionCloseKeepsCloneReference(t *testing.T) {
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

	sock := filepath.Join(t.TempDir(), "smartmap.sock")
	stop, err := Start(v, sock)
	require.NoError(t, err)
	defer stop()

	baseOpen, baseErr := openSharedMemoryFile(t, sock, "/base-memory", memorySize)
	skipUnavailableHugeTLB(t, baseErr)
	require.NoError(t, baseErr)
	defer baseOpen.Close()
	cloneOpen, cloneErr := openSharedMemoryFile(t, sock, "/clone-memory", memorySize)
	skipUnavailableHugeTLB(t, cloneErr)
	require.NoError(t, cloneErr)
	defer cloneOpen.Close()
	require.Equal(t, baseOpen.extents, cloneOpen.extents)

	baseOffset := shmOffsetForFileOffset(t, baseOpen.extents, 0)
	baseOpen.Close()
	baseOpen.client = nil

	otherOpen, otherErr := openSharedMemoryFile(t, sock, "/other-memory", memorySize)
	skipUnavailableHugeTLB(t, otherErr)
	require.NoError(t, otherErr)
	defer otherOpen.Close()
	require.NotEqual(t, baseOffset, shmOffsetForFileOffset(t, otherOpen.extents, 0), "closing one clone reference must not free a page still used by another open memory")

	cloneOpen.Close()
	cloneOpen.client = nil
	afterOpen, afterErr := openSharedMemoryFile(t, sock, "/after-memory", memorySize)
	skipUnavailableHugeTLB(t, afterErr)
	require.NoError(t, afterErr)
	defer afterOpen.Close()
	require.Equal(t, baseOffset, shmOffsetForFileOffset(t, afterOpen.extents, 0), "slot should become reusable after the last clone reference is closed")
}

func TestUFFDSharedSocketCloseReleasesActiveSession(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real userfaultfd hugetlb integration test in short mode")
	}

	v, _ := createTestVFS(nil, "")
	ctx := vfs.NewLogContext(meta.Background())
	const memorySize = uffdHugePageSize
	writePatternVFSFile(t, v, ctx, "base-memory", memorySize, 0x33)

	sock := filepath.Join(t.TempDir(), "smartmap.sock")
	stop, err := Start(v, sock)
	require.NoError(t, err)
	defer stop()

	activeClient, activeErr := OpenRustClient(sock, "/base-memory", memorySize, 0, nil)
	skipUnavailableHugeTLB(t, activeErr)
	require.NoError(t, activeErr)
	require.Equal(t, patternedUFFDByte(0x33, 0), readMappedByte(t, activeClient.Mapped(), 0))
	activeClient.Close()

	reopened, err := openSharedMemoryFile(t, sock, "/base-memory", memorySize)
	skipUnavailableHugeTLB(t, err)
	require.NoError(t, err)
	defer reopened.Close()
	require.Equal(t, patternedUFFDByte(0x33, 0), readMappedByte(t, reopened.client.Mapped(), 0))
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

	sock := filepath.Join(t.TempDir(), "smartmap.sock")
	stop, err := StartWithOptions(v, sock, Options{ResidentSize: uffdHugePageSize})
	require.NoError(t, err)
	defer stop()

	opened, openErr := openSharedMemoryFile(t, sock, "/base-memory", memorySize)
	skipUnavailableHugeTLB(t, openErr)
	require.NoError(t, openErr)
	defer opened.Close()

	runSharedUFFDClient(t, sock, opened.path, memorySize, []uffdClientAction{
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

		sock := filepath.Join(t.TempDir(), "smartmap.sock")
		stop, err := StartWithOptions(v, sock, Options{ResidentSize: 4 * uffdHugePageSize})
		require.NoError(t, err)
		defer stop()

		opened, openErr := openSharedMemoryFile(t, sock, "/base-memory", memorySize)
		skipUnavailableHugeTLB(t, openErr)
		require.NoError(t, openErr)
		defer opened.Close()

		runSharedUFFDClient(t, sock, opened.path, memorySize, []uffdClientAction{
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

		sock := filepath.Join(t.TempDir(), "smartmap.sock")
		stop, err := StartWithOptions(v, sock, Options{
			ResidentSize:        4 * uffdHugePageSize,
			EvictionPolicy:      uffdEvictionPolicyProbe,
			ProbeCandidatePages: 2,
			ProbeEvictPages:     1,
			ProbeMonitorMillis:  40,
		})
		require.NoError(t, err)
		defer stop()

		opened, openErr := openSharedMemoryFile(t, sock, "/base-memory", memorySize)
		skipUnavailableHugeTLB(t, openErr)
		require.NoError(t, openErr)
		defer opened.Close()

		script := newSharedUFFDClientScript(sock, opened.path, memorySize, []uffdClientAction{
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
		runUFFDClientProcess(t, script)
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

	sock := filepath.Join(t.TempDir(), "smartmap.sock")
	stop, err := StartWithOptions(v, sock, Options{ResidentSize: 4 * uffdHugePageSize})
	require.NoError(t, err)
	defer stop()

	opened, openErr := openSharedMemoryFile(t, sock, "/base-memory", memorySize)
	skipUnavailableHugeTLB(t, openErr)
	require.NoError(t, openErr)
	defer opened.Close()

	runSharedUFFDClient(t, sock, opened.path, memorySize, []uffdClientAction{
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

func BenchmarkUFFDSharedMapFile1GiB(b *testing.B) {
	v, _ := createTestVFS(nil, "")
	ctx := vfs.NewLogContext(meta.Background())
	const memorySize = 1024 << 20
	writeLargeVFSFile(b, v, ctx, "base-memory", memorySize)
	cache := newUFFDSharedCache()
	defer cache.close()

	if _, err := cache.openMappedFile(v, ctx, "/base-memory", memorySize); err != nil {
		skipUnavailableHugeTLB(b, err)
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(memorySize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		memory, err := cache.openMappedFile(v, ctx, "/base-memory", memorySize)
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
			sock := filepath.Join(b.TempDir(), "smartmap.sock")
			stop, err := Start(v, sock)
			if err != nil {
				b.Fatal(err)
			}
			defer stop()

			client, err := OpenRustClient(sock, "/base-memory", memorySize, 0, nil)
			skipUnavailableHugeTLB(b, err)
			if err != nil {
				b.Fatal(err)
			}
			defer client.Close()

			b.StartTimer()
			if got := readMappedByte(b, client.Mapped(), 17); got != deterministicUFFDByte(17) {
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

func countOpenFDs(t testing.TB) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	return len(entries)
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
	uffdClientHelperEnv = "JUICEFS_UFFD_CLIENT_HELPER"
)

type uffdClientScript struct {
	Sock              string             `json:"sock"`
	Path              string             `json:"path,omitempty"`
	Size              uint64             `json:"size"`
	WritebackPath     string             `json:"writeback_path,omitempty"`
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
	evictions  <-chan smartmapFrame
	probes     <-chan smartmapFrame
	baseAddr   uintptr
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
	return runSharedUFFDClientScript(script)
}

func runSharedUFFDClientScript(script uffdClientScript) uffdClientResult {
	ensureUFFDTestSchedulerRoom(2)
	handler := &testRustSmartmapControlHandler{
		evictions: make(chan smartmapFrame, 16),
		probes:    make(chan smartmapFrame, 16),
	}
	client, err := OpenRustClient(script.Sock, script.Path, script.Size, 0, handler)
	if err != nil {
		if skip := hugeTLBUnavailableReason(err); skip != "" {
			return uffdClientResult{Skip: skip}
		}
		return uffdClientResult{Error: err.Error()}
	}
	defer client.Close()
	go client.ServeControls()

	state := uffdClientActionState{
		evictions:  handler.evictions,
		probes:     handler.probes,
		baseAddr:   client.BaseAddr(),
		mappedName: "shared memory range",
	}
	if err := runUFFDClientActions(client.Mapped(), script.Actions, state); err != nil {
		return uffdClientResult{Error: err.Error()}
	}
	return uffdClientResult{OK: true}
}

type testRustSmartmapControlHandler struct {
	evictions chan smartmapFrame
	probes    chan smartmapFrame
}

func (h *testRustSmartmapControlHandler) Release(ranges []RustControlRange) error {
	msg := h.controlMessage(smartmapFrameRelease, ranges)
	select {
	case h.evictions <- msg:
	default:
	}
	return nil
}

func (h *testRustSmartmapControlHandler) Probe(ranges []RustControlRange) error {
	msg := h.controlMessage(smartmapFrameProbe, ranges)
	select {
	case h.probes <- msg:
	default:
	}
	return nil
}

func (h *testRustSmartmapControlHandler) WriteFault(ranges []RustControlRange) error {
	return nil
}

func (h *testRustSmartmapControlHandler) controlMessage(kind string, ranges []RustControlRange) smartmapFrame {
	msg := smartmapFrame{Type: kind}
	for _, r := range ranges {
		msg.Ranges = append(msg.Ranges, uffdControlRange{FileOffset: r.FileOffset, Length: r.Length, ShmOffset: r.ShmOffset})
	}
	return msg
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
		case "expect_eviction":
			if err := expectClientEviction(action, state.evictions); err != nil {
				return err
			}
		case "expect_probe":
			if err := expectClientProbe(action, state.probes); err != nil {
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

func expectClientEviction(action uffdClientAction, evictions <-chan smartmapFrame) error {
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

func expectClientProbe(action uffdClientAction, probes <-chan smartmapFrame) error {
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

func controlMessageContainsOffset(msg smartmapFrame, off uint64) bool {
	for _, r := range msg.Ranges {
		if r.Length == uint64(uffdHugePageSize) && r.FileOffset == off {
			return true
		}
	}
	return false
}

func runSharedUFFDClient(t testing.TB, sock string, filePath string, size uint64, actions []uffdClientAction) {
	t.Helper()
	runUFFDClientProcess(t, newSharedUFFDClientScript(sock, filePath, size, actions))
}

func newSharedUFFDClientScript(sock string, filePath string, size uint64, actions []uffdClientAction) uffdClientScript {
	return uffdClientScript{
		Sock:    sock,
		Path:    filePath,
		Size:    size,
		Actions: actions,
	}
}

func runUFFDClientProcess(t testing.TB, script uffdClientScript) uffdClientResult {
	t.Helper()
	proc := startUFFDClientProcess(t, script, uffdClientTimeout(script))
	result := proc.wait(t)
	requireUFFDClientResult(t, result)
	return result
}

func startUFFDClientProcess(t testing.TB, script uffdClientScript, timeout time.Duration) *uffdClientProcess {
	t.Helper()
	payload, err := json.Marshal(script)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestUFFDClientHelperProcess$")
	cmd.Env = append(os.Environ(), uffdClientHelperEnv+"=1")
	cmd.Stdin = bytes.NewReader(payload)

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

type sharedMemoryOpen struct {
	client  *RustClient
	fd      int
	extents []UFFDExtent
	path    string
}

func (o sharedMemoryOpen) Close() {
	if o.client != nil {
		o.client.Close()
	}
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
	client, err := OpenRustClient(sock, filePath, size, 0, noopRustControlHandler{})
	if err != nil {
		return sharedMemoryOpen{}, err
	}
	go client.ServeControls()
	return sharedMemoryOpen{
		client:  client,
		fd:      client.SharedFD(),
		extents: client.Extents(),
		path:    filePath,
	}, nil
}

type noopRustControlHandler struct{}

func (noopRustControlHandler) Release([]RustControlRange) error { return nil }
func (noopRustControlHandler) Probe([]RustControlRange) error   { return nil }
func (noopRustControlHandler) WriteFault([]RustControlRange) error {
	return nil
}

func requireCloseSharedMemoryFile(t testing.TB, opened sharedMemoryOpen) {
	t.Helper()
	opened.Close()
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
