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

package fuse

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/juicedata/juicefs/pkg/vfs/smartmap"
	"github.com/juicedata/juicefs/pkg/vfs/smartmap/rustclient"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const (
	fuseUFFDHelperEnv    = "JUICEFS_FUSE_UFFD_HELPER"
	fuseUFFDHugePageSize = 2 << 20

	fuseUFFDPagemapEntrySize  = 8
	fuseUFFDPagemapPresentBit = uint64(1) << 63
	fuseUFFDPagemapWPBit      = uint64(1) << 57

	fuseUFFDAPI                  = 0xAA
	fuseUFFDIOWriteProtectModeWP = 1 << 0
)

var (
	fuseUFFDIOWriteProtectIoctl = fuseUFFDIOWR(0x06, unsafe.Sizeof(fuseUFFDIOWriteProtect{}))
)

type fuseUFFDIORange struct {
	start uint64
	len   uint64
}

type fuseUFFDIOWriteProtect struct {
	rng  fuseUFFDIORange
	mode uint64
}

func fuseUFFDIOWR(nr, size uintptr) uintptr {
	const (
		iocNRBits    = 8
		iocTypeBits  = 8
		iocSizeBits  = 14
		iocNRShift   = 0
		iocTypeShift = iocNRShift + iocNRBits
		iocSizeShift = iocTypeShift + iocTypeBits
		iocDirShift  = iocSizeShift + iocSizeBits
		iocWrite     = 1
		iocRead      = 2
	)
	return uintptr(iocRead|iocWrite)<<iocDirShift |
		size<<iocSizeShift |
		uintptr(fuseUFFDAPI)<<iocTypeShift |
		nr<<iocNRShift
}

type fuseUFFDClientScript struct {
	Sock                string                 `json:"sock"`
	Path                string                 `json:"path"`
	WritebackPath       string                 `json:"writeback_path"`
	AlternatePath       string                 `json:"alternate_path,omitempty"`
	Size                uint64                 `json:"size"`
	FlushIntervalMillis int                    `json:"flush_interval_millis,omitempty"`
	Actions             []fuseUFFDClientAction `json:"actions"`
}

type fuseUFFDClientAction struct {
	Op           string `json:"op"`
	Offset       uint64 `json:"offset,omitempty"`
	Value        byte   `json:"value,omitempty"`
	Want         byte   `json:"want,omitempty"`
	Count        int    `json:"count,omitempty"`
	UseAlternate bool   `json:"use_alternate,omitempty"`
	Milliseconds int    `json:"milliseconds,omitempty"`
}

type fuseUFFDClientResult struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
	Skip  string `json:"skip,omitempty"`
}

type fuseUFFDControlRange struct {
	FileOffset uint64 `json:"file_offset"`
	Length     uint64 `json:"length"`
	ShmOffset  uint64 `json:"shm_offset"`
}

type fuseUFFDControlMessage struct {
	Type   string                 `json:"type"`
	Ranges []fuseUFFDControlRange `json:"ranges,omitempty"`
}

type fuseUFFDOpenedMemory struct {
	client  *rustclient.Client
	extents []rustclient.Extent
}

type fuseUFFDClientState struct {
	mapped    []byte
	uffdFD    int
	baseAddr  uintptr
	writeback string
	alternate string
	client    *rustclient.Client
	evictions chan fuseUFFDControlMessage
	mu        sync.Mutex
	cond      *sync.Cond
	pauses    int
	resumes   int
	syncing   bool
	synced    map[uint64]bool
	resume    *fuseUFFDResumeWrite
}

type fuseRustSmartmapControlHandler struct {
	state *fuseUFFDClientState
}

type fuseUFFDResumeWrite struct {
	offset uint64
	value  byte
	done   chan error
}

func TestUFFDPrivatePeriodicWritebackThroughMountedFuse(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real FUSE/userfaultfd integration test in short mode")
	}

	tmp := t.TempDir()
	metaPath := filepath.Join(tmp, "meta.db")
	metaURL := "sqlite3://" + metaPath
	mp := filepath.Join(tmp, "mp")
	sock := filepath.Join(tmp, "smartmap.sock")
	require.NoError(t, setUpWithUFFD(metaURL, mp, sock))
	defer umount(mp, true)

	const memorySize = 2 * fuseUFFDHugePageSize
	memoryPath := filepath.Join(mp, "vm-memory")
	writeFuseUFFDFile(t, memoryPath, memorySize)

	const privateByte = 0xc7
	result := runFuseUFFDClient(t, fuseUFFDClientScript{
		Sock:                sock,
		Path:                "/vm-memory",
		WritebackPath:       memoryPath,
		Size:                memorySize,
		FlushIntervalMillis: 25,
		Actions: []fuseUFFDClientAction{
			{Op: "read", Offset: 17, Want: fuseUFFDDeterministicByte(17)},
			{Op: "write", Offset: 0, Value: privateByte},
			{Op: "read", Offset: 0, Want: privateByte},
			{Op: "sleep", Milliseconds: 100},
			{Op: "read", Offset: 0, Want: privateByte},
			{Op: "read", Offset: 17, Want: fuseUFFDDeterministicByte(17)},
		},
	})
	if result.Skip != "" {
		t.Skip(result.Skip)
	}
	require.True(t, result.OK, "helper error: %s", result.Error)

	requireFuseFileByte(t, memoryPath, 0, privateByte)
	requireFuseFileByte(t, memoryPath, 17, fuseUFFDDeterministicByte(17))
	requireFuseFileByte(t, memoryPath, fuseUFFDHugePageSize-1, fuseUFFDDeterministicByte(fuseUFFDHugePageSize-1))
	requireFuseFileByte(t, memoryPath, fuseUFFDHugePageSize+19, fuseUFFDDeterministicByte(fuseUFFDHugePageSize+19))
}

func TestUFFDPrivateWriteWithoutClientFlushDoesNotPersistThroughMountedFuse(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real FUSE/userfaultfd integration test in short mode")
	}

	tmp := t.TempDir()
	mp, sock := setupFuseUFFDMount(t, tmp)
	const memorySize = fuseUFFDHugePageSize
	memoryPath := filepath.Join(mp, "vm-memory")
	writeFuseUFFDFile(t, memoryPath, memorySize)

	const privateByte = 0x9e
	result := runFuseUFFDClient(t, fuseUFFDClientScript{
		Sock:          sock,
		Path:          "/vm-memory",
		WritebackPath: memoryPath,
		Size:          memorySize,
		Actions: []fuseUFFDClientAction{
			{Op: "read", Offset: 17, Want: fuseUFFDDeterministicByte(17)},
			{Op: "write", Offset: 0, Value: privateByte},
			{Op: "read", Offset: 0, Want: privateByte},
		},
	})
	requireFuseUFFDClientResult(t, result)
	requireFuseFileByte(t, memoryPath, 0, fuseUFFDDeterministicByte(0))
	requireFuseFileByte(t, memoryPath, 17, fuseUFFDDeterministicByte(17))
}

func TestUFFDPrivateWritebackPreservesPartialHugePageThroughMountedFuse(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real FUSE/userfaultfd integration test in short mode")
	}

	tmp := t.TempDir()
	mp, sock := setupFuseUFFDMount(t, tmp)
	const memorySize = 2 * fuseUFFDHugePageSize
	memoryPath := filepath.Join(mp, "vm-memory")
	writeFuseUFFDFile(t, memoryPath, memorySize)

	const dirtyOff = uint64(12345)
	const privateByte = 0x4a
	result := runFuseUFFDClient(t, fuseUFFDClientScript{
		Sock:                sock,
		Path:                "/vm-memory",
		WritebackPath:       memoryPath,
		Size:                memorySize,
		FlushIntervalMillis: 25,
		Actions: []fuseUFFDClientAction{
			{Op: "read", Offset: 0, Want: fuseUFFDDeterministicByte(0)},
			{Op: "write", Offset: dirtyOff, Value: privateByte},
			{Op: "sleep", Milliseconds: 100},
			{Op: "read", Offset: fuseUFFDHugePageSize + 7, Want: fuseUFFDDeterministicByte(fuseUFFDHugePageSize + 7)},
		},
	})
	requireFuseUFFDClientResult(t, result)
	requireFuseFileByte(t, memoryPath, dirtyOff, privateByte)
	requireFuseFileByte(t, memoryPath, 0, fuseUFFDDeterministicByte(0))
	requireFuseFileByte(t, memoryPath, 17, fuseUFFDDeterministicByte(17))
	requireFuseFileByte(t, memoryPath, fuseUFFDHugePageSize-1, fuseUFFDDeterministicByte(fuseUFFDHugePageSize-1))
	requireFuseFileByte(t, memoryPath, fuseUFFDHugePageSize+7, fuseUFFDDeterministicByte(fuseUFFDHugePageSize+7))
}

func TestUFFDPrivatePeriodicWritebackCloneIsolationThroughMountedFuse(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real FUSE/userfaultfd integration test in short mode")
	}

	tmp := t.TempDir()
	mp, sock := setupFuseUFFDMount(t, tmp)
	const memorySize = 2 * fuseUFFDHugePageSize
	basePath := filepath.Join(mp, "base-memory")
	clonePath := filepath.Join(mp, "clone-memory")
	writeFuseUFFDFile(t, basePath, memorySize)
	cloneFuseFile(t, mp, "base-memory", "clone-memory")

	baseOpen := openFuseUFFDMemoryForTest(t, sock, "/base-memory", memorySize)
	cloneOpen := openFuseUFFDMemoryForTest(t, sock, "/clone-memory", memorySize)
	require.Equal(t, baseOpen.extents, cloneOpen.extents)
	closeFuseUFFDMemoryForTest(t, sock, cloneOpen)
	closeFuseUFFDMemoryForTest(t, sock, baseOpen)

	const privateByte = 0xd2
	result := runFuseUFFDClient(t, fuseUFFDClientScript{
		Sock:                sock,
		Path:                "/clone-memory",
		WritebackPath:       clonePath,
		Size:                memorySize,
		FlushIntervalMillis: 25,
		Actions: []fuseUFFDClientAction{
			{Op: "read", Offset: 17, Want: fuseUFFDDeterministicByte(17)},
			{Op: "write", Offset: 0, Value: privateByte},
			{Op: "sleep", Milliseconds: 100},
			{Op: "read", Offset: 0, Want: privateByte},
			{Op: "read", Offset: fuseUFFDHugePageSize + 19, Want: fuseUFFDDeterministicByte(fuseUFFDHugePageSize + 19)},
		},
	})
	requireFuseUFFDClientResult(t, result)

	requireFuseFileByte(t, clonePath, 0, privateByte)
	requireFuseFileByte(t, clonePath, 17, fuseUFFDDeterministicByte(17))
	requireFuseFileByte(t, basePath, 0, fuseUFFDDeterministicByte(0))
	requireFuseFileByte(t, basePath, 17, fuseUFFDDeterministicByte(17))

	baseAfter := openFuseUFFDMemoryForTest(t, sock, "/base-memory", memorySize)
	defer closeFuseUFFDMemoryForTest(t, sock, baseAfter)
	cloneAfter := openFuseUFFDMemoryForTest(t, sock, "/clone-memory", memorySize)
	defer closeFuseUFFDMemoryForTest(t, sock, cloneAfter)
	require.NotEqual(t,
		fuseUFFDShmOffsetForFileOffset(t, baseAfter.extents, 0),
		fuseUFFDShmOffsetForFileOffset(t, cloneAfter.extents, 0),
		"dirty clone page should no longer dedupe with the base file",
	)
	require.Equal(t,
		fuseUFFDShmOffsetForFileOffset(t, baseAfter.extents, fuseUFFDHugePageSize),
		fuseUFFDShmOffsetForFileOffset(t, cloneAfter.extents, fuseUFFDHugePageSize),
		"clean cloned page should remain deduped",
	)
}

func TestUFFDPeriodicFlushPersistsDirtyPrivatePageAcrossEviction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real FUSE/userfaultfd integration test in short mode")
	}

	tmp := t.TempDir()
	mp, sock := setupFuseUFFDMountWithOptions(t, tmp, smartmap.Options{ResidentSize: fuseUFFDHugePageSize})
	const memorySize = 2 * fuseUFFDHugePageSize
	memoryPath := filepath.Join(mp, "vm-memory")
	writeFuseUFFDFile(t, memoryPath, memorySize)

	const privateByte = 0xc7
	result := runFuseUFFDClient(t, fuseUFFDClientScript{
		Sock:                sock,
		Path:                "/vm-memory",
		WritebackPath:       memoryPath,
		Size:                memorySize,
		FlushIntervalMillis: 10,
		Actions: []fuseUFFDClientAction{
			{Op: "read", Offset: 17, Want: fuseUFFDDeterministicByte(17)},
			{Op: "write", Offset: 0, Value: privateByte},
			{Op: "read", Offset: 0, Want: privateByte},
			{Op: "sleep", Milliseconds: 50},
			{Op: "read", Offset: fuseUFFDHugePageSize + 19, Want: fuseUFFDDeterministicByte(fuseUFFDHugePageSize + 19)},
			{Op: "expect_eviction", Offset: 0},
			{Op: "read", Offset: 0, Want: privateByte},
			{Op: "expect_eviction", Offset: fuseUFFDHugePageSize},
			{Op: "read", Offset: 17, Want: fuseUFFDDeterministicByte(17)},
		},
	})
	requireFuseUFFDClientResult(t, result)
	requireFuseFileByte(t, memoryPath, 0, privateByte)
	requireFuseFileByte(t, memoryPath, 17, fuseUFFDDeterministicByte(17))
}

func TestUFFDEvictionPreservesClientFlushedWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real FUSE/userfaultfd integration test in short mode")
	}

	tmp := t.TempDir()
	mp, sock := setupFuseUFFDMountWithOptions(t, tmp, smartmap.Options{ResidentSize: fuseUFFDHugePageSize})
	const memorySize = 2 * fuseUFFDHugePageSize
	memoryPath := filepath.Join(mp, "vm-memory")
	writeFuseUFFDFile(t, memoryPath, memorySize)

	const privateByte = 0xd4
	result := runFuseUFFDClient(t, fuseUFFDClientScript{
		Sock:                sock,
		Path:                "/vm-memory",
		WritebackPath:       memoryPath,
		Size:                memorySize,
		FlushIntervalMillis: 10,
		Actions: []fuseUFFDClientAction{
			{Op: "read", Offset: 0, Want: fuseUFFDDeterministicByte(0)},
			{Op: "write", Offset: 0, Value: privateByte},
			{Op: "sleep", Milliseconds: 50},
			{Op: "read", Offset: fuseUFFDHugePageSize, Want: fuseUFFDDeterministicByte(fuseUFFDHugePageSize)},
			{Op: "expect_eviction", Offset: 0},
			{Op: "read", Offset: 0, Want: privateByte},
		},
	})
	requireFuseUFFDClientResult(t, result)
	requireFuseFileByte(t, memoryPath, 0, privateByte)
}

func TestUFFDSyncPausesMutatorAndFlushesDirtyPrivatePage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real FUSE/userfaultfd integration test in short mode")
	}

	tmp := t.TempDir()
	mp, sock := setupFuseUFFDMountWithOptions(t, tmp, smartmap.Options{})
	const memorySize = fuseUFFDHugePageSize
	memoryPath := filepath.Join(mp, "vm-memory")
	writeFuseUFFDFile(t, memoryPath, memorySize)

	const privateByte = 0x9a
	result := runFuseUFFDClient(t, fuseUFFDClientScript{
		Sock:          sock,
		Path:          "/vm-memory",
		WritebackPath: memoryPath,
		Size:          memorySize,
		Actions: []fuseUFFDClientAction{
			{Op: "read", Offset: 0, Want: fuseUFFDDeterministicByte(0)},
			{Op: "write", Offset: 0, Value: privateByte},
			{Op: "sync"},
			{Op: "expect_pause_count", Count: 1},
			{Op: "read", Offset: 0, Want: privateByte},
		},
	})
	requireFuseUFFDClientResult(t, result)
	requireFuseFileByte(t, memoryPath, 0, privateByte)
}

func TestUFFDSyncResumesMutatorOnWritebackError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real FUSE/userfaultfd integration test in short mode")
	}

	tmp := t.TempDir()
	mp, sock := setupFuseUFFDMountWithOptions(t, tmp, smartmap.Options{})
	const memorySize = fuseUFFDHugePageSize
	memoryPath := filepath.Join(mp, "vm-memory")
	writeFuseUFFDFile(t, memoryPath, memorySize)

	result := runFuseUFFDClient(t, fuseUFFDClientScript{
		Sock:          sock,
		Path:          "/vm-memory",
		WritebackPath: memoryPath,
		AlternatePath: filepath.Join(mp, "missing-memory"),
		Size:          memorySize,
		Actions: []fuseUFFDClientAction{
			{Op: "read", Offset: 0, Want: fuseUFFDDeterministicByte(0)},
			{Op: "write", Offset: 0, Value: 0xa1},
			{Op: "sync_expect_error"},
			{Op: "expect_pause_count", Count: 1},
		},
	})
	requireFuseUFFDClientResult(t, result)
}

func TestUFFDSyncFlushesOnlyPagesDirtyAtSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real FUSE/userfaultfd integration test in short mode")
	}

	tmp := t.TempDir()
	mp, sock := setupFuseUFFDMountWithOptions(t, tmp, smartmap.Options{})
	const memorySize = 2 * fuseUFFDHugePageSize
	memoryPath := filepath.Join(mp, "vm-memory")
	writeFuseUFFDFile(t, memoryPath, memorySize)

	const firstByte = 0xb1
	const secondByte = 0xb2
	result := runFuseUFFDClient(t, fuseUFFDClientScript{
		Sock:          sock,
		Path:          "/vm-memory",
		WritebackPath: memoryPath,
		Size:          memorySize,
		Actions: []fuseUFFDClientAction{
			{Op: "read", Offset: 0, Want: fuseUFFDDeterministicByte(0)},
			{Op: "read", Offset: fuseUFFDHugePageSize, Want: fuseUFFDDeterministicByte(fuseUFFDHugePageSize)},
			{Op: "write", Offset: 0, Value: firstByte},
			{Op: "sync"},
			{Op: "write", Offset: fuseUFFDHugePageSize, Value: secondByte},
			{Op: "expect_file_byte", Offset: 0, Want: firstByte},
			{Op: "expect_file_byte", Offset: fuseUFFDHugePageSize, Want: fuseUFFDDeterministicByte(fuseUFFDHugePageSize)},
			{Op: "sync"},
			{Op: "expect_file_byte", Offset: fuseUFFDHugePageSize, Want: secondByte},
		},
	})
	requireFuseUFFDClientResult(t, result)
}

func TestUFFDSyncWriteDuringBackgroundFlushIsNextGenerationDirty(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real FUSE/userfaultfd integration test in short mode")
	}

	tmp := t.TempDir()
	mp, sock := setupFuseUFFDMountWithOptions(t, tmp, smartmap.Options{})
	const memorySize = fuseUFFDHugePageSize
	memoryPath := filepath.Join(mp, "vm-memory")
	writeFuseUFFDFile(t, memoryPath, memorySize)

	const firstByte = 0xd1
	const secondByte = 0xd2
	result := runFuseUFFDClient(t, fuseUFFDClientScript{
		Sock:          sock,
		Path:          "/vm-memory",
		WritebackPath: memoryPath,
		Size:          memorySize,
		Actions: []fuseUFFDClientAction{
			{Op: "read", Offset: 0, Want: fuseUFFDDeterministicByte(0)},
			{Op: "write", Offset: 0, Value: firstByte},
			{Op: "sync_with_resume_write", Offset: 0, Value: secondByte},
			{Op: "expect_file_byte", Offset: 0, Want: firstByte},
			{Op: "read", Offset: 0, Want: secondByte},
			{Op: "sync"},
			{Op: "expect_file_byte", Offset: 0, Want: secondByte},
		},
	})
	requireFuseUFFDClientResult(t, result)
}

func TestUFFDNormalWriteProtectFaultDoesNotWriteBack(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real FUSE/userfaultfd integration test in short mode")
	}

	tmp := t.TempDir()
	mp, sock := setupFuseUFFDMountWithOptions(t, tmp, smartmap.Options{})
	const memorySize = fuseUFFDHugePageSize
	memoryPath := filepath.Join(mp, "vm-memory")
	writeFuseUFFDFile(t, memoryPath, memorySize)

	result := runFuseUFFDClient(t, fuseUFFDClientScript{
		Sock:          sock,
		Path:          "/vm-memory",
		WritebackPath: memoryPath,
		Size:          memorySize,
		Actions: []fuseUFFDClientAction{
			{Op: "read", Offset: 0, Want: fuseUFFDDeterministicByte(0)},
			{Op: "write", Offset: 0, Value: 0xc1},
			{Op: "expect_file_byte", Offset: 0, Want: fuseUFFDDeterministicByte(0)},
		},
	})
	requireFuseUFFDClientResult(t, result)
}

func TestFUSEUFFDClientHelperProcess(t *testing.T) {
	if os.Getenv(fuseUFFDHelperEnv) != "1" {
		return
	}
	var script fuseUFFDClientScript
	if err := json.NewDecoder(os.Stdin).Decode(&script); err != nil {
		_ = json.NewEncoder(os.Stdout).Encode(fuseUFFDClientResult{Error: fmt.Sprintf("decode helper script: %s", err)})
		return
	}
	_ = json.NewEncoder(os.Stdout).Encode(runFuseUFFDClientScript(script))
}

func setUpWithUFFD(metaURL, mp, sock string) error {
	return setUpWithUFFDOptions(metaURL, mp, sock, smartmap.Options{})
}

func setUpWithUFFDOptions(metaURL, mp, sock string, options smartmap.Options) error {
	format(metaURL)
	go mountWithUFFD(metaURL, mp, sock, options)
	if err := <-waitMountpoint(mp); err != nil {
		return err
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: sock, Net: "unix"})
		if err == nil {
			_ = conn.Close()
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("wait for smartmap socket %s: %w", sock, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func setupFuseUFFDMount(t testing.TB, tmp string) (string, string) {
	return setupFuseUFFDMountWithOptions(t, tmp, smartmap.Options{})
}

func setupFuseUFFDMountWithOptions(t testing.TB, tmp string, options smartmap.Options) (string, string) {
	t.Helper()
	metaPath := filepath.Join(tmp, "meta.db")
	metaURL := "sqlite3://" + metaPath
	mp := filepath.Join(tmp, "mp")
	sock := filepath.Join(tmp, "smartmap.sock")
	require.NoError(t, setUpWithUFFDOptions(metaURL, mp, sock, options))
	t.Cleanup(func() { umount(mp, true) })
	return mp, sock
}

func mountWithUFFD(url, mp, sock string, options smartmap.Options) {
	if err := os.MkdirAll(mp, 0777); err != nil {
		log.Fatalf("create %s: %s", mp, err)
	}
	metaConf := meta.DefaultConf()
	metaConf.MountPoint = mp
	m := meta.NewClient(url, metaConf)
	format, err := m.Load(true)
	if err != nil {
		log.Fatalf("load setting: %s", err)
	}

	chunkConf := chunk.Config{
		BlockSize:   format.BlockSize * 1024,
		Compress:    format.Compression,
		MaxUpload:   20,
		MaxDownload: 200,
		BufferSize:  300 << 20,
		CacheSize:   1024,
		CacheDir:    "memory",
	}
	blob, err := object.CreateStorage(strings.ToLower(format.Storage), format.Bucket, format.AccessKey, format.SecretKey, format.SessionToken)
	if err != nil {
		log.Fatalf("object storage: %s", err)
	}
	blob = object.WithPrefix(blob, format.Name+"/")
	store := chunk.NewCachedStore(blob, chunkConf, nil)
	m.OnMsg(meta.CompactChunk, meta.MsgCallback(func(args ...interface{}) error {
		slices := args[0].([]meta.Slice)
		sliceID := args[1].(uint64)
		return vfs.Compact(chunkConf, store, slices, sliceID, 0)
	}))

	conf := &vfs.Config{
		Meta:     metaConf,
		Format:   *format,
		Chunk:    &chunkConf,
		FuseOpts: &vfs.FuseOptions{},
	}
	if err = m.NewSession(true); err != nil {
		log.Fatalf("new session: %s", err)
	}
	conf.AttrTimeout = time.Second
	conf.EntryTimeout = time.Second
	conf.DirEntryTimeout = time.Second
	conf.HideInternal = true
	jfs := vfs.NewVFS(conf, m, store, nil, nil)
	stopUFFD, err := smartmap.StartWithOptions(jfs, sock, options)
	if err != nil {
		log.Fatalf("smartmap socket: %s", err)
	}
	defer stopUFFD()
	if err = Serve(jfs, "", true, true); err != nil {
		log.Fatalf("fuse server err: %s\n", err)
	}
	_ = m.CloseSession()
}

func writeFuseUFFDFile(t testing.TB, path string, size uint64) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	require.NoError(t, err)
	defer file.Close()
	page := make([]byte, fuseUFFDHugePageSize)
	for off := uint64(0); off < size; off += uint64(len(page)) {
		for i := range page {
			page[i] = fuseUFFDDeterministicByte(int(off) + i)
		}
		_, err = file.WriteAt(page, int64(off))
		require.NoError(t, err)
	}
	require.NoError(t, file.Sync())
}

func cloneFuseFile(t testing.TB, mp, srcName, dstName string) {
	t.Helper()
	srcPath := filepath.Join(mp, srcName)
	dstPath := filepath.Join(mp, dstName)
	srcIno, err := utils.GetFileInode(srcPath)
	require.NoError(t, err)
	srcParentIno, err := utils.GetFileInode(filepath.Dir(srcPath))
	require.NoError(t, err)
	dstParentIno, err := utils.GetFileInode(filepath.Dir(dstPath))
	require.NoError(t, err)

	control, err := openFuseController(filepath.Dir(dstPath))
	require.NoError(t, err)
	defer control.Close()

	contentSize := uint32(8 + 8 + 8 + 1 + len(filepath.Base(dstPath)) + 2 + 1 + 1)
	wb := utils.NewBuffer(8 + contentSize)
	wb.Put32(meta.Clone)
	wb.Put32(contentSize)
	wb.Put64(srcIno)
	wb.Put64(srcParentIno)
	wb.Put64(dstParentIno)
	wb.Put8(uint8(len(filepath.Base(dstPath))))
	wb.Put([]byte(filepath.Base(dstPath)))
	wb.Put16(022)
	wb.Put8(0)
	wb.Put8(1)
	_, err = control.Write(wb.Bytes())
	require.NoError(t, err)
	require.Equal(t, syscall.Errno(0), readFuseControlStatus(t, control))
}

func openFuseController(dir string) (*os.File, error) {
	control, err := os.OpenFile(filepath.Join(dir, ".jfs.control"), os.O_RDWR, 0)
	if os.IsNotExist(err) {
		control, err = os.OpenFile(filepath.Join(dir, ".control"), os.O_RDWR, 0)
	}
	return control, err
}

func readFuseControlStatus(t testing.TB, control *os.File) syscall.Errno {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	resp := make([]byte, 2<<16)
	for {
		n, err := control.Read(resp)
		if err == nil {
			for off := 0; off < n; {
				if off+1 == n {
					return syscall.Errno(resp[off])
				}
				if off+17 <= n && resp[off] == meta.CPROGRESS {
					off += 17
					continue
				}
				if off+5 <= n && resp[off] == meta.CDATA {
					size := int(binary.BigEndian.Uint32(resp[off+1 : off+5]))
					off += 5 + size
					if off <= n {
						continue
					}
				}
				t.Fatalf("bad control response at offset %d of %d: %v", off, n, resp[:n])
			}
			continue
		}
		if !errors.Is(err, io.EOF) {
			t.Fatalf("read control response: %s", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out reading control response")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func requireFuseFileByte(t testing.TB, path string, off uint64, want byte) {
	t.Helper()
	got, err := readFileByte(path, off)
	require.NoError(t, err)
	require.Equal(t, want, got, "byte at offset %d", off)
}

func readFileByte(path string, off uint64) (byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	var got [1]byte
	_, err = file.ReadAt(got[:], int64(off))
	if err != nil {
		return 0, err
	}
	return got[0], nil
}

func fuseUFFDDeterministicByte(i int) byte {
	return byte((i*31 + 7) % 251)
}

func requireFuseUFFDClientResult(t testing.TB, result fuseUFFDClientResult) {
	t.Helper()
	if result.Skip != "" {
		t.Skip(result.Skip)
	}
	require.True(t, result.OK, "helper error: %s", result.Error)
}

func runFuseUFFDClient(t testing.TB, script fuseUFFDClientScript) fuseUFFDClientResult {
	t.Helper()
	payload, err := json.Marshal(script)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFUSEUFFDClientHelperProcess$")
	cmd.Env = append(os.Environ(), fuseUFFDHelperEnv+"=1")
	cmd.Stdin = bytes.NewReader(payload)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Start())
	err = cmd.Wait()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("uffd fuse helper timed out\nstdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
	}
	require.NoError(t, err, "stdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
	var result fuseUFFDClientResult
	require.NoError(t, json.NewDecoder(bytes.NewReader(stdout.Bytes())).Decode(&result), "stdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
	return result
}

func runFuseUFFDClientScript(script fuseUFFDClientScript) (result fuseUFFDClientResult) {
	defer func() {
		if r := recover(); r != nil {
			result = fuseUFFDClientResult{Error: fmt.Sprintf("helper panic: %v", r)}
		}
	}()
	state := &fuseUFFDClientState{
		writeback: script.WritebackPath,
		alternate: script.AlternatePath,
		evictions: make(chan fuseUFFDControlMessage, 8),
	}
	handler := &fuseRustSmartmapControlHandler{state: state}
	client, err := rustclient.Open(script.Sock, script.Path, script.Size, 0, handler)
	if err != nil {
		if skip := fuseUFFDHugeTLBUnavailableReason(err); skip != "" {
			return fuseUFFDClientResult{Skip: skip}
		}
		return fuseUFFDClientResult{Error: err.Error()}
	}
	defer client.Close()
	state.client = client
	state.mapped = client.Mapped()
	state.baseAddr = client.BaseAddr()
	go client.ServeControls()
	stopPeriodicFlush := state.startPeriodicFlush(time.Duration(script.FlushIntervalMillis) * time.Millisecond)
	if err := state.runActions(script.Actions); err != nil {
		_ = stopPeriodicFlush()
		return fuseUFFDClientResult{Error: err.Error()}
	}
	if err := stopPeriodicFlush(); err != nil {
		return fuseUFFDClientResult{Error: err.Error()}
	}
	return fuseUFFDClientResult{OK: true}
}

func openFuseUFFDMemoryForTest(t testing.TB, sock, path string, size uint64) fuseUFFDOpenedMemory {
	t.Helper()
	client, err := rustclient.Open(sock, path, size, 0, nil)
	if skip := fuseUFFDHugeTLBUnavailableReason(err); skip != "" {
		t.Skip(skip)
	}
	require.NoError(t, err)
	return fuseUFFDOpenedMemory{client: client, extents: client.Extents()}
}

func closeFuseUFFDMemoryForTest(t testing.TB, _ string, opened fuseUFFDOpenedMemory) {
	t.Helper()
	opened.client.Close()
}

func fuseUFFDShmOffsetForFileOffset(t testing.TB, extents []rustclient.Extent, off uint64) uint64 {
	t.Helper()
	for _, extent := range extents {
		if off >= extent.FileOffset && off < extent.FileOffset+extent.Length {
			return extent.ShmOffset + off - extent.FileOffset
		}
	}
	t.Fatalf("file offset %d is outside extents %#v", off, extents)
	return 0
}

func fuseUFFDHugeTLBUnavailableReason(err error) string {
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

func (h *fuseRustSmartmapControlHandler) Release(ranges []rustclient.ControlRange) error {
	msg, err := h.controlMessage(ranges)
	if err != nil {
		return err
	}
	select {
	case h.state.evictions <- msg:
	default:
	}
	return nil
}

func (h *fuseRustSmartmapControlHandler) Probe(ranges []rustclient.ControlRange) error {
	_, err := h.controlMessage(ranges)
	return err
}

func (h *fuseRustSmartmapControlHandler) WriteFault(ranges []rustclient.ControlRange) error {
	h.state.mu.Lock()
	defer h.state.mu.Unlock()
	for h.state.syncing {
		allSynced := true
		for _, r := range ranges {
			if !h.state.synced[r.FileOffset] {
				allSynced = false
				break
			}
		}
		if allSynced {
			return nil
		}
		h.state.cond.Wait()
	}
	return nil
}

func (h *fuseRustSmartmapControlHandler) controlMessage(ranges []rustclient.ControlRange) (fuseUFFDControlMessage, error) {
	msg := fuseUFFDControlMessage{Ranges: make([]fuseUFFDControlRange, 0, len(ranges))}
	for _, r := range ranges {
		if r.Length != fuseUFFDHugePageSize {
			return msg, fmt.Errorf("control length %d, want %d", r.Length, fuseUFFDHugePageSize)
		}
		msg.Ranges = append(msg.Ranges, fuseUFFDControlRange{
			FileOffset: r.FileOffset,
			Length:     r.Length,
			ShmOffset:  r.ShmOffset,
		})
	}
	return msg, nil
}

func (h *fuseRustSmartmapControlHandler) flushRanges(ranges []rustclient.ControlRange) (fuseUFFDControlMessage, error) {
	return h.flushRangesWithDrop(ranges, true)
}

func (h *fuseRustSmartmapControlHandler) flushRangesWithDrop(ranges []rustclient.ControlRange, drop bool) (fuseUFFDControlMessage, error) {
	msg, err := h.controlMessage(ranges)
	if err != nil {
		return msg, err
	}
	for _, r := range msg.Ranges {
		if err := h.state.flushDirty(r.FileOffset, r.FileOffset+r.Length, drop); err != nil {
			return msg, err
		}
	}
	return msg, nil
}

func (s *fuseUFFDClientState) startPeriodicFlush(interval time.Duration) func() error {
	if interval <= 0 {
		return func() error { return nil }
	}
	done := make(chan struct{})
	exited := make(chan struct{})
	errs := make(chan error, 1)
	go func() {
		defer close(exited)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := s.syncDirty(); err != nil {
					select {
					case errs <- err:
					default:
					}
					return
				}
			case <-done:
				return
			}
		}
	}()
	return func() error {
		close(done)
		<-exited
		select {
		case err := <-errs:
			return err
		default:
			return nil
		}
	}
}

func (s *fuseUFFDClientState) runActions(actions []fuseUFFDClientAction) error {
	for _, action := range actions {
		switch action.Op {
		case "read":
			if action.Offset >= uint64(len(s.mapped)) {
				return fmt.Errorf("read offset %d is outside mapping length %d", action.Offset, len(s.mapped))
			}
			if got := s.mapped[int(action.Offset)]; got != action.Want {
				return fmt.Errorf("read byte at %d: got %d want %d", action.Offset, got, action.Want)
			}
		case "write":
			if action.Offset >= uint64(len(s.mapped)) {
				return fmt.Errorf("write offset %d is outside mapping length %d", action.Offset, len(s.mapped))
			}
			s.mapped[int(action.Offset)] = action.Value
		case "expect_eviction":
			if err := s.expectEviction(action.Offset); err != nil {
				return err
			}
		case "sync":
			if err := s.syncDirty(); err != nil {
				return err
			}
		case "sync_with_resume_write":
			if err := s.syncWithResumeWrite(action.Offset, action.Value); err != nil {
				return err
			}
		case "sync_expect_error":
			if err := s.syncPath(s.alternate); err == nil {
				return errors.New("sync unexpectedly succeeded")
			}
		case "expect_pause_count":
			if got := s.pauseCount(); got != action.Count {
				return fmt.Errorf("pause count got %d want %d", got, action.Count)
			}
		case "expect_file_byte":
			p := s.writeback
			if action.UseAlternate {
				p = s.alternate
			}
			got, err := readFileByte(p, action.Offset)
			if err != nil {
				return err
			}
			if got != action.Want {
				return fmt.Errorf("file byte at %d got %d want %d", action.Offset, got, action.Want)
			}
		case "sleep":
			time.Sleep(time.Duration(action.Milliseconds) * time.Millisecond)
		default:
			return fmt.Errorf("unknown action %q", action.Op)
		}
	}
	return nil
}

func (s *fuseUFFDClientState) syncDirty() error {
	return s.syncPath(s.writeback)
}

func (s *fuseUFFDClientState) syncWithResumeWrite(off uint64, value byte) error {
	resume := &fuseUFFDResumeWrite{offset: off, value: value, done: make(chan error, 1)}
	s.mu.Lock()
	s.resume = resume
	s.mu.Unlock()
	err := s.syncDirty()
	s.mu.Lock()
	if s.resume == resume {
		s.resume = nil
	}
	s.mu.Unlock()
	select {
	case writeErr := <-resume.done:
		if err != nil {
			return err
		}
		return writeErr
	case <-time.After(5 * time.Second):
		if err != nil {
			return err
		}
		return errors.New("timed out waiting for resume write")
	}
}

func (s *fuseUFFDClientState) syncPath(path string) error {
	if s.client == nil {
		return errors.New("smartmap client is missing")
	}
	s.mu.Lock()
	s.syncing = true
	s.synced = make(map[uint64]bool)
	if s.cond == nil {
		s.cond = sync.NewCond(&s.mu)
	}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.syncing = false
		s.cond.Broadcast()
		s.mu.Unlock()
	}()
	return s.client.Sync(path, s)
}

func (s *fuseUFFDClientState) PauseMutator() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pauses++
	return nil
}

func (s *fuseUFFDClientState) ResumeMutator() error {
	s.mu.Lock()
	s.resumes++
	resume := s.resume
	s.resume = nil
	mapped := s.mapped
	s.mu.Unlock()
	if resume != nil {
		go func() {
			idx, err := checkedFuseMappedIndex(mapped, resume.offset)
			if err == nil {
				mapped[idx] = resume.value
			}
			resume.done <- err
		}()
	}
	return nil
}

func (s *fuseUFFDClientState) PageSynced(offset uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.synced != nil {
		s.synced[offset] = true
	}
	if s.cond != nil {
		s.cond.Broadcast()
	}
	return nil
}

func checkedFuseMappedIndex(mapped []byte, off uint64) (int, error) {
	if off >= uint64(len(mapped)) {
		return 0, fmt.Errorf("mapped offset %d is outside range length %d", off, len(mapped))
	}
	return int(off), nil
}

func (s *fuseUFFDClientState) pauseCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pauses != s.resumes {
		return -1
	}
	return s.pauses
}

func (s *fuseUFFDClientState) expectEviction(off uint64) error {
	select {
	case msg := <-s.evictions:
		if len(msg.Ranges) == 0 {
			return errors.New("eviction control had no ranges")
		}
		if msg.Ranges[0].FileOffset != off {
			return fmt.Errorf("evicted offset %d, want %d", msg.Ranges[0].FileOffset, off)
		}
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timed out waiting for eviction at %d", off)
	}
}

func (s *fuseUFFDClientState) flushDirty(start, end uint64, drop bool) error {
	if start%fuseUFFDHugePageSize != 0 || end%fuseUFFDHugePageSize != 0 || end > uint64(len(s.mapped)) || start > end {
		return fmt.Errorf("invalid dirty range [%d, %d)", start, end)
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
	for off := start; off < end; off += fuseUFFDHugePageSize {
		dirty, err := fuseUFFDPageDirty(pagemap, s.baseAddr+uintptr(off))
		if err != nil {
			return err
		}
		if !dirty {
			continue
		}
		if s.writeback == "" {
			return errors.New("dirty writeback path is empty")
		}
		if file == nil {
			file, err = os.OpenFile(s.writeback, os.O_RDWR, 0)
			if err != nil {
				return fmt.Errorf("open dirty writeback file: %w", err)
			}
		}
		page := s.mapped[int(off):int(off+fuseUFFDHugePageSize)]
		if _, err = file.WriteAt(page, int64(off)); err != nil {
			return fmt.Errorf("write dirty page at %d: %w", off, err)
		}
		if err = fuseUFFDWriteProtect(s.uffdFD, s.baseAddr+uintptr(off), true); err != nil {
			return fmt.Errorf("re-protect dirty page at %d: %w", off, err)
		}
	}
	if file != nil {
		if err = file.Sync(); err != nil {
			return fmt.Errorf("sync dirty writeback file: %w", err)
		}
	}
	if drop && end > start {
		if err = unix.Madvise(s.mapped[int(start):int(end)], unix.MADV_DONTNEED); err != nil {
			return fmt.Errorf("madvise dropped range [%d, %d): %w", start, end, err)
		}
	}
	return nil
}

func fuseUFFDPageDirty(pagemap *os.File, addr uintptr) (bool, error) {
	var buf [fuseUFFDPagemapEntrySize]byte
	vpn := uint64(addr) / uint64(os.Getpagesize())
	if _, err := pagemap.ReadAt(buf[:], int64(vpn*fuseUFFDPagemapEntrySize)); err != nil {
		return false, fmt.Errorf("read pagemap at %#x: %w", addr, err)
	}
	entry := binary.LittleEndian.Uint64(buf[:])
	if entry&fuseUFFDPagemapPresentBit == 0 {
		return false, nil
	}
	return entry&fuseUFFDPagemapWPBit == 0, nil
}

func fuseUFFDWriteProtect(fd int, addr uintptr, protect bool) error {
	mode := uint64(0)
	if protect {
		mode = uint64(fuseUFFDIOWriteProtectModeWP)
	}
	wp := fuseUFFDIOWriteProtect{
		rng:  fuseUFFDIORange{start: uint64(addr & ^(uintptr(fuseUFFDHugePageSize) - 1)), len: fuseUFFDHugePageSize},
		mode: mode,
	}
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), uintptr(fuseUFFDIOWriteProtectIoctl), uintptr(unsafe.Pointer(&wp))); errno != 0 {
		return errno
	}
	return nil
}
