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
	"github.com/juicedata/juicefs/pkg/vfs/uffd"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const (
	fuseUFFDHelperEnv      = "JUICEFS_FUSE_UFFD_HELPER"
	fuseUFFDHugePageSize   = 2 << 20
	fuseUFFDPayloadSize    = 64 << 10
	fuseUFFDSharedMemoryFD = 3

	fuseUFFDOpOpenMemoryFile   = "open_memory_file"
	fuseUFFDOpCloseMemoryFile  = "close_memory_file"
	fuseUFFDOpServeMemoryFault = "serve_memory_faults"
	fuseUFFDControlEvict       = "evict"
	fuseUFFDControlEvictAck    = "evict_ack"
	fuseUFFDControlProbe       = "probe"
	fuseUFFDControlProbeAck    = "probe_ack"

	fuseUFFDPagemapEntrySize  = 8
	fuseUFFDPagemapPresentBit = uint64(1) << 63
	fuseUFFDPagemapWPBit      = uint64(1) << 57

	fuseUFFDAPI = 0xAA

	fuseUFFDFeatureMissingHugetlbfs = 1 << 4
	fuseUFFDFeatureMinorHugetlbfs   = 1 << 9
	fuseUFFDFeatureWPAsync          = 1 << 15

	fuseUFFDIORegisterModeMissing  = 1 << 0
	fuseUFFDIORegisterModeWP       = 1 << 1
	fuseUFFDIORegisterModeMinor    = 1 << 2
	fuseUFFDIOWriteProtectModeWP   = 1 << 0
	fuseUFFDUnavailableSkipMessage = "userfaultfd unavailable under this kernel policy or feature set"
)

var (
	fuseUFFDIOAPIIoctl          = fuseUFFDIOWR(0x3F, unsafe.Sizeof(fuseUFFDIOAPI{}))
	fuseUFFDIORegisterIoctl     = fuseUFFDIOWR(0x00, unsafe.Sizeof(fuseUFFDIORegister{}))
	fuseUFFDIOWriteProtectIoctl = fuseUFFDIOWR(0x06, unsafe.Sizeof(fuseUFFDIOWriteProtect{}))
)

type fuseUFFDIOAPI struct {
	api      uint64
	features uint64
	ioctls   uint64
}

type fuseUFFDIORange struct {
	start uint64
	len   uint64
}

type fuseUFFDIORegister struct {
	rng    fuseUFFDIORange
	mode   uint64
	ioctls uint64
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
	Size                uint64                 `json:"size"`
	MaxResidentPages    int                    `json:"max_resident_pages"`
	FlushIntervalMillis int                    `json:"flush_interval_millis,omitempty"`
	Actions             []fuseUFFDClientAction `json:"actions"`
}

type fuseUFFDClientAction struct {
	Op           string `json:"op"`
	Offset       uint64 `json:"offset,omitempty"`
	Value        byte   `json:"value,omitempty"`
	Want         byte   `json:"want,omitempty"`
	Milliseconds int    `json:"milliseconds,omitempty"`
}

type fuseUFFDClientResult struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
	Skip  string `json:"skip,omitempty"`
}

type fuseUFFDResponse struct {
	OK       bool              `json:"ok"`
	Error    string            `json:"error,omitempty"`
	MemoryID string            `json:"memory_id,omitempty"`
	Extents  []uffd.UFFDExtent `json:"extents,omitempty"`
}

type fuseUFFDControlRange struct {
	FileOffset uint64 `json:"file_offset"`
	Length     uint64 `json:"length"`
	ShmOffset  uint64 `json:"shm_offset"`
}

type fuseUFFDControlMessage struct {
	Type      string                 `json:"type"`
	RequestID uint64                 `json:"request_id"`
	MemoryID  string                 `json:"memory_id,omitempty"`
	Ranges    []fuseUFFDControlRange `json:"ranges,omitempty"`
}

type fuseUFFDControlAck struct {
	Type      string `json:"type"`
	RequestID uint64 `json:"request_id"`
	OK        bool   `json:"ok"`
	Error     string `json:"error,omitempty"`
}

type fuseUFFDOpenedMemory struct {
	fd       int
	memoryID string
	extents  []uffd.UFFDExtent
}

type fuseUFFDClientState struct {
	mapped    []byte
	conn      *net.UnixConn
	uffdFD    int
	baseAddr  uintptr
	writeback string
	evictions chan fuseUFFDControlMessage
}

func TestUFFDPrivatePeriodicWritebackThroughMountedFuse(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real FUSE/userfaultfd integration test in short mode")
	}

	tmp := t.TempDir()
	metaPath := filepath.Join(tmp, "meta.db")
	metaURL := "sqlite3://" + metaPath
	mp := filepath.Join(tmp, "mp")
	sock := filepath.Join(tmp, "uffd.sock")
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

func TestUFFDEvictionFlushesDirtyPrivatePageBeforeDropThroughMountedFuse(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real FUSE/userfaultfd integration test in short mode")
	}

	tmp := t.TempDir()
	mp, sock := setupFuseUFFDMount(t, tmp)
	const memorySize = 2 * fuseUFFDHugePageSize
	memoryPath := filepath.Join(mp, "vm-memory")
	writeFuseUFFDFile(t, memoryPath, memorySize)

	const privateByte = 0xc7
	result := runFuseUFFDClient(t, fuseUFFDClientScript{
		Sock:             sock,
		Path:             "/vm-memory",
		WritebackPath:    memoryPath,
		Size:             memorySize,
		MaxResidentPages: 1,
		Actions: []fuseUFFDClientAction{
			{Op: "read", Offset: 17, Want: fuseUFFDDeterministicByte(17)},
			{Op: "write", Offset: 0, Value: privateByte},
			{Op: "read", Offset: 0, Want: privateByte},
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
	format(metaURL)
	go mountWithUFFD(metaURL, mp, sock)
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
			return fmt.Errorf("wait for uffd socket %s: %w", sock, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func setupFuseUFFDMount(t testing.TB, tmp string) (string, string) {
	t.Helper()
	metaPath := filepath.Join(tmp, "meta.db")
	metaURL := "sqlite3://" + metaPath
	mp := filepath.Join(tmp, "mp")
	sock := filepath.Join(tmp, "uffd.sock")
	require.NoError(t, setUpWithUFFD(metaURL, mp, sock))
	t.Cleanup(func() { umount(mp, true) })
	return mp, sock
}

func mountWithUFFD(url, mp, sock string) {
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
	stopUFFD, err := uffd.Start(jfs, sock)
	if err != nil {
		log.Fatalf("uffd socket: %s", err)
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
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	var got [1]byte
	_, err = file.ReadAt(got[:], int64(off))
	require.NoError(t, err)
	require.Equal(t, want, got[0], "byte at offset %d", off)
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
	opened, skip, err := fuseUFFDOpenMemory(script.Sock, script.Path, script.Size)
	if skip != "" {
		return fuseUFFDClientResult{Skip: skip}
	}
	if err != nil {
		return fuseUFFDClientResult{Error: err.Error()}
	}
	defer unix.Close(opened.fd)
	defer func() { _ = fuseUFFDCloseMemory(script.Sock, opened.memoryID) }()

	mapped, cleanup, skip, err := fuseUFFDMapPrivate(opened.fd, script.Size, opened.extents)
	if skip != "" {
		return fuseUFFDClientResult{Skip: skip}
	}
	if err != nil {
		return fuseUFFDClientResult{Error: err.Error()}
	}
	defer cleanup()

	uffdFD, closeUFFD, skip, err := fuseUFFDRegister(mapped)
	if skip != "" {
		return fuseUFFDClientResult{Skip: skip}
	}
	if err != nil {
		return fuseUFFDClientResult{Error: err.Error()}
	}
	defer closeUFFD()

	conn, err := fuseUFFDServeMemory(script.Sock, uffdFD, opened.memoryID, script.MaxResidentPages, mapped)
	if err != nil {
		return fuseUFFDClientResult{Error: err.Error()}
	}
	defer conn.Close()

	state := &fuseUFFDClientState{
		mapped:    mapped,
		conn:      conn,
		uffdFD:    uffdFD,
		baseAddr:  uintptr(unsafe.Pointer(&mapped[0])),
		writeback: script.WritebackPath,
		evictions: make(chan fuseUFFDControlMessage, 8),
	}
	go state.serveControls()
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

func fuseUFFDOpenMemory(sock, path string, size uint64) (fuseUFFDOpenedMemory, string, error) {
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: sock, Net: "unix"})
	if err != nil {
		return fuseUFFDOpenedMemory{}, "", err
	}
	defer conn.Close()
	req := uffd.UFFDRequest{Op: fuseUFFDOpOpenMemoryFile, Path: path, Size: size}
	payload, err := json.Marshal(req)
	if err != nil {
		return fuseUFFDOpenedMemory{}, "", err
	}
	if _, _, err = conn.WriteMsgUnix(payload, nil, nil); err != nil {
		return fuseUFFDOpenedMemory{}, "", err
	}
	resp, fds, err := fuseUFFDReadResponseWithFD(conn)
	if err != nil {
		return fuseUFFDOpenedMemory{}, "", err
	}
	if !resp.OK {
		closeFuseUFFDFDs(fds)
		return fuseUFFDOpenedMemory{}, fuseUFFDHugeTLBUnavailableReason(errors.New(resp.Error)), errors.New(resp.Error)
	}
	if len(fds) != 1 {
		closeFuseUFFDFDs(fds)
		return fuseUFFDOpenedMemory{}, "", fmt.Errorf("open_memory_file returned %d fds, want 1", len(fds))
	}
	return fuseUFFDOpenedMemory{fd: fds[0], memoryID: resp.MemoryID, extents: resp.Extents}, "", nil
}

func openFuseUFFDMemoryForTest(t testing.TB, sock, path string, size uint64) fuseUFFDOpenedMemory {
	t.Helper()
	opened, skip, err := fuseUFFDOpenMemory(sock, path, size)
	if skip != "" {
		t.Skip(skip)
	}
	require.NoError(t, err)
	return opened
}

func closeFuseUFFDMemoryForTest(t testing.TB, sock string, opened fuseUFFDOpenedMemory) {
	t.Helper()
	require.NoError(t, fuseUFFDCloseMemory(sock, opened.memoryID))
	require.NoError(t, unix.Close(opened.fd))
}

func fuseUFFDShmOffsetForFileOffset(t testing.TB, extents []uffd.UFFDExtent, off uint64) uint64 {
	t.Helper()
	for _, extent := range extents {
		if off >= extent.FileOffset && off < extent.FileOffset+extent.Length {
			return extent.ShmOffset + off - extent.FileOffset
		}
	}
	t.Fatalf("file offset %d is outside extents %#v", off, extents)
	return 0
}

func fuseUFFDCloseMemory(sock, memoryID string) error {
	var lastErr error
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := fuseUFFDCloseMemoryOnce(sock, memoryID)
		if err == nil {
			return nil
		}
		lastErr = err
		if !strings.Contains(err.Error(), "active") || time.Now().After(deadline) {
			return lastErr
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func fuseUFFDCloseMemoryOnce(sock, memoryID string) error {
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: sock, Net: "unix"})
	if err != nil {
		return err
	}
	defer conn.Close()
	req := uffd.UFFDRequest{Op: fuseUFFDOpCloseMemoryFile, MemoryID: memoryID}
	payload, err := json.Marshal(req)
	if err != nil {
		return err
	}
	if _, _, err = conn.WriteMsgUnix(payload, nil, nil); err != nil {
		return err
	}
	var resp fuseUFFDResponse
	if err = json.NewDecoder(conn).Decode(&resp); err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}
	return nil
}

func fuseUFFDServeMemory(sock string, uffdFD int, memoryID string, maxResidentPages int, mapped []byte) (*net.UnixConn, error) {
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: sock, Net: "unix"})
	if err != nil {
		return nil, err
	}
	req := uffd.UFFDRequest{
		Op:               fuseUFFDOpServeMemoryFault,
		MemoryID:         memoryID,
		MaxResidentPages: maxResidentPages,
		Mappings: []uffd.UFFDRegion{{
			BaseHostVirtAddr: uintptr(unsafe.Pointer(&mapped[0])),
			Size:             uintptr(len(mapped)),
			Offset:           0,
			PageSize:         fuseUFFDHugePageSize,
		}},
	}
	payload, err := json.Marshal(req)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if _, _, err = conn.WriteMsgUnix(payload, syscall.UnixRights(uffdFD), nil); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func fuseUFFDReadResponseWithFD(conn *net.UnixConn) (fuseUFFDResponse, []int, error) {
	msg := make([]byte, fuseUFFDPayloadSize)
	oob := make([]byte, syscall.CmsgSpace(4))
	n, oobn, flags, _, err := conn.ReadMsgUnix(msg, oob)
	if err != nil {
		return fuseUFFDResponse{}, nil, err
	}
	if flags&unix.MSG_CTRUNC != 0 || flags&unix.MSG_TRUNC != 0 {
		return fuseUFFDResponse{}, nil, errors.New("truncated uffd response")
	}
	var resp fuseUFFDResponse
	if err = json.Unmarshal(msg[:n], &resp); err != nil {
		return fuseUFFDResponse{}, nil, err
	}
	msgs, err := syscall.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return fuseUFFDResponse{}, nil, err
	}
	var fds []int
	for i := range msgs {
		rights, err := syscall.ParseUnixRights(&msgs[i])
		if err != nil {
			closeFuseUFFDFDs(fds)
			return fuseUFFDResponse{}, nil, err
		}
		fds = append(fds, rights...)
	}
	return resp, fds, nil
}

func closeFuseUFFDFDs(fds []int) {
	for _, fd := range fds {
		_ = unix.Close(fd)
	}
}

func fuseUFFDMapPrivate(fd int, size uint64, extents []uffd.UFFDExtent) ([]byte, func(), string, error) {
	reserve, err := unix.Mmap(-1, 0, int(size+fuseUFFDHugePageSize), unix.PROT_NONE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS)
	if err != nil {
		return nil, nil, fuseUFFDHugeTLBUnavailableReason(err), err
	}
	reserveBase := uintptr(unsafe.Pointer(&reserve[0]))
	base := (reserveBase + fuseUFFDHugePageSize - 1) & ^(uintptr(fuseUFFDHugePageSize) - 1)
	for _, extent := range extents {
		addr := base + uintptr(extent.FileOffset)
		_, _, errno := syscall.Syscall6(syscall.SYS_MMAP, addr, uintptr(extent.Length), uintptr(unix.PROT_READ|unix.PROT_WRITE), uintptr(unix.MAP_PRIVATE|unix.MAP_FIXED), uintptr(fd), uintptr(extent.ShmOffset))
		if errno != 0 {
			_ = unix.Munmap(reserve)
			return nil, nil, fuseUFFDHugeTLBUnavailableReason(errno), fmt.Errorf("mmap hugetlb private extent: %w", errno)
		}
	}
	mapped := unsafe.Slice((*byte)(unsafe.Pointer(base)), int(size))
	return mapped, func() { _ = unix.Munmap(reserve) }, "", nil
}

func fuseUFFDRegister(mapped []byte) (int, func() error, string, error) {
	fd, _, errno := syscall.Syscall(uintptr(unix.SYS_USERFAULTFD), uintptr(unix.O_CLOEXEC|unix.O_NONBLOCK), 0, 0)
	if errno != 0 {
		return -1, nil, fuseUFFDUnavailableReason(errno), fmt.Errorf("userfaultfd syscall: %w", errno)
	}
	api := fuseUFFDIOAPI{api: fuseUFFDAPI, features: fuseUFFDFeatureMissingHugetlbfs | fuseUFFDFeatureMinorHugetlbfs | fuseUFFDFeatureWPAsync}
	if _, _, errno = syscall.Syscall(syscall.SYS_IOCTL, fd, uintptr(fuseUFFDIOAPIIoctl), uintptr(unsafe.Pointer(&api))); errno != 0 {
		_ = unix.Close(int(fd))
		return -1, nil, fuseUFFDUnavailableReason(errno), fmt.Errorf("UFFDIO_API hugetlb: %w", errno)
	}
	reg := fuseUFFDIORegister{
		rng:  fuseUFFDIORange{start: uint64(uintptr(unsafe.Pointer(&mapped[0]))), len: uint64(len(mapped))},
		mode: fuseUFFDIORegisterModeMissing | fuseUFFDIORegisterModeMinor | fuseUFFDIORegisterModeWP,
	}
	if _, _, errno = syscall.Syscall(syscall.SYS_IOCTL, fd, uintptr(fuseUFFDIORegisterIoctl), uintptr(unsafe.Pointer(&reg))); errno != 0 {
		_ = unix.Close(int(fd))
		return -1, nil, fuseUFFDUnavailableReason(errno), fmt.Errorf("UFFDIO_REGISTER hugetlb: %w", errno)
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

func fuseUFFDUnavailableReason(errno syscall.Errno) string {
	if errors.Is(errno, syscall.ENOSYS) || errors.Is(errno, syscall.EPERM) || errors.Is(errno, syscall.EACCES) || errors.Is(errno, syscall.EINVAL) {
		return fmt.Sprintf("%s: %s", fuseUFFDUnavailableSkipMessage, errno)
	}
	return ""
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

func (s *fuseUFFDClientState) serveControls() {
	dec := json.NewDecoder(s.conn)
	enc := json.NewEncoder(s.conn)
	for {
		var msg fuseUFFDControlMessage
		if err := dec.Decode(&msg); err != nil {
			return
		}
		ack := fuseUFFDControlAck{Type: msg.Type + "_ack", RequestID: msg.RequestID, OK: false, Error: "unexpected control message"}
		if msg.Type == fuseUFFDControlEvict {
			ack = s.handleEvict(msg)
		} else if msg.Type == fuseUFFDControlProbe {
			ack = s.handleProbe(msg)
		}
		if err := enc.Encode(ack); err != nil {
			return
		}
		if ack.OK && msg.Type == fuseUFFDControlEvict {
			select {
			case s.evictions <- msg:
			default:
			}
		}
	}
}

func (s *fuseUFFDClientState) handleEvict(msg fuseUFFDControlMessage) fuseUFFDControlAck {
	ack := fuseUFFDControlAck{Type: fuseUFFDControlEvictAck, RequestID: msg.RequestID, OK: true}
	for _, r := range msg.Ranges {
		if r.Length != fuseUFFDHugePageSize {
			ack.OK = false
			ack.Error = fmt.Sprintf("eviction length %d, want %d", r.Length, fuseUFFDHugePageSize)
			return ack
		}
		if err := s.flushDirty(r.FileOffset, r.FileOffset+r.Length, true); err != nil {
			ack.OK = false
			ack.Error = err.Error()
			return ack
		}
	}
	return ack
}

func (s *fuseUFFDClientState) handleProbe(msg fuseUFFDControlMessage) fuseUFFDControlAck {
	ack := fuseUFFDControlAck{Type: fuseUFFDControlProbeAck, RequestID: msg.RequestID, OK: true}
	for _, r := range msg.Ranges {
		if r.Length != fuseUFFDHugePageSize {
			ack.OK = false
			ack.Error = fmt.Sprintf("probe length %d, want %d", r.Length, fuseUFFDHugePageSize)
			return ack
		}
		if err := s.flushDirty(r.FileOffset, r.FileOffset+r.Length, true); err != nil {
			ack.OK = false
			ack.Error = err.Error()
			return ack
		}
	}
	return ack
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
				if err := s.flushDirty(0, uint64(len(s.mapped)), false); err != nil {
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
		case "sleep":
			time.Sleep(time.Duration(action.Milliseconds) * time.Millisecond)
		default:
			return fmt.Errorf("unknown action %q", action.Op)
		}
	}
	return nil
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
