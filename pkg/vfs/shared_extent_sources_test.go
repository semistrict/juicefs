package vfs

import (
	"encoding/binary"
	"encoding/json"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
)

func TestSharedExtentSourcesSelectsEarliestSharedSource(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	base := createSharedExtentSourceFile(t, v, "base")
	clone1 := createSharedExtentSourceFile(t, v, "clone1")
	clone2 := createSharedExtentSourceFile(t, v, "clone2")
	writeSharedExtentSourceSlice(t, v, base, 0, 0, 16<<10)
	copySharedExtentSourceRange(t, v, base, clone1, 0, 0, 16<<10)
	copySharedExtentSourceRange(t, v, clone1, clone2, 0, 0, 16<<10)

	resp, eno := v.SharedExtentSources(meta.Background(), &SharedExtentSourcesRequest{
		Files:  []Ino{base, clone1, clone2},
		Ranges: []SharedExtentSourceRange{{Off: 0, Len: 16 << 10}},
	})
	if eno != 0 {
		t.Fatal(eno)
	}
	want := []SharedExtentSourceSpan{{Off: 0, Len: 16 << 10, SourceIndex: 0, SourceIno: base}}
	if !reflect.DeepEqual(resp.Spans, want) {
		t.Fatalf("spans = %+v, want %+v", resp.Spans, want)
	}
}

func TestSharedExtentSourcesReportsUniqueAndHoleSpans(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	base := createSharedExtentSourceFile(t, v, "base")
	target := createSharedExtentSourceFile(t, v, "target")
	writeSharedExtentSourceSlice(t, v, base, 0, 0, 16<<10)
	copySharedExtentSourceRange(t, v, base, target, 0, 0, 16<<10)
	writeSharedExtentSourceSlice(t, v, target, 4<<10, 0, 4<<10)

	resp, eno := v.SharedExtentSources(meta.Background(), &SharedExtentSourcesRequest{
		Files:  []Ino{base, target},
		Ranges: []SharedExtentSourceRange{{Off: 0, Len: 20 << 10}},
	})
	if eno != 0 {
		t.Fatal(eno)
	}
	want := []SharedExtentSourceSpan{
		{Off: 0, Len: 4 << 10, SourceIndex: 0, SourceIno: base},
		{Off: 4 << 10, Len: 4 << 10, SourceIndex: 1, SourceIno: target},
		{Off: 8 << 10, Len: 8 << 10, SourceIndex: 0, SourceIno: base},
		{Off: 16 << 10, Len: 4 << 10, SourceIndex: -1, SourceIno: 0},
	}
	if !reflect.DeepEqual(resp.Spans, want) {
		t.Fatalf("spans = %+v, want %+v", resp.Spans, want)
	}
}

func TestSharedExtentSourcesSplitsAcrossChunks(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	base := createSharedExtentSourceFile(t, v, "base")
	target := createSharedExtentSourceFile(t, v, "target")
	writeSharedExtentSourceSlice(t, v, base, meta.ChunkSize-4096, 0, 8192)
	copySharedExtentSourceRange(t, v, base, target, meta.ChunkSize-4096, meta.ChunkSize-4096, 8192)

	resp, eno := v.SharedExtentSources(meta.Background(), &SharedExtentSourcesRequest{
		Files:  []Ino{base, target},
		Ranges: []SharedExtentSourceRange{{Off: meta.ChunkSize - 4096, Len: 8192}},
	})
	if eno != 0 {
		t.Fatal(eno)
	}
	want := []SharedExtentSourceSpan{{Off: meta.ChunkSize - 4096, Len: 8192, SourceIndex: 0, SourceIno: base}}
	if !reflect.DeepEqual(resp.Spans, want) {
		t.Fatalf("spans = %+v, want %+v", resp.Spans, want)
	}
}

func TestSharedExtentSourcesInternalControl(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ctx := NewLogContext(meta.Background())
	base := createSharedExtentSourceFile(t, v, "base")
	target := createSharedExtentSourceFile(t, v, "target")
	writeSharedExtentSourceSlice(t, v, base, 0, 0, 4096)
	copySharedExtentSourceRange(t, v, base, target, 0, 0, 4096)

	fe, eno := v.Lookup(ctx, 1, ".control")
	if eno != 0 {
		t.Fatal(eno)
	}
	fe, fh, eno := v.Open(ctx, fe.Inode, syscall.O_RDWR)
	if eno != 0 {
		t.Fatal(eno)
	}
	defer v.Release(ctx, fe.Inode, fh)

	payload := encodeSharedExtentSourcesRequest([]Ino{base, target}, []SharedExtentSourceRange{{Off: 0, Len: 4096}})
	msg := utils.NewBuffer(uint32(8 + len(payload)))
	msg.Put32(meta.SharedExtentSources)
	msg.Put32(uint32(len(payload)))
	msg.Put(payload)
	if eno = v.Write(ctx, fe.Inode, msg.Bytes(), 0, fh); eno != 0 {
		t.Fatal(eno)
	}
	data, eno := readSharedExtentSourceControlData(t, v, fe.Inode, fh, uint64(len(msg.Bytes())))
	if eno != 0 {
		t.Fatal(eno)
	}
	var resp SharedExtentSourcesResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		t.Fatal(err)
	}
	want := []SharedExtentSourceSpan{{Off: 0, Len: 4096, SourceIndex: 0, SourceIno: base}}
	if !reflect.DeepEqual(resp.Spans, want) {
		t.Fatalf("spans = %+v, want %+v", resp.Spans, want)
	}
}

func encodeSharedExtentSourcesRequest(files []Ino, ranges []SharedExtentSourceRange) []byte {
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

func createSharedExtentSourceFile(t *testing.T, v *VFS, name string) Ino {
	t.Helper()
	ctx := NewLogContext(meta.Background())
	fe, fh, eno := v.Create(ctx, 1, name, 0644, 0, syscall.O_RDWR)
	if eno != 0 {
		t.Fatalf("create %s: %s", name, eno)
	}
	v.Release(ctx, fe.Inode, fh)
	return fe.Inode
}

func writeSharedExtentSourceSlice(t *testing.T, v *VFS, ino Ino, off uint64, sliceOff, length uint32) {
	t.Helper()
	var id uint64
	if eno := v.Meta.NewSlice(meta.Background(), &id); eno != 0 {
		t.Fatalf("new slice: %s", eno)
	}
	remaining := length
	fileOff := off
	objectOff := sliceOff
	for remaining > 0 {
		chunkOff := uint32(fileOff % meta.ChunkSize)
		n := min(remaining, uint32(meta.ChunkSize)-chunkOff)
		if eno := v.Meta.Write(meta.Background(), ino, uint32(fileOff/meta.ChunkSize), chunkOff, meta.Slice{Id: id, Size: sliceOff + length, Off: objectOff, Len: n}, time.Now()); eno != 0 {
			t.Fatalf("write slice: %s", eno)
		}
		remaining -= n
		fileOff += uint64(n)
		objectOff += n
	}
}

func copySharedExtentSourceRange(t *testing.T, v *VFS, src, dst Ino, offIn, offOut, size uint64) {
	t.Helper()
	var copied uint64
	if eno := v.Meta.CopyFileRange(meta.Background(), src, offIn, dst, offOut, size, 0, &copied, nil); eno != 0 {
		t.Fatalf("copy file range: %s", eno)
	}
	if copied != size {
		t.Fatalf("copied = %d, want %d", copied, size)
	}
}

func readSharedExtentSourceControlData(t *testing.T, v *VFS, ino Ino, fh uint64, off uint64) ([]byte, syscall.Errno) {
	t.Helper()
	buf := make([]byte, 1<<20)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		n, eno := v.Read(NewLogContext(meta.Background()), ino, buf, off, fh)
		if eno != 0 {
			return nil, eno
		}
		if n == 0 {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if n == 1 {
			return nil, syscall.Errno(buf[0])
		}
		if buf[0] != meta.CDATA || n < 5 {
			return nil, syscall.EIO
		}
		size := binary.BigEndian.Uint32(buf[1:5])
		if int(size)+5 > n {
			return nil, syscall.EIO
		}
		return buf[5 : 5+size], 0
	}
	return nil, syscall.ETIMEDOUT
}
