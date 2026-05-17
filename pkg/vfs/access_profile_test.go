package vfs

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
)

func TestAccessProfileFileRing(t *testing.T) {
	runs := []accessProfileRun{
		{id: 1, entries: []accessProfileEntry{{order: 1, typ: 'r', block: 1}}},
		{id: 2, entries: []accessProfileEntry{{order: 1, typ: 'r', block: 2}}},
		{id: 3, entries: []accessProfileEntry{{order: 1, typ: 'w', block: 3}}},
	}
	got, blockSize, err := parseAccessProfile(formatAccessProfile(4096, 3, runs))
	if err != nil {
		t.Fatal(err)
	}
	if blockSize != 4096 || len(got) != 3 {
		t.Fatalf("unexpected profile: blockSize=%d runs=%d", blockSize, len(got))
	}

	got = append(got, accessProfileRun{id: 4, entries: []accessProfileEntry{{order: 1, typ: 'r', block: 4}}})
	got = got[len(got)-3:]
	got, _, err = parseAccessProfile(formatAccessProfile(4096, 3, got))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].id != 2 || got[2].id != 4 {
		t.Fatalf("oldest run was not dropped: %+v", got)
	}
}

func TestAccessProfileBreadthFirstEntries(t *testing.T) {
	runs := []accessProfileRun{
		{id: 1, entries: []accessProfileEntry{{order: 1, typ: 'r', block: 1}, {order: 2, typ: 'r', block: 3}}},
		{id: 2, entries: []accessProfileEntry{{order: 1, typ: 'r', block: 2}, {order: 2, typ: 'r', block: 3}}},
	}
	got, duplicates := breadthFirstAccessProfileEntries(runs)
	want := []uint64{1, 2, 3}
	if duplicates != 1 {
		t.Fatalf("duplicates = %d, want 1", duplicates)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].block != want[i] {
			t.Fatalf("entry %d block = %d, want %d", i, got[i].block, want[i])
		}
	}
}

func TestAccessProfileSlicesForBlock(t *testing.T) {
	slices := []meta.Slice{
		{Id: 1, Len: 1024, Size: 1024},
		{Id: 2, Len: 1024, Size: 1024},
		{Id: 3, Len: 1024, Size: 1024},
	}
	got := accessProfileSlicesForBlock(slices, 1024, 1024)
	if len(got) != 1 || got[0].Id != 2 {
		t.Fatalf("got %+v, want slice 2", got)
	}
	got = accessProfileSlicesForBlock(slices, 512, 2048)
	if len(got) != 3 || got[0].Id != 1 || got[1].Id != 2 || got[2].Id != 3 {
		t.Fatalf("got %+v, want slices 1,2,3", got)
	}
}

func TestAccessProfileRPCRecordLifecycle(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ctx := NewLogContext(meta.Background())

	fe, fh, eno := v.Create(ctx, 1, "memfile", 0644, 0, syscall.O_RDWR)
	if eno != 0 {
		t.Fatalf("create: %s", eno)
	}
	defer v.Release(ctx, fe.Inode, fh)

	resp := v.accessProfiles.Handle(ctx, AccessProfileRequest{Op: AccessProfileRecordStart, Inode: fe.Inode, Profile: "/memfile.profile"})
	if !resp.OK || !resp.Recording {
		t.Fatalf("record-start: %+v", resp)
	}
	if eno = v.Write(ctx, fe.Inode, []byte("hello"), 0, fh); eno != 0 {
		t.Fatalf("write: %s", eno)
	}
	buf := make([]byte, 5)
	if n, eno := v.Read(ctx, fe.Inode, buf, 0, fh); eno != 0 || n != 5 {
		t.Fatalf("read: %s %d", eno, n)
	}
	resp = v.accessProfiles.Handle(ctx, AccessProfileRequest{Op: AccessProfileRecordStop, Inode: fe.Inode})
	if !resp.OK || resp.Recording {
		t.Fatalf("record-stop: %+v", resp)
	}

	target, err := v.accessProfiles.resolveProfileTarget(ctx, fe.Inode, "/memfile.profile")
	if err != nil {
		t.Fatal(err)
	}
	runs, blockSize, err := v.accessProfiles.readAccessProfileFile(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	if blockSize != uint64(v.Conf.Chunk.BlockSize) {
		t.Fatalf("block size = %d, want %d", blockSize, v.Conf.Chunk.BlockSize)
	}
	if len(runs) != 1 || len(runs[0].entries) != 1 || runs[0].entries[0].typ != 'w' {
		t.Fatalf("unexpected runs: %+v", runs)
	}
}

func TestAccessProfileRecordKeepsDefaultRunLimit(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ctx := NewLogContext(meta.Background())

	fe, fh, eno := v.Create(ctx, 1, "memfile", 0644, 0, syscall.O_RDWR)
	if eno != 0 {
		t.Fatalf("create: %s", eno)
	}
	defer v.Release(ctx, fe.Inode, fh)
	for i := 0; i < defaultAccessProfileRuns+1; i++ {
		off := uint64(i) * uint64(v.Conf.Chunk.BlockSize)
		resp := v.accessProfiles.Handle(ctx, AccessProfileRequest{Op: AccessProfileRecordStart, Inode: fe.Inode, Profile: "/memfile.profile"})
		if !resp.OK || !resp.Recording {
			t.Fatalf("record-start: %+v", resp)
		}
		if eno = v.Write(ctx, fe.Inode, []byte("hello"), off, fh); eno != 0 {
			t.Fatalf("write: %s", eno)
		}
		resp = v.accessProfiles.Handle(ctx, AccessProfileRequest{Op: AccessProfileRecordStop, Inode: fe.Inode})
		if !resp.OK || resp.Recording {
			t.Fatalf("record-stop: %+v", resp)
		}
	}

	target, err := v.accessProfiles.resolveProfileTarget(ctx, fe.Inode, "/memfile.profile")
	if err != nil {
		t.Fatal(err)
	}
	runs, _, err := v.accessProfiles.readAccessProfileFile(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != defaultAccessProfileRuns || runs[0].id != 2 {
		t.Fatalf("default run limit was not honored: %+v", runs)
	}
}

func TestAccessProfileRecordSkipsWhenProfileLocked(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ctx := NewLogContext(meta.Background())

	fe, fh, eno := v.Create(ctx, 1, "memfile", 0644, 0, syscall.O_RDWR)
	if eno != 0 {
		t.Fatalf("create: %s", eno)
	}
	defer v.Release(ctx, fe.Inode, fh)

	target, err := v.accessProfiles.resolveProfileTarget(ctx, fe.Inode, "/memfile.profile")
	if err != nil {
		t.Fatal(err)
	}
	lockIno, err := v.accessProfiles.ensureProfileFile(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	const owner = 42
	if eno = v.Meta.Flock(ctx, lockIno, owner, meta.F_WRLCK, false); eno != 0 {
		t.Fatalf("lock profile: %s", eno)
	}
	defer v.Meta.Flock(ctx, lockIno, owner, meta.F_UNLCK, false)

	resp := v.accessProfiles.Handle(ctx, AccessProfileRequest{Op: AccessProfileRecordStart, Inode: fe.Inode, Profile: "/memfile.profile"})
	if !resp.OK || resp.Status != accessProfileStatusLocked {
		t.Fatalf("record-start locked: %+v", resp)
	}
	if v.accessProfiles.recorders[fe.Inode] != nil {
		t.Fatal("recorder started despite locked profile")
	}
	if eno = v.Write(ctx, fe.Inode, []byte("hello"), 0, fh); eno != 0 {
		t.Fatalf("write: %s", eno)
	}
	resp = v.accessProfiles.Handle(ctx, AccessProfileRequest{Op: AccessProfileRecordStop, Inode: fe.Inode})
	if !resp.OK || resp.Status != accessProfileStatusNotRecording {
		t.Fatalf("record-stop: %+v", resp)
	}

	runs, _, err := v.accessProfiles.readAccessProfileFile(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 0 {
		t.Fatalf("locked recorder wrote runs: %+v", runs)
	}
}

func TestAccessProfileInternalControlStatus(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	ctx := NewLogContext(meta.Background())

	fe, fh, eno := v.Create(ctx, 1, "memfile", 0644, 0, syscall.O_RDWR)
	if eno != 0 {
		t.Fatalf("create: %s", eno)
	}
	defer v.Release(ctx, fe.Inode, fh)

	resp := runAccessProfileControl(t, v, AccessProfileRequest{Op: AccessProfileStatusOp, Inode: fe.Inode})
	if !resp.OK || resp.Recording || resp.Loading {
		t.Fatalf("status: %+v", resp)
	}
}

func runAccessProfileControl(t *testing.T, v *VFS, req AccessProfileRequest) AccessProfileResponse {
	t.Helper()
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	v.handleInternalMsg(meta.Background(), meta.AccessProfile, utils.FromBuffer(data), &out)
	respData := out.Bytes()
	if len(respData) == 1 {
		t.Fatalf("control error: %s", syscall.Errno(respData[0]))
	}
	if len(respData) < 5 || respData[0] != meta.CDATA {
		t.Fatalf("unexpected control response: %v", respData)
	}
	size := binary.BigEndian.Uint32(respData[1:5])
	if int(size)+5 != len(respData) {
		t.Fatalf("unexpected control response size: %d response=%v", size, respData)
	}
	var resp AccessProfileResponse
	if err = json.Unmarshal(respData[5:], &resp); err != nil {
		t.Fatal(err)
	}
	return resp
}
