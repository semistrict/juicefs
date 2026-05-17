//go:build linux

package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/chunk"
	jfsfuse "github.com/juicedata/juicefs/pkg/fuse"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/vfs"
)

const (
	firecrackerITEnv = "JFS_FIRECRACKER_IT"
)

var firecrackerSuite struct {
	skipReason     string
	tmpDir         string
	metaURL        string
	bucket         string
	kernel         string
	rootfs         string
	fcBin          string
	memMiB         int
	workMiB        int
	snapshotSerial string
}

func TestMain(m *testing.M) {
	if os.Getenv(firecrackerITEnv) != "1" {
		firecrackerSuite.skipReason = "set JFS_FIRECRACKER_IT=1 to run Firecracker integration tests"
		os.Exit(m.Run())
	}

	code := 1
	if err := setupFirecrackerSuite(); err != nil {
		firecrackerSuite.skipReason = err.Error()
		code = m.Run()
	} else {
		code = m.Run()
	}
	cleanupFirecrackerSuite()
	os.Exit(code)
}

func TestFirecrackerMemfileAccessProfileRecordAndLoad(t *testing.T) {
	requireFirecrackerSuite(t)

	createSnapshot(t)
	recorded := runResumeTrial(t, "correctness-record", true, false, nil)
	result := runResumeTrial(t, "correctness-profiled", false, true, nil)
	t.Logf("snapshot load recording actual resume profile took %s", recorded.duration)
	t.Logf("snapshot load with access profile took %s", result.duration)
}

func TestFirecrackerMemfileAccessProfileSpeedup(t *testing.T) {
	requireFirecrackerSuite(t)
	createSnapshot(t)

	trials := envInt("JFS_FC_SPEED_TRIALS", 3)
	lead := envDuration("JFS_FC_PROFILE_LOAD_LEAD", 500*time.Millisecond)
	slow := &slowObjectConfig{
		latency:   envDuration("JFS_FC_OBJECT_LATENCY", 25*time.Millisecond),
		bandwidth: envBytesPerSecond("JFS_FC_OBJECT_BANDWIDTH", 100<<20),
		lead:      lead,
	}
	t.Logf("simulated object storage: latency=%s bandwidth=%dB/s profile_lead=%s trials=%d", slow.latency, slow.bandwidth, slow.lead, trials)

	var baseline, profiled []resumeTrialResult
	for i := 0; i < trials; i++ {
		baseline = append(baseline, runResumeTrial(t, fmt.Sprintf("baseline-%d", i), true, false, slow))
		profiled = append(profiled, runResumeTrial(t, fmt.Sprintf("profiled-%d", i), false, true, slow))
	}

	baseStats := summarizeTrials(baseline)
	profileStats := summarizeTrials(profiled)
	speedup := 0.0
	if baseStats.mean > 0 {
		speedup = (float64(baseStats.mean-profileStats.mean) / float64(baseStats.mean)) * 100
	}
	t.Logf("baseline: n=%d min=%s median=%s mean=%s gets=%d bytes=%d injected_delay=%s",
		trials, baseStats.min, baseStats.median, baseStats.mean, baseStats.gets, baseStats.bytes, baseStats.injectedDelay)
	t.Logf("profiled: n=%d min=%s median=%s mean=%s gets=%d bytes=%d injected_delay=%s",
		trials, profileStats.min, profileStats.median, profileStats.mean, profileStats.gets, profileStats.bytes, profileStats.injectedDelay)
	t.Logf("profiled speedup by mean /snapshot/load duration: %.1f%%", speedup)

	if baseStats.gets == 0 {
		t.Fatal("baseline did not issue object reads")
	}
	if profileStats.gets == 0 {
		t.Fatal("profiled run did not issue object reads")
	}
}

func setupFirecrackerSuite() error {
	if _, err := os.Stat("/dev/kvm"); err != nil {
		return fmt.Errorf("/dev/kvm is required: %w", err)
	}

	kernel := os.Getenv("JFS_FC_KERNEL")
	if kernel == "" {
		return fmt.Errorf("JFS_FC_KERNEL is required")
	}
	rootfs := os.Getenv("JFS_FC_ROOTFS")
	if rootfs == "" {
		return fmt.Errorf("JFS_FC_ROOTFS is required")
	}
	fcBin := os.Getenv("JFS_FIRECRACKER_BIN")
	if fcBin == "" {
		var err error
		fcBin, err = exec.LookPath("firecracker")
		if err != nil {
			return fmt.Errorf("firecracker binary is required: %w", err)
		}
	}

	tmpDir, err := os.MkdirTemp("", "jfs-firecracker-it-")
	if err != nil {
		return err
	}
	firecrackerSuite.tmpDir = tmpDir
	firecrackerSuite.kernel = kernel
	firecrackerSuite.fcBin = fcBin
	firecrackerSuite.memMiB = envInt("JFS_FC_MEM_MIB", 4096)
	firecrackerSuite.workMiB = envInt("JFS_FC_WORKLOAD_MIB", 3072)

	metaURL := "sqlite3://" + filepath.Join(tmpDir, "meta.db")
	bucket := filepath.Join(tmpDir, "objects") + string(filepath.Separator)
	if err := os.MkdirAll(bucket, 0o755); err != nil {
		return err
	}
	firecrackerSuite.metaURL = metaURL
	firecrackerSuite.bucket = bucket

	m := meta.NewClient(metaURL, nil)
	format := &meta.Format{
		Name:      "firecracker-it",
		UUID:      uuid.New().String(),
		Storage:   "file",
		Bucket:    bucket,
		BlockSize: 4096,
		TrashDays: 0,
	}
	if err := m.Init(format, true); err != nil {
		return fmt.Errorf("format juicefs: %w", err)
	}
	workloadRootfs, err := prepareWorkloadRootfs(tmpDir, rootfs, firecrackerSuite.workMiB)
	if err != nil {
		return err
	}
	firecrackerSuite.rootfs = workloadRootfs
	return nil
}

func cleanupFirecrackerSuite() {
	if firecrackerSuite.tmpDir != "" {
		_ = os.RemoveAll(firecrackerSuite.tmpDir)
	}
}

func prepareWorkloadRootfs(tmpDir, source string, workMiB int) (string, error) {
	dst := filepath.Join(tmpDir, "workload-rootfs.ext4")
	in, err := os.Open(source)
	if err != nil {
		return "", fmt.Errorf("open rootfs: %w", err)
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return "", fmt.Errorf("create workload rootfs: %w", err)
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return "", fmt.Errorf("copy workload rootfs: %w", err)
	}
	if err := out.Close(); err != nil {
		return "", fmt.Errorf("close workload rootfs: %w", err)
	}

	script := filepath.Join(tmpDir, "jfs-workload.sh")
	if err := os.WriteFile(script, []byte(fmt.Sprintf(`#!/bin/sh
echo JFS_WORKLOAD_START >/dev/ttyS0
JFS_WORKLOAD_MIB=%d python3 - <<'PY'
import mmap
import os
import time

mib = int(os.environ.get("JFS_WORKLOAD_MIB", "3072"))
stride = 2 * 1024 * 1024
buf = mmap.mmap(-1, mib * 1024 * 1024)
blocks = len(buf) // stride
for round_no in range(2):
    value = round_no & 255
    for off in range(0, len(buf), stride):
        buf[off:off+4096] = bytes([(value + (off // stride)) & 255]) * 4096
with open("/dev/ttyS0", "w") as serial:
    serial.write("JFS_WORKLOAD_READY\n")
    serial.flush()
    time.sleep(0.2)
    serial.write("JFS_WORKLOAD_SCAN_START\n")
    serial.flush()
    checksum = 0
    # Prime-step walk over the whole touched working set. This is deterministic
    # and broad enough that resume has to fault meaningful guest memory back in.
    for i in range(blocks):
        block = (i * 977) %% blocks
        off = block * stride
        page = buf[off:off+4096]
        checksum = (checksum + page[0] + page[2048] + page[4095]) & 0xffffffff
        buf[off:off+4096] = bytes([(checksum + block) & 255]) * 4096
    serial.write("JFS_WORKLOAD_DONE %%08x\n" %% checksum)
    serial.flush()
    tick = 0
    while True:
        for off in range(0, len(buf), stride * 16):
            buf[off:off+4096] = bytes([(tick + (off // stride)) & 255]) * 4096
        tick += 1
        serial.write("JFS_WORKLOAD_TICK %%d\n" %% tick)
        serial.flush()
        time.sleep(0.2)
PY
`, workMiB)), 0o755); err != nil {
		return "", fmt.Errorf("write workload script: %w", err)
	}
	service := filepath.Join(tmpDir, "jfs-workload.service")
	if err := os.WriteFile(service, []byte(`[Unit]
Description=JuiceFS Firecracker memory workload
After=multi-user.target

[Service]
Type=simple
ExecStart=/usr/local/bin/jfs-workload.sh

[Install]
WantedBy=multi-user.target
`), 0o644); err != nil {
		return "", fmt.Errorf("write workload service: %w", err)
	}

	for _, cmd := range [][]string{
		{"write", script, "/usr/local/bin/jfs-workload.sh"},
		{"write", service, "/etc/systemd/system/jfs-workload.service"},
	} {
		args := append([]string{"-w", "-R", strings.Join(cmd, " "), dst})
		if out, err := exec.Command("debugfs", args...).CombinedOutput(); err != nil {
			return "", fmt.Errorf("debugfs %q: %w\n%s", strings.Join(cmd, " "), err, out)
		}
	}
	for _, command := range []string{
		"rm /etc/systemd/system/multi-user.target.wants/jfs-workload.service",
		"symlink /etc/systemd/system/multi-user.target.wants/jfs-workload.service ../jfs-workload.service",
	} {
		args := []string{"-w", "-R", command, dst}
		if out, err := exec.Command("debugfs", args...).CombinedOutput(); err != nil && !strings.Contains(command, "rm ") {
			return "", fmt.Errorf("debugfs %q: %w\n%s", command, err, out)
		}
	}
	return dst, nil
}

func requireFirecrackerSuite(t *testing.T) {
	t.Helper()
	if firecrackerSuite.skipReason != "" {
		t.Skip(firecrackerSuite.skipReason)
	}
}

type mountedFirecrackerVolume struct {
	mountPoint string
	meta       meta.Meta
	slow       *slowObjectStorage
}

type slowObjectConfig struct {
	latency   time.Duration
	bandwidth int64
	lead      time.Duration
}

type slowObjectStorage struct {
	object.ObjectStorage
	latency       time.Duration
	bandwidth     int64
	gets          atomic.Uint64
	bytes         atomic.Uint64
	injectedDelay atomic.Int64
}

func (s *slowObjectStorage) Get(ctx context.Context, key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	size := limit
	if size < 0 {
		size = 0
		if obj, err := s.ObjectStorage.Head(ctx, key); err == nil && obj.Size() > off {
			size = obj.Size() - off
		}
	}
	if size < 0 {
		size = 0
	}
	delay := s.latency
	if s.bandwidth > 0 && size > 0 {
		delay += time.Duration((int64(time.Second) * size) / s.bandwidth)
	}
	if delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	s.gets.Add(1)
	s.bytes.Add(uint64(size))
	s.injectedDelay.Add(int64(delay))
	return s.ObjectStorage.Get(ctx, key, off, limit, getters...)
}

func mountFirecrackerVolume(t *testing.T, name string, slow *slowObjectConfig) *mountedFirecrackerVolume {
	t.Helper()
	mountPoint := filepath.Join(firecrackerSuite.tmpDir, "mnt-"+name)
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		t.Fatal(err)
	}

	metaConf := meta.DefaultConf()
	metaConf.MountPoint = mountPoint
	m := meta.NewClient(firecrackerSuite.metaURL, metaConf)
	format, err := m.Load(true)
	if err != nil {
		t.Fatalf("load format: %v", err)
	}
	chunkConf := chunk.Config{
		BlockSize:   format.BlockSize * 1024,
		Compress:    format.Compression,
		MaxUpload:   20,
		MaxDownload: 200,
		BufferSize:  300 << 20,
		CacheSize:   512 << 20,
		CacheDir:    "memory",
	}
	blob, err := object.CreateStorage(strings.ToLower(format.Storage), format.Bucket, format.AccessKey, format.SecretKey, format.SessionToken)
	if err != nil {
		t.Fatalf("object storage: %v", err)
	}
	var slowStore *slowObjectStorage
	if slow != nil {
		slowStore = &slowObjectStorage{ObjectStorage: blob, latency: slow.latency, bandwidth: slow.bandwidth}
		blob = slowStore
	}
	blob = object.WithPrefix(blob, format.Name+"/")
	store := chunk.NewCachedStore(blob, chunkConf, nil)
	m.OnMsg(meta.CompactChunk, meta.MsgCallback(func(args ...interface{}) error {
		return vfs.Compact(chunkConf, store, args[0].([]meta.Slice), args[1].(uint64), 0)
	}))
	if err := m.NewSession(true); err != nil {
		t.Fatalf("new session: %v", err)
	}
	conf := &vfs.Config{
		Meta:            metaConf,
		Format:          *format,
		Chunk:           &chunkConf,
		FuseOpts:        &vfs.FuseOptions{},
		AttrTimeout:     time.Second,
		EntryTimeout:    time.Second,
		HideInternal:    true,
		DirEntryTimeout: time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- jfsfuse.Serve(vfs.NewVFS(conf, m, store, nil, nil), "", true, false)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-errCh:
			t.Fatalf("fuse server: %v", err)
		default:
		}
		if _, err := os.Stat(filepath.Join(mountPoint, ".stats")); err == nil {
			return &mountedFirecrackerVolume{mountPoint: mountPoint, meta: m, slow: slowStore}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("juicefs mount did not become ready")
	return nil
}

func (v *mountedFirecrackerVolume) close() {
	_ = exec.Command("fusermount3", "-uz", v.mountPoint).Run()
	_ = exec.Command("fusermount", "-uz", v.mountPoint).Run()
	_ = exec.Command("umount", "-l", v.mountPoint).Run()
	jfsfuse.Shutdown()
	if v.meta != nil {
		_ = v.meta.CloseSession()
	}
	_ = os.RemoveAll(v.mountPoint)
	time.Sleep(100 * time.Millisecond)
}

func createSnapshot(t *testing.T) {
	t.Helper()
	vol := mountFirecrackerVolume(t, "snapshot", nil)
	defer vol.close()

	memfile := filepath.Join(vol.mountPoint, "snapshot.mem")
	snapfile := filepath.Join(vol.mountPoint, "snapshot.vmstate")

	createVM := newFirecrackerVM(t, "create")
	createVM.start(t)
	defer createVM.close()
	configureAndStartVM(t, createVM)
	createVM.waitForSerial(t, regexp.MustCompile(`JFS_WORKLOAD_READY`), envDuration("JFS_FC_WORKLOAD_TIMEOUT", 360*time.Second))
	firecrackerSuite.snapshotSerial = createVM.serial
	createVM.patch(t, "/vm", map[string]any{"state": "Paused"})

	if f, err := os.OpenFile(memfile, os.O_CREATE|os.O_RDWR, 0o644); err != nil {
		t.Fatalf("create memfile: %v", err)
	} else if err := f.Close(); err != nil {
		t.Fatalf("close memfile: %v", err)
	}
	createVM.put(t, "/snapshot/create", map[string]any{
		"snapshot_type": "Full",
		"snapshot_path": snapfile,
		"mem_file_path": memfile,
	})
	createVM.close()
}

func configureAndStartVM(t *testing.T, vm *firecrackerVM) {
	t.Helper()
	vm.put(t, "/machine-config", map[string]any{
		"vcpu_count":   1,
		"mem_size_mib": firecrackerSuite.memMiB,
		"smt":          false,
	})
	vm.put(t, "/serial", map[string]any{
		"serial_out_path": vm.serial,
	})
	vm.put(t, "/boot-source", map[string]any{
		"kernel_image_path": firecrackerSuite.kernel,
		"boot_args":         "console=ttyS0 reboot=k panic=1 pci=off root=/dev/vda rw",
	})
	vm.put(t, "/drives/rootfs", map[string]any{
		"drive_id":       "rootfs",
		"path_on_host":   firecrackerSuite.rootfs,
		"is_root_device": true,
		"is_read_only":   false,
	})
	vm.put(t, "/actions", map[string]any{"action_type": "InstanceStart"})
}

type resumeTrialResult struct {
	duration      time.Duration
	gets          uint64
	bytes         uint64
	injectedDelay time.Duration
}

func runResumeTrial(t *testing.T, name string, recordProfile, loadProfile bool, slow *slowObjectConfig) resumeTrialResult {
	t.Helper()
	vol := mountFirecrackerVolume(t, name, slow)
	defer vol.close()

	memfile := filepath.Join(vol.mountPoint, "snapshot.mem")
	snapfile := filepath.Join(vol.mountPoint, "snapshot.vmstate")
	profilePath := "snapshot.mem.profile"

	loadVM := newFirecrackerVM(t, "load-"+name)
	loadVM.start(t)
	defer loadVM.close()
	stdoutPath := filepath.Join(loadVM.dir, "stdout.log")
	stdoutOffset := fileSize(stdoutPath)

	if recordProfile {
		resp := accessProfileControl(t, memfile, vfs.AccessProfileRequest{Op: vfs.AccessProfileRecordStart, Profile: profilePath})
		if !resp.OK || !resp.Recording {
			t.Fatalf("record-start: %+v", resp)
		}
	}
	if loadProfile {
		resp := accessProfileControl(t, memfile, vfs.AccessProfileRequest{Op: vfs.AccessProfileLoadStart, Profile: profilePath})
		if !resp.OK || !resp.Loading {
			t.Fatalf("load-start: %+v", resp)
		}
		var lead time.Duration
		if slow != nil {
			lead = slow.lead
		} else {
			lead = envDuration("JFS_FC_PROFILE_LOAD_LEAD", 500*time.Millisecond)
		}
		time.Sleep(lead)
	}
	start := time.Now()
	loadVM.put(t, "/snapshot/load", map[string]any{
		"snapshot_path": snapfile,
		"mem_backend": map[string]any{
			"backend_type": "File",
			"backend_path": memfile,
		},
		"resume_vm": true,
	})
	waitForFilePatternFromOffset(t, stdoutPath, stdoutOffset, regexp.MustCompile(`JFS_WORKLOAD_DONE`), envDuration("JFS_FC_RESUME_READY_TIMEOUT", 180*time.Second))
	duration := time.Since(start)
	if recordProfile {
		resp := accessProfileControl(t, memfile, vfs.AccessProfileRequest{Op: vfs.AccessProfileRecordStop})
		if !resp.OK || resp.Recording {
			t.Fatalf("record-stop: %+v", resp)
		}
	}
	if loadProfile {
		time.Sleep(envDuration("JFS_FC_PROFILE_WARMUP_SETTLE", 100*time.Millisecond))
		resp := accessProfileControl(t, memfile, vfs.AccessProfileRequest{Op: vfs.AccessProfileLoadStop})
		if !resp.OK || resp.Loading {
			t.Fatalf("load-stop: %+v", resp)
		}
	}

	var result resumeTrialResult
	result.duration = duration
	if vol.slow != nil {
		result.gets = vol.slow.gets.Load()
		result.bytes = vol.slow.bytes.Load()
		result.injectedDelay = time.Duration(vol.slow.injectedDelay.Load())
	}
	return result
}

func accessProfileControl(t *testing.T, path string, req vfs.AccessProfileRequest) vfs.AccessProfileResponse {
	t.Helper()
	ino, err := fileInode(path)
	if err != nil {
		t.Fatalf("inode for %s: %v", path, err)
	}
	req.Inode = meta.Ino(ino)
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	cf, err := os.OpenFile(filepath.Join(filepath.Dir(path), ".control"), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open control: %v", err)
	}
	defer cf.Close()
	msg := make([]byte, 8+len(data))
	binary.BigEndian.PutUint32(msg[0:4], meta.AccessProfile)
	binary.BigEndian.PutUint32(msg[4:8], uint32(len(data)))
	copy(msg[8:], data)
	if _, err = cf.Write(msg); err != nil {
		t.Fatalf("write control: %v", err)
	}
	respData := readControlJSON(t, cf)
	var resp vfs.AccessProfileResponse
	if err = json.Unmarshal(respData, &resp); err != nil {
		t.Fatalf("decode access profile response: %v", err)
	}
	return resp
}

func readControlJSON(t *testing.T, cf *os.File) []byte {
	t.Helper()
	buf := make([]byte, 1<<20)
	for {
		n, err := cf.Read(buf)
		if err != nil {
			if err == io.EOF {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			t.Fatalf("read control: %v", err)
		}
		if n == 1 {
			t.Fatalf("control error: %s", syscall.Errno(buf[0]))
		}
		if n >= 5 && buf[0] == meta.CDATA {
			size := binary.BigEndian.Uint32(buf[1:5])
			if int(size)+5 > n {
				t.Fatalf("short control response: n=%d size=%d", n, size)
			}
			return append([]byte(nil), buf[5:5+size]...)
		}
	}
}

func fileInode(path string) (uint64, error) {
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		return 0, err
	}
	return st.Ino, nil
}

type trialSummary struct {
	min           time.Duration
	median        time.Duration
	mean          time.Duration
	gets          uint64
	bytes         uint64
	injectedDelay time.Duration
}

func summarizeTrials(results []resumeTrialResult) trialSummary {
	var s trialSummary
	if len(results) == 0 {
		return s
	}
	durations := make([]time.Duration, 0, len(results))
	var total time.Duration
	for _, r := range results {
		durations = append(durations, r.duration)
		total += r.duration
		s.gets += r.gets
		s.bytes += r.bytes
		s.injectedDelay += r.injectedDelay
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	s.min = durations[0]
	s.median = durations[len(durations)/2]
	s.mean = total / time.Duration(len(durations))
	return s
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return d
}

func envInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func envBytesPerSecond(name string, fallback int64) int64 {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err == nil && n > 0 {
		return n
	}
	if strings.HasSuffix(value, "MiB/s") {
		n, err = strconv.ParseInt(strings.TrimSuffix(value, "MiB/s"), 10, 64)
		if err == nil && n > 0 {
			return n << 20
		}
	}
	return fallback
}

type firecrackerVM struct {
	name   string
	dir    string
	socket string
	serial string
	cmd    *exec.Cmd
	client *http.Client
}

func newFirecrackerVM(t *testing.T, name string) *firecrackerVM {
	t.Helper()
	dir := filepath.Join(firecrackerSuite.tmpDir, "fc-"+name)
	_ = os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(dir, "api.sock")
	serial := filepath.Join(dir, "serial.log")
	return &firecrackerVM{
		name:   name,
		dir:    dir,
		socket: socket,
		serial: serial,
		client: unixHTTPClient(socket),
	}
}

func (v *firecrackerVM) start(t *testing.T) {
	t.Helper()
	v.cmd = exec.Command(firecrackerSuite.fcBin, "--api-sock", v.socket)
	v.cmd.Stdout = mustCreate(t, filepath.Join(v.dir, "stdout.log"))
	v.cmd.Stderr = mustCreate(t, filepath.Join(v.dir, "stderr.log"))
	if err := v.cmd.Start(); err != nil {
		t.Fatalf("start firecracker: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(v.socket); err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("firecracker socket did not appear")
}

func (v *firecrackerVM) close() {
	if v.cmd == nil || v.cmd.Process == nil {
		return
	}
	_ = v.cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		_, _ = v.cmd.Process.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		_ = v.cmd.Process.Kill()
		<-done
	}
}

func (v *firecrackerVM) waitForSerial(t *testing.T, pattern *regexp.Regexp, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last []byte
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(v.serial)
		if err == nil {
			last = data
			if pattern.Match(data) {
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	if len(last) > 4096 {
		last = last[len(last)-4096:]
	}
	t.Fatalf("timed out waiting for serial pattern %q; tail:\n%s", pattern.String(), last)
}

func waitForFilePatternFromOffset(t *testing.T, path string, offset int64, pattern *regexp.Regexp, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last []byte
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			if offset < int64(len(data)) {
				data = data[offset:]
			}
			last = data
			if pattern.Match(data) {
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	if len(last) > 4096 {
		last = last[len(last)-4096:]
	}
	t.Fatalf("timed out waiting for %q in %s; tail:\n%s", pattern.String(), path, last)
}

func fileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}

func (v *firecrackerVM) put(t *testing.T, path string, body any) {
	v.do(t, http.MethodPut, path, body)
}

func (v *firecrackerVM) patch(t *testing.T, path string, body any) {
	v.do(t, http.MethodPatch, path, body)
}

func (v *firecrackerVM) do(t *testing.T, method, path string, body any) {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(method, "http://firecracker"+path, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := v.client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("%s %s: status=%s body=%s", method, path, resp.Status, strings.TrimSpace(string(b)))
	}
}

func unixHTTPClient(socket string) *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socket)
			},
		},
	}
}

func mustCreate(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}
