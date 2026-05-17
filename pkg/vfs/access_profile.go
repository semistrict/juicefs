/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
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

package vfs

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	accessProfileVersion     = 1
	defaultAccessProfileRuns = 3
)

var (
	accessProfileLockOwner atomic.Uint64

	accessProfileRecordRuns = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "juicefs_access_profile_record_runs_total",
		Help: "Access profile record runs.",
	}, []string{"result"})
	accessProfileRecordEntries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "juicefs_access_profile_record_entries_total",
		Help: "Access profile record entries.",
	}, []string{"type"})
	accessProfileRecordDuplicates = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "juicefs_access_profile_record_duplicates_total",
		Help: "Duplicate access profile record entries skipped.",
	})
	accessProfileLoadRuns = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "juicefs_access_profile_load_runs_total",
		Help: "Access profile load runs.",
	}, []string{"result"})
	accessProfileLoadEntries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "juicefs_access_profile_load_entries_total",
		Help: "Access profile load entries.",
	}, []string{"result"})
	accessProfileLoadBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "juicefs_access_profile_load_bytes_total",
		Help: "Bytes attempted by access profile load.",
	})
	accessProfileLoadDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "juicefs_access_profile_load_duration_seconds",
		Help:    "Access profile load duration.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 18),
	})
	accessProfileWarmReadBlocks = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "juicefs_access_profile_warm_read_blocks_total",
		Help: "Reads against blocks attempted by access profile load.",
	}, []string{"result"})
	accessProfileWarmReadBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "juicefs_access_profile_warm_read_bytes_total",
		Help: "Read bytes against blocks attempted by access profile load.",
	}, []string{"result"})
)

func registerAccessProfileMetrics(registerer prometheus.Registerer) {
	_ = registerer.Register(accessProfileRecordRuns)
	_ = registerer.Register(accessProfileRecordEntries)
	_ = registerer.Register(accessProfileRecordDuplicates)
	_ = registerer.Register(accessProfileLoadRuns)
	_ = registerer.Register(accessProfileLoadEntries)
	_ = registerer.Register(accessProfileLoadBytes)
	_ = registerer.Register(accessProfileLoadDuration)
	_ = registerer.Register(accessProfileWarmReadBlocks)
	_ = registerer.Register(accessProfileWarmReadBytes)
}

type accessProfileEntryType byte

const (
	accessProfileEntryRead  accessProfileEntryType = 'r'
	accessProfileEntryWrite accessProfileEntryType = 'w'
)

type accessProfileEntry struct {
	order uint64
	typ   accessProfileEntryType
	block uint64
}

type accessProfileRun struct {
	id      uint64
	entries []accessProfileEntry
}

type accessProfileRecorder struct {
	target     accessProfileTarget
	lockIno    Ino
	lockOwner  uint64
	maxRuns    int
	started    time.Time
	seen       map[uint64]struct{}
	entries    []accessProfileEntry
	nextOrder  uint64
	duplicates uint64
	read       uint64
	write      uint64
}

type accessProfileLoader struct {
	cancel context.CancelFunc
	warmed map[uint64]struct{}
}

type AccessProfileOp string

const (
	AccessProfileRecordStart AccessProfileOp = "record-start"
	AccessProfileRecordStop  AccessProfileOp = "record-stop"
	AccessProfileLoadStart   AccessProfileOp = "load-start"
	AccessProfileLoadStop    AccessProfileOp = "load-stop"
	AccessProfileStatusOp    AccessProfileOp = "status"
)

type AccessProfileStatus string

const (
	accessProfileStatusCanceled     AccessProfileStatus = "canceled"
	accessProfileStatusFailed       AccessProfileStatus = "failed"
	accessProfileStatusFinished     AccessProfileStatus = "finished"
	accessProfileStatusInvalid      AccessProfileStatus = "invalid"
	accessProfileStatusLocked       AccessProfileStatus = "locked"
	accessProfileStatusNotLoading   AccessProfileStatus = "not_loading"
	accessProfileStatusNotRecording AccessProfileStatus = "not_recording"
	accessProfileStatusReplaced     AccessProfileStatus = "replaced"
	accessProfileStatusStarted      AccessProfileStatus = "started"
	accessProfileStatusStatus       AccessProfileStatus = "status"
)

type AccessProfileRequest struct {
	Op      AccessProfileOp `json:"op"`
	Inode   Ino             `json:"inode"`
	Profile string          `json:"profile,omitempty"`
}

type AccessProfileResponse struct {
	OK        bool                `json:"ok"`
	Status    AccessProfileStatus `json:"status"`
	Message   string              `json:"message,omitempty"`
	Recording bool                `json:"recording"`
	Loading   bool                `json:"loading"`
}

type accessProfileTarget struct {
	path   string
	parent Ino
	name   string
}

type accessProfileManager struct {
	sync.Mutex
	v         *VFS
	recorders map[Ino]*accessProfileRecorder
	loaders   map[Ino]*accessProfileLoader
}

func newAccessProfileManager(v *VFS) *accessProfileManager {
	return &accessProfileManager{
		v:         v,
		recorders: make(map[Ino]*accessProfileRecorder),
		loaders:   make(map[Ino]*accessProfileLoader),
	}
}

func (m *accessProfileManager) RecordAccess(ino Ino, typ byte, off, size uint64) {
	if size == 0 || m.v.Conf == nil || m.v.Conf.Chunk == nil || m.v.Conf.Chunk.BlockSize <= 0 {
		return
	}
	blockSize := uint64(m.v.Conf.Chunk.BlockSize)
	first := off / blockSize
	last := (off + size - 1) / blockSize

	m.Lock()
	defer m.Unlock()
	r := m.recorders[ino]
	if r == nil {
		return
	}
	for block := first; block <= last; block++ {
		if _, ok := r.seen[block]; ok {
			r.duplicates++
			accessProfileRecordDuplicates.Inc()
			continue
		}
		r.seen[block] = struct{}{}
		entryType := accessProfileEntryType(typ)
		r.entries = append(r.entries, accessProfileEntry{order: r.nextOrder, typ: entryType, block: block})
		r.nextOrder++
		if entryType == accessProfileEntryWrite {
			r.write++
		} else {
			r.read++
		}
		accessProfileRecordEntries.WithLabelValues(string(entryType)).Inc()
	}
}

func (m *accessProfileManager) RecordWarmRead(ino Ino, off, size uint64) {
	if size == 0 || m.v.Conf == nil || m.v.Conf.Chunk == nil || m.v.Conf.Chunk.BlockSize <= 0 {
		return
	}
	blockSize := uint64(m.v.Conf.Chunk.BlockSize)
	first := off / blockSize
	last := (off + size - 1) / blockSize

	m.Lock()
	l := m.loaders[ino]
	if l == nil {
		m.Unlock()
		return
	}
	var warmed, cold uint64
	for block := first; block <= last; block++ {
		if _, ok := l.warmed[block]; ok {
			warmed++
		} else {
			cold++
		}
	}
	m.Unlock()

	if warmed > 0 {
		accessProfileWarmReadBlocks.WithLabelValues("warmed").Add(float64(warmed))
		accessProfileWarmReadBytes.WithLabelValues("warmed").Add(float64(min(size, warmed*blockSize)))
	}
	if cold > 0 {
		accessProfileWarmReadBlocks.WithLabelValues("not_warmed").Add(float64(cold))
		accessProfileWarmReadBytes.WithLabelValues("not_warmed").Add(float64(min(size, cold*blockSize)))
	}
}

func (m *accessProfileManager) Handle(ctx meta.Context, req AccessProfileRequest) AccessProfileResponse {
	resp := AccessProfileResponse{OK: true}
	if req.Inode == 0 {
		return AccessProfileResponse{OK: false, Status: accessProfileStatusInvalid, Message: "inode is required"}
	}
	logCtx := NewLogContext(ctx)
	switch req.Op {
	case AccessProfileRecordStart:
		resp = m.startRecord(logCtx, req.Inode, strings.TrimSpace(req.Profile))
	case AccessProfileRecordStop:
		resp = m.stopRecord(req.Inode, accessProfileStatusFinished)
	case AccessProfileLoadStart:
		resp = m.startLoad(logCtx, req.Inode, strings.TrimSpace(req.Profile))
	case AccessProfileLoadStop:
		resp = m.stopLoad(req.Inode, accessProfileStatusCanceled)
	case AccessProfileStatusOp:
		resp = m.status(req.Inode, accessProfileStatusStatus)
	default:
		return AccessProfileResponse{OK: false, Status: accessProfileStatusInvalid, Message: "unknown op"}
	}
	return resp
}

func (m *accessProfileManager) startRecord(ctx Context, ino Ino, profilePath string) AccessProfileResponse {
	target, err := m.resolveProfileTarget(ctx, ino, profilePath)
	if err != nil {
		logger.Warnf("access profile record resolve inode %d: %s", ino, err)
		accessProfileRecordRuns.WithLabelValues(string(accessProfileStatusFailed)).Inc()
		return m.status(ino, accessProfileStatusFailed).withError(err)
	}

	m.stopRecord(ino, accessProfileStatusReplaced)
	lockIno, lockOwner, err := m.acquireRecordLock(ctx, target)
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok && errno == syscall.EAGAIN {
			accessProfileRecordRuns.WithLabelValues(string(accessProfileStatusLocked)).Inc()
			logger.Infof("access profile record inode=%d profile=%s locked", ino, target.path)
			return m.status(ino, accessProfileStatusLocked)
		} else {
			accessProfileRecordRuns.WithLabelValues(string(accessProfileStatusFailed)).Inc()
			logger.Warnf("access profile record lock inode=%d profile=%s: %s", ino, target.path, err)
			return m.status(ino, accessProfileStatusFailed).withError(err)
		}
	}
	m.Lock()
	m.recorders[ino] = &accessProfileRecorder{
		target:    target,
		lockIno:   lockIno,
		lockOwner: lockOwner,
		maxRuns:   defaultAccessProfileRuns,
		started:   time.Now(),
		seen:      make(map[uint64]struct{}),
		nextOrder: 1,
	}
	m.Unlock()
	accessProfileRecordRuns.WithLabelValues(string(accessProfileStatusStarted)).Inc()
	return m.status(ino, accessProfileStatusStarted)
}

func (m *accessProfileManager) stopRecord(ino Ino, result AccessProfileStatus) AccessProfileResponse {
	m.Lock()
	r := m.recorders[ino]
	if r != nil {
		delete(m.recorders, ino)
	}
	m.Unlock()
	if r == nil {
		return m.status(ino, accessProfileStatusNotRecording)
	}

	err := m.commitRecord(r)
	m.releaseRecordLock(r)
	if err != nil {
		logger.Warnf("access profile record inode=%d entries=%d error=%s", ino, len(r.entries), err)
		accessProfileRecordRuns.WithLabelValues(string(accessProfileStatusFailed)).Inc()
		return m.status(ino, accessProfileStatusFailed).withError(err)
	}
	accessProfileRecordRuns.WithLabelValues(string(result)).Inc()
	logger.Infof("access profile record inode=%d run_entries=%d read=%d write=%d duplicates=%d duration=%s", ino, len(r.entries), r.read, r.write, r.duplicates, time.Since(r.started))
	return m.status(ino, result)
}

func (m *accessProfileManager) acquireRecordLock(ctx Context, target accessProfileTarget) (Ino, uint64, error) {
	ino, err := m.ensureProfileFile(ctx, target)
	if err != nil {
		return 0, 0, err
	}
	owner := accessProfileLockOwner.Add(1)
	if owner == 0 {
		owner = accessProfileLockOwner.Add(1)
	}
	if st := m.v.Meta.Flock(ctx, ino, owner, meta.F_WRLCK, false); st != 0 {
		return 0, 0, st
	}
	return ino, owner, nil
}

func (m *accessProfileManager) releaseRecordLock(r *accessProfileRecorder) {
	if r.lockIno == 0 || r.lockOwner == 0 {
		return
	}
	if st := m.v.Meta.Flock(meta.Background(), r.lockIno, r.lockOwner, meta.F_UNLCK, false); st != 0 {
		logger.Warnf("access profile unlock inode=%d owner=%d: %s", r.lockIno, r.lockOwner, st)
	}
}

func (m *accessProfileManager) startLoad(ctx Context, ino Ino, profilePath string) AccessProfileResponse {
	target, err := m.resolveProfileTarget(ctx, ino, profilePath)
	if err != nil {
		logger.Warnf("access profile load resolve inode %d: %s", ino, err)
		accessProfileLoadRuns.WithLabelValues(string(accessProfileStatusFailed)).Inc()
		return m.status(ino, accessProfileStatusFailed).withError(err)
	}

	m.stopLoad(ino, accessProfileStatusReplaced)
	loadCtx, cancel := context.WithCancel(context.Background())
	loader := &accessProfileLoader{cancel: cancel, warmed: make(map[uint64]struct{})}
	m.Lock()
	m.loaders[ino] = loader
	m.Unlock()
	accessProfileLoadRuns.WithLabelValues(string(accessProfileStatusStarted)).Inc()
	go m.load(loadCtx, ino, target, loader)
	return m.status(ino, accessProfileStatusStarted)
}

func (m *accessProfileManager) stopLoad(ino Ino, result AccessProfileStatus) AccessProfileResponse {
	m.Lock()
	l := m.loaders[ino]
	if l != nil {
		delete(m.loaders, ino)
	}
	m.Unlock()
	if l == nil {
		return m.status(ino, accessProfileStatusNotLoading)
	}
	l.cancel()
	accessProfileLoadRuns.WithLabelValues(string(result)).Inc()
	return m.status(ino, result)
}

func (m *accessProfileManager) resolveProfileTarget(ctx Context, ino Ino, profilePath string) (accessProfileTarget, error) {
	if profilePath == "" {
		return accessProfileTarget{}, syscall.EINVAL
	}
	var clean string
	if strings.HasPrefix(profilePath, "/") {
		clean = path.Clean(profilePath)
	} else {
		paths := m.v.Meta.GetPaths(ctx, ino)
		if len(paths) == 0 {
			return accessProfileTarget{}, fmt.Errorf("no paths for inode %d", ino)
		}
		clean = path.Join(path.Dir(paths[0]), profilePath)
	}
	parentPath, name := path.Split(clean)
	name = strings.TrimSuffix(name, "/")
	if name == "" || name == "." || name == ".." {
		return accessProfileTarget{}, syscall.EINVAL
	}
	parentPath = strings.Trim(parentPath, "/")
	parent := meta.RootInode
	if parentPath != "" {
		var attr Attr
		if st := m.v.Meta.Resolve(ctx, meta.RootInode, parentPath, &parent, &attr, true); st != 0 {
			return accessProfileTarget{}, st
		}
		if attr.Typ != meta.TypeDirectory {
			return accessProfileTarget{}, syscall.ENOTDIR
		}
	}
	return accessProfileTarget{path: clean, parent: parent, name: name}, nil
}

func (m *accessProfileManager) commitRecord(r *accessProfileRecorder) error {
	runs, blockSize, err := m.readAccessProfileFile(NewLogContext(meta.Background()), r.target)
	if err != nil && err != syscall.ENOENT {
		return err
	}
	if m.v.Conf != nil && m.v.Conf.Chunk != nil {
		blockSize = uint64(m.v.Conf.Chunk.BlockSize)
	}
	var nextID uint64 = 1
	for _, run := range runs {
		if run.id >= nextID {
			nextID = run.id + 1
		}
	}
	runs = append(runs, accessProfileRun{id: nextID, entries: r.entries})
	limit := r.maxRuns
	if len(runs) > limit {
		runs = runs[len(runs)-limit:]
	}
	return m.writeAccessProfileFile(NewLogContext(meta.Background()), r.target, blockSize, limit, runs)
}

func (m *accessProfileManager) status(ino Ino, status AccessProfileStatus) AccessProfileResponse {
	m.Lock()
	defer m.Unlock()
	return AccessProfileResponse{
		OK:        true,
		Status:    status,
		Recording: m.recorders[ino] != nil,
		Loading:   m.loaders[ino] != nil,
	}
}

func (r AccessProfileResponse) withError(err error) AccessProfileResponse {
	r.OK = false
	if err != nil {
		r.Message = err.Error()
	}
	return r
}

func (m *accessProfileManager) load(ctx context.Context, ino Ino, target accessProfileTarget, loader *accessProfileLoader) {
	start := time.Now()
	var warmed, duplicate, invalid, failed uint64
	defer func() {
		accessProfileLoadDuration.Observe(time.Since(start).Seconds())
		logger.Infof("access profile load inode=%d warmed=%d duplicate=%d invalid=%d failed=%d duration=%s", ino, warmed, duplicate, invalid, failed, time.Since(start))
	}()

	runs, blockSize, err := m.readAccessProfileFile(NewLogContext(meta.Background()), target)
	if err != nil {
		logger.Warnf("access profile load inode=%d path=%s: %s", ino, target.path, err)
		accessProfileLoadRuns.WithLabelValues(string(accessProfileStatusFailed)).Inc()
		return
	}
	if blockSize == 0 {
		accessProfileLoadRuns.WithLabelValues(string(accessProfileStatusFailed)).Inc()
		return
	}
	entries, duplicateEntries := breadthFirstAccessProfileEntries(runs)
	if duplicateEntries > 0 {
		duplicate += uint64(duplicateEntries)
		accessProfileLoadEntries.WithLabelValues("duplicate_skipped").Add(float64(duplicateEntries))
	}
	for _, entry := range entries {
		if entry.typ != accessProfileEntryRead && entry.typ != accessProfileEntryWrite {
			invalid++
			accessProfileLoadEntries.WithLabelValues(string(accessProfileStatusInvalid)).Inc()
			continue
		}
		m.Lock()
		if _, ok := loader.warmed[entry.block]; ok {
			m.Unlock()
			duplicate++
			accessProfileLoadEntries.WithLabelValues("duplicate_skipped").Inc()
			continue
		}
		loader.warmed[entry.block] = struct{}{}
		m.Unlock()
		if ctx.Err() != nil {
			accessProfileLoadRuns.WithLabelValues(string(accessProfileStatusCanceled)).Inc()
			return
		}
		if err := m.warmBlock(ctx, ino, entry.block, blockSize); err != nil {
			failed++
			accessProfileLoadEntries.WithLabelValues(string(accessProfileStatusFailed)).Inc()
			continue
		}
		warmed++
		accessProfileLoadEntries.WithLabelValues("loaded").Inc()
		accessProfileLoadBytes.Add(float64(blockSize))
	}
	accessProfileLoadRuns.WithLabelValues(string(accessProfileStatusFinished)).Inc()
}

func breadthFirstAccessProfileEntries(runs []accessProfileRun) ([]accessProfileEntry, int) {
	maxLen := 0
	for _, run := range runs {
		if len(run.entries) > maxLen {
			maxLen = len(run.entries)
		}
	}
	seen := make(map[uint64]struct{})
	entries := make([]accessProfileEntry, 0)
	duplicates := 0
	for i := 0; i < maxLen; i++ {
		for _, run := range runs {
			if i >= len(run.entries) {
				continue
			}
			entry := run.entries[i]
			if _, ok := seen[entry.block]; ok {
				duplicates++
				continue
			}
			seen[entry.block] = struct{}{}
			entries = append(entries, entry)
		}
	}
	return entries, duplicates
}

func (m *accessProfileManager) warmBlock(ctx context.Context, ino Ino, block, blockSize uint64) error {
	var slices []meta.Slice
	chunk := block * blockSize / meta.ChunkSize
	if st := m.v.Meta.Read(meta.Background(), ino, uint32(chunk), &slices); st != 0 {
		return st
	}
	blockStart := uint32((block * blockSize) % meta.ChunkSize)
	for _, s := range accessProfileSlicesForBlock(slices, blockStart, uint32(blockSize)) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := m.v.Store.FillCache(s.Id, s.Size); err != nil {
			return err
		}
	}
	return nil
}

func accessProfileSlicesForBlock(slices []meta.Slice, blockStart, blockSize uint32) []meta.Slice {
	blockEnd := blockStart + blockSize
	var pos uint32
	var selected []meta.Slice
	for _, s := range slices {
		sliceStart := pos
		sliceEnd := pos + s.Len
		if blockStart < sliceEnd && blockEnd > sliceStart {
			selected = append(selected, s)
		}
		pos = sliceEnd
	}
	return selected
}

func (m *accessProfileManager) ensureProfileFile(ctx Context, target accessProfileTarget) (Ino, error) {
	entry, eno := m.v.Lookup(ctx, target.parent, target.name)
	if eno == 0 {
		return entry.Inode, nil
	}
	if eno != syscall.ENOENT {
		return 0, eno
	}
	entry, fh, eno := m.v.Create(ctx, target.parent, target.name, 0644, 0, syscall.O_RDWR)
	if eno != 0 {
		return 0, eno
	}
	m.v.Release(ctx, entry.Inode, fh)
	return entry.Inode, nil
}

func parseAccessProfile(data []byte) ([]accessProfileRun, uint64, error) {
	var runs []accessProfileRun
	var current *accessProfileRun
	var blockSize uint64
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		switch fields[0] {
		case "JFS_ACCESS_PROFILE":
			if len(fields) < 3 {
				return nil, 0, fmt.Errorf("invalid header")
			}
			version, err := strconv.Atoi(fields[1])
			if err != nil || version != accessProfileVersion {
				return nil, 0, fmt.Errorf("unsupported version %q", fields[1])
			}
			for _, field := range fields[2:] {
				if strings.HasPrefix(field, "block_size=") {
					blockSize, _ = strconv.ParseUint(strings.TrimPrefix(field, "block_size="), 10, 64)
				}
			}
		case "RUN":
			id := uint64(len(runs) + 1)
			for _, field := range fields[1:] {
				if strings.HasPrefix(field, "id=") {
					id, _ = strconv.ParseUint(strings.TrimPrefix(field, "id="), 10, 64)
				}
			}
			runs = append(runs, accessProfileRun{id: id})
			current = &runs[len(runs)-1]
		case "END":
			current = nil
		default:
			if current == nil || len(fields) != 3 {
				continue
			}
			order, err1 := strconv.ParseUint(fields[0], 10, 64)
			block, err2 := strconv.ParseUint(fields[2], 10, 64)
			if err1 != nil || err2 != nil || len(fields[1]) != 1 {
				continue
			}
			current.entries = append(current.entries, accessProfileEntry{order: order, typ: accessProfileEntryType(fields[1][0]), block: block})
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, 0, err
	}
	for i := range runs {
		sort.Slice(runs[i].entries, func(a, b int) bool {
			return runs[i].entries[a].order < runs[i].entries[b].order
		})
	}
	return runs, blockSize, nil
}

func formatAccessProfile(blockSize uint64, maxRuns int, runs []accessProfileRun) []byte {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "JFS_ACCESS_PROFILE %d block_size=%d max_runs=%d\n", accessProfileVersion, blockSize, maxRuns)
	for _, run := range runs {
		fmt.Fprintf(&buf, "RUN id=%d\n", run.id)
		for _, entry := range run.entries {
			fmt.Fprintf(&buf, "%d %c %d\n", entry.order, byte(entry.typ), entry.block)
		}
		fmt.Fprintln(&buf, "END")
	}
	return buf.Bytes()
}

func (m *accessProfileManager) readAccessProfileFile(ctx Context, target accessProfileTarget) ([]accessProfileRun, uint64, error) {
	entry, eno := m.v.Lookup(ctx, target.parent, target.name)
	if eno != 0 {
		return nil, 0, eno
	}
	_, fh, eno := m.v.Open(ctx, entry.Inode, syscall.O_RDONLY)
	if eno != 0 {
		return nil, 0, eno
	}
	defer m.v.Release(ctx, entry.Inode, fh)

	data := make([]byte, entry.Attr.Length)
	var off uint64
	for off < uint64(len(data)) {
		n, eno := m.v.Read(ctx, entry.Inode, data[off:], off, fh)
		if eno != 0 {
			return nil, 0, eno
		}
		if n == 0 {
			break
		}
		off += uint64(n)
	}
	return parseAccessProfile(data[:off])
}

func (m *accessProfileManager) writeAccessProfileFile(ctx Context, target accessProfileTarget, blockSize uint64, maxRuns int, runs []accessProfileRun) error {
	data := formatAccessProfile(blockSize, maxRuns, runs)
	entry, eno := m.v.Lookup(ctx, target.parent, target.name)
	var fh uint64
	var ino Ino
	if eno == syscall.ENOENT {
		entry, fh, eno = m.v.Create(ctx, target.parent, target.name, 0644, 0, syscall.O_RDWR)
	} else if eno == 0 {
		entry, fh, eno = m.v.Open(ctx, entry.Inode, syscall.O_RDWR)
	}
	if eno != 0 {
		return eno
	}
	ino = entry.Inode
	defer m.v.Release(ctx, ino, fh)

	var attr Attr
	if eno = m.v.Truncate(ctx, ino, 0, fh, &attr); eno != 0 {
		return eno
	}
	if len(data) > 0 {
		if eno = m.v.Write(ctx, ino, data, 0, fh); eno != 0 {
			return eno
		}
	}
	if eno = m.v.Flush(ctx, ino, fh, 0); eno != 0 {
		return eno
	}
	return nil
}
