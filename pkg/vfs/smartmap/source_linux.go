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
	"fmt"
	"strconv"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
)

type uffdPageSource struct {
	inode      vfs.Ino
	chunkIndex uint32
	offset     uint32
	slices     []meta.Slice
	key        string // content fingerprint used to dedupe cloned clean pages
}

// captureUFFDPageSource records the JuiceFS slices backing one 2 MiB page.
func captureUFFDPageSource(v *vfs.VFS, ctx vfs.LogContext, ino vfs.Ino, fileLength, off uint64) (uffdPageSource, error) {
	if off%uffdHugePageSize != 0 {
		return uffdPageSource{}, fmt.Errorf("shared page offset %d is not 2 MiB aligned", off)
	}
	chunkIndex := uint32(off / meta.ChunkSize)
	chunkBase := uint64(chunkIndex) * meta.ChunkSize
	pageStart := uint32(off - chunkBase)
	pageEnd := pageStart + uffdHugePageSize
	var slices []meta.Slice
	if st := v.Meta.Read(ctx, ino, chunkIndex, &slices); st != 0 {
		return uffdPageSource{}, fmt.Errorf("read metadata slices for inode %d chunk %d: %w", ino, chunkIndex, st)
	}
	source := uffdPageSource{
		inode:      ino,
		chunkIndex: chunkIndex,
		offset:     pageStart,
		slices:     make([]meta.Slice, 0, len(slices)+1),
	}
	key := make([]byte, 0, 64+len(slices)*48)
	key = append(key, "range="...)
	key = strconv.AppendUint(key, uint64(chunkIndex), 10)
	key = append(key, ':')
	key = strconv.AppendUint(key, uint64(pageStart), 10)
	key = append(key, ';')
	pos := uint32(0)
	for _, s := range slices {
		sStart, sEnd := pos, pos+s.Len
		if sEnd <= pageStart || sStart >= pageEnd {
			pos = sEnd
			continue
		}
		partStart := max32(sStart, pageStart)
		partEnd := min32(sEnd, pageEnd)
		part := s
		part.Len = partEnd - partStart
		if s.Id != 0 {
			part.Off = s.Off + partStart - sStart
		} else {
			part.Off = 0
			part.Size = part.Len
		}
		source.slices = append(source.slices, part)
		key = appendUFFDSourceKeySlice(key, part)
		pos = sEnd
	}
	if pos < pageEnd {
		part := meta.Slice{Size: pageEnd - max32(pos, pageStart), Len: pageEnd - max32(pos, pageStart)}
		source.slices = append(source.slices, part)
		key = appendUFFDSourceKeySlice(key, part)
	}
	if off+uffdHugePageSize > fileLength {
		key = append(key, "eof:"...)
		key = strconv.AppendUint(key, fileLength-off, 10)
		key = append(key, ';')
	}
	source.key = string(key)
	return source, nil
}

func appendUFFDSourceKeySlice(key []byte, s meta.Slice) []byte {
	if s.Id == 0 {
		key = append(key, "zero:"...)
		key = strconv.AppendUint(key, uint64(s.Len), 10)
		key = append(key, ';')
		return key
	}
	key = append(key, "slice:"...)
	key = strconv.AppendUint(key, s.Id, 10)
	key = append(key, ':')
	key = strconv.AppendUint(key, uint64(s.Size), 10)
	key = append(key, ':')
	key = strconv.AppendUint(key, uint64(s.Off), 10)
	key = append(key, ':')
	key = strconv.AppendUint(key, uint64(s.Len), 10)
	key = append(key, ';')
	return key
}

func min32(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}

func max32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}
