package vfs

import (
	"errors"
	"sort"
	"syscall"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
)

type SharedExtentSourcesRequest struct {
	Files  []Ino
	Ranges []SharedExtentSourceRange
}

type SharedExtentSourceRange struct {
	Off uint64
	Len uint64
}

type SharedExtentSourcesResponse struct {
	Spans []SharedExtentSourceSpan `json:"spans"`
}

type SharedExtentSourceSpan struct {
	Off         uint64 `json:"off"`
	Len         uint64 `json:"len"`
	SourceIndex int    `json:"sourceIndex"`
	SourceIno   Ino    `json:"sourceIno"`
}

type sharedExtentSliceSpan struct {
	start uint64
	end   uint64
	id    uint64
	off   uint64
}

func decodeSharedExtentSourcesRequest(r *utils.Buffer) (*SharedExtentSourcesRequest, error) {
	if r.Left() < 4 {
		return nil, errors.New("missing file count")
	}
	nfiles := int(r.Get32())
	if nfiles < 2 || r.Left() < nfiles*8+4 {
		return nil, errors.New("invalid file count")
	}
	req := &SharedExtentSourcesRequest{Files: make([]Ino, nfiles)}
	for i := range req.Files {
		req.Files[i] = Ino(r.Get64())
	}
	nranges := int(r.Get32())
	if nranges == 0 || r.Left() != nranges*16 {
		return nil, errors.New("invalid range count")
	}
	req.Ranges = make([]SharedExtentSourceRange, nranges)
	for i := range req.Ranges {
		req.Ranges[i] = SharedExtentSourceRange{Off: r.Get64(), Len: r.Get64()}
	}
	return req, nil
}

func (v *VFS) SharedExtentSources(ctx meta.Context, req *SharedExtentSourcesRequest) (*SharedExtentSourcesResponse, syscall.Errno) {
	if req == nil || len(req.Files) < 2 || len(req.Ranges) == 0 {
		return nil, syscall.EINVAL
	}
	for _, ino := range req.Files {
		var attr meta.Attr
		if st := v.Meta.GetAttr(ctx, ino, &attr); st != 0 {
			return nil, st
		}
		if attr.Typ != meta.TypeFile {
			return nil, syscall.EINVAL
		}
	}

	resp := &SharedExtentSourcesResponse{}
	for _, rg := range req.Ranges {
		if rg.Len == 0 || rg.Off+rg.Len < rg.Off {
			return nil, syscall.EINVAL
		}
		spans, st := v.sharedExtentSourceSpans(ctx, req.Files, rg.Off, rg.Len)
		if st != 0 {
			return nil, st
		}
		for _, span := range spans {
			appendSharedExtentSourceSpan(resp, span)
		}
	}
	return resp, 0
}

func (v *VFS) sharedExtentSourceSpans(ctx meta.Context, files []Ino, off, length uint64) ([]SharedExtentSourceSpan, syscall.Errno) {
	end := off + length
	firstChunk := off / meta.ChunkSize
	lastChunk := (end - 1) / meta.ChunkSize
	var out []SharedExtentSourceSpan

	for chunk := firstChunk; chunk <= lastChunk; chunk++ {
		chunkStart := chunk * meta.ChunkSize
		chunkEnd := chunkStart + meta.ChunkSize
		if chunkEnd > end {
			chunkEnd = end
		}
		if chunkStart < off {
			chunkStart = off
		}

		byFile := make([][]sharedExtentSliceSpan, len(files))
		bounds := []uint64{chunkStart, chunkEnd}
		for i, ino := range files {
			var slices []meta.Slice
			if st := v.Meta.Read(ctx, ino, uint32(chunk), &slices); st != 0 {
				return nil, st
			}
			byFile[i] = sharedExtentSliceSpans(chunk*meta.ChunkSize, slices)
			for _, span := range byFile[i] {
				if span.end <= chunkStart || span.start >= chunkEnd {
					continue
				}
				bounds = append(bounds, max(chunkStart, span.start), min(chunkEnd, span.end))
			}
		}
		bounds = sortedUniqueBounds(bounds)
		targetIndex := len(files) - 1
		for i := 0; i+1 < len(bounds); i++ {
			start, stop := bounds[i], bounds[i+1]
			if start == stop {
				continue
			}
			sourceIndex := -1
			sourceIno := Ino(0)
			target, ok := sharedExtentSpanAt(byFile[targetIndex], start)
			if ok && target.id != 0 {
				sourceIndex = targetIndex
				sourceIno = files[targetIndex]
				for j := 0; j < targetIndex; j++ {
					if source, ok := sharedExtentSpanAt(byFile[j], start); ok && sharedExtentSharedAt(target, source, start) {
						sourceIndex = j
						sourceIno = files[j]
						break
					}
				}
			}
			appendSharedExtentSourceSpanSlice(&out, SharedExtentSourceSpan{
				Off:         start,
				Len:         stop - start,
				SourceIndex: sourceIndex,
				SourceIno:   sourceIno,
			})
		}
	}
	return out, 0
}

func sharedExtentSliceSpans(chunkStart uint64, slices []meta.Slice) []sharedExtentSliceSpan {
	var pos uint64
	spans := make([]sharedExtentSliceSpan, 0, len(slices))
	for _, s := range slices {
		start := chunkStart + pos
		end := start + uint64(s.Len)
		spans = append(spans, sharedExtentSliceSpan{
			start: start,
			end:   end,
			id:    s.Id,
			off:   uint64(s.Off),
		})
		pos += uint64(s.Len)
	}
	return spans
}

func sharedExtentSpanAt(spans []sharedExtentSliceSpan, off uint64) (sharedExtentSliceSpan, bool) {
	i := sort.Search(len(spans), func(i int) bool { return spans[i].end > off })
	if i < len(spans) && spans[i].start <= off && off < spans[i].end {
		return spans[i], true
	}
	return sharedExtentSliceSpan{}, false
}

func sharedExtentSharedAt(a, b sharedExtentSliceSpan, off uint64) bool {
	return a.id != 0 && a.id == b.id && a.off+off-a.start == b.off+off-b.start
}

func sortedUniqueBounds(bounds []uint64) []uint64 {
	sort.Slice(bounds, func(i, j int) bool { return bounds[i] < bounds[j] })
	n := 0
	for _, b := range bounds {
		if n == 0 || bounds[n-1] != b {
			bounds[n] = b
			n++
		}
	}
	return bounds[:n]
}

func appendSharedExtentSourceSpan(resp *SharedExtentSourcesResponse, span SharedExtentSourceSpan) {
	appendSharedExtentSourceSpanSlice(&resp.Spans, span)
}

func appendSharedExtentSourceSpanSlice(spans *[]SharedExtentSourceSpan, span SharedExtentSourceSpan) {
	if span.Len == 0 {
		return
	}
	last := len(*spans) - 1
	if last >= 0 {
		prev := &(*spans)[last]
		if prev.Off+prev.Len == span.Off && prev.SourceIndex == span.SourceIndex && prev.SourceIno == span.SourceIno {
			prev.Len += span.Len
			return
		}
	}
	*spans = append(*spans, span)
}
