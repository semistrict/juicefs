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

const (
	uffdEvictionPolicyProbe = "probe"

	smartmapFrameMap        = "map"
	smartmapFrameMapped     = "mapped"
	smartmapFrameAttach     = "attach"
	smartmapFrameAttached   = "attached"
	smartmapFrameFatal      = "fatal"
	smartmapFrameAck        = "ack"
	smartmapFrameRelease    = "release"
	smartmapFrameProbe      = "probe"
	smartmapFrameWriteFault = "write_fault"

	smartmapFDMemory = "memory"
	smartmapFDUFFD   = "uffd"
)

type uffdControlRange struct {
	FileOffset uint64 `json:"file_offset"`
	Length     uint64 `json:"length"`
	ShmOffset  uint64 `json:"shm_offset"`
}

type smartmapFrame struct {
	Type             string             `json:"type"`
	Path             string             `json:"path,omitempty"`
	Size             uint64             `json:"size,omitempty"`
	PageSize         uintptr            `json:"page_size,omitempty"`
	FD               string             `json:"fd,omitempty"`
	Extents          []UFFDExtent       `json:"extents,omitempty"`
	BaseHostVirtAddr uintptr            `json:"base_host_virt_addr,omitempty"`
	Ranges           []uffdControlRange `json:"ranges,omitempty"`
	Released         []uffdControlRange `json:"released,omitempty"`
	OK               *bool              `json:"ok,omitempty"`
	Error            string             `json:"error,omitempty"`
}
