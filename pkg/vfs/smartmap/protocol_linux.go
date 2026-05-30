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

// Control messages are exchanged as JSON on the serve_memory_faults UDS
// connection after the initial SCM_RIGHTS handoff.
const (
	uffdEvictionPolicyProbe = "probe"

	uffdControlEvict         = "evict"
	uffdControlEvictAck      = "evict_ack"
	uffdControlProbe         = "probe"
	uffdControlProbeAck      = "probe_ack"
	uffdControlWriteFault    = "write_fault"
	uffdControlWriteFaultAck = "write_fault_ack"
)

type uffdControlRange struct {
	FileOffset uint64 `json:"file_offset"`
	Length     uint64 `json:"length"`
	ShmOffset  uint64 `json:"shm_offset"`
}

type uffdControlMessage struct {
	Type      string             `json:"type"`
	RequestID uint64             `json:"request_id"`
	MemoryID  string             `json:"memory_id,omitempty"`
	Ranges    []uffdControlRange `json:"ranges,omitempty"`
}

type uffdControlAck struct {
	Type      string `json:"type"`
	RequestID uint64 `json:"request_id"`
	OK        bool   `json:"ok"`
	Error     string `json:"error,omitempty"`
}
