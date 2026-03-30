//go:build !windows

/*
 * JuiceFS, Copyright 2024 Juicedata, Inc.
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

package chunk

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
)

// Cache protocol operation codes
const (
	opLoad       uint8 = 0x01
	opCache      uint8 = 0x02
	opExist      uint8 = 0x03
	opRemove     uint8 = 0x04
	opStage      uint8 = 0x05
	opUploaded   uint8 = 0x06
	opStats      uint8 = 0x07
	opRemoveStag uint8 = 0x08
)

// Response status codes
const (
	statusOK       uint8 = 0x00
	statusNotFound uint8 = 0x01
	statusError    uint8 = 0x02
)

// LOAD response flags
const (
	flagFDSent   uint8 = 0x01 // FD was sent over control channel
	flagFDCached uint8 = 0x00 // Client already has the FD
)

var (
	errProtoShort = errors.New("cache proto: short read")
)

// Request represents a cache protocol request message.
// Wire format: [op:1][flags:1][key_len:2][key:var][payload_len:4][payload:var]
type protoRequest struct {
	Op      uint8
	Flags   uint8
	Key     string
	Payload []byte
}

// Response represents a cache protocol response message.
// Wire format: [status:1][flags:1][payload_len:4][payload:var]
type protoResponse struct {
	Status  uint8
	Flags   uint8
	Payload []byte
}

func encodeRequest(req *protoRequest) []byte {
	keyLen := len(req.Key)
	payloadLen := len(req.Payload)
	buf := make([]byte, 1+1+2+keyLen+4+payloadLen)
	buf[0] = req.Op
	buf[1] = req.Flags
	binary.LittleEndian.PutUint16(buf[2:4], uint16(keyLen))
	copy(buf[4:4+keyLen], req.Key)
	binary.LittleEndian.PutUint32(buf[4+keyLen:8+keyLen], uint32(payloadLen))
	if payloadLen > 0 {
		copy(buf[8+keyLen:], req.Payload)
	}
	return buf
}

func decodeRequest(r io.Reader) (*protoRequest, error) {
	hdr := make([]byte, 4) // op + flags + key_len
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, err
	}
	req := &protoRequest{
		Op:    hdr[0],
		Flags: hdr[1],
	}
	keyLen := binary.LittleEndian.Uint16(hdr[2:4])
	if keyLen > 0 {
		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBuf); err != nil {
			return nil, err
		}
		req.Key = string(keyBuf)
	}
	var plenBuf [4]byte
	if _, err := io.ReadFull(r, plenBuf[:]); err != nil {
		return nil, err
	}
	payloadLen := binary.LittleEndian.Uint32(plenBuf[:])
	if payloadLen > 0 {
		req.Payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(r, req.Payload); err != nil {
			return nil, err
		}
	}
	return req, nil
}

func encodeResponse(resp *protoResponse) []byte {
	payloadLen := len(resp.Payload)
	buf := make([]byte, 1+1+4+payloadLen)
	buf[0] = resp.Status
	buf[1] = resp.Flags
	binary.LittleEndian.PutUint32(buf[2:6], uint32(payloadLen))
	if payloadLen > 0 {
		copy(buf[6:], resp.Payload)
	}
	return buf
}

func decodeResponse(r io.Reader) (*protoResponse, error) {
	hdr := make([]byte, 6) // status + flags + payload_len
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, err
	}
	resp := &protoResponse{
		Status: hdr[0],
		Flags:  hdr[1],
	}
	payloadLen := binary.LittleEndian.Uint32(hdr[2:6])
	if payloadLen > 0 {
		resp.Payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(r, resp.Payload); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

// sendFD sends file descriptors over a Unix domain socket using SCM_RIGHTS.
// msg is a small metadata payload sent alongside the FDs.
func sendFD(conn *net.UnixConn, msg []byte, fds ...int) error {
	if len(fds) == 0 {
		return nil
	}
	viaf, err := conn.File()
	if err != nil {
		return fmt.Errorf("get socket fd: %w", err)
	}
	defer viaf.Close()
	socket := int(viaf.Fd())
	rights := syscall.UnixRights(fds...)
	return syscall.Sendmsg(socket, msg, rights, nil, 0)
}

// recvFD receives file descriptors from a Unix domain socket.
// Returns the metadata message and the received FDs.
func recvFD(conn *net.UnixConn, numFDs int) ([]byte, []int, error) {
	if numFDs < 1 {
		return nil, nil, nil
	}
	viaf, err := conn.File()
	if err != nil {
		return nil, nil, fmt.Errorf("get socket fd: %w", err)
	}
	defer viaf.Close()
	socket := int(viaf.Fd())

	msg := make([]byte, 64)
	oob := make([]byte, syscall.CmsgSpace(numFDs*4))
	n, oobn, _, _, err := syscall.Recvmsg(socket, msg, oob, 0)
	if err != nil {
		return nil, nil, err
	}

	msgs, err := syscall.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return nil, nil, err
	}

	fds := make([]int, 0, len(msgs))
	for _, m := range msgs {
		rights, err := syscall.ParseUnixRights(&m)
		if err != nil {
			for _, fd := range fds {
				syscall.Close(fd)
			}
			return nil, nil, err
		}
		fds = append(fds, rights...)
	}
	return msg[:n], fds, nil
}

// Helper to encode a uint64 as little-endian bytes
func encodeUint64(v uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, v)
	return buf
}

// Helper to decode a uint64 from little-endian bytes
func decodeUint64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}

// Helper to encode a uint32 as little-endian bytes
func encodeUint32(v uint32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, v)
	return buf
}

func decodeUint32(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}
