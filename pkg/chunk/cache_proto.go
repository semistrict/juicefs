//go:build linux

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
	"io"
	"net"
	"syscall"

	"github.com/cloudwego/shmipc-go"
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
// Wire format: [op:1][flags:1][client_id_len:1][client_id:var][key_len:2][key:var][payload_len:4][payload:var]
type protoRequest struct {
	Op       uint8
	Flags    uint8
	ClientID string
	Key      string
	Payload  []byte
}

// Response represents a cache protocol response message.
// Wire format: [status:1][flags:1][payload_len:4][payload:var]
type protoResponse struct {
	Status  uint8
	Flags   uint8
	Payload []byte
}

func encodeRequest(req *protoRequest) []byte {
	cidLen := len(req.ClientID)
	keyLen := len(req.Key)
	payloadLen := len(req.Payload)
	buf := make([]byte, 1+1+1+cidLen+2+keyLen+4+payloadLen)
	buf[0] = req.Op
	buf[1] = req.Flags
	buf[2] = uint8(cidLen)
	off := 3
	copy(buf[off:off+cidLen], req.ClientID)
	off += cidLen
	binary.LittleEndian.PutUint16(buf[off:off+2], uint16(keyLen))
	off += 2
	copy(buf[off:off+keyLen], req.Key)
	off += keyLen
	binary.LittleEndian.PutUint32(buf[off:off+4], uint32(payloadLen))
	off += 4
	if payloadLen > 0 {
		copy(buf[off:], req.Payload)
	}
	return buf
}

func decodeRequest(r io.Reader) (*protoRequest, error) {
	hdr := make([]byte, 3) // op + flags + client_id_len
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, err
	}
	req := &protoRequest{
		Op:    hdr[0],
		Flags: hdr[1],
	}
	cidLen := int(hdr[2])
	if cidLen > 0 {
		cidBuf := make([]byte, cidLen)
		if _, err := io.ReadFull(r, cidBuf); err != nil {
			return nil, err
		}
		req.ClientID = string(cidBuf)
	}
	var keyLenBuf [2]byte
	if _, err := io.ReadFull(r, keyLenBuf[:]); err != nil {
		return nil, err
	}
	keyLen := binary.LittleEndian.Uint16(keyLenBuf[:])
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
func sendFD(conn *net.UnixConn, msg []byte, fds ...int) error {
	if len(fds) == 0 {
		return nil
	}
	viaf, err := conn.File()
	if err != nil {
		return err
	}
	defer viaf.Close()
	rights := syscall.UnixRights(fds...)
	return syscall.Sendmsg(int(viaf.Fd()), msg, rights, nil, 0)
}

// recvFD receives file descriptors from a Unix domain socket.
// Uses ReadMsgUnix which respects Go deadlines (needed for clean shutdown).
func recvFD(conn *net.UnixConn, numFDs int) ([]byte, []int, error) {
	if numFDs < 1 {
		return nil, nil, nil
	}
	msg := make([]byte, 128)
	oob := make([]byte, syscall.CmsgSpace(numFDs*4))
	n, oobn, _, _, err := conn.ReadMsgUnix(msg, oob)
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

// writeClientID writes a length-prefixed client ID to a connection.
func writeClientID(conn net.Conn, id string) error {
	buf := make([]byte, 2+len(id))
	binary.LittleEndian.PutUint16(buf[0:2], uint16(len(id)))
	copy(buf[2:], id)
	_, err := conn.Write(buf)
	return err
}

// readClientID reads a length-prefixed client ID from a connection.
func readClientID(conn net.Conn) (string, error) {
	var lenBuf [2]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return "", err
	}
	idLen := binary.LittleEndian.Uint16(lenBuf[:])
	idBuf := make([]byte, idLen)
	if _, err := io.ReadFull(conn, idBuf); err != nil {
		return "", err
	}
	return string(idBuf), nil
}

func encodeUint64(v uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, v)
	return buf
}

func decodeUint64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}

func encodeUint32(v uint32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, v)
	return buf
}

func decodeUint32(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}

// --- shmipc zero-copy encode/decode ---

// encodeRequestToShm writes a request directly into shared memory via Reserve.
func encodeRequestToShm(req *protoRequest, w shmipc.BufferWriter) error {
	cidLen := len(req.ClientID)
	keyLen := len(req.Key)
	payloadLen := len(req.Payload)

	hdr, err := w.Reserve(3)
	if err != nil {
		return err
	}
	hdr[0] = req.Op
	hdr[1] = req.Flags
	hdr[2] = uint8(cidLen)

	if cidLen > 0 {
		if err := w.WriteString(req.ClientID); err != nil {
			return err
		}
	}

	kl, err := w.Reserve(2)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint16(kl, uint16(keyLen))
	if keyLen > 0 {
		if err := w.WriteString(req.Key); err != nil {
			return err
		}
	}

	pl, err := w.Reserve(4)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(pl, uint32(payloadLen))
	if payloadLen > 0 {
		if _, err := w.WriteBytes(req.Payload); err != nil {
			return err
		}
	}
	return nil
}

// encodeResponseToShm writes a response directly into shared memory via Reserve.
func encodeResponseToShm(resp *protoResponse, w shmipc.BufferWriter) error {
	payloadLen := len(resp.Payload)
	hdr, err := w.Reserve(6)
	if err != nil {
		return err
	}
	hdr[0] = resp.Status
	hdr[1] = resp.Flags
	binary.LittleEndian.PutUint32(hdr[2:6], uint32(payloadLen))
	if payloadLen > 0 {
		if _, err := w.WriteBytes(resp.Payload); err != nil {
			return err
		}
	}
	return nil
}

// writeResponseToShm writes a response with an optional uint64 payload directly
// into shared memory in a single Reserve call — zero heap allocations.
func writeResponseToShm(w shmipc.BufferWriter, status, flags uint8, u64payload ...uint64) error {
	payloadLen := len(u64payload) * 8
	hdr, err := w.Reserve(6 + payloadLen)
	if err != nil {
		return err
	}
	hdr[0] = status
	hdr[1] = flags
	binary.LittleEndian.PutUint32(hdr[2:6], uint32(payloadLen))
	for i, v := range u64payload {
		binary.LittleEndian.PutUint64(hdr[6+i*8:], v)
	}
	return nil
}

// decodeRequestFromBufInto decodes a request from shmipc shared memory into
// an existing protoRequest, avoiding struct allocation.
func decodeRequestFromBufInto(r shmipc.BufferReader, req *protoRequest) error {
	hdr, err := r.ReadBytes(3)
	if err != nil {
		return err
	}
	req.Op = hdr[0]
	req.Flags = hdr[1]
	cidLen := int(hdr[2])
	if cidLen > 0 {
		req.ClientID, err = r.ReadString(cidLen)
		if err != nil {
			return err
		}
	}
	klBuf, err := r.ReadBytes(2)
	if err != nil {
		return err
	}
	keyLen := int(binary.LittleEndian.Uint16(klBuf))
	if keyLen > 0 {
		req.Key, err = r.ReadString(keyLen)
		if err != nil {
			return err
		}
	}
	plBuf, err := r.ReadBytes(4)
	if err != nil {
		return err
	}
	payloadLen := int(binary.LittleEndian.Uint32(plBuf))
	if payloadLen > 0 {
		raw, err := r.ReadBytes(payloadLen)
		if err != nil {
			return err
		}
		req.Payload = make([]byte, payloadLen)
		copy(req.Payload, raw)
	}
	return nil
}

func decodeRequestFromBuf(r shmipc.BufferReader) (*protoRequest, error) {
	req := &protoRequest{}
	if err := decodeRequestFromBufInto(r, req); err != nil {
		return nil, err
	}
	return req, nil
}

func decodeResponseFromBuf(r shmipc.BufferReader) (*protoResponse, error) {
	hdr, err := r.ReadBytes(6)
	if err != nil {
		return nil, err
	}
	resp := &protoResponse{
		Status: hdr[0],
		Flags:  hdr[1],
	}
	payloadLen := int(binary.LittleEndian.Uint32(hdr[2:6]))
	if payloadLen > 0 {
		raw, err := r.ReadBytes(payloadLen)
		if err != nil {
			return nil, err
		}
		resp.Payload = make([]byte, payloadLen)
		copy(resp.Payload, raw)
	}
	return resp, nil
}
