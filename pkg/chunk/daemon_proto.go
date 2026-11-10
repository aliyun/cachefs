/*
 * Copyright 2024 Alibaba Cloud, Inc. or its affiliates.
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
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

const (
	// Message header type
	REQ_GETBLOCK  uint8 = 1
	REQ_PUTBLOCK  uint8 = 2
	REQ_CMTBLOCK  uint8 = 3
	REQ_GETBLKSIZ uint8 = 4
	REQ_PREPARE   uint8 = 5
	REQ_COMMIT    uint8 = 6
	REQ_FILLBLOCK uint8 = 7
	REQ_START     uint8 = REQ_GETBLOCK
	REQ_END       uint8 = REQ_FILLBLOCK

	RESP_GETBLOCK  uint8 = 101
	RESP_PUTBLOCK  uint8 = 102
	RESP_CMTBLOCK  uint8 = 103
	RESP_GETBLKSIZ uint8 = 104
	RESP_PREPARE   uint8 = 105
	RESP_COMMIT    uint8 = 106
	RESP_FILLBLOCK uint8 = 107
	RESP_START     uint8 = RESP_GETBLOCK
	RESP_END       uint8 = RESP_FILLBLOCK

	// message status
	STS_OK       uint8 = 0
	STS_FAIL     uint8 = 1
	STS_NOTFOUND uint8 = 2
	STS_EXISTS   uint8 = 3

	// header size
	HeaderSize int = 8

	// Connection initial message size
	InitKeySize int = 8
)

type MsgHdr struct {
	Type   uint8  // message type
	Status uint8  // response message status
	Length uint16 // message payload length
	Id     uint32 // unique message ID
}

type ReqInfo struct {
	Target    string `json:"target,omitempty"`     // cache target
	Length    uint64 `json:"length,omitempty"`     // total length of target
	BlockNum  uint32 `json:"block_num,omitempty"`  // block number of target
	Size      uint32 `json:"size,omitempty"`       // actual size of this block
	BlockSize uint32 `json:"block_size,omitempty"` // cache block size

	// used by rund cachefs device
	Index uint32 `json:"index,omitempty"` // block index in DAX window
}

type RespInfo struct {
	// size of requested cache data.
	Size uint64 `json:"size,omitempty"`
	// Status message on error
	Message string `json:"message,omitempty"`
}

type Req struct {
	Header *MsgHdr
	Info   []*ReqInfo
}

type Resp struct {
	Header *MsgHdr
	Info   *RespInfo
	File   *os.File
}

// caller ensures buf has HeaderSize len
func ParseMessageHeader(buf []byte) *MsgHdr {
	if len(buf) != HeaderSize {
		panic(fmt.Errorf("unexpected header buf length %d, expect %d", len(buf), HeaderSize))
	}
	return &MsgHdr{
		Type:   buf[0],
		Status: buf[1],
		Length: binary.LittleEndian.Uint16(buf[2:4]),
		Id:     binary.LittleEndian.Uint32(buf[4:8]),
	}
}

// caller ensures buf has HeaderSize len
func (h *MsgHdr) ToBytes(buf []byte) error {
	if len(buf) != HeaderSize {
		panic(fmt.Errorf("unexpected header buf length %d, expect %d", len(buf), HeaderSize))
	}
	buffer := bytes.NewBuffer(buf[:0])
	return binary.Write(buffer, binary.LittleEndian, h)
}

func (h *MsgHdr) IsRequest() bool {
	return h.Type >= REQ_START && h.Type <= REQ_END
}

func (h *MsgHdr) IsResponse() bool {
	return h.Type >= RESP_START && h.Type <= RESP_END
}

func (h *MsgHdr) IsValidStatus() bool {
	return h.Status == STS_OK ||
		h.Status == STS_FAIL ||
		h.Status == STS_NOTFOUND ||
		h.Status == STS_EXISTS

}

func (h *MsgHdr) IsValid() bool {
	return (h.IsRequest() || h.IsResponse()) && h.IsValidStatus()
}
