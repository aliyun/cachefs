/*
 * Copyright 2025 Alibaba Cloud, Inc. or its affiliates.
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

package apiclient

const (
	// FdTransferRequest is the message type for a client fd-transfer request.
	FdTransferRequest uint8 = 1
	// FdTransferResponse is the message type for a server fd-transfer response.
	FdTransferResponse uint8 = 2
	// FdTransferError is the message type for a server fd-transfer error.
	FdTransferError uint8 = 3

	// FdTransferMsgHeaderSize is the size in bytes of the fd-transfer message header.
	FdTransferMsgHeaderSize = 8

	// MaxFDCount is the maximum number of file descriptors transferred in one batch.
	MaxFDCount int = 128
)

// FdTransferRequestMsg is the JSON body of a client fd-transfer request.
type FdTransferRequestMsg struct {
	RequestId  uint32 `json:"request_id"`
	RangeStart int    `json:"range_start"`
	RangeLen   int    `json:"range_len"`
}

// FdTransferResponseMsg is the JSON body of a server fd-transfer response.
type FdTransferResponseMsg struct {
	RequestId uint32 `json:"request_id"`
}

// FdTransferMsgHeader is the fixed-size header prepended to every fd-transfer message.
type FdTransferMsgHeader struct {
	Type     uint8  `json:"type"`
	Length   uint16 `json:"length"`
	Reserved uint8  `json:"reserved"`
}
