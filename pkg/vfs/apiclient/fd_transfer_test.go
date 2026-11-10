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

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFdTransferMsgHeaderSerialization(t *testing.T) {
	tests := []struct {
		name   string
		header FdTransferMsgHeader
	}{
		{
			name:   "request header",
			header: FdTransferMsgHeader{Type: FdTransferRequest, Length: 100, Reserved: 0},
		},
		{
			name:   "response header",
			header: FdTransferMsgHeader{Type: FdTransferResponse, Length: 50, Reserved: 1},
		},
		{
			name:   "error header",
			header: FdTransferMsgHeader{Type: FdTransferError, Length: 200, Reserved: 255},
		},
		{
			name:   "max length header",
			header: FdTransferMsgHeader{Type: FdTransferResponse, Length: 65535, Reserved: 128},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, FdTransferMsgHeaderSize)
			buf[0] = tt.header.Type
			buf[1] = byte(tt.header.Length & 0xff)
			buf[2] = byte((tt.header.Length >> 8) & 0xff)
			buf[3] = tt.header.Reserved

			assert.Equal(t, tt.header.Type, buf[0])
			assert.Equal(t, tt.header.Length, uint16(buf[1])|uint16(buf[2])<<8)
			assert.Equal(t, tt.header.Reserved, buf[3])

			parsed := FdTransferMsgHeader{
				Type:     buf[0],
				Length:   uint16(buf[1]) | uint16(buf[2])<<8,
				Reserved: buf[3],
			}
			assert.Equal(t, tt.header, parsed)
		})
	}
}

func TestFdTransferRequestMsgSerialization(t *testing.T) {
	tests := []struct {
		name string
		req  FdTransferRequestMsg
	}{
		{"basic request", FdTransferRequestMsg{RequestId: 12345, RangeStart: 0, RangeLen: 0}},
		{"request with range", FdTransferRequestMsg{RequestId: 67890, RangeStart: 1024, RangeLen: 2048}},
		{"request with negative range", FdTransferRequestMsg{RequestId: 99999, RangeStart: -1, RangeLen: -1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.req)
			assert.NoError(t, err)
			assert.NotEmpty(t, data)

			var parsed FdTransferRequestMsg
			err = json.Unmarshal(data, &parsed)
			assert.NoError(t, err)
			assert.Equal(t, tt.req, parsed)
		})
	}
}

func TestFdTransferResponseMsgSerialization(t *testing.T) {
	tests := []struct {
		name string
		resp FdTransferResponseMsg
	}{
		{"basic response", FdTransferResponseMsg{RequestId: 12345}},
		{"zero request id", FdTransferResponseMsg{RequestId: 0}},
		{"max request id", FdTransferResponseMsg{RequestId: 4294967295}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.resp)
			assert.NoError(t, err)
			assert.NotEmpty(t, data)

			var parsed FdTransferResponseMsg
			err = json.Unmarshal(data, &parsed)
			assert.NoError(t, err)
			assert.Equal(t, tt.resp, parsed)
		})
	}
}

func TestFdTransferConstants(t *testing.T) {
	assert.Equal(t, uint8(1), FdTransferRequest)
	assert.Equal(t, uint8(2), FdTransferResponse)
	assert.Equal(t, uint8(3), FdTransferError)
	assert.Equal(t, 8, FdTransferMsgHeaderSize)

	types := []uint8{FdTransferRequest, FdTransferResponse, FdTransferError}
	for i := 0; i < len(types); i++ {
		for j := i + 1; j < len(types); j++ {
			assert.NotEqual(t, types[i], types[j])
		}
	}
}

func TestMaxFDCountConstant(t *testing.T) {
	assert.Equal(t, 128, MaxFDCount)
	assert.Greater(t, MaxFDCount, 0)
	assert.LessOrEqual(t, MaxFDCount, 1024)
}

func TestFdTransferMsgHeaderBounds(t *testing.T) {
	headers := []FdTransferMsgHeader{
		{Type: 0, Length: 0, Reserved: 0},
		{Type: 255, Length: 65535, Reserved: 255},
	}
	for _, header := range headers {
		buf := make([]byte, FdTransferMsgHeaderSize)
		buf[0] = header.Type
		buf[1] = byte(header.Length & 0xff)
		buf[2] = byte((header.Length >> 8) & 0xff)
		buf[3] = header.Reserved

		parsed := FdTransferMsgHeader{
			Type:     buf[0],
			Length:   uint16(buf[1]) | uint16(buf[2])<<8,
			Reserved: buf[3],
		}
		assert.Equal(t, header, parsed)
	}
}
