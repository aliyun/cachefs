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

package vfs

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestFdTransferMsgHeaderSerialization(t *testing.T) {
	tests := []struct {
		name   string
		header FdTransferMsgHeader
	}{
		{
			name: "request header",
			header: FdTransferMsgHeader{
				Type:     FdTransferRequest,
				Length:   100,
				Reserved: 0,
			},
		},
		{
			name: "response header",
			header: FdTransferMsgHeader{
				Type:     FdTransferResponse,
				Length:   50,
				Reserved: 1,
			},
		},
		{
			name: "error header",
			header: FdTransferMsgHeader{
				Type:     FdTransferError,
				Length:   200,
				Reserved: 255,
			},
		},
		{
			name: "max length header",
			header: FdTransferMsgHeader{
				Type:     FdTransferResponse,
				Length:   65535, // max uint16
				Reserved: 128,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test serialization by simulating header creation
			buf := make([]byte, FdTransferMsgHeaderSize)
			buf[0] = tt.header.Type
			buf[1] = byte(tt.header.Length & 0xff)
			buf[2] = byte((tt.header.Length >> 8) & 0xff)
			buf[3] = tt.header.Reserved

			// Verify buffer content
			assert.Equal(t, tt.header.Type, buf[0])
			assert.Equal(t, tt.header.Length, uint16(buf[1])|uint16(buf[2])<<8)
			assert.Equal(t, tt.header.Reserved, buf[3])

			// Test deserialization
			parsedHeader := FdTransferMsgHeader{
				Type:     buf[0],
				Length:   uint16(buf[1]) | uint16(buf[2])<<8,
				Reserved: buf[3],
			}
			assert.Equal(t, tt.header, parsedHeader)
		})
	}
}

func TestFdTransferRequestMsgSerialization(t *testing.T) {
	tests := []struct {
		name string
		req  FdTransferRequestMsg
	}{
		{
			name: "basic request",
			req: FdTransferRequestMsg{
				RequestId:  12345,
				RangeStart: 0,
				RangeLen:   0,
			},
		},
		{
			name: "request with range",
			req: FdTransferRequestMsg{
				RequestId:  67890,
				RangeStart: 1024,
				RangeLen:   2048,
			},
		},
		{
			name: "request with negative range",
			req: FdTransferRequestMsg{
				RequestId:  99999,
				RangeStart: -1,
				RangeLen:   -1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test JSON serialization
			data, err := json.Marshal(tt.req)
			assert.NoError(t, err)
			assert.NotEmpty(t, data)

			// Test JSON deserialization
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
		{
			name: "basic response",
			resp: FdTransferResponseMsg{
				RequestId: 12345,
			},
		},
		{
			name: "zero request id",
			resp: FdTransferResponseMsg{
				RequestId: 0,
			},
		},
		{
			name: "max request id",
			resp: FdTransferResponseMsg{
				RequestId: 4294967295, // max uint32
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test JSON serialization
			data, err := json.Marshal(tt.resp)
			assert.NoError(t, err)
			assert.NotEmpty(t, data)

			// Test JSON deserialization
			var parsed FdTransferResponseMsg
			err = json.Unmarshal(data, &parsed)
			assert.NoError(t, err)
			assert.Equal(t, tt.resp, parsed)
		})
	}
}

func TestFdChunkInfoClose(t *testing.T) {
	// Create a temporary file for testing
	tmpFile, err := os.CreateTemp("", "test_fd_chunk")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	// Close tmpFile so its *os.File finalizer is cleared; otherwise the finalizer
	// would later close the fd number (which FdChunkInfo.Close already closed and
	// may have been reused by another file), causing sporadic EBADF elsewhere.
	defer tmpFile.Close()

	// FdChunkInfo takes ownership of a raw fd and closes it via unix.Close, so hand
	// it an independent dup rather than the fd still owned by tmpFile.
	fd, err := unix.Dup(int(tmpFile.Fd()))
	require.NoError(t, err)

	chunk := &FdChunkInfo{
		BlockIndex: 0,
		fd:         fd,
		reader:     nil, // We'll test with nil reader
	}

	// Test close with valid fd
	chunk.Close()
	assert.Equal(t, -1, chunk.fd)
	assert.Nil(t, chunk.reader)

	// Test close again (should not panic)
	chunk.Close()
	assert.Equal(t, -1, chunk.fd)
}

func TestFdChunkInfoCloseWithInvalidFd(t *testing.T) {
	chunk := &FdChunkInfo{
		BlockIndex: 0,
		fd:         -1, // Invalid fd
		reader:     nil,
	}

	// Should not panic with invalid fd
	chunk.Close()
	assert.Equal(t, -1, chunk.fd)
}

type blockingListener struct {
	acceptStarted chan struct{}
	closed        chan struct{}
	acceptOnce    sync.Once
	closeOnce     sync.Once
}

func (l *blockingListener) Accept() (net.Conn, error) {
	l.acceptOnce.Do(func() { close(l.acceptStarted) })
	<-l.closed
	return nil, net.ErrClosed
}

func (l *blockingListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}

func (l *blockingListener) Addr() net.Addr {
	return &net.UnixAddr{Name: "blocking", Net: "unix"}
}

func TestFdTransferServerStopUnblocksAccept(t *testing.T) {
	listener := &blockingListener{
		acceptStarted: make(chan struct{}),
		closed:        make(chan struct{}),
	}
	server := &fdTransferServer{
		listener: listener,
		fds:      make(map[uint32]*fdTransferInfo),
		stopCh:   make(chan struct{}),
	}
	serveDone := make(chan struct{})
	go func() {
		server.serve()
		close(serveDone)
	}()

	select {
	case <-listener.acceptStarted:
	case <-time.After(time.Second):
		t.Fatal("serve did not enter Accept")
	}
	server.stop()
	select {
	case <-serveDone:
	case <-time.After(time.Second):
		t.Fatal("serve did not exit after stop")
	}

	server.Lock()
	assert.True(t, server.stopped)
	assert.Nil(t, server.listener)
	assert.Empty(t, server.fds)
	server.Unlock()
}

func TestFdTransferServerCreation(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test_fd.sock")

	server, err := newFdTransferServer(socketPath)
	assert.NoError(t, err)
	assert.NotNil(t, server)
	server.Lock()
	assert.NotNil(t, server.listener)
	assert.NotNil(t, server.fds)
	assert.NotNil(t, server.stopCh)
	server.Unlock()

	// Test server stop
	server.stop()
	server.Lock()
	assert.Nil(t, server.listener)
	server.Unlock()

	// Verify socket file is removed
	_, err = os.Stat(socketPath)
	assert.True(t, os.IsNotExist(err))
}

func TestFdTransferServerCreationFailure(t *testing.T) {
	// Test creation failure with invalid path
	server, err := newFdTransferServer("/invalid/path/that/does/not/exist/test.sock")
	assert.Error(t, err)
	assert.Nil(t, server)
}

func TestFdTransferServerAddRequest(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test_fd.sock")

	server, err := newFdTransferServer(socketPath)
	require.NoError(t, err)
	defer server.stop()

	// Create test chunks
	chunks := []*FdChunkInfo{
		{
			BlockIndex: 0,
			fd:         -1, // Use invalid fd for testing
		},
		{
			BlockIndex: 1,
			fd:         -1,
		},
	}

	// Add request
	requestId := uint32(12345)
	respSize, err := server.addRequest(requestId, "test-target", false, nil, nil, time.Now(), chunks)
	assert.NoError(t, err)
	assert.Greater(t, respSize, 0)

	// Verify request is stored
	server.Lock()
	fdInfo, exists := server.fds[requestId]
	server.Unlock()
	assert.True(t, exists)
	assert.NotNil(t, fdInfo)
	assert.Equal(t, requestId, fdInfo.requestId)
	assert.Equal(t, len(chunks), len(fdInfo.chunks))
	assert.Equal(t, "test-target", fdInfo.target)
	assert.False(t, fdInfo.claimed)
	assert.True(t, fdInfo.claimedAt.IsZero())
	assert.True(t, fdInfo.expireAt.After(time.Now()))
}

// A GetFileChunks handler can still be loading blocks when the server stops:
// ttrpc's Close does not drain in-flight handlers. Such a late addRequest must
// be rejected rather than parked in an already-drained map, and it must not
// release the target itself (its handler rolls back lock and budget).
func TestFdTransferServerAddRequestAfterStop(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "test_fd.sock")

	server, err := newFdTransferServer(socketPath)
	require.NoError(t, err)

	var released int
	server.onRelease = func(_ *fdTransferInfo, _ string) {
		released++
	}

	server.stop()
	// stop() is idempotent: apiServer.stop() keeps the pointer around now.
	server.stop()

	_, err = server.addRequest(42, "test-target", false, nil, nil, time.Now(), []*FdChunkInfo{{BlockIndex: 0, fd: -1}})
	assert.Error(t, err)

	server.Lock()
	assert.Empty(t, server.fds)
	server.Unlock()
	assert.Equal(t, 0, released)
}

func TestFdTransferServerTakeRequest(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test_fd.sock")

	server, err := newFdTransferServer(socketPath)
	require.NoError(t, err)
	defer server.stop()

	requestId := uint32(12345)
	chunks := []*FdChunkInfo{{BlockIndex: 0, fd: -1}}

	// Add request
	_, err = server.addRequest(requestId, "test-target", false, nil, nil, time.Now(), chunks)
	require.NoError(t, err)

	// Verify request exists
	server.Lock()
	_, exists := server.fds[requestId]
	server.Unlock()
	assert.True(t, exists)

	// Take request: first taker owns it, second gets nil
	fdInfo := server.takeRequest(requestId)
	assert.NotNil(t, fdInfo)
	assert.Nil(t, server.takeRequest(requestId))

	// Verify request is deleted
	server.Lock()
	_, exists = server.fds[requestId]
	server.Unlock()
	assert.False(t, exists)
}

func TestFdTransferServerCleanupExpired(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test_fd.sock")

	server, err := newFdTransferServer(socketPath)
	require.NoError(t, err)
	defer server.stop()

	// Add request with past expiration time
	requestId := uint32(12345)
	chunks := []*FdChunkInfo{{BlockIndex: 0, fd: -1}}
	_, err = server.addRequest(requestId, "test-target", false, nil, nil, time.Now(), chunks)
	require.NoError(t, err)

	// A claimed entry must be shielded from the reaper even when expired
	server.Lock()
	fdInfo := server.fds[requestId]
	fdInfo.expireAt = time.Now().Add(-time.Hour) // Expired 1 hour ago
	fdInfo.claimed = true
	server.Unlock()

	server.cleanupExpired()
	server.Lock()
	_, exists := server.fds[requestId]
	server.Unlock()
	assert.True(t, exists)

	// An unclaimed expired entry is reaped
	server.Lock()
	fdInfo.claimed = false
	server.Unlock()

	// Run cleanup
	server.cleanupExpired()

	// Verify expired request is removed
	server.Lock()
	_, exists = server.fds[requestId]
	server.Unlock()
	assert.False(t, exists)
}

func TestMaxFDCountConstant(t *testing.T) {
	// Verify MaxFDCount is within reasonable bounds
	assert.Equal(t, 128, MaxFDCount)
	assert.Greater(t, MaxFDCount, 0)
	assert.LessOrEqual(t, MaxFDCount, 1024) // Reasonable upper bound
}

func TestFdTransferConstants(t *testing.T) {
	// Test message type constants
	assert.Equal(t, uint8(1), FdTransferRequest)
	assert.Equal(t, uint8(2), FdTransferResponse)
	assert.Equal(t, uint8(3), FdTransferError)

	// Test header size constant
	assert.Equal(t, 8, FdTransferMsgHeaderSize)

	// Verify constants are unique
	types := []uint8{FdTransferRequest, FdTransferResponse, FdTransferError}
	for i := 0; i < len(types); i++ {
		for j := i + 1; j < len(types); j++ {
			assert.NotEqual(t, types[i], types[j])
		}
	}
}

func TestFdTransferInfoExpiration(t *testing.T) {
	requestId := uint32(12345)
	chunks := []*FdChunkInfo{{BlockIndex: 0, fd: -1}}

	info := &fdTransferInfo{
		requestId: requestId,
		response:  []byte("test response"),
		chunks:    chunks,
		expireAt:  time.Now().Add(5 * time.Minute),
	}

	// Test that info is not expired
	assert.False(t, time.Now().After(info.expireAt))

	// Test with past expiration
	info.expireAt = time.Now().Add(-time.Hour)
	assert.True(t, time.Now().After(info.expireAt))
}

func TestFdTransferMsgHeaderBounds(t *testing.T) {
	// Test with minimum values
	header := FdTransferMsgHeader{
		Type:     0,
		Length:   0,
		Reserved: 0,
	}
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

	// Test with maximum values
	header = FdTransferMsgHeader{
		Type:     255,   // max uint8
		Length:   65535, // max uint16
		Reserved: 255,   // max uint8
	}
	buf[0] = header.Type
	buf[1] = byte(header.Length & 0xff)
	buf[2] = byte((header.Length >> 8) & 0xff)
	buf[3] = header.Reserved

	parsed = FdTransferMsgHeader{
		Type:     buf[0],
		Length:   uint16(buf[1]) | uint16(buf[2])<<8,
		Reserved: buf[3],
	}
	assert.Equal(t, header, parsed)
}

func TestFdTransferRequestResponseCycle(t *testing.T) {
	// Test a complete request-response cycle structure
	requestId := uint32(98765)

	// Create request
	req := FdTransferRequestMsg{
		RequestId:  requestId,
		RangeStart: 0,
		RangeLen:   0,
	}

	// Serialize request
	reqData, err := json.Marshal(req)
	require.NoError(t, err)

	// Create request header
	reqHeader := FdTransferMsgHeader{
		Type:     FdTransferRequest,
		Length:   uint16(len(reqData)),
		Reserved: 0,
	}

	// Create response
	resp := FdTransferResponseMsg{
		RequestId: requestId,
	}

	// Serialize response
	respData, err := json.Marshal(resp)
	require.NoError(t, err)

	// Create response header
	respHeader := FdTransferMsgHeader{
		Type:     FdTransferResponse,
		Length:   uint16(len(respData)),
		Reserved: 0,
	}

	// Verify request and response have matching request IDs
	var parsedReq FdTransferRequestMsg
	err = json.Unmarshal(reqData, &parsedReq)
	require.NoError(t, err)

	var parsedResp FdTransferResponseMsg
	err = json.Unmarshal(respData, &parsedResp)
	require.NoError(t, err)

	assert.Equal(t, requestId, parsedReq.RequestId)
	assert.Equal(t, requestId, parsedResp.RequestId)
	assert.Equal(t, FdTransferRequest, reqHeader.Type)
	assert.Equal(t, FdTransferResponse, respHeader.Type)
}

func TestFdTransferServerRelease(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test_fd.sock")

	server, err := newFdTransferServer(socketPath)
	require.NoError(t, err)
	defer server.stop()

	// Wire a drain callback that records the entry release saw.
	var gotInfo *fdTransferInfo
	var gotSource string
	server.onRelease = func(fdInfo *fdTransferInfo, source string) {
		gotInfo, gotSource = fdInfo, source
	}

	// Build an entry with chunks (invalid fds are fine; Close is idempotent)
	// and take it so release owns it.
	chunks := []*FdChunkInfo{{BlockIndex: 0, fd: -1}, {BlockIndex: 1, fd: -1}}
	lease := &shareLease{target: "t-drain", blocks: []shareBlock{{index: 0, bytes: 4}, {index: 1, bytes: 4}}}
	start := time.Now()
	_, err = server.addRequest(42, "t-drain", true, lease, nil, start, chunks)
	require.NoError(t, err)
	fdInfo := server.takeRequest(42)
	require.NotNil(t, fdInfo)

	server.release(fdInfo, shareReleaseEof)

	require.Same(t, fdInfo, gotInfo)
	assert.Equal(t, shareReleaseEof, gotSource)
	assert.Equal(t, "t-drain", gotInfo.target)
	assert.True(t, gotInfo.allBlocks)
	assert.Same(t, lease, gotInfo.shareLease)
	assert.Equal(t, start, gotInfo.shareStart)
	// chunks are closed and nilled
	assert.Nil(t, fdInfo.chunks)
}

func TestFdTransferServerReleaseNoCallback(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test_fd.sock")

	server, err := newFdTransferServer(socketPath)
	require.NoError(t, err)
	defer server.stop()

	// No onRelease set: release must still close chunks without panicking.
	chunks := []*FdChunkInfo{{BlockIndex: 0, fd: -1}}
	_, err = server.addRequest(7, "t-nocb", false, nil, nil, time.Now(), chunks)
	require.NoError(t, err)
	fdInfo := server.takeRequest(7)
	require.NotNil(t, fdInfo)

	assert.NotPanics(t, func() { server.release(fdInfo, shareReleaseStop) })
	assert.Nil(t, fdInfo.chunks)
}

// blockPoolTestSize is the pool block size used by the export-accounting tests.
// It is deliberately not a size anything else allocates, so an enabled pool can
// never hand a recycled block to an unrelated test in this package.
const blockPoolTestSize = 4104

var exportTestPoolOnce sync.Once

func enableExportTestPool() {
	exportTestPoolOnce.Do(func() {
		utils.SetupBlockPool(&utils.BlockPoolConfig{BlockSize: blockPoolTestSize, Slots: 8, Reuse: true, Memfd: true})
	})
}

// newExportTestServer starts a server with the block pool enabled, so "did the
// block reach the pool" becomes observable, which is the only external proof that
// its export count came back to zero.
func newExportTestServer(t *testing.T) *fdTransferServer {
	t.Helper()
	enableExportTestPool()
	server, err := newFdTransferServer(filepath.Join(t.TempDir(), "test_fd.sock"))
	require.NoError(t, err)
	return server
}

// exportedChunk builds what GetFileChunks builds for a memfd-backed block: an
// exported fd plus the Memfd that counts it.
func exportedChunk(t *testing.T, m *utils.Memfd, idx uint32) *FdChunkInfo {
	t.Helper()
	fd, err := m.ExportFd()
	require.NoError(t, err)
	return &FdChunkInfo{BlockIndex: idx, fd: fd, memfd: m}
}

// TestFdTransferExportAccounting walks the release sources of one exported block.
// Freeing the block after an authoritative release must pool it; after an
// ambiguous one it must not, because a client mapping may still be live.
func TestFdTransferExportAccounting(t *testing.T) {
	tests := []struct {
		name   string
		pooled bool
		// drive performs the release for this row, given a server holding the
		// entry under requestId.
		drive func(t *testing.T, s *fdTransferServer, requestId uint32)
	}{
		{
			name:   "release rpc",
			pooled: true,
			drive: func(t *testing.T, s *fdTransferServer, requestId uint32) {
				defer s.stop()
				fdInfo := s.takeRequest(requestId)
				require.NotNil(t, fdInfo)
				s.release(fdInfo, shareReleaseRpc)
			},
		},
		{
			name:   "expired before pickup",
			pooled: true,
			drive: func(t *testing.T, s *fdTransferServer, requestId uint32) {
				defer s.stop()
				s.Lock()
				s.fds[requestId].expireAt = time.Now().Add(-time.Hour)
				s.Unlock()
				s.cleanupExpired()
			},
		},
		{
			name:   "connection eof",
			pooled: false,
			drive: func(t *testing.T, s *fdTransferServer, requestId uint32) {
				defer s.stop()
				fdInfo := s.takeRequest(requestId)
				require.NotNil(t, fdInfo)
				s.release(fdInfo, shareReleaseEof)
			},
		},
		{
			name:   "server stop",
			pooled: false,
			drive: func(t *testing.T, s *fdTransferServer, requestId uint32) {
				s.stop()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := newExportTestServer(t)
			m, err := utils.MemfdAlloc("export", blockPoolTestSize)
			require.NoError(t, err)

			requestId := uint32(1)
			_, err = server.addRequest(requestId, "t-export", false, nil, nil, time.Now(),
				[]*FdChunkInfo{exportedChunk(t, m, 0)})
			require.NoError(t, err)
			server.Lock()
			require.Len(t, server.fds[requestId].exported, 1)
			server.Unlock()

			tt.drive(t, server, requestId)

			cached := utils.GetBlockPoolStats().Cached
			require.NoError(t, m.Free())
			if tt.pooled {
				assert.Equal(t, cached+1, utils.GetBlockPoolStats().Cached,
					"an authoritatively released block must be reusable")
			} else {
				assert.Equal(t, cached, utils.GetBlockPoolStats().Cached,
					"a block whose export is unproven must never be reused")
			}
		})
	}
}

// TestFdTransferExportAccountingRollback covers the GetFileChunks error path,
// where the exported fds never leave this process: the rollback gives the count
// back, so the block stays reusable.
func TestFdTransferExportAccountingRollback(t *testing.T) {
	enableExportTestPool()
	m, err := utils.MemfdAlloc("rollback", blockPoolTestSize)
	require.NoError(t, err)

	// What the rollback defer in GetFileChunks does for each built chunk.
	fdChunk := exportedChunk(t, m, 0)
	fdChunk.Close()
	fdChunk.memfd.UnexportFd()

	cached := utils.GetBlockPoolStats().Cached
	require.NoError(t, m.Free())
	assert.Equal(t, cached+1, utils.GetBlockPoolStats().Cached)
}

// TestFdTransferExportAccountingShared covers two requests exporting the same
// block: the block is reusable only once every one of them has been released
// authoritatively.
func TestFdTransferExportAccountingShared(t *testing.T) {
	server := newExportTestServer(t)
	defer server.stop()
	m, err := utils.MemfdAlloc("shared", blockPoolTestSize)
	require.NoError(t, err)

	_, err = server.addRequest(1, "t-a", false, nil, nil, time.Now(), []*FdChunkInfo{exportedChunk(t, m, 0)})
	require.NoError(t, err)
	_, err = server.addRequest(2, "t-b", false, nil, nil, time.Now(), []*FdChunkInfo{exportedChunk(t, m, 0)})
	require.NoError(t, err)

	// The first client releases properly, the second is still mapped.
	first := server.takeRequest(1)
	require.NotNil(t, first)
	server.release(first, shareReleaseRpc)

	cached := utils.GetBlockPoolStats().Cached
	require.NoError(t, m.Free())
	assert.Equal(t, cached, utils.GetBlockPoolStats().Cached,
		"one outstanding export is enough to keep the block out of the pool")
}
