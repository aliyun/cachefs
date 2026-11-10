/*
 * Copyright 2026 Alibaba Cloud, Inc. or its affiliates.
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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/aliyun/cachefs/pkg/ttrpc/cachefs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// chunkFdsSvc supplies only the two RPCs exercised by direct-FD tests while
// embedding the package's general API mock for the remaining service methods.
type chunkFdsSvc struct {
	mockSvc

	mu           sync.Mutex
	getResp      *pb.GetFileChunksResponse
	getErr       error
	releaseErr   error
	releaseHook  func()
	lastGetReq   *pb.GetFileChunksRequest
	lastRelease  *pb.ReleaseFileChunksRequest
	releaseCalls int
}

func (s *chunkFdsSvc) GetFileChunks(_ context.Context, req *pb.GetFileChunksRequest) (*pb.GetFileChunksResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastGetReq = req
	if s.getErr != nil {
		return nil, s.getErr
	}
	if s.getResp != nil {
		return s.getResp, nil
	}
	return &pb.GetFileChunksResponse{}, nil
}

func (s *chunkFdsSvc) ReleaseFileChunks(_ context.Context, req *pb.ReleaseFileChunksRequest) (*pb.ReleaseFileChunksResponse, error) {
	if s.releaseHook != nil {
		s.releaseHook()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastRelease = req
	s.releaseCalls++
	return &pb.ReleaseFileChunksResponse{}, s.releaseErr
}

func (s *chunkFdsSvc) getRequest() *pb.GetFileChunksRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastGetReq
}

func (s *chunkFdsSvc) releaseRequest() *pb.ReleaseFileChunksRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastRelease
}

func (s *chunkFdsSvc) releaseCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.releaseCalls
}

// startChunkFdTransferServer serves a single existing one-shot FD request and
// reports once the client closes its connection. It deliberately keeps the
// connection open after delivery to exercise the batch lifetime.
func startChunkFdTransferServer(t *testing.T, fds []int) (string, <-chan error) {
	t.Helper()
	listener, err := net.Listen("unix", filepath.Join(t.TempDir(), "transfer.sock"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		defer closeFDs(fds)

		header := make([]byte, FdTransferMsgHeaderSize)
		if _, err := io.ReadFull(conn, header); err != nil {
			done <- err
			return
		}
		if header[0] != FdTransferRequest {
			done <- fmt.Errorf("unexpected request type: %d", header[0])
			return
		}
		bodyLen := int(uint16(header[1]) | uint16(header[2])<<8)
		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(conn, body); err != nil {
			done <- err
			return
		}
		var request FdTransferRequestMsg
		if err := json.Unmarshal(body, &request); err != nil {
			done <- err
			return
		}

		responseBody, err := json.Marshal(&FdTransferResponseMsg{RequestId: request.RequestId})
		if err != nil {
			done <- err
			return
		}
		message := make([]byte, FdTransferMsgHeaderSize+len(responseBody))
		message[0] = FdTransferResponse
		message[1] = byte(len(responseBody))
		message[2] = byte(len(responseBody) >> 8)
		copy(message[FdTransferMsgHeaderSize:], responseBody)

		unixConn := conn.(*net.UnixConn)
		for offset := 0; offset < len(fds); {
			end := offset + MaxFDCount
			if end > len(fds) {
				end = len(fds)
			}
			oob := unix.UnixRights(fds[offset:end]...)
			n, oobn, err := unixConn.WriteMsgUnix(message, oob, nil)
			if err != nil {
				done <- err
				return
			}
			if n != len(message) || oobn != len(oob) {
				done <- fmt.Errorf("partial fd response write: data %d/%d, oob %d/%d", n, len(message), oobn, len(oob))
				return
			}
			offset = end
		}

		_, err = io.Copy(io.Discard, conn)
		if err != nil {
			done <- err
			return
		}
		done <- nil
	}()
	return listener.Addr().String(), done
}

func fdResponseSize(t *testing.T, requestID uint32) uint32 {
	t.Helper()
	body, err := json.Marshal(&FdTransferResponseMsg{RequestId: requestID})
	require.NoError(t, err)
	return uint32(FdTransferMsgHeaderSize + len(body))
}

func TestChunkFdViews(t *testing.T) {
	const blockSize = uint32(4)
	fullResponse := func() *pb.GetFileChunksResponse {
		return &pb.GetFileChunksResponse{
			FileSize:  10,
			BlockSize: blockSize,
			Chunks: []*pb.FileChunk{
				{BlockIndex: 0, Size: blockSize},
				{BlockIndex: 1, Size: blockSize},
				{BlockIndex: 2, Size: 2},
			},
		}
	}

	tests := []struct {
		name      string
		resp      *pb.GetFileChunksResponse
		fds       []int
		fileRange *FileRange
		want      []ChunkFd
		wantErr   string
	}{
		{
			name: "full file",
			resp: fullResponse(),
			fds:  []int{10, 11, 12},
			want: []ChunkFd{
				{fd: 10, BlockIndex: 0, Size: 4, FileOffset: 0, DataOffset: 0, DataLen: 4},
				{fd: 11, BlockIndex: 1, Size: 4, FileOffset: 4, DataOffset: 0, DataLen: 4},
				{fd: 12, BlockIndex: 2, Size: 2, FileOffset: 8, DataOffset: 0, DataLen: 2},
			},
		},
		{
			name:      "range spans blocks",
			resp:      fullResponse(),
			fds:       []int{10, 11, 12},
			fileRange: &FileRange{Start: 6, Length: 4},
			want: []ChunkFd{
				{fd: 11, BlockIndex: 1, Size: 4, FileOffset: 4, DataOffset: 2, DataLen: 2},
				{fd: 12, BlockIndex: 2, Size: 2, FileOffset: 8, DataOffset: 0, DataLen: 2},
			},
		},
		{
			name: "full file with dropped block",
			resp: &pb.GetFileChunksResponse{
				FileSize:  12,
				BlockSize: blockSize,
				Chunks: []*pb.FileChunk{
					{BlockIndex: 0, Size: 4},
					{BlockIndex: 2, Size: 4},
				},
			},
			fds:     []int{10, 11},
			wantErr: "non-contiguous chunks",
		},
		{
			name:    "fd count mismatch",
			resp:    fullResponse(),
			fds:     []int{10, 11},
			wantErr: "expected 3 FDs, received 2",
		},
		{
			name: "no chunks",
			resp: &pb.GetFileChunksResponse{
				FileSize:  4,
				BlockSize: blockSize,
			},
			wantErr: "server returned no chunks",
		},
		{
			name: "zero block size",
			resp: &pb.GetFileChunksResponse{
				FileSize: 4,
				Chunks:   []*pb.FileChunk{{BlockIndex: 0, Size: 4}},
			},
			fds:     []int{10},
			wantErr: "zero block size",
		},
		{
			name:      "overflowing range",
			resp:      fullResponse(),
			fds:       []int{10, 11, 12},
			fileRange: &FileRange{Start: ^uint64(0), Length: 1},
			wantErr:   "file range is outside the server response",
		},
		{
			name: "nil chunk",
			resp: &pb.GetFileChunksResponse{
				FileSize:  4,
				BlockSize: blockSize,
				Chunks:    []*pb.FileChunk{nil},
			},
			fds:     []int{10},
			wantErr: "server returned an invalid chunk 0",
		},
		{
			name: "zero-size chunk",
			resp: &pb.GetFileChunksResponse{
				FileSize:  4,
				BlockSize: blockSize,
				Chunks:    []*pb.FileChunk{{BlockIndex: 0}},
			},
			fds:     []int{10},
			wantErr: "server returned an invalid chunk 0",
		},
		{
			name:      "response misses range",
			resp:      &pb.GetFileChunksResponse{FileSize: 8, BlockSize: blockSize, Chunks: []*pb.FileChunk{{BlockIndex: 0, Size: 4}}},
			fds:       []int{10},
			fileRange: &FileRange{Start: 4, Length: 4},
			wantErr:   "server response does not contain the requested range",
		},
		{
			name:      "response misses range head",
			resp:      &pb.GetFileChunksResponse{FileSize: 8, BlockSize: blockSize, Chunks: []*pb.FileChunk{{BlockIndex: 1, Size: 4}}},
			fds:       []int{10},
			fileRange: &FileRange{Start: 0, Length: 8},
			wantErr:   "non-contiguous chunks",
		},
		{
			name: "response misses middle block",
			resp: &pb.GetFileChunksResponse{FileSize: 12, BlockSize: blockSize, Chunks: []*pb.FileChunk{
				{BlockIndex: 0, Size: 4},
				{BlockIndex: 2, Size: 4},
			}},
			fds:       []int{10, 11},
			fileRange: &FileRange{Start: 0, Length: 12},
			wantErr:   "non-contiguous chunks",
		},
		{
			name: "response misses range tail",
			resp: &pb.GetFileChunksResponse{FileSize: 12, BlockSize: blockSize, Chunks: []*pb.FileChunk{
				{BlockIndex: 0, Size: 4},
				{BlockIndex: 1, Size: 4},
			}},
			fds:       []int{10, 11},
			fileRange: &FileRange{Start: 0, Length: 12},
			wantErr:   "do not cover the requested range",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := chunkFdViews(tt.resp, tt.fds, tt.fileRange)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHandleGetFileError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "share admission timeout",
			err:  status.Error(codes.DeadlineExceeded, "quota wait timed out"),
			want: "GetFileChunks: share admission wait timed out (retryable): rpc error: code = DeadlineExceeded desc = quota wait timed out",
		},
		{
			name: "share quota too small",
			err:  status.Error(codes.FailedPrecondition, "file exceeds share cap"),
			want: "GetFileChunks: rpc error: code = FailedPrecondition desc = file exceeds share cap",
		},
		{
			name: "other error",
			err:  status.Error(codes.Unknown, "server side failure"),
			want: "GetFileChunks failed: rpc error: code = Unknown desc = server side failure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := handleGetFileError(tt.err)
			assert.ErrorIs(t, got, tt.err)
			assert.Equal(t, tt.want, got.Error())
		})
	}
}

func TestGetFileChunkFdsByInodeFiltersRangeExternalBlocks(t *testing.T) {
	const blockSize = 4096
	requestID := uint32(123)
	// The server answers with complete blocks, which reach past the requested range.
	socket, transferDone := startChunkFdTransferServer(t, []int{
		mmapTestFD(t, strings.Repeat("a", blockSize)),
		mmapTestFD(t, strings.Repeat("b", blockSize)),
	})

	svc := &chunkFdsSvc{getResp: &pb.GetFileChunksResponse{
		FileSize:  2 * blockSize,
		BlockSize: blockSize,
		Chunks: []*pb.FileChunk{
			{BlockIndex: 0, Size: blockSize},
			{BlockIndex: 1, Size: blockSize},
		},
		RequestId:  requestID,
		FdRespSize: fdResponseSize(t, requestID),
	}}
	client := newLimitedClient(NewCacheLimiter(2 * blockSize))
	client.TtrpcSvc = svc
	client.FdSockPath = socket

	batch, err := client.GetFileChunkFdsByInode(7, 2*blockSize,
		FileChunkFdWithRange(blockSize+1, 2),
		FileChunkFdWithWaitTimeout(1500*time.Millisecond))
	require.NoError(t, err)

	req := svc.getRequest()
	require.NotNil(t, req)
	require.NotNil(t, req.GetRange())
	assert.Equal(t, uint32(1500), req.GetWaitTimeoutMs())
	assert.Equal(t, uint64(blockSize+1), req.GetRange().GetStart())
	assert.Equal(t, uint64(2), req.GetRange().GetLength())

	chunks := batch.Chunks()
	require.Len(t, chunks, 1)
	assert.Equal(t, uint32(1), chunks[0].BlockIndex)
	assert.Equal(t, uint32(blockSize), chunks[0].Size)
	assert.Equal(t, uint64(blockSize), chunks[0].FileOffset)
	assert.Equal(t, uint32(1), chunks[0].DataOffset)
	assert.Equal(t, uint32(2), chunks[0].DataLen)
	data := make([]byte, chunks[0].DataLen)
	n, err := unix.Pread(chunks[0].FD(), data, int64(chunks[0].DataOffset))
	require.NoError(t, err)
	assert.Equal(t, "bb", string(data[:n]))

	// Both FDs must remain owned by the batch even though only the second is
	// exposed by the range view.
	allFDs := append([]int(nil), batch.fds...)
	require.Len(t, allFDs, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.Error(t, client.acquireCache(ctx, 1))

	require.NoError(t, batch.Close())
	require.Equal(t, requestID, svc.releaseRequest().GetRequestId())
	require.Equal(t, 1, svc.releaseCallCount())
	for _, fd := range allFDs {
		_, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		assert.ErrorIs(t, err, unix.EBADF)
	}
	require.NoError(t, <-transferDone)
	require.NoError(t, client.acquireCache(context.Background(), 2*blockSize))
}

func TestGetFileChunkFdsEmptyResponseSkipsFDTransferAndRelease(t *testing.T) {
	limiter := NewCacheLimiter(100)
	svc := &chunkFdsSvc{getResp: &pb.GetFileChunksResponse{}}
	client := newLimitedClient(limiter)
	client.TtrpcSvc = svc
	client.FdSockPath = "/nonexistent/fd-transfer.sock"

	batch, err := client.GetFileChunkFdsByInode(1, 100, FileChunkFdWithRange(0, 1))
	require.NoError(t, err)
	assert.Empty(t, batch.Chunks())
	require.NoError(t, batch.Close())
	assert.Equal(t, 0, svc.releaseCallCount())
	require.NoError(t, client.acquireCache(context.Background(), 100))
}

func TestGetFileChunkFdsSetupFailureUsesEOFFallback(t *testing.T) {
	const blockSize = 4096
	requestID := uint32(456)
	socket, transferDone := startChunkFdTransferServer(t, []int{mmapTestFD(t, string(make([]byte, blockSize)))})
	svc := &chunkFdsSvc{getResp: &pb.GetFileChunksResponse{
		FileSize:   blockSize,
		Chunks:     []*pb.FileChunk{{BlockIndex: 0, Size: blockSize}},
		RequestId:  requestID,
		FdRespSize: fdResponseSize(t, requestID),
	}}
	client := newLimitedClient(NewCacheLimiter(blockSize))
	client.TtrpcSvc = svc
	client.FdSockPath = socket

	_, err := client.GetFileChunkFdsByInode(1, blockSize)
	require.ErrorContains(t, err, "zero block size")
	require.Equal(t, 0, svc.releaseCallCount())
	require.NoError(t, <-transferDone)
	require.NoError(t, client.acquireCache(context.Background(), blockSize))
}

func TestChunkFdsClose(t *testing.T) {
	t.Run("does not block Chunks behind a slow release", func(t *testing.T) {
		started := make(chan struct{})
		unblock := make(chan struct{})
		svc := &chunkFdsSvc{releaseHook: func() {
			close(started)
			<-unblock
		}}
		batch := &ChunkFds{client: &ApiClient{TtrpcSvc: svc}, requestID: 1}

		closeDone := make(chan error, 1)
		go func() { closeDone <- batch.Close() }()
		<-started

		chunksDone := make(chan struct{})
		go func() {
			batch.Chunks()
			close(chunksDone)
		}()
		select {
		case <-chunksDone:
		case <-time.After(time.Second):
			t.Fatal("Chunks blocked behind a slow Close")
		}

		close(unblock)
		require.NoError(t, <-closeDone)
	})

	t.Run("concurrent Close releases exactly once and closes FDs", func(t *testing.T) {
		svc := &chunkFdsSvc{releaseErr: status.Error(codes.Internal, "release failed")}
		fd := mmapTestFD(t, "fd")
		batch := &ChunkFds{client: &ApiClient{TtrpcSvc: svc}, fds: []int{fd}, requestID: 1}

		var wg sync.WaitGroup
		errs := make(chan error, 2)
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs <- batch.Close()
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			assert.NoError(t, err)
		}
		assert.Equal(t, 1, svc.releaseCallCount())
		_, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		assert.ErrorIs(t, err, unix.EBADF)
	})

	t.Run("release errors are logged and swallowed", func(t *testing.T) {
		svc := &chunkFdsSvc{releaseErr: status.Error(codes.Unavailable, "daemon down")}
		batch := &ChunkFds{client: &ApiClient{TtrpcSvc: svc}, requestID: 2}

		require.NoError(t, batch.Close())
		require.NoError(t, batch.Close())
		assert.Equal(t, 1, svc.releaseCallCount())
	})
}
