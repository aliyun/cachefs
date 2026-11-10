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
	"fmt"
	"net"
	"sync"
	"time"

	pb "github.com/aliyun/cachefs/pkg/ttrpc/cachefs"
)

// FileChunkFdOptions holds options for all GetFileChunks consumers, including
// MmapFile/MmapFileByInode and GetFileChunkFds/GetFileChunkFdsByInode.
type FileChunkFdOptions struct {
	// WaitTimeout bounds the time the daemon may keep the request queued for
	// shared-bytes quota admission. 0 (the default) means wait forever.
	WaitTimeout time.Duration

	// Range selects a file byte interval. Nil (the default) returns the whole
	// file. GetFileChunks consumers receive complete cache blocks intersecting it.
	Range *FileRange
}

// FileChunkFdOption configures FileChunkFdOptions for any GetFileChunks consumer.
type FileChunkFdOption func(*FileChunkFdOptions)

// FileChunkFdWithWaitTimeout sets the daemon-side share admission wait timeout
// for any GetFileChunks consumer.
func FileChunkFdWithWaitTimeout(d time.Duration) FileChunkFdOption {
	return func(o *FileChunkFdOptions) {
		o.WaitTimeout = d
	}
}

// FileChunkFdWithRange selects the half-open byte interval [start, start+length)
// for any GetFileChunks consumer. CacheFS transfers complete cache blocks
// intersecting the range.
func FileChunkFdWithRange(start, length uint64) FileChunkFdOption {
	return func(o *FileChunkFdOptions) {
		o.Range = &FileRange{Start: start, Length: length}
	}
}

// ChunkFd is a borrowed view of a cache block FD returned by ChunkFds.Chunks.
// The FD remains owned by its parent ChunkFds and is valid only until the parent
// is closed. Callers must not close, transfer, or use the FD after Close returns.
type ChunkFd struct {
	fd int

	BlockIndex uint32
	Size       uint32
	FileOffset uint64
	DataOffset uint32
	DataLen    uint32
}

// FD returns the borrowed cache block file descriptor. The descriptor is owned
// by the parent ChunkFds and must not be closed by the caller.
func (c ChunkFd) FD() int {
	return c.fd
}

// ChunkFds owns one fully received cache-block FD batch. It must be explicitly
// closed after all callers have stopped using FDs returned by Chunks. ApiClient
// does not track this handle, so callers must close it before closing the client.
type ChunkFds struct {
	client    *ApiClient
	fds       []int
	chunks    []ChunkFd
	conn      net.Conn
	requestID uint32
	reserved  int64

	mu     sync.Mutex
	closed bool
}

// Chunks returns borrowed views for the complete blocks that intersect the
// requested range. The batch keeps ownership of every received FD internally,
// including any that fall outside that range.
func (c *ChunkFds) Chunks() []ChunkFd {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]ChunkFd(nil), c.chunks...)
}

// Close closes all received FDs, releases the daemon request, closes the
// fd-transfer socket, and returns the local cache reservation. It is safe to
// call concurrently and only its first invocation performs cleanup. A release
// failure (e.g. a daemon too busy to answer in time) is logged and swallowed:
// closing the fd-transfer connection supplies the EOF cleanup fallback, so
// Close always returns nil.
func (c *ChunkFds) Close() error {
	// The mutex only covers the state transition; the release RPC runs outside
	// it so a slow daemon cannot block concurrent Chunks callers.
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	fds, conn := c.fds, c.conn
	requestID, client, reserved := c.requestID, c.client, c.reserved
	c.fds, c.conn, c.requestID, c.reserved = nil, nil, 0, 0
	c.mu.Unlock()

	closeFDs(fds)

	if requestID != 0 && client != nil {
		req := &pb.ReleaseFileChunksRequest{RequestId: requestID}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := client.TtrpcSvc.ReleaseFileChunks(ctx, req)
		cancel()
		if err != nil {
			logger.Infof("ApiClient: ReleaseFileChunks request %d: %s (ignored)", requestID, err)
		}
	}

	if conn != nil {
		_ = conn.Close()
	}
	if client != nil {
		client.releaseCache(reserved)
	}
	return nil
}

// GetFileChunkFds returns complete cache block FDs for a CacheFS file path.
// The path must reside in a CacheFS mount point. The returned batch owns every
// FD until Close, including any that fall outside the requested range.
func (c *ApiClient) GetFileChunkFds(filePath string, opts ...FileChunkFdOption) (*ChunkFds, error) {
	inode, size, err := getInodeAndSize(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get inode from path %s: %s", filePath, err)
	}
	return c.GetFileChunkFdsByInode(inode, size, opts...)
}

// GetFileChunkFdsByInode returns complete cache block FDs for inode. A positive
// sizeHint is reserved against the local CacheLimiter for the batch lifetime: a
// range reserves its complete selected-block span when cached server block
// metadata is available, otherwise the full sizeHint. A non-positive hint skips
// local limiting and lets the daemon validate any requested range.
func (c *ApiClient) GetFileChunkFdsByInode(inode uint64, sizeHint int64, opts ...FileChunkFdOption) (*ChunkFds, error) {
	options := &FileChunkFdOptions{}
	for _, opt := range opts {
		opt(options)
	}
	if err := validateFileRange(sizeHint, options.Range); err != nil {
		return nil, err
	}

	// Reserve before the daemon materializes the request, with the same policy
	// as mmap: selected-block span for a range when block metadata is cached,
	// full sizeHint otherwise.
	reservation, err := c.shareReservation(sizeHint, options.Range)
	if err != nil {
		return nil, err
	}
	var reserved int64
	if err := c.acquireCache(context.Background(), reservation); err != nil {
		return nil, err
	}
	if c.limiter != nil && reservation > 0 {
		reserved = reservation
	}
	defer func() { c.releaseCache(reserved) }()

	req := &pb.GetFileChunksRequest{
		Inode:         inode,
		WaitTimeoutMs: uint32(options.WaitTimeout.Milliseconds()),
	}
	if options.Range != nil {
		req.Range = &pb.FileRange{Start: options.Range.Start, Length: options.Range.Length}
	}
	resp, err := c.TtrpcSvc.GetFileChunks(context.Background(), req)
	if err != nil {
		return nil, handleGetFileError(err)
	}
	if resp.FileSize == 0 {
		return &ChunkFds{client: c}, nil
	}

	conn, fds, err := c.recvFileChunkFDs(resp)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			closeFDs(fds)
			_ = conn.Close()
		}
	}()

	chunks, err := chunkFdViews(resp, fds, options.Range)
	if err != nil {
		return nil, err
	}
	batch := &ChunkFds{
		client:    c,
		fds:       fds,
		chunks:    chunks,
		conn:      conn,
		requestID: resp.RequestId,
		reserved:  reserved,
	}
	reserved = 0
	success = true
	return batch, nil
}

// fileChunkSpan is one validated chunk of a GetFileChunks response: its
// position in the response (and in the received FDs), the chunk's file offset,
// and the byte span [dataOffset, dataOffset+dataLen) the requested range
// covers within it.
type fileChunkSpan struct {
	index      int
	blockIndex uint32
	size       uint32
	fileOffset uint64
	dataOffset uint32
	dataLen    uint32
}

// selectFileChunks validates a GetFileChunks response against the FDs received
// with it and returns the chunks intersecting the requested range (the whole
// file when fileRange is nil) as clamped data spans. The spans are guaranteed
// to tile the requested interval exactly at block granularity: a daemon that
// drops or reorders a selected block fails validation instead of surfacing as
// a silently short copy.
func selectFileChunks(resp *pb.GetFileChunksResponse, fds []int, fileRange *FileRange) ([]fileChunkSpan, error) {
	if len(fds) != len(resp.Chunks) {
		return nil, fmt.Errorf("expected %d FDs, received %d", len(resp.Chunks), len(fds))
	}
	if len(resp.Chunks) == 0 {
		return nil, fmt.Errorf("server returned no chunks for a nonempty file")
	}
	if resp.BlockSize == 0 {
		return nil, fmt.Errorf("server returned a zero block size")
	}

	start, end := uint64(0), resp.FileSize
	if fileRange != nil {
		start = fileRange.Start
		end = start + fileRange.Length
		if end < start || start >= resp.FileSize || end > resp.FileSize {
			return nil, fmt.Errorf("file range is outside the server response")
		}
	}

	spans := make([]fileChunkSpan, 0, len(resp.Chunks))
	for i, chunk := range resp.Chunks {
		if chunk == nil || chunk.Size == 0 {
			return nil, fmt.Errorf("server returned an invalid chunk %d", i)
		}
		fileOffset := uint64(chunk.BlockIndex) * uint64(resp.BlockSize)
		chunkEnd := fileOffset + uint64(chunk.Size)
		if chunkEnd < fileOffset {
			return nil, fmt.Errorf("chunk %d overflows file offset", i)
		}
		if fileRange != nil && (chunkEnd <= start || end <= fileOffset) {
			continue
		}

		dataStart, dataEnd := fileOffset, chunkEnd
		if dataStart < start {
			dataStart = start
		}
		if end < dataEnd {
			dataEnd = end
		}
		dataOffset := dataStart - fileOffset
		dataLen := dataEnd - dataStart
		if dataLen == 0 || dataOffset+dataLen > uint64(chunk.Size) {
			return nil, fmt.Errorf("chunk %d does not cover the requested range", i)
		}
		spans = append(spans, fileChunkSpan{
			index:      i,
			blockIndex: chunk.BlockIndex,
			size:       chunk.Size,
			fileOffset: fileOffset,
			dataOffset: uint32(dataOffset),
			dataLen:    uint32(dataLen),
		})
	}
	if len(spans) == 0 {
		return nil, fmt.Errorf("server response does not contain the requested range")
	}

	// The selected spans must tile [start, end) exactly: a daemon that drops a
	// selected block must not surface as a silently short copy.
	next := start
	expectedBlock := spans[0].blockIndex
	for _, span := range spans {
		if span.blockIndex != expectedBlock || span.fileOffset+uint64(span.dataOffset) != next {
			return nil, fmt.Errorf("server returned non-contiguous chunks for the requested range")
		}
		expectedBlock++
		next += uint64(span.dataLen)
	}
	if next != end {
		return nil, fmt.Errorf("server chunks do not cover the requested range")
	}
	return spans, nil
}

// chunkFdViews builds public range views without dropping range-external FDs.
// Keeping the full fds slice lets Close safely terminate an old-daemon full-file
// response after exposing only the requested blocks.
func chunkFdViews(resp *pb.GetFileChunksResponse, fds []int, fileRange *FileRange) ([]ChunkFd, error) {
	spans, err := selectFileChunks(resp, fds, fileRange)
	if err != nil {
		return nil, err
	}
	views := make([]ChunkFd, 0, len(spans))
	for _, span := range spans {
		views = append(views, ChunkFd{
			fd:         fds[span.index],
			BlockIndex: span.blockIndex,
			Size:       span.size,
			FileOffset: span.fileOffset,
			DataOffset: span.dataOffset,
			DataLen:    span.dataLen,
		})
	}
	return views, nil
}
