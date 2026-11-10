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
	"net"
	"os"
	"time"
	"unsafe"

	pb "github.com/aliyun/cachefs/pkg/ttrpc/cachefs"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/aliyun/cachefs/pkg/vfs/common"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// munmapSlice is a thin wrapper so Close() can call it without importing unix directly.
func munmapSlice(b []byte) error {
	return unix.Munmap(b)
}

// getInodeAndSize resolves a CacheFS file path to its inode and size. The size
// is used as a sizeHint for cache-limit admission control, obtained for free
// from the same stat that validates the file.
func getInodeAndSize(filePath string) (uint64, int64, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return 0, 0, fmt.Errorf("file does not exist: %v", err)
	}
	if !fi.Mode().IsRegular() {
		return 0, 0, fmt.Errorf("path %s is not a regular file", filePath)
	}
	if err := common.ValidateMountPoint(filePath); err != nil {
		return 0, 0, fmt.Errorf("not a CacheFS mount point: %v", err)
	}
	inode, err := utils.GetFileInode(filePath)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get inode: %v", err)
	}
	return inode, fi.Size(), nil
}

// FileRange selects the half-open byte interval [Start, Start+Length) for all
// GetFileChunks consumers. CacheFS still transfers complete cache blocks
// intersecting the range.
type FileRange struct {
	Start  uint64
	Length uint64
}

// MmapOptions is an alias for FileChunkFdOptions.
//
// Deprecated: use FileChunkFdOptions.
type MmapOptions = FileChunkFdOptions

// MmapOption is an alias for FileChunkFdOption.
//
// Deprecated: use FileChunkFdOption.
type MmapOption = FileChunkFdOption

// MmapWithWaitTimeout sets the daemon-side share admission wait timeout.
//
// Deprecated: use FileChunkFdWithWaitTimeout.
func MmapWithWaitTimeout(d time.Duration) MmapOption {
	return FileChunkFdWithWaitTimeout(d)
}

// MmapWithRange selects the half-open byte interval [start, start+length).
//
// Deprecated: use FileChunkFdWithRange.
func MmapWithRange(start, length uint64) MmapOption {
	return FileChunkFdWithRange(start, length)
}

func validateFileRange(sizeHint int64, fileRange *FileRange) error {
	if fileRange == nil {
		return nil
	}
	if fileRange.Length == 0 {
		return fmt.Errorf("file range length must be positive")
	}
	end := fileRange.Start + fileRange.Length
	if end < fileRange.Start {
		return fmt.Errorf("file range overflows the file")
	}
	if sizeHint <= 0 {
		return nil
	}
	fileSize := uint64(sizeHint)
	if fileRange.Start >= fileSize || end > fileSize {
		return fmt.Errorf("file range is outside the file")
	}
	return nil
}

// handleGetFileError preserves the status-aware error:
//   - DeadlineExceeded: share admission wait timed out, retryable with backoff
//   - FailedPrecondition: selected blocks larger than the share cap, not retryable until the daemon
//     configuration changes.
//
// Anything else is surfaced as is, without retry; never string-match the message.
func handleGetFileError(err error) error {
	switch status.Code(err) {
	case codes.DeadlineExceeded:
		return fmt.Errorf("GetFileChunks: share admission wait timed out (retryable): %w", err)
	case codes.FailedPrecondition:
		return fmt.Errorf("GetFileChunks: %w", err)
	default:
		return fmt.Errorf("GetFileChunks failed: %w", err)
	}
}

// shareReservation computes the local limiter reservation for one GetFileChunks
// request: the selected span when cached server metadata can describe it,
// otherwise the whole file.
func (c *ApiClient) shareReservation(sizeHint int64, fileRange *FileRange) (int64, error) {
	if fileRange == nil || sizeHint <= 0 {
		return sizeHint, nil
	}
	info, err := c.cachedServerInfo()
	if err != nil || info.BlockSize == 0 {
		return sizeHint, nil
	}
	return utils.GetRangeBlockBytes(uint64(sizeHint), fileRange.Start, fileRange.Length, info.BlockSize)
}

// MaxAllowedRange returns the requested range, shortened when needed so that GetFileChunks will
// not reject it for exceeding the daemon's share cap.
//
// The daemon charges a range for every complete cache block it touches, so a range whose length
// equals the share cap is still rejected when its block-aligned size exceeds the cap.
// MaxAllowedRange keeps start, clamps the end to fileSize, and, if the touched blocks still exceed
// the share cap, moves the end back to the last block boundary that fits. To read a large range in
// order, call it again with start set to the end of the returned range until the whole range is
// covered; after the first call every start is block-aligned, so each returned range uses the full
// cap rounded down to whole blocks.
//
// Example with block size 128MiB and share cap 13856MiB (108.25 blocks):
//
//	MaxAllowedRange(size, 0, 13856MiB)        -> {Start: 0, Length: 13824MiB}      // blocks 0-107
//	MaxAllowedRange(size, 100MiB, 13856MiB)   -> {Start: 100MiB, Length: 13724MiB} // blocks 0-107
//	MaxAllowedRange(size, 13824MiB, 13856MiB) -> {Start: 13824MiB, Length: 13824MiB}
//
// It needs the block size from GetServerInfo, so it fails against daemons that do not implement
// it. It only bounds a single request; the request may still wait for budget held by others.
func (c *ApiClient) MaxAllowedRange(fileSize int64, start, length uint64) (FileRange, error) {
	if fileSize <= 0 {
		return FileRange{}, fmt.Errorf("file size must be positive, got %d", fileSize)
	}
	if length == 0 {
		return FileRange{}, fmt.Errorf("file range length must be positive")
	}
	size := uint64(fileSize)
	if start >= size {
		return FileRange{}, fmt.Errorf("file range start %d must be less than file size %d", start, size)
	}
	length = min(length, size-start)

	info, err := c.cachedServerInfo()
	if err != nil {
		return FileRange{}, err
	}
	if info.BlockSize == 0 {
		return FileRange{}, fmt.Errorf("cache block size is unknown")
	}
	limit := getLimitBytes(info)
	if limit == 0 {
		logger.Infof("MaxAllowedRange: daemon reports no share cap, falling back to no range limit")
		return FileRange{Start: start, Length: length}, nil
	}

	span, err := utils.GetRangeBlockBytes(size, start, length, info.BlockSize)
	if err != nil {
		return FileRange{}, err
	}
	if span <= limit {
		return FileRange{Start: start, Length: length}, nil
	}

	// The touched blocks exceed the limit, so the range is longer than n whole blocks counted from
	// start's block. End it at the boundary after those n blocks: the range then charges exactly
	// n blocks, and the next call starts block-aligned.
	blockBytes := uint64(info.BlockSize)
	n := uint64(limit) / blockBytes
	if n == 0 {
		return FileRange{}, fmt.Errorf("cache block size %d exceeds cache limit %d", info.BlockSize, limit)
	}
	end := (start/blockBytes + n) * blockBytes
	return FileRange{Start: start, Length: end - start}, nil
}

// MmapFile memory-maps a file by its path for zero-copy access.
// The file must reside in a CacheFS mount point. The returned byte slice
// remains valid until MunmapFile is called.
func (c *ApiClient) MmapFile(filePath string, opts ...FileChunkFdOption) ([]byte, error) {
	inode, size, err := getInodeAndSize(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get inode from path %s: %s", filePath, err)
	}
	return c.MmapFileByInode(inode, size, opts...)
}

// MmapFileByInode memory-maps a file by its inode number for zero-copy access.
//
// When the client was created with a CacheLimiter, a full mapping reserves
// sizeHint (the file size) BEFORE the server materializes it. An explicit range
// reserves its complete selected-block span when cached server block metadata is
// available; otherwise it safely falls back to sizeHint. Pass an accurate size;
// a hint <= 0 skips limiting for this call, and a reservation larger than the
// limiter capacity is rejected. Callers that only have a path should use
// MmapFile, which derives the size via stat.
func (c *ApiClient) MmapFileByInode(inode uint64, sizeHint int64, opts ...FileChunkFdOption) ([]byte, error) {
	options := &FileChunkFdOptions{}
	for _, opt := range opts {
		opt(options)
	}
	if err := validateFileRange(sizeHint, options.Range); err != nil {
		return nil, err
	}

	// Reserve budget before materializing the file on the server. Full mappings
	// use sizeHint; ranges use their selected-block span when cached block metadata
	// is available, otherwise sizeHint keeps them locally gated.
	reservation, err := c.shareReservation(sizeHint, options.Range)
	if err != nil {
		return nil, err
	}
	var reserved int64
	limiterStart := time.Now()
	if err := c.acquireCache(context.Background(), reservation); err != nil {
		return nil, err
	}
	if c.limiter != nil && reservation > 0 {
		reserved = reservation
		logger.Infof("ApiClient: mmap inode %d waited %s for local cache limiter", inode, time.Since(limiterStart))
	}

	// reserved is what we still owe the limiter. Every return below gives it back,
	// including the FileSize == 0 one, which is a success but maps nothing. The
	// handoff to mappedRegion disowns it by zeroing reserved.
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
		return []byte{}, nil
	}

	// mmap cache fds to construct file content; the fd-transfer connection is
	// kept open for the whole mapping lifetime (daemon crash-safety fallback).
	mapped, err := c.mmapFileContent(resp, options.Range)
	if err != nil {
		return nil, fmt.Errorf("failed to mmap file content: %v", err)
	}

	dataPtr := uintptr(unsafe.Pointer(&mapped.data[0]))
	c.mappingMu.Lock()
	if c.closed {
		c.mappingMu.Unlock()
		// Close already swapped the ledger: registering would orphan the fresh
		// mapping. Tear it down instead; the daemon reclaims the share session
		// from the connection's EOF, and reserved stays owned by the defer.
		_ = munmapSlice(mapped.base)
		_ = mapped.conn.Close()
		return nil, fmt.Errorf("failed to mmap inode %d: client is closed", inode)
	}
	c.mappings[dataPtr] = &mappedRegion{data: mapped.data, base: mapped.base,
		reserved: reserved, requestId: resp.RequestId, conn: mapped.conn}
	c.mappingMu.Unlock()
	reserved = 0 // ownership moved to the mapping

	return mapped.data, nil
}

// releaseFileChunks tells the daemon the mapping is gone so it can unlock the
// file's cache blocks, then closes the held-open fd-transfer connection.
// Errors are logged and swallowed: after a daemon restart the lock table is
// gone, and the closed connection's EOF is the cleanup fallback, so munmap
// succeeds regardless.
func (c *ApiClient) releaseFileChunks(r *mappedRegion) {
	if r.requestId != 0 {
		req := &pb.ReleaseFileChunksRequest{RequestId: r.requestId}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if _, err := c.TtrpcSvc.ReleaseFileChunks(ctx, req); err != nil {
			logger.Infof("ApiClient: ReleaseFileChunks request %d: %s (ignored)", r.requestId, err)
		}
		cancel()
	}
	if r.conn != nil {
		r.conn.Close()
		r.conn = nil
	}
}

// MunmapFile unmaps a previously mapped file.
func (c *ApiClient) MunmapFile(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	dataPtr := uintptr(unsafe.Pointer(&data[0]))
	c.mappingMu.Lock()
	r := c.mappings[dataPtr]
	if r != nil {
		delete(c.mappings, dataPtr)
	}
	c.mappingMu.Unlock()
	if r == nil {
		return nil
	}
	if err := unix.Munmap(r.base); err != nil {
		c.mappingMu.Lock()
		if !c.closed {
			c.mappings[dataPtr] = r
			c.mappingMu.Unlock()
			return fmt.Errorf("failed to munmap file: %v", err)
		}
		c.mappingMu.Unlock()
		// Close already drained the ledger; re-inserting would orphan the entry
		// forever. Give the resources their remaining exits instead: the daemon
		// reclaims the share session from the connection's EOF. The mapping
		// itself stays up after the failed munmap — the caller must drop data.
		c.releaseFileChunks(r)
		c.releaseCache(r.reserved)
		return fmt.Errorf("failed to munmap file: %v", err)
	}
	// Tell the daemon after the mapping is gone, then close the fd-transfer
	// connection (its EOF is the fallback), then release the local limiter budget.
	c.releaseFileChunks(r)
	c.releaseCache(r.reserved)
	return nil
}

// mmapContent holds the exposed range view, the complete mmap allocation, and
// the FD-transfer connection held until release.
type mmapContent struct {
	data []byte
	base []byte
	conn net.Conn
}

func closeFDs(fds []int) {
	for _, fd := range fds {
		if fd >= 0 {
			_ = unix.Close(fd)
		}
	}
}

// recvFileChunkFDs dials the fd-transfer socket, claims resp.RequestId, and
// receives every FD in the response. The returned connection must remain open
// until the caller has released the request. On any setup failure it closes the
// connection so the daemon can use EOF as its cleanup fallback.
func (c *ApiClient) recvFileChunkFDs(resp *pb.GetFileChunksResponse) (net.Conn, []int, error) {
	conn, err := net.Dial("unix", c.FdSockPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to FD transfer socket: %s", err)
	}
	chunkFds := make([]int, 0, len(resp.Chunks))
	success := false
	defer func() {
		if !success {
			closeFDs(chunkFds)
			_ = conn.Close()
		}
	}()

	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return nil, nil, fmt.Errorf("FD transfer socket is not a Unix connection")
	}
	req := &FdTransferRequestMsg{RequestId: resp.RequestId}
	reqBody, err := json.Marshal(req)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal request: %v", err)
	}

	header := &FdTransferMsgHeader{
		Type:   FdTransferRequest,
		Length: uint16(len(reqBody)),
	}

	msgBuf := utils.Alloc(FdTransferMsgHeaderSize + len(reqBody))
	defer utils.Free(msgBuf)
	headerBuf := msgBuf[:FdTransferMsgHeaderSize]
	headerBuf[0] = header.Type
	headerBuf[1] = byte(header.Length & 0xff)
	headerBuf[2] = byte((header.Length >> 8) & 0xff)
	copy(msgBuf[FdTransferMsgHeaderSize:], reqBody)

	c.fdMu.Lock()
	defer c.fdMu.Unlock()
	n, err := conn.Write(msgBuf)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to send session request: %v", err)
	}
	if n != len(msgBuf) {
		return nil, nil, fmt.Errorf("wrote %d but expect %d", n, len(msgBuf))
	}

	left := len(resp.Chunks)
	for left > 0 {
		batch := left
		if batch > MaxFDCount {
			batch = MaxFDCount
		}
		left -= batch
		fds, err := c.receiveFDs(unixConn, batch, resp.FdRespSize)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to receive fd: %s", err)
		}
		chunkFds = append(chunkFds, fds...)
	}

	success = true
	return conn, chunkFds, nil
}

// get cache fds and mmap selected fds to virtual address. Every received cache
// FD closes after mmap, while the fd-transfer connection stays open for the
// mapping lifetime: the daemon treats connection EOF as the client-crash unlock
// fallback.
func (c *ApiClient) mmapFileContent(resp *pb.GetFileChunksResponse, fileRange *FileRange) (*mmapContent, error) {
	conn, chunkFds, err := c.recvFileChunkFDs(resp)
	if err != nil {
		return nil, err
	}

	base, data, err := c.mmapChunks(chunkFds, resp, fileRange)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &mmapContent{data: data, base: base, conn: conn}, nil
}

func (c *ApiClient) receiveFDs(conn *net.UnixConn, expectedCount int, bufSize uint32) ([]int, error) {
	var fds []int
	success := false
	defer func() {
		if !success {
			closeFDs(fds)
		}
	}()
	dataBuf := utils.Alloc(int(bufSize))
	defer utils.Free(dataBuf)
	oobBuf := utils.Alloc(unix.CmsgSpace(int(MaxFDCount) * 4))
	defer utils.Free(oobBuf)
	n, oobn, _, _, err := conn.ReadMsgUnix(dataBuf, oobBuf)
	if err != nil {
		return nil, fmt.Errorf("ReadMsgUnix failed: %v", err)
	}

	if n < FdTransferMsgHeaderSize {
		return nil, fmt.Errorf("message too short: expected at least %d bytes, got %d", FdTransferMsgHeaderSize, n)
	}

	header := &FdTransferMsgHeader{
		Type:   dataBuf[0],
		Length: uint16(dataBuf[1]) | (uint16(dataBuf[2]) << 8),
	}
	if header.Type == FdTransferError {
		errorBody := dataBuf[FdTransferMsgHeaderSize : FdTransferMsgHeaderSize+int(header.Length)]
		return nil, fmt.Errorf("server error: %s", string(errorBody))
	} else if header.Type != FdTransferResponse {
		return nil, fmt.Errorf("unexpected message type: %d", header.Type)
	}

	responseBody := dataBuf[FdTransferMsgHeaderSize : FdTransferMsgHeaderSize+int(header.Length)]
	response := FdTransferResponseMsg{}
	if err = json.Unmarshal(responseBody, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	messages, err := unix.ParseSocketControlMessage(oobBuf[:oobn])
	if err != nil {
		return nil, fmt.Errorf("ParseSocketControlMessage failed: %v", err)
	}

	for _, msg := range messages {
		if msg.Header.Level == unix.SOL_SOCKET && msg.Header.Type == unix.SCM_RIGHTS {
			parsedFds, err := unix.ParseUnixRights(&msg)
			if err != nil {
				return nil, fmt.Errorf("ParseUnixRights failed: %v", err)
			}
			fds = append(fds, parsedFds...)
		}
	}
	if len(fds) != expectedCount {
		return nil, fmt.Errorf("expected %d FDs, received %d", expectedCount, len(fds))
	}
	success = true
	return fds, nil
}

func (c *ApiClient) mmapChunks(fds []int, resp *pb.GetFileChunksResponse, fileRange *FileRange) ([]byte, []byte, error) {
	defer closeFDs(fds)

	spans, err := selectFileChunks(resp, fds, fileRange)
	if err != nil {
		return nil, nil, err
	}

	// The mapping holds every selected block in full; the exposed view is the
	// requested range inside it.
	var mapSize uint64
	for _, span := range spans {
		mapSize += uint64(span.size)
	}
	if mapSize > uint64(^uint(0)>>1) {
		return nil, nil, fmt.Errorf("selected block span is too large to mmap")
	}
	fileData, err := unix.Mmap(-1, 0, int(mapSize), unix.PROT_NONE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to allocate virtual address space: %v", err)
	}

	var offset uint64
	for _, span := range spans {
		targetAddr := unsafe.Pointer(uintptr(unsafe.Pointer(&fileData[0])) + uintptr(offset))
		_, err := unix.MmapPtr(fds[span.index], 0, targetAddr, uintptr(span.size), unix.PROT_READ, unix.MAP_SHARED|unix.MAP_FIXED)
		if err != nil {
			_ = unix.Munmap(fileData)
			return nil, nil, fmt.Errorf("failed to mmap chunk %d to target address: %v", span.index, err)
		}
		offset += uint64(span.size)
	}

	// The spans tile the requested range, so the view starts at the first
	// span's data offset and runs for their combined data length.
	viewStart := uint64(spans[0].dataOffset)
	var viewLen uint64
	for _, span := range spans {
		viewLen += uint64(span.dataLen)
	}
	return fileData, fileData[int(viewStart):int(viewStart+viewLen)], nil
}
