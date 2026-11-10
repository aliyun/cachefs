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
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/aliyun/cachefs/pkg/chunk"
	pb "github.com/aliyun/cachefs/pkg/ttrpc/cachefs"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/aliyun/cachefs/pkg/vfs/apiclient"
	"golang.org/x/sys/unix"
)

// Protocol types and constants are defined in pkg/vfs/apiclient/fd_transfer.go
// and re-aliased here for use within the vfs server implementation.
const (
	FdTransferRequest       = apiclient.FdTransferRequest
	FdTransferResponse      = apiclient.FdTransferResponse
	FdTransferError         = apiclient.FdTransferError
	FdTransferMsgHeaderSize = apiclient.FdTransferMsgHeaderSize
	MaxFDCount              = apiclient.MaxFDCount
)

type (
	FdTransferRequestMsg  = apiclient.FdTransferRequestMsg
	FdTransferResponseMsg = apiclient.FdTransferResponseMsg
	FdTransferMsgHeader   = apiclient.FdTransferMsgHeader
)

// FdChunkInfo holds a chunk's block index and its associated file descriptor
// or chunk reader. It is used exclusively by the fd-transfer server.
type FdChunkInfo struct {
	BlockIndex uint32            `json:"block_index"`
	fd         int               `json:"-"`
	reader     chunk.ChunkReader `json:"-"`
	// memfd is the block whose fd was exported into this chunk, nil for a
	// file-backed chunk. It is carried so an authoritative release can give the
	// export count back; Close deliberately does not, because closing our own copy
	// of the fd says nothing about the copy already sent to the client.
	memfd *utils.Memfd `json:"-"`
}

func (fi *FdChunkInfo) Close() {
	if fi.fd >= 0 {
		unix.Close(fi.fd)
		fi.fd = -1
	}
	if fi.reader != nil {
		fi.reader.Close()
		fi.reader = nil
	}
}

// Request data for fd transfer. The entry carries the unlock obligation of the
// share lock: it is NOT deleted at fd pickup, but lives until the client's
// ReleaseFileChunks RPC, the fd-transfer connection EOF (client crash), or the
// pre-pickup expiry reaper. Whoever atomically deletes the entry from s.fds
// under s.Mutex owns the unlock (exactly-once), and performs it after
// releasing s.Mutex.
type fdTransferInfo struct {
	requestId  uint32
	response   []byte
	chunks     []*FdChunkInfo
	expireAt   time.Time
	target     string        // vfs-layer target whose share lock this entry carries
	allBlocks  bool          // true locks the whole target; false locks selected lease blocks
	shareLease *shareLease   // quota reservation for this request
	fileRange  *pb.FileRange // requested range, nil means whole file request
	claimed    bool          // fds picked up; shields the entry from the expiry reaper
	claimedAt  time.Time     // when the fds were picked up; zero means never claimed
	shareStart time.Time     // when share admission succeeded, for hold-time metrics

	// exported holds the memfd blocks whose fds this request exported to the
	// client. It is kept on the entry, not on the chunks, because the chunks are
	// handed off and closed at fd pickup while the entry lives on until release -
	// and release is the only place that knows whether the exported copies are
	// provably gone.
	exported []*utils.Memfd
}

// drain sources, passed to onRelease by the entry deleter
const (
	shareReleaseRpc     = "rpc"
	shareReleaseEof     = "eof"
	shareReleaseExpired = "expired"
	shareReleaseStop    = "stop"
)

// FD transfer server
type fdTransferServer struct {
	sync.Mutex
	listener net.Listener
	fds      map[uint32]*fdTransferInfo
	stopCh   chan struct{}

	// stopped is set under Mutex before stop() drains fds, so an entry is either registered
	// in time to be drained or rejected outright. It is a plain bool, not an atomic, exactly
	// because that ordering must be atomic with the map: a stopCh-only check could insert into
	// an already-drained map and leak the exported fds and the share charge.
	stopped bool

	// onRelease drops the entry's share lock and quota lease. allBlocks selects a whole-target
	// unlock for a request without a range; otherwise release derives selected blocks from its lease.
	// Called by the drain site that won the entry delete, OUTSIDE s.Mutex.
	onRelease func(fdInfo *fdTransferInfo, source string)
}

func newFdTransferServer(socketPath string) (*fdTransferServer, error) {
	// Remove existing socket file
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to remove existing socket: %v", err)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on Unix socket %s: %v", socketPath, err)
	}

	server := &fdTransferServer{
		listener: listener,
		fds:      make(map[uint32]*fdTransferInfo),
		stopCh:   make(chan struct{}),
	}

	go server.serve()
	go server.cleanupExpiredSessions()

	return server, nil
}

func (s *fdTransferServer) serve() {
	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		s.Lock()
		listener := s.listener
		s.Unlock()
		if listener == nil {
			return
		}
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return
			default:
				logger.Warnf("FD transfer server accept error: %v", err)
				continue
			}
		}

		go s.handleConnection(conn)
	}
}

func (s *fdTransferServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Read request
	req, err := s.readRequest(conn)
	if err != nil {
		if err != io.EOF {
			logger.Warnf("FD transfer: read request error: %s", err)
		} else {
			logger.Info("FD transfer: read EOF, disconnect")
		}
		return
	}

	// Find request and claim it: the entry is NOT deleted at pickup — it
	// carries the share lock for the whole client mapping. claimed=true
	// shields it from the expiry reaper; its lifetime is now governed by
	// ReleaseFileChunks or this connection's EOF. Chunk ownership moves to
	// this goroutine so no drain site can close fds still being sent.
	reqid := req.RequestId
	var chunks []*FdChunkInfo
	s.Lock()
	fdInfo, exists := s.fds[reqid]
	if exists {
		fdInfo.claimed = true
		fdInfo.claimedAt = time.Now()
		chunks = fdInfo.chunks
		fdInfo.chunks = nil
	}
	s.Unlock()

	if !exists {
		logger.Warnf("FD transfer: request not found: %d", reqid)
		s.sendError(conn, fmt.Sprintf("request %d not found or expired", reqid))
		return
	}

	// From here on, this goroutine must drain the entry when the connection
	// ends, unless ReleaseFileChunks already deleted it (then takeRequest
	// no-ops).
	defer func() {
		if info := s.takeRequest(reqid); info != nil {
			logger.Debugf("FD transfer: connection closed for request %d, release target", reqid)
			s.release(info, shareReleaseEof)
		}
	}()

	// Send response with file descriptors
	err = s.sendResponse(conn, fdInfo.response, chunks)

	// fd and chunk readers are not needed after the transfer: the share lock,
	// not the reader, protects the blocks from here on.
	for _, chunk := range chunks {
		chunk.Close()
	}

	if err != nil {
		logger.Warnf("FD transfer: send response error for request %d: %s", reqid, err)
		return
	}
	logger.Debugf("FD transfer: sent %d file descriptors for request %d", len(chunks), reqid)

	// Keep the connection open for the whole mapping lifetime, purely to
	// detect EOF/error, which means the client crashed (or an old SDK closed
	// right after pickup) or forgot to Release.
	buf := make([]byte, 1)
	for {
		if _, err := conn.Read(buf); err != nil {
			return
		}
	}
}

// takeRequest atomically removes the entry from s.fds and returns it, or nil
// if another drain site already owns it. The caller that receives a non-nil
// entry owns the unlock and must call release() outside s.Mutex.
func (s *fdTransferServer) takeRequest(requestId uint32) *fdTransferInfo {
	s.Lock()
	defer s.Unlock()
	fdInfo, ok := s.fds[requestId]
	if !ok {
		return nil
	}
	delete(s.fds, requestId)
	return fdInfo
}

// release closes any remaining chunks and drops the share lock/quota charge.
// Must be called outside s.Mutex, exactly once per entry (by its deleter).
//
// It also gives back the export count of the memfd blocks this request exported,
// but only for the sources that prove the client's copies are gone: the Release
// RPC (the SDK munmaps before sending it) and the pre-pickup reaper (the fds
// never left this process). An EOF or a server stop proves nothing - an old SDK
// that closes right after pickup, a crashed client, or a client outliving us all
// look the same from here - so those sources leave the count where it is, and the
// blocks are never pooled again for this process's lifetime. That is the whole
// point: an unprovable export must cost reuse, not correctness.
func (s *fdTransferServer) release(fdInfo *fdTransferInfo, source string) {
	for _, chunk := range fdInfo.chunks {
		chunk.Close()
	}
	fdInfo.chunks = nil
	if source == shareReleaseRpc || source == shareReleaseExpired {
		for _, memfd := range fdInfo.exported {
			memfd.UnexportFd()
		}
	}
	fdInfo.exported = nil
	if s.onRelease != nil && fdInfo.target != "" {
		s.onRelease(fdInfo, source)
	}
}

func (s *fdTransferServer) readRequest(conn net.Conn) (*FdTransferRequestMsg, error) {
	// Read header
	headerBuf := utils.Alloc(FdTransferMsgHeaderSize)
	defer utils.Free(headerBuf)
	n, err := conn.Read(headerBuf)
	if err != nil {
		return nil, err
	}
	if n != FdTransferMsgHeaderSize {
		return nil, fmt.Errorf("expected %d bytes header, got %d", FdTransferMsgHeaderSize, n)
	}

	// Parse header
	header := &FdTransferMsgHeader{
		Type:   headerBuf[0],
		Length: uint16(headerBuf[1]) | (uint16(headerBuf[2]) << 8),
	}
	if header.Type != FdTransferRequest {
		return nil, fmt.Errorf("unexpected message type: %d", header.Type)
	}

	// Read body
	bodyBuf := utils.Alloc(int(header.Length))
	defer utils.Free(bodyBuf)
	n, err = conn.Read(bodyBuf)
	if err != nil {
		return nil, err
	}
	if n != int(header.Length) {
		return nil, fmt.Errorf("expected %d bytes body, got %d", header.Length, n)
	}

	// Parse request
	var req FdTransferRequestMsg
	err = json.Unmarshal(bodyBuf, &req)
	if err != nil {
		return nil, fmt.Errorf("failed to parse request: %v", err)
	}

	return &req, nil
}

func (s *fdTransferServer) sendResponse(conn net.Conn, response []byte, chunks []*FdChunkInfo) error {
	// Prepare header
	respSize := len(response)
	header := &FdTransferMsgHeader{
		Type:   FdTransferResponse,
		Length: uint16(respSize),
	}

	headerBuf := make([]byte, FdTransferMsgHeaderSize)
	headerBuf[0] = header.Type
	headerBuf[1] = byte(header.Length & 0xff)
	headerBuf[2] = byte((header.Length >> 8) & 0xff)

	// Prepare message
	msgBuf := make([]byte, FdTransferMsgHeaderSize+respSize)
	copy(msgBuf[:FdTransferMsgHeaderSize], headerBuf)
	copy(msgBuf[FdTransferMsgHeaderSize:], response)

	// Prepare out-of-band data for file descriptors
	fdCount := len(chunks)
	left := fdCount
	fds := make([]int, fdCount)
	for i := 0; i < fdCount; i++ {
		fds[i] = chunks[i].fd
	}
	unixConn := conn.(*net.UnixConn)
	for left > 0 {
		batch := left
		if batch > MaxFDCount {
			batch = MaxFDCount
		}
		currentFds := fds[fdCount-left : fdCount-left+batch]
		left -= batch
		if len(currentFds) == 0 {
			return errors.New("no fd to send")
		}
		oob := unix.UnixRights(currentFds...)

		// Send message with file descriptors
		n, oobn, err := unixConn.WriteMsgUnix(msgBuf, oob, nil)
		if err != nil {
			return fmt.Errorf("failed to send message: %s", err)
		}
		if n != len(msgBuf) {
			return fmt.Errorf("partial write: expect %d, wrote %d", len(msgBuf), n)
		}
		if len(oob) > 0 && oobn != len(oob) {
			return fmt.Errorf("partial oob write: expect %d, wrote %d", len(oob), oobn)
		}
	}
	return nil
}

func (s *fdTransferServer) sendError(conn net.Conn, errorMsg string) {
	header := &FdTransferMsgHeader{
		Type:   FdTransferError,
		Length: uint16(len(errorMsg)),
	}

	msgBuf := utils.Alloc(FdTransferMsgHeaderSize + len(errorMsg))
	defer utils.Free(msgBuf)
	headerBuf := msgBuf[:FdTransferMsgHeaderSize]
	headerBuf[0] = header.Type
	headerBuf[1] = byte(header.Length & 0xff)
	headerBuf[2] = byte((header.Length >> 8) & 0xff)
	copy(msgBuf[FdTransferMsgHeaderSize:], []byte(errorMsg))

	conn.Write(msgBuf)
}

func (s *fdTransferServer) addRequest(requestId uint32, target string, allBlocks bool,
	lease *shareLease, fileRange *pb.FileRange, shareStart time.Time,
	chunks []*FdChunkInfo) (int, error) {
	resp := &FdTransferResponseMsg{
		RequestId: requestId,
	}
	respBody, err := json.Marshal(resp)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal response: %v", err)
	}
	var exported []*utils.Memfd
	for _, c := range chunks {
		if c.memfd != nil {
			exported = append(exported, c.memfd)
		}
	}
	fdInfo := &fdTransferInfo{
		requestId:  requestId,
		response:   respBody,
		chunks:     chunks,
		expireAt:   time.Now().Add(time.Minute * 5),
		target:     target,
		allBlocks:  allBlocks,
		shareLease: lease,
		fileRange:  fileRange,
		shareStart: shareStart,
		exported:   exported,
	}

	s.Lock()
	if s.stopped {
		s.Unlock()
		// The server is gone: nobody would ever pick this entry up or drain it. Reject so
		// the caller rolls back the chunks it loaded.
		return 0, fmt.Errorf("fd transfer server stopped")
	}
	s.fds[requestId] = fdInfo
	s.Unlock()
	return FdTransferMsgHeaderSize + len(respBody), nil
}

func (s *fdTransferServer) cleanupExpiredSessions() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.cleanupExpired()
		}
	}
}

func (s *fdTransferServer) cleanupExpired() {
	// Collect victims under s.Mutex, then drain outside it (lock ordering:
	// UnlockTarget/quota.release are never called under s.Mutex).
	var reaped []*fdTransferInfo
	s.Lock()
	now := time.Now()
	for requestId, fdInfo := range s.fds {
		// A claimed entry lives for the whole mapping, governed by
		// Release/EOF, not the timer. The reaper only handles the pre-pickup
		// crash: the client got a requestId but never connected.
		if !fdInfo.claimed && now.After(fdInfo.expireAt) {
			delete(s.fds, requestId)
			reaped = append(reaped, fdInfo)
			logger.Infof("FD transfer: expired request %d", requestId)
		}
	}
	s.Unlock()
	for _, fdInfo := range reaped {
		s.release(fdInfo, shareReleaseExpired)
	}
}

func (s *fdTransferServer) stop() {
	// Marking stopped before the drain below closes the door on late addRequest calls from
	// handlers the ttrpc server did not wait for, and makes stop() idempotent so stopCh is
	// never closed twice.
	s.Lock()
	if s.stopped {
		s.Unlock()
		return
	}
	s.stopped = true
	listener := s.listener
	s.listener = nil

	// Drain all remaining entries (snapshot under s.Mutex, release outside).
	var remaining []*fdTransferInfo
	for requestId, fdInfo := range s.fds {
		delete(s.fds, requestId)
		remaining = append(remaining, fdInfo)
	}
	s.Unlock()

	close(s.stopCh)
	if listener != nil {
		listener.Close()
	}
	for _, fdInfo := range remaining {
		s.release(fdInfo, shareReleaseStop)
	}
}
