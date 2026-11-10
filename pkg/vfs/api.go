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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	chunkcommon "github.com/aliyun/cachefs/pkg/chunk/common"
	"github.com/aliyun/cachefs/pkg/meta"
	pb "github.com/aliyun/cachefs/pkg/ttrpc/cachefs"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/containerd/ttrpc"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const DefAPIServerSock string = "/run/cachefs/cachefs-api.sock"

var logger = utils.GetLogger("cachefs")

type blockRange struct {
	shareBlocks []shareBlock
	bytes       int64
}

type apiServer struct {
	socket         string
	fs             *Cachefs
	ttrpcServer    *ttrpc.Server
	listener       net.Listener
	fdTransfer     *fdTransferServer
	fdSocketPath   string
	requestCounter uint32
	getFileTokens  *utils.Token // global token pool for GetFileChunks

	// mmap API (GetFileChunks) cache budget and its reserved headroom
	quota        *shareQuota
	shareReserve int64 // bytes of burst cache kept for regular read traffic, unavailable to the mmap API

	// api server metrics
	loadCacheCount     prometheus.Counter
	clearCacheCount    prometheus.Counter
	cancelLoadCount    prometheus.Counter
	sourceReadyCount   prometheus.Counter
	metaReadyCount     prometheus.Counter
	dataReadyCount     prometheus.Counter
	getPidCount        prometheus.Counter
	unmountCount       prometheus.Counter
	getFileCount       prometheus.Counter
	releaseFileCount   prometheus.Counter
	getPeersCount      prometheus.Counter
	getServerInfoCount prometheus.Counter
	setCacheSizeCount  prometheus.Counter
	getLoadStatsCount  prometheus.Counter

	// mmap API admission / release metrics
	shareAdmits          prometheus.Counter
	shareAdmitWaited     prometheus.Counter
	shareWaitTimeouts    prometheus.Counter
	shareWaitCanceled    prometheus.Counter
	shareTooLarge        prometheus.Counter
	shareReleaseRpcCount prometheus.Counter
	shareReleaseEofCount prometheus.Counter
	shareReleaseExpCount prometheus.Counter
	shareRollback        prometheus.Counter
	shareWaitHist        prometheus.Histogram
	shareHoldHist        prometheus.Histogram

	loadCacheFlight     int32
	clearCacheFlight    int32
	cancelLoadFlight    int32
	sourceReadyFlight   int32
	metaReadyFlight     int32
	dataReadyFlight     int32
	getPidFlight        int32
	unmountFlight       int32
	getFileFlight       int32
	getPeersFlight      int32
	getServerInfoFlight int32
	setCacheSizeFlight  int32
	getLoadStatsFlight  int32
}

func newApiServer(sock string, fs *Cachefs, reg prometheus.Registerer) (*apiServer, error) {
	server := &apiServer{
		socket:        sock,
		fs:            fs,
		fdSocketPath:  getFdSocketPath(sock),
		getFileTokens: utils.NewToken(32),
	}
	server.InitMetrics(reg)

	// Build the mmap API's cache budget (burst capacity minus the reserved
	// headroom). The reserve-vs-burst sanity check lives in the command-line
	// layer. Conf/cache may be nil in unit tests building a bare Cachefs.
	var burstCap, blockSize int64
	if fs.Conf != nil {
		server.shareReserve = fs.Conf.ShareReserve
		if fs.Conf.Store != nil {
			blockSize = int64(fs.Conf.Store.BlockSize)
		}
	}
	if fs.cache != nil {
		_, burstCap = fs.cache.getCacheSize()
	}
	if blockSize <= 0 {
		// Unit tests build a bare Cachefs without a Store; give the quota a
		// sane block size so it can be constructed. Production always sets one.
		blockSize = 4 << 20
	}
	quota, err := newShareQuota(shareCap(burstCap, server.shareReserve), blockSize)
	if err != nil {
		return nil, fmt.Errorf("init share quota: %w", err)
	}
	server.quota = quota

	if err := os.MkdirAll(filepath.Dir(sock), 0755); err != nil {
		return nil, err
	}
	if err := os.Remove(sock); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to remove api socket: %v", err)
	}
	listener, err := net.Listen("unix", sock)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on Unix socket: %s", err)
	}
	server.listener = listener

	// Create fd transfer server
	fdTransfer, err := newFdTransferServer(server.fdSocketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create fd transfer server: %v", err)
	}
	fdTransfer.onRelease = server.releaseShare
	server.fdTransfer = fdTransfer

	// Create ttrpc server
	server.ttrpcServer, err = ttrpc.NewServer()
	if err != nil {
		return nil, fmt.Errorf("failed to create ttrpc server: %v", err)
	}
	pb.RegisterCachefsApiService(server.ttrpcServer, server)
	return server, nil
}

func (a *apiServer) serve() {
	go func() {
		logger.Infof("Serving vfs API server on %s", a.socket)
		if err := a.ttrpcServer.Serve(context.Background(), a.listener); err != nil {
			// Check if the server was closed normally
			if err == ttrpc.ErrServerClosed || strings.Contains(err.Error(), "server closed") {
				logger.Info("vfs API server closed")
			} else {
				logger.Fatalf("Serve for vfs API: %s", err)
			}
		}
	}()
}

func (a *apiServer) stop() {
	if a.ttrpcServer != nil {
		a.ttrpcServer.Close()
		a.ttrpcServer = nil
	}
	if a.listener != nil {
		a.listener.Close()
		a.listener = nil
	}

	// Don't reset fdTransfer to nil here, ttrpcServer.Close() does not drain in-flight
	// handlers, and a GetFileChunks still loading blocks will reach addRequest after this
	// returns. Clearing the field would make that a nil dereference.
	if a.fdTransfer != nil {
		a.fdTransfer.stop()
	}
	if a.socket != "" {
		os.Remove(a.socket)
		a.socket = ""
	}
	if a.fdSocketPath != "" {
		os.Remove(a.fdSocketPath)
	}
}

func (a *apiServer) LoadCache(ctx context.Context, req *pb.LoadCacheRequest) (*pb.LoadCacheResponse, error) {
	broadcast := req.GetBroadcast()

	// Handle cancel request first
	if req.GetCancel() {
		a.cancelLoadCount.Inc()
		atomic.AddInt32(&a.cancelLoadFlight, 1)
		defer atomic.AddInt32(&a.cancelLoadFlight, -1)
		logger.Infof("API Server: Cancel preload, broadcast=%v", broadcast)
		if broadcast {
			if err := a.broadcastPreload([]string{}, false, false, true); err != nil {
				return nil, err
			}
		}

		request := &loadReq{
			cancel: true,
		}
		eno := a.fs.loadCache(request)
		if eno != 0 {
			logger.Warnf("API Server: Cancel preload failed: %s", eno)
			return nil, eno
		}
		return &pb.LoadCacheResponse{}, nil
	}

	a.loadCacheCount.Inc()
	atomic.AddInt32(&a.loadCacheFlight, 1)
	defer atomic.AddInt32(&a.loadCacheFlight, -1)

	paths := req.GetPaths()
	background := req.GetBackground()
	loadAll := req.GetLoadAll()
	force := req.GetForce()

	logger.Infof("API Server: LoadCache paths %v, background=%v, broadcast=%v", paths, background, broadcast)

	if len(paths) == 0 {
		return &pb.LoadCacheResponse{}, nil
	}

	// Broadcast to cluster if requested
	if broadcast {
		if err := a.broadcastPreload(paths, loadAll, force, false); err != nil {
			return nil, err
		}
	}

	// Always perform local preload
	var count, bytes uint64
	done := make(chan struct{})
	loadCtx := ctx
	if background {
		loadCtx = context.Background()
		done = nil
	}
	request := &loadReq{
		ctx:     loadCtx,
		paths:   paths,
		count:   &count,
		bytes:   &bytes,
		loadAll: loadAll,
		force:   force,
		done:    done,
	}
	eno := a.fs.loadCache(request)
	if eno != 0 {
		if eno == syscall.EINPROGRESS {
			logger.Infof("API Server: LoadCache in progress: %s", eno)
		} else {
			logger.Warnf("API Server: LoadCache failed: %s", eno)
		}
		return nil, eno
	}

	if background {
		return &pb.LoadCacheResponse{}, nil
	}

	select {
	case <-request.ctx.Done():
		logger.Info("API Server: LoadCache cancelled")
		return nil, errors.New("operation cancelled")
	case <-done:
	}

	resp := &pb.LoadCacheResponse{
		LoadCount: count,
		LoadBytes: bytes,
	}
	return resp, nil
}

func (a *apiServer) broadcastPreload(paths []string, loadAll, force, cancel bool) error {
	if a.fs.serf == nil {
		return errors.New("serf service not available for broadcast")
	}
	info := &serfInfo{
		SrcAddr: a.fs.serf.LocalAddr(),
		Paths:   paths,
		LoadAll: loadAll,
		Force:   force,
		Cancel:  cancel,
	}
	err := a.fs.broadCast(preloadEvent, info)
	if err != nil {
		return fmt.Errorf("broadcast preload failed: %s", err)
	}
	if cancel {
		logger.Info("Successfully broadcast preload cancellation request to cluster")
	} else {
		logger.Infof("Successfully broadcast preload request to cluster, paths: %v", paths)
	}
	return nil
}

func (a *apiServer) ClearCache(ctx context.Context, req *pb.ClearCacheRequest) (*pb.ClearCacheResponse, error) {
	a.clearCacheCount.Inc()
	atomic.AddInt32(&a.clearCacheFlight, 1)
	defer atomic.AddInt32(&a.clearCacheFlight, -1)

	paths := req.GetPaths()
	broadcast := req.GetBroadcast()
	clearBurst := req.GetClearBurst()
	logger.Infof("API Server: ClearCache of paths %v, broadcast=%v, clear_burst=%v", paths, broadcast, clearBurst)
	if a.fs.Conf.IsFuseLayerMode() {
		// TODO: Fuse layer mode doesn't support clear cache yet
		return nil, errors.New("clear cache not supported in fuse layer mode")
	}

	// Handle clear burst mode
	if clearBurst {
		if broadcast {
			if a.fs.serf == nil {
				return nil, errors.New("serf service not available for broadcast")
			}
			info := &serfInfo{
				SrcAddr:    a.fs.serf.LocalAddr(),
				Paths:      paths,
				ClearBurst: true,
			}
			if err := a.fs.broadCast(clearEvent, info); err != nil {
				return nil, fmt.Errorf("broadcast clear burst failed: %s", err)
			}
			logger.Info("Successfully broadcast clear burst request to cluster")
		}
		start := time.Now()
		count, bytes := a.fs.cache.clearBurst()
		logger.Infof("Clear burst: evicted %d blocks %dMiB in %s", count, bytes>>20, time.Since(start))
		return &pb.ClearCacheResponse{
			ClearCount: uint64(count),
			ClearBytes: uint64(bytes),
		}, nil
	}

	if len(paths) == 0 {
		return &pb.ClearCacheResponse{}, nil
	}

	// Broadcast to cluster if requested
	if broadcast {
		if a.fs.serf == nil {
			return nil, errors.New("serf service not available for broadcast")
		}
		info := &serfInfo{
			SrcAddr: a.fs.serf.LocalAddr(),
			Paths:   paths,
		}
		err := a.fs.broadCast(clearEvent, info)
		if err != nil {
			return nil, fmt.Errorf("broadcast clear cache failed: %s", err)
		}
		logger.Infof("Successfully broadcast clear cache request to cluster, paths: %v", paths)
	}

	// Convert paths to inodes first by looking up from filesystem
	prefix := a.fs.pathsToCachePrefixes(paths)
	if len(prefix) == 0 {
		logger.Warn("API Server: ClearCache failed to convert paths to cache prefixes")
		return &pb.ClearCacheResponse{}, nil
	}

	start := time.Now()
	count, bytes := a.fs.cache.removeByPrefix(prefix, nil)
	logger.Infof("Evicted %d blocks %dMiB cache from paths: %v, with prefix: %v, in %ss",
		count, bytes>>20, paths, prefix, time.Since(start))
	return &pb.ClearCacheResponse{
		ClearCount: uint64(count),
		ClearBytes: uint64(bytes),
	}, nil
}

func (a *apiServer) SourceReady(ctx context.Context, req *pb.SourceReadyRequest) (*pb.SourceReadyResponse, error) {
	a.sourceReadyCount.Inc()
	atomic.AddInt32(&a.sourceReadyFlight, 1)
	defer atomic.AddInt32(&a.sourceReadyFlight, -1)

	background := req.GetBackground()
	logger.Infof("API Server: SourceReady request, background %v", background)
	if !a.fs.Conf.Meta.MetaProvider() {
		return nil, errors.New("set source ready not supported: not in sourceless meta provider mode")
	}

	eno := a.fs.Meta.SourceReady(true)
	if eno != 0 {
		logger.Warnf("API Server: Prepare SourceReady failed: %s", eno)
		return nil, eno
	}

	var count, bytes uint64
	start := time.Now()
	done := make(chan struct{})
	request := &loadReq{
		ctx:     ctx,
		paths:   []string{"/"},
		count:   &count,
		bytes:   &bytes,
		loadAll: true,
		force:   true,
		done:    done,
	}
	if background {
		request.ctx = context.Background()
	}

	eno = a.fs.loadCache(request)
	if eno != 0 {
		if eno == syscall.EINPROGRESS {
			logger.Infof("API Server: SourceReady: Load data in progress: %s", eno)
		} else {
			logger.Warnf("API Server: SourceReady: Load data failed: %s", eno)
		}
		return nil, eno
	}

	sourceReady := func() error {
		// Load data done, announce meta data
		eno := a.fs.Meta.SourceReady(false)
		if eno != 0 {
			logger.Warnf("API Server: Set SourceReady failed: %s", eno)
			return eno
		} else {
			logger.Infof("API Server: Set SourceReady in %s", time.Since(start))
			return nil
		}
	}

	if background {
		go func() {
			<-done
			_ = sourceReady()
		}()
		return &pb.SourceReadyResponse{}, nil
	}

	select {
	case <-request.ctx.Done():
		logger.Info("API Server: SourceReady cancelled")
		return nil, errors.New("operation cancelled")
	case <-done:
	}
	err := sourceReady()
	if err != nil {
		return nil, err
	} else {
		return &pb.SourceReadyResponse{
			LoadCount: count,
			LoadBytes: bytes,
		}, nil
	}
}

func (a *apiServer) MetaReady(ctx context.Context, req *pb.MetaReadyRequest) (*pb.MetaReadyResponse, error) {
	a.metaReadyCount.Inc()
	atomic.AddInt32(&a.metaReadyFlight, 1)
	defer atomic.AddInt32(&a.metaReadyFlight, -1)

	logger.Info("API Server: Set MetaReady")
	if !a.fs.Conf.Meta.PeerMeta() {
		return nil, errors.New("set meta ready not supported: source peermeta not enabled")
	}
	eno := a.fs.Meta.MetaReady()
	if eno != 0 {
		return nil, eno
	}
	return &pb.MetaReadyResponse{}, nil
}

func (a *apiServer) DataReady(ctx context.Context, req *pb.DataReadyRequest) (*pb.DataReadyResponse, error) {
	a.dataReadyCount.Inc()
	atomic.AddInt32(&a.dataReadyFlight, 1)
	defer atomic.AddInt32(&a.dataReadyFlight, -1)

	logger.Info("API Server: Set DataReady")
	if !a.fs.Conf.Meta.MetaConsumer() {
		return nil, errors.New("set data ready not supported: not in sourceless meta consumer mode")
	}
	a.fs.cache.dataReady()
	return &pb.DataReadyResponse{}, nil
}

func (a *apiServer) GetPid(ctx context.Context, req *pb.GetPidRequest) (*pb.GetPidResponse, error) {
	a.getPidCount.Inc()
	atomic.AddInt32(&a.getPidFlight, 1)
	defer atomic.AddInt32(&a.getPidFlight, -1)

	logger.Info("API Server: GetPid request")
	res := &pb.GetPidResponse{
		Pid: int64(os.Getpid()),
	}
	return res, nil
}

func (a *apiServer) GetPeers(ctx context.Context, req *pb.GetPeersRequest) (*pb.GetPeersResponse, error) {
	a.getPeersCount.Inc()
	atomic.AddInt32(&a.getPeersFlight, 1)
	defer atomic.AddInt32(&a.getPeersFlight, -1)

	raw := req.GetRaw()
	withPort := req.GetPort()
	logger.Infof("API Server: GetPeers request, raw=%v, port=%v", raw, withPort)
	if a.fs.serf == nil {
		return nil, errors.New("serf service not available")
	}
	peers := a.fs.serf.GetMembers(raw)
	if !withPort {
		for i, peer := range peers {
			host, _, err := net.SplitHostPort(peer)
			if err == nil {
				peers[i] = host
			}
		}
	}
	return &pb.GetPeersResponse{Peers: peers}, nil
}

func (a *apiServer) Unmount(ctx context.Context, req *pb.UnmountRequest) (*pb.UnmountResponse, error) {
	a.unmountCount.Inc()
	atomic.AddInt32(&a.unmountFlight, 1)
	defer atomic.AddInt32(&a.unmountFlight, -1)

	pid := req.GetPid()
	name := req.GetName()
	mp := a.fs.Conf.Meta.MountPoint
	logger.Infof("API Server: Unmount %s from process %s(%d)", mp, name, pid)
	done := make(chan struct{})
	go func() {
		_ = utils.Unmount(mp, true)
		close(done)
	}()
	timer := time.NewTimer(time.Second * 5)
	select {
	case <-done:
	case <-timer.C:
		logger.Infof("API Server: unmount 5s timed out, force exit")
		os.Exit(1)
	}
	return &pb.UnmountResponse{}, nil
}

// shareCap returns the mmap API's cache budget: burst capacity minus the reserved headroom for
// regular read traffic. A negative reserve is treated as 0 so the budget never exceeds burstCap.
func shareCap(burstCap, reserve int64) int64 {
	return max(0, burstCap-max(0, reserve))
}

// getBlockRange validates the optional half-open byte range and returns every complete cache block
// it intersects.
func getBlockRange(fileSize uint64, blockSize uint32, requested *pb.FileRange) (*blockRange, error) {
	if requested != nil && requested.GetLength() == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "file range length must be positive, got %d", requested.GetLength())
	}
	if fileSize == 0 {
		return &blockRange{}, nil
	}
	if blockSize == 0 {
		return nil, status.Error(codes.Internal, "cache block size is zero")
	}

	start, length := uint64(0), fileSize
	if requested != nil {
		start, length = requested.GetStart(), requested.GetLength()
		end := start + length
		if start >= fileSize || end < start || end > fileSize {
			return nil, status.Error(codes.InvalidArgument, "file range is outside the file")
		}
	}

	first := start / uint64(blockSize)
	last := (start + length - 1) / uint64(blockSize)
	if last > uint64(^uint32(0)) {
		return nil, status.Error(codes.InvalidArgument, "file range has too many blocks")
	}
	count := last - first + 1
	if count > uint64(^uint(0)>>1) {
		return nil, status.Error(codes.InvalidArgument, "file range has too many blocks")
	}
	bytes, err := utils.GetRangeBlockBytes(fileSize, start, length, blockSize)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "file range block bytes: %v", err)
	}

	br := &blockRange{
		shareBlocks: make([]shareBlock, 0, count),
		bytes:       bytes,
	}
	for block := first; block <= last; block++ {
		blockIndex := uint32(block)
		blockBytes := int64(utils.GetBlockSize(blockSize, fileSize, blockIndex))
		br.shareBlocks = append(br.shareBlocks, shareBlock{index: blockIndex, bytes: blockBytes})
	}
	return br, nil
}

// releaseShare cleans up one GetFileChunks session: it unlocks the whole target for a request
// without a range, or the selected blocks for an explicit range, then credits its quota lease.
// Called when a session ends, i.e. the client sent ReleaseFileChunks, the fd-transfer connection
// closed (client crash), or the request expired before the client picked up its fds.
func (a *apiServer) releaseShare(fdInfo *fdTransferInfo, source string) {
	var lockBlocks []uint32
	if !fdInfo.allBlocks {
		lockBlocks = shareBlockIndexes(fdInfo.shareLease.blocks)
	}
	a.fs.cache.unlockTarget(fdInfo.target, lockBlocks)
	a.quota.release(fdInfo.shareLease)
	a.shareHoldHist.Observe(time.Since(fdInfo.shareStart).Seconds())
	switch source {
	case shareReleaseRpc:
		a.shareReleaseRpcCount.Inc()
	case shareReleaseEof:
		a.shareReleaseEofCount.Inc()
	case shareReleaseExpired:
		a.shareReleaseExpCount.Inc()
	}
	logger.Debugf("API Server: released share for target %s (%s), range %s",
		fdInfo.target, source, fmtFileRange(fdInfo.fileRange))
}

// ReleaseFileChunks is the normal end of a mmap session: the client calls it
// after munmap so the daemon can unlock the file's cached blocks. If the
// client crashes instead, the daemon notices via the fd-transfer connection
// closing and runs the same cleanup.
func (a *apiServer) ReleaseFileChunks(ctx context.Context, req *pb.ReleaseFileChunksRequest) (*pb.ReleaseFileChunksResponse, error) {
	a.releaseFileCount.Inc()
	requestId := req.GetRequestId()
	fdInfo := a.fdTransfer.takeRequest(requestId)
	if fdInfo == nil {
		// Already drained (EOF raced us, or expired, or unknown id): no-op.
		logger.Infof("API Server: ReleaseFileChunks request %d not found", requestId)
		return &pb.ReleaseFileChunksResponse{}, nil
	}
	a.fdTransfer.release(fdInfo, shareReleaseRpc)

	// A zero claimedAt means the fds were never picked up, so print that instead of a nonsense
	// since-epoch duration.
	mapped := "unclaimed"
	if !fdInfo.claimedAt.IsZero() {
		mapped = time.Since(fdInfo.claimedAt).String()
	}
	logger.Infof("API Server: ReleaseFileChunks request %d target %s range %s, held %s mapped %s",
		requestId, fdInfo.target, fmtFileRange(fdInfo.fileRange), time.Since(fdInfo.shareStart), mapped)
	return &pb.ReleaseFileChunksResponse{}, nil
}

// Get all backed fd of cached blocks of a given inode
func (a *apiServer) GetFileChunks(ctx context.Context, req *pb.GetFileChunksRequest) (resp *pb.GetFileChunksResponse, err error) {
	a.getFileCount.Inc()
	atomic.AddInt32(&a.getFileFlight, 1)
	defer atomic.AddInt32(&a.getFileFlight, -1)

	inode := meta.Ino(req.GetInode())
	logger.Infof("API Server: GetFileChunks for inode %d, range %s", inode, fmtFileRange(req.GetRange()))

	// Special inodes are not supported
	if IsSpecialNode(inode) {
		return nil, errors.New("cannot get chunks for special nodes")
	}

	cino, eno := a.fs.Meta.GetInode(inode)
	if eno != 0 {
		return nil, fmt.Errorf("get inode %d failed: %s", inode, eno)
	}

	// Only support regular file
	var attr meta.Attr
	eno = a.fs.Meta.GetAttr(meta.Background, inode, &attr)
	if eno != 0 {
		return nil, fmt.Errorf("get attributes for inode %d failed: %s", inode, eno)
	}
	if attr.Typ != meta.TypeFile {
		return nil, errors.New("inode is not a regular file")
	}

	target := a.fs.inodeToCacheTarget(cino, &attr)
	srcTarget := cino.GetPath()
	fileSize := attr.Length
	blockSize := a.fs.Conf.Store.BlockSize

	// Get block indexes that cover the given range.
	requestedRange := req.GetRange()
	allBlocks := requestedRange == nil
	br, err := getBlockRange(fileSize, blockSize, requestedRange)
	if err != nil {
		return nil, err
	}
	if fileSize == 0 {
		return &pb.GetFileChunksResponse{
			FileSize:  fileSize,
			BlockSize: blockSize,
			Chunks:    []*pb.FileChunk{},
		}, nil
	}

	// Fast reject: selected blocks larger than the GetFileChunks cache budget can never be
	// admitted, so fail before loading any blocks. admit() re-checks this under the quota lock,
	// so this is only an early friendly error, not a guard.
	shareCap := a.quota.getCap()
	if br.bytes > shareCap {
		a.shareTooLarge.Inc()
		return nil, shareTooLargeErr(br.bytes, shareCap)
	}

	// Reserve cache budget for the selected blocks, then lock them against eviction before
	// loading any of them. wait_timeout_ms bounds the budget wait (0 = wait forever); ttrpc ctx
	// cancellation is also honored.
	var deadline time.Time
	if ms := req.GetWaitTimeoutMs(); ms > 0 {
		deadline = time.Now().Add(time.Duration(ms) * time.Millisecond)
	}
	lease, waited, err := a.quota.admit(ctx, target, br.shareBlocks, deadline)
	if err != nil {
		switch {
		case status.Code(err) == codes.FailedPrecondition:
			a.shareTooLarge.Inc()
		case errors.Is(err, ErrShareWaitTimeout):
			a.shareWaitTimeouts.Inc()
		case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
			a.shareWaitCanceled.Inc()
			err = status.Error(codes.Canceled, err.Error())
		}
		return nil, err
	}
	a.shareAdmits.Inc()
	if waited > 0 {
		a.shareAdmitWaited.Inc()
	}
	a.shareWaitHist.Observe(waited.Seconds())
	var lockBlocks []uint32
	if !allBlocks {
		lockBlocks = shareBlockIndexes(br.shareBlocks)
	}
	shareStart := time.Now()
	a.fs.cache.lockTarget(target, lockBlocks)

	// If we return before registering the fd-transfer entry, roll back the lock and budget
	// ourselves. Once registered, the entry's cleanup paths own it.
	registered := false
	defer func() {
		if !registered {
			a.fs.cache.unlockTarget(target, lockBlocks)
			a.quota.release(lease)
			a.shareRollback.Inc()
		}
	}()

	// Get source file in case there's no local cache nor peer cache
	sourceFile := a.fs.source.newSourceFile(srcTarget, fileSize)
	defer sourceFile.release()

	var chunkMutex sync.Mutex
	var wg sync.WaitGroup
	var loadErr error
	chunks := make([]*pb.FileChunk, len(br.shareBlocks))
	fdChunks := make([]*FdChunkInfo, len(br.shareBlocks))

	// On any error return, the loaded chunks are never handed off to the fd transfer service.
	// So release them here to avoid leaking exported fds and the page refs held by each reader.
	// This release is authoritative: the exported fd never left this process, so the export
	// count is given back and the block stays poolable.
	defer func() {
		if err != nil {
			for _, fdChunk := range fdChunks {
				if fdChunk != nil {
					fdChunk.Close()
					if fdChunk.memfd != nil {
						fdChunk.memfd.UnexportFd()
					}
				}
			}
		}
	}()

	// Get blocks concurrently, using global token pool
	start := time.Now()
	for chunkIndex, shareBlock := range br.shareBlocks {
		wg.Add(1)
		go func(chunkIndex int, blockIndex uint32) {
			a.getFileTokens.Get()
			defer a.getFileTokens.Put()
			defer wg.Done()
			chunkSize := utils.GetBlockSize(blockSize, fileSize, blockIndex)

			// load from store
		read_from_store:
			reader, err := a.fs.cache.readFromStore(ctx, target, fileSize, blockIndex, uint32(chunkSize), true)
			if err != nil {
				// check if there's a pending cache operation
				cached := a.fs.checkPendingCache(target, blockIndex)
				if cached {
					goto read_from_store
				}

				// cache miss, load from source and add data to cache
				cacheMiss.Inc()
				cacheMissBytes.Add(float64(chunkSize))

				reader, err = a.fs.sourceReader.readFromSource(sourceFile, target, fileSize, blockIndex, true)
				if err != nil {
					chunkMutex.Lock()
					if loadErr == nil {
						loadErr = fmt.Errorf("failed to read block %d: %s", blockIndex, err)
					}
					chunkMutex.Unlock()
					return
				}
			}
			cacheHits.Inc()
			cacheHitBytes.Add(float64(chunkSize))

			// Export the cache backend fd: it leaves this process for the client,
			// which mmaps it. memfd is the only backend whose block can be recycled
			// underneath such a mapping, so it also reports its *utils.Memfd here,
			// which now counts this exported copy.
			dupFd, memfd, err := reader.ExportFd()
			if err != nil {
				chunkMutex.Lock()
				if loadErr == nil {
					loadErr = fmt.Errorf("block %d export fd: %s", blockIndex, err)
				}
				chunkMutex.Unlock()
				return
			}

			chunk := &pb.FileChunk{
				BlockIndex: blockIndex,
				Size:       uint32(chunkSize),
			}
			fdChunk := &FdChunkInfo{
				BlockIndex: blockIndex,
				fd:         dupFd,
				reader:     reader,
				memfd:      memfd,
			}

			chunkMutex.Lock()
			chunks[chunkIndex] = chunk
			fdChunks[chunkIndex] = fdChunk
			chunkMutex.Unlock()
		}(chunkIndex, shareBlock.index)
	}
	wg.Wait()
	if loadErr != nil {
		return nil, loadErr
	}

	// register fd chunks to fd transfer service
	requestId := a.getRequestID()
	respSize, err := a.fdTransfer.addRequest(requestId, target, allBlocks, lease, requestedRange, shareStart, fdChunks)
	if err != nil {
		return nil, err
	}

	// The chunks now belong to the fd-transfer entry; disarm the error-close defer so a future
	// error return can't double-close them.
	fdChunks = nil
	registered = true

	// Only print range size when a range is requested.
	rangeSize := ""
	if requestedRange != nil {
		rangeSize = fmt.Sprintf(" range size %s", utils.FormatBytes(uint64(br.bytes)))
	}
	logger.Infof("API Server: GetFileChunks for \"%s\"(%d) file size %s range %s%s in %s (share wait %s), %d chunks, request ID %d",
		cino.GetPath(), inode, utils.FormatBytes(fileSize), fmtFileRange(requestedRange),
		rangeSize, time.Since(start), waited, len(chunks), requestId)

	return &pb.GetFileChunksResponse{
		FileSize:   fileSize,
		BlockSize:  blockSize,
		Chunks:     chunks,
		RequestId:  requestId,
		FdRespSize: uint32(respSize),
	}, nil

}

// fmtFileRange renders a request range for logs. Proto text marshaling omits zero-valued fields,
// hiding a zero start, so spell the half-open interval out; nil means the whole file.
func fmtFileRange(r *pb.FileRange) string {
	if r == nil {
		return "full file"
	}
	return fmt.Sprintf("[%d,%d)", r.GetStart(), r.GetStart()+r.GetLength())
}

// fmtSetCacheSizeArg renders a SetCacheSize request argument for logging,
// printing the sentinels as words instead of the misleading >>20 result
// (e.g. BurstCacheSizeRestore=-2 would otherwise show as -1MB).
func fmtSetCacheSizeArg(v int64) string {
	switch v {
	case chunkcommon.CacheSizeKeepCurrent:
		return "keep"
	case chunkcommon.BurstCacheSizeRestore:
		return "restore"
	default:
		return fmt.Sprintf("%dMB", v>>20)
	}
}

// GetServerInfo returns a read-only snapshot of server information, such as cache configuration.
func (a *apiServer) GetServerInfo(ctx context.Context, req *pb.GetServerInfoRequest) (*pb.GetServerInfoResponse, error) {
	a.getServerInfoCount.Inc()
	atomic.AddInt32(&a.getServerInfoFlight, 1)
	defer atomic.AddInt32(&a.getServerInfoFlight, -1)

	var blockSize uint32
	var cacheSize, burstCacheSize int64
	if a.fs != nil {
		if a.fs.Conf != nil && a.fs.Conf.Store != nil {
			blockSize = a.fs.Conf.Store.BlockSize
		}
		if a.fs.cache != nil {
			cacheSize, burstCacheSize = a.fs.cache.getCacheSize()
		}
	}

	format := func(size int64) string {
		return utils.FormatBytesShort(uint64(size))
	}
	shareCap := a.quota.getCap()
	logger.Infof("API Server: GetServerInfo, BlockSize %s, CacheSize %s, BurstCacheSize %s, ShareCap %s",
		format(int64(blockSize)), format(cacheSize), format(burstCacheSize), format(shareCap))
	return &pb.GetServerInfoResponse{
		BlockSize:      blockSize,
		CacheSize:      cacheSize,
		BurstCacheSize: burstCacheSize,
		Pid:            int64(os.Getpid()),
		ShareCap:       shareCap,
	}, nil
}

func (a *apiServer) SetCacheSize(ctx context.Context, req *pb.SetCacheSizeRequest) (*pb.SetCacheSizeResponse, error) {
	a.setCacheSizeCount.Inc()
	atomic.AddInt32(&a.setCacheSizeFlight, 1)
	defer atomic.AddInt32(&a.setCacheSizeFlight, -1)
	cacheSize := req.GetCacheSize()
	burstSize := req.GetBurstCacheSize()
	broadcast := req.GetBroadcast()
	logger.Infof("API Server: SetCacheSize cacheSize=%s burstSize=%s broadcast=%v",
		fmtSetCacheSizeArg(cacheSize), fmtSetCacheSizeArg(burstSize), broadcast)

	if a.fs.Conf.IsFuseLayerMode() {
		return nil, errors.New("set cache size not supported in fuse layer mode")
	}
	if cacheSize < 0 && cacheSize != chunkcommon.CacheSizeKeepCurrent {
		return nil, fmt.Errorf("invalid cache size sentinel: %d", cacheSize)
	}
	if burstSize < 0 && burstSize != chunkcommon.CacheSizeKeepCurrent && burstSize != chunkcommon.BurstCacheSizeRestore {
		return nil, fmt.Errorf("invalid burst cache size sentinel: %d", burstSize)
	}

	if broadcast {
		if a.fs.serf == nil {
			return nil, errors.New("serf service not available for broadcast")
		}
		info := &serfInfo{
			SrcAddr:        a.fs.serf.LocalAddr(),
			CacheSize:      cacheSize,
			BurstCacheSize: burstSize,
		}
		if err := a.fs.broadCast(setCacheEvent, info); err != nil {
			return nil, fmt.Errorf("broadcast set-cache failed: %s", err)
		}
		logger.Info("Successfully broadcast set-cache request to cluster")
	}

	start := time.Now()
	newCacheSize, newBurstSize, evictCnt, evictBytes := a.fs.cache.setCacheSize(cacheSize, burstSize)
	evictDur := time.Since(start)

	// The mmap budget tracks burst capacity; update it. If the shrink makes a
	// queued request impossible to admit, wakeFront fails it.
	newShareCap := shareCap(newBurstSize, a.shareReserve)
	a.quota.setCap(newShareCap)
	logger.Infof("SetCacheSize done: cache=%dMB burst=%dMB share=%dMB evicted %d blocks %dMiB in %v",
		newCacheSize>>20, newBurstSize>>20, newShareCap>>20, evictCnt, evictBytes>>20, evictDur)
	return &pb.SetCacheSizeResponse{
		CacheSize:      newCacheSize,
		BurstCacheSize: newBurstSize,
		EvictCount:     uint64(evictCnt),
		EvictBytes:     uint64(evictBytes),
		ShareCap:       newShareCap,
	}, nil
}

func (a *apiServer) getRequestID() uint32 {
	return atomic.AddUint32(&a.requestCounter, 1)
}

// GetLoadStats returns the data-loading report, optionally also writing it to log so it outlives
// the process.
func (a *apiServer) GetLoadStats(ctx context.Context, req *pb.GetLoadStatsRequest) (*pb.GetLoadStatsResponse, error) {
	a.getLoadStatsCount.Inc()
	atomic.AddInt32(&a.getLoadStatsFlight, 1)
	defer atomic.AddInt32(&a.getLoadStatsFlight, -1)

	if !req.GetLog() {
		// With log=true the report itself lands in the log below, so an entry line
		// would only announce a copy of what follows it.
		logger.Infof("API Server: GetLoadStats request")
	}

	if a.fs.Conf.IsFuseLayerMode() {
		// not supported yet
		return nil, status.Error(codes.Unimplemented, "load stats not supported in fuse layer mode")
	}

	stats := a.fs.collectLoadStats()
	summary, detail := stats.Format()
	payload, err := json.Marshal(stats)
	if err != nil {
		return nil, fmt.Errorf("marshal load stats: %w", err)
	}

	if req.GetLog() {
		logger.Info(summary)
		logger.Info(detail)
	}
	return &pb.GetLoadStatsResponse{Payload: payload, Text: detail}, nil
}

func (a *apiServer) InitMetrics(reg prometheus.Registerer) {
	a.loadCacheCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vfs_api_loadcache",
		Help: "number of load cache requests",
	})
	a.clearCacheCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vfs_api_clearcache",
		Help: "number of clear cache requests",
	})
	a.cancelLoadCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vfs_api_cancelload",
		Help: "number of cancel load cache requests",
	})
	a.sourceReadyCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vfs_api_sourceready",
		Help: "number of source ready requests",
	})
	a.metaReadyCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vfs_api_metaready",
		Help: "number of meta ready requests",
	})
	a.dataReadyCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vfs_api_dataready",
		Help: "number of data ready requests",
	})
	a.getPidCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vfs_api_getpid",
		Help: "number of get pid requests",
	})
	a.unmountCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vfs_api_unmount",
		Help: "number of unmount requests",
	})
	a.getFileCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vfs_api_getfile",
		Help: "number of get file chunks requests",
	})
	a.releaseFileCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vfs_api_releasefile",
		Help: "number of release file chunks requests",
	})
	a.getPeersCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vfs_api_getpeers",
		Help: "number of get peers requests",
	})
	a.getServerInfoCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vfs_api_getserverinfo",
		Help: "number of get server info requests",
	})
	a.setCacheSizeCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vfs_api_setcachesize",
		Help: "number of set cache size requests",
	})
	a.getLoadStatsCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vfs_api_getloadstats",
		Help: "number of get load stats requests",
	})
	a.shareAdmits = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vfs_api_share_admits",
		Help: "mmap requests admitted (immediately or after waiting for cache budget)",
	})
	a.shareAdmitWaited = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vfs_api_share_admit_waited",
		Help: "mmap requests that had to wait for cache budget before admission",
	})
	a.shareWaitTimeouts = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vfs_api_share_wait_timeouts",
		Help: "mmap requests that gave up waiting for cache budget (wait_timeout_ms)",
	})
	a.shareWaitCanceled = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vfs_api_share_wait_canceled",
		Help: "mmap requests canceled while queued waiting for cache budget",
	})
	a.shareTooLarge = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vfs_api_share_too_large",
		Help: "mmap requests rejected: file larger than the cache budget",
	})
	a.shareReleaseRpcCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vfs_api_share_release_rpc",
		Help: "mmap sessions released via ReleaseFileChunks RPC (normal path)",
	})
	a.shareReleaseEofCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vfs_api_share_release_eof",
		Help: "mmap sessions released via fd-transfer connection EOF (client crash or old SDK)",
	})
	a.shareReleaseExpCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vfs_api_share_release_expired",
		Help: "mmap requests expired before the client picked up its fds",
	})
	a.shareRollback = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "vfs_api_share_rollback",
		Help: "mmap requests rolled back after admission but before fd-transfer registration",
	})
	a.shareWaitHist = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "vfs_api_share_wait_seconds",
		Help:    "cache budget wait duration, observed at successful admission",
		Buckets: []float64{.001, .01, .1, .5, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048},
	})
	a.shareHoldHist = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "vfs_api_share_hold_seconds",
		Help:    "mmap session duration (admit to release), observed at every release",
		Buckets: []float64{.01, .1, .5, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096},
	})
	if reg == nil {
		return
	}

	reg.MustRegister(a.loadCacheCount)
	reg.MustRegister(a.clearCacheCount)
	reg.MustRegister(a.cancelLoadCount)
	reg.MustRegister(a.sourceReadyCount)
	reg.MustRegister(a.metaReadyCount)
	reg.MustRegister(a.dataReadyCount)
	reg.MustRegister(a.getPidCount)
	reg.MustRegister(a.unmountCount)
	reg.MustRegister(a.getFileCount)
	reg.MustRegister(a.releaseFileCount)
	reg.MustRegister(a.getPeersCount)
	reg.MustRegister(a.getServerInfoCount)
	reg.MustRegister(a.setCacheSizeCount)
	reg.MustRegister(a.getLoadStatsCount)
	reg.MustRegister(a.shareAdmits)
	reg.MustRegister(a.shareAdmitWaited)
	reg.MustRegister(a.shareWaitTimeouts)
	reg.MustRegister(a.shareWaitCanceled)
	reg.MustRegister(a.shareTooLarge)
	reg.MustRegister(a.shareReleaseRpcCount)
	reg.MustRegister(a.shareReleaseEofCount)
	reg.MustRegister(a.shareReleaseExpCount)
	reg.MustRegister(a.shareRollback)
	reg.MustRegister(a.shareWaitHist)
	reg.MustRegister(a.shareHoldHist)

	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "vfs_api_shared_bytes",
			Help: "total bytes of files currently mmap'd by clients, charged once per file",
		},
		func() float64 {
			a.quota.mu.Lock()
			defer a.quota.mu.Unlock()
			return float64(a.quota.total)
		}))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "vfs_api_shared_blocks",
			Help: "total blocks of files currently mmap'd by clients",
		},
		func() float64 {
			a.quota.mu.Lock()
			defer a.quota.mu.Unlock()
			return float64(a.quota.sharedBlocks)
		}))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "vfs_api_shared_files",
			Help: "number of distinct files currently mmap'd by clients",
		},
		func() float64 {
			a.quota.mu.Lock()
			defer a.quota.mu.Unlock()
			return float64(len(a.quota.targets))
		}))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "vfs_api_share_cap_bytes",
			Help: "mmap API cache budget in bytes (burst cache size minus share reserve)",
		},
		func() float64 {
			a.quota.mu.Lock()
			defer a.quota.mu.Unlock()
			return float64(a.quota.cap)
		}))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "vfs_api_share_waiters",
			Help: "mmap requests currently queued waiting for cache budget",
		},
		func() float64 {
			a.quota.mu.Lock()
			defer a.quota.mu.Unlock()
			return float64(a.quota.waiters.Len())
		}))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "vfs_api_share_waiting_bytes",
			Help: "total file bytes of mmap requests queued waiting for cache budget",
		},
		func() float64 {
			a.quota.mu.Lock()
			defer a.quota.mu.Unlock()
			return float64(a.quota.waitingBytes)
		}))

	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "vfs_api_loadcache_flight",
			Help: "in flight load cache request",
		},
		func() float64 {
			return float64(atomic.LoadInt32(&a.loadCacheFlight))
		}))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "vfs_api_clearcache_flight",
			Help: "in flight clear cache request",
		},
		func() float64 {
			return float64(atomic.LoadInt32(&a.clearCacheFlight))
		}))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "vfs_api_cancelload_flight",
			Help: "in flight cancel load cache request",
		},
		func() float64 {
			return float64(atomic.LoadInt32(&a.cancelLoadFlight))
		}))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "vfs_api_sourceready_flight",
			Help: "in flight source ready request",
		},
		func() float64 {
			return float64(atomic.LoadInt32(&a.sourceReadyFlight))
		}))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "vfs_api_metaready_flight",
			Help: "in flight meta ready request",
		},
		func() float64 {
			return float64(atomic.LoadInt32(&a.metaReadyFlight))
		}))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "vfs_api_dataready_flight",
			Help: "in flight data ready request",
		},
		func() float64 {
			return float64(atomic.LoadInt32(&a.dataReadyFlight))
		}))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "vfs_api_getpid_flight",
			Help: "in flight get pid request",
		},
		func() float64 {
			return float64(atomic.LoadInt32(&a.getPidFlight))
		}))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "vfs_api_unmount_flight",
			Help: "in flight unmount request",
		},
		func() float64 {
			return float64(atomic.LoadInt32(&a.unmountFlight))
		}))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "vfs_api_getfile_flight",
			Help: "in flight get file chunks request",
		},
		func() float64 {
			return float64(atomic.LoadInt32(&a.getFileFlight))
		}))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "vfs_api_getpeers_flight",
			Help: "in flight get peers request",
		},
		func() float64 {
			return float64(atomic.LoadInt32(&a.getPeersFlight))
		}))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "vfs_api_getserverinfo_flight",
			Help: "in flight get server info request",
		},
		func() float64 {
			return float64(atomic.LoadInt32(&a.getServerInfoFlight))
		}))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "vfs_api_setcachesize_flight",
			Help: "in flight set cache size request",
		},
		func() float64 {
			return float64(atomic.LoadInt32(&a.setCacheSizeFlight))
		}))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "vfs_api_getloadstats_flight",
			Help: "in flight get load stats request",
		},
		func() float64 {
			return float64(atomic.LoadInt32(&a.getLoadStatsFlight))
		}))
}

func getFdSocketPath(sock string) string {
	return sock + ".fd"
}
