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

package chunk

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	pb "github.com/aliyun/cachefs/pkg/rpc/cachefs"
	"github.com/aliyun/cachefs/pkg/transfer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func (t *transport) Load(stream pb.CachefsCache_LoadServer) (rerr error) {
	// First recv gets LoadRequest
	req, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.Unknown, "recv request: %s", err)
	}

	target := req.GetTarget()
	blknum := req.GetBlockNum()
	blksiz := req.GetBlockSize()
	blklen := req.GetLen()
	group := req.GetSerfGroup()
	cacherootGroup := req.GetCacheRootGroup()
	ctx := stream.Context()
	s := t.store
	blockSize := t.config.blockSize
	serfGroup := t.config.serfGroup
	hasher := t.config.hasher

	clientAddr := "unknown"
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		clientAddr, _, _ = net.SplitHostPort(p.Addr.String())
	}

	defer func() {
		if rerr == nil {
			uploadCount.Inc()
			uploadBytes.Add(float64(blklen))
		} else {
			missCount.Inc()
			missBytes.Add(float64(blklen))
		}
	}()

	logger.Debugf("Cachefs transport server Load %v", req)
	if blklen > blockSize {
		return status.Errorf(codes.OutOfRange, "requested size(%d) greater than block size(%d)", blklen, blockSize)
	}
	if blksiz != blockSize {
		return status.Errorf(codes.InvalidArgument, "block size mismatch: %d != %d", blksiz, blockSize)
	}
	if group != serfGroup {
		code := codes.FailedPrecondition
		msg := fmt.Sprintf("serf group mismatch: expected \"%s\", got \"%s\", cache root group \"%s\"", serfGroup, group, cacherootGroup)

		if !t.config.cacherootServerEnabled() {
			return status.Error(code, msg)
		}

		// We started cache root server, this request may come from other
		// groups, so check cache root group as well, which should be set
		// as our serf group.
		if cacherootGroup != serfGroup {
			return status.Errorf(code, msg)
		}
	}

	if t.rdmaDisabled && !req.GetSupportsFallback() {
		return status.Error(codes.FailedPrecondition, "rdma initialization failed and client does not support tcp fallback")
	}
	serverFallback := t.rdmaDisabled && req.GetSupportsFallback()
	if serverFallback && t.fallbackService == nil {
		return status.Error(codes.FailedPrecondition, "rdma disabled and tcp fallback not enabled")
	}
	clientFallback := req.GetSupportsFallback() && req.GetUseFallback()
	primaryIsRdma := transfer.IsRdmaTransport(t.config.transport)
	if clientFallback && t.fallbackService == nil && primaryIsRdma {
		return status.Error(codes.FailedPrecondition, "client requires tcp fallback but tcp fallback not enabled")
	}

	page, err := s.doLoad(ctx, target, blknum, blklen, false, false)
	if err != nil {
		return status.Error(codes.NotFound, err.Error())
	}
	defer page.Release()

	// Make sure page has digest computed & set
	if s.checksumEnabled() {
		if _, err := page.GenDigest(s.config.Hasher); err != nil {
			return status.Error(codes.Unknown, err.Error())
		}
	}
	if page.Hasher != hasher {
		msg := fmt.Sprintf("data hasher %s is different with configured hasher %s", page.Hasher, hasher)
		logger.Warnf("Cachefs transport server Load %s block %d from %s: %s", target, blknum, clientAddr, msg)
		return status.Error(codes.Unknown, msg)
	}

	dataLen := len(page.Data)
	if int(blklen) != dataLen {
		logger.Warnf("Cachefs transport server Load %s block %d from %s data size mismatch (%d != %d)", target, blknum, clientAddr, blklen, dataLen)
		return status.Errorf(codes.Unknown, "data size mismatch (%d != %d)", blklen, dataLen)
	}

	useFallbackService := t.fallbackService != nil && (serverFallback || clientFallback)
	uploadService := t.service
	if useFallbackService {
		uploadService = t.fallbackService
	}

	// Register and prepare page to be uploaded via transfer service. The upload
	// outlives this handler and keeps reading page.Data, so the registration
	// holds a page reference of its own.
	cacheKey := CacheKey{Target: target, Blk: blknum}
	page.Acquire()
	id, err := uploadService.Register(cacheKey.String(), page.Data, page.Release)
	if err != nil {
		page.Release()
		logger.Warnf("Cachefs transport server Load %s block %d from %s: %s", target, blknum, clientAddr, err)
		return status.Error(codes.Unknown, err.Error())
	}
	reqID := uint64(id)

	// The data plane removes a claimed registration before it accesses page.Data,
	// so this only reclaims transfers that never started.
	defer uploadService.Unregister(transfer.RequestID(reqID))

	// Notify client that it's ready to download data
	downAddr, err := uploadService.SelectServerAddr()
	if err != nil {
		return status.Errorf(codes.Internal, "%v", err)
	}
	res := &pb.LoadResponse{
		Addr:  downAddr,
		ReqId: reqID,
		SvcId: uploadService.ServerID(),
	}
	if t.fallbackService != nil {
		fallbackAddr := downAddr
		if !useFallbackService {
			fallbackAddr, err = t.fallbackService.SelectServerAddr()
			if err != nil {
				return status.Errorf(codes.Internal, "%v", err)
			}
		}
		res.FallbackAddr = fallbackAddr
	} else if clientFallback {
		res.FallbackAddr = downAddr
	}
	if serverFallback {
		res.Addr = ""
	}
	if hasher != "" {
		res.Hash = encodeDigest(page.Digest, page.Hasher)
	}
	err = stream.Send(res)
	if err != nil {
		msg := fmt.Sprintf("send: %s", err)
		logger.Warnf("Cachefs transport server Load %s block %d from %s: %s", target, blknum, clientAddr, msg)
		return status.Error(codes.Unknown, msg)
	}

	// Wait for client to finish downloading data
	start := time.Now()
	_, err = stream.Recv()
	code := status.Code(err)
	if code == codes.DeadlineExceeded {
		uploadTimeout.Inc()
	}

	if err != nil {
		// Client should sent package and close the stream, so ignore Canceled error code
		if code != codes.Canceled {
			logger.Infof("Cachefs transport server Load %s block %d from %s: recv to wait: %s", target, blknum, clientAddr, err)
		}
		return err
	}
	logger.Debugf("Cachefs transport server Load %s block %d in %s", target, blknum, time.Since(start))
	return nil
}

// Handle ExchangeTracker requests from clients.
func (t *transport) ExchangeTracker(ctx context.Context, req *pb.ExchangeTrackerRequest) (*pb.ExchangeTrackerResponse, error) {
	clientAddr := "unknown"
	p, ok := peer.FromContext(ctx)
	if ok && p.Addr != nil {
		clientAddr, _, _ = net.SplitHostPort(p.Addr.String())
	}

	start := time.Now()
	group := req.GetSerfGroup()
	logger.Debugf("Cachefs transport server ExchangeTracker request from group %s", group)

	if group != t.config.serfGroup {
		return nil, fmt.Errorf("serf group mismatch: from \"%s\" to \"%s\"", group, t.config.serfGroup)
	}

	localMeta := t.getMetaDump()
	localData, err := json.Marshal(localMeta)
	if err != nil {
		logger.Warnf("ExchangeTracker server: failed to marshal local metaInfo: %s", err)
		localData, err = json.Marshal(nil)
	}

	// Merge incoming metaInfo from peer
	var targets int
	var blocks int
	remoteData := req.GetMetaInfo()
	remoteMeta := &metaInfo{}
	if len(remoteData) > 0 {
		err := json.Unmarshal(remoteData, remoteMeta)
		if err != nil {
			logger.Warnf("Failed to unmarshal remote metaInfo from %s: %s", clientAddr, err)
		} else {
			targets, blocks = t.store.rcache.reconcilePeer(remoteMeta)
		}
	}

	duration := time.Since(start)
	if t.serverStats != nil {
		t.serverStats.record(duration, targets, blocks, len(localData), len(remoteData))
	}
	logger.Debugf("ExchangeTracker server exchanged with %s (send %d, recv %d): merged %d targets %d blocks in %v",
		clientAddr, len(localData), len(remoteData), targets, blocks, duration)
	return &pb.ExchangeTrackerResponse{MetaInfo: localData}, nil
}
