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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aliyun/cachefs/pkg/digest"
	pb "github.com/aliyun/cachefs/pkg/rpc/cachefs"
	"github.com/aliyun/cachefs/pkg/transfer"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SupportedTransport returns the list of transports supported by the chunk
// layer.
func SupportedTransport() []string {
	return transfer.SupportedTransport()
}

// newTransferService is the factory function for creating transfer services.
// It can be replaced in tests to inject mock implementations.
var newTransferService = transfer.NewTransferService

type transportInterface interface {
	// Load block from remote
	load(ctx context.Context, addr, target string, blk, blklen uint32, page *Page, group string, cacheroot bool) error

	// RDMA peer group membership used by candidate ordering.
	updateRdmaPeer(addr, group string)
	isRdmaPeer(addr string) bool

	// ExchangeTracker exchanges metaMap with a remote peer: sends local metaMap
	// and receives remote metaMap, merging the result locally.
	exchangeTracker(ctx context.Context, addr string) error

	// Stop transport
	stop() error
}

type transport struct {
	config *remoteCacheConfig
	store  *store

	service         transfer.TransferService
	fallbackService transfer.TransferService
	rdmaDisabled    bool

	cacherootService transfer.TransferClient

	rdmaMu    sync.RWMutex
	rdmaPeers map[string]struct{}

	clientMu sync.Mutex
	clients  map[string]*transportClient

	clientStats *exchangeStats
	serverStats *exchangeStats
}

func initTransferService(tcfg *transfer.TransferConfig, reg prometheus.Registerer, needStart bool) (transfer.TransferService, error) {
	svc, err := newTransferService(tcfg, reg)
	if err != nil {
		return nil, fmt.Errorf("New %s transfer service failed: %s", tcfg.Transport, err)
	}
	if needStart {
		if err := svc.StartServer(); err != nil {
			_ = svc.StopServer()
			return nil, fmt.Errorf("Start %s transfer service: %s", tcfg.Transport, err)
		}
	}
	return svc, nil
}

func newTransport(cfg *remoteCacheConfig, store *store, reg prometheus.Registerer) (xprt transportInterface, rerr error) {
	trans := &transport{
		config:      cfg,
		store:       store,
		clients:     make(map[string]*transportClient),
		clientStats: newExchangeStats(statsFlushCount, statsFlushInterval),
		serverStats: newExchangeStats(statsFlushCount, statsFlushInterval),
	}

	tcfg := &transfer.TransferConfig{
		Transport:     cfg.transport,
		TransportNics: cfg.transportNics,
		TransportIP:   cfg.localIP,
		BasePort:      cfg.localPort + 1,
		UploadLimit:   cfg.uploadLimit,
		DownloadLimit: cfg.downloadLimit,
		TotalLimit:    cfg.totalLimit,
		SingleLimit:   cfg.singleLimit,
		PeerTimeout:   cfg.peerTimeout,
		RdmaBufSize:   cfg.rdmaBufSize,
		RdmaBufCount:  cfg.rdmaBufCount,
		RdmaMaxMem:    cfg.rdmaMaxMem,
	}

	transferSvc, err := initTransferService(tcfg, reg, true)
	if err != nil && cfg.rdmaFallbackTcp && tcfg.RdmaEnabled() {
		logger.Infof("RDMA initialization failed: %s, fallback transport to TCP", err)
		tcfg.Transport = transfer.TCPTransport
		tcfg.TransportNics = []string{}
		trans.rdmaDisabled = true
		transferSvc, err = initTransferService(tcfg, reg, true)
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		if rerr != nil {
			if trans.fallbackService != nil && trans.fallbackService != transferSvc {
				_ = trans.fallbackService.StopServer()
			}
			_ = transferSvc.StopServer()
		}
	}()
	trans.service = transferSvc
	if trans.rdmaDisabled {
		trans.fallbackService = transferSvc
	} else if cfg.rdmaFallbackTcp && tcfg.RdmaEnabled() {
		fallbackCfg := tcfg.DeepCopy()
		fallbackCfg.Transport = transfer.TCPTransport
		fallbackCfg.TransportNics = nil
		fallbackCfg.BasePort += len(tcfg.TransportNics)
		fallbackSvc, err := initTransferService(fallbackCfg, nil, true)
		if err != nil {
			return nil, fmt.Errorf("New TCP fallback transfer service failed: %s", err)
		}
		trans.fallbackService = fallbackSvc
		logger.Infof("TCP fallback transfer service on %v", fallbackSvc.ServerAddrs())
	}

	pb.RegisterCachefsCacheServer(store.rpc.GetServer(), trans)
	go trans.gcClient()

	trans.clientStats.startPrinter(func(elapsed time.Duration, s *exchangeSummary) {
		logFunc := logger.Debugf
		if elapsed < statsFlushInterval*2 {
			// On startup we send & handle exchange requests with all joined members
			// within a short time, and we want to make sure the exchanges are doing
			// correctly, so use Info level log.
			logFunc = logger.Infof
		}
		logFunc("Exchanged P2P tracker with %d peers in last %s, avg stats: files %.1f blocks %.1f, send %.0f recv %.0f, time %s",
			s.count, elapsed.Truncate(time.Millisecond), s.avgTargets, s.avgBlocks,
			s.avgSendBytes, s.avgRecvBytes, s.avgDuration)
	})
	trans.serverStats.startPrinter(func(elapsed time.Duration, s *exchangeSummary) {
		logFunc := logger.Debugf
		if elapsed < statsFlushInterval*2 {
			logFunc = logger.Infof
		}
		logFunc("ExchangeTracker server served %d peers in last %s, avg stats: files %.1f blocks %.1f, send %.0f recv %.0f, time %s",
			s.count, elapsed.Truncate(time.Millisecond), s.avgTargets, s.avgBlocks,
			s.avgSendBytes, s.avgRecvBytes, s.avgDuration)
	})

	// Just return if no cacheroot tracker addrs specified
	if len(cfg.cacherootAddrs) == 0 {
		return trans, nil
	}

	// Setup transfer service used to load data from cacheroot
	if cfg.cacherootXprt == "" && len(cfg.cacherootNics) == 0 {
		// cacheroot transport and nics are not specified, use the same service
		trans.cacherootService = transferSvc
		return trans, nil
	}

	// Create new transfer service client with cacheroot transport and NICs,
	// i.e. no need to StartServer()
	crtCfg := tcfg.DeepCopy()
	crtCfg.Transport = cfg.cacherootXprt
	crtCfg.TransportNics = cfg.cacherootNics
	trans.cacherootService, err = initTransferService(crtCfg, nil, false)
	if err != nil {
		return nil, fmt.Errorf("New %s cacheroot transfer service client failed: %s", cfg.cacherootXprt, err)
	}
	return trans, nil
}

func (t *transport) getClient(addr string) (*transportClient, error) {
	t.clientMu.Lock()
	defer t.clientMu.Unlock()

	client, ok := t.clients[addr]
	if ok {
		client.touch()
		return client, nil
	}
	client, err := newTransportClient(addr, t)
	if err != nil {
		return nil, err
	}
	t.clients[addr] = client
	return client, nil
}

func (t *transport) servicesFor(cacheroot bool) (transfer.TransferClient, transfer.TransferClient, error) {
	if cacheroot {
		if t.cacherootService == nil {
			return nil, nil, errors.New("Cacheroot transfer service client is not initialized")
		}
		return t.cacherootService, nil, nil
	}
	return t.service, t.fallbackService, nil
}

// updateRdmaPeer does nothing when the local RDMA group is empty. Otherwise,
// addr is added when group matches, or removed when it is different or empty.
func (t *transport) updateRdmaPeer(addr, group string) {
	if t.config.rdmaGroup == "" {
		return
	}
	t.rdmaMu.Lock()
	defer t.rdmaMu.Unlock()
	if group == t.config.rdmaGroup {
		if t.rdmaPeers == nil {
			t.rdmaPeers = make(map[string]struct{})
		}
		t.rdmaPeers[addr] = struct{}{}
	} else {
		delete(t.rdmaPeers, addr)
	}
}

// isRdmaPeer returns true for every peer when group routing is disabled.
func (t *transport) isRdmaPeer(addr string) bool {
	if t.config.rdmaGroup == "" {
		return true
	}
	t.rdmaMu.RLock()
	defer t.rdmaMu.RUnlock()
	_, ok := t.rdmaPeers[addr]
	return ok
}

func (t *transport) useFallback(addr string, cacheroot bool) bool {
	if cacheroot {
		return false
	}
	return t.rdmaDisabled || !t.isRdmaPeer(addr)
}

func (t *transport) gcClient() {
	for {
		now := time.Now()
		t.clientMu.Lock()
		for addr, client := range t.clients {
			if client.getAtime().Add(t.config.peerTimeout * 2).Before(now) {
				if err := client.stop(); err != nil {
					logger.Warnf("Stop transport client to %s: %s", addr, err)
				}
				delete(t.clients, addr)
				logger.Debugf("GC connection to %s", addr)
			}
		}
		t.clientMu.Unlock()
		time.Sleep(time.Minute)
	}
}

func (t *transport) stop() error {
	t.clientMu.Lock()
	for _, client := range t.clients {
		_ = client.stop()
	}
	t.clientMu.Unlock()
	if t.fallbackService != nil && t.fallbackService != t.service {
		_ = t.fallbackService.StopServer()
	}
	_ = t.service.StopServer()
	if t.clientStats != nil {
		t.clientStats.stop()
	}
	if t.serverStats != nil {
		t.serverStats.stop()
	}
	return nil
}

func (t *transport) load(ctx context.Context, addr, target string, blk, blklen uint32, page *Page, group string, cacheroot bool) (rerr error) {
	logger.Tracef("Transport load %s block %d try candidate %s", target, blk, addr)
	client, err := t.getClient(addr)
	if err != nil {
		logger.Infof("Get cachefs transport client to %s: %s", addr, err)
		return err
	}
	nctx, cancel := context.WithTimeout(ctx, t.config.peerTimeout)
	defer cancel()

	start := time.Now()
	err = client.load(nctx, target, blk, page, group, cacheroot, t.useFallback(addr, cacheroot))
	if err != nil {
		code := status.Code(err)
		if code == codes.FailedPrecondition {
			logger.Warnf("Peer Load %s block %d from %s: %s", target, blk, addr, err)
		} else if code != codes.NotFound && code != codes.Unavailable {
			logger.Infof("Failed to peer load %s block %d from %s: %s, time %s",
				target, blk, addr, err, time.Since(start))
		}
		return err
	}

	// Check digest if there's any
	if t.config.checksumEnabled() {
		if err := digest.CheckDigest(t.config.hasher, page.Digest, page.Data); err != nil {
			t.store.checksumErr.Inc()
			logger.Warnf("Peer Load %s block %d from %s checksum error: %s", target, blk, addr, err)
			return err
		}
	}
	logger.Debugf("Success to peer load %s block %d from %s in %s", target, blk, addr, time.Since(start))
	return nil
}

func (t *transport) getMetaDump() *metaInfo {
	info := t.store.bcache.dump()
	if info == nil {
		return nil
	}
	info.SrcAddr = t.config.localAddr
	info.BlockSize = t.config.blockSize
	return info
}

func (t *transport) exchangeTracker(ctx context.Context, addr string) error {
	client, err := t.getClient(addr)
	if err != nil {
		return err
	}

	start := time.Now()
	localMeta := t.getMetaDump()
	localData, err := json.Marshal(localMeta)
	if err != nil {
		logger.Warnf("exchangeTracker: failed to marshal local metaInfo: %s", err)
		localData, err = json.Marshal(nil)
	}

	remoteData, err := client.exchangeTracker(ctx, localData)
	if err != nil {
		return err
	}

	remoteMeta := &metaInfo{}
	err = json.Unmarshal(remoteData, remoteMeta)
	if err != nil {
		logger.Warnf("Failed to unmarshal remote metaInfo from %s: %s", addr, err)
		return err
	}

	targets, blocks := t.store.rcache.reconcilePeer(remoteMeta)
	duration := time.Since(start)
	if t.clientStats != nil {
		t.clientStats.record(duration, targets, blocks, len(localData), len(remoteData))
	}
	logger.Debugf("Exchanged P2P tracker with %s (send %d, recv %d): merged %d targets %d blocks in %v",
		addr, len(localData), len(remoteData), targets, blocks, duration)
	return nil
}
