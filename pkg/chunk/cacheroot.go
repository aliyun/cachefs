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
	"errors"
	"fmt"
	"sync"
	"time"

	pb "github.com/aliyun/cachefs/pkg/rpc/cacheroot"
	"github.com/aliyun/cachefs/pkg/rpcsvc"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Use var to define timeouts, so unit test could re-define them
var (
	// 5s timeout of local cached P2P tracker info queried from cache root
	trackerInfoTimeout = time.Second * 5

	// 30s timeout of client that connects to cache root to query tracker info
	trackerClientTimeout = time.Second * 30
)

var (
	errTrackerNotFound = errors.New("tracker not found")
	errTrackerTimeout  = errors.New("tracker timed out")
)

type cacheRoot interface {
	// Stop cache root server
	stop() error

	// Query tracker info of given target and block from cache root
	query(ctx context.Context, target string, blk uint32) ([]string, string, error)
}

type trackerInfo struct {
	// List of hosts that contains given key
	nodes []string
	// Serf group of host
	group string
	// Create time of this tracker info
	ctime time.Time
}

type cacherootManager struct {
	addrs []string
	peer  *peerCache

	clientMu sync.Mutex
	clients  map[string]*cacheRootClient

	metaMu  sync.Mutex
	metaMap map[string]map[uint32]*trackerInfo

	cacherootTrackerQuery     prometheus.Counter
	cacherootTrackerQueryHit  prometheus.Counter
	cacherootTrackerQueryMiss prometheus.Counter
}

func (i *trackerInfo) String() string {
	return fmt.Sprintf("nodes: %v, serf group: %s", i.nodes, i.group)
}

func newCacheRoot(peer *peerCache, reg prometheus.Registerer) (cacheRoot, error) {
	cfg := peer.config
	cacheroot := &cacherootManager{
		addrs:   cfg.cacherootAddrs,
		peer:    peer,
		clients: make(map[string]*cacheRootClient),
		metaMap: make(map[string]map[uint32]*trackerInfo),
	}
	cacheroot.InitMetrics(reg)

	// Use cacheRoot as client only when there're cache root servers specified
	if cfg.cacherootClientEnabled() {
		logger.Infof("Remote cache root tracker servers: %v", cfg.cacherootAddrs)
		go cacheroot.gcClient()
		go cacheroot.gcTrackerInfo()
	}

	// Register on the shared cacheroot rpcsvc only when cache root server port is
	// specified. The rpcsvc itself is created and its lifecycle (Serve/Stop) is
	// managed at the cmd layer, shared with the CacheRootCluster service.
	if !cfg.cacherootServerEnabled() {
		return cacheroot, nil
	}
	if peer.store == nil || peer.store.cacherootRpc == nil {
		return nil, fmt.Errorf("cache root server enabled but no cacheroot rpcsvc provided (daemon mode doesn't support cacheroot yet)")
	}
	pb.RegisterCachefsCacheRootServer(peer.store.cacherootRpc.GetServer(), cacheroot)
	return cacheroot, nil
}

func (t *cacherootManager) InitMetrics(reg prometheus.Registerer) {
	t.cacherootTrackerQuery = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cacheroot_tracker_query",
		Help: "cacheroot server tracker info Query request count in total",
	})
	t.cacherootTrackerQueryHit = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cacheroot_tracker_hits",
		Help: "cacheroot server tracker info Query request hit count",
	})
	t.cacherootTrackerQueryMiss = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cacheroot_tracker_miss",
		Help: "cacheroot server tracker info Query request miss count",
	})
	if reg == nil {
		return
	}

	reg.MustRegister(t.cacherootTrackerQuery)
	reg.MustRegister(t.cacherootTrackerQueryHit)
	reg.MustRegister(t.cacherootTrackerQueryMiss)
}

func (t *cacherootManager) stop() error {
	// The shared cacheroot rpcsvc is stopped at the cmd layer; only client-side
	// resources are released here.
	t.clientMu.Lock()
	defer t.clientMu.Unlock()
	for _, client := range t.clients {
		_ = client.stop()
	}
	return nil
}

func (t *cacherootManager) query(ctx context.Context, target string, blk uint32) ([]string, string, error) {
	logger.Tracef("Cache root query %s block %d", target, blk)
	info, err := t.lookupInfo(target, blk)
	if err == nil {
		return info.nodes, info.group, nil
	}

	for _, addr := range t.addrs {
		client, err := t.getClient(addr)
		if err != nil {
			logger.Infof("Get cachefs cache root client to %s: %s", addr, err)
			continue
		}
		nctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		start := time.Now()
		info, err := client.query(nctx, target, blk)
		t.peer.store.cacherootQueryHist.Observe(time.Since(start).Seconds())
		if err != nil {
			if err != errTrackerNotFound && status.Code(err) != codes.Unavailable {
				logger.Infof("Cache root Query %s block %d from %s: %s", target, blk, addr, err)
			}
			continue
		}
		t.insertInfo(target, blk, info)
		return info.nodes, info.group, nil
	}
	return nil, "", errTrackerNotFound
}

// lookup local meta map
func (t *cacherootManager) lookupInfo(target string, blk uint32) (*trackerInfo, error) {
	t.metaMu.Lock()
	defer t.metaMu.Unlock()

	blkMap, ok := t.metaMap[target]
	if !ok {
		return nil, errTrackerNotFound
	}
	info, ok := blkMap[blk]
	if !ok {
		return nil, errTrackerNotFound
	}

	if info.ctime.Add(trackerInfoTimeout).Before(time.Now()) {
		delete(blkMap, blk)
		return nil, errTrackerTimeout
	}
	if len(info.nodes) == 0 {
		delete(blkMap, blk)
		return nil, errTrackerNotFound
	}
	return info, nil
}

// insert tracker info to local meta map
func (t *cacherootManager) insertInfo(target string, blk uint32, info *trackerInfo) {
	t.metaMu.Lock()
	defer t.metaMu.Unlock()

	blkMap, ok := t.metaMap[target]
	if !ok {
		blkMap = make(map[uint32]*trackerInfo)
		t.metaMap[target] = blkMap
	}
	blkMap[blk] = info
	info.ctime = time.Now()
	return
}

func (t *cacherootManager) getClient(addr string) (*cacheRootClient, error) {
	t.clientMu.Lock()
	defer t.clientMu.Unlock()

	var err error
	client, ok := t.clients[addr]
	if ok {
		client.touch()
		return client, nil
	}
	client, err = newCacheRootClient(addr, t.peer.config.blockSize)
	if err != nil {
		return nil, err
	}
	t.clients[addr] = client
	return client, nil
}

func (t *cacherootManager) gcTrackerInfo() {
	for {
		now := time.Now()
		t.metaMu.Lock()
		for target, blockMap := range t.metaMap {
			for blk, info := range blockMap {
				if info.ctime.Add(trackerInfoTimeout).Before(now) {
					delete(blockMap, blk)
					logger.Tracef("GC tracker info of target %s block %d", target, blk)
				}
			}
			if len(blockMap) == 0 {
				delete(t.metaMap, target)
			}
		}
		t.metaMu.Unlock()
		time.Sleep(time.Minute)
	}
}

func (t *cacherootManager) gcClient() {
	for {
		now := time.Now()
		t.clientMu.Lock()
		for addr, client := range t.clients {
			if client.getAtime().Add(trackerClientTimeout).Before(now) {
				if err := client.stop(); err != nil {
					logger.Warnf("Stop cache root client to %s: %s", addr, err)
				}
				delete(t.clients, addr)
				logger.Debugf("GC cache root client to %s", addr)
			}
		}
		t.clientMu.Unlock()
		time.Sleep(time.Minute)
	}
}

// gRPC client implementation
type cacheRootClient struct {
	blockSize  uint32
	grpcClient pb.CachefsCacheRootClient
	conn       *grpc.ClientConn
	atime      time.Time
}

func newCacheRootClient(addr string, blockSize uint32, opts ...grpc.DialOption) (*cacheRootClient, error) {
	conn, err := rpcsvc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}
	client := &cacheRootClient{
		blockSize:  blockSize,
		grpcClient: pb.NewCachefsCacheRootClient(conn),
		conn:       conn,
		atime:      time.Now(),
	}
	return client, nil
}

func (c *cacheRootClient) touch() {
	c.atime = time.Now()
}

func (c *cacheRootClient) getAtime() time.Time {
	return c.atime
}

func (c *cacheRootClient) stop() error {
	return c.conn.Close()
}

func (c *cacheRootClient) query(ctx context.Context, target string, blk uint32) (*trackerInfo, error) {
	req := &pb.QueryRequest{
		Target:    target,
		BlockNum:  blk,
		BlockSize: c.blockSize,
	}
	res, err := c.grpcClient.Query(ctx, req)
	if err != nil {
		return nil, err
	}
	nodes := res.GetNodes()
	if len(nodes) == 0 {
		return nil, errTrackerNotFound
	}
	group := res.GetSerfGroup()
	info := &trackerInfo{
		nodes: nodes,
		group: group,
		ctime: time.Now(),
	}
	logger.Debugf("Cache root Query target %s block %d: %s", target, blk, info)
	return info, nil
}

// gRPC server implementation
func (t *cacherootManager) Query(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	target := req.GetTarget()
	blk := req.GetBlockNum()
	blksiz := req.GetBlockSize()

	logger.Debugf("Cachefs cache root server Query %v", req)
	t.cacherootTrackerQuery.Inc()
	if blksiz != t.peer.config.blockSize {
		t.cacherootTrackerQueryMiss.Inc()
		return nil, status.Errorf(codes.InvalidArgument, "block size mismatch: %d != %d", blksiz, t.peer.config.blockSize)
	}
	nodes := t.peer.findCandidates(target, blk, 0, false)
	if len(nodes) == 0 {
		t.cacherootTrackerQueryMiss.Inc()
	} else {
		t.cacherootTrackerQueryHit.Inc()
	}
	return &pb.QueryResponse{
		Nodes:     nodes,
		SerfGroup: t.peer.config.serfGroup,
	}, nil
}
