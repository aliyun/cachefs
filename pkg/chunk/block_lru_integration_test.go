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

package chunk

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aliyun/cachefs/pkg/rpcsvc"
	"github.com/aliyun/cachefs/pkg/serf"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/stretchr/testify/assert"
)

type lruIntegrationNode struct {
	store    *store
	rpc      rpcsvc.RpcService
	serf     serf.SerfService
	addr     string
	serfAddr string
}

func findAvailableCachePort(t *testing.T) int {
	t.Helper()
	for {
		port := findAvailablePort()
		if port == 65535 {
			continue
		}
		listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port+1))
		if err == nil {
			listener.Close()
			return port
		}
	}
}

func startLRUIntegrationNode(t *testing.T, name, joinAddr string, lruSize, capacityBlocks int, blockSize uint32) *lruIntegrationNode {
	t.Helper()
	cachePort := findAvailableCachePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", cachePort)
	rpcService, err := rpcsvc.NewRpcService(&rpcsvc.RpcConfig{Addr: addr})
	if !assert.NoError(t, err) {
		return nil
	}

	serfPort := findAvailablePort()
	serfAddr := fmt.Sprintf("127.0.0.1:%d", serfPort)
	serfConfig := serf.DefaultSerfConfig()
	serfConfig.CacheAddr = addr
	serfConfig.CachePort = cachePort
	serfConfig.BindAddr = "127.0.0.1"
	serfConfig.BindPort = serfPort
	serfConfig.JoinAddr = joinAddr
	serfConfig.Group = "lru-integration"
	serfConfig.NodeName = name
	serfConfig.LeaveDelay = 10 * time.Millisecond
	serfService := serf.NewSerfService(serfConfig, nil)

	node := &lruIntegrationNode{rpc: rpcService, serf: serfService, addr: addr, serfAddr: serfAddr}
	t.Cleanup(func() {
		if node.store != nil {
			node.store.Stop()
		}
		node.rpc.Stop()
		node.serf.Stop()
	})

	config := &StoreConfig{
		BlockSize:      blockSize,
		CacheDir:       "memory",
		CacheSize:      int64(capacityBlocks) * int64(blockSize),
		BurstCacheSize: int64(capacityBlocks) * int64(blockSize),
		BlockLRUSize:   lruSize,
		CacheEviction:  HashCleanup,
		Transport:      "tcp",
		ThpPolicy:      "default",
		CacheAddr:      addr,
		CacheIP:        "127.0.0.1",
		CachePort:      cachePort,
		PeerTimeout:    2,
		ExchangeTime:   3600,
		GoGc:           utils.GoGcConfig{GCPercent: 100, GCPercentSet: true},
	}
	node.store = doNewStore(config, rpcService, nil, serfService, nil)
	rpcService.Serve()
	if !assert.NoError(t, serfService.Start()) {
		return nil
	}
	return node
}

func startLRUIntegrationCluster(t *testing.T, lruSize, capacityBlocks int, blockSize uint32) ([2]*lruIntegrationNode, bool) {
	t.Helper()
	var nodes [2]*lruIntegrationNode
	nodes[0] = startLRUIntegrationNode(t, "lru-node-0", "", lruSize, capacityBlocks, blockSize)
	if nodes[0] == nil {
		return nodes, false
	}
	nodes[1] = startLRUIntegrationNode(t, "lru-node-1", nodes[0].serfAddr, lruSize, capacityBlocks, blockSize)
	if nodes[1] == nil {
		return nodes, false
	}
	joined := assert.Eventually(t, func() bool {
		if nodes[0].serf.GetMemberCount() != 2 || nodes[1].serf.GetMemberCount() != 2 {
			return false
		}
		cacheKey := CacheKey{Target: "/lru-convergence", Blk: 0}
		key := cacheKey.String()
		_, addr0, err0 := nodes[0].serf.GetHashOwner(key)
		_, addr1, err1 := nodes[1].serf.GetHashOwner(key)
		return err0 == nil && err1 == nil && addr0 == addr1 &&
			(addr0 == nodes[0].addr || addr0 == nodes[1].addr)
	}, 10*time.Second, 20*time.Millisecond)
	return nodes, joined
}

func peerOwnedSequence(t *testing.T, reader, owner *lruIntegrationNode, target string, count int) ([]uint32, bool) {
	t.Helper()
	blocks := make([]uint32, 0, count)
	for blk := uint32(0); len(blocks) < count; blk++ {
		if !assert.Less(t, blk, uint32(10000)) {
			return nil, false
		}
		key := CacheKey{Target: target, Blk: blk}
		_, addr, err := reader.store.serf.GetHashOwner(key.String())
		if !assert.NoError(t, err) {
			return nil, false
		}
		if addr == owner.addr {
			blocks = append(blocks, blk)
		}
	}
	return blocks, true
}

func cacheAtOwner(owner *lruIntegrationNode, target string, blk, blockSize uint32, sourceLoads *atomic.Int64) {
	if owner.store.Check(target, blk) {
		return
	}
	page := NewPage(make([]byte, blockSize))
	owner.store.Cache(target, blk, page)
	page.Release()
	if sourceLoads != nil {
		sourceLoads.Add(1)
	}
}

func fillFromPeer(ctx context.Context, reader, owner *lruIntegrationNode, target string, blk, blockSize uint32, sourceLoads *atomic.Int64) error {
	cacheAtOwner(owner, target, blk, blockSize, sourceLoads)
	err := reader.store.doFill(ctx, target, blk, blockSize)
	if err == nil {
		reader.store.checkPendingCache(target, blk)
		return nil
	}
	cacheAtOwner(owner, target, blk, blockSize, sourceLoads)
	err = reader.store.doFill(ctx, target, blk, blockSize)
	if err == nil {
		reader.store.checkPendingCache(target, blk)
	}
	return err
}

func loadFromPeer(ctx context.Context, reader, owner *lruIntegrationNode, target string, blk, blockSize uint32, sourceLoads *atomic.Int64) error {
	page, err := reader.store.Load(ctx, target, uint64(1)<<60, blk)
	if errors.Is(err, ErrNotFound) {
		cacheAtOwner(owner, target, blk, blockSize, sourceLoads)
		page, err = reader.store.Load(ctx, target, uint64(1)<<60, blk)
	}
	if err != nil {
		return err
	}
	page.Release()
	return nil
}

func runSequentialPrefetch(ctx context.Context, reader, owner *lruIntegrationNode, target string, blocks []uint32, blockSize uint32, prefetch int, sourceLoads *atomic.Int64) error {
	for i := 0; i < prefetch; i++ {
		if err := fillFromPeer(ctx, reader, owner, target, blocks[i], blockSize, sourceLoads); err != nil {
			return fmt.Errorf("initial prefetch block %d: %w", blocks[i], err)
		}
	}
	for i, blk := range blocks {
		if i+prefetch < len(blocks) {
			next := blocks[i+prefetch]
			if err := fillFromPeer(ctx, reader, owner, target, next, blockSize, sourceLoads); err != nil {
				return fmt.Errorf("prefetch block %d: %w", next, err)
			}
		}
		if err := loadFromPeer(ctx, reader, owner, target, blk, blockSize, sourceLoads); err != nil {
			return fmt.Errorf("read block %d: %w", blk, err)
		}
	}
	return nil
}

func verifyPeerLoadDoesNotTouchLRU(t *testing.T, nodes [2]*lruIntegrationNode, blockSize uint32) bool {
	t.Helper()
	reader, owner := nodes[0], nodes[1]
	target := "/lru-peer-probe"
	blocks, ok := peerOwnedSequence(t, reader, owner, target, 1)
	if !ok {
		return false
	}
	cacheAtOwner(owner, target, blocks[0], blockSize, nil)
	defer func() {
		reader.store.bcache.removeByPrefix([]string{target}, nil)
		owner.store.bcache.removeByPrefix([]string{target}, nil)
	}()
	cache := owner.store.bcache.(*mcache)
	cache.Lock()
	before := lruTestKeys(cache.lru)
	cache.Unlock()

	if !assert.NoError(t, reader.store.doFill(context.Background(), target, blocks[0], blockSize)) {
		return false
	}
	reader.store.checkPendingCache(target, blocks[0])
	cache.Lock()
	after := lruTestKeys(cache.lru)
	cache.Unlock()
	unchanged := assert.Equal(t, before, after)
	return unchanged
}

type lruIntegrationResult struct {
	peerHits    uint64
	sourceLoads int64
}

func runLRUIntegrationWorkload(t *testing.T, lruSize int) (lruIntegrationResult, bool) {
	t.Helper()
	const (
		blockSize      = uint32(256)
		capacityBlocks = 8
		blocksPerRead  = 32
		prefetch       = 1
	)
	nodes, ok := startLRUIntegrationCluster(t, lruSize, capacityBlocks, blockSize)
	if !ok {
		return lruIntegrationResult{}, false
	}
	if lruSize > 0 && !verifyPeerLoadDoesNotTouchLRU(t, nodes, blockSize) {
		return lruIntegrationResult{}, false
	}
	targets := [2]string{"/lru-reader-0", "/lru-reader-1"}
	sequence0, ok := peerOwnedSequence(t, nodes[0], nodes[1], targets[0], blocksPerRead)
	if !ok {
		return lruIntegrationResult{}, false
	}
	sequence1, ok := peerOwnedSequence(t, nodes[1], nodes[0], targets[1], blocksPerRead)
	if !ok {
		return lruIntegrationResult{}, false
	}
	sequences := [2][]uint32{sequence0, sequence1}
	if !assert.Greater(t, 2*blocksPerRead, 2*capacityBlocks) {
		return lruIntegrationResult{}, false
	}

	var sourceLoads atomic.Int64
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := range nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			reader, owner := nodes[i], nodes[1-i]
			errs <- runSequentialPrefetch(context.Background(), reader, owner, targets[i], sequences[i], blockSize, prefetch, &sourceLoads)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	succeeded := true
	for err := range errs {
		if !assert.NoError(t, err) {
			succeeded = false
		}
	}
	if !succeeded {
		return lruIntegrationResult{}, false
	}
	return lruIntegrationResult{
		peerHits:    nodes[0].store.GetPeerLoadStats().Hits + nodes[1].store.GetPeerLoadStats().Hits,
		sourceLoads: sourceLoads.Load(),
	}, true
}

// TestMCacheBlockLRUMultiNodeSequentialPrefetch verifies fewer peer reloads
// under concurrent over-capacity reads and no peer-side LRU refresh.
func TestMCacheBlockLRUMultiNodeSequentialPrefetch(t *testing.T) {
	if testing.Short() {
		t.Skip("skip multi-node integration test in short mode")
	}
	oldMemfd := PageMemfd
	PageMemfd = false
	oldGCPercent := debug.SetGCPercent(100)
	t.Cleanup(func() {
		PageMemfd = oldMemfd
		debug.SetGCPercent(oldGCPercent)
	})

	var disabled, enabled lruIntegrationResult
	var disabledOK, enabledOK bool
	t.Run("disabled", func(t *testing.T) {
		disabled, disabledOK = runLRUIntegrationWorkload(t, 0)
	})
	t.Run("enabled", func(t *testing.T) {
		enabled, enabledOK = runLRUIntegrationWorkload(t, 4)
	})
	if !disabledOK || !enabledOK {
		return
	}
	t.Logf("peer hits without LRU: %d, with LRU: %d; source loads without LRU: %d, with LRU: %d",
		disabled.peerHits, enabled.peerHits, disabled.sourceLoads, enabled.sourceLoads)
	if !assert.Positive(t, disabled.peerHits) || !assert.Positive(t, enabled.peerHits) {
		return
	}
	assert.Less(t, enabled.peerHits, disabled.peerHits,
		"the LRU must keep prefetched non-hash-hit blocks resident until their sequential read")
}
