/*
 * Copyright 2024 Alibaba Cloud, Inc. or its affiliates.
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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aliyun/cachefs/pkg/chunk"
	"github.com/aliyun/cachefs/pkg/meta"
	"github.com/aliyun/cachefs/pkg/metric"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/willf/bitset"
	"golang.org/x/sync/singleflight"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type cacheReader interface {
	readFromStore(ctx context.Context, target string, length uint64, blk, dataSize uint32, cache bool) (chunk.ChunkReader, error)
	fillStore(ctx meta.Context, target string, length uint64, blk uint32) error
	addToStore(ctx meta.Context, target string, length uint64, blk uint32, page *chunk.Page, cache bool) error
	check(target string, blk uint32) bool
	size() int64
	removeByPrefix(remove, keep []string) (int64, int64)
	dataReady()
	stop()
	isHashHit(target string, blk uint32) bool
	clearBurst() (int64, int64)
	setCacheSize(cacheSize, burstCacheSize int64) (int64, int64, int64, int64)
	getCacheSize() (cacheSize, burstCacheSize int64)
	getCounter(name string) (*metric.AtomicCounter, error)

	// share-lock (eviction gate) for zero-copy GetFileChunks mappings
	lockTarget(target string, blocks []uint32)
	unlockTarget(target string, blocks []uint32)

	// TODO: pin cache support in daemon mode
	setPinCache(prefixes []string)
	isPinned(target string) bool
	hasPins() bool

	// TODO: cache state support in daemon mode
	getCacheState() chunk.CacheState
	trackerSnapshot(prefixes []string, local bool) map[string]map[string]*bitset.BitSet

	// TODO: peer load stats support in daemon mode
	getPeerLoadStats() chunk.PeerLoadStats
}

func newCacheReader(fs *Cachefs, store chunk.Store, reg prometheus.Registerer) cacheReader {
	if fs.Conf.IsFuseLayerMode() {
		reader, err := newDaemonReader(fs, reg)
		if err != nil {
			logger.Fatalf("Failed to connect to cachefs daemon at %s: %s", fs.Conf.DaemonSocket, err)
		}
		return reader
	} else {
		if store == nil {
			logger.Fatal("Cachefs store is not initialized in non cachefs daemon mode")
		}
		return newStoreReader(fs, store, reg)
	}
}

type storeReader struct {
	sync.Mutex
	store   chunk.Store
	fs      *Cachefs
	blocks  map[chunk.CacheKey]*singleLoad
	timeout time.Duration
	name    string
	fillsf  singleflight.Group // single flight for fill cache operations
}

func (sr *storeReader) getStoreCache(ctx context.Context, target string, length uint64, blk uint32) (*singleLoad, error) {
	key := chunk.CacheKey{Target: target, Blk: blk}
	sl, created := getSingleLoad(key, sr.blocks, sr, sr.timeout, sr.name)
	if sl == nil {
		return nil, chunk.ErrNotFound
	}
	if !created {
		return sl, nil
	}

	// load cache from store
	reader, err := sr.store.ReadBlock(ctx, target, length, blk)
	if err != nil {
		if sl.setFail() {
			// someone else has set singleLoad ready
			return sl, nil
		} else {
			return nil, err
		}
	}

	sl.setReaderReady(reader)
	return sl, nil
}

func (sr *storeReader) readFromStore(ctx context.Context, target string, length uint64, blk, dataSize uint32, cache bool) (chunk.ChunkReader, error) {
	maxRetry := 5
	retry := 0

START:
	// read from store
	sl, err := sr.getStoreCache(ctx, target, length, blk)
	if err != nil {
		if !sr.fs.Conf.Meta.MetaConsumer() {
			return nil, err
		}
		sourcelessRetry.Inc()
		sourcelessRetryBytes.Add(float64(dataSize))

		// If we're in sourceless meta consumer mode, there's no source to read,
		// let's retry read from peer.
		if retry >= maxRetry {
			logger.Warnf("Load target %s block %d from peer failed all %d attempts",
				target, blk, retry)
			return nil, err
		}
		logger.Infof("Target %s block %d not found in sourceless meta consumer mode, retry %d",
			target, blk, retry)
		retry += 1
		time.Sleep(time.Second)
		goto START
	}
	return sl, nil
}

func (sr *storeReader) fillStore(ctx meta.Context, target string, length uint64, blk uint32) error {
	// fill in local store
	key := chunk.CacheKey{Target: target, Blk: blk}
	_, err, _ := sr.fillsf.Do(key.String(), func() (any, error) {
		err := sr.store.Fill(ctx, target, length, blk)
		return nil, err
	})
	return err
}

func (sr *storeReader) addToStore(ctx meta.Context, target string, length uint64, blk uint32, page *chunk.Page, cache bool) error {
	sr.store.Cache(target, blk, page)
	return nil
}

func (sr *storeReader) check(target string, blk uint32) bool {
	return sr.store.Check(target, blk)
}

func (sr *storeReader) size() int64 {
	return sr.store.Size()
}

func (sr *storeReader) removeByPrefix(remove, keep []string) (int64, int64) {
	return sr.store.RemoveByPrefix(remove, keep)
}

func (sr *storeReader) dataReady() {
	sr.store.DataReady()
}

func (sr *storeReader) stop() {}

func (sr *storeReader) isHashHit(target string, blk uint32) bool {
	return sr.store.IsHashHit(target, blk)
}

func (sr *storeReader) clearBurst() (int64, int64) {
	return sr.store.ClearBurst()
}

func (sr *storeReader) setCacheSize(cacheSize, burstCacheSize int64) (int64, int64, int64, int64) {
	return sr.store.SetCacheSize(cacheSize, burstCacheSize)
}

func (sr *storeReader) getCacheSize() (int64, int64) {
	return sr.store.GetCacheSize()
}

func (sr *storeReader) getCounter(name string) (*metric.AtomicCounter, error) {
	return sr.store.GetCounter(name)
}

func (sr *storeReader) setPinCache(prefixes []string) { sr.store.SetPinCache(prefixes) }
func (sr *storeReader) isPinned(target string) bool   { return sr.store.IsPinned(target) }
func (sr *storeReader) hasPins() bool                 { return sr.store.HasPins() }

func (sr *storeReader) lockTarget(target string, blocks []uint32) {
	sr.store.LockTarget(target, blocks)
}
func (sr *storeReader) unlockTarget(target string, blocks []uint32) {
	sr.store.UnlockTarget(target, blocks)
}

// Per-instance raw cache state aggregated from the local store (capacity/used, hashhit/resident
// blocks & bytes).
func (sr *storeReader) getCacheState() chunk.CacheState {
	return sr.store.GetCacheState()
}

// Peer-load counters from the local store, for the GetLoadStats API.
func (sr *storeReader) getPeerLoadStats() chunk.PeerLoadStats {
	return sr.store.GetPeerLoadStats()
}

// Local snapshot of the cluster tracker view, filtered by the given (path) prefixes; empty means
// all. The returned map is keyed by restored target (KeyPrefix/sourcePrefix stripped), then by node
// address, each value a bitset whose set bits mark blocks that node holds. Union the per-node
// bitsets for cluster presence.
func (sr *storeReader) trackerSnapshot(prefixes []string, local bool) map[string]map[string]*bitset.BitSet {
	return sr.store.TrackerSnapshot(prefixes, local)
}

func newStoreReader(fs *Cachefs, store chunk.Store, reg prometheus.Registerer) cacheReader {
	return &storeReader{
		fs:      fs,
		store:   store,
		blocks:  make(map[chunk.CacheKey]*singleLoad),
		timeout: time.Second * time.Duration(fs.Conf.Store.PeerTimeout),
		name:    "cache reader",
	}
}

type daemonReader struct {
	sync.Mutex
	fs      *Cachefs
	blocks  map[chunk.CacheKey]*singleLoad
	client  chunk.DaemonClient
	timeout time.Duration
	name    string
	fillsf  singleflight.Group // single flight for fill cache operations
}

func newDaemonReader(fs *Cachefs, reg prometheus.Registerer) (cacheReader, error) {
	var client chunk.DaemonClient
	var err error

	socket := fs.Conf.DaemonSocket
	blksiz := fs.Conf.Store.BlockSize
	reqTimeout := time.Second * time.Duration(fs.Conf.Store.PeerTimeout)
	client, err = chunk.NewDaemonClient(socket, blksiz, reqTimeout)
	if err != nil {
		return nil, err
	}

	return &daemonReader{
		fs:      fs,
		blocks:  make(map[chunk.CacheKey]*singleLoad),
		client:  client,
		timeout: reqTimeout,
		name:    "daemon reader",
	}, nil
}

func (dr *daemonReader) getDaemonCache(ctx context.Context, target string, length uint64, blk uint32) (*singleLoad, error) {
	key := chunk.CacheKey{Target: target, Blk: blk}
	blklen := uint32(utils.GetBlockSize(dr.fs.Conf.Store.BlockSize, length, blk))
	sl, created := getSingleLoad(key, dr.blocks, dr, dr.timeout, dr.name)
	if sl == nil {
		return nil, chunk.ErrNotFound
	}
	if !created {
		return sl, nil
	}

	// get block from daemon
	reader, err := dr.client.ReadBlock(ctx, target, blk, blklen)
	if err != nil {
		if sl.setFail() {
			// someone else has set singleLoad ready
			return sl, nil
		} else {
			return nil, err
		}
	}

	sl.setReaderReady(reader)
	return sl, nil
}

func (dr *daemonReader) readFromStore(ctx context.Context, target string, length uint64, blk, dataSize uint32, cache bool) (chunk.ChunkReader, error) {
	// read from daemon
	sl, err := dr.getDaemonCache(ctx, target, length, blk)
	if err != nil {
		if status.Code(err) != codes.NotFound && !strings.Contains(err.Error(), "not found") {
			logger.Warnf("Get block from daemon target %s block %d: %s", target, blk, err)
		}
		return nil, err
	}
	return sl, nil
}

func (dr *daemonReader) fillStore(ctx meta.Context, target string, length uint64, blk uint32) error {
	// fill in local store
	key := chunk.CacheKey{Target: target, Blk: blk}
	blklen := uint32(utils.GetBlockSize(dr.fs.Conf.Store.BlockSize, length, blk))
	_, err, _ := dr.fillsf.Do(key.String(), func() (any, error) {
		err := dr.client.FillBlock(ctx, target, blk, blklen)
		return nil, err
	})
	return err
}

func (dr *daemonReader) addToStore(ctx meta.Context, target string, length uint64, blk uint32, page *chunk.Page, cache bool) error {
	blklen := uint32(utils.GetBlockSize(dr.fs.Conf.Store.BlockSize, length, blk))
	err := dr.client.PutBlock(ctx, target, blk, blklen, page)
	if err != nil {
		if status.Code(err) != codes.NotFound && !strings.Contains(err.Error(), "not found") {
			logger.Warnf("PutBlock target %s block %d failed: %s", target, blk, err)
		}
		return err
	}
	return nil
}

// no cheap local presence probe across the daemon boundary; the daemon-side
// fill dedups anyway
func (dr *daemonReader) check(target string, blk uint32) bool {
	return false
}

// daemon client is not trusted to get info about current used cache size of daemon
func (dr *daemonReader) size() int64 {
	return 0
}

// TODO: implement it in daemon mode
func (dr *daemonReader) removeByPrefix(remove, keep []string) (int64, int64) {
	return 0, 0
}

// this is only used in sourceless mode, and daemon mode doesn't support sourceless
func (dr *daemonReader) dataReady() {
}

func (dr *daemonReader) stop() {
	dr.client.Stop()
}

func (dr *daemonReader) isHashHit(target string, blk uint32) bool {
	return true
}

func (dr *daemonReader) clearBurst() (int64, int64) {
	// TODO: implement it in daemon mode
	logger.Warnf("clearBurst is not supported in daemon mode, burst cache will not be evicted")
	return 0, 0
}

func (dr *daemonReader) setCacheSize(cacheSize, burstCacheSize int64) (int64, int64, int64, int64) {
	// TODO: implement it in daemon mode
	logger.Warnf("setCacheSize is not supported in daemon mode")
	return -1, -1, 0, 0
}

func (dr *daemonReader) getCacheSize() (int64, int64) {
	// In daemon mode, dynamic resize is not supported; return the initial config values.
	return atomic.LoadInt64(&dr.fs.Conf.Store.CacheSize), atomic.LoadInt64(&dr.fs.Conf.Store.BurstCacheSize)
}

// TODO: expose store counters to daemon mode via RPC
func (dr *daemonReader) getCounter(name string) (*metric.AtomicCounter, error) {
	return nil, fmt.Errorf("store counter %q not available in daemon mode", name)
}

// cache state / tracker snapshot are not available in daemon mode
func (dr *daemonReader) getCacheState() chunk.CacheState { return chunk.CacheState{} }
func (dr *daemonReader) trackerSnapshot(prefixes []string, local bool) map[string]map[string]*bitset.BitSet {
	return map[string]map[string]*bitset.BitSet{}
}

// TODO: implement it in daemon mode
func (dr *daemonReader) getPeerLoadStats() chunk.PeerLoadStats { return chunk.PeerLoadStats{} }

// TODO: implement pin cache protocol for daemon mode
func (dr *daemonReader) setPinCache(prefixes []string) {}
func (dr *daemonReader) isPinned(target string) bool   { return false }
func (dr *daemonReader) hasPins() bool                 { return false }

// zero-copy GetFileChunks (and its eviction gate) is unsupported in fuse-layer
// mode: the real bcache lives in the cachefsd daemon process
func (dr *daemonReader) lockTarget(target string, blocks []uint32)   {}
func (dr *daemonReader) unlockTarget(target string, blocks []uint32) {}
