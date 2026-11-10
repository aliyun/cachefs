/*
 * Copyright 2023 Alibaba Cloud, Inc. or its affiliates.
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
	"sync"
	"time"

	"github.com/aliyun/cachefs/pkg/chunk"
	"github.com/aliyun/cachefs/pkg/meta"
	"github.com/willf/bitset"
)

type prefetchReq struct {
	target    string
	srcTarget string
	length    uint64
	block     uint32
	cache     bool
	doneCh    chan struct{}
	ctx       meta.Context
}

type prefetchStatus struct {
	sync.Mutex
	caching *bitset.BitSet
	atime   time.Time
}

func (sts *prefetchStatus) check(blk uint32) bool {
	sts.Lock()
	bitIsSet := sts.caching.Test(uint(blk))
	sts.Unlock()
	return bitIsSet
}

func (sts *prefetchStatus) set(blk uint32) {
	sts.Lock()
	sts.atime = time.Now()
	sts.caching.Set(uint(blk))
	sts.Unlock()
}

func (sts *prefetchStatus) clear(blk uint32) {
	sts.Lock()
	sts.caching.Clear(uint(blk))
	sts.Unlock()
}

var prefetchReqPool *sync.Pool

func init() {
	prefetchReqPool = &sync.Pool{
		New: func() interface{} {
			return &prefetchReq{}
		},
	}
}

func (c *Cachefs) checkPrefetch(target string, blk uint32) bool {
	if c.cache.check(target, blk) {
		return false
	}
	c.prefetchMu.RLock()
	sts, ok := c.prefetches[target]
	c.prefetchMu.RUnlock()
	if ok {
		if sts.check(blk) {
			return false
		}
	}
	return true
}

func (c *Cachefs) doPrefetch(req *prefetchReq) {
	target := req.target
	srcTarget := req.srcTarget
	length := req.length
	blk := req.block
	ctx := req.ctx
	cache := req.cache

	defer func() {
		if req.doneCh != nil {
			req.doneCh <- struct{}{}
		}
		req.target = ""
		req.srcTarget = ""
		req.ctx = nil
		req.doneCh = nil
		prefetchReqPool.Put(req)
	}()

	// don't prefetch if file has been closed
	if ctx.Canceled() {
		return
	}

retry:
	c.prefetchMu.RLock()
	sts, ok := c.prefetches[target]
	c.prefetchMu.RUnlock()
	if ok {
		if sts.check(blk) || c.cache.check(target, blk) {
			return
		}
	} else {
		sts = &prefetchStatus{
			caching: bitset.New(uint(length/uint64(c.Conf.Store.BlockSize) + 1)),
			atime:   time.Now(),
		}
		c.prefetchMu.Lock()
		// check again, as we released the lock
		if s, ok := c.prefetches[target]; ok {
			if s.check(blk) || c.cache.check(target, blk) {
				c.prefetchMu.Unlock()
				return
			}
		} else {
			c.prefetches[target] = sts
		}
		c.prefetchMu.Unlock()
	}
	sts.set(blk)
	defer func() {
		sts.clear(blk)
	}()

	var reader chunk.ChunkReader
	var rerr error
	start := time.Now()
	// The singleflight mechanism for 'fill' interface cannot be coordinated with the
	// singleload in 'readFromStore' calls used by read/getFileChunk. While the redundant
	// readers returned by singleLoad have no overhead for memory backend, therefore
	// readFromStore can be used.
	// Retain the 'fill' interface only for file backend.
	if c.Conf.Store.IsMemoryBackend() {
		reader, rerr = c.cache.readFromStore(meta.Background, target, length, blk, 0, cache)
		if rerr == nil {
			reader.Close()
			return
		}
	} else {
		rerr = c.cache.fillStore(meta.Background, target, length, blk)
		if rerr == nil {
			return
		}
	}

	cached := c.checkPendingCache(target, blk)
	if cached {
		goto retry
	}

	sourceFile := c.source.newSourceFile(srcTarget, length)
	defer func() {
		sourceFile.release()
	}()
	reader, rerr = c.sourceReader.readFromSource(sourceFile, target, length, blk, cache)
	if rerr == nil {
		reader.Close()
		logger.Debugf("prefetched %s block %d from source in %s", target, blk, time.Since(start))
	} else {
		logger.Warnf("prefetch read from source %s block %d: %s", target, blk, rerr)
	}
}

func (c *Cachefs) handlePrefetch() {
	for {
		select {
		case <-c.stopCh:
			return
		case req := <-c.prefetchCh:
			c.doPrefetch(req)
		}
	}
}

func (c *Cachefs) prefetch() {
	var i uint32
	for i = 0; i < c.Conf.PrefetchWorkers; i++ {
		go c.handlePrefetch()
	}
	tick := time.NewTicker(20 * time.Second)
	defer tick.Stop()
	timeout := time.Duration(c.Conf.Store.PeerTimeout+5) * time.Second
	for {
		select {
		case <-c.stopCh:
			return
		case <-tick.C:
			c.prefetchMu.Lock()
			for target, sts := range c.prefetches {
				if sts.atime.Add(timeout).Before(time.Now()) {
					delete(c.prefetches, target)
					logger.Debugf("remove prefetch status for %s", target)
				}
			}
			c.prefetchMu.Unlock()
		}
	}
}
