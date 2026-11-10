/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
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
package chunk

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aliyun/cachefs/pkg/metric"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/willf/bitset"
	"golang.org/x/sys/unix"
)

const (
	evictionCleanup        = "cleanup"
	evictionExpire         = "expire"
	evictionRemoveByPrefix = "removeByPrefix"
	evictionClearBurst     = "clearBurst"
)

type pendingPage struct {
	created time.Time
	page    *Page
	size    uint32
}

type memItem struct {
	atime time.Time
	page  *Page
}

type mcache struct {
	sync.Mutex
	store       *store
	capacity    int64
	burstCap    int64 // burst cache size upper limit (BurstCacheSize), must be >= capacity
	used        int64
	pages       map[CacheKey]memItem
	lru         *blockLRU // recent-add/load blocks protected from normal capacity eviction
	cacheExpire time.Duration

	// Protected block reference counts by target and block index. Protected blocks
	// are not evicted under cache pressure or expiration (RemoveByPrefix is an
	// exception). A block remains protected until all of its references are released.
	lockedBlocks map[string]map[int]int

	targetMu sync.Mutex
	targets  map[string]*targetItem

	commitMu sync.Mutex
	toCommit map[CacheKey]*pendingPage

	cacheWrites                 prometheus.Counter
	cacheWriteBytes             prometheus.Counter
	cacheEvicts                 prometheus.Counter
	cacheEvictsHashHit          *metric.AtomicCounter
	cacheEvictsHashMiss         prometheus.Counter
	cacheCleanup                *metric.AtomicCounter
	cacheCleanupHashHit         prometheus.Counter
	cacheCleanupHashMiss        prometheus.Counter
	cacheExpired                prometheus.Counter
	cacheExpiredHashHit         prometheus.Counter
	cacheExpiredHashMiss        prometheus.Counter
	cacheRemoveByPrefix         prometheus.Counter
	cacheRemoveByPrefixHashHit  prometheus.Counter
	cacheRemoveByPrefixHashMiss prometheus.Counter
	burstEvicts                 prometheus.Counter
	burstEvictBytes             prometheus.Counter
	burstBytes                  prometheus.GaugeFunc
	pinEvicts                   *metric.AtomicCounter
	pinEvictsHashHit            *metric.AtomicCounter
	pinEvictBytes               prometheus.Counter
	evictsSkipLocked            prometheus.Counter
	evictsSkipPinned            prometheus.Counter
	evictsLocked                prometheus.Counter
	lruBlocks                   prometheus.GaugeFunc
	lruBytes                    prometheus.GaugeFunc
	lruFallbackEvicts           prometheus.Counter
}

type memEvictionContext struct {
	age        time.Duration
	delTargets map[string]*targetItem
	freed      int64
	prefix     string
	todel      map[CacheKey]*Page
}

func newMemEvictionContext() *memEvictionContext {
	return &memEvictionContext{
		delTargets: make(map[string]*targetItem),
		todel:      make(map[CacheKey]*Page),
	}
}

// shouldEvictByPrefix returns whether target should be evicted and the
// matched remove prefix. The prefix is empty for keep-mode evictions and
// non-evicted targets.
func shouldEvictByPrefix(target string, remove, keep []string) (bool, string) {
	if len(remove) > 0 {
		for _, prefix := range remove {
			if strings.HasPrefix(target, prefix) {
				return true, prefix
			}
		}
		return false, ""
	}
	if len(keep) > 0 {
		for _, prefix := range keep {
			if strings.HasPrefix(target, prefix) {
				return false, ""
			}
		}
		return true, ""
	}
	return false, ""
}

func newMemCache(config *StoreConfig, store *store, reg prometheus.Registerer) *mcache {
	c := &mcache{
		store:    store,
		capacity: config.CacheSize,
		// initBurstSizes has already settled this, but floor it anyway: a burst cap
		// below capacity would make every insert evict down to it.
		burstCap:     max(config.BurstCacheSize, config.CacheSize),
		pages:        make(map[CacheKey]memItem),
		lru:          newBlockLRU(config.BlockLRUSize),
		lockedBlocks: make(map[string]map[int]int),
		targets:      make(map[string]*targetItem),
		toCommit:     make(map[CacheKey]*pendingPage),
		cacheExpire:  config.CacheExpire,
	}
	c.InitMetrics(reg)
	runtime.SetFinalizer(c, func(c *mcache) {
		for _, p := range c.pages {
			p.page.Release()
		}
		c.pages = nil
	})
	if c.cacheExpire > 0 {
		go c.cleanupExpire()
	}
	go c.gcCommitPages()
	go c.gcCommitTargets()
	logger.Infof("Memory cache: capacity %d MB, burst capacity %d MB", c.capacity>>20, c.burstCap>>20)
	return c
}

func (c *mcache) InitMetrics(reg prometheus.Registerer) {
	c.cacheWrites = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_writes",
		Help: "written cached block",
	})
	c.cacheEvicts = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts",
		Help: "evicted cache blocks",
	})
	c.cacheWriteBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_write_bytes",
		Help: "write bytes of cached block",
	})
	c.cacheEvictsHashHit = c.store.counterReg.NewCounter(prometheus.CounterOpts{
		Name: CounterEvictsHashHit,
		Help: "total evicted blocks that are hash hits",
	})
	c.cacheEvictsHashMiss = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_hashmiss",
		Help: "total evicted blocks that are hash miss",
	})
	c.cacheCleanup = c.store.counterReg.NewCounter(prometheus.CounterOpts{
		Name: CounterEvictsCleanup,
		Help: "evicted blocks by cleanup",
	})
	c.cacheCleanupHashHit = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_cleanup_hashhit",
		Help: "evicted blocks by cleanup that are hash hits",
	})
	c.cacheCleanupHashMiss = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_cleanup_hashmiss",
		Help: "evicted blocks by cleanup that are hash miss",
	})
	c.cacheExpired = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_expire",
		Help: "evicted blocks by expire",
	})
	c.cacheExpiredHashHit = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_expire_hashhit",
		Help: "evicted blocks by expire that are hash hits",
	})
	c.cacheExpiredHashMiss = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_expire_hashmiss",
		Help: "evicted blocks by expire that are hash miss",
	})
	c.cacheRemoveByPrefix = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_removebyprefix",
		Help: "evicted blocks by removebyprefix",
	})
	c.cacheRemoveByPrefixHashHit = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_removebyprefix_hashhit",
		Help: "evicted blocks by removebyprefix that are hash hits",
	})
	c.cacheRemoveByPrefixHashMiss = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_removebyprefix_hashmiss",
		Help: "evicted blocks by removebyprefix that are hash miss",
	})
	c.burstEvicts = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_burst_evicts",
		Help: "total evicted burst blocks",
	})
	c.burstEvictBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_burst_evict_bytes",
		Help: "total bytes of evicted burst blocks",
	})
	c.pinEvicts = c.store.counterReg.NewCounter(prometheus.CounterOpts{
		Name: CounterEvictsPinned,
		Help: "total evicted pinned blocks",
	})
	c.pinEvictsHashHit = c.store.counterReg.NewCounter(prometheus.CounterOpts{
		Name: CounterEvictsPinnedHashHit,
		Help: "total evicted pinned blocks that are hash hits",
	})
	c.pinEvictBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_pinned_bytes",
		Help: "total bytes of evicted pinned blocks",
	})
	c.evictsSkipLocked = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_skip_locked",
		Help: "eviction candidates skipped because their block is share-locked",
	})
	c.evictsSkipPinned = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_skip_pinned",
		Help: "eviction candidates skipped because their block is pinned",
	})
	c.evictsLocked = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_locked",
		Help: "share-locked blocks deleted by explicit removal",
	})
	c.lruFallbackEvicts = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_lru_fallback",
		Help: "blocks evicted from the recent-add/load LRU after regular cleanup rounds",
	})
	c.burstBytes = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "blockcache_burst_bytes",
		Help: "current burst cache usage in bytes (used - capacity when positive)",
	}, func() float64 {
		c.Lock()
		defer c.Unlock()
		burst := c.used - c.capacity
		if burst < 0 {
			burst = 0
		}
		return float64(burst)
	})
	c.lruBlocks = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "blockcache_lru_blocks",
		Help: "current blocks protected by the recent-add/load LRU",
	}, func() float64 {
		c.Lock()
		defer c.Unlock()
		return float64(c.lru.len())
	})
	c.lruBytes = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "blockcache_lru_bytes",
		Help: "current bytes protected by the recent-add/load LRU",
	}, func() float64 {
		c.Lock()
		defer c.Unlock()
		// Scan the typically small window.
		// Maintain a separate byte total if the block limit grows significantly.
		var size int64
		c.lru.forEach(func(key CacheKey) bool {
			size += int64(cap(c.pages[key].page.Data))
			return true
		})
		return float64(size)
	})
	if reg == nil {
		return
	}

	reg.MustRegister(c.cacheWrites)
	reg.MustRegister(c.cacheWriteBytes)
	reg.MustRegister(c.cacheEvicts)
	reg.MustRegister(c.cacheEvictsHashHit)
	reg.MustRegister(c.cacheEvictsHashMiss)
	reg.MustRegister(c.cacheCleanup)
	reg.MustRegister(c.cacheCleanupHashHit)
	reg.MustRegister(c.cacheCleanupHashMiss)
	reg.MustRegister(c.cacheExpired)
	reg.MustRegister(c.cacheExpiredHashHit)
	reg.MustRegister(c.cacheExpiredHashMiss)
	reg.MustRegister(c.cacheRemoveByPrefix)
	reg.MustRegister(c.cacheRemoveByPrefixHashHit)
	reg.MustRegister(c.cacheRemoveByPrefixHashMiss)
	reg.MustRegister(c.burstEvicts)
	reg.MustRegister(c.burstEvictBytes)
	reg.MustRegister(c.burstBytes)
	reg.MustRegister(c.pinEvicts)
	reg.MustRegister(c.pinEvictsHashHit)
	reg.MustRegister(c.pinEvictBytes)
	reg.MustRegister(c.evictsSkipLocked)
	reg.MustRegister(c.evictsSkipPinned)
	reg.MustRegister(c.evictsLocked)
	reg.MustRegister(c.lruBlocks)
	reg.MustRegister(c.lruBytes)
	reg.MustRegister(c.lruFallbackEvicts)
}

func (c *mcache) stats() (int64, int64) {
	c.Lock()
	defer c.Unlock()
	return int64(len(c.pages)), c.used
}

// Aggregate the raw cache state of this backend: capacity and used bytes, plus
// cached blocks/bytes and, among them, the hash-hit blocks/bytes (keys whose
// content is verified present via the store hash ring).
func (c *mcache) getCacheState() CacheState {
	// Snapshot the resident pages under the lock, then compute hashHit/isPinned
	// outside it. The per-entry work (key formatting, hash-ring lookup, pinCache
	// prefix scan) dominates the scan cost, so keeping it out of the lock cuts the
	// lock hold time and avoids blocking cache IO.
	type stateEntry struct {
		key  CacheKey
		size int64
	}
	c.Lock()
	st := CacheState{Capacity: c.capacity, Used: c.used}
	entries := make([]stateEntry, 0, len(c.pages))
	for k, it := range c.pages {
		entries = append(entries, stateEntry{k, int64(len(it.page.Data))})
	}
	c.Unlock()

	for _, e := range entries {
		st.CachedBlocks++
		st.CachedBytes += e.size
		if c.store.hashHit(e.key.String()) {
			st.HashHitBlocks++
			st.HashHitBytes += e.size
		}
		if c.store.pinCache.isPinned(e.key.Target) {
			st.PinnedBlocks++
			st.PinnedBytes += e.size
		}
	}
	return st
}

func (c *mcache) gcCommitPages() {
	for {
		time.Sleep(time.Minute)
		twoMinAgo := time.Now().Add(-time.Minute * 2)
		toDelete := make(map[CacheKey]*pendingPage)
		c.commitMu.Lock()
		for key, pp := range c.toCommit {
			if pp.created.Before(twoMinAgo) {
				delete(c.toCommit, key)
				toDelete[key] = pp
			}
		}
		c.commitMu.Unlock()
		if len(toDelete) > 0 {
			for key, pp := range toDelete {
				logger.Infof("Discard stale pending cache page %s", key.String())
				pp.page.Release()
			}
		}
	}
}

func (c *mcache) gcCommitTargets() {
	// Don't start at the same time as gcCommitPages()
	time.Sleep(time.Second * 5)
	for {
		time.Sleep(time.Minute)
		twoMinAgo := time.Now().Add(-time.Minute * 2)
		dels := []string{}
		c.targetMu.Lock()
		for target, ti := range c.targets {
			if ti.atime.Before(twoMinAgo) && !ti.complete {
				delete(c.targets, target)
				dels = append(dels, target)
			}
		}
		c.targetMu.Unlock()
		if len(dels) > 0 {
			for _, target := range dels {
				logger.Infof("Discard stale pending target %s", target)
			}
		}
	}
}

func (c *mcache) cache(target string, blk uint32, p *Page) {
	if c.burstCap == 0 {
		return
	}
	key := CacheKey{target, blk}
	c.Lock()
	defer c.Unlock()
	if _, ok := c.pages[key]; ok {
		return
	}
	if c.store.checksumEnabled() && (p.Digest == "" || p.Hasher == "") {
		start := time.Now()
		// Compute digest without lock
		c.Unlock()
		digest, err := p.GenDigest(c.store.config.Hasher)
		if err != nil {
			logger.Warnf("Cache %s block %d: %s", target, blk, err)
			c.Lock()
			return
		}
		logger.Debugf("Compute checksum %s block %d in %s: %s:%s", target, blk, time.Since(start), p.Hasher, digest)
		c.Lock()

		// Check again, as we released the lock
		if _, ok := c.pages[key]; ok {
			return
		}
	}
	size := int64(cap(p.Data))
	c.cacheWrites.Add(1)
	c.cacheWriteBytes.Add(float64(size))
	p.Acquire()
	now := time.Now()
	item := memItem{atime: now, page: p}
	c.pages[key] = item
	c.used += size
	c.lru.touch(key)
	if c.used > c.burstCap {
		skip := make(map[CacheKey]struct{}, 1)
		skip[key] = struct{}{}
		// Don't cleanup just added cache
		c.cleanup(skip)
	}
	c.store.rcache.announce(key.Target, 0, key.Blk)
}

func (c *mcache) removeByPrefix(remove, keep []string) (int64, int64) {
	ectx := newMemEvictionContext()
	c.Lock()
	defer c.Unlock()
	for k, mi := range c.pages {
		evict, prefix := shouldEvictByPrefix(k.Target, remove, keep)
		if evict {
			ectx.prefix = prefix
			c.evictCacheItem(ectx, k, mi, evictionRemoveByPrefix)
		}
	}
	c.Unlock()
	c.revokeDel(ectx)
	c.Lock()
	return int64(len(ectx.todel)), ectx.freed
}

func (c *mcache) load(target string, blk uint32, touchLRU bool) (*Page, ChunkReader, error) {
	key := CacheKey{target, blk}
	c.Lock()
	defer c.Unlock()
	if item, ok := c.pages[key]; ok {
		c.pages[key] = memItem{atime: time.Now(), page: item.page}
		if touchLRU {
			c.lru.touch(key)
		}
		item.page.Acquire()
		return item.page, nil, nil
	}
	return nil, nil, ErrNotFound
}

func (c *mcache) check(target string, blk uint32) bool {
	key := CacheKey{target, blk}
	c.Lock()
	defer c.Unlock()
	if item, ok := c.pages[key]; ok {
		c.pages[key] = memItem{atime: time.Now(), page: item.page}
		return true
	}
	return false
}

func (c *mcache) prepareFile(target string, length uint64) error {
	if length > uint64(c.burstCap) {
		return ErrTooLarge
	}
	c.targetMu.Lock()
	defer c.targetMu.Unlock()
	if ti, ok := c.targets[target]; !ok {
		c.targets[target] = &targetItem{time.Now(), length, false}
		return nil
	} else {
		ti.atime = time.Now()
		return ErrExists
	}
}

func (c *mcache) commitFile(target string) error {
	c.targetMu.Lock()
	defer c.targetMu.Unlock()
	if ti, ok := c.targets[target]; ok {
		if !ti.complete {
			ti.complete = true
			c.store.rcache.announce(target, ti.length, 0)
		}
		return nil
	} else {
		return ErrNotFound
	}
}

func (c *mcache) getBlockFile(target string, blk uint32) (*os.File, error) {
	page, _, err := c.load(target, blk, true)
	if err != nil {
		return nil, err
	}
	defer page.Release()
	if page.Memfd == nil {
		// page not memfd backed
		return nil, ErrNotFound
	}

	// We hand this fd to another process, which is out of our page lifecycle
	// control, so export it and use the exported fd. cachefsd gets no release
	// signal back, so the export is never accounted as gone and the block will
	// never be pooled - the only safe reading, since a client mapping may outlive
	// us.
	fd, err := page.Memfd.ExportFd()
	if err != nil {
		return nil, ErrNotFound
	}
	key := CacheKey{target, blk}
	return os.NewFile(uintptr(fd), key.String()), nil
}

func (c *mcache) prepareBlockFile(target string, blk, size uint32) (*os.File, error) {
	var cleanup bool
	var dupFile *os.File
	key := CacheKey{target, blk}
	c.commitMu.Lock()
	if _, ok := c.toCommit[key]; ok {
		c.commitMu.Unlock()
		return nil, nil
	}
	c.commitMu.Unlock()

	if c.check(target, blk) {
		return nil, nil
	}

	// allocate page via memfd so we could pass this fd to cachefs fuse client via UDS.
	page, err := AllocPage(int(size), key.String())
	if err != nil {
		return nil, err
	}
	if page.Memfd == nil {
		page.Release()
		return nil, errors.New("page is not memfd backed")
	}
	defer func() {
		if cleanup {
			// The exported copy never left this process on any cleanup path, so
			// closing it here proves it is gone. Give the export back before freeing
			// the page, in that order, or the block would be kept out of the pool for
			// no reason.
			if dupFile != nil {
				dupFile.Close()
				page.Memfd.UnexportFd()
			}
			page.Release()
		}
	}()

	// Export a copy and keep the original open, so we can commit it in
	// `commitBlockFile`. Same as getBlockFile: the copy goes to the fuse client
	// and its disappearance is never proven, so this block stays out of the pool.
	dupFd, err := page.Memfd.ExportFd()
	if err != nil {
		cleanup = true
		return nil, err
	}
	dupFile = os.NewFile(uintptr(dupFd), key.String())

	// check toCommit again, as we released the lock
	now := time.Now()
	c.commitMu.Lock()
	defer c.commitMu.Unlock()
	if _, ok := c.toCommit[key]; ok {
		// someone else prepared this cache file while we released the lock
		cleanup = true
		return nil, nil
	} else {
		c.toCommit[key] = &pendingPage{now, page, size}
		return dupFile, nil
	}
}

// commit previously prepared cache file
func (c *mcache) commitBlockFile(target string, blk uint32, commit bool) (rerr error) {
	key := CacheKey{target, blk}
	name := key.String()
	c.commitMu.Lock()
	pp, ok := c.toCommit[key]
	delete(c.toCommit, key)
	c.commitMu.Unlock()
	if !ok {
		logger.Warnf("Cache for target %s block %d not prepared", target, blk)
		return errors.New("cache not prepared")
	}

	page := pp.page
	pp.page = nil
	if page.Memfd == nil {
		logger.Fatal("missing memfd in page")
	}
	defer page.Release()

	// Not commit, cache file may not be written correctly, just discard it.
	if !commit {
		return nil
	}

	// make sure file size and occupied space is valid
	var stat unix.Stat_t
	if err := unix.Fstat(page.Memfd.Fd, &stat); err != nil {
		logger.Warnf("Fstat to-commit cache file for target %s block %d: %s", target, blk, err)
		return err
	}
	if stat.Size != int64(pp.size) {
		logger.Warnf("To-commit cache file %s has unexpected size %d, want %d", name, stat.Size, pp.size)
		return fmt.Errorf("unexpected file size %d, want %d", stat.Size, pp.size)
	}
	if stat.Blocks == 0 {
		logger.Warnf("To-commit cache file %s occupies 0 space", name)
		return fmt.Errorf("to-commit cache file %s has 0 filesystem block", name)
	}

	timeUsed := time.Since(pp.created)
	c.cache(target, blk, page)
	logger.Debugf("Daemon commited %s block %d in %s", target, blk, timeUsed)
	return nil
}

// lockTarget adds one eviction-protection reference for every supplied block.
// A zero-length block list adds one reference that protects every block of target.
func (c *mcache) lockTarget(target string, blocks []uint32) {
	c.Lock()
	defer c.Unlock()
	locked := c.lockedBlocks[target]
	if locked == nil {
		locked = make(map[int]int)
		c.lockedBlocks[target] = locked
	}
	if len(blocks) == 0 {
		locked[-1]++
		return
	}
	for _, block := range blocks {
		locked[int(block)]++
	}
}

// unlockTarget removes one reference for the same target and block selection
// passed to lockTarget; the target entry disappears only after every reference
// has been removed. A zero-length block list means whole-target protection. An
// unlock without a matching reference leaves existing references unchanged.
func (c *mcache) unlockTarget(target string, blocks []uint32) {
	c.Lock()
	defer c.Unlock()
	locked := c.lockedBlocks[target]
	if locked == nil {
		logger.Warnf("Unlock target %s which is not locked", target)
		return
	}
	keys := make([]int, 0, len(blocks))
	if len(blocks) == 0 {
		keys = append(keys, -1)
	} else {
		for _, block := range blocks {
			keys = append(keys, int(block))
		}
	}
	for _, key := range keys {
		if n := locked[key]; n > 1 {
			locked[key] = n - 1
		} else if n == 1 {
			delete(locked, key)
		} else {
			logger.Warnf("Unlock target %s block %d which is not locked", target, key)
		}
	}
	if len(locked) == 0 {
		delete(c.lockedBlocks, target)
	}
}

// isLocked reports whether a target-wide or per-block reference protects key.
// Caller must hold the cache mutex.
func (c *mcache) isLocked(key CacheKey) bool {
	locked := c.lockedBlocks[key.Target]
	return locked != nil && (locked[-1] > 0 || locked[int(key.Blk)] > 0)
}

func (c *mcache) evictCacheItem(ectx *memEvictionContext, key CacheKey, mi memItem, evictionType string) {
	if c.isLocked(key) {
		// explicit removal of protected block
		c.evictsLocked.Add(1)
	}
	size := int64(len(mi.page.Data))
	isHashHit := c.store.hashHit(key.String())

	if c.store.pinCache.isPinned(key.Target) {
		c.pinEvicts.Add(1)
		c.pinEvictBytes.Add(float64(size))
		if isHashHit {
			c.pinEvictsHashHit.Add(1)
		}
	}

	c.cacheEvicts.Add(1)
	if isHashHit {
		c.cacheEvictsHashHit.Add(1)
	} else {
		c.cacheEvictsHashMiss.Add(1)
	}

	switch evictionType {
	case evictionCleanup:
		c.cacheCleanup.Add(1)
		if isHashHit {
			c.cacheCleanupHashHit.Add(1)
		} else {
			c.cacheCleanupHashMiss.Add(1)
		}
	case evictionExpire:
		c.cacheExpired.Add(1)
		if isHashHit {
			c.cacheExpiredHashHit.Add(1)
		} else {
			c.cacheExpiredHashMiss.Add(1)
		}
	case evictionRemoveByPrefix:
		c.cacheRemoveByPrefix.Add(1)
		if isHashHit {
			c.cacheRemoveByPrefixHashHit.Add(1)
		} else {
			c.cacheRemoveByPrefixHashMiss.Add(1)
		}
	case evictionClearBurst:
		c.burstEvicts.Add(1)
		c.burstEvictBytes.Add(float64(size))
	}

	ectx.freed += int64(cap(mi.page.Data))
	ectx.todel[key] = mi.page
	c.used -= int64(cap(mi.page.Data))
	delete(c.pages, key)
	c.lru.remove(key)
	if ti, ok := c.targets[key.Target]; ok {
		delete(c.targets, key.Target)
		ectx.delTargets[key.Target] = ti
	}

	if evictionType == evictionRemoveByPrefix {
		if len(ectx.prefix) == 0 {
			logger.Debugf("remove %s by keep prefixes", key.String())
		} else {
			logger.Debugf("remove %s by prefix %s", key.String(), ectx.prefix)
		}
	} else {
		logger.Debugf("remove %s from cache, age: %s", key.String(), ectx.age)
	}
}

// locked
func (c *mcache) cleanup(skipKeys map[CacheKey]struct{}) {
	c.doCleanup(c.burstCap, evictionCleanup, skipKeys)
}

// locked
func (c *mcache) doCleanup(targetCapacity int64, evictionType string, skipKeys map[CacheKey]struct{}) (int64, int64) {
	var now = time.Now()
	ectx := newMemEvictionContext()

	shouldStop := func() bool {
		return c.used <= targetCapacity
	}

	shouldSkipHard := func(k CacheKey) bool {
		// Hard-skip protected blocks in ALL rounds including R5. Placed first so it
		// short-circuits before hashHit: when protected bytes reach the cap, every
		// insert's inline cleanup scans the whole map and almost every entry is protected;
		// without this ordering each visited entry would allocate a string in
		// hashHit(k.String()), creating heavy GC pressure.
		if c.isLocked(k) {
			c.evictsSkipLocked.Add(1)
			return true
		}

		// Skip all specified to-skip targets
		if _, ok := skipKeys[k]; ok {
			logger.Tracef("Skip cleanup cache key %s", k.String())
			return true
		}
		if ti, ok := c.targets[k.Target]; ok {
			// Don't cleanup not completed target, which might be in the middle of Set
			if !ti.complete {
				return true
			}
		}
		return false
	}

	shouldSkip := func(k CacheKey, hash, pin bool) bool {
		// Skip hard-protected blocks and blocks in the recent-add/load window.
		if shouldSkipHard(k) || c.lru.contains(k) {
			return true
		}
		// Skip cache hashed to local node if hash is enabled
		if hash && c.store.hashHit(k.String()) {
			return true
		}
		if pin && c.store.pinCache.isPinned(k.Target) {
			return true
		}
		return false
	}

	compareAndCleanup := func(compare, hash, pin bool) {
		c.targetMu.Lock()
		defer c.targetMu.Unlock()
	RETRY:
		var cnt int
		var lastKey CacheKey
		var lastValue memItem
		if compare {
			cnt = 0
		} else {
			cnt = 2
		}
		lastEvicted := len(ectx.todel) // track progress for this pass
		for k, v := range c.pages {
			if shouldSkip(k, hash, pin) {
				logger.Tracef("Skip cleanup cache key %s", k.String())
				continue
			}
			if compare {
				if cnt == 0 || lastValue.atime.After(v.atime) {
					lastKey = k
					lastValue = v
				}
				cnt++
			} else {
				lastKey = k
				lastValue = v
			}
			if cnt > 1 {
				ectx.age = now.Sub(lastValue.atime)
				c.evictCacheItem(ectx, lastKey, lastValue, evictionType)
				if compare {
					cnt = 0
				} else {
					cnt = 2
				}
				if shouldStop() {
					break
				}
			}
		}
		// We only cleanup half blocks at max if compare is enabled, let's try again if
		// needed. Guards:
		//   - only RETRY if this pass made progress (evicted at least one key).
		//   - only RETRY if >= 2 pages remain to form a comparison pair.
		if compare && !shouldStop() && len(ectx.todel) > lastEvicted && len(c.pages) >= 2 {
			goto RETRY
		}
	}

	hasPins := c.store.pinCache.hasPins()

	// Hash based eviction is only useful when there're multiple nodes in cluster
	if c.store.config.HashringEnabled() && c.store.serf.GetMemberCount() > 1 {
		// R1: 2-random evict non-hash-hit, skip pinned if hasPins
		compareAndCleanup(true, true, hasPins)

		// R2: direct evict non-hash-hit, skip pinned if hasPins
		if !shouldStop() {
			compareAndCleanup(false, true, hasPins)
		}

		// R3: 2-random evict non-hash-hit including pinned (only needed when hasPins)
		if hasPins && !shouldStop() {
			compareAndCleanup(true, true, false)
		}
	}

	// R4: 2-random evict hash-hit but non-pinned
	if hasPins && !shouldStop() {
		compareAndCleanup(true, false, true)
	}

	// R5: last resort, evict all including pinned
	if !shouldStop() {
		compareAndCleanup(true, false, false)
	}

	// If regular rounds cannot reach the target, evict LRU entries oldest first while retaining
	// hard-skip conditions.
	if !shouldStop() && c.lru != nil {
		c.targetMu.Lock()
		c.lru.forEach(func(key CacheKey) bool {
			if shouldStop() {
				return false
			}
			item := c.pages[key]
			if !shouldSkipHard(key) {
				ectx.age = now.Sub(item.atime)
				c.evictCacheItem(ectx, key, item, evictionType)
				c.lruFallbackEvicts.Add(1)
			}
			return !shouldStop()
		})
		c.targetMu.Unlock()
	}

	if len(ectx.todel) > 0 {
		logger.Debugf("cleanup memory cache: remaining %d blocks %d MB, freed %d blocks %d MB, %d targets in %s",
			len(c.pages), c.used>>20, len(ectx.todel), ectx.freed>>20, len(ectx.delTargets), time.Since(now))
	}

	c.Unlock()
	c.revokeDel(ectx)
	c.Lock()

	return int64(len(ectx.todel)), ectx.freed
}

func (c *mcache) dump() *metaInfo {
	tms := make(map[string]*targetMeta)
	skipTargets := make(map[string]struct{})
	c.Lock()
	c.targetMu.Lock()
	defer c.targetMu.Unlock()
	defer c.Unlock()
	for target, titem := range c.targets {
		// Skip not fully cached targets
		if !titem.complete {
			continue
		}
		// targetMeta with block bitset as nil indicates treat target as a whole
		tms[target] = &targetMeta{
			Length: titem.length,
		}
		skipTargets[target] = struct{}{}
	}
	for k := range c.pages {
		// Skip already processed targets
		if _, ok := skipTargets[k.Target]; ok {
			continue
		}
		tm, ok := tms[k.Target]
		if !ok {
			tm = &targetMeta{
				Length: 0,
				Blocks: bitset.New(uint(k.Blk)),
			}
			tms[k.Target] = tm
		}
		tm.Blocks.Set(uint(k.Blk))
	}
	if len(tms) > 0 {
		return &metaInfo{Targets: tms}
	} else {
		return nil
	}
}

func (c *mcache) cleanupExpire() {
	interval := getExpireInterval(c.cacheExpire)

	for {
		time.Sleep(interval)
		now := time.Now()
		ectx := newMemEvictionContext()
		c.Lock()
		hasPins := c.store.pinCache.hasPins()

		for key, mi := range c.pages {
			// Do not expire protected blocks: a client may still access their
			// zero-copy descriptors until the matching reference is released.
			if c.isLocked(key) {
				c.evictsSkipLocked.Add(1)
				continue
			}
			// Pinned blocks are exempt from expiration.
			if hasPins && c.store.pinCache.isPinned(key.Target) {
				c.evictsSkipPinned.Add(1)
				continue
			}
			age := now.Sub(mi.atime)
			if age > interval {
				ectx.age = age
				c.evictCacheItem(ectx, key, mi, evictionExpire)
			}
		}
		if len(ectx.todel) > 0 {
			logger.Infof("Expire cache: remaining %d blocks (%d MB), expired %d blocks (%d MB)",
				len(c.pages), c.used>>20, len(ectx.todel), ectx.freed>>20)
		}
		c.Unlock()
		c.revokeDel(ectx)
	}
}

func (c *mcache) revokeDel(ectx *memEvictionContext) {
	for key, p := range ectx.todel {
		p.Release()
		c.store.rcache.revoke(key.Target, 0, key.Blk)
	}
	for target, ti := range ectx.delTargets {
		c.store.rcache.revoke(target, ti.length, 0)
	}
}

func getExpireInterval(cacheExpire time.Duration) time.Duration {
	const defaultInterval = 24 * time.Hour
	switch {
	case cacheExpire <= time.Minute:
		return cacheExpire
	case cacheExpire <= defaultInterval:
		return cacheExpire / 2
	default:
		return defaultInterval
	}
}

// clearBurst evicts cache items to bring usage down to baseCapacity (cache-size).
// Returns the number of evicted blocks and bytes.
func (c *mcache) clearBurst() (int64, int64) {
	c.Lock()
	defer c.Unlock()
	if c.used <= c.capacity {
		return 0, 0
	}

	start := time.Now()
	totalCnt, totalBytes := c.doCleanup(c.capacity, evictionClearBurst, nil)

	if totalCnt > 0 {
		logger.Infof("Clear burst: evicted %d blocks %d MB in %s, remaining %d blocks %d MB",
			totalCnt, totalBytes>>20, time.Since(start), len(c.pages), c.used>>20)
	}
	return totalCnt, totalBytes
}

// setCapacity updates cache capacity and burst cache capacity at runtime.
// CacheSizeKeepCurrent means "keep current value"; BurstCacheSizeRestore
// restores the startup-configured burst cap.
// burstCacheSize is always kept >= cacheSize after update.
// Returns new cache size, new burst size, evicted block count and bytes.
func (c *mcache) setCapacity(cacheSize, burstCacheSize int64) (int64, int64, int64, int64) {
	c.Lock()
	oldCacheSize := c.capacity
	oldBurstCap := c.burstCap

	// Resolve sentinels before normalization
	if cacheSize == CacheSizeKeepCurrent {
		cacheSize = oldCacheSize
	}
	if burstCacheSize == BurstCacheSizeRestore {
		initial := c.store.config.InitialBurstSize
		logger.Infof("Memory cache: restoring burst cache to startup-configured %dMB", initial>>20)
		burstCacheSize = initial
	} else if burstCacheSize == CacheSizeKeepCurrent {
		burstCacheSize = oldBurstCap
	}
	// Normalize: burst must be >= cache
	if burstCacheSize < cacheSize {
		burstCacheSize = cacheSize
	}
	c.capacity = cacheSize
	c.burstCap = burstCacheSize

	// Use evictionClearBurst when base capacity is unchanged and burst is being reduced.
	evictType := evictionCleanup
	if cacheSize == oldCacheSize && burstCacheSize < oldBurstCap {
		evictType = evictionClearBurst
	}

	// Sync effective values to config atomically so concurrent readers via GetCacheSize see
	// the updated capacity.
	atomic.StoreInt64(&c.store.config.CacheSize, cacheSize)
	atomic.StoreInt64(&c.store.config.BurstCacheSize, burstCacheSize)
	logger.Infof("Memory cache: capacity %dMB -> %dMB, burstCap %dMB -> %dMB",
		oldCacheSize>>20, cacheSize>>20, oldBurstCap>>20, burstCacheSize>>20)
	var evictedCnt, evictedBytes int64
	if c.used > c.burstCap {
		// doCleanup targets new burstCap; call directly to capture evict stats
		evictedCnt, evictedBytes = c.doCleanup(c.burstCap, evictType, nil)
	}
	c.Unlock()
	return cacheSize, burstCacheSize, evictedCnt, evictedBytes
}
