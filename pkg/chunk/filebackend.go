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
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aliyun/cachefs/pkg/metric"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/willf/bitset"
	"golang.org/x/sys/unix"
)

const (
	metaFileName string = "meta"
	sizeFileName string = "size"
)

type cacheItem struct {
	size  int32
	atime uint32
}

type ChunkReader interface {
	// Read from cached chunk
	io.Reader
	io.ReaderAt

	// Close this chunk
	io.Closer

	// ExportFd duplicates the chunk's backend fd for use outside this process, and
	// returns an error if there's no backing fd. For a memfd-backed chunk it also
	// returns the *utils.Memfd, which has counted the exported copy; the caller
	// must pass it on to whoever can later prove that copy is gone. It is nil for
	// a file-backed chunk, which needs no such accounting.
	ExportFd() (int, *utils.Memfd, error)
}

type localCache interface {
	cache(target string, blk uint32, p *Page)
	load(target string, blk uint32, touchLRU bool) (*Page, ChunkReader, error)
	check(target string, blk uint32) bool
	removeByPrefix(remove, keep []string) (int64, int64)
	prepareFile(target string, length uint64) error
	commitFile(target string) error
	getBlockFile(target string, blk uint32) (*os.File, error)
	prepareBlockFile(target string, blk uint32, size uint32) (*os.File, error)
	commitBlockFile(target string, blk uint32, commit bool) error
	stats() (int64, int64)
	getCacheState() CacheState
	dump() *metaInfo
	clearBurst() (int64, int64)
	setCapacity(cacheSize, burstCacheSize int64) (int64, int64, int64, int64)
	lockTarget(target string, blocks []uint32)
	unlockTarget(target string, blocks []uint32)
}

// parse cache $target/$block into CacheKey
func parseCacheKey(p string) (CacheKey, error) {
	target := filepath.Dir(p)
	blk, err := strconv.Atoi(filepath.Base(p))
	if err != nil {
		return CacheKey{}, err
	}
	return CacheKey{target, uint32(blk)}, nil
}

type pendingCache struct {
	key  CacheKey
	page *Page
}

type pendingBlockFile struct {
	created time.Time
	file    *os.File
	size    uint32
}

type targetItem struct {
	atime    time.Time
	length   uint64
	complete bool
}

type diskCache struct {
	sync.Mutex
	store          *store
	dir            string
	capacity       int64
	burstCap       int64 // burst cache size upper limit (BurstCacheSize), should be >= capacity
	freeRatio      float32
	hashhitpending []pendingCache
	nonhitpending  []pendingCache
	pendingLimit   int
	pendingCond    *sync.Cond
	pages          map[CacheKey]*Page
	cacheExpire    time.Duration

	used    int64
	keys    map[CacheKey]cacheItem
	tmpKeys map[CacheKey]cacheItem
	scanned bool
	full    bool

	// Protected block reference counts by target and block index. Protected blocks
	// are not evicted under cache pressure or expiration (RemoveByPrefix is an
	// exception). A block remains protected until all of its references are released.
	lockedBlocks map[string]map[int]int

	targetMu sync.Mutex
	targets  map[string]*targetItem

	commitMu sync.Mutex
	toCommit map[CacheKey]*pendingBlockFile

	// O_DIRECT for the local cache backend. readDIO/writeDIO are set once at construction
	// (read-only afterwards); dio is a shared latch that permanently disables both once the
	// cache filesystem rejects O_DIRECT.
	readDIO  bool
	writeDIO bool
	dio      *utils.DioGate

	cacheWrites                 prometheus.Counter
	cacheWriteBytes             prometheus.Counter
	cacheWriteHist              prometheus.Histogram
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
	cacheDrops                  prometheus.Counter
	cacheDropHashHit            prometheus.Counter
	cacheDropHashMiss           prometheus.Counter
	cacheDropBytes              prometheus.Counter
	cacheDropPreempts           prometheus.Counter
	cacheDropPreemptBytes       prometheus.Counter
	burstEvicts                 prometheus.Counter
	burstEvictBytes             prometheus.Counter
	burstBytes                  prometheus.GaugeFunc
	pinEvicts                   *metric.AtomicCounter
	pinEvictsHashHit            *metric.AtomicCounter
	pinEvictBytes               prometheus.Counter
	evictsSkipLocked            prometheus.Counter
	evictsSkipPinned            prometheus.Counter
	evictsLocked                prometheus.Counter
	readDirectIOGauge           prometheus.GaugeFunc
	writeDirectIOGauge          prometheus.GaugeFunc
}

// dioAllowed reports whether O_DIRECT may be attempted at all for the local cache backend. It returns
// false silently when the feature was not requested; when requested but unavailable it logs a
// one-time warning explaining why. Requirement: not running as a daemon server. Buffer alignment is
// checked per IO, since a partial block is shorter than a full one.
func dioAllowed(config *StoreConfig, requested bool, kind string) bool {
	switch {
	case !requested:
		return false
	case config.Socket != "":
		logger.Warnf("Cache %s direct IO is enabled but ignored in daemon-server mode; using buffered IO", kind)
		return false
	default:
		return true
	}
}

// readDirectIOEnabled reports whether O_DIRECT should currently be attempted for
// reading cached blocks.
func (cache *diskCache) readDirectIOEnabled() bool {
	return cache.readDIO && !cache.dio.Disabled()
}

// writeDirectIOEnabled reports whether O_DIRECT should currently be attempted for
// writing cached blocks.
func (cache *diskCache) writeDirectIOEnabled() bool {
	return cache.writeDIO && !cache.dio.Disabled()
}

type cacheEvictionContext struct {
	age        uint32
	delTargets map[string]*targetItem
	freed      int64
	prefix     string
	todel      []CacheKey
}

func newCacheEvictionContext() *cacheEvictionContext {
	return &cacheEvictionContext{
		todel:      make([]CacheKey, 0),
		delTargets: make(map[string]*targetItem),
	}
}

func newLocalCache(config *StoreConfig, store *store, reg prometheus.Registerer) localCache {
	if config.CacheSize == 0 {
		logger.Info("Cache size is 0, local cache disabled.")
	}
	if config.IsMemoryBackend() {
		return newMemCache(config, store, reg)
	}
	if config.FreeSpace == 0.0 {
		config.FreeSpace = 0.1 // 10% as default
	}

	pendingPages := config.BufferSize / int64(config.BlockSize)
	if pendingPages == 0 {
		logger.Warnf("Writeback buffer %d too small, reset to block size", config.BufferSize)
		pendingPages = 1
	}

	readDIO := dioAllowed(config, config.CacheReadDirectIO, "read")
	writeDIO := dioAllowed(config, config.CacheWriteDirectIO, "write")
	if readDIO {
		logger.Infof("Cache read direct IO (O_DIRECT) enabled for %s", config.CacheDir)
	}
	if writeDIO {
		logger.Infof("Cache write direct IO (O_DIRECT) enabled for %s", config.CacheDir)
	}

	c := &diskCache{
		store:    store,
		dir:      config.CacheDir,
		capacity: config.CacheSize,
		// initBurstSizes has already settled this, but floor it anyway: a burst cap
		// below capacity would make every insert evict down to it.
		burstCap:       max(config.BurstCacheSize, config.CacheSize),
		freeRatio:      config.FreeSpace,
		keys:           make(map[CacheKey]cacheItem),
		lockedBlocks:   make(map[string]map[int]int),
		hashhitpending: make([]pendingCache, 0, int(pendingPages)),
		nonhitpending:  make([]pendingCache, 0, int(pendingPages)),
		pendingLimit:   int(pendingPages),
		pages:          make(map[CacheKey]*Page),
		targets:        make(map[string]*targetItem),
		toCommit:       make(map[CacheKey]*pendingBlockFile),
		cacheExpire:    config.CacheExpire,
		readDIO:        readDIO,
		writeDIO:       writeDIO,
		dio:            utils.NewDioGate("local cache"),
	}
	c.pendingCond = sync.NewCond(c)
	c.InitMetrics(reg)

	os.MkdirAll(c.dir, 0755)
	br, fr := c.curFreeRatio()
	if br < c.freeRatio || fr < c.freeRatio {
		logger.Warnf("not enough space (%.1f%%) or inodes (%.1f%%) for caching in %s: free ratio should be >= %.1f%%", br*100, fr*100, c.dir, c.freeRatio*100)
	}
	logger.Infof("Disk cache (%s): capacity %d MB, burst capacity %d MB, block size %d MB free ratio %.1f%%, max pending pages %d",
		c.dir, c.capacity>>20, c.burstCap>>20, config.BlockSize>>20, c.freeRatio*100, pendingPages)
	logger.Infof("Start %d disk cache writeback flusher threads", config.WritebackWorker)
	for i := 0; i < config.WritebackWorker; i++ {
		go c.flush()
	}
	go c.checkFreeSpace()
	if c.cacheExpire > 0 {
		go c.cleanupExpire()
	}
	go c.refreshCacheKeys()
	go c.gcCommitBlockFiles()
	go c.gcCommitTargets()
	return c
}

func (cache *diskCache) InitMetrics(reg prometheus.Registerer) {
	cache.cacheWrites = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_writes",
		Help: "written cached block",
	})
	cache.cacheEvicts = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts",
		Help: "evicted cache blocks",
	})
	cache.cacheWriteBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_write_bytes",
		Help: "write bytes of cached block",
	})
	cache.cacheWriteHist = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "blockcache_write_hist_seconds",
		Help:    "write cached block latency distribution",
		Buckets: prometheus.ExponentialBuckets(0.00001, 2, 20),
	})
	cache.cacheEvictsHashHit = cache.store.counterReg.NewCounter(prometheus.CounterOpts{
		Name: CounterEvictsHashHit,
		Help: "total evicted blocks that are hash hits",
	})
	cache.cacheEvictsHashMiss = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_hashmiss",
		Help: "total evicted blocks that are hash miss",
	})
	cache.cacheCleanup = cache.store.counterReg.NewCounter(prometheus.CounterOpts{
		Name: CounterEvictsCleanup,
		Help: "evicted blocks by cleanup",
	})
	cache.cacheCleanupHashHit = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_cleanup_hashhit",
		Help: "evicted blocks by cleanup that are hash hits",
	})
	cache.cacheCleanupHashMiss = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_cleanup_hashmiss",
		Help: "evicted blocks by cleanup that are hash miss",
	})
	cache.cacheExpired = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_expire",
		Help: "evicted blocks by expire",
	})
	cache.cacheExpiredHashHit = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_expire_hashhit",
		Help: "evicted blocks by expire that are hash hits",
	})
	cache.cacheExpiredHashMiss = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_expire_hashmiss",
		Help: "evicted blocks by expire that are hash miss",
	})
	cache.cacheRemoveByPrefix = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_removebyprefix",
		Help: "evicted blocks by removebyprefix",
	})
	cache.cacheRemoveByPrefixHashHit = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_removebyprefix_hashhit",
		Help: "evicted blocks by removebyprefix that are hash hits",
	})
	cache.cacheRemoveByPrefixHashMiss = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_removebyprefix_hashmiss",
		Help: "evicted blocks by removebyprefix that are hash miss",
	})
	cache.cacheDrops = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_drops",
		Help: "dropped blocks due to limited writeback bandwidth",
	})
	cache.cacheDropBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_drop_bytes",
		Help: "size of dropped blocks",
	})
	cache.cacheDropHashHit = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_drop_hashhit",
		Help: "dropped blocks that are hash hit",
	})
	cache.cacheDropHashMiss = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_drop_hashmiss",
		Help: "dropped blocks that are hash miss",
	})
	cache.cacheDropPreempts = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_drop_preempts",
		Help: "hash-miss blocks evicted from the writeback queue to make room for hash-hit blocks",
	})
	cache.cacheDropPreemptBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_drop_preempt_bytes",
		Help: "size of hash-miss blocks evicted to make room for hash-hit blocks",
	})
	cache.burstEvicts = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_burst_evicts",
		Help: "total evicted burst blocks",
	})
	cache.burstEvictBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_burst_evict_bytes",
		Help: "total bytes of evicted burst blocks",
	})
	cache.pinEvicts = cache.store.counterReg.NewCounter(prometheus.CounterOpts{
		Name: CounterEvictsPinned,
		Help: "total evicted pinned blocks",
	})
	cache.pinEvictsHashHit = cache.store.counterReg.NewCounter(prometheus.CounterOpts{
		Name: CounterEvictsPinnedHashHit,
		Help: "total evicted pinned blocks that are hash hits",
	})
	cache.pinEvictBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_pinned_bytes",
		Help: "total bytes of evicted pinned blocks",
	})
	cache.evictsSkipLocked = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_skip_locked",
		Help: "eviction candidates skipped because their block is share-locked",
	})
	cache.evictsSkipPinned = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_skip_pinned",
		Help: "eviction candidates skipped because their block is pinned",
	})
	cache.evictsLocked = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "blockcache_evicts_locked",
		Help: "share-locked blocks deleted by explicit removal",
	})
	cache.burstBytes = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "blockcache_burst_bytes",
		Help: "current burst cache usage in bytes (used - capacity when positive)",
	}, func() float64 {
		cache.Lock()
		defer cache.Unlock()
		burst := cache.used - cache.capacity
		if burst < 0 {
			burst = 0
		}
		return float64(burst)
	})
	cache.readDirectIOGauge = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "blockcache_read_directio_enabled",
		Help: "whether O_DIRECT is currently in effect for reading cached blocks (1) or not (0)",
	}, func() float64 {
		if cache.readDirectIOEnabled() {
			return 1
		}
		return 0
	})
	cache.writeDirectIOGauge = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "blockcache_write_directio_enabled",
		Help: "whether O_DIRECT is currently in effect for writing cached blocks (1) or not (0)",
	}, func() float64 {
		if cache.writeDirectIOEnabled() {
			return 1
		}
		return 0
	})
	if reg == nil {
		return
	}

	reg.MustRegister(cache.readDirectIOGauge)
	reg.MustRegister(cache.writeDirectIOGauge)
	reg.MustRegister(cache.cacheWrites)
	reg.MustRegister(cache.cacheWriteBytes)
	reg.MustRegister(cache.cacheWriteHist)
	reg.MustRegister(cache.cacheEvicts)
	reg.MustRegister(cache.cacheEvictsHashHit)
	reg.MustRegister(cache.cacheEvictsHashMiss)
	reg.MustRegister(cache.cacheCleanup)
	reg.MustRegister(cache.cacheCleanupHashHit)
	reg.MustRegister(cache.cacheCleanupHashMiss)
	reg.MustRegister(cache.cacheExpired)
	reg.MustRegister(cache.cacheExpiredHashHit)
	reg.MustRegister(cache.cacheExpiredHashMiss)
	reg.MustRegister(cache.cacheRemoveByPrefix)
	reg.MustRegister(cache.cacheRemoveByPrefixHashHit)
	reg.MustRegister(cache.cacheRemoveByPrefixHashMiss)
	reg.MustRegister(cache.cacheDrops)
	reg.MustRegister(cache.cacheDropBytes)
	reg.MustRegister(cache.cacheDropHashHit)
	reg.MustRegister(cache.cacheDropHashMiss)
	reg.MustRegister(cache.cacheDropPreempts)
	reg.MustRegister(cache.cacheDropPreemptBytes)
	reg.MustRegister(cache.burstEvicts)
	reg.MustRegister(cache.burstEvictBytes)
	reg.MustRegister(cache.burstBytes)
	reg.MustRegister(cache.pinEvicts)
	reg.MustRegister(cache.pinEvictsHashHit)
	reg.MustRegister(cache.pinEvictBytes)
	reg.MustRegister(cache.evictsSkipLocked)
	reg.MustRegister(cache.evictsSkipPinned)
	reg.MustRegister(cache.evictsLocked)
}

func (cache *diskCache) stats() (int64, int64) {
	cache.Lock()
	defer cache.Unlock()
	return int64(len(cache.pages) + len(cache.keys)), cache.used
}

// Aggregate the raw cache state of this backend: capacity and used bytes, plus
// cached blocks/bytes and, among them, the hash-hit blocks/bytes (keys whose
// content is verified present via the store hash ring). Both on-disk keys and
// in-memory pages count as cached.
func (cache *diskCache) getCacheState() CacheState {
	// Snapshot the resident keys under the lock, then compute hashHit/isPinned
	// outside it. The per-entry work (key formatting, hash-ring lookup, pinCache
	// prefix scan) dominates the scan cost, so keeping it out of the exclusive
	// lock cuts the lock hold time by ~95% and avoids blocking cache IO.
	type stateEntry struct {
		key  CacheKey
		size int64
	}
	cache.Lock()
	st := CacheState{Capacity: cache.capacity, Used: cache.used}
	entries := make([]stateEntry, 0, len(cache.keys)+len(cache.pages))
	for k, it := range cache.keys {
		entries = append(entries, stateEntry{k, int64(it.size)})
	}
	for k, p := range cache.pages {
		entries = append(entries, stateEntry{k, int64(len(p.Data))})
	}
	cache.Unlock()

	for _, e := range entries {
		st.CachedBlocks++
		st.CachedBytes += e.size
		if cache.store.hashHit(e.key.String()) {
			st.HashHitBlocks++
			st.HashHitBytes += e.size
		}
		if cache.store.pinCache.isPinned(e.key.Target) {
			st.PinnedBlocks++
			st.PinnedBytes += e.size
		}
	}
	return st
}

func (cache *diskCache) checkFreeSpace() {
	for {
		br, fr := cache.curFreeRatio()
		cache.full = br < cache.freeRatio/2 || fr < cache.freeRatio/2
		if br < cache.freeRatio || fr < cache.freeRatio {
			logger.Tracef("Cleanup cache when check free space (%s): free ratio (%.1f%%), space usage (%.1f%%), inodes usage (%.1f%%)", cache.dir, cache.freeRatio*100, br*100, fr*100)
			cache.Lock()
			cache.cleanup(nil)
			cache.Unlock()
		}
		time.Sleep(time.Second)
	}
}

func (cache *diskCache) revokeDel(ectx *cacheEvictionContext) {
	for _, key := range ectx.todel {
		_ = os.Remove(cache.cachePath(key))
		cache.store.rcache.revoke(key.Target, 0, key.Blk)
	}
	for target, ti := range ectx.delTargets {
		_, sizeFile, _ := cache.metaPath(CacheKey{Target: target})
		_ = os.Remove(sizeFile)
		cache.store.rcache.revoke(target, ti.length, 0)
	}
}

func (cache *diskCache) cleanupExpire() {
	interval := getExpireInterval(cache.cacheExpire)

	for {
		time.Sleep(interval)
		now := uint32(time.Now().UnixMilli())
		ectx := newCacheEvictionContext()
		cache.Lock()
		hasPins := cache.store.pinCache.hasPins()

		for key, value := range cache.keys {
			// Do not expire protected blocks: a client may still access their
			// zero-copy descriptors until the matching reference is released.
			if cache.isLocked(key) {
				cache.evictsSkipLocked.Add(1)
				continue
			}
			// Pinned blocks are exempt from expiration.
			if hasPins && cache.store.pinCache.isPinned(key.Target) {
				cache.evictsSkipPinned.Add(1)
				continue
			}
			age := now - value.atime
			if age > uint32(interval.Milliseconds()) {
				ectx.age = age
				cache.evictKeyCacheItem(ectx, key, int64(value.size), evictionExpire)
			}
		}
		delCnt := len(ectx.todel)
		if delCnt > 0 {
			logger.Infof("Expire cache (%s): remaining %d blocks (%d MB), expired %d blocks (%d MB)", cache.dir, len(cache.keys), cache.used>>20, delCnt, ectx.freed>>20)
		}
		cache.Unlock()
		cache.revokeDel(ectx)

	}
}

func (cache *diskCache) refreshCacheKeys() {
	for {
		cache.scanCached()
		time.Sleep(time.Minute * 5)
	}
}

func (cache *diskCache) gcCommitBlockFiles() {
	for {
		time.Sleep(time.Minute)
		twoMinAgo := time.Now().Add(-time.Minute * 2)
		toDelete := []*pendingBlockFile{}
		cache.commitMu.Lock()
		for key, pbf := range cache.toCommit {
			if pbf.created.Before(twoMinAgo) {
				delete(cache.toCommit, key)
				toDelete = append(toDelete, pbf)
			}
		}
		cache.commitMu.Unlock()
		if len(toDelete) > 0 {
			for _, pbf := range toDelete {
				logger.Infof("Discard stale pending cache file %s", pbf.file.Name())
				pbf.file.Close()
			}
		}
	}
}

func (cache *diskCache) gcCommitTargets() {
	// Don't start at the same time as gcCommitBlockFiles()
	time.Sleep(time.Second * 5)
	for {
		time.Sleep(time.Minute)
		twoMinAgo := time.Now().Add(-time.Minute * 2)
		dels := []string{}
		cache.targetMu.Lock()
		for target, ti := range cache.targets {
			if ti.atime.Before(twoMinAgo) && !ti.complete {
				delete(cache.targets, target)
				dels = append(dels, target)
			}
		}
		cache.targetMu.Unlock()
		if len(dels) > 0 {
			logger.Infof("Discard stale pending targets: %v", dels)
		}
	}
}

// pendingLen returns the total number of blocks queued for writeback across both priority queues.
// Caller must hold cache.Mutex.
func (cache *diskCache) pendingLen() int {
	return len(cache.hashhitpending) + len(cache.nonhitpending)
}

// dropPendingCache discards a block that will not be written back: it releases the page, removes
// the key from cache.pages and records the drop metrics.  Caller must hold cache.Mutex.
func (cache *diskCache) dropPendingCache(w pendingCache, isHashHit bool) {
	size := len(w.page.Data)
	if _, ok := cache.pages[w.key]; ok {
		delete(cache.pages, w.key)
		logger.Debugf("Pending cache queue is full, drop %s (%d bytes)", w.key.String(), size)
		cache.cacheDrops.Add(1)
		cache.cacheDropBytes.Add(float64(size))
		if isHashHit {
			cache.cacheDropHashHit.Add(1)
		} else {
			cache.cacheDropHashMiss.Add(1)
		}
	}
	w.page.Release()
}

// pushPending enqueues a block for writeback, giving hash-hit blocks priority. When the shared
// writeback queue is full, a hash-hit block preempts the newest queued hash-miss block to make
// room; otherwise the block is dropped. Returns true when the block was queued, and the key of a
// preempted block (if there's any) that the caller must revoke.  Caller must hold cache.Mutex.
func (cache *diskCache) pushPending(w pendingCache, isHashHit bool) (bool, *CacheKey) {
	if cache.pendingLen() < cache.pendingLimit {
		if isHashHit {
			cache.hashhitpending = append(cache.hashhitpending, w)
		} else {
			cache.nonhitpending = append(cache.nonhitpending, w)
		}
		cache.pendingCond.Signal()
		return true, nil
	}
	if isHashHit && len(cache.nonhitpending) > 0 {
		// Evict the most recently queued hash-miss block to protect the head
		// of the queue (which the flush workers are about to write) from being
		// starved. The eviction is a hash-miss drop and additionally counted as
		// a preemption (a subset of the hash-miss drops).
		n := len(cache.nonhitpending) - 1
		dropped := cache.nonhitpending[n]
		cache.nonhitpending[n] = pendingCache{}
		cache.nonhitpending = cache.nonhitpending[:n]
		cache.cacheDropPreempts.Add(1)
		cache.cacheDropPreemptBytes.Add(float64(len(dropped.page.Data)))
		cache.dropPendingCache(dropped, false)
		cache.hashhitpending = append(cache.hashhitpending, w)
		cache.pendingCond.Signal()
		return true, &dropped.key
	}
	cache.dropPendingCache(w, isHashHit)
	return false, nil
}

func (cache *diskCache) cache(target string, blk uint32, p *Page) {
	if cache.burstCap == 0 {
		return
	}
	var cached bool
	var preempted *CacheKey
	defer func() {
		if preempted != nil {
			cache.store.rcache.revoke(preempted.Target, 0, preempted.Blk)
		}
		if cached {
			cache.store.rcache.announce(target, 0, blk)
		}
	}()
	key := CacheKey{target, blk}
	isHashHit := cache.store.hashHit(key.String())
	cache.Lock()
	defer cache.Unlock()
	if _, ok := cache.pages[key]; ok {
		return
	}
	p.Acquire()
	cache.pages[key] = p
	cached, preempted = cache.pushPending(pendingCache{key, p}, isHashHit)
}

func (cache *diskCache) curFreeRatio() (float32, float32) {
	total, free, files, ffree := getDiskUsage(cache.dir)
	return float32(free) / float32(total), float32(ffree) / float32(files)
}

// Write file which contains target length, cache target with this file indicates we treat the
// target as a whole.
func (cache *diskCache) saveSize(target string, length uint64) error {
	// Fake a key, block count doesn't matter here
	key := CacheKey{target, 0}
	_, sizeFilePath, _ := cache.metaPath(key)
	_, err := os.Stat(sizeFilePath)
	if err == nil {
		// file already exists, just return
		return nil
	}

	if err := saveFile(sizeFilePath, []byte(strconv.FormatUint(length, 10))); err != nil {
		return err
	}
	return nil
}

func (cache *diskCache) loadSize(sizeFilePath string) (uint64, error) {
	file, err := os.Open(sizeFilePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	fi, err := file.Stat()
	length := int(fi.Size())
	if length > 20 {
		return 0, fmt.Errorf("Size file %s is larger than 20 (%d)", sizeFilePath, length)
	}

	// target length <= 20
	data := make([]byte, length)
	n, err := file.Read(data)
	if err != nil {
		return 0, err
	}
	if n != length {
		return 0, fmt.Errorf("Partial read %s, expect %d got %d", sizeFilePath, length, n)
	}
	size, err := strconv.ParseUint(string(data), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("Parse size file %s: %s", sizeFilePath, err)
	}
	return size, nil
}

// Write cache meta file which contains its real target string, if key.Target is different with its
// on-disk path. e.g. key.Target is "../test", its on-disk path would be "$CACHE_DIR/dotdot/test",
// so we'll write its real target in "$CACHE_DIR/dotdot/test/meta" file.
func (cache *diskCache) saveMeta(key CacheKey) (rerr error) {
	target := key.Target
	metaFilePath, _, onDiskTarget := cache.metaPath(key)
	if target == onDiskTarget && strings.HasPrefix(target, "/") {
		return nil
	}
	_, err := os.Stat(metaFilePath)
	if err == nil {
		// meta file already exists, just return
		return nil
	}

	if err := saveFile(metaFilePath, []byte(target)); err != nil {
		return err
	}
	return nil
}

func (cache *diskCache) loadMeta(metaFilePath string) (string, error) {
	metaFile, err := os.Open(metaFilePath)
	if err != nil {
		return "", err
	}
	defer metaFile.Close()
	fi, err := metaFile.Stat()
	length := int(fi.Size())
	if length > 4096 {
		return "", fmt.Errorf("Meta file %s is larger than 4K (%d)", metaFilePath, length)
	}

	// target length <= 4K
	data := make([]byte, length)
	n, err := metaFile.Read(data)
	if err != nil {
		return "", err
	}
	if n != length {
		return "", fmt.Errorf("Partial read %s, expect %d got %d", metaFilePath, length, n)
	}
	return string(data), nil
}

func (cache *diskCache) flushPage(key CacheKey, data []byte) (err error) {
	path := cache.cachePath(key)
	start := time.Now()
	cache.cacheWrites.Add(1)
	cache.cacheWriteBytes.Add(float64(len(data)))
	defer func() {
		cache.cacheWriteHist.Observe(time.Since(start).Seconds())
	}()
	os.MkdirAll(filepath.Dir(path), 0755)
	tmp := path + ".tmp"

	// Prefer O_DIRECT for a full, page-aligned block; the last/partial block or
	// an unaligned buffer uses buffered IO. On EINVAL (filesystem rejects
	// O_DIRECT) permanently disable and fall back to buffered.
	useDirect := cache.writeDirectIOEnabled() && utils.BufAligned(data)
	var f *os.File
	if useDirect {
		if f, err = utils.OpenDirect(tmp, os.O_WRONLY|os.O_CREATE, 0600); errors.Is(err, syscall.EINVAL) {
			// O_DIRECT unsupported by this filesystem; downgrade permanently and
			// reopen buffered below. A non-EINVAL open error is a real failure:
			// keep useDirect so we skip the buffered reopen and surface it.
			cache.dio.Disable(err)
			useDirect = false
		}
	}
	if !useDirect {
		f, err = os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE, 0600)
	}
	if err != nil {
		logger.Warnf("Can't create cache file %s: %s", tmp, err)
		return err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(tmp)
		} else {
			err = cache.saveMeta(key)
			if err != nil {
				logger.Warn(err.Error())
			}
		}
	}()

	if _, err = f.Write(data); err != nil {
		if useDirect && errors.Is(err, syscall.EINVAL) {
			// O_DIRECT accepted at open but the write was rejected (e.g. device
			// logical block size larger than our alignment); disable and retry
			// buffered from a truncated handle.
			cache.dio.Disable(err)
			_ = f.Close()
			if f, err = os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600); err != nil {
				logger.Warnf("Can't create cache file %s: %s", tmp, err)
				return err
			}
			_, err = f.Write(data)
		}
		if err != nil {
			logger.Warnf("Write to cache file %s failed: %s", tmp, err)
			_ = f.Close()
			return
		}
	}
	if err = f.Close(); err != nil {
		logger.Warnf("Close cache file %s failed: %s", tmp, err)
		return
	}
	if err = os.Rename(tmp, path); err != nil {
		// has been flushed by getBlockFile()
		if !errors.Is(err, fs.ErrNotExist) {
			logger.Warnf("Rename cache file %s -> %s failed: %s", tmp, path, err)
		}
	}
	return
}

// Disk cache has no eviction LRU, so touchLRU is ignored.
// The parameter is kept only for interface parity with mcache.
func (cache *diskCache) load(target string, blk uint32, _ bool) (*Page, ChunkReader, error) {
	cache.Lock()
	defer cache.Unlock()
	key := CacheKey{target, blk}
	if p, ok := cache.pages[key]; ok {
		p.Acquire()
		return p, nil, nil
	}
	if cache.scanned && cache.keys[key].atime == 0 {
		return nil, nil, ErrNotFound
	}
	cache.Unlock()
	f, err := os.Open(cache.cachePath(key))
	cache.Lock()
	if err == nil {
		if it, ok := cache.keys[key]; ok {
			// update atime
			cache.keys[key] = cacheItem{size: it.size, atime: uint32(time.Now().UnixMilli())}
		} else {
			// found untracked cache file, announce as well
			cache.store.rcache.announce(target, 0, blk)
		}
		if cache.readDirectIOEnabled() {
			return nil, newDirectCacheReader(cache.cachePath(key), cache.dio, f), nil
		}
		return nil, newOsFileReader(f), nil
	} else if it, ok := cache.keys[key]; ok {
		cache.used -= int64(it.size)
		delete(cache.keys, key)
	}
	// f is nil here: this branch is only reached when os.Open failed.
	return nil, nil, err
}

// directCacheReader is a ChunkReader over an immutable on-disk cache file. It
// serves aligned ReadAt from an O_DIRECT handle (opened lazily) and everything
// else — Read, ExportFd, and unaligned/partial ReadAt — from the buffered handle
// that load() already opened. ExportFd() intentionally exports the buffered fd:
// it is handed to clients that mmap it, and mmap ignores O_DIRECT, so a plain fd
// is correct and needs no caller cooperation.
type directCacheReader struct {
	mu       sync.Mutex
	path     string
	dio      *utils.DioGate
	direct   *os.File // opened lazily with O_DIRECT on first aligned ReadAt
	buffered *os.File // pre-opened by load()
}

func newDirectCacheReader(path string, dio *utils.DioGate, buffered *os.File) *directCacheReader {
	return &directCacheReader{path: path, dio: dio, buffered: buffered}
}

func (r *directCacheReader) getDirect() (*os.File, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.direct != nil {
		return r.direct, nil
	}
	f, err := utils.OpenDirect(r.path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	r.direct = f
	return f, nil
}

func (r *directCacheReader) ReadAt(p []byte, off int64) (int, error) {
	if !r.dio.Disabled() && off%utils.DirectIOAlignSize == 0 && utils.BufAligned(p) {
		if f, err := r.getDirect(); err == nil {
			n, rerr := f.ReadAt(p, off)
			if rerr == nil {
				return n, nil
			}
			if !errors.Is(rerr, syscall.EINVAL) {
				// A real read error, not an alignment rejection; surface it.
				return n, rerr
			}
			// O_DIRECT open succeeded but the read was rejected; disable
			// mount-wide and fall through to buffered.
			r.dio.Disable(rerr)
		} else if errors.Is(err, syscall.EINVAL) {
			r.dio.Disable(err)
		} else {
			return 0, err
		}
	}
	return r.buffered.ReadAt(p, off)
}

// Read is buffered: a sequential read has an arbitrary offset, length and
// caller buffer, none of which O_DIRECT's alignment requirements can be
// guaranteed for. It is also not the block-read hot path (that goes via ReadAt).
func (r *directCacheReader) Read(p []byte) (int, error) {
	return r.buffered.Read(p)
}

// ExportFd exports a dup of the buffered fd: it is handed to clients that mmap
// it, and mmap ignores O_DIRECT, so a plain fd is correct and needs no caller
// cooperation. No *utils.Memfd accompanies it, for the same reason as
// osFileReader: a cache file's inode never comes from the block pool.
func (r *directCacheReader) ExportFd() (int, *utils.Memfd, error) {
	fd, err := unix.Dup(int(r.buffered.Fd()))
	if err != nil {
		return -1, nil, err
	}
	return fd, nil, nil
}

func (r *directCacheReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var err error
	if r.direct != nil {
		err = r.direct.Close()
		r.direct = nil
	}
	if r.buffered != nil {
		if e := r.buffered.Close(); e != nil && err == nil {
			err = e
		}
		r.buffered = nil
	}
	return err
}

func (cache *diskCache) check(target string, blk uint32) bool {
	key := CacheKey{target, blk}
	cache.Lock()
	defer cache.Unlock()

	// check pending pages
	if _, ok := cache.pages[key]; ok {
		return true
	}
	if cache.scanned && cache.keys[key].atime == 0 {
		return false
	}

	// check on-disk file
	cache.Unlock()
	_, err := os.Stat(cache.cachePath(key))
	cache.Lock()
	if err != nil {
		return false
	}

	// update atime
	if it, ok := cache.keys[key]; ok {
		cache.keys[key] = cacheItem{size: it.size, atime: uint32(time.Now().UnixMilli())}
	}
	return true
}

func (cache *diskCache) removeByPrefix(remove, keep []string) (int64, int64) {
	ectx := newCacheEvictionContext()

	cache.Lock()
	defer cache.Unlock()

	for k, p := range cache.pages {
		evict, prefix := shouldEvictByPrefix(k.Target, remove, keep)
		if evict {
			ectx.prefix = prefix
			cache.evictPageCacheItem(ectx, k, int64(cap(p.Data)), evictionRemoveByPrefix)
		}
	}
	for k, item := range cache.keys {
		evict, prefix := shouldEvictByPrefix(k.Target, remove, keep)
		if evict {
			ectx.prefix = prefix
			cache.evictKeyCacheItem(ectx, k, int64(item.size), evictionRemoveByPrefix)
		}
	}

	cache.Unlock()
	cache.revokeDel(ectx)
	cache.Lock()
	return int64(len(ectx.todel)), ectx.freed
}

func (cache *diskCache) prepareFile(target string, length uint64) error {
	if length > uint64(cache.burstCap) {
		return fmt.Errorf("Data length larger than cache capacity (%d > %d)", length, cache.burstCap)
	}
	cache.targetMu.Lock()
	defer cache.targetMu.Unlock()
	if t, ok := cache.targets[target]; !ok {
		cache.targets[target] = &targetItem{time.Now(), length, false}
		return nil
	} else {
		t.atime = time.Now()
		return ErrExists
	}
}

func (cache *diskCache) commitFile(target string) error {
	cache.targetMu.Lock()
	defer cache.targetMu.Unlock()
	if ti, ok := cache.targets[target]; ok {
		if !ti.complete {
			ti.complete = true
			cache.store.rcache.announce(target, ti.length, 0)
			cache.targetMu.Unlock()
			if err := cache.saveSize(target, ti.length); err != nil {
				logger.Warn(err.Error())
			}
			cache.targetMu.Lock()
		}
		return nil
	} else {
		return ErrNotFound
	}
}

func (cache *diskCache) getBlockFile(target string, blk uint32) (*os.File, error) {
	key := CacheKey{target, blk}
	cache.Lock()
	defer cache.Unlock()
	if p, ok := cache.pages[key]; ok {
		// flush to disk so we could get a *os.File
		delete(cache.pages, key)
		p.Acquire()
		cache.Unlock()
		cache.doFlush(key, p.Data)
		cache.Lock()
		p.Release()
	}
	if cache.scanned && cache.keys[key].atime == 0 {
		return nil, ErrNotFound
	}

	cache.Unlock()
	f, err := os.Open(cache.cachePath(key))
	cache.Lock()
	if err != nil {
		return nil, err
	}
	if it, ok := cache.keys[key]; ok {
		// update atime
		cache.keys[key] = cacheItem{size: it.size, atime: uint32(time.Now().UnixMilli())}
	}
	return f, nil
}

func (cache *diskCache) prepareBlockFile(target string, blk, size uint32) (*os.File, error) {
	var cleanup bool
	var dupFile *os.File
	key := CacheKey{target, blk}
	cache.commitMu.Lock()
	if _, ok := cache.toCommit[key]; ok {
		cache.commitMu.Unlock()
		return nil, nil
	}
	cache.commitMu.Unlock()

	if cache.check(target, blk) {
		return nil, nil
	}

	path := cache.cachePath(key)
	dir := filepath.Dir(path)
	base := filepath.Base(path)

	// tmp file must end with ".tmp" so stale tmp file could be cleaned up by `scanCached`
	tmpfile := fmt.Sprintf("%s_*.tmp", base)
	os.MkdirAll(dir, 0755)
	file, err := os.CreateTemp(dir, tmpfile)
	if err != nil {
		return nil, err
	}
	name := file.Name()

	defer func() {
		if cleanup {
			file.Close()
			if dupFile != nil {
				dupFile.Close()
			}
			os.Remove(name)
		}
	}()

	// duplicate and keep original file open, so we can commit it in `commitBlockFile`
	dupFd, err := syscall.Dup(int(file.Fd()))
	if err != nil {
		cleanup = true
		return nil, err
	}
	dupFile = os.NewFile(uintptr(dupFd), name)

	// check toCommit again, as we released the lock
	now := time.Now()
	cache.commitMu.Lock()
	defer cache.commitMu.Unlock()
	if _, ok := cache.toCommit[key]; ok {
		// someone else prepared this cache file while we released the lock
		cleanup = true
		return nil, nil
	} else {
		cache.toCommit[key] = &pendingBlockFile{now, file, size}
		return dupFile, nil
	}
}

// commit previously prepared cache file
func (cache *diskCache) commitBlockFile(target string, blk uint32, commit bool) (rerr error) {
	key := CacheKey{target, blk}
	cache.commitMu.Lock()
	bf, ok := cache.toCommit[key]
	delete(cache.toCommit, key)
	cache.commitMu.Unlock()
	if !ok {
		logger.Warnf("Cache file for target %s block %d not prepared", target, blk)
		return errors.New("cache file not prepared")
	}

	name := bf.file.Name()
	// Not commit, cache file may not be written correctly, just discard it.
	if !commit {
		bf.file.Close()
		os.Remove(name)
		return nil
	}

	defer func() {
		if bf.file != nil {
			bf.file.Close()
		}
		if rerr != nil {
			os.Remove(name)
		} else {
			rerr = cache.saveMeta(key)
			if rerr != nil {
				logger.Warn(rerr.Error())
			}
		}
	}()

	// make sure file size and occupied space is valid
	var stat unix.Stat_t
	if err := unix.Fstat(int(bf.file.Fd()), &stat); err != nil {
		logger.Warnf("Fstat to-commit cache file for target %s block %d: %s", target, blk, err)
		return err
	}
	if stat.Size != int64(bf.size) {
		logger.Warnf("To-commit cache file %s has unexpected size %d, want %d", name, stat.Size, bf.size)
		return fmt.Errorf("unexpected file size %d, want %d", stat.Size, bf.size)
	}
	if stat.Blocks == 0 {
		logger.Warnf("To-commit cache file %s occupies 0 space", name)
		return fmt.Errorf("to-commit cache file %s has 0 filesystem block", name)
	}

	if err := bf.file.Close(); err != nil {
		logger.Warnf("Close to-commit file %s: %s", name, err)
		return err
	}
	bf.file = nil

	// commit cache by renaming tmp file to final cache file path
	path := cache.cachePath(key)
	if err := os.Rename(name, path); err != nil {
		logger.Warnf("Commit cache file: %s", err)
		return err
	}

	timeUsed := time.Since(bf.created)
	cache.cacheWrites.Add(1)
	cache.cacheWriteBytes.Add(float64(bf.size))
	cache.cacheWriteHist.Observe(timeUsed.Seconds())
	cache.add(key, int32(bf.size), uint32(time.Now().UnixMilli()))
	logger.Debugf("Daemon commited %s block %d in %s", target, blk, timeUsed)
	return nil
}

// Construct cache file absolute path of given key. We use '#' as escape char to make sure different
// keys won't use the same cache file path after filepath.Join().
func (cache *diskCache) cachePath(key CacheKey) string {
	target := escapeTarget(key.Target)
	return filepath.Join(cache.dir, target, strconv.FormatUint(uint64(key.Blk), 10))
}

// Return meta/size file path and its on-disk target of the given cache key.
func (cache *diskCache) metaPath(key CacheKey) (string, string, string) {
	cachePath := cache.cachePath(key)
	cacheDir := filepath.Dir(cachePath)
	onDiskTarget := cacheDir[len(cache.dir):]
	return filepath.Join(cacheDir, metaFileName), filepath.Join(cacheDir, sizeFileName), onDiskTarget
}

func (cache *diskCache) doFlush(key CacheKey, data []byte) {
	if cache.flushPage(key, data) == nil {
		cache.add(key, int32(len(data)), uint32(time.Now().UnixMilli()))
	}
}

// popPending blocks until a block is available, then returns the next block to
// write back, always draining hash-hit blocks before hash-miss blocks.
// Caller must NOT hold cache.Mutex; popPending acquires it internally.
func (cache *diskCache) popPending() pendingCache {
	cache.Lock()
	defer cache.Unlock()
	for len(cache.hashhitpending) == 0 && len(cache.nonhitpending) == 0 {
		cache.pendingCond.Wait()
	}
	if len(cache.hashhitpending) > 0 {
		w := cache.hashhitpending[0]
		cache.hashhitpending[0] = pendingCache{}
		cache.hashhitpending = cache.hashhitpending[1:]
		return w
	}
	w := cache.nonhitpending[0]
	cache.nonhitpending[0] = pendingCache{}
	cache.nonhitpending = cache.nonhitpending[1:]
	return w
}

// flush cached block into disk
func (cache *diskCache) flush() {
	for {
		w := cache.popPending()
		cache.Lock()
		_, ok := cache.pages[w.key]
		cache.Unlock()
		if ok {
			cache.doFlush(w.key, w.page.Data)
			cache.Lock()
			delete(cache.pages, w.key)
			cache.Unlock()
		}
		w.page.Release()
	}
}

func (cache *diskCache) add(key CacheKey, size int32, atime uint32) {
	cache.Lock()
	defer cache.Unlock()
	it, ok := cache.keys[key]
	if ok {
		cache.used -= int64(it.size)
	}
	item := cacheItem{size: size, atime: atime}
	cache.keys[key] = item
	cache.used += int64(size)

	if cache.used > cache.burstCap {
		logger.Debugf("Cleanup cache when add new data (%s): %d blocks (%d MB)", cache.dir, len(cache.keys), cache.used>>20)
		skip := make(map[CacheKey]struct{}, 1)
		skip[key] = struct{}{}
		// Don't cleanup just added cache
		cache.cleanup(skip)
	}
}

// get cleanup capacity and file goals
func (cache *diskCache) getCleanupGoal(isBurst bool) (int64, int) {
	var goal int64
	if isBurst {
		goal = cache.capacity // burst mode: target is capacity
	} else {
		goal = cache.burstCap // normal mode: target is burstCap
	}
	num := len(cache.keys)

	// make sure we have enough free space after cleanup
	br, fr := cache.curFreeRatio()
	if br < cache.freeRatio {
		total, _, _, _ := getDiskUsage(cache.dir)
		toFree := int64(float32(total) * (cache.freeRatio - br))
		if toFree > cache.used {
			goal = 0
		} else if cache.used-toFree < goal {
			goal = cache.used - toFree
		}
	}
	if fr < cache.freeRatio {
		_, _, files, _ := getDiskUsage(cache.dir)
		toFree := int(float32(files) * (cache.freeRatio - fr))
		if toFree > len(cache.keys) {
			num = 0
		} else {
			num = len(cache.keys) - toFree
		}
	}
	return goal, num
}

// lockTarget adds one eviction-protection reference for every supplied block.
// A zero-length block list adds one reference that protects every block of target.
func (cache *diskCache) lockTarget(target string, blocks []uint32) {
	cache.Lock()
	defer cache.Unlock()
	locked := cache.lockedBlocks[target]
	if locked == nil {
		locked = make(map[int]int)
		cache.lockedBlocks[target] = locked
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
func (cache *diskCache) unlockTarget(target string, blocks []uint32) {
	cache.Lock()
	defer cache.Unlock()
	locked := cache.lockedBlocks[target]
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
		delete(cache.lockedBlocks, target)
	}
}

// isLocked reports whether a target-wide or per-block reference protects key.
// Caller must hold the cache mutex.
func (cache *diskCache) isLocked(key CacheKey) bool {
	locked := cache.lockedBlocks[key.Target]
	return locked != nil && (locked[-1] > 0 || locked[int(key.Blk)] > 0)
}

func (cache *diskCache) evictCacheItem(ectx *cacheEvictionContext, key CacheKey, size int64, evictionType string) {
	if cache.isLocked(key) {
		// explicit removal of protected block
		cache.evictsLocked.Add(1)
	}
	ectx.freed += size
	ectx.todel = append(ectx.todel, key)
	cache.used -= size
	isHashHit := cache.store.hashHit(key.String())

	if cache.store.pinCache.isPinned(key.Target) {
		cache.pinEvicts.Add(1)
		cache.pinEvictBytes.Add(float64(size))
		if isHashHit {
			cache.pinEvictsHashHit.Add(1)
		}
	}

	cache.cacheEvicts.Add(1)
	if isHashHit {
		cache.cacheEvictsHashHit.Add(1)
	} else {
		cache.cacheEvictsHashMiss.Add(1)
	}
	switch evictionType {
	case evictionCleanup:
		cache.cacheCleanup.Add(1)
		if isHashHit {
			cache.cacheCleanupHashHit.Add(1)
		} else {
			cache.cacheCleanupHashMiss.Add(1)
		}
	case evictionExpire:
		cache.cacheExpired.Add(1)
		if isHashHit {
			cache.cacheExpiredHashHit.Add(1)
		} else {
			cache.cacheExpiredHashMiss.Add(1)
		}
	case evictionRemoveByPrefix:
		cache.cacheRemoveByPrefix.Add(1)
		if isHashHit {
			cache.cacheRemoveByPrefixHashHit.Add(1)
		} else {
			cache.cacheRemoveByPrefixHashMiss.Add(1)
		}
	case evictionClearBurst:
		cache.burstEvicts.Add(1)
		cache.burstEvictBytes.Add(float64(size))
	}

	if ti, ok := cache.targets[key.Target]; ok {
		delete(cache.targets, key.Target)
		ectx.delTargets[key.Target] = ti
	}

	if evictionType == evictionRemoveByPrefix {
		if len(ectx.prefix) == 0 {
			logger.Debugf("remove %s by keep prefixes", key.String())
		} else {
			logger.Debugf("remove %s by prefix %s", key.String(), ectx.prefix)
		}
	} else {
		logger.Debugf("remove %s from cache, age: %dms", key.String(), ectx.age)
	}
}

func (cache *diskCache) evictPageCacheItem(ectx *cacheEvictionContext, key CacheKey, size int64, evictionType string) {
	delete(cache.pages, key)
	cache.evictCacheItem(ectx, key, size, evictionType)
}

func (cache *diskCache) evictKeyCacheItem(ectx *cacheEvictionContext, key CacheKey, size int64, evictionType string) {
	delete(cache.keys, key)
	cache.evictCacheItem(ectx, key, size, evictionType)
}

// locked
func (cache *diskCache) cleanup(skipKeys map[CacheKey]struct{}) {
	cache.doCleanup(false, evictionCleanup, skipKeys)
}

// locked
func (cache *diskCache) doCleanup(isBurst bool, evictionType string, skipKeys map[CacheKey]struct{}) (int64, int64) {
	start := time.Now()
	var now = uint32(start.UnixMilli())
	goal, num := cache.getCleanupGoal(isBurst)
	ectx := newCacheEvictionContext()

	shouldStop := func() bool {
		return len(cache.keys) < num && cache.used <= goal
	}

	shouldSkip := func(k CacheKey, hash, pin bool) bool {
		// Hard-skip protected blocks in ALL rounds including R5. Placed first so it
		// short-circuits before hashHit: when protected bytes reach the cap, every
		// insert's inline cleanup scans the whole map and almost every entry is protected;
		// without this ordering each visited entry would allocate a string in
		// hashHit(k.String()), creating heavy GC pressure.
		if cache.isLocked(k) {
			cache.evictsSkipLocked.Add(1)
			return true
		}

		// Skip all specified to-skip targets
		if _, ok := skipKeys[k]; ok {
			logger.Tracef("Skip cleanup cache key %s", k.String())
			return true
		}
		if ti, ok := cache.targets[k.Target]; ok {
			// Don't cleanup not completed target, which might be in the middle of Set
			if !ti.complete {
				return true
			}
		}
		// Skip cache hashed to local node if hash is enabled
		if hash && cache.store.hashHit(k.String()) {
			return true
		}
		if pin && cache.store.pinCache.isPinned(k.Target) {
			return true
		}
		return false
	}

	compareAndCleanup := func(compare, hash, pin bool) {
		cache.targetMu.Lock()
		defer cache.targetMu.Unlock()
	RETRY:
		var cnt int
		var lastKey CacheKey
		var lastValue cacheItem
		if compare {
			cnt = 0
		} else {
			cnt = 2
		}
		lastEvicted := len(ectx.todel) // track progress for this pass
		for key, value := range cache.keys {
			if shouldSkip(key, hash, pin) {
				logger.Tracef("Skip cleanup cache key %s", key.String())
				continue
			}
			if compare {
				if cnt == 0 || lastValue.atime > value.atime {
					lastKey = key
					lastValue = value
				}
				cnt++
			} else {
				lastKey = key
				lastValue = value
			}
			if cnt > 1 {
				ectx.age = now - lastValue.atime
				cache.evictKeyCacheItem(ectx, lastKey, int64(lastValue.size), evictionType)
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
		//   - only RETRY if >= 2 keys remain to form a comparison pair.
		if compare && !shouldStop() && len(ectx.todel) > lastEvicted && len(cache.keys) >= 2 {
			goto RETRY
		}
	}

	shouldContinue := func() bool {
		return len(ectx.todel) == 0 || !shouldStop()
	}
	hasPins := cache.store.pinCache.hasPins()

	// Hash based eviction is only useful when there're multiple nodes in cluster
	if cache.store.config.HashringEnabled() && cache.store.serf.GetMemberCount() > 1 {
		// R1: 2-random evict non-hash-hit, skip pinned if hasPins
		compareAndCleanup(true, true, hasPins)

		// R2: direct evict non-hash-hit, skip pinned if hasPins
		if shouldContinue() {
			compareAndCleanup(false, true, hasPins)
		}

		// R3: 2-random evict non-hash-hit including pinned (only needed when hasPins)
		if hasPins && shouldContinue() {
			compareAndCleanup(true, true, false)
		}
	}

	// R4: 2-random evict hash-hit but non-pinned
	if hasPins && shouldContinue() {
		compareAndCleanup(true, false, true)
	}

	// R5: last resort, evict all including pinned
	if shouldContinue() {
		compareAndCleanup(true, false, false)
	}

	delCnt := len(ectx.todel)
	if delCnt > 0 {
		logger.Debugf("cleanup cache (%s): remaining %d blocks (%d MB), freed %d blocks (%d MB), %d targets in %s",
			cache.dir, len(cache.keys), cache.used>>20, delCnt, ectx.freed>>20, len(ectx.delTargets), time.Since(start))
	}
	cache.Unlock()
	cache.revokeDel(ectx)
	cache.Lock()

	return int64(delCnt), ectx.freed
}

func (cache *diskCache) scanCached() {
	cache.Lock()
	cache.used = 0
	cache.tmpKeys = cache.keys
	cache.keys = make(map[CacheKey]cacheItem)
	cache.scanned = false
	cache.Unlock()

	type targetSize struct {
		current uint64
		total   uint64
	}

	var start = time.Now()
	var oneMinAgo = start.Add(-time.Minute)
	targetMap := make(map[string]string)
	sizeMap := make(map[string]*targetSize)
	staleMetaFiles := make(map[string]struct{})
	targetCnt := 0

	logger.Debugf("Scan %s to find cached blocks", cache.dir)
	_ = filepath.Walk(cache.dir, func(path string, fi os.FileInfo, err error) error {
		if fi == nil {
			return nil
		}
		if fi.IsDir() || strings.HasSuffix(path, ".tmp") {
			if path != cache.dir && fi.ModTime().Before(oneMinAgo) {
				// try to remove empty directory and stale tmp file
				if os.Remove(path) == nil {
					logger.Debugf("Remove stale dir or file: %s", path)
				}
			}
		} else {
			var key CacheKey
			var err error

			// Load and save cache real target if there's meta file
			onDiskPath := path[len(cache.dir):]
			onDiskTarget := filepath.Dir(onDiskPath)
			if _, ok := targetMap[onDiskTarget]; !ok {
				metaFilePath := filepath.Join(filepath.Dir(path), metaFileName)
				if _, err := os.Stat(metaFilePath); err == nil {
					// There's a meta file, load it for real target
					target, err := cache.loadMeta(metaFilePath)
					if err != nil {
						logger.Warn(err.Error())
						return nil
					}
					targetMap[onDiskTarget] = target
					staleMetaFiles[onDiskTarget] = struct{}{}
				}
			}

			// Skip meta and size file
			name := filepath.Base(path)
			if name == metaFileName || name == sizeFileName {
				return nil
			}

			// Parse cache key and update target according to saved meta map
			key, err = parseCacheKey(onDiskPath)
			if err != nil {
				logger.Warnf("Scanned invalid cache path %s: %s", path, err)
				return nil
			}
			if target, ok := targetMap[onDiskTarget]; ok {
				key.Target = target
			}

			// There're cache files except meta file, remove it from stale meta file map
			if _, ok := staleMetaFiles[onDiskTarget]; ok {
				delete(staleMetaFiles, onDiskTarget)
			}

			// Load and save target length if there's size file
			if ts, ok := sizeMap[key.Target]; !ok {
				sizeFilePath := filepath.Join(filepath.Dir(path), sizeFileName)
				if _, err := os.Stat(sizeFilePath); err == nil {
					length, err := cache.loadSize(sizeFilePath)
					if err != nil {
						logger.Warn(err.Error())
					} else {
						ts := &targetSize{
							current: uint64(fi.Size()),
							total:   length,
						}
						sizeMap[key.Target] = ts
						if err := cache.prepareFile(key.Target, length); err != nil {
							logger.Infof("Prepare target %s during scan: %s", key.Target, err)
						}
					}
				}
			} else {
				ts.current += uint64(fi.Size())
				if ts.current == ts.total {
					if err := cache.commitFile(key.Target); err != nil {
						// If target size is larger than cache capacity,
						// prepare may fail, and commit here may fail as
						// well.
						logger.Infof("Commit target %s during scan: %s", key.Target, err)
					} else {
						targetCnt++
					}
				}
			}

			// Add and announce cache, but skip announce previously cached blocks
			atime := uint32(getAtime(fi).UnixMilli())
			cache.add(key, int32(fi.Size()), atime)
			// tmpKeys are read-only, no need to lock
			if _, ok := cache.tmpKeys[key]; !ok {
				cache.store.rcache.announce(key.Target, 0, key.Blk)
			}
		}
		return nil
	})

	// Remove all stale meta files
	for onDiskTarget, _ := range staleMetaFiles {
		metaFilePath := filepath.Join(cache.dir, onDiskTarget, metaFileName)
		sizeFilePath := filepath.Join(cache.dir, onDiskTarget, sizeFileName)
		if err := os.Remove(metaFilePath); err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				logger.Warn(err.Error())
			}
		}
		_ = os.Remove(sizeFilePath)
		logger.Debugf("Remove stale meta file %s", metaFilePath)
	}

	cache.Lock()
	cache.scanned = true
	cache.tmpKeys = nil
	logger.Infof("Found %d cached blocks (%d bytes), %d targets, in %s with %s", len(cache.keys), cache.used, targetCnt, cache.dir, time.Since(start))
	cache.Unlock()
}

func (cache *diskCache) dump() *metaInfo {
	tms := make(map[string]*targetMeta)
	skipTargets := make(map[string]struct{})
	cache.Lock()
	cache.targetMu.Lock()
	defer cache.targetMu.Unlock()
	defer cache.Unlock()

	for target, titem := range cache.targets {
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

	keys := cache.keys
	if keys == nil || len(keys) == 0 {
		keys = cache.tmpKeys
	}
	addTargets := func(k CacheKey) {
		// Skip already processed targets
		if _, ok := skipTargets[k.Target]; ok {
			return
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
	for k := range keys {
		addTargets(k)
	}
	for k := range cache.pages {
		addTargets(k)
	}
	if len(tms) > 0 {
		return &metaInfo{Targets: tms}
	} else {
		return nil
	}
}

func escapeTarget(target string) string {
	// Escape all original '#' to "##", all '.' to "#dot"
	target = strings.ReplaceAll(target, "#", "##")

	// Make sure keys are different after filepath.Join(), i.e. we dont' want "/../", "/./" etc.
	target = strings.ReplaceAll(target, "/./", "/#dot/")
	target = strings.ReplaceAll(target, "/../", "/#dot#dot/")
	target = strings.ReplaceAll(target, "//", "#slash#slash")
	if strings.HasSuffix(target, "/..") {
		target = replaceLast(target, "/..", "/#dot#dot")
	}
	if strings.HasSuffix(target, "/.") {
		target = replaceLast(target, "/.", "/#dot")
	}
	if strings.HasSuffix(target, "/") {
		target = replaceLast(target, "/", "#slash")
	}

	// Make sure all keys are staring with "/"
	if !strings.HasPrefix(target, "/") {
		target = "/#slash" + target
	}

	// Make sure there's no \0 in path
	target = strings.ReplaceAll(target, string('\000'), "0")
	return target
}

func replaceLast(original string, old string, new string) string {
	// Find last index of `old`
	index := strings.LastIndex(original, old)
	if index == -1 {
		// Not found, return original
		return original
	}

	// Split and join new string
	return original[:index] + new + original[index+len(old):]
}

func saveFile(final string, data []byte) (rerr error) {
	// tmp file must end with ".tmp" so stale tmp file could be cleaned up by `scanCached`
	name := filepath.Base(final)
	dir := filepath.Dir(final)
	tmp := fmt.Sprintf("%s_*.tmp", name)
	file, err := os.CreateTemp(dir, tmp)
	if err != nil {
		return err
	}
	tmpName := file.Name()
	defer func() {
		if rerr != nil {
			if file != nil {
				file.Close()
			}
			os.Remove(tmpName)
		}
	}()

	n, err := file.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return fmt.Errorf("Partial write on file %s, expect %d written %d", tmpName, len(data), n)
	}
	file.Close()
	file = nil
	if err := os.Rename(tmpName, final); err != nil {
		return err
	}
	return nil
}

// clearBurst evicts cache items to bring usage down to capacity (cache-size).
// Returns the number of evicted blocks and bytes.
func (cache *diskCache) clearBurst() (int64, int64) {
	cache.Lock()
	defer cache.Unlock()
	if cache.used <= cache.capacity {
		return 0, 0
	}

	start := time.Now()
	totalCnt, totalBytes := cache.doCleanup(true, evictionClearBurst, nil)

	if totalCnt > 0 {
		logger.Infof("Clear burst (%s): evicted %d blocks (%d MB) in %s, remaining %d blocks (%d MB)",
			cache.dir, totalCnt, totalBytes>>20, time.Since(start), len(cache.keys), cache.used>>20)
	}
	return totalCnt, totalBytes
}

// setCapacity updates cache capacity and burst cache capacity at runtime.
// CacheSizeKeepCurrent means "keep current value"; BurstCacheSizeRestore
// restores the startup-configured burst cap.
// burstCacheSize is always kept >= cacheSize after update.
// Returns new cache size, new burst size, evicted block count and bytes.
func (cache *diskCache) setCapacity(cacheSize, burstCacheSize int64) (int64, int64, int64, int64) {
	cache.Lock()
	oldCacheSize := cache.capacity
	oldBurstCap := cache.burstCap

	// Resolve sentinels before normalization
	if cacheSize == CacheSizeKeepCurrent {
		cacheSize = oldCacheSize
	}
	if burstCacheSize == BurstCacheSizeRestore {
		initial := cache.store.config.InitialBurstSize
		logger.Infof("Disk cache (%s): restoring burst cache to startup-configured %dMB", cache.dir, initial>>20)
		burstCacheSize = initial
	} else if burstCacheSize == CacheSizeKeepCurrent {
		burstCacheSize = oldBurstCap
	}
	// Normalize: burst must be >= cache
	if burstCacheSize < cacheSize {
		burstCacheSize = cacheSize
	}
	cache.capacity = cacheSize
	cache.burstCap = burstCacheSize

	// Use evictionClearBurst when base capacity is unchanged and burst is being reduced.
	evictType := evictionCleanup
	if cacheSize == oldCacheSize && burstCacheSize < oldBurstCap {
		evictType = evictionClearBurst
	}

	// Sync effective values to config atomically so concurrent readers via GetCacheSize see
	// the updated capacity.
	atomic.StoreInt64(&cache.store.config.CacheSize, cacheSize)
	atomic.StoreInt64(&cache.store.config.BurstCacheSize, burstCacheSize)
	logger.Infof("Disk cache (%s): capacity %dMB -> %dMB, burstCap %dMB -> %dMB",
		cache.dir, oldCacheSize>>20, cacheSize>>20, oldBurstCap>>20, burstCacheSize>>20)
	var evictedCnt, evictedBytes int64
	if cache.used > cache.burstCap {
		// doCleanup(isBurst=false) targets burstCap; call directly to capture evict stats
		evictedCnt, evictedBytes = cache.doCleanup(false, evictType, nil)
	}
	cache.Unlock()
	return cacheSize, burstCacheSize, evictedCnt, evictedBytes
}
