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
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aliyun/cachefs/pkg/chunk"
	"github.com/aliyun/cachefs/pkg/meta"
	cachefsmeta "github.com/aliyun/cachefs/pkg/meta/cachefs"
	"github.com/aliyun/cachefs/pkg/metric"
	"github.com/aliyun/cachefs/pkg/srcmgr"
	"github.com/aliyun/cachefs/pkg/utils"
)

var (
	maxEntrySize = 20

	// sourceDirFS builds the fs.FS for walking a source dir replica. It is a variable so tests
	// can substitute an error-injecting filesystem.
	sourceDirFS = os.DirFS
)

func init() {
	if s := os.Getenv("CFS_PRELOAD_ENTRY_SIZE"); s != "" {
		const maxAllowed = 1000
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			maxEntrySize = v
			if maxEntrySize > maxAllowed {
				maxEntrySize = maxAllowed
				logger.Warnf("Max allowed CFS_PRELOAD_ENTRY_SIZE %d, reset %d to %d",
					maxAllowed, v, maxAllowed)
			}
			logger.Infof("Set preload max entry size to %d via CFS_PRELOAD_ENTRY_SIZE", maxEntrySize)
		} else {
			logger.Warnf("Invalid CFS_PRELOAD_ENTRY_SIZE %q, using default %d",
				s, maxEntrySize)
		}
	}
}

type fileInfo struct {
	ino   Ino
	path  string
	size  uint64
	mtime syscall.Timespec
}

type loadReq struct {
	ctx        context.Context
	paths      []string
	count      *uint64 // number of loaded files
	bytes      *uint64 // size of loaded data
	stopOnFull bool    // stop loading if cache is full, only affects hash hit blocks
	loadAll    bool    // load all data even if cache is full
	force      bool    // don't skip load if prefetch channel is full
	cancel     bool    // cancel the current preload progress
	done       chan struct{}
}

func (r *loadReq) String() string {
	return fmt.Sprintf("paths: \"%s\"", strings.Join(r.paths, " "))
}

type preloadState struct {
	sync.Mutex
	fileCnt      atomic.Int32
	blockCnt     atomic.Int32
	fileScanned  atomic.Int32
	blockScanned atomic.Int32
	total        atomic.Int64
	stopped      bool
	cancel       atomic.Bool

	// staging dir entries, used to randomize entries
	stageEntries map[string]*fileInfo
	fileSet      map[uint64]struct{}

	// record already loaded blocks
	loadedMu     sync.Mutex
	loadedBlocks map[chunk.CacheKey]struct{}
	loadedFiles  map[string]struct{}

	useSource bool
	fileCh    chan *fileInfo
	stopCh    chan struct{}
	loadCh    chan *loadReq
	loadWg    sync.WaitGroup

	// Per round stop signal, armed by resetStopWalk before every round and
	// closed by the first worker that decides the round should stop.
	stopWalkCh   chan struct{}
	stopWalkOnce sync.Once

	initReq *loadReq
	curReq  *loadReq

	blkTokens   *utils.Token
	shouldLoad  func(target string, blk uint32) bool
	stopWalkDir func() (bool, string)

	// for cache full detection
	cache                cacheReader
	blockSize            uint32
	cleanupEvictCounter  *metric.AtomicCounter
	cleanupEvictBaseline float64
}

// Flush all staging dir entries to fileCh randomly
func (s *preloadState) flushEntries() {
	for path, fi := range s.stageEntries {
		delete(s.stageEntries, path)
		s.loadWg.Add(1)
		s.fileCh <- fi
	}
}

// Stage dir entries locally, then flush them out randomly when staging area is full, so we could
// preload files in random order on different instances.
func (s *preloadState) pushEntry(fi *fileInfo) {
	s.stageEntries[fi.path] = fi
	if len(s.stageEntries) == maxEntrySize {
		s.flushEntries()
	}
}

func (s *preloadState) stop() {
	s.Lock()
	if !s.stopped {
		close(s.stopCh)
		s.stopped = true
	}
	s.Unlock()
}

// resetStopWalk arms the stop signal for a new preload round. It is called from
// the preload loop between rounds, when no worker is running.
func (s *preloadState) resetStopWalk() {
	s.stopWalkCh = make(chan struct{})
	s.stopWalkOnce = sync.Once{}
}

// stopWalking logs why the current preload round stopped and aborts WalkDir if
// it is still walking. Workers call it concurrently, the first reason wins and
// gets logged once, whether or not a walk is left to notice the signal.
func (s *preloadState) stopWalking(reason string) {
	s.stopWalkOnce.Do(func() {
		logger.Infof("Preload stop walking dir: %s", reason)
		close(s.stopWalkCh)
	})
}

func (s *preloadState) isCacheFull() bool {
	// Use burst cache size as the effective capacity upper limit; in burst mode it is larger
	// than the base cache size and represents the actual maximum usable cache space.
	_, burstCacheSize := s.cache.getCacheSize()

	// It's not likely to be 100% full due to cache eviction, so give it a block size margin.
	capacityFull := s.cache.size() >= (burstCacheSize - int64(s.blockSize))

	// Detect space pressure via cleanup eviction growth since the start of this preload cycle.
	// This catches cases where free-space-ratio causes eviction before the configured capacity
	// is reached.
	evicting := s.cleanupEvictCounter != nil && s.cleanupEvictCounter.Load() > s.cleanupEvictBaseline
	return capacityFull || evicting
}

// stopWalkDir predicate: stop the current preload round once the cache is full.
func (s *preloadState) stopOnFull() (bool, string) {
	if s.isCacheFull() {
		return true, "cache already full"
	}
	return false, ""
}

// stopOnEviction returns a stopWalkDir predicate that stops the current preload
// round when the cache is full AND the given per-priority eviction counter has
// grown since the round started, i.e. we are evicting equal-priority blocks and
// continuing would only churn the cache with no net benefit.
//
// If the counter can't be read (e.g. daemon mode where store counters aren't
// accessible) eviction can't be detected, so we conservatively fall back to
// stopOnFull instead of degenerating to never-stop and churning indefinitely.
func (s *preloadState) stopOnEviction(counterName, reason string) func() (bool, string) {
	counter, err := s.cache.getCounter(counterName)
	if err != nil {
		logger.Warnf("Eviction counter %s unavailable, fall back to stopOnFull: %s",
			counterName, err)
		return s.stopOnFull
	}
	before := counter.Load()
	return func() (bool, string) {
		if s.isCacheFull() && counter.Load() > before {
			return true, reason
		}
		return false, ""
	}
}

// Bypass hash-based multi-round loading and load all blocks directly in one pass or not.
func (c *Cachefs) shouldSkipHashRing(req *loadReq) bool {
	return req.loadAll || !c.Conf.Store.HashringEnabled() || c.serf == nil || c.serf.GetMemberCount() <= 1
}

func (c *Cachefs) preloadWorker(id int) {
	for {
		select {
		case <-c.preloadState.stopCh:
			logger.Infof("Stop preload worker %d", id)
			return
		case info := <-c.preloadState.fileCh:
			c.preloadFile(info)
		}
	}
}

// file has been validated, i.e. non-zero length regular file
func (c *Cachefs) preloadFile(fi *fileInfo) {
	state := c.preloadState
	defer state.loadWg.Done()
	state.fileScanned.Add(1)
	path := filepath.Join("/", fi.path)
	logger.Debugf("Preload processing path %s", path)

	var target, srcTarget string
	mtime, mtimensec := fi.mtime.Unix()
	nsec := uint32(mtimensec)
	if state.useSource {
		// Preload via source dir, encode cache key with file size and mtime
		target = c.encodeCacheTarget(c.Meta.GetPath(path), fi.size, mtime, nsec)
		srcTarget = path
	} else {
		cino, eno := c.Meta.GetInode(fi.ino)
		if eno != 0 {
			logger.Warnf("Get inode %d for %s: %s, skip preload", fi.ino, path, eno)
			return
		} else {
			target = c.encodeCacheTarget(cino.GetPath(), fi.size, mtime, nsec)
			srcTarget = cino.GetPath()
		}
	}

	bs := c.Conf.Store.BlockSize
	blkcnt := uint32((fi.size + uint64(bs-1)) / uint64(bs))
	hit := false
	wg := sync.WaitGroup{}
	for i := uint32(0); i < blkcnt; i++ {
		state.blockScanned.Add(1)
		if state.cancel.Load() {
			state.stopWalking("preload cancelled")
			return
		}
		if stop, reason := state.stopWalkDir(); stop {
			state.stopWalking(reason)
			return
		}
		if !state.shouldLoad(target, i) {
			continue
		}

		// Acquire preload token
		state.blkTokens.Get()
		wg.Add(1)
		go func(blk uint32) {
			req := prefetchReqPool.Get().(*prefetchReq)
			req.target = target
			req.srcTarget = srcTarget
			req.length = fi.size
			req.block = blk
			req.cache = false
			req.doneCh = make(chan struct{})
			req.ctx = meta.NewContext(0, 0, []uint32{0})
			if state.curReq != nil && state.curReq.force {
				c.prefetchCh <- req
			} else {
				select {
				case c.prefetchCh <- req:
				default:
					// Prefetch queue is full, release preload token and return
					wg.Done()
					state.blkTokens.Put()
					return
				}
			}
			// Wait for block to be preloaded
			<-req.doneCh
			state.blockCnt.Add(1)
			blkSize := utils.GetBlockSize(bs, fi.size, blk)
			state.total.Add(int64(blkSize))
			if state.curReq != nil {
				if state.curReq.bytes != nil {
					atomic.AddUint64(state.curReq.bytes, blkSize)
				}
			}
			hit = true
			wg.Done()
			// Release preload token
			state.blkTokens.Put()
		}(i)
	}
	wg.Wait()
	if hit {
		state.loadedMu.Lock()
		_, seen := state.loadedFiles[target]
		if !seen {
			c.preloadState.fileCnt.Add(1)
			state.loadedFiles[target] = struct{}{}
		}
		state.loadedMu.Unlock()
		if state.curReq != nil {
			if state.curReq.count != nil && !seen {
				atomic.AddUint64(state.curReq.count, 1)
			}
		}
	}
}

// Trigger reload now, no need to wait for next timer trigger.
func (c *Cachefs) loadCache(req *loadReq) syscall.Errno {
	if c.preloadState == nil {
		return syscall.EAGAIN
	}
	if c.preloadState.stopped {
		return syscall.ENODEV
	}

	state := c.preloadState
	// Handle cancel request first
	if req != nil && req.cancel {
		if state.curReq == nil {
			logger.Info("No preload in progress, cancel request ignored")
			return 0
		}
		state.cancel.Store(true)
		logger.Info("Current preload cancelled by request")
		return 0
	}

	if req == nil || len(req.paths) == 0 {
		req = state.initReq
	}
	if req == nil {
		return syscall.ENODATA
	}
	select {
	case state.loadCh <- req:
	default:
		return syscall.EINPROGRESS
	}
	return 0
}

func (c *Cachefs) loadLoop(doLoad func(paths []string)) {
	state := c.preloadState

	neverStop := func() (bool, string) { return false, "" }
	stopOnFull := state.stopOnFull
	stopOnEviction := state.stopOnEviction
	isPinned := func(target string) bool {
		return c.cache.isPinned(target)
	}

	type roundSnapshot struct {
		files  int32
		blocks int32
		bytes  int64
		start  time.Time
	}

	handleLoad := func(req *loadReq) {
		logger.Debugf("Handle load request: %#v", req)
		state.curReq = req
		state.cancel.Store(false)
		defer func() {
			state.curReq = nil
		}()

		// Reset all counters and state at the start of each preload cycle
		state.fileCnt.Store(0)
		state.blockCnt.Store(0)
		state.fileScanned.Store(0)
		state.blockScanned.Store(0)
		state.total.Store(0)
		state.fileSet = make(map[uint64]struct{})
		state.loadedBlocks = make(map[chunk.CacheKey]struct{})
		state.loadedFiles = make(map[string]struct{})

		// Snapshot cleanup eviction counter for isCacheFull() eviction detection
		if counter, err := c.cache.getCounter(chunk.CounterEvictsCleanup); err == nil {
			state.cleanupEvictCounter = counter
			state.cleanupEvictBaseline = counter.Load()
		} else {
			state.cleanupEvictCounter = nil
		}

		// Collect metrics before preload; cache the *AtomicCounter pointers so
		// the after-section can call .Load() directly without a second map lookup.
		srcReadsC, err1 := c.counterReg.GetCounter(counterSourceReads)
		srcBytesC, err2 := c.counterReg.GetCounter(counterSourceReadBytes)
		remoteHitsC, err3 := c.cache.getCounter(chunk.CounterRemoteHits)
		remoteHitBytesC, err4 := c.cache.getCounter(chunk.CounterRemoteHitBytes)
		var srcReadsBefore, srcBytesBefore, remoteHitsBefore, remoteHitBytesBefore float64
		if err1 == nil {
			srcReadsBefore = srcReadsC.Load()
		}
		if err2 == nil {
			srcBytesBefore = srcBytesC.Load()
		}
		if err3 == nil {
			remoteHitsBefore = remoteHitsC.Load()
		}
		if err4 == nil {
			remoteHitBytesBefore = remoteHitBytesC.Load()
		}
		var metricsErr error
		for _, e := range []error{err1, err2, err3, err4} {
			if e != nil {
				metricsErr = e
				break
			}
		}
		metricsOk := metricsErr == nil

		skipHashRing := c.shouldSkipHashRing(req)
		hasPins := c.cache.hasPins()

		recordLoaded := func(target string, blk uint32) {
			key := chunk.CacheKey{Target: target, Blk: blk}
			state.loadedMu.Lock()
			state.loadedBlocks[key] = struct{}{}
			state.loadedMu.Unlock()
		}
		alreadyLoaded := func(target string, blk uint32) bool {
			key := chunk.CacheKey{Target: target, Blk: blk}
			state.loadedMu.Lock()
			defer state.loadedMu.Unlock()
			_, ok := state.loadedBlocks[key]
			return ok
		}

		start := time.Now()

		// Snapshot counters before a round to compute per-round deltas
		snapshot := func() roundSnapshot {
			return roundSnapshot{
				files:  state.fileCnt.Load(),
				blocks: state.blockCnt.Load(),
				bytes:  state.total.Load(),
				start:  time.Now(),
			}
		}
		// runRound walks all requested paths once with the currently configured
		// shouldLoad and stopWalkDir predicates. Every round gets a fresh stop
		// signal, so a round that stopped early never leaves the following ones
		// unable to stop, and every round can report its own stop reason.
		runRound := func(desc string) {
			state.resetStopWalk()
			before := snapshot()
			doLoad(req.paths)
			files := state.fileCnt.Load() - before.files
			blocks := state.blockCnt.Load() - before.blocks
			bytes := state.total.Load() - before.bytes
			size := utils.FormatBytes(uint64(bytes))
			logger.Infof("Preload %s: scanned %s from %d blocks in %d files in %s",
				desc, size, blocks, files, time.Since(before.start))
		}

		if skipHashRing {
			// User wants to load all data to local, no need to check pinned or not
			if req.loadAll {
				state.shouldLoad = func(target string, blk uint32) bool {
					return true
				}
				state.stopWalkDir = neverStop
				runRound("all")
			} else {
				// No hash ring and no load all: pin splits into 2 rounds (pinned -> all),
				// no pin degrades to 1 round.
				if hasPins {
					state.shouldLoad = func(target string, blk uint32) bool {
						if isPinned(target) {
							recordLoaded(target, blk)
							return true
						}
						return false
					}
					if req.stopOnFull {
						state.stopWalkDir = stopOnFull
					} else {
						// Stop if cache is full and pinned blocks are being
						// evicted, as continuing preload would only swap
						// equal-priority blocks with no benefit.
						state.stopWalkDir = stopOnEviction(chunk.CounterEvictsPinned, "pinned blocks being evicted")
					}
					runRound("pinned")
				}

				if !state.isCacheFull() || !hasPins {
					state.shouldLoad = func(target string, blk uint32) bool {
						return !alreadyLoaded(target, blk)
					}
					state.stopWalkDir = stopOnFull
					runRound("remaining")
				}
			}
		} else {
			// Hash ring enabled: pin splits into 4 rounds, no pin degrades to original 2 rounds.

			// Round 1: pinned + hash-hit (skip if no pins)
			if hasPins {
				state.shouldLoad = func(target string, blk uint32) bool {
					if c.cache.isHashHit(target, blk) && isPinned(target) {
						recordLoaded(target, blk)
						return true
					}
					return false
				}
				if req.stopOnFull {
					state.stopWalkDir = stopOnFull
				} else {
					// Stop if cache is full and pinned hash-hit blocks are
					// being evicted.
					state.stopWalkDir = stopOnEviction(chunk.CounterEvictsPinnedHashHit, "pinned hash-hit blocks being evicted")
				}
				runRound("pinned hash-hit")
			}

			// Round 2: hash-hit (no pin degrades to original Round 1: all hash-hit)
			state.shouldLoad = func(target string, blk uint32) bool {
				if c.cache.isHashHit(target, blk) && !alreadyLoaded(target, blk) {
					recordLoaded(target, blk)
					return true
				}
				return false
			}
			if req.stopOnFull {
				state.stopWalkDir = stopOnFull
			} else {
				// Stop if cache is full and hash-hit blocks are being evicted.
				state.stopWalkDir = stopOnEviction(chunk.CounterEvictsHashHit, "non-pinned hash-hit blocks being evicted")
			}
			runRound("hash-hit")

			// Round 3: pinned + non-hash-hit (skip if no pins)
			if hasPins && !state.isCacheFull() {
				state.shouldLoad = func(target string, blk uint32) bool {
					if isPinned(target) && !alreadyLoaded(target, blk) {
						recordLoaded(target, blk)
						return true
					}
					return false
				}
				state.stopWalkDir = stopOnFull
				runRound("pinned non-hash-hit")
			}

			// Round 4: non-hash-hit (no pin degrades to original Round 2: all non-hash-hit)
			if !state.isCacheFull() {
				state.shouldLoad = func(target string, blk uint32) bool {
					return !alreadyLoaded(target, blk)
				}
				state.stopWalkDir = stopOnFull
				runRound("remaining")
			}
		}

		fileScanned := state.fileScanned.Load()
		blockScanned := state.blockScanned.Load()
		logger.Infof("Preload total: %s from %d/%d files %d/%d blocks in %s",
			utils.FormatBytes(uint64(state.total.Load())), state.fileCnt.Load(), fileScanned,
			state.blockCnt.Load(), blockScanned, time.Since(start))

		// Notify preload done in the end to avoid next preload request hitting EINPROGRESS,
		// as we only could accept next request till this function returns.
		defer func() {
			state.fileSet = nil
			state.loadedBlocks = nil
			state.loadedFiles = nil
			if req.done != nil {
				close(req.done)
			}
		}()

		// Collect metrics after preload and log the delta
		if metricsOk {
			srcBlks := srcReadsC.Load() - srcReadsBefore
			srcBytes := srcBytesC.Load() - srcBytesBefore
			peerBlks := remoteHitsC.Load() - remoteHitsBefore
			peerBytes := remoteHitBytesC.Load() - remoteHitBytesBefore

			srcSize := utils.FormatBytes(uint64(srcBytes))
			peerSize := utils.FormatBytes(uint64(peerBytes))
			logger.Infof("During preload loaded %d blocks %s from source, %d blocks %s from peers",
				int64(srcBlks), srcSize, int64(peerBlks), peerSize)
		} else {
			logger.Warnf("Skip preload metrics reporting: %s", metricsErr)
		}
	}
	for {
		var timerCh <-chan time.Time
		if c.Conf.PreloadInterval > 0 {
			timer := time.NewTimer(time.Second * time.Duration(c.Conf.PreloadInterval))
			timerCh = timer.C
		}
		select {
		case <-state.stopCh:
			logger.Info("Stop preload loop")
			return
		case req := <-state.loadCh:
			if req == nil || len(req.paths) == 0 {
				logger.Warn("Preload request has no paths")
				continue
			}
			logger.Infof("Preload %s from source or peer, triggered by reload event", req)
			handleLoad(req)
		case <-timerCh:
			req := state.initReq
			if req == nil || len(req.paths) == 0 {
				continue
			}
			logger.Infof("Preload %s from source or peer, triggered by timer", req)
			handleLoad(req)
		}
	}
}

func (c *Cachefs) preload(req *loadReq) {
	cfg := c.Conf
	state := &preloadState{
		fileCnt:      atomic.Int32{},
		blockCnt:     atomic.Int32{},
		fileScanned:  atomic.Int32{},
		blockScanned: atomic.Int32{},
		total:        atomic.Int64{},
		fileCh:       make(chan *fileInfo, cfg.PreloadWorkers),
		stopCh:       make(chan struct{}),
		loadCh:       make(chan *loadReq),
		initReq:      req,
		loadWg:       sync.WaitGroup{},
		blkTokens:    utils.NewToken(int(cfg.PrefetchWorkers)),
		stageEntries: make(map[string]*fileInfo, maxEntrySize),
		fileSet:      make(map[uint64]struct{}),
		loadedBlocks: make(map[chunk.CacheKey]struct{}),
		loadedFiles:  make(map[string]struct{}),
		cache:        c.cache,
		blockSize:    cfg.Store.BlockSize,
	}
	state.resetStopWalk()
	c.preloadState = state
	sourceFS := c.getSourceFS()
	if req != nil && len(req.paths) > 0 {
		logger.Infof("Initial preload paths: %s", strings.Join(req.paths, " "))
	}

	// Start preload workers
	for i := 0; i < int(cfg.PreloadWorkers); i++ {
		go c.preloadWorker(i)
	}

	statErrs := 0
	walkFunc := func(path string, d fs.DirEntry, err error) error {
		var ctxDone <-chan struct{}
		if state.curReq != nil && state.curReq.ctx != nil {
			ctxDone = state.curReq.ctx.Done()
		}
		select {
		case <-state.stopWalkCh:
			return fs.SkipAll
		case <-ctxDone:
			logger.Info("Preload stopped by context")
			return fs.SkipAll
		default:
		}
		if err != nil {
			if srcmgr.IsInfraError(err) {
				return err
			}
			fullpath := filepath.Join(cfg.Meta.SourceManager.CurrentAliveDir(), path)
			logger.Warnf("Preload hit error %s on path %s, skip", err, fullpath)
			return fs.SkipDir
		}
		if path == "." {
			// Skip current and parent dir entry
			return nil
		}

		info, err := d.Info()
		if err != nil {
			if srcmgr.IsInfraError(err) {
				// Propagate to WalkDir's caller for replica failover.
				return err
			}
			statErrs++
			logger.Warnf("Preload hit stat error on %s, skip: %s", path, err)
			// Abort walk if too many consecutive stat errors, which likely
			// indicates a systemic issue (e.g. NFS stale handles).
			if statErrs >= 10 {
				logger.Errorf("Preload abort: %d consecutive stat errors, last on %s: %s",
					statErrs, path, err)
				return fs.SkipAll
			}
			// For directories, return SkipDir to avoid a futile descent attempt,
			// since ReadDir will likely fail for the same reason as stat.
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		statErrs = 0

		// Only preload regular file
		if !info.Mode().IsRegular() || info.Size() == 0 {
			return nil
		}

		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			logger.Warnf("Can't cast stat of %s to Stat_t, skip", path)
			return nil
		}
		if st.Nlink > 1 {
			// Skip already processed hardlink files
			if _, ok := state.fileSet[st.Ino]; ok {
				return nil
			} else {
				state.fileSet[st.Ino] = struct{}{}
			}
		}
		fi := &fileInfo{
			ino:   Ino(st.Ino),
			path:  path,
			size:  uint64(info.Size()),
			mtime: st.Mtim,
		}
		c.preloadState.pushEntry(fi)
		return nil
	}

	doLoad := func(paths []string) {
		sm := cfg.Meta.SourceManager
		for _, p := range paths {
			p = path.Clean(p)
			if p == "/" {
				p = "."
			}

			// WalkDir uses relative path
			var walkErr error
			p = strings.TrimPrefix(p, "/")
			if state.useSource && sm != nil {
				// Entries pushed before a replica failure are re-pushed on
				// the failover retry and deduplicated by shouldLoad.
				walkErr = sm.RunOp("walk", p, func(dir string) error {
					// The consecutive-stat-error budget restarts with each
					// walk attempt on a fresh replica.
					statErrs = 0
					return fs.WalkDir(sourceDirFS(dir), p, walkFunc)
				})
			} else {
				walkErr = fs.WalkDir(sourceFS, p, walkFunc)
			}
			if walkErr != nil {
				logger.Warnf("Preload walk %s failed: %s", p, walkErr)
			}
		}
		state.flushEntries()
		state.loadWg.Wait()
	}

	// Preload walks cachefs mount point, need to wait for the mount
	c.WaitMount()
	go c.loadLoop(doLoad)

	// Trigger initial preload on startup
	go func() {
		// Give serf a few seconds to join P2P network
		time.Sleep(time.Millisecond * 2500)
		req := state.initReq
		if req != nil && len(req.paths) > 0 {
			state.loadCh <- req
			// For later periodical preloads, scan & load all files for first
			// round consistent-hash hit loading. Sleep to make sure config
			// update is after current preload.
			time.Sleep(time.Second)
			req.stopOnFull = false
		}
	}()
}

// Get actual source dir preload will walk, default to the current alive
// source dir replica, but we need to use Mountpoint in special cases
func (c *Cachefs) getPreloadSource() (string, bool) {
	useSource := true
	sourceDir := c.Conf.Meta.SourceManager.CurrentAliveDir()

	// Source dir subpath is enabled, we should preload via mount point to filter out
	// dirs out of subpath config.
	if c.Conf.Meta.SubpathEnabled() {
		useSource = false
		sourceDir = c.Conf.Meta.MountPoint
	}

	return sourceDir, useSource
}

// getSourceFS decides which fs.FS to use for the preload walk, sets state.useSource accordingly,
// and logs the chosen base dir. In peer meta mode, if the in-memory metadata tree is ready we walk
// it directly and skip the source dir entirely. Otherwise we fall back to os.DirFS over the
// directory returned by getPreloadSource. It also waits for the FUSE mount to complete before
// checking readiness when in peer meta mode.
func (c *Cachefs) getSourceFS() fs.FS {
	walkDir, useSource := c.getPreloadSource()
	if c.Conf.Meta.PeerMeta() {
		if !c.Meta.IsMetaReady() {
			// WaitMount returns once FUSE is up, which happens after Init() has
			// finished (either peer load succeeded or timed out and fell back).
			c.WaitMount()
		}
		if c.Meta.IsMetaReady() {
			// Peer meta is fully loaded; walk the in-memory inode tree directly
			// so we do not need to access the source dir at all.
			c.preloadState.useSource = false
			logger.Infof("Preload base dir: cached filesystem meta")
			return cachefsmeta.NewMetaFS(c.Meta)
		}
	}
	c.preloadState.useSource = useSource
	logger.Infof("Preload base dir: %s", walkDir)
	return os.DirFS(walkDir)
}
