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

package chunk

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aliyun/cachefs/pkg/chunk/common"
	"github.com/aliyun/cachefs/pkg/digest"
	"github.com/aliyun/cachefs/pkg/metric"
	"github.com/aliyun/cachefs/pkg/rpcsvc"
	"github.com/aliyun/cachefs/pkg/serf"
	"github.com/aliyun/cachefs/pkg/transfer"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/willf/bitset"
)

type CleanupMode string

const (
	HashCleanup   CleanupMode = "hash"
	RandomCleanup CleanupMode = "2-random"

	// The SetCacheSize sentinels are defined in pkg/chunk/common so that clients of
	// the cache size API can use them without depending on this package. They are
	// re-exported here because they are part of the chunk layer's vocabulary.
	CacheSizeKeepCurrent  = common.CacheSizeKeepCurrent
	BurstCacheSizeRestore = common.BurstCacheSizeRestore
)

var (
	logger = utils.GetLogger("cachefs")

	ErrNotFound error = errors.New("cache not found")
	ErrExists   error = errors.New("cache already exists")
	ErrTooLarge error = errors.New("cache larger than capacity")
)

type CacheKey struct {
	Target string
	Blk    uint32
}

// CacheState holds single-instance cache aggregates gathered from the in-memory
// index. Byte fields are the actual cached block sizes, not capacity-aligned.
type CacheState struct {
	Capacity      int64
	Used          int64
	HashHitBlocks int64
	HashHitBytes  int64
	CachedBlocks  int64
	CachedBytes   int64
	PinnedBlocks  int64
	PinnedBytes   int64
}

// PeerLoadStats holds this node's peer-load counters, accumulated since process
// start and never reset.
type PeerLoadStats struct {
	Hits           uint64              `json:"hits"`
	HitBytes       uint64              `json:"hit_bytes"`
	Miss           uint64              `json:"miss"`
	MissBytes      uint64              `json:"miss_bytes"`
	Timeouts       uint64              `json:"timeouts"`
	Blacklisted    uint64              `json:"blacklisted"`
	ChecksumErrors uint64              `json:"checksum_errors"`
	InFlight       int64               `json:"in_flight"`
	Latency        metric.HistSnapshot `json:"latency"`
}

func (k *CacheKey) String() string {
	return fmt.Sprintf("%s/%d", k.Target, k.Blk)
}

type targetInfo struct {
	length uint64              // target length
	nodes  map[string]struct{} // nodes that has this target
}

type targetMeta struct {
	Length uint64         // target length
	Blocks *bitset.BitSet // bitset of target blocks, nil means treat target as a whole
}

// describe tracker info
type metaInfo struct {
	SrcAddr   string
	BlockSize uint32
	Targets   map[string]*targetMeta
}

func (tm *targetMeta) String() string {
	if tm.Blocks == nil {
		return fmt.Sprintf("Length: %d, Blocks: nil", tm.Length)
	}
	return fmt.Sprintf("Length: %d, Blocks: %v", tm.Length, tm.Blocks)
}

func (mi *metaInfo) String() string {
	tms := make([]string, 0, len(mi.Targets))
	for key, target := range mi.Targets {
		tms = append(tms, fmt.Sprintf("%s: %s", key, target))
	}
	return fmt.Sprintf("SrcAddr: %s, BlockSize: %d, Targets: {%v}", mi.SrcAddr, mi.BlockSize, tms)
}

// cachefs store
type Store interface {
	CachePinner

	// Load `blk` block of `target`, which has `length` size, from cachefs store.
	Load(ctx context.Context, target string, length uint64, blk uint32) (*Page, error)

	// Return a reader for given `blk` block of `target`
	ReadBlock(ctx context.Context, target string, length uint64, blk uint32) (ChunkReader, error)

	// Add `blk` block of `target` to cachefs store.
	Cache(target string, blk uint32, page *Page)

	// Try fill local cache for 'blk` block of `target` from remote cache.
	Fill(ctx context.Context, target string, length uint64, blk uint32) error

	// Check whether `blk` block of `target` is present in the local cache.
	Check(target string, blk uint32) bool

	// LockTarget prevents eviction of blocks while a client may access their
	// zero-copy descriptors. Cleanup and expiration skip protected blocks in every
	// round (RemoveByPrefix is the exception). A zero-length block list protects
	// every block of target; a non-empty list protects only its listed blocks.
	// Protection is refcounted, so each call must be paired with one matching
	// UnlockTarget.
	LockTarget(target string, blocks []uint32)

	// UnlockTarget drops one matching protection reference. Its block list must
	// select the same blocks as the corresponding LockTarget call; nil and empty
	// lists are equivalent whole-target selections.
	UnlockTarget(target string, blocks []uint32)

	// Remove all cache items that matche given target prefixes in `remove`, or only keep cache
	// items with target prefixes in `keep`. Return removed blocks and data size.
	RemoveByPrefix(remove, keep []string) (int64, int64)

	// Notify all data has been read
	DataReady()

	// Return total used cache space by cachefs store.
	Size() int64

	// Stop cachefs store.
	Stop()

	// Check if given `blk` of `target` hits local consisten hash node.
	IsHashHit(target string, blk uint32) bool

	// Aggregate single-instance cache state (capacity/used and hashhit/resident
	// block & byte counts). Scans only the in-memory index.
	GetCacheState() CacheState

	// Snapshot of the peer-load counters.
	GetPeerLoadStats() PeerLoadStats

	// Return a node-aware snapshot: restored target -> node addr -> set of block numbers that
	// node holds. prefixes filters by target prefix (empty = all). When local is false, returns
	// the cluster tracker (metaMap, empty when remote cache is disabled); when local is true,
	// returns this node's own authoritative cache distribution (single local node) read from the
	// local block cache. Callers wanting cluster presence union the per-node bitsets.
	TrackerSnapshot(prefixes []string, local bool) map[string]map[string]*bitset.BitSet

	// GetCounter returns the AtomicCounter registered under name, allowing
	// callers to read its value via .Load() or cache the pointer for hot paths.
	GetCounter(name string) (*metric.AtomicCounter, error)

	// Shrink cache usage down to base cache size, return evicted block count and bytes.
	ClearBurst() (int64, int64)

	// SetCacheSize updates cache capacity and burst cache capacity at runtime.
	// burstCacheSize < cacheSize will disable burst (reset to cacheSize).
	// Returns new cache size, new burst cache size, evicted block count and bytes.
	SetCacheSize(cacheSize, burstCacheSize int64) (int64, int64, int64, int64)

	// GetCacheSize returns the current effective cache size and burst cache size.
	GetCacheSize() (cacheSize, burstCacheSize int64)
}

type StoreConfig struct {
	LoggerName       string
	BlockSize        uint32
	CacheDir         string
	CacheSize        int64
	BurstCacheSize   int64  // burst cache size in effect, always >= CacheSize
	InitialBurstSize int64  // burst cache size configured at startup, used by BurstCacheSizeRestore
	DeferBurst       bool   // if true, startup burst cache size is recorded but not enabled until restore
	BufferSize       int64  // Buffer used for disk cache writeback
	WritebackWorker  int    // Number of writeback workers
	KeyPrefix        string // Add this prefix to internal cache key
	SourceID         string // Identify source uniquely, e.g. OSS bucket name; must not contain "/"
	ThpPolicy        string
	GoGc             utils.GoGcConfig
	FreeSpace        float32
	Cleanup          bool
	CacheEviction    CleanupMode
	BlockLRUSize     int // recent-add/load blocks protected from capacity eviction; 0 disables
	CacheExpire      time.Duration
	Hasher           string
	SerfGroup        string
	Transport        string
	TransportNics    []string

	// O_DIRECT for the local disk cache backend; independently settable for read
	// and write. Requires mmap/memfd page allocation and is disabled in
	// daemon-server mode. Falls back to buffered IO if the cache filesystem
	// rejects O_DIRECT.
	CacheReadDirectIO  bool
	CacheWriteDirectIO bool

	// remote cache related
	UploadLimit   int64 // bytes per second
	DownloadLimit int64 // bytes per second
	TotalLimit    int64 // bytes per second
	SingleLimit   int64 // bytes per second, used instead of TotalLimit when only a single transport NIC is in use
	CacheAddr     string
	CacheIP       string
	CachePort     int
	PeerTimeout   int
	BlacklistTime int
	ExchangeTime  int

	// RDMA buffer related configs used by RDMA transport
	RdmaBufSize     int
	RdmaBufCount    int
	RdmaMaxMem      uint64
	RdmaFallbackTcp bool
	RdmaGroup       string

	// Alternative P2P cache root related
	CacheRootPort  int      // Local cache root server port
	CacheRootAddrs []string // Remote cache root server addr list
	CacheRootXprt  string   // Transport used by cacheroot
	CacheRootNics  []string // Devs used to load data from cache root server

	// cachefsd related
	Socket string
}

func (conf *StoreConfig) HashringEnabled() bool {
	// enable hashring when using HashCleanup
	return conf.CacheEviction == HashCleanup
}

func (conf *StoreConfig) IsMemoryBackend() bool {
	return conf.CacheDir == "memory"
}

func (conf *StoreConfig) RdmaEnabled() bool {
	return conf.Transport == transfer.RDMATransport ||
		conf.CacheRootXprt == transfer.RDMATransport
}

// initBurstSizes settles both burst cache sizes in the config before the cache backend is created,
// which from then on takes BurstCacheSize as its upper limit.
func initBurstSizes(config *StoreConfig) {
	burst := max(config.InitialBurstSize, config.BurstCacheSize)
	config.BurstCacheSize, config.InitialBurstSize = GenBurstSizes(config.CacheSize, burst, config.DeferBurst)
	if config.BurstCacheSize < config.InitialBurstSize {
		logger.Infof("Burst cache deferred: effective %dMB, recorded %dMB",
			config.BurstCacheSize>>20, config.InitialBurstSize>>20)
	}
}

// GenBurstSizes returns the burst cache size to take effect now and the one to record for
// BurstCacheSizeRestore, both always >= cacheSize. Startup only: burstSize must be the configured
// cap, never one already held back by defer or lowered by SetCacheSize, or the recorded size
// collapses to the current one.
func GenBurstSizes(cacheSize, burstSize int64, deferBurst bool) (effective, initial int64) {
	initial = max(cacheSize, burstSize)
	effective = initial
	if deferBurst {
		effective = cacheSize
	}
	return effective, initial
}

type store struct {
	config       *StoreConfig
	sourcePrefix string
	pinCache     *pinCache
	bcache       localCache
	rcache       remoteCache
	rpc          rpcsvc.RpcService
	cacherootRpc rpcsvc.RpcService
	serf         serf.SerfService
	stopCh       chan struct{}
	counterReg   *metric.CounterRegistry // per-instance registry for store-layer counters

	// metrics
	remoteHits *metric.AtomicCounter
	// counts all failed attempts to load data from peers or cacheroot,
	// including both "not found" and "found but failed to load" cases.
	remoteMiss                   *metric.AtomicCounter
	remoteHitBytes               *metric.AtomicCounter
	remoteMissBytes              *metric.AtomicCounter
	remoteFallbackRdmaGroupHits  prometheus.Counter
	remoteFallbackRdmaGroupBytes prometheus.Counter
	remoteTimeout                *metric.AtomicCounter
	remoteBlacklist              *metric.AtomicCounter
	seederHits                   prometheus.Counter
	seederHitBytes               prometheus.Counter
	remoteReadHist               prometheus.Histogram
	checksumErr                  *metric.AtomicCounter
	remoteReadInFlight           int32

	// hash ring owner metrics: success/failure loading from the consistent-hash owner
	hashOwnerHits      prometheus.Counter
	hashOwnerHitBytes  prometheus.Counter
	hashOwnerMiss      prometheus.Counter
	hashOwnerMissBytes prometheus.Counter

	// cacheroot metrics
	cacherootQuery     prometheus.Counter
	cacherootQueryHits prometheus.Counter
	cacherootQueryMiss prometheus.Counter
	cacherootQueryHist prometheus.Histogram
	// success to load from cacheroot
	cacherootHits prometheus.Counter
	// success to query the block but failed to load from cacheroot
	cacherootMiss      prometheus.Counter
	cacherootHitBytes  prometheus.Counter
	cacherootMissBytes prometheus.Counter

	// cache state query (GetCacheState RPC) request count
	cacheStateQuery prometheus.Counter

	// pending cache operations manager
	pendingOps *utils.PendingOpsManager
}

func NewStore(config *StoreConfig, rpc rpcsvc.RpcService, cacherootRpc rpcsvc.RpcService, serf serf.SerfService, reg prometheus.Registerer) Store {
	return doNewStore(config, rpc, cacherootRpc, serf, reg)
}

func doNewStore(config *StoreConfig, rpc rpcsvc.RpcService, cacherootRpc rpcsvc.RpcService, serf serf.SerfService, reg prometheus.Registerer) *store {
	if config.LoggerName != "" {
		logger = utils.GetLogger(config.LoggerName)
	}

	// Make sure transport types are all lower case
	config.Transport = strings.ToLower(config.Transport)
	config.CacheRootXprt = strings.ToLower(config.CacheRootXprt)
	config.RdmaGroup = strings.TrimSpace(config.RdmaGroup)
	if config.RdmaGroup != "" {
		if !transfer.IsRdmaTransport(config.Transport) {
			logger.Warnf("RDMA group %q is ignored for %s transport", config.RdmaGroup, config.Transport)
			config.RdmaGroup = ""
		} else if !config.RdmaFallbackTcp {
			logger.Infof("RDMA group %q is set without TCP fallback; enabling TCP fallback automatically", config.RdmaGroup)
			config.RdmaFallbackTcp = true
		}
	}

	rcacheCfg := &remoteCacheConfig{
		blockSize:       config.BlockSize,
		uploadLimit:     config.UploadLimit,
		downloadLimit:   config.DownloadLimit,
		totalLimit:      config.TotalLimit,
		singleLimit:     config.SingleLimit,
		localAddr:       config.CacheAddr,
		localIP:         config.CacheIP,
		localPort:       config.CachePort,
		transport:       config.Transport,
		transportNics:   config.TransportNics,
		peerTimeout:     time.Second * time.Duration(config.PeerTimeout),
		blacklistTime:   time.Second * time.Duration(config.BlacklistTime),
		exchangeTime:    time.Second * time.Duration(config.ExchangeTime),
		hasher:          config.Hasher,
		serfGroup:       config.SerfGroup,
		rdmaBufSize:     config.RdmaBufSize,
		rdmaBufCount:    config.RdmaBufCount,
		rdmaMaxMem:      config.RdmaMaxMem,
		rdmaFallbackTcp: config.RdmaFallbackTcp,
		rdmaGroup:       config.RdmaGroup,
		cacherootPort:   config.CacheRootPort,
		cacherootAddrs:  config.CacheRootAddrs,
		cacherootNics:   config.CacheRootNics,
		cacherootXprt:   config.CacheRootXprt,
	}

	// Turn on data checksum when cacheroot server is enabled, so we can serve
	// requests from both instances with checksum enabled or not.
	if config.Hasher == "" && rcacheCfg.cacherootServerEnabled() {
		config.Hasher = "crc32"
		rcacheCfg.hasher = config.Hasher
		logger.Info("Force enable data checksum with crc32, as cache root server is enabled")
	}

	if config.Hasher != "" {
		if _, err := digest.NewHasher(config.Hasher); err != nil {
			logger.Fatal(err.Error())
		}
		logger.Infof("Enable P2P data checksum with %s", config.Hasher)
	} else {
		logger.Info("P2P data checksum is not enabled")
	}

	var sourcePrefix string
	if config.SourceID != "" {
		sourcePrefix = "/stag_" + config.SourceID
	}
	store := &store{
		config:       config,
		sourcePrefix: sourcePrefix,
		pinCache:     newPinCache(),
		rpc:          rpc,
		cacherootRpc: cacherootRpc,
		serf:         serf,
		stopCh:       make(chan struct{}),
		pendingOps:   utils.NewPendingOpsManager(500 * time.Millisecond),
		counterReg:   metric.NewCounterRegistry(),
	}
	store.InitMetrics(reg)

	utils.SetupThp(config.ThpPolicy)

	// Cache pages are always allocated off the Go heap, via memfd or anonymous
	// mmap, to keep the live heap low and avoid OOM.
	if PageMemfd {
		utils.SetupShmThp(config.ThpPolicy)
		logger.Info("Use memfd to allocate memory for cache")
	} else {
		logger.Info("Use mmap to allocate memory for cache")
	}

	// Disable GO GC by default to keep goruntime heap low, and limit heap usage under 1GiB.
	utils.SetupGoGc(config.GoGc, int64(1<<30))

	// All THP policies set, now check if we should do madvise call for hugepage.
	utils.SetupMadvise()

	initBurstSizes(config)

	// new remote cache first, local cache depends on remote cache
	store.rcache = newRemoteCache(rcacheCfg, store, reg)
	store.bcache = newLocalCache(config, store, reg)
	return store
}

func (s *store) readBlock(ctx context.Context, target string, blk, blklen uint32, remote bool) (ChunkReader, error) {
retry:
	// try local cache
	page, r, err := s.bcache.load(target, blk, true)
	if err == nil {
		if page != nil {
			defer page.Release()
			return NewPageReader(page), nil
		}
		return r, nil
	}

	if !remote {
		return nil, ErrNotFound
	}

	cached := s.checkPendingCache(target, blk)
	if cached {
		goto retry
	}

	// local cache miss, try remote cache
	page, err = s.loadRemote(ctx, target, blk, blklen)
	if err != nil {
		return nil, err
	}
	defer page.Release()
	page.Acquire()
	s.addToBcache(target, blk, page)
	return NewPageReader(page), nil
}

func (s *store) doLoad(ctx context.Context, target string, blk, blklen uint32, remote, touchLRU bool) (*Page, error) {
retry:
	// try local cache
	page, r, err := s.bcache.load(target, blk, touchLRU)
	if err == nil {
		if page != nil {
			return page, nil
		}
		page, err = AllocPage(int(blklen))
		if err != nil {
			r.Close()
			return nil, err
		}
		_, err = r.ReadAt(page.Data, 0)
		r.Close()
		if err == nil {
			return page, nil
		}
		page.Release()
		logger.Warnf("Read local cache for target %s block %d: %s", target, blk, err)
	}

	if !remote {
		return nil, ErrNotFound
	}

	cached := s.checkPendingCache(target, blk)
	if cached {
		goto retry
	}

	// local cache miss, try remote cache
	page, err = s.loadRemote(ctx, target, blk, blklen)
	if err != nil {
		return nil, err
	}
	page.Acquire()
	s.addToBcache(target, blk, page)
	return page, nil
}

func (s *store) loadRemote(ctx context.Context, target string, blk, blklen uint32) (*Page, error) {
	start := time.Now()
	atomic.AddInt32(&s.remoteReadInFlight, 1)
	defer atomic.AddInt32(&s.remoteReadInFlight, -1)
	page, err := s.rcache.load(ctx, target, blk, blklen)
	if err == nil {
		s.remoteHits.Inc()
		s.remoteHitBytes.Add(float64(blklen))
		s.remoteReadHist.Observe(time.Since(start).Seconds())
		return page, nil
	}
	s.remoteMiss.Inc()
	s.remoteMissBytes.Add(float64(int(blklen)))
	return nil, err
}

func (s *store) doFill(ctx context.Context, target string, blk, blklen uint32) error {
retry:
	// Just return if already cached locally
	if s.bcache.check(target, blk) {
		return nil
	}

	cached := s.checkPendingCache(target, blk)
	if cached {
		goto retry
	}

	// Not found in local cache, try load from remote cache
	page, err := s.loadRemote(ctx, target, blk, blklen)
	if err != nil {
		return err
	}
	s.addToBcache(target, blk, page)
	return nil
}

func (s *store) genTarget(target string) string {
	return s.config.KeyPrefix + s.sourcePrefix + target
}

func (s *store) Load(ctx context.Context, target string, length uint64, blk uint32) (*Page, error) {
	blklen := s.blockSize(length, blk)
	return s.doLoad(ctx, s.genTarget(target), blk, blklen, true, true)
}

func (s *store) ReadBlock(ctx context.Context, target string, length uint64, blk uint32) (ChunkReader, error) {
	blklen := s.blockSize(length, blk)
	return s.readBlock(ctx, s.genTarget(target), blk, blklen, true)
}

func (s *store) Cache(target string, blk uint32, page *Page) {
	s.bcache.cache(s.genTarget(target), blk, page)
}

func (s *store) Check(target string, blk uint32) bool {
	return s.bcache.check(s.genTarget(target), blk)
}

func (s *store) LockTarget(target string, blocks []uint32) {
	s.bcache.lockTarget(s.genTarget(target), blocks)
}

func (s *store) UnlockTarget(target string, blocks []uint32) {
	s.bcache.unlockTarget(s.genTarget(target), blocks)
}

func (s *store) Fill(ctx context.Context, target string, length uint64, blk uint32) error {
	tgt := s.genTarget(target)
	blklen := s.blockSize(length, blk)

	return s.doFill(ctx, tgt, blk, blklen)
}

func (s *store) RemoveByPrefix(remove, keep []string) (int64, int64) {
	toRemove := []string{}
	toKeep := []string{}
	for _, p := range remove {
		toRemove = append(toRemove, s.genTarget(p))
	}
	for _, p := range keep {
		toKeep = append(toKeep, s.genTarget(p))
	}
	return s.bcache.removeByPrefix(toRemove, toKeep)
}

func (s *store) DataReady() {
	s.rcache.setDataReady()
}

func (s *store) Size() int64 {
	_, used := s.bcache.stats()
	return used
}

func (s *store) Stop() {
	if s.stopCh != nil {
		close(s.stopCh)
		s.stopCh = nil
	}
	s.rcache.stop()
}

func (s *store) IsHashHit(target string, blk uint32) bool {
	key := CacheKey{Target: s.genTarget(target), Blk: blk}
	return s.serf.HashHit(key.String())
}

// hashHit reports whether an already-gen'd cache key belongs to this node on the
// consistent hash ring. Returns false when the hash ring (serf) is unavailable.
func (s *store) hashHit(genKey string) bool {
	if s.serf == nil {
		return false
	}
	return s.serf.HashHit(genKey)
}

func (s *store) GetCacheState() CacheState {
	s.cacheStateQuery.Inc()
	return s.bcache.getCacheState()
}

func (s *store) GetPeerLoadStats() PeerLoadStats {
	return PeerLoadStats{
		Hits:           uint64(s.remoteHits.Load()),
		Miss:           uint64(s.remoteMiss.Load()),
		HitBytes:       uint64(s.remoteHitBytes.Load()),
		MissBytes:      uint64(s.remoteMissBytes.Load()),
		Timeouts:       uint64(s.remoteTimeout.Load()),
		Blacklisted:    uint64(s.remoteBlacklist.Load()),
		ChecksumErrors: uint64(s.checksumErr.Load()),
		InFlight:       int64(atomic.LoadInt32(&s.remoteReadInFlight)),
		Latency:        metric.ReadHist(s.remoteReadHist),
	}
}

func (s *store) TrackerSnapshot(prefixes []string, local bool) map[string]map[string]*bitset.BitSet {
	if local {
		return s.localTrackerSnapshot(prefixes)
	}
	if s.rcache == nil {
		return map[string]map[string]*bitset.BitSet{}
	}
	return s.rcache.trackerSnapshot(prefixes)
}

func (s *store) localTrackerSnapshot(prefixes []string) map[string]map[string]*bitset.BitSet {
	dump := s.bcache.dump()
	if dump == nil {
		return map[string]map[string]*bitset.BitSet{}
	}
	genPrefixes := make([]string, 0, len(prefixes))
	for _, p := range prefixes {
		genPrefixes = append(genPrefixes, s.genTarget(p))
	}
	strip := s.config.KeyPrefix + s.sourcePrefix
	node := s.config.CacheAddr
	out := make(map[string]map[string]*bitset.BitSet, len(dump.Targets))
	for key, tm := range dump.Targets {
		if len(genPrefixes) > 0 && !hasAnyPrefix(key, genPrefixes) {
			continue
		}
		var bs *bitset.BitSet
		if tm.Blocks != nil {
			bs = tm.Blocks
		} else {
			// Fully-cached target: expand the whole file into individual block numbers.
			nblk := uint((tm.Length + uint64(s.config.BlockSize) - 1) / uint64(s.config.BlockSize))
			bs = bitset.New(nblk)
			for i := uint(0); i < nblk; i++ {
				bs.Set(i)
			}
		}
		out[strings.TrimPrefix(key, strip)] = map[string]*bitset.BitSet{node: bs}
	}
	return out
}

func (s *store) ClearBurst() (int64, int64) {
	return s.bcache.clearBurst()
}

func (s *store) SetCacheSize(cacheSize, burstCacheSize int64) (int64, int64, int64, int64) {
	return s.bcache.setCapacity(cacheSize, burstCacheSize)
}

func (s *store) GetCacheSize() (int64, int64) {
	return atomic.LoadInt64(&s.config.CacheSize), atomic.LoadInt64(&s.config.BurstCacheSize)
}

func (s *store) blockSize(length uint64, blk uint32) uint32 {
	blockSize := uint64(s.config.BlockSize)
	bsize := length - uint64(blk)*blockSize
	if bsize > blockSize {
		bsize = blockSize
	}
	return uint32(bsize)
}

// Returns the named AtomicCounter from the store registry.
func (s *store) GetCounter(name string) (*metric.AtomicCounter, error) {
	return s.counterReg.GetCounter(name)
}

func (s *store) InitMetrics(reg prometheus.Registerer) {
	s.remoteHits = s.counterReg.NewCounter(prometheus.CounterOpts{
		Name: CounterRemoteHits,
		Help: "read from remote cache",
	})
	s.remoteMiss = s.counterReg.NewCounter(prometheus.CounterOpts{
		Name: CounterRemoteMiss,
		Help: "missed read from remote cache",
	})
	s.remoteHitBytes = s.counterReg.NewCounter(prometheus.CounterOpts{
		Name: CounterRemoteHitBytes,
		Help: "read bytes from remote cache",
	})
	s.remoteMissBytes = s.counterReg.NewCounter(prometheus.CounterOpts{
		Name: CounterRemoteMissBytes,
		Help: "missed bytes from remote cache",
	})
	s.remoteFallbackRdmaGroupHits = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "remote_fallback_rdmagroup_hits",
		Help: "blocks read through TCP fallback from peers outside the local RDMA group",
	})
	s.remoteFallbackRdmaGroupBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "remote_fallback_rdmagroup_bytes",
		Help: "bytes read through TCP fallback from peers outside the local RDMA group",
	})
	s.cacherootQuery = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cacheroot_query",
		Help: "count of queries to remote cache root",
	})
	s.cacherootQueryHits = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cacheroot_query_hits",
		Help: "count of successful queries to remote cache root",
	})
	s.cacherootQueryMiss = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cacheroot_query_miss",
		Help: "count of failed queries to remote cache root",
	})
	s.cacherootQueryHist = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "cacheroot_query_hist_seconds",
		Help:    "cacheroot client query latency distribution",
		Buckets: prometheus.ExponentialBuckets(0.001, 1.5, 20),
	})
	s.cacherootHits = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cacheroot_hits",
		Help: "read from remote cache root",
	})
	s.cacherootMiss = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cacheroot_miss",
		Help: "missed read from remote cache root",
	})
	s.cacherootHitBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cacheroot_hit_bytes",
		Help: "read bytes from remote cache root",
	})
	s.cacherootMissBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cacheroot_miss_bytes",
		Help: "missed bytes from remote cache root",
	})
	s.seederHits = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "seeder_hits",
		Help: "read from seeders",
	})
	s.seederHitBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "seeder_hit_bytes",
		Help: "read bytes from seeders",
	})
	s.hashOwnerHits = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hashowner_hits",
		Help: "read from consistent-hash owner",
	})
	s.hashOwnerHitBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hashowner_hit_bytes",
		Help: "read bytes from consistent-hash owner",
	})
	s.hashOwnerMiss = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hashowner_miss",
		Help: "missed read from consistent-hash owner",
	})
	s.hashOwnerMissBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hashowner_miss_bytes",
		Help: "missed bytes from consistent-hash owner",
	})
	s.remoteReadHist = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "remote_read_hist_seconds",
		Help:    "read remote cache latency distribution",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 20),
	})
	s.checksumErr = metric.NewCounter(prometheus.CounterOpts{
		Name: "remote_checksum_error_count",
		Help: "cache checksum error count when loading from remote cache",
	})
	s.remoteTimeout = metric.NewCounter(prometheus.CounterOpts{
		Name: "remote_read_timeout",
		Help: "read timeout from remote cache",
	})
	s.remoteBlacklist = metric.NewCounter(prometheus.CounterOpts{
		Name: "remote_blacklist",
		Help: "peer in blacklist while read from remote cache",
	})
	s.cacheStateQuery = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cache_state_query",
		Help: "cache state query (GetCacheState) request count in total",
	})
	if reg == nil {
		return
	}

	reg.MustRegister(s.remoteHits)
	reg.MustRegister(s.remoteHitBytes)
	reg.MustRegister(s.remoteMiss)
	reg.MustRegister(s.remoteMissBytes)
	reg.MustRegister(s.remoteFallbackRdmaGroupHits)
	reg.MustRegister(s.remoteFallbackRdmaGroupBytes)
	reg.MustRegister(s.cacherootQuery)
	reg.MustRegister(s.cacherootQueryHits)
	reg.MustRegister(s.cacherootQueryMiss)
	reg.MustRegister(s.cacherootQueryHist)
	reg.MustRegister(s.cacherootHits)
	reg.MustRegister(s.cacherootHitBytes)
	reg.MustRegister(s.cacherootMiss)
	reg.MustRegister(s.cacherootMissBytes)
	reg.MustRegister(s.seederHits)
	reg.MustRegister(s.seederHitBytes)
	reg.MustRegister(s.hashOwnerHits)
	reg.MustRegister(s.hashOwnerHitBytes)
	reg.MustRegister(s.hashOwnerMiss)
	reg.MustRegister(s.hashOwnerMissBytes)
	reg.MustRegister(s.remoteReadHist)
	reg.MustRegister(s.checksumErr)
	reg.MustRegister(s.remoteTimeout)
	reg.MustRegister(s.remoteBlacklist)
	reg.MustRegister(s.cacheStateQuery)
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "cache_blocks",
			Help: "number of cached blocks",
		},
		func() float64 {
			cnt, _ := s.bcache.stats()
			return float64(cnt)
		}))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "cache_bytes",
			Help: "number of cached bytes",
		},
		func() float64 {
			_, used := s.bcache.stats()
			return float64(used)
		}))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "remote_read_flight",
			Help: "in flight read from remote cache",
		},
		func() float64 {
			return float64(atomic.LoadInt32(&s.remoteReadInFlight))
		}))
}

func (s *store) checksumEnabled() bool {
	return s.config.Hasher != ""
}

func (s *store) addPendingCache(target string, blk uint32) bool {
	key := CacheKey{Target: target, Blk: blk}
	return s.pendingOps.Add(key.String())
}

func (s *store) removePendingCache(target string, blk uint32) {
	key := CacheKey{Target: target, Blk: blk}
	s.pendingOps.Remove(key.String())
}

func (s *store) checkPendingCache(target string, blk uint32) bool {
	key := CacheKey{Target: target, Blk: blk}
	return s.pendingOps.Wait(key.String())
}

func (s *store) addToBcache(target string, blk uint32, page *Page) {
	if !s.addPendingCache(target, blk) {
		// Another goroutine owns the pending cache write; release this caller's extra page reference.
		page.Release()
		return
	}
	go func() {
		s.bcache.cache(target, blk, page)
		s.removePendingCache(target, blk)
		page.Release()
	}()
}

// encode digest and hasher to "hasher:digest" format, e.g.
// "crc32:xxxxxxxx"
func encodeDigest(digest, hasher string) string {
	return fmt.Sprintf("%s:%s", hasher, digest)
}

// hash should be in the format of following:
// "crc32:xxxxxxxx"
func parseDigest(hash, localHasher string) (string, string, error) {
	if hash == "" {
		if localHasher != "" {
			return "", "", errors.New("remote node disabled cache digest")
		} else {
			return "", "", nil
		}
	} else {
		if localHasher == "" {
			return "", "", errors.New("remote node enabled cache digest")
		}
	}
	ds := strings.Split(hash, ":")
	if len(ds) != 2 {
		return "", "", fmt.Errorf("invalid hash string: %s", hash)
	}
	if localHasher != ds[0] {
		return "", "", fmt.Errorf("want hasher %s but got %s", localHasher, ds[0])
	}
	return ds[0], ds[1], nil
}
