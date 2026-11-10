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
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aliyun/cachefs/pkg/rpcsvc"
	ss "github.com/aliyun/cachefs/pkg/serf"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/eryugey/serf/serf"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/willf/bitset"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// number of peer candidate
	nrPeerCandidate = 10

	// timeout for a single ExchangeTracker RPC call on initial join
	exchangeTrackerTimeout = 10 * time.Second

	// channel size for buffering peer addresses pending ExchangeTracker
	exchangeTrackerChSize = 512

	// number of worker goroutines serving the exchange channel
	exchangeTrackerWorkers = 4
)

// interval to aggregate blacklist skip logs into one summary line;
// a var so tests can shorten it
var blacklistSkipLogInterval = 10 * time.Second

var (
	once sync.Once

	// remote cache server metrics
	uploadCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "remote_server_upload_count",
		Help: "uploaded block count by remote cache server",
	})
	uploadBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "remote_server_upload_bytes",
		Help: "uploaded bytes by remote cache server",
	})
	missCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "remote_server_miss_count",
		Help: "cache miss count of remote cache server",
	})
	missBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "remote_server_miss_bytes",
		Help: "cache miss bytes of remote cache server",
	})
	uploadTimeout = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "remote_server_timeout",
		Help: "upload timeout count",
	})
)

type remoteCache interface {
	announce(target string, length uint64, nrblk uint32)
	revoke(target string, length uint64, nrblk uint32)
	load(ctx context.Context, target string, nrblk, blklen uint32) (*Page, error)
	setDataReady()
	addMetaInfo(remoteMeta *metaInfo) (int, int)
	reconcilePeer(remoteMeta *metaInfo) (int, int)
	trackerSnapshot(prefixes []string) map[string]map[string]*bitset.BitSet
	stop()
}

type remoteCacheConfig struct {
	blockSize       uint32
	localIP         string
	localPort       int
	localAddr       string
	transport       string
	transportNics   []string
	uploadLimit     int64
	downloadLimit   int64
	totalLimit      int64
	singleLimit     int64
	peerTimeout     time.Duration
	blacklistTime   time.Duration
	exchangeTime    time.Duration
	serfGroup       string
	hasher          string
	rdmaBufSize     int
	rdmaBufCount    int
	rdmaMaxMem      uint64
	rdmaFallbackTcp bool
	cacherootPort   int
	cacherootAddrs  []string
	cacherootNics   []string
	cacherootXprt   string
}

type pendingTargetMeta struct {
	// bitset of blocks, nil means announce or revoke target as a whole
	target *targetMeta
	// creation time
	ctime time.Time
}

type peerCache struct {
	config    *remoteCacheConfig
	store     *store
	transport transportInterface
	cacheroot cacheRoot // secondary P2P tracker in addition to serf

	serfUser *ss.SerfUser
	serf     ss.SerfService
	rpc      rpcsvc.RpcService

	announceCh    chan *metaInfo
	revokeCh      chan *metaInfo
	stopCh        chan struct{}
	announceReady chan struct{}

	announceMu      sync.Mutex
	revokeMu        sync.Mutex
	pendingAnnounce map[string]*pendingTargetMeta
	pendingRevoke   map[string]*pendingTargetMeta

	// target map tracks cache targets own by which nodes,
	// first indexed by target file, then node ip that has this target as a whole.
	// lock targetMu first then metaMu
	targetMu  sync.Mutex
	targetMap map[string]*targetInfo

	// meta map tracks cache blocks own by which nodes,
	// first indexed by target file, then by block number, then node ip that has this block.
	// RWMutex: findCandidates and trackerSnapshot only read metaMap and take RLock so they
	// run concurrently; addBlocks/delBlocks/cleanupMember mutate it and take the write Lock.
	metaMu  sync.RWMutex
	metaMap map[string]map[uint32]map[string]struct{}

	// peerMap is a reverse view of metaMap: node ip -> target -> bitset of blocks that node
	// holds. It is maintained in lockstep with metaMap under metaMu, so it always mirrors it.
	peerMap map[string]map[string]*bitset.BitSet

	// seeders, and can be a seeder if data is ready
	seederMu  sync.Mutex
	seeders   map[string]struct{}
	dataReady atomic.Bool

	// peer black list due to connection/load timeout
	blackMu    sync.Mutex
	blacklists map[string]time.Time

	// skip counts per peer for blacklist skip log aggregation, guarded by blackMu
	skipCounts  map[string]int64
	lastSkipLog time.Time

	// exchangeCh buffers peer addresses that need an ExchangeTracker RPC.
	// Worker goroutines consume from this channel and perform the exchange.
	exchangeCh chan string

	// exchangeDoneCh is closed when the initial serf join completes,
	// signaling handleSerfMemberJoinEvent to stop pushing to exchangeCh.
	exchangeDoneCh chan struct{}
}

func (cfg *remoteCacheConfig) checksumEnabled() bool {
	return cfg.hasher != ""
}

func (cfg *remoteCacheConfig) cacherootServerEnabled() bool {
	return cfg.cacherootPort > 0
}

func (cfg *remoteCacheConfig) cacherootClientEnabled() bool {
	return len(cfg.cacherootAddrs) > 0
}

func (cfg *remoteCacheConfig) deepCopy() *remoteCacheConfig {
	newCfg := *cfg
	newCfg.transportNics = make([]string, len(cfg.transportNics))
	copy(newCfg.transportNics, cfg.transportNics)
	newCfg.cacherootAddrs = make([]string, len(cfg.cacherootAddrs))
	copy(newCfg.cacherootAddrs, cfg.cacherootAddrs)
	newCfg.cacherootNics = make([]string, len(cfg.cacherootNics))
	copy(newCfg.cacherootNics, cfg.cacherootNics)
	return &newCfg
}

func initPackageMetrics(reg prometheus.Registerer) {
	if reg == nil {
		return
	}
	reg.MustRegister(uploadCount)
	reg.MustRegister(uploadBytes)
	reg.MustRegister(missCount)
	reg.MustRegister(missBytes)
	reg.MustRegister(uploadTimeout)
}

func newRemoteCache(cfg *remoteCacheConfig, store *store, reg prometheus.Registerer) remoteCache {
	var err error

	// announceCh is a channel that process announce event, with at least 1024 size
	announceChSize := ss.EventQueueDepth / ss.EventWorkerCount
	if announceChSize < 1024 {
		announceChSize = 1024
	}
	cache := &peerCache{
		config:          cfg,
		store:           store,
		rpc:             store.rpc,
		serf:            store.serf,
		stopCh:          make(chan struct{}),
		announceReady:   make(chan struct{}),
		announceCh:      make(chan *metaInfo, announceChSize),
		revokeCh:        make(chan *metaInfo, 1024),
		metaMap:         make(map[string]map[uint32]map[string]struct{}),
		peerMap:         make(map[string]map[string]*bitset.BitSet),
		targetMap:       make(map[string]*targetInfo),
		seeders:         make(map[string]struct{}),
		dataReady:       atomic.Bool{},
		blacklists:      make(map[string]time.Time),
		skipCounts:      make(map[string]int64),
		lastSkipLog:     time.Now(),
		pendingAnnounce: make(map[string]*pendingTargetMeta),
		pendingRevoke:   make(map[string]*pendingTargetMeta),
		exchangeCh:      make(chan string, exchangeTrackerChSize),
		exchangeDoneCh:  make(chan struct{}),
	}
	initPackageMetrics(reg)

	// Start transport server first, then register serf service, so we could serve
	// requests as soon as possible.
	cache.transport, err = newTransport(cfg, store, reg)
	if err != nil {
		logger.Fatalf("Create transport: %s", err)
	}

	cache.cacheroot, err = newCacheRoot(cache, reg)
	if err != nil {
		logger.Fatalf("Create cacheroot: %s", err)
	}

	// Register as a user of serf service
	cache.serfUser = &ss.SerfUser{
		Name:               "store",
		UserEvents:         supportedSerfEvents,
		MemberJoinCh:       make(chan []ss.SerfMember, 128),
		MemberLeaveCh:      make(chan []ss.SerfMember, 128),
		UserEventCh:        make(chan serf.UserEvent, announceChSize<<1),
		UserEventCoalescer: newUserEventCoalescer(cache),
		Filter:             &storeEventFilter{},
		ReadyCh:            make(chan struct{}),
		JoinedCh:           make(chan struct{}, 16),
	}
	if err := cache.serf.RegisterUser(cache.serfUser); err != nil {
		logger.Fatalf("Register serf user: %s", err)
	}

	// Start goroutines that work with serf service to maintain P2P tracker info.
	go cache.serveAnnounce()
	go cache.serveRevoke()
	go cache.collectMeta()
	go cache.scanPendingEvents()
	go cache.handleSerfUserEvent()
	go cache.handleSerfMemberJoinEvent()
	go cache.handleSerfMemberLeaveEvent()

	// Start exchange tracker workers
	for i := 0; i < exchangeTrackerWorkers; i++ {
		go cache.serveExchangeTracker(i)
	}
	// Start periodic exchange if configured
	if cfg.exchangeTime > 0 {
		go cache.periodicExchange()
	}

	go cache.gcBlacklist()
	once.Do(func() {
		http.HandleFunc("/peer_blacklist", func(w http.ResponseWriter, r *http.Request) {
			lists := cache.getBlacklist()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(lists); err != nil {
				http.Error(w, "Unable to encode JSON", http.StatusInternalServerError)
			}
		})
	})
	return cache
}

func (r *peerCache) scanPendingEvents() {
	// wait serveAnnounce to be running for 1s
	t := time.NewTimer(time.Second)
	select {
	case <-t.C:
		logger.Warn("Waiting for serveAnnounce timed out, scan pending announce anyway")
	case <-r.announceReady:
	}

	scanFunc := func(ch chan *metaInfo, mu *sync.Mutex, pending map[string]*pendingTargetMeta) {
		for {
			minfo := &metaInfo{
				SrcAddr:   r.config.localAddr,
				BlockSize: r.config.blockSize,
			}
			minfo.Targets = make(map[string]*targetMeta)
			mu.Lock()
			for target, ptm := range pending {
				count := ptm.target.Blocks.Count()
				// broadcast if older than 2 x GossipInterval or has more
				// 10 blocks or broadcast target file as a whole (length > 0)
				if ptm.ctime.Add(2*ss.GossipInterval).Before(time.Now()) ||
					count >= 10 || ptm.target.Length > 0 {
					delete(pending, target)
					minfo.Targets[target] = ptm.target
				}

				// broadcast 10 files at most
				if len(minfo.Targets) == 10 {
					break
				}
			}
			mu.Unlock()
			if len(minfo.Targets) > 0 {
				ch <- minfo
			}
			time.Sleep(ss.GossipInterval)
		}
	}

	// scan both pending announce and revoke messages
	go scanFunc(r.announceCh, &r.announceMu, r.pendingAnnounce)
	time.Sleep(ss.GossipInterval / 2)
	go scanFunc(r.revokeCh, &r.revokeMu, r.pendingRevoke)
}

func addPendingEvents(mu *sync.Mutex, pending map[string]*pendingTargetMeta, target string, length uint64, nrblk uint32) {
	mu.Lock()
	defer mu.Unlock()
	ptm, ok := pending[target]
	if !ok {
		tm := &targetMeta{}
		// length > 0 indicates announce the whole target
		if length == 0 {
			tm.Blocks = bitset.New(uint(nrblk))
		}
		ptm = &pendingTargetMeta{
			target: tm,
			ctime:  time.Now(),
		}
		pending[target] = ptm
	}
	if length > 0 {
		ptm.target.Length = length
		ptm.target.Blocks = nil
	}
	if ptm.target.Blocks != nil {
		ptm.target.Blocks.Set(uint(nrblk))
	}
}

// announce `target` block `nrblk` to peers, if length > 0, then we announce `target` as a whole
func (r *peerCache) announce(target string, length uint64, nrblk uint32) {
	addPendingEvents(&r.announceMu, r.pendingAnnounce, target, length, nrblk)
}

// revoke `target` block `nrblk` to peers, if length > 0 then we announce `target` as a whole
func (r *peerCache) revoke(target string, length uint64, nrblk uint32) {
	addPendingEvents(&r.revokeMu, r.pendingRevoke, target, length, nrblk)
}

// add given peer addr to black list
func (r *peerCache) addBlacklist(addr string) {
	if r.config.blacklistTime == 0 {
		return
	}
	r.blackMu.Lock()
	r.blacklists[addr] = time.Now()
	r.blackMu.Unlock()
}

func (r *peerCache) inBlacklist(addr string) bool {
	if r.config.blacklistTime == 0 {
		return false
	}
	r.blackMu.Lock()
	defer r.blackMu.Unlock()
	ctime, ok := r.blacklists[addr]
	if !ok {
		return false
	}
	if ctime.Add(r.config.blacklistTime).Before(time.Now()) {
		delete(r.blacklists, addr)
		return false
	} else {
		return true
	}
}

// countBlacklistSkip accumulates per-peer blacklist skip counts and lazily
// flushes one aggregated log line per blacklistSkipLogInterval, instead of
// logging on every skipped request.
func (r *peerCache) countBlacklistSkip(addr string) {
	r.blackMu.Lock()
	if r.skipCounts == nil {
		r.skipCounts = make(map[string]int64)
	}
	if r.lastSkipLog.IsZero() {
		r.lastSkipLog = time.Now()
	}
	r.skipCounts[addr]++
	var counts map[string]int64
	var elapsed time.Duration
	if time.Since(r.lastSkipLog) >= blacklistSkipLogInterval {
		counts = r.skipCounts
		r.skipCounts = make(map[string]int64)
		elapsed = time.Since(r.lastSkipLog)
		r.lastSkipLog = time.Now()
	}
	r.blackMu.Unlock()
	if counts != nil {
		logger.Infof("Blacklist skip stats in last %s: %v", elapsed.Round(time.Second), counts)
	}
}

func (r *peerCache) getBlacklist() []string {
	r.blackMu.Lock()
	defer r.blackMu.Unlock()
	if len(r.blacklists) == 0 {
		return []string{}
	}
	blacklist := make([]string, 0, len(r.blacklists))
	for addr := range r.blacklists {
		blacklist = append(blacklist, addr)
	}
	return blacklist
}

func (r *peerCache) gcBlacklist() {
	timeout := r.config.blacklistTime
	if timeout == 0 {
		return
	}
	for {
		select {
		case <-r.stopCh:
			return
		default:
			now := time.Now()
			remain := []string{}
			removed := []string{}
			r.blackMu.Lock()
			for addr, ctime := range r.blacklists {
				if ctime.Add(timeout).Before(now) {
					delete(r.blacklists, addr)
					removed = append(removed, addr)
				} else {
					remain = append(remain, addr)
				}
			}
			r.blackMu.Unlock()
			if len(remain) > 0 || len(removed) > 0 {
				logger.Infof("GC peer blacklist, removed %v, still in list: %v", removed, remain)
			}
			utils.SleepWithJitter(5 * time.Minute)
		}
	}
}

func (r *peerCache) findCandidates(target string, nrblk uint32, n int, skipLocal bool) []string {
	candidates := []string{}

	r.metaMu.RLock()
	blockMap, ok := r.metaMap[target]
	if !ok {
		r.metaMu.RUnlock()
		return candidates
	}
	hostMap, ok := blockMap[nrblk]
	if !ok {
		r.metaMu.RUnlock()
		return candidates
	}

	// find at most n random candidates
	for addr := range hostMap {
		if skipLocal && addr == r.config.localAddr {
			continue
		}
		candidates = append(candidates, addr)
		if n > 0 && len(candidates) >= n {
			break
		}
	}
	r.metaMu.RUnlock()

	// Randomize order to avoid repeatedly hitting the same peer.
	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})
	return candidates
}

// Copy the metaMap into a node-aware view keyed by restored target (internal key prefix stripped),
// then by node address, each value a bitset of the blocks that node holds. When prefixes is
// non-empty, only targets whose internal key matches one of the gen'd prefixes are included, so the
// metaMu is held only long enough to scan matching targets. Callers that only need cluster presence
// (a block cached on any node) union the per-node bitsets of a target.
func (r *peerCache) trackerSnapshot(prefixes []string) map[string]map[string]*bitset.BitSet {
	genPrefixes := make([]string, 0, len(prefixes))
	for _, p := range prefixes {
		genPrefixes = append(genPrefixes, r.store.genTarget(p))
	}
	strip := r.store.config.KeyPrefix + r.store.sourcePrefix
	out := make(map[string]map[string]*bitset.BitSet)

	r.metaMu.RLock()
	for key, blockMap := range r.metaMap {
		if len(genPrefixes) > 0 && !hasAnyPrefix(key, genPrefixes) {
			continue
		}
		perNode := make(map[string]*bitset.BitSet)
		for blk, hosts := range blockMap {
			for node := range hosts {
				bs := perNode[node]
				if bs == nil {
					bs = bitset.New(0)
					perNode[node] = bs
				}
				bs.Set(uint(blk))
			}
		}
		out[strings.TrimPrefix(key, strip)] = perNode
	}
	r.metaMu.RUnlock()
	return out
}

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

func (r *peerCache) loadFromPeer(ctx context.Context, target string, nrblk uint32, blklen uint32, peers []string, group string, cacheroot bool) (p *Page, rerr error) {
	// potentially allocate page via memfd so we could pass this fd to cachefs fuse
	// client via UDS if we're running in cachefs daemon mode.
	key := CacheKey{target, nrblk}
	page, err := AllocPage(int(blklen), key.String())
	if err != nil {
		return nil, err
	}
	defer func() {
		if rerr != nil {
			page.Release()
		}
	}()

	for _, addr := range peers {
		if r.inBlacklist(addr) {
			r.countBlacklistSkip(addr)
			r.store.remoteBlacklist.Inc()
			continue
		}
		err := r.transport.load(ctx, addr, target, nrblk, blklen, page, group, cacheroot)
		if err == nil {
			return page, nil
		}

		// coro-rpc may return "Connection timed out" on transport timeout,
		// and will retry on connection refused until timeout, and return
		// "not connected", so add to blacklist as well.
		if status.Code(err) == codes.DeadlineExceeded ||
			strings.Contains(err.Error(), "time out") ||
			strings.Contains(err.Error(), "timed out") ||
			strings.Contains(err.Error(), "timeout") ||
			strings.Contains(err.Error(), "not connected") {
			r.store.remoteTimeout.Inc()
			r.addBlacklist(addr)
		}
	}
	return nil, ErrNotFound
}

func (r *peerCache) load(ctx context.Context, target string, nrblk uint32, blklen uint32) (*Page, error) {
	group := r.config.serfGroup
	candidates := r.findCandidates(target, nrblk, nrPeerCandidate, true)

	start := time.Now()
	if len(candidates) > 0 {
		page, err := r.loadFromPeer(ctx, target, nrblk, blklen, candidates, group, false)
		if err == nil {
			return page, nil
		}
	}

	// cache not loaded from candidates, try seeders
	r.seederMu.Lock()
	seeders := make([]string, 0, len(r.seeders))
	for addr := range r.seeders {
		seeders = append(seeders, addr)
	}
	r.seederMu.Unlock()
	if len(seeders) > 0 {
		page, err := r.loadFromPeer(ctx, target, nrblk, blklen, seeders, group, false)
		if err == nil {
			r.store.seederHits.Inc()
			r.store.seederHitBytes.Add(float64(blklen))
			return page, nil
		}
	}

	// try hash owner
	if r.store.config.HashringEnabled() && r.store.serf.GetMemberCount() > 1 {
		ownerKey := CacheKey{target, nrblk}
		_, ownerAddr, oerr := r.store.serf.GetHashOwner(ownerKey.String())
		if oerr != nil {
			logger.Infof("Get hash owner of %s block %d: %s", target, nrblk, oerr)
		} else if ownerAddr != "" && ownerAddr != r.config.localAddr && !slices.Contains(candidates, ownerAddr) {
			page, err := r.loadFromPeer(ctx, target, nrblk, blklen, []string{ownerAddr}, group, false)
			if err == nil {
				r.store.hashOwnerHits.Inc()
				r.store.hashOwnerHitBytes.Add(float64(blklen))
				return page, nil
			}
			r.store.hashOwnerMiss.Inc()
			r.store.hashOwnerMissBytes.Add(float64(blklen))
		}
	}

	var cacherootPeers []string
	var err error
	if r.config.cacherootClientEnabled() {
		r.store.cacherootQuery.Inc()
		cacherootPeers, group, err = r.cacheroot.query(ctx, target, nrblk)
		if err != nil {
			r.store.cacherootQueryMiss.Inc()
			return nil, err
		}
		r.store.cacherootQueryHits.Inc()

		// Randomize candidates to avoid overloading the same server
		rand.Shuffle(len(cacherootPeers), func(i, j int) {
			cacherootPeers[i], cacherootPeers[j] = cacherootPeers[j], cacherootPeers[i]
		})
		if len(cacherootPeers) > nrPeerCandidate {
			cacherootPeers = cacherootPeers[:nrPeerCandidate]
		}
		page, err := r.loadFromPeer(ctx, target, nrblk, blklen, cacherootPeers, group, true)
		if err == nil {
			r.store.cacherootHits.Inc()
			r.store.cacherootHitBytes.Add(float64(blklen))
			return page, nil
		} else {
			r.store.cacherootMiss.Inc()
			r.store.cacherootMissBytes.Add(float64(blklen))
		}
	}

	if len(candidates) > 0 || len(cacherootPeers) > 0 {
		logger.Infof("Peer Load: %s block %d, tried local candidates %v, "+
			"cacheroot candidates %v in %s but all failed",
			target, nrblk, candidates, cacherootPeers, time.Since(start))
	}
	return nil, ErrNotFound
}

func (r *peerCache) validateMetaInfo(info *metaInfo) error {
	if info.SrcAddr == "" {
		return fmt.Errorf("has no SrcAddr: %v", info)
	}
	if info.BlockSize != r.config.blockSize {
		return fmt.Errorf("SrcAddr %s has different block size: %d", info.SrcAddr, info.BlockSize)
	}
	return nil
}

func (r *peerCache) fromLocal(addr string) bool {
	return addr == r.config.localAddr
}

// setPeerBlock records that host owns block i of target, updating both metaMap and its
// reverse view peerMap. Caller must hold metaMu (write).
func (r *peerCache) setPeerBlock(host, target string, i uint32) {
	blockMap, ok := r.metaMap[target]
	if !ok {
		blockMap = make(map[uint32]map[string]struct{})
		r.metaMap[target] = blockMap
	}
	hostMap, ok := blockMap[i]
	if !ok {
		hostMap = make(map[string]struct{})
		blockMap[i] = hostMap
	}
	hostMap[host] = struct{}{}

	tm, ok := r.peerMap[host]
	if !ok {
		tm = make(map[string]*bitset.BitSet)
		r.peerMap[host] = tm
	}
	bs, ok := tm[target]
	if !ok {
		bs = bitset.New(uint(i) + 1)
		tm[target] = bs
	}
	bs.Set(uint(i))
}

// clearPeerBlock removes host as an owner of block i of target from both metaMap and
// peerMap, pruning any emptied maps/bitsets. Caller must hold metaMu (write).
func (r *peerCache) clearPeerBlock(host, target string, i uint32) {
	if blockMap, ok := r.metaMap[target]; ok {
		if hostMap, ok := blockMap[i]; ok {
			delete(hostMap, host)
			if len(hostMap) == 0 {
				delete(blockMap, i)
			}
		}
		if len(blockMap) == 0 {
			delete(r.metaMap, target)
		}
	}
	if tm, ok := r.peerMap[host]; ok {
		if bs, ok := tm[target]; ok {
			bs.Clear(uint(i))
			if bs.None() {
				delete(tm, target)
			}
		}
		if len(tm) == 0 {
			delete(r.peerMap, host)
		}
	}
}

func (r *peerCache) addMetaInfo(info *metaInfo) (int, int) {
	var targetcnt, blkcnt int

	if info == nil || info.SrcAddr == "" {
		return 0, 0
	}

	addBlocks := func(target string, blocks *bitset.BitSet) {
		logger.Debugf("Add announce info for target %s blocks %v", target, blocks)
		r.metaMu.Lock()
		for i, e := blocks.NextSet(0); e; i, e = blocks.NextSet(i + 1) {
			r.setPeerBlock(info.SrcAddr, target, uint32(i))
			blkcnt++
		}
		r.metaMu.Unlock()
	}
	addTarget := func(target string, length uint64) {
		logger.Debugf("Add announce info for target %s length %v", target, length)
		r.targetMu.Lock()
		if t, ok := r.targetMap[target]; ok {
			if t.length != length {
				logger.Warnf("Announced target %s length %d != local length %d", target, length, t.length)
				r.targetMu.Unlock()
				return
			} else {
				t.nodes[info.SrcAddr] = struct{}{}
			}
		} else {
			t = &targetInfo{
				length: length,
				nodes:  make(map[string]struct{}),
			}
			t.nodes[info.SrcAddr] = struct{}{}
			t.length = length
			r.targetMap[target] = t
		}
		r.targetMu.Unlock()

		blksiz := r.config.blockSize
		cnt := utils.GetBlockCount(length, blksiz)
		blocks := bitset.New(uint(cnt))
		for i := uint32(0); i < cnt; i++ {
			blocks.Set(uint(i))
		}
		// Set all blocks of target (addBlocks will increment blkcnt)
		addBlocks(target, blocks)
	}

	for target, tm := range info.Targets {
		targetcnt++
		if tm.Length == 0 {
			// Process target blocks
			addBlocks(target, tm.Blocks)
		} else {
			// Process whole target
			addTarget(target, tm.Length)
		}
	}

	return targetcnt, blkcnt
}

func (r *peerCache) delMetaInfo(info *metaInfo) {
	delBlocks := func(target string, blocks *bitset.BitSet) {
		r.metaMu.Lock()
		defer r.metaMu.Unlock()
		for i, e := blocks.NextSet(0); e; i, e = blocks.NextSet(i + 1) {
			r.clearPeerBlock(info.SrcAddr, target, uint32(i))
		}
	}
	delTarget := func(target string) {
		r.targetMu.Lock()
		defer r.targetMu.Unlock()
		if t, ok := r.targetMap[target]; ok {
			delete(t.nodes, info.SrcAddr)
			if len(t.nodes) == 0 {
				delete(r.targetMap, target)
			}
		}
	}
	for target, tm := range info.Targets {
		if tm.Length == 0 {
			// Process target blocks
			delBlocks(target, tm.Blocks)
		} else {
			// Process whole target
			delTarget(target)
		}
	}
}

// reconcilePeer merges a peer's full metaInfo dump exchanged via ExchangeTracker. Unlike
// addMetaInfo, which only unions, it also removes blocks the peer no longer holds: the dump is the
// peer's complete current state, so any block recorded locally for this peer but absent from the
// dump is stale and dropped. Returns (targetCount, addedBlocks) to match addMetaInfo's stats.
func (r *peerCache) reconcilePeer(info *metaInfo) (int, int) {
	if info == nil || info.SrcAddr == "" || r.fromLocal(info.SrcAddr) {
		return 0, 0
	}
	host := info.SrcAddr

	// Expand the dump into the desired per-target block sets. Whole targets (Length != 0,
	// Blocks == nil) are expanded to a full bitset, matching addMetaInfo's addTarget.
	newSet := make(map[string]*bitset.BitSet, len(info.Targets))
	whole := make(map[string]uint64) // target -> length, for whole targets only
	for target, tm := range info.Targets {
		if tm.Length == 0 {
			if tm.Blocks == nil {
				continue
			}
			newSet[target] = tm.Blocks
		} else {
			cnt := utils.GetBlockCount(tm.Length, r.config.blockSize)
			bs := bitset.New(uint(cnt))
			for i := uint32(0); i < cnt; i++ {
				bs.Set(uint(i))
			}
			newSet[target] = bs
			whole[target] = tm.Length
		}
	}

	// targetMap upkeep for whole targets (parity with addTarget). Lock order: targetMu then
	// metaMu. On a length mismatch we skip the target, leaving local state untouched.
	skip := make(map[string]struct{})
	r.targetMu.Lock()
	for target, length := range whole {
		if t, ok := r.targetMap[target]; ok {
			if t.length != length {
				logger.Warnf("Announced target %s length %d != local length %d", target, length, t.length)
				skip[target] = struct{}{}
				continue
			}
			t.nodes[host] = struct{}{}
		} else {
			t := &targetInfo{length: length, nodes: make(map[string]struct{})}
			t.nodes[host] = struct{}{}
			r.targetMap[target] = t
		}
	}

	r.metaMu.Lock()
	old := r.peerMap[host]
	// Make skipped (length-mismatch) targets a no-op: keep whatever we currently hold.
	for target := range skip {
		if bs, ok := old[target]; ok {
			newSet[target] = bs
		} else {
			delete(newSet, target)
		}
	}

	var added, removed int

	// Removals: blocks the peer used to hold but the dump no longer lists.
	for target, obs := range old {
		want := newSet[target]
		for i, e := obs.NextSet(0); e; i, e = obs.NextSet(i + 1) {
			if want != nil && want.Test(i) {
				continue
			}
			r.clearPeerBlock(host, target, uint32(i))
			removed++
		}
		// Peer dropped this target entirely; also purge it from targetMap.
		if want == nil {
			if t, ok := r.targetMap[target]; ok {
				delete(t.nodes, host)
				if len(t.nodes) == 0 {
					delete(r.targetMap, target)
				}
			}
		}
	}

	// Additions: blocks in the dump not already recorded for this peer.
	for target, want := range newSet {
		obs := old[target]
		for i, e := want.NextSet(0); e; i, e = want.NextSet(i + 1) {
			if obs != nil && obs.Test(i) {
				continue
			}
			r.setPeerBlock(host, target, uint32(i))
			added++
		}
	}
	r.metaMu.Unlock()
	r.targetMu.Unlock()

	if removed > 0 {
		logger.Debugf("Reconciled peer %s: added %d, removed %d stale blocks", host, added, removed)
	}
	return len(info.Targets), added
}

func (r *peerCache) serveAnnounce() {
	// Wait for serf service to be started
	if r.serfUser != nil {
		r.serfUser.WaitReady()
	}

	// Notify announce channel is ready
	close(r.announceReady)
	for {
		select {
		case <-r.stopCh:
			logger.Info("Stop serving announce event")
			return
		case info := <-r.announceCh:
			if err := r.validateMetaInfo(info); err != nil {
				logger.Warnf("Skip invalid announce meta info: %s", err)
				continue
			}
			r.addMetaInfo(info)

			// broadcast event to cluster if request is from local
			if r.fromLocal(info.SrcAddr) {
				r.broadCast(announceEvent, info)
				continue
			}
			logger.Debugf("Handle announce %v", info)
		}
	}
}

func (r *peerCache) serveRevoke() {
	for {
		select {
		case <-r.stopCh:
			logger.Info("Stop serving revoke event")
			return
		case info := <-r.revokeCh:
			if err := r.validateMetaInfo(info); err != nil {
				logger.Warnf("Skip invalid revoke meta info: %s", err)
				continue
			}
			r.delMetaInfo(info)

			// broadcast event to cluster if request is from local
			if r.fromLocal(info.SrcAddr) {
				r.broadCast(revokeEvent, info)
				continue
			}
			logger.Debugf("Handle revoke %v", info)
		}
	}
}

func (r *peerCache) setDataReady() {
	r.dataReady.Store(true)
	r.broadCast(dataEvent, &metaInfo{
		SrcAddr:   r.config.localAddr,
		BlockSize: r.config.blockSize,
	})
}

func (r *peerCache) stop() {
	logger.Info("Stop cachefs remote cache")

	if r.transport != nil {
		r.transport.stop()
	}
	if r.cacheroot != nil {
		r.cacheroot.stop()
	}
	if r.stopCh != nil {
		close(r.stopCh)
		r.stopCh = nil
	}
}

func (r *peerCache) addSeeders(addrs []string) {
	logger.Debugf("Processing new seeders: %v", addrs)
	added := make([]string, 0, len(addrs))
	r.seederMu.Lock()
	for _, addr := range addrs {
		if _, ok := r.seeders[addr]; ok {
			continue
		}
		r.seeders[addr] = struct{}{}
		added = append(added, addr)
	}
	count := len(r.seeders)
	r.seederMu.Unlock()
	if len(added) > 0 {
		logger.Infof("Added %v as new seeders, total count: %d", added, count)
	}
}
