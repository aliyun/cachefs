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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aliyun/cachefs/pkg/consistent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/willf/bitset"
)

// With serf nil, hashHit returns false, so no block counts as a hash hit and
// only the resident aggregates are populated.
func TestMCacheGetCacheState(t *testing.T) {
	c := &mcache{
		store:    &store{}, // serf == nil -> hashHit always false
		capacity: 4096,
		used:     3072,
		pages:    make(map[CacheKey]memItem),
	}
	c.pages[CacheKey{"t1", 0}] = memItem{atime: time.Now(), page: &Page{Data: make([]byte, 1024)}}
	c.pages[CacheKey{"t1", 1}] = memItem{atime: time.Now(), page: &Page{Data: make([]byte, 2048)}}

	st := c.getCacheState()
	assert.Equal(t, int64(4096), st.Capacity)
	assert.Equal(t, int64(3072), st.Used)
	assert.Equal(t, int64(2), st.CachedBlocks)
	assert.Equal(t, int64(3072), st.CachedBytes)
	assert.Equal(t, int64(0), st.HashHitBlocks)
	assert.Equal(t, int64(0), st.HashHitBytes)
	assert.Equal(t, int64(0), st.PinnedBlocks) // pinCache nil -> nothing pinned
	assert.Equal(t, int64(0), st.PinnedBytes)
}

// diskCache counts both on-disk keys and in-memory pages as cached.
func TestDiskCacheGetCacheState(t *testing.T) {
	cache := &diskCache{
		store:    &store{}, // serf == nil -> hashHit always false
		capacity: 8192,
		used:     100,
		keys:     make(map[CacheKey]cacheItem),
		pages:    make(map[CacheKey]*Page),
	}
	cache.keys[CacheKey{"t1", 0}] = cacheItem{size: 4096}
	cache.keys[CacheKey{"t1", 1}] = cacheItem{size: 512}
	cache.pages[CacheKey{"t2", 0}] = &Page{Data: make([]byte, 256)}

	st := cache.getCacheState()
	assert.Equal(t, int64(8192), st.Capacity)
	assert.Equal(t, int64(100), st.Used)
	assert.Equal(t, int64(3), st.CachedBlocks)
	assert.Equal(t, int64(4864), st.CachedBytes)
	assert.Equal(t, int64(0), st.HashHitBlocks)
	assert.Equal(t, int64(0), st.HashHitBytes)
	assert.Equal(t, int64(0), st.PinnedBlocks) // pinCache nil -> nothing pinned
	assert.Equal(t, int64(0), st.PinnedBytes)
}

// A store with a pinned prefix marks matching cached blocks as pinned across
// both backends; non-matching targets stay unpinned.
func TestGetCacheStatePinned(t *testing.T) {
	newStore := func() *store {
		s := &store{pinCache: newPinCache()}
		s.pinCache.add([]string{"t1"}) // internal-key prefix; "t1" keys are pinned
		return s
	}

	t.Run("mcache", func(t *testing.T) {
		c := &mcache{
			store: newStore(),
			pages: make(map[CacheKey]memItem),
		}
		c.pages[CacheKey{"t1", 0}] = memItem{atime: time.Now(), page: &Page{Data: make([]byte, 1024)}}
		c.pages[CacheKey{"t1", 1}] = memItem{atime: time.Now(), page: &Page{Data: make([]byte, 2048)}}
		c.pages[CacheKey{"t2", 0}] = memItem{atime: time.Now(), page: &Page{Data: make([]byte, 512)}}

		st := c.getCacheState()
		assert.Equal(t, int64(3), st.CachedBlocks)
		assert.Equal(t, int64(2), st.PinnedBlocks)   // only the two t1 blocks
		assert.Equal(t, int64(3072), st.PinnedBytes) // 1024 + 2048
	})

	t.Run("diskCache", func(t *testing.T) {
		cache := &diskCache{
			store: newStore(),
			keys:  make(map[CacheKey]cacheItem),
			pages: make(map[CacheKey]*Page),
		}
		cache.keys[CacheKey{"t1", 0}] = cacheItem{size: 4096}
		cache.keys[CacheKey{"t1", 1}] = cacheItem{size: 512}
		cache.pages[CacheKey{"t2", 0}] = &Page{Data: make([]byte, 256)}

		st := cache.getCacheState()
		assert.Equal(t, int64(3), st.CachedBlocks)
		assert.Equal(t, int64(2), st.PinnedBlocks)   // both t1 keys
		assert.Equal(t, int64(4608), st.PinnedBytes) // 4096 + 512
	})
}

// hashRingSerf is a SerfService whose HashHit consults a real consistent hash
// ring, so the benchmark below pays the true per-block xxhash + ring lookup cost
// instead of the MockSerfService's constant-time stub.
type hashRingSerf struct {
	MockSerfService
	ring     *consistent.ConsistentHashRing
	localKey string
}

func (s *hashRingSerf) HashHit(key string) bool {
	addr, err := s.ring.Get(key)
	return err == nil && addr == s.localKey
}

// newBenchDiskCache builds a full diskCache with nblocks resident keys, backed by
// a store with a real hash ring (nnodes members) so GetCacheState exercises the
// real hashHit path. All blocks belong to a single instance's local index.
func newBenchDiskCache(nblocks, nnodes int) *diskCache {
	ring := consistent.NewHashRing(100)
	for i := 0; i < nnodes; i++ {
		ring.AddToHashring(fmt.Sprintf("10.0.0.%d:9000", i))
	}
	store := &store{
		pinCache: newPinCache(),
		serf:     &hashRingSerf{ring: ring, localKey: "10.0.0.0:9000"},
	}
	cache := &diskCache{
		store:    store,
		capacity: int64(nblocks) << 20,
		keys:     make(map[CacheKey]cacheItem, nblocks),
		pages:    make(map[CacheKey]*Page),
	}
	for i := 0; i < nblocks; i++ {
		cache.keys[CacheKey{Target: fmt.Sprintf("cfs/vol/blocks/%08d", i)}] = cacheItem{size: 1 << 20}
		cache.used += 1 << 20
	}
	return cache
}

// BenchmarkGetCacheState measures a single diskCache.getCacheState() scan (held
// under the backend's exclusive lock) at a few cache sizes. 122880 blocks is the
// 15 TiB / 128 MiB full-cache case; smaller block sizes scale the block count up.
func BenchmarkGetCacheState(b *testing.B) {
	// 15 TiB divided by 128 MiB / 32 MiB / 8 MiB block sizes.
	for _, nblocks := range []int{122880, 491520, 1966080} {
		cache := newBenchDiskCache(nblocks, 10)
		b.Run(fmt.Sprintf("blocks=%d", nblocks), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = cache.getCacheState()
			}
		})
	}
}

// newBenchPeerCache builds a peerCache whose metaMap holds ntargets targets, each
// with nblocks blocks, every block held by nnodes nodes. This exercises the full
// node-aware bitset build in trackerSnapshot.
func newBenchPeerCache(ntargets, nblocks, nnodes int) *peerCache {
	store := &store{
		config:       &StoreConfig{KeyPrefix: "kp"},
		sourcePrefix: "/sp",
	}
	meta := make(map[string]map[uint32]map[string]struct{}, ntargets)
	nodes := make([]string, nnodes)
	for n := 0; n < nnodes; n++ {
		nodes[n] = fmt.Sprintf("10.0.0.%d:9000", n)
	}
	for t := 0; t < ntargets; t++ {
		key := fmt.Sprintf("kp/sp/vol/blocks/%08d", t)
		blockMap := make(map[uint32]map[string]struct{}, nblocks)
		for b := 0; b < nblocks; b++ {
			hosts := make(map[string]struct{}, nnodes)
			for _, n := range nodes {
				hosts[n] = struct{}{}
			}
			blockMap[uint32(b)] = hosts
		}
		meta[key] = blockMap
	}
	return &peerCache{store: store, metaMap: meta}
}

// BenchmarkTrackerSnapshot measures a single trackerSnapshot over metaMaps of a
// few sizes (targets x blocks-per-target, 3 nodes each). The whole scan and the
// per-node bitset build run under metaMu.RLock, so concurrent trackerSnapshot and
// findCandidates readers do not block each other.
func BenchmarkTrackerSnapshot(b *testing.B) {
	cases := []struct{ ntargets, nblocks int }{
		{1000, 128},
		{10000, 128},
		{1000, 1024},
	}
	for _, c := range cases {
		r := newBenchPeerCache(c.ntargets, c.nblocks, 3)
		b.Run(fmt.Sprintf("targets=%d/blocks=%d", c.ntargets, c.nblocks), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = r.trackerSnapshot(nil)
			}
		})
	}
}

// BenchmarkTrackerSnapshotContention measures the findCandidates hot path (also
// the cacheroot Query path) while a background goroutine continuously runs
// trackerSnapshot. With metaMu as an RWMutex both take RLock and run
// concurrently, so findCandidates latency stays low instead of being blocked for
// the whole snapshot duration.
func BenchmarkTrackerSnapshotContention(b *testing.B) {
	cases := []struct{ ntargets, nblocks int }{
		{10000, 128},
		{1000, 1024},
	}
	for _, c := range cases {
		r := newBenchPeerCache(c.ntargets, c.nblocks, 3)
		keys := make([]string, c.ntargets)
		for t := 0; t < c.ntargets; t++ {
			keys[t] = fmt.Sprintf("kp/sp/vol/blocks/%08d", t)
		}
		b.Run(fmt.Sprintf("targets=%d/blocks=%d", c.ntargets, c.nblocks), func(b *testing.B) {
			stop := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
						_ = r.trackerSnapshot(nil)
					}
				}
			}()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = r.findCandidates(keys[i%len(keys)], 0, 1, false)
			}
			b.StopTimer()
			close(stop)
			wg.Wait()
		})
	}
}

// --- Mutex vs RWMutex under a concurrent writer -------------------------------
//
// The benchmarks below model the exact metaMap access pattern with the lock type
// as the only variable, so Mutex and RWMutex can be compared in one run. Three
// concurrent op kinds mirror the real code:
//   - reader   : findCandidates-like short read (the timed hot path)
//   - snapshot : trackerSnapshot-like long read, run continuously in background
//   - writer   : addBlocks-like short write, run by N background goroutines
// Under RWMutex a pending writer blocks new RLock readers (writer priority), so
// this shows how much the reader-reader concurrency win survives write pressure.

type metaMapT = map[string]map[uint32]map[string]struct{}

func buildMetaMap(ntargets, nblocks, nnodes int) (metaMapT, []string) {
	nodes := make([]string, nnodes)
	for n := 0; n < nnodes; n++ {
		nodes[n] = fmt.Sprintf("10.0.0.%d:9000", n)
	}
	m := make(metaMapT, ntargets)
	keys := make([]string, ntargets)
	for t := 0; t < ntargets; t++ {
		key := fmt.Sprintf("blocks/%08d", t)
		keys[t] = key
		bm := make(map[uint32]map[string]struct{}, nblocks)
		for b := 0; b < nblocks; b++ {
			hosts := make(map[string]struct{}, nnodes+1)
			for _, n := range nodes {
				hosts[n] = struct{}{}
			}
			bm[uint32(b)] = hosts
		}
		m[key] = bm
	}
	return m, keys
}

// readerOp mirrors findCandidates: look up one block's hosts and collect one.
func readerOp(m metaMapT, target string) {
	if bm, ok := m[target]; ok {
		if hm, ok := bm[0]; ok {
			for range hm {
				break
			}
		}
	}
}

// snapshotOp mirrors trackerSnapshot: scan the whole map building per-node bitsets.
func snapshotOp(m metaMapT) {
	out := make(map[string]map[string]*bitset.BitSet, len(m))
	for key, bm := range m {
		perNode := make(map[string]*bitset.BitSet)
		for blk, hosts := range bm {
			for node := range hosts {
				bs := perNode[node]
				if bs == nil {
					bs = bitset.New(0)
					perNode[node] = bs
				}
				bs.Set(uint(blk))
			}
		}
		out[key] = perNode
	}
	_ = out
}

// writerOp mirrors addBlocks: toggle a writer-owned host on one block. Uses a
// dedicated host name so it never removes the base nodes the readers count on.
func writerOp(m metaMapT, keys []string, wid, i int) {
	bm := m[keys[i%len(keys)]]
	hm := bm[uint32(i%len(bm))]
	host := fmt.Sprintf("w%d:9000", wid)
	if _, ok := hm[host]; ok {
		delete(hm, host)
	} else {
		hm[host] = struct{}{}
	}
}

// runLockContention starts one background snapshotter and `writers` background
// writers, then times the reader op. reader/snap/write acquire the lock via the
// supplied closures (RLock for reads under RWMutex, Lock otherwise).
func runLockContention(b *testing.B, keys []string, writers int, rd func(string), snap func(), wr func(wid, i int)) {
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				snap()
			}
		}
	}()
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
					wr(wid, i)
				}
			}
		}(w)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rd(keys[i%len(keys)])
	}
	b.StopTimer()
	close(stop)
	wg.Wait()
}

// BenchmarkMetaLockUnderWriter compares Mutex vs RWMutex for the reader hot path
// while a long snapshot reader and N short writers run concurrently.
func BenchmarkMetaLockUnderWriter(b *testing.B) {
	const ntargets, nblocks, nnodes = 500, 64, 3
	for _, writers := range []int{0, 1, 4} {
		m, keys := buildMetaMap(ntargets, nblocks, nnodes)
		var mu sync.Mutex
		b.Run(fmt.Sprintf("mutex/writers=%d", writers), func(b *testing.B) {
			runLockContention(b, keys, writers,
				func(t string) { mu.Lock(); readerOp(m, t); mu.Unlock() },
				func() { mu.Lock(); snapshotOp(m); mu.Unlock() },
				func(wid, i int) { mu.Lock(); writerOp(m, keys, wid, i); mu.Unlock() },
			)
		})

		m2, keys2 := buildMetaMap(ntargets, nblocks, nnodes)
		var rw sync.RWMutex
		b.Run(fmt.Sprintf("rwmutex/writers=%d", writers), func(b *testing.B) {
			runLockContention(b, keys2, writers,
				func(t string) { rw.RLock(); readerOp(m2, t); rw.RUnlock() },
				func() { rw.RLock(); snapshotOp(m2); rw.RUnlock() },
				func(wid, i int) { rw.Lock(); writerOp(m2, keys2, wid, i); rw.Unlock() },
			)
		})
	}
}

// --- Non-cacheroot workload: concurrent short reads + short writes, no snapshot -
//
// This models the main (non-cacheroot) deployment: every node runs high-frequency
// findCandidates reads for its own peer loads while batch scale-out drives many
// addBlocks writes, but there is NO trackerSnapshot (no long reader). RWMutex's only
// advantage (long-read / short-read concurrency) is absent here, so this isolates
// its downside: higher RLock overhead and writer-priority serialization under write
// pressure. Readers run via RunParallel (GOMAXPROCS concurrent) to expose read-read
// concurrency; N background writers apply scale-out write pressure.

// runParallelReaders starts `writers` background writers, then times the reader op
// across GOMAXPROCS parallel goroutines. No snapshotter runs.
func runParallelReaders(b *testing.B, keys []string, writers int, rd func(string), wr func(wid, i int)) {
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
					wr(wid, i)
				}
			}
		}(w)
	}
	var ctr int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(atomic.AddInt64(&ctr, 1))
		for pb.Next() {
			rd(keys[i%len(keys)])
			i++
		}
	})
	b.StopTimer()
	close(stop)
	wg.Wait()
}

// BenchmarkMetaLockNoSnapshot compares Mutex vs RWMutex for the non-cacheroot
// workload: parallel short reads plus N background short writers, no snapshot.
func BenchmarkMetaLockNoSnapshot(b *testing.B) {
	// ~500G / 128M per node -> ~4000 targets; a handful of blocks each.
	const ntargets, nblocks, nnodes = 4000, 32, 3
	for _, writers := range []int{0, 4, 16} {
		m, keys := buildMetaMap(ntargets, nblocks, nnodes)
		var mu sync.Mutex
		b.Run(fmt.Sprintf("mutex/writers=%d", writers), func(b *testing.B) {
			runParallelReaders(b, keys, writers,
				func(t string) { mu.Lock(); readerOp(m, t); mu.Unlock() },
				func(wid, i int) { mu.Lock(); writerOp(m, keys, wid, i); mu.Unlock() },
			)
		})

		m2, keys2 := buildMetaMap(ntargets, nblocks, nnodes)
		var rw sync.RWMutex
		b.Run(fmt.Sprintf("rwmutex/writers=%d", writers), func(b *testing.B) {
			runParallelReaders(b, keys2, writers,
				func(t string) { rw.RLock(); readerOp(m2, t); rw.RUnlock() },
				func(wid, i int) { rw.Lock(); writerOp(m2, keys2, wid, i); rw.Unlock() },
			)
		})
	}
}

// trackerSnapshot strips the KeyPrefix+sourcePrefix from internal keys, sets a
// block bit only when at least one node holds it, and honors prefix filtering.
func TestPeerCacheTrackerSnapshot(t *testing.T) {
	store := &store{
		config:       &StoreConfig{KeyPrefix: "kp"},
		sourcePrefix: "/sp",
	}
	// Internal key == genTarget("/a.txt") == "kp" + "/sp" + "/a.txt".
	r := &peerCache{
		store: store,
		metaMap: map[string]map[uint32]map[string]struct{}{
			"kp/sp/a.txt": {
				0: {"node1": {}},              // held -> bit set
				1: {},                         // no host -> bit clear
				2: {"node1": {}, "node2": {}}, // held -> bit set
			},
		},
	}

	// No prefixes: whole map, key stripped back to "/a.txt", node-aware.
	out := r.trackerSnapshot(nil)
	require.Contains(t, out, "/a.txt")
	perNode := out["/a.txt"]
	// node1 holds blocks 0 and 2; node2 holds only block 2.
	require.Contains(t, perNode, "node1")
	require.Contains(t, perNode, "node2")
	assert.True(t, perNode["node1"].Test(0))
	assert.False(t, perNode["node1"].Test(1))
	assert.True(t, perNode["node1"].Test(2))
	assert.False(t, perNode["node2"].Test(0))
	assert.True(t, perNode["node2"].Test(2))
	// Cluster presence is the union across nodes: blocks 0 and 2 set, block 1 clear
	// (its host set was empty).
	union := bitset.New(0)
	for _, bs := range perNode {
		union.InPlaceUnion(bs)
	}
	assert.True(t, union.Test(0))
	assert.False(t, union.Test(1))
	assert.True(t, union.Test(2))

	// Matching prefix keeps the target.
	out = r.trackerSnapshot([]string{"/a.txt"})
	assert.Contains(t, out, "/a.txt")

	// Non-matching prefix filters everything out.
	out = r.trackerSnapshot([]string{"/other"})
	assert.Empty(t, out)
}

func TestLocalTrackerSnapshot(t *testing.T) {
	// /a.txt is partially cached (blocks 0 and 2); /b.txt is fully cached and
	// dump reports a nil bitset with the target length.
	partial := bitset.New(0)
	partial.Set(0)
	partial.Set(2)
	bcache := &MockLocalCache{}
	bcache.On("dump").Return(&metaInfo{
		Targets: map[string]*targetMeta{
			"kp/sp/a.txt": {Blocks: partial},
			"kp/sp/b.txt": {Length: 10, Blocks: nil}, // BlockSize 4 -> ceil(10/4) = 3 blocks
		},
	})
	store := &store{
		config:       &StoreConfig{KeyPrefix: "kp", BlockSize: 4, CacheAddr: "self:7711"},
		sourcePrefix: "/sp",
		bcache:       bcache,
	}

	out := store.localTrackerSnapshot(nil)
	require.Len(t, out, 2)

	// Partial target: only the local node, blocks 0 and 2 set, block 1 clear.
	a := out["/a.txt"]
	require.Len(t, a, 1)
	require.Contains(t, a, "self:7711")
	assert.True(t, a["self:7711"].Test(0))
	assert.False(t, a["self:7711"].Test(1))
	assert.True(t, a["self:7711"].Test(2))

	// Fully-cached target expanded to blocks [0,3): 0,1,2 set, 3 clear.
	b := out["/b.txt"]["self:7711"]
	assert.True(t, b.Test(0))
	assert.True(t, b.Test(1))
	assert.True(t, b.Test(2))
	assert.False(t, b.Test(3))

	// Prefix filter keeps only the matching target.
	out = store.localTrackerSnapshot([]string{"/a.txt"})
	require.Len(t, out, 1)
	assert.Contains(t, out, "/a.txt")
}
