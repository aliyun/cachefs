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

package chunk

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/aliyun/cachefs/pkg/metric"
	"github.com/aliyun/cachefs/pkg/serf"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/willf/bitset"
)

func TestShouldEvictByPrefix(t *testing.T) {
	tests := []struct {
		name       string
		target     string
		remove     []string
		keep       []string
		wantEvict  bool
		wantPrefix string
	}{
		{
			name:       "remove matched prefix",
			target:     "/modelscope/old/file",
			remove:     []string{"/modelscope/old/", "/modelscope/tmp/"},
			wantEvict:  true,
			wantPrefix: "/modelscope/old/",
		},
		{
			name:      "remove unmatched prefix",
			target:    "/modelscope/keep/file",
			remove:    []string{"/modelscope/old/"},
			wantEvict: false,
		},
		{
			name:      "keep matched prefix",
			target:    "/modelscope/keep2/file",
			keep:      []string{"/modelscope/keep1/", "/modelscope/keep2/"},
			wantEvict: false,
		},
		{
			name:      "keep unmatched prefixes evicts once",
			target:    "/modelscope/drop/file",
			keep:      []string{"/modelscope/keep1/", "/modelscope/keep2/"},
			wantEvict: true,
		},
		{
			name:       "remove takes precedence over keep",
			target:     "/modelscope/drop/file",
			remove:     []string{"/modelscope/drop/"},
			keep:       []string{"/modelscope/drop/"},
			wantEvict:  true,
			wantPrefix: "/modelscope/drop/",
		},
		{
			name:      "empty config does not evict",
			target:    "/modelscope/drop/file",
			wantEvict: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evict, prefix := shouldEvictByPrefix(tt.target, tt.remove, tt.keep)
			assert.Equal(t, tt.wantEvict, evict)
			assert.Equal(t, tt.wantPrefix, prefix)
		})
	}
}

func TestMCacheStats(t *testing.T) {
	c := &mcache{
		pages: make(map[CacheKey]memItem),
		used:  0,
	}

	c.pages[CacheKey{"some-target", 1}] = memItem{atime: time.Now(), page: &Page{Data: make([]byte, 1024)}}
	c.used = 1024

	length, used := c.stats()

	assert.Equal(t, int64(1), length)
	assert.Equal(t, int64(1024), used)
}

type RCacheMock struct {
	Announced    map[string]uint32
	Revoked      map[string]uint32
	MemberList   []string
	LoadBehavior func(ctx context.Context, target string, nrblk, blklen uint32) (*Page, error)
}

func (r *RCacheMock) announce(target string, length uint64, nrblk uint32) {
	r.Announced[target] = nrblk
}

func (r *RCacheMock) revoke(target string, length uint64, nrblk uint32) {
	r.Revoked[target] = nrblk
}

func (r *RCacheMock) setDataReady() {
	return
}

func (r *RCacheMock) load(ctx context.Context, target string, nrblk, blklen uint32) (*Page, error) {
	if r.LoadBehavior != nil {
		return r.LoadBehavior(ctx, target, nrblk, blklen)
	}
	return nil, errors.New("not implemented")
}

func (r *RCacheMock) stop() {}

func (r *RCacheMock) addMetaInfo(remoteMeta *metaInfo) (int, int) {
	return 0, 0
}

func (r *RCacheMock) reconcilePeer(remoteMeta *metaInfo) (int, int) {
	return 0, 0
}

func (r *RCacheMock) trackerSnapshot(prefixes []string) map[string]map[string]*bitset.BitSet {
	return map[string]map[string]*bitset.BitSet{}
}

func NewRCacheMock() *RCacheMock {
	return &RCacheMock{
		Announced: make(map[string]uint32),
		Revoked:   make(map[string]uint32),
	}
}

func TestMCacheCache(t *testing.T) {
	config := &StoreConfig{}
	rcache := NewRCacheMock()
	serfCfg := serf.DefaultSerfConfig()
	serfCfg.BindPort = 0
	ss := serf.NewSerfService(serfCfg, nil)
	assert.NoError(t, ss.Start())
	store := &store{
		config:     config,
		rcache:     rcache,
		serf:       ss,
		counterReg: metric.NewCounterRegistry(),
		pinCache:   newPinCache(),
	}
	c := newMemCache(&StoreConfig{CacheSize: 2048}, store, nil)
	c.pages = make(map[CacheKey]memItem)
	page := &Page{Data: make([]byte, 1024)}

	t.Run("add new page", func(t *testing.T) {
		c.cache("some-target", 1, page)
		cnt, used := c.stats()

		assert.Equal(t, int64(1), cnt)
		assert.Equal(t, int64(1024), used)
		assert.Contains(t, c.pages, CacheKey{"some-target", 1})
		assert.Equal(t, uint32(1), rcache.Announced["some-target"])
	})

	t.Run("add existing page", func(t *testing.T) {
		c.cache("some-target", 1, page)
		cnt, used := c.stats()

		assert.Equal(t, int64(1), cnt)
		assert.Equal(t, int64(1024), used)
	})

	t.Run("exceed capacity", func(t *testing.T) {
		page1 := &Page{Data: make([]byte, 1024)}
		c.cache("some-target-2", 1, page1)
		page2 := &Page{Data: make([]byte, 1024)}
		c.cache("some-target-2", 2, page2)
		cnt, used := c.stats()

		// should evict oldest key
		assert.Equal(t, int64(2048), used)
		assert.Equal(t, int64(2), cnt)
	})
}

func TestMCacheCheck(t *testing.T) {
	config := &StoreConfig{}
	rcache := NewRCacheMock()
	store := &store{
		config:     config,
		rcache:     rcache,
		counterReg: metric.NewCounterRegistry(),
		pinCache:   newPinCache(),
	}
	c := newMemCache(&StoreConfig{CacheSize: 2048}, store, nil)
	c.pages = make(map[CacheKey]memItem)
	page := &Page{Data: make([]byte, 1024)}

	t.Run("check existing cache", func(t *testing.T) {
		c.cache("some-target", 1, page)
		exist := c.check("some-target", 1)
		assert.True(t, exist)
	})
	t.Run("check non-existing cache", func(t *testing.T) {
		c.cache("some-target", 1, page)
		exist := c.check("some-target", 2)
		assert.False(t, exist)
	})
}

func newLRUTestMcache(t *testing.T, limit int, capacity int64) *mcache {
	t.Helper()
	c := newTestMcache(t)
	c.capacity = capacity
	c.burstCap = capacity
	c.lru = newBlockLRU(limit)
	return c
}

func addLRUTestBlock(c *mcache, key CacheKey, size int) {
	p := NewPage(make([]byte, size))
	c.cache(key.Target, key.Blk, p)
	p.Release()
}

// TestMCacheBlockLRUOrdering verifies recency updates and overflow.
// It also verifies that checks and peer reads do not refresh the LRU.
func TestMCacheBlockLRUOrdering(t *testing.T) {
	c := newLRUTestMcache(t, 2, 1024)
	keys := []CacheKey{{"target", 0}, {"target", 1}, {"target", 2}, {"target", 3}}

	addLRUTestBlock(c, keys[0], 100)
	addLRUTestBlock(c, keys[1], 100)
	assert.Equal(t, []CacheKey{keys[0], keys[1]}, lruTestKeys(c.lru))

	addLRUTestBlock(c, keys[0], 100)
	assert.Equal(t, []CacheKey{keys[0], keys[1]}, lruTestKeys(c.lru), "duplicate add must not refresh recency")
	assert.True(t, c.check(keys[0].Target, keys[0].Blk))
	assert.Equal(t, []CacheKey{keys[0], keys[1]}, lruTestKeys(c.lru), "check must not refresh recency")

	addLRUTestBlock(c, keys[2], 100)
	assert.Equal(t, []CacheKey{keys[1], keys[2]}, lruTestKeys(c.lru))
	assert.Contains(t, c.pages, keys[0], "leaving the protection window must not delete the block")

	oldAtime := c.pages[keys[0]].atime
	peerPage, _, err := c.load(keys[0].Target, keys[0].Blk, false)
	if !assert.NoError(t, err) || !assert.NotNil(t, peerPage) {
		return
	}
	peerPage.Release()
	assert.Equal(t, []CacheKey{keys[1], keys[2]}, lruTestKeys(c.lru), "peer reads must not refresh recency")
	assert.True(t, c.pages[keys[0]].atime.After(oldAtime), "peer reads must retain the existing atime update")

	page, reader, err := c.load(keys[1].Target, keys[1].Blk, true)
	if !assert.NoError(t, err) || !assert.Nil(t, reader) || !assert.NotNil(t, page) {
		return
	}
	page.Release()
	assert.Equal(t, []CacheKey{keys[2], keys[1]}, lruTestKeys(c.lru))

	addLRUTestBlock(c, keys[3], 100)
	assert.Equal(t, []CacheKey{keys[1], keys[3]}, lruTestKeys(c.lru))
	assert.Len(t, c.pages, 4)
}

// TestMCacheBlockLRUCapacityCleanup verifies normal eviction outside the window
// and oldest-first fallback within it.
func TestMCacheBlockLRUCapacityCleanup(t *testing.T) {
	keys := []CacheKey{{"target", 0}, {"target", 1}, {"target", 2}}

	t.Run("normal rounds evict blocks outside window", func(t *testing.T) {
		c := newLRUTestMcache(t, 1, 300)
		for i, key := range keys {
			addLRUTestBlock(c, key, 100)
			item := c.pages[key]
			item.atime = time.Now().Add(time.Duration(i) * time.Second)
			c.pages[key] = item
		}

		c.Lock()
		c.doCleanup(200, evictionCleanup, nil)
		c.Unlock()

		assert.NotContains(t, c.pages, keys[0])
		assert.Contains(t, c.pages, keys[1])
		assert.Contains(t, c.pages, keys[2])
		assert.Equal(t, []CacheKey{keys[2]}, lruTestKeys(c.lru))
		assert.Equal(t, float64(0), testutil.ToFloat64(c.lruFallbackEvicts))
	})

	t.Run("fallback evicts oldest protected block", func(t *testing.T) {
		c := newLRUTestMcache(t, 3, 300)
		for _, key := range keys {
			addLRUTestBlock(c, key, 100)
		}

		c.Lock()
		c.doCleanup(200, evictionCleanup, map[CacheKey]struct{}{keys[2]: {}})
		c.Unlock()

		assert.NotContains(t, c.pages, keys[0])
		assert.Contains(t, c.pages, keys[1])
		assert.Contains(t, c.pages, keys[2])
		assert.Equal(t, []CacheKey{keys[1], keys[2]}, lruTestKeys(c.lru))
		assert.Equal(t, int64(200), c.used)
		assert.Equal(t, float64(1), testutil.ToFloat64(c.lruFallbackEvicts))
	})
}

// TestMCacheBlockLRUFallbackHardLimits verifies that fallback honors locked blocks
// and skipKeys while removing evicted LRU entries.
func TestMCacheBlockLRUFallbackHardLimits(t *testing.T) {
	c := newLRUTestMcache(t, 3, 300)
	keys := []CacheKey{{"target", 0}, {"target", 1}, {"target", 2}}
	for _, key := range keys {
		addLRUTestBlock(c, key, 100)
	}
	c.lockTarget(keys[0].Target, []uint32{keys[0].Blk})

	c.Lock()
	c.doCleanup(100, evictionCleanup, map[CacheKey]struct{}{keys[1]: {}})
	c.Unlock()
	assert.Contains(t, c.pages, keys[0])
	assert.Contains(t, c.pages, keys[1])
	assert.NotContains(t, c.pages, keys[2])
	assert.Equal(t, []CacheKey{keys[0], keys[1]}, lruTestKeys(c.lru))
	assert.Equal(t, float64(1), testutil.ToFloat64(c.lruFallbackEvicts))

	c.Lock()
	c.doCleanup(100, evictionCleanup, nil)
	c.Unlock()
	assert.Contains(t, c.pages, keys[0])
	assert.NotContains(t, c.pages, keys[1])
	assert.Equal(t, []CacheKey{keys[0]}, lruTestKeys(c.lru))
	assert.Equal(t, float64(2), testutil.ToFloat64(c.lruFallbackEvicts))

	c.unlockTarget(keys[0].Target, []uint32{keys[0].Blk})
	c.Lock()
	c.doCleanup(0, evictionCleanup, nil)
	c.Unlock()
	assert.Empty(t, c.pages)
	assert.Empty(t, lruTestKeys(c.lru))
	assert.Equal(t, float64(3), testutil.ToFloat64(c.lruFallbackEvicts))
}

// TestMCacheBlockLRURemovalPaths verifies that expiration and prefix removal
// delete LRU entries without counting fallback evictions.
func TestMCacheBlockLRURemovalPaths(t *testing.T) {
	t.Run("remove by prefix", func(t *testing.T) {
		c := newLRUTestMcache(t, 2, 1000)
		drop := CacheKey{"/drop/file", 0}
		keep := CacheKey{"/keep/file", 0}
		addLRUTestBlock(c, drop, 100)
		addLRUTestBlock(c, keep, 100)

		cnt, _ := c.removeByPrefix([]string{"/drop/"}, nil)
		assert.Equal(t, int64(1), cnt)
		assert.NotContains(t, c.pages, drop)
		assert.Equal(t, []CacheKey{keep}, lruTestKeys(c.lru))
		assert.Equal(t, float64(0), testutil.ToFloat64(c.lruFallbackEvicts))
	})

	t.Run("expire", func(t *testing.T) {
		c := newLRUTestMcache(t, 2, 1000)
		expired := CacheKey{"target", 0}
		keep := CacheKey{"target", 1}
		addLRUTestBlock(c, expired, 100)
		addLRUTestBlock(c, keep, 100)

		ectx := newMemEvictionContext()
		c.Lock()
		item := c.pages[expired]
		ectx.age = time.Since(item.atime)
		c.evictCacheItem(ectx, expired, item, evictionExpire)
		c.Unlock()
		c.revokeDel(ectx)

		assert.NotContains(t, c.pages, expired)
		assert.Equal(t, []CacheKey{keep}, lruTestKeys(c.lru))
		assert.Equal(t, float64(0), testutil.ToFloat64(c.lruFallbackEvicts))
	})
}

// TestMCacheBlockLRUCapacityChanges verifies that ClearBurst and SetCacheSize
// can shrink usage through the LRU fallback.
func TestMCacheBlockLRUCapacityChanges(t *testing.T) {
	keys := []CacheKey{{"target", 0}, {"target", 1}, {"target", 2}}

	t.Run("clear burst", func(t *testing.T) {
		c := newLRUTestMcache(t, 3, 300)
		c.capacity = 200
		for _, key := range keys {
			addLRUTestBlock(c, key, 100)
		}

		cnt, freed := c.clearBurst()
		assert.Equal(t, int64(1), cnt)
		assert.Equal(t, int64(100), freed)
		assert.Equal(t, int64(200), c.used)
		assert.Equal(t, []CacheKey{keys[1], keys[2]}, lruTestKeys(c.lru))
		assert.Equal(t, float64(1), testutil.ToFloat64(c.lruFallbackEvicts))
	})

	t.Run("set capacity", func(t *testing.T) {
		c := newLRUTestMcache(t, 3, 300)
		for _, key := range keys {
			addLRUTestBlock(c, key, 100)
		}

		_, _, cnt, freed := c.setCapacity(200, 200)
		assert.Equal(t, int64(1), cnt)
		assert.Equal(t, int64(100), freed)
		assert.Equal(t, int64(200), c.used)
		assert.Equal(t, []CacheKey{keys[1], keys[2]}, lruTestKeys(c.lru))
		assert.Equal(t, float64(1), testutil.ToFloat64(c.lruFallbackEvicts))
	})
}

// TestMCacheBlockLRUDisabled verifies that a zero window preserves the original
// skipKeys behavior without fallback evictions.
func TestMCacheBlockLRUDisabled(t *testing.T) {
	c := newLRUTestMcache(t, 0, 200)
	keys := []CacheKey{{"target", 0}, {"target", 1}, {"target", 2}}
	for _, key := range keys {
		addLRUTestBlock(c, key, 100)
	}

	assert.Len(t, c.pages, 2)
	assert.Contains(t, c.pages, keys[2], "skipKeys must still protect the newly added block")
	assert.Equal(t, int64(200), c.used)
	assert.Empty(t, lruTestKeys(c.lru))
	assert.Equal(t, float64(0), testutil.ToFloat64(c.lruFallbackEvicts))
}

// TestMCacheBlockLRUMetrics verifies LRU block and byte gauges
// and the fallback eviction counter.
func TestMCacheBlockLRUMetrics(t *testing.T) {
	config := &StoreConfig{CacheSize: 1000, BurstCacheSize: 1000, BlockLRUSize: 2}
	store := &store{
		config:     config,
		rcache:     NewRCacheMock(),
		counterReg: metric.NewCounterRegistry(),
		pinCache:   newPinCache(),
	}
	c := newMemCache(config, store, nil)
	keys := []CacheKey{{"target-0", 0}, {"target-1", 0}, {"target-2", 0}}
	addLRUTestBlock(c, keys[0], 10)
	addLRUTestBlock(c, keys[1], 20)
	addLRUTestBlock(c, keys[2], 30)

	assert.Equal(t, float64(2), testutil.ToFloat64(c.lruBlocks))
	assert.Equal(t, float64(50), testutil.ToFloat64(c.lruBytes))
	c.removeByPrefix([]string{keys[0].Target}, nil)
	assert.Equal(t, float64(2), testutil.ToFloat64(c.lruBlocks))
	assert.Equal(t, float64(50), testutil.ToFloat64(c.lruBytes))

	c.Lock()
	c.doCleanup(30, evictionCleanup, nil)
	c.Unlock()
	assert.Equal(t, []CacheKey{keys[2]}, lruTestKeys(c.lru))
	assert.Equal(t, float64(1), testutil.ToFloat64(c.lruBlocks))
	assert.Equal(t, float64(30), testutil.ToFloat64(c.lruBytes))
	assert.Equal(t, float64(1), testutil.ToFloat64(c.lruFallbackEvicts))
}

// func TestMemCleanup(t *testing.T) {
// 	mockRCache := new(MockRemoteCache)
// 	mockStoreConfig, err := NewStoreConfig()
// 	assert.NoError(t, err)

// 	cache := &mcache{
// 		store: &store{
// 			rcache:   mockRCache,
// 			config:   mockStoreConfig,
// 		},
// 		pages:           make(map[CacheKey]memItem, 10),
// 		cacheWrites:     prometheus.NewCounter(prometheus.CounterOpts{}),
// 		cacheEvicts:     prometheus.NewCounter(prometheus.CounterOpts{}),
// 		cacheWriteBytes: prometheus.NewCounter(prometheus.CounterOpts{}),
// 	}
// 	cache.store.config.CacheEviction = HashCleanup

// 	t.Run("Test cleanup evicts cache items when over capacity", func(t *testing.T) {
// 		cache.capacity = 1024 * 1024 * 1024
// 		cache.burstCap = 1024 * 1024 * 1024
// 		cache.used = 1500 * 1024 * 1024
// 		page := &Page{Data: make([]byte, 1024)}

// 		cache.pages = map[CacheKey]memItem{
// 			{"target1", 1}: {atime: time.Now(), page: page},
// 			{"target2", 2}: {atime: time.Now().Add(-5 * time.Minute), page: page},
// 			{"target3", 3}: {atime: time.Now().Add(-10 * time.Minute), page: page},
// 		}
// 		mockRCache.On("revoke", mock.Anything, mock.Anything, mock.Anything)

// 		cache.cleanup(make(map[CacheKey]struct{}))

// 		_, exists := cache.pages[CacheKey{"target3", 3}]
// 		assert.False(t, exists, "The target3 item should have been evicted")
// 		mockRCache.AssertExpectations(t)
// 	})
// }

func TestMemGetBlockFile(t *testing.T) {
	mockRCache := new(MockRemoteCache)
	mockStoreConfig, err := NewStoreConfig()
	assert.NoError(t, err)
	mockStoreConfig.CacheDir = "memory"
	PageMemfd = true
	defer func() { PageMemfd = false }()

	cache := &mcache{
		store: &store{
			rcache: mockRCache,
			config: mockStoreConfig,
		},

		cacheWrites:                 prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheWriteBytes:             prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheEvicts:                 prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheEvictsHashHit:          metric.NewCounter(prometheus.CounterOpts{}),
		cacheEvictsHashMiss:         prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheCleanup:                metric.NewCounter(prometheus.CounterOpts{}),
		cacheCleanupHashHit:         prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheCleanupHashMiss:        prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheExpired:                prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheExpiredHashHit:         prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheExpiredHashMiss:        prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheRemoveByPrefix:         prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheRemoveByPrefixHashHit:  prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheRemoveByPrefixHashMiss: prometheus.NewCounter(prometheus.CounterOpts{}),
	}

	t.Run("Success to get block file", func(t *testing.T) {
		cache.capacity = 1024 * 1024 * 1024
		cache.burstCap = 1024 * 1024 * 1024
		target := "target1"
		key := CacheKey{target, 1}
		memSize := len(target)
		mockPage, err := AllocPage(memSize, target)
		if !assert.NoError(t, err) {
			return
		}
		copy(mockPage.Data, target)
		memItem0 := memItem{
			atime: time.Now(),
			page:  mockPage,
		}
		cache.pages = map[CacheKey]memItem{
			key: memItem0,
		}

		file, err := cache.getBlockFile(target, 1)

		assert.Nil(t, err)
		assert.NotNil(t, file)
		assert.Equal(t, file.Name(), key.String())

		newContent, err := io.ReadAll(file)
		assert.Nil(t, err)
		assert.Equal(t, string(newContent), target)
	})

	t.Run("Error: load not found", func(t *testing.T) {
		cache.capacity = 1024 * 1024 * 1024
		cache.burstCap = 1024 * 1024 * 1024
		target := "target1"
		key := CacheKey{target, 1}
		memItem0 := memItem{
			atime: time.Now(),
			page:  &Page{Data: []byte("mock data")},
		}

		cache.pages = map[CacheKey]memItem{
			key: memItem0,
		}

		file, err := cache.getBlockFile(target, 2)

		assert.NotNil(t, err)
		assert.Nil(t, file)
		assert.Equal(t, err, ErrNotFound)
	})

	t.Run("Error: Memfd is nil", func(t *testing.T) {
		cache.capacity = 1024 * 1024 * 1024
		cache.burstCap = 1024 * 1024 * 1024
		target := "target1"
		key := CacheKey{target, 1}
		memItem0 := memItem{
			atime: time.Now(),
			page:  &Page{Data: []byte("mock data")},
		}

		cache.pages = map[CacheKey]memItem{
			key: memItem0,
		}

		file, err := cache.getBlockFile(target, 1)

		assert.NotNil(t, err)
		assert.Nil(t, file)
		assert.Equal(t, err, ErrNotFound)
	})
}

func TestMemPrepareBlockFile(t *testing.T) {
	mockRCache := new(MockRemoteCache)
	mockStoreConfig, err := NewStoreConfig()
	assert.NoError(t, err)
	mockStoreConfig.CacheDir = "memory"
	mockStoreConfig.Hasher = "crc32"
	PageMemfd = true
	defer func() { PageMemfd = false }()

	cache := &mcache{
		store: &store{
			rcache: mockRCache,
			config: mockStoreConfig,
		},
		toCommit: make(map[CacheKey]*pendingPage, 100),
	}

	t.Run("Success to prepare block file", func(t *testing.T) {
		cache.capacity = 1024 * 1024 * 1024
		cache.burstCap = 1024 * 1024 * 1024
		target := "target1"
		key := CacheKey{target, 1}
		memSize := len(target)

		file, err := cache.prepareBlockFile(target, 1, uint32(memSize))

		assert.Nil(t, err)
		assert.NotNil(t, file)
		assert.Equal(t, file.Name(), key.String())

		newContent, err := io.ReadAll(file)
		assert.Nil(t, err)
		assert.Equal(t, len(newContent), memSize)
	})

	t.Run("First toCommit return ok", func(t *testing.T) {
		cache.capacity = 1024 * 1024 * 1024
		cache.burstCap = 1024 * 1024 * 1024
		target := "target1"
		key := CacheKey{target, 1}
		memSize := len(target)
		cache.toCommit = map[CacheKey]*pendingPage{
			key: {
				page: &Page{Data: []byte("mock data")},
			},
		}

		file, err := cache.prepareBlockFile(target, 1, uint32(memSize))

		assert.Nil(t, err)
		assert.Nil(t, file)
	})
}

func TestMemCommitBlockFile(t *testing.T) {
	mockRCache := new(MockRemoteCache)
	mockStoreConfig, err := NewStoreConfig()
	assert.NoError(t, err)
	mockStoreConfig.CacheDir = "memory"
	hasher := "crc32"
	mockStoreConfig.Hasher = hasher
	PageMemfd = true
	defer func() { PageMemfd = false }()

	cache := &mcache{
		store: &store{
			rcache: mockRCache,
			config: mockStoreConfig,
		},

		cacheWrites:                 prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheWriteBytes:             prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheEvicts:                 prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheEvictsHashHit:          metric.NewCounter(prometheus.CounterOpts{}),
		cacheEvictsHashMiss:         prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheCleanup:                metric.NewCounter(prometheus.CounterOpts{}),
		cacheCleanupHashHit:         prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheCleanupHashMiss:        prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheExpired:                prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheExpiredHashHit:         prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheExpiredHashMiss:        prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheRemoveByPrefix:         prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheRemoveByPrefixHashHit:  prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheRemoveByPrefixHashMiss: prometheus.NewCounter(prometheus.CounterOpts{}),
	}

	t.Run("toCommit return not ok", func(t *testing.T) {
		target := "target1"

		err = cache.commitBlockFile(target, 1, true)

		assert.NotNil(t, err)
	})

	t.Run("Not commit", func(t *testing.T) {
		cache.capacity = 1024 * 1024 * 1024
		cache.burstCap = 1024 * 1024 * 1024
		target := "target1"
		key := CacheKey{target, 1}
		memSize := len(target)
		mockPage, err := AllocPage(memSize, target)
		if !assert.NoError(t, err) {
			return
		}
		copy(mockPage.Data, target)
		memItem0 := memItem{
			atime: time.Now(),
			page:  mockPage,
		}
		cache.pages = map[CacheKey]memItem{
			key: memItem0,
		}
		cache.toCommit = map[CacheKey]*pendingPage{
			key: {
				page: mockPage,
			},
		}

		err = cache.commitBlockFile(target, 1, false)

		assert.Nil(t, err)
	})

	t.Run("commit success", func(t *testing.T) {
		cache.capacity = 1024 * 1024 * 1024
		cache.burstCap = 1024 * 1024 * 1024
		target := "target1"
		key := CacheKey{target, 1}
		memSize := len(target)
		mockPage, err := AllocPage(memSize, target)
		if !assert.NoError(t, err) {
			return
		}
		copy(mockPage.Data, target)
		mockPage.Hasher = hasher
		cache.pages = map[CacheKey]memItem{}
		cache.toCommit = map[CacheKey]*pendingPage{
			key: {
				page: mockPage,
				size: uint32(memSize),
			},
		}

		mockRCache.On("announce", target, uint64(0), uint32(1)).Once()

		err = cache.commitBlockFile(target, 1, true)

		assert.Nil(t, err)
		mockRCache.AssertExpectations(t)
	})

	t.Run("stat.Blocks is 0", func(t *testing.T) {
		cache.capacity = 1024 * 1024 * 1024
		cache.burstCap = 1024 * 1024 * 1024
		target := "target1"
		key := CacheKey{target, 1}
		memSize := len(target)
		mockPage, err := AllocPage(memSize, target)
		if !assert.NoError(t, err) {
			return
		}
		copy(mockPage.Data, "")
		memItem0 := memItem{
			atime: time.Now(),
			page:  mockPage,
		}
		cache.pages = map[CacheKey]memItem{
			key: memItem0,
		}
		cache.toCommit = map[CacheKey]*pendingPage{
			key: {
				page: mockPage,
				size: uint32(memSize),
			},
		}

		err = cache.commitBlockFile(target, 1, true)

		assert.NotNil(t, err)
	})

	t.Run("stat.Size not equal to int64(pp.size)", func(t *testing.T) {
		cache.capacity = 1024 * 1024 * 1024
		cache.burstCap = 1024 * 1024 * 1024
		target := "target1"
		key := CacheKey{target, 1}
		memSize := len(target)
		mockPage, err := AllocPage(memSize, target)
		if !assert.NoError(t, err) {
			return
		}
		copy(mockPage.Data, target)
		memItem0 := memItem{
			atime: time.Now(),
			page:  mockPage,
		}
		cache.pages = map[CacheKey]memItem{
			key: memItem0,
		}
		cache.toCommit = map[CacheKey]*pendingPage{
			key: {
				page: mockPage,
			},
		}

		err = cache.commitBlockFile(target, 1, true)

		assert.NotNil(t, err)
	})
}

func TestMemDump(t *testing.T) {
	cache := &mcache{
		pages: make(map[CacheKey]memItem),
	}

	t.Run("success to dump", func(t *testing.T) {
		target := "target"
		target1 := "target1"
		key1 := CacheKey{target, 1}
		key2 := CacheKey{target, 2}
		key3 := CacheKey{target1, 1}
		cache.pages[key1] = memItem{}
		cache.pages[key2] = memItem{}
		cache.pages[key3] = memItem{}

		result := cache.dump()

		assert.NotNil(t, result)
		assert.Len(t, result.Targets, 2)
	})
}

func TestGetExpireInterval(t *testing.T) {
	tests := []struct {
		name         string
		cacheExpire  time.Duration
		wantInterval time.Duration
	}{
		{
			name:         "zero duration",
			cacheExpire:  0,
			wantInterval: 0,
		},
		{
			name:         "very short expiration (1ns)",
			cacheExpire:  1,
			wantInterval: 1,
		},
		{
			name:         "short expiration (30s)",
			cacheExpire:  30 * time.Second,
			wantInterval: 30 * time.Second,
		},
		{
			name:         "exactly 1 minute",
			cacheExpire:  time.Minute,
			wantInterval: time.Minute,
		},
		{
			name:         "slightly over 1 minute (1m1s)",
			cacheExpire:  61 * time.Second,
			wantInterval: 30*time.Second + 500*time.Millisecond,
		},
		{
			name:         "medium expiration (2h)",
			cacheExpire:  2 * time.Hour,
			wantInterval: 1 * time.Hour,
		},
		{
			name:         "exactly 24 hours",
			cacheExpire:  24 * time.Hour,
			wantInterval: 12 * time.Hour,
		},
		{
			name:         "long expiration (25h)",
			cacheExpire:  25 * time.Hour,
			wantInterval: 24 * time.Hour,
		},
		{
			name:         "very long expiration (1 week)",
			cacheExpire:  7 * 24 * time.Hour,
			wantInterval: 24 * time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getExpireInterval(tt.cacheExpire); got != tt.wantInterval {
				t.Errorf("getExpireInterval() = %v, want %v", got, tt.wantInterval)
			}
		})
	}
}

func TestMCacheRemoveByPrefixKeepMultiplePrefixes(t *testing.T) {
	cache := newTestMcache(t)

	keepKey := CacheKey{Target: "/modelscope/qwen/Qwen3-4B/file", Blk: 1}
	evictKey := CacheKey{Target: "/modelscope/old/Model/file", Blk: 2}
	keepPage := NewPage(make([]byte, 10))
	evictPage := NewPage(make([]byte, 20))
	keepPage.Acquire()
	evictPage.Acquire()
	cache.pages[keepKey] = memItem{atime: time.Now(), page: keepPage}
	cache.pages[evictKey] = memItem{atime: time.Now(), page: evictPage}
	cache.used = 30

	cnt, freed := cache.removeByPrefix(nil, []string{
		"/modelscope/qwen/Qwen3.5-0.4B/",
		"/modelscope/qwen/Qwen3.5-0.8B/",
		"/modelscope/qwen/Qwen3-4B/",
		"/modelscope/deepseek/DeepSeek-OCR/",
		"/modelscope/MiniMax/MiniMax-M2/",
	})

	assert.Equal(t, int64(1), cnt)
	assert.Equal(t, int64(20), freed)
	assert.Contains(t, cache.pages, keepKey)
	assert.NotContains(t, cache.pages, evictKey)
	assert.Equal(t, int32(2), keepPage.refs)
	assert.Equal(t, int32(1), evictPage.refs)
}

func newTestMcache(t *testing.T) *mcache {
	t.Helper()
	mockStoreConfig, err := NewStoreConfig()
	require.NoError(t, err)

	serfCfg := serf.DefaultSerfConfig()
	serfCfg.BindPort = 0
	ss := serf.NewSerfService(serfCfg, nil)
	if err := ss.Start(); err != nil {
		t.Skipf("Start serf service failed: %s", err)
	}

	// Use RCacheMock which does not enforce strict call expectations,
	// since evictCacheItem always calls rcache.revoke for each evicted item.
	rcache := NewRCacheMock()

	return &mcache{
		store: &store{
			rcache:   rcache,
			config:   mockStoreConfig,
			serf:     ss,
			pinCache: newPinCache(),
		},
		pages:        make(map[CacheKey]memItem),
		lru:          newBlockLRU(0),
		lockedBlocks: make(map[string]map[int]int),
		targets:      make(map[string]*targetItem),

		cacheWrites:                 prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheWriteBytes:             prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheEvicts:                 prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheEvictsHashHit:          metric.NewCounter(prometheus.CounterOpts{}),
		cacheEvictsHashMiss:         prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheCleanup:                metric.NewCounter(prometheus.CounterOpts{}),
		cacheCleanupHashHit:         prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheCleanupHashMiss:        prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheExpired:                prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheExpiredHashHit:         prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheExpiredHashMiss:        prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheRemoveByPrefix:         prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheRemoveByPrefixHashHit:  prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheRemoveByPrefixHashMiss: prometheus.NewCounter(prometheus.CounterOpts{}),
		burstEvicts:                 prometheus.NewCounter(prometheus.CounterOpts{}),
		burstEvictBytes:             prometheus.NewCounter(prometheus.CounterOpts{}),
		evictsSkipLocked:            prometheus.NewCounter(prometheus.CounterOpts{}),
		evictsSkipPinned:            prometheus.NewCounter(prometheus.CounterOpts{}),
		evictsLocked:                prometheus.NewCounter(prometheus.CounterOpts{}),
		lruFallbackEvicts:           prometheus.NewCounter(prometheus.CounterOpts{}),
	}
}

func TestMemClearBurst(t *testing.T) {
	tests := []struct {
		name      string
		capacity  int64
		burstCap  int64
		setupFn   func(cache *mcache)
		wantCnt   int64 // 0 means exactly 0; -1 means > 0
		wantBytes int64 // 0 means exactly 0; -1 means > 0
		checkUsed func(t *testing.T, cache *mcache)
	}{
		{
			name:     "no burst when used <= capacity",
			capacity: 1024,
			burstCap: 2048,
			setupFn: func(cache *mcache) {
				cache.used = 512
			},
			wantCnt:   0,
			wantBytes: 0,
		},
		{
			name:     "burst eviction reduces used to capacity",
			capacity: 100,
			burstCap: 300,
			setupFn: func(cache *mcache) {
				// Three pages of 50 bytes each: used=150 > capacity=100.
				for i := uint32(1); i <= 3; i++ {
					page := &Page{Data: make([]byte, 50)}
					page.Acquire()
					cache.pages[CacheKey{"target", i}] = memItem{
						atime: time.Now().Add(-time.Duration(i) * time.Second),
						page:  page,
					}
				}
				cache.used = 150
			},
			wantCnt:   -1,
			wantBytes: -1,
			checkUsed: func(t *testing.T, cache *mcache) {
				assert.LessOrEqual(t, cache.used, cache.capacity)
			},
		},
		{
			// Only 1 page exists: compare mode requires >= 2 pages to form a pair, so no
			// eviction is possible. The RETRY terminates immediately without looping.
			name:     "single page - used > capacity but too few pages to compare, clearBurst returns without eviction",
			capacity: 50,
			burstCap: 200,
			setupFn: func(cache *mcache) {
				page := &Page{Data: make([]byte, 100)} // size=100 > capacity=50
				page.Acquire()
				cache.pages[CacheKey{"target", 1}] = memItem{
					atime: time.Now().Add(-time.Second),
					page:  page,
				}
				cache.used = 100
			},
			wantCnt:   0,
			wantBytes: 0,
		},
		{
			// All pages belong to an incomplete target and are always skipped by shouldSkip.
			// Every compare pass makes 0 evictions, so the RETRY terminates without looping.
			name:     "incomplete target - all pages skipped every pass, clearBurst returns without eviction",
			capacity: 50,
			burstCap: 200,
			setupFn: func(cache *mcache) {
				for i := uint32(1); i <= 2; i++ {
					page := &Page{Data: make([]byte, 50)}
					page.Acquire()
					cache.pages[CacheKey{"t", i}] = memItem{
						atime: time.Now().Add(-time.Duration(i) * time.Second),
						page:  page,
					}
				}
				cache.used = 100
				// Incomplete target: shouldSkip returns true for all pages of "t".
				cache.targetMu.Lock()
				cache.targets["t"] = &targetItem{atime: time.Now(), length: 100, complete: false}
				cache.targetMu.Unlock()
			},
			wantCnt:   0,
			wantBytes: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := newTestMcache(t)
			cache.capacity = tt.capacity
			cache.burstCap = tt.burstCap
			tt.setupFn(cache)

			cnt, bytes := cache.clearBurst()

			if tt.wantCnt == 0 {
				assert.Equal(t, int64(0), cnt)
			} else {
				assert.Greater(t, cnt, int64(0))
			}
			if tt.wantBytes == 0 {
				assert.Equal(t, int64(0), bytes)
			} else {
				assert.Greater(t, bytes, int64(0))
			}
			if tt.checkUsed != nil {
				tt.checkUsed(t, cache)
			}
		})
	}
}

func TestNewMemCacheBurstCap(t *testing.T) {
	tests := []struct {
		name        string
		cacheSize   int64
		burstSize   int64
		deferBurst  bool
		wantInitial int64
		wantBurst   int64
	}{
		{"burst enabled at startup", 100, 400, false, 400, 400},
		{"burst disabled at startup", 100, 0, false, 100, 100},
		{"burst equal to cache size", 100, 100, false, 100, 100},
		{"burst smaller than cache size", 100, 50, false, 100, 100},
		{"burst deferred at startup", 100, 400, true, 400, 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &StoreConfig{
				CacheSize:  tt.cacheSize,
				DeferBurst: tt.deferBurst,
			}
			// doNewStore resolves both sizes before the backend is created
			config.BurstCacheSize, config.InitialBurstSize = GenBurstSizes(tt.cacheSize, tt.burstSize, tt.deferBurst)
			store := &store{
				config:     config,
				rcache:     NewRCacheMock(),
				counterReg: metric.NewCounterRegistry(),
			}
			c := newMemCache(config, store, nil)
			assert.Equal(t, tt.wantInitial, config.InitialBurstSize)
			assert.Equal(t, tt.wantBurst, c.burstCap)
			if tt.deferBurst {
				_, burstCap, cnt, bytes := c.setCapacity(CacheSizeKeepCurrent, BurstCacheSizeRestore)
				assert.Equal(t, tt.wantInitial, burstCap)
				assert.Equal(t, int64(0), cnt)
				assert.Equal(t, int64(0), bytes)
			}
		})
	}
}

func TestMemSetCapacity(t *testing.T) {
	tests := []struct {
		name            string
		newCapacity     int64
		newBurst        int64
		setupFn         func(cache *mcache)
		wantCnt         int64 // 0 means exactly 0; -1 means > 0
		wantBytes       int64
		wantBurstEvicts int64 // 0 means exactly 0; -1 means > 0
		checkState      func(t *testing.T, cache *mcache)
	}{
		{
			name:        "no eviction needed",
			newCapacity: 100,
			newBurst:    200,
			setupFn:     func(c *mcache) { c.used = 50 },
			wantCnt:     0,
			wantBytes:   0,
			checkState: func(t *testing.T, c *mcache) {
				assert.Equal(t, int64(100), c.capacity)
				assert.Equal(t, int64(200), c.burstCap)
				assert.Equal(t, int64(100), c.store.config.CacheSize,
					"config.CacheSize should be synced with effective cache size")
				assert.Equal(t, int64(200), c.store.config.BurstCacheSize,
					"config.BurstCacheSize should be synced with effective burst cache size")
			},
		},
		{
			// Both base capacity and burst are reduced: evictions counted as cleanup.
			name:        "eviction triggered by shrink",
			newCapacity: 50,
			newBurst:    50,
			setupFn: func(c *mcache) {
				// Three pages of 50 bytes each: used=150 > new burstCap=50
				for i := uint32(1); i <= 3; i++ {
					page := &Page{Data: make([]byte, 50)}
					page.Acquire()
					c.pages[CacheKey{"target", i}] = memItem{
						atime: time.Now().Add(-time.Duration(i) * time.Second),
						page:  page,
					}
				}
				c.used = 150
			},
			wantCnt:         -1,
			wantBytes:       -1,
			wantBurstEvicts: 0, // base also changed -> cleanup metrics, not burst
			checkState: func(t *testing.T, c *mcache) {
				assert.LessOrEqual(t, c.used, c.burstCap)
				assert.Equal(t, int64(50), c.capacity)
				assert.Equal(t, int64(50), c.burstCap)
				assert.Equal(t, int64(50), c.store.config.CacheSize,
					"config.CacheSize should be synced with effective cache size")
				assert.Equal(t, int64(50), c.store.config.BurstCacheSize,
					"config.BurstCacheSize should be synced with effective burst cache size")
			},
		},
		{
			name:        "burstCacheSize < cacheSize: burstCap reset to cacheSize",
			newCapacity: 200,
			newBurst:    50, // 50 < 200, gets reset to 200
			setupFn:     func(c *mcache) { c.used = 50 },
			wantCnt:     0,
			wantBytes:   0,
			checkState: func(t *testing.T, c *mcache) {
				assert.Equal(t, int64(200), c.capacity)
				assert.Equal(t, int64(200), c.burstCap)
				assert.Equal(t, int64(200), c.store.config.CacheSize,
					"config.CacheSize should be synced with effective cache size")
				assert.Equal(t, int64(200), c.store.config.BurstCacheSize,
					"config.BurstCacheSize should be synced with effective burst cache size")
			},
		},
		{
			// set-cache with CacheSizeKeepCurrent (keep base), burst shrinks: evictions
			// must be recorded in burst metrics (blockcache_burst_evicts/bytes).
			name:        "burst-only shrink via sentinel (keep current): eviction counted as burst",
			newCapacity: CacheSizeKeepCurrent, // keep current base capacity
			newBurst:    80,                   // < current burst(300); normalized to max(80,base=100)=100
			setupFn: func(c *mcache) {
				c.capacity = 100
				c.burstCap = 300
				for i := uint32(1); i <= 3; i++ {
					page := &Page{Data: make([]byte, 50)}
					page.Acquire()
					c.pages[CacheKey{"target", i}] = memItem{
						atime: time.Now().Add(-time.Duration(i) * time.Second),
						page:  page,
					}
				}
				c.used = 150 // > new burstCap=100, eviction needed
			},
			wantCnt:         -1,
			wantBytes:       -1,
			wantBurstEvicts: -1, // burst-only shrink -> burst metrics must increment
		},
		{
			// set-cache with explicit cacheSize equal to current: semantically the
			// same as a burst-only shrink, so burst metrics must also increment.
			name:        "burst-only shrink via explicit equal cacheSize: eviction counted as burst",
			newCapacity: 100, // explicitly same as current base
			newBurst:    80,  // < current burst(300); normalized to max(80,base=100)=100
			setupFn: func(c *mcache) {
				c.capacity = 100
				c.burstCap = 300
				for i := uint32(1); i <= 3; i++ {
					page := &Page{Data: make([]byte, 50)}
					page.Acquire()
					c.pages[CacheKey{"target", i}] = memItem{
						atime: time.Now().Add(-time.Duration(i) * time.Second),
						page:  page,
					}
				}
				c.used = 150 // > new burstCap=100, eviction needed
			},
			wantCnt:         -1,
			wantBytes:       -1,
			wantBurstEvicts: -1, // same semantic as sentinel path -> burst metrics
		},
		{
			// Toggle scenario: burst was configured at startup (400), then disabled
			// via burst=0; restore brings it back without eviction.
			name:        "restore burst to startup-configured value",
			newCapacity: CacheSizeKeepCurrent,
			newBurst:    BurstCacheSizeRestore,
			setupFn: func(c *mcache) {
				c.capacity = 100
				c.burstCap = 100
				c.store.config.InitialBurstSize = 400
				c.used = 50
			},
			wantCnt:         0,
			wantBytes:       0,
			wantBurstEvicts: 0,
			checkState: func(t *testing.T, c *mcache) {
				assert.Equal(t, int64(100), c.capacity)
				assert.Equal(t, int64(400), c.burstCap)
				assert.Equal(t, int64(100), c.store.config.CacheSize,
					"config.CacheSize should be synced with effective cache size")
				assert.Equal(t, int64(400), c.store.config.BurstCacheSize,
					"config.BurstCacheSize should be synced with effective burst cache size")
			},
		},
		{
			// Burst was never configured at startup; burstCap=300 is the leftover
			// after shrinking cache size from 300 to 100 with CacheSizeKeepCurrent. Restore
			// resolves to 0 and is normalized up to cache size, i.e. disabled.
			// Base capacity is unchanged and burst shrinks -> burst metrics.
			name:        "restore burst never enabled: shrink to cache size counted as burst eviction",
			newCapacity: CacheSizeKeepCurrent,
			newBurst:    BurstCacheSizeRestore,
			setupFn: func(c *mcache) {
				c.capacity = 100
				c.burstCap = 300
				c.store.config.InitialBurstSize = 0
				for i := uint32(1); i <= 3; i++ {
					page := &Page{Data: make([]byte, 50)}
					page.Acquire()
					c.pages[CacheKey{"target", i}] = memItem{
						atime: time.Now().Add(-time.Duration(i) * time.Second),
						page:  page,
					}
				}
				c.used = 150 // > new burstCap=100, eviction needed
			},
			wantCnt:         -1,
			wantBytes:       -1,
			wantBurstEvicts: -1,
			checkState: func(t *testing.T, c *mcache) {
				assert.Equal(t, int64(100), c.capacity)
				assert.Equal(t, int64(100), c.burstCap)
				assert.LessOrEqual(t, c.used, c.burstCap)
			},
		},
		{
			// Cache size was raised to 500, exceeding the startup burst (400):
			// restore is normalized up to cache size, effectively a no-op.
			name:        "restore burst below current cache size: raised to cache size",
			newCapacity: CacheSizeKeepCurrent,
			newBurst:    BurstCacheSizeRestore,
			setupFn: func(c *mcache) {
				c.capacity = 500
				c.burstCap = 500
				c.store.config.InitialBurstSize = 400
				c.used = 100
			},
			wantCnt:         0,
			wantBytes:       0,
			wantBurstEvicts: 0,
			checkState: func(t *testing.T, c *mcache) {
				assert.Equal(t, int64(500), c.capacity)
				assert.Equal(t, int64(500), c.burstCap)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := newTestMcache(t)
			tt.setupFn(cache)
			_, _, cnt, bytes := cache.setCapacity(tt.newCapacity, tt.newBurst)
			if tt.wantCnt == 0 {
				assert.Equal(t, int64(0), cnt)
			} else {
				assert.Greater(t, cnt, int64(0))
			}
			if tt.wantBytes == 0 {
				assert.Equal(t, int64(0), bytes)
			} else {
				assert.Greater(t, bytes, int64(0))
			}
			// Verify burst metrics are updated only for burst-only shrinks.
			if tt.wantBurstEvicts == 0 {
				assert.Equal(t, float64(0), testutil.ToFloat64(cache.burstEvicts),
					"burstEvicts should not increment when base capacity also changes")
				assert.Equal(t, float64(0), testutil.ToFloat64(cache.burstEvictBytes),
					"burstEvictBytes should not increment when base capacity also changes")
			} else if tt.wantBurstEvicts < 0 {
				assert.Greater(t, testutil.ToFloat64(cache.burstEvicts), float64(0),
					"burstEvicts must increment for burst-only shrink")
				assert.Greater(t, testutil.ToFloat64(cache.burstEvictBytes), float64(0),
					"burstEvictBytes must increment for burst-only shrink")
			}
			if tt.checkState != nil {
				tt.checkState(t, cache)
			}
		})
	}
}

func TestMCacheLockTarget(t *testing.T) {
	rcache := NewRCacheMock()
	store := &store{
		config:     &StoreConfig{},
		rcache:     rcache,
		counterReg: metric.NewCounterRegistry(),
		pinCache:   newPinCache(),
	}
	c := newMemCache(&StoreConfig{CacheSize: 4096}, store, nil)

	pageOf := func() *Page { return &Page{Data: make([]byte, 1024)} }
	c.cache("locked-target", 0, pageOf())
	c.cache("locked-target", 1, pageOf())
	c.cache("plain-target", 0, pageOf())
	c.cache("plain-target", 1, pageOf())

	c.lockTarget("locked-target", nil)

	t.Run("capacity eviction skips locked target in all rounds", func(t *testing.T) {
		c.Lock()
		c.doCleanup(0, evictionCleanup, nil)
		c.Unlock()

		// Locked blocks must all survive; the plain target loses blocks
		// (2-random compare mode may leave one unpaired block behind).
		assert.True(t, c.check("locked-target", 0))
		assert.True(t, c.check("locked-target", 1))
		plainLeft := 0
		for _, blk := range []uint32{0, 1} {
			if c.check("plain-target", blk) {
				plainLeft++
			}
		}
		assert.LessOrEqual(t, plainLeft, 1)
	})

	t.Run("all-locked cleanup terminates without progress", func(t *testing.T) {
		c.Lock()
		c.doCleanup(0, evictionCleanup, nil)
		c.Unlock()
		assert.True(t, c.check("locked-target", 0))
		assert.True(t, c.check("locked-target", 1))
	})

	t.Run("refcount: still locked after first unlock", func(t *testing.T) {
		c.lockTarget("locked-target", nil)
		c.unlockTarget("locked-target", nil)
		c.Lock()
		c.doCleanup(0, evictionCleanup, nil)
		c.Unlock()
		assert.True(t, c.check("locked-target", 0))
	})

	t.Run("evictable after last unlock", func(t *testing.T) {
		c.unlockTarget("locked-target", nil)
		c.Lock()
		assert.Empty(t, c.lockedBlocks)
		c.doCleanup(0, evictionCleanup, nil)
		// Formerly locked blocks are now evictable: at most one block of any
		// target may remain (2-random unpaired leftover).
		remaining := len(c.pages)
		c.Unlock()
		assert.LessOrEqual(t, remaining, 1)
	})
}

func TestMCacheSelectedBlockLocks(t *testing.T) {
	rcache := NewRCacheMock()
	store := &store{
		config:     &StoreConfig{},
		rcache:     rcache,
		counterReg: metric.NewCounterRegistry(),
		pinCache:   newPinCache(),
	}
	c := newMemCache(&StoreConfig{CacheSize: 4096}, store, nil)

	c.lockTarget("t", []uint32{})
	c.Lock()
	assert.True(t, c.isLocked(CacheKey{Target: "t", Blk: 2}))
	c.Unlock()
	c.unlockTarget("t", nil)
	assert.Empty(t, c.lockedBlocks)

	c.lockTarget("t", []uint32{0})
	c.lockTarget("t", []uint32{0, 1})
	c.lockTarget("t", nil)
	c.Lock()
	assert.True(t, c.isLocked(CacheKey{Target: "t", Blk: 0}))
	assert.True(t, c.isLocked(CacheKey{Target: "t", Blk: 1}))
	assert.True(t, c.isLocked(CacheKey{Target: "t", Blk: 2}))
	c.Unlock()

	c.unlockTarget("t", []uint32{})
	c.Lock()
	assert.True(t, c.isLocked(CacheKey{Target: "t", Blk: 0}))
	assert.True(t, c.isLocked(CacheKey{Target: "t", Blk: 1}))
	assert.False(t, c.isLocked(CacheKey{Target: "t", Blk: 2}))
	c.Unlock()

	c.unlockTarget("t", []uint32{0, 1})
	c.Lock()
	assert.True(t, c.isLocked(CacheKey{Target: "t", Blk: 0}))
	assert.False(t, c.isLocked(CacheKey{Target: "t", Blk: 1}))
	c.Unlock()

	c.unlockTarget("t", []uint32{0})
	assert.Empty(t, c.lockedBlocks)
}

func TestMCacheLockedDeleteCounter(t *testing.T) {
	rcache := NewRCacheMock()
	store := &store{
		config:     &StoreConfig{},
		rcache:     rcache,
		counterReg: metric.NewCounterRegistry(),
		pinCache:   newPinCache(),
	}
	c := newMemCache(&StoreConfig{CacheSize: 4096}, store, nil)
	c.cache("t", 0, &Page{Data: make([]byte, 1024)})
	c.lockTarget("t", nil)

	// Explicit removal proceeds regardless of the lock (unlink-while-mapped)
	// and is counted by blockcache_evicts_locked.
	c.removeByPrefix([]string{"t"}, nil)
	assert.False(t, c.check("t", 0))
	assert.Equal(t, float64(1), testutil.ToFloat64(c.evictsLocked))
	c.unlockTarget("t", nil)
}
