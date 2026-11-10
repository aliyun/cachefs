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
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aliyun/cachefs/pkg/metric"
	"github.com/aliyun/cachefs/pkg/serf"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func newTestCache(t *testing.T, dir string) (*diskCache, *MockRemoteCache) {
	mockRCache := new(MockRemoteCache)
	mockStoreConfig, err := NewStoreConfig()
	assert.NoError(t, err)

	serfConf := serf.DefaultSerfConfig()
	serfConf.HashRing = mockStoreConfig.HashringEnabled()
	serfConf.BindAddr = "127.0.0.1"
	serfConf.BindPort = 0
	ss := serf.NewSerfService(serfConf, nil)
	if err := ss.Start(); err != nil {
		t.Skipf("Start serf service failed: %s", err)
	}

	cache := &diskCache{
		pages: make(map[CacheKey]*Page),
		keys:  make(map[CacheKey]cacheItem),
		store: &store{
			serf:       ss,
			rcache:     mockRCache,
			config:     mockStoreConfig,
			counterReg: metric.NewCounterRegistry(),
			pinCache:   newPinCache(),
		},
		hashhitpending: make([]pendingCache, 0, 100),
		nonhitpending:  make([]pendingCache, 0, 100),
		pendingLimit:   100,
		toCommit:       make(map[CacheKey]*pendingBlockFile),
		dir:            dir,
		capacity:       1024 * 1024 * 1024,
		burstCap:       1024 * 1024 * 1024,

		cacheWrites:     prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheWriteBytes: prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheWriteHist: prometheus.NewHistogram(prometheus.HistogramOpts{
			Buckets: prometheus.ExponentialBuckets(0.00001, 2, 20),
		}),
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
		cacheDrops:                  prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheDropHashHit:            prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheDropHashMiss:           prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheDropBytes:              prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheDropPreempts:           prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheDropPreemptBytes:       prometheus.NewCounter(prometheus.CounterOpts{}),
		burstEvicts:                 prometheus.NewCounter(prometheus.CounterOpts{}),
		burstEvictBytes:             prometheus.NewCounter(prometheus.CounterOpts{}),
	}
	cache.pendingCond = sync.NewCond(cache)

	return cache, mockRCache
}

func TestParseCacheKey(t *testing.T) {
	tests := []struct {
		path string
		want CacheKey
		err  bool
	}{
		{
			path: "/path/to/cache/123",
			want: CacheKey{"/path/to/cache", 123},
			err:  false,
		},
		{
			path: "/path/to/cache/not-a-number",
			err:  true,
		},
	}

	for _, test := range tests {
		got, err := parseCacheKey(test.path)
		if (err != nil) != test.err {
			t.Errorf("parseCacheKey(%q) error = %v, wantErr %v", test.path, err != nil, test.err)
			return
		}
		if err == nil && got != test.want {
			t.Errorf("parseCacheKey(%q) = %v, want %v", test.path, got, test.want)
		}
	}
}

func TestStats(t *testing.T) {
	diskCache := &diskCache{
		pages: make(map[CacheKey]*Page),
		keys:  make(map[CacheKey]cacheItem),
	}

	diskCache.pages[CacheKey{"some-file", 0}] = &Page{}
	cacheItem := cacheItem{size: 4096, atime: 12345}
	diskCache.keys[CacheKey{"some-file", 1}] = cacheItem

	diskCache.used += int64(cacheItem.size)

	count, used := diskCache.stats()
	expectedCount := int64(1 + 1)
	expectedUsed := int64(4096)

	if count != expectedCount {
		t.Errorf("Stats Count = %d, expected %d", count, expectedCount)
	}

	if used != expectedUsed {
		t.Errorf("Stats Used = %d, expected %d", used, expectedUsed)
	}
}

func TestEscapeTarget(t *testing.T) {
	results := make(map[string]string)
	tests := []struct {
		target string
		want   string
	}{
		{
			target: "",
			want:   "/#slash",
		},
		{
			target: ".",
			want:   "/#slash.",
		},
		{
			target: "/",
			want:   "/#slash#slash",
		},
		{
			target: "/.",
			want:   "/#dot",
		},
		{
			target: "..",
			want:   "/#slash..",
		},
		{
			target: "/..",
			want:   "/#dot#dot",
		},
		{
			target: "/../",
			want:   "/#dot#dot#slash",
		},
		{
			target: "///",
			want:   "/#slash#slash#slash#slash",
		},
		{
			target: "////",
			want:   "/#slash#slash#slash#slash#slash",
		},
		{
			target: "/../#",
			want:   "/#dot#dot/##",
		},
		{
			target: "/..////.././//...",
			want:   "/#dot#dot#slash#slash#slash#slash#dot#dot/#dot#slash#slash/...",
		},
		{
			target: "/somepath",
			want:   "/somepath",
		},
		{
			target: "somepath",
			want:   "/#slashsomepath",
		},
		{
			target: "./somepath",
			want:   "/#slash./somepath",
		},
		{
			target: "//somepath",
			want:   "/#slash#slash#slashsomepath",
		},
		{
			target: "/somepath/0",
			want:   "/somepath/0",
		},
		{
			target: "/somepath/file.txt",
			want:   "/somepath/file.txt",
		},
		{
			target: "somepath/0",
			want:   "/#slashsomepath/0",
		},
		{
			target: "./../somepath",
			want:   "/#slash./#dot#dot/somepath",
		},
		{
			target: "../../somepath",
			want:   "/#slash../#dot#dot/somepath",
		},
		{
			target: "./../somepath/..",
			want:   "/#slash./#dot#dot/somepath/#dot#dot",
		},
		{
			target: "./../somepath/.../..",
			want:   "/#slash./#dot#dot/somepath/.../#dot#dot",
		},
		{
			target: "./../somepath/../\000",
			want:   "/#slash./#dot#dot/somepath/#dot#dot/0",
		},
		{
			target: ".../../somepath/../new\000\000path/\000",
			want:   "/#slash.../#dot#dot/somepath/#dot#dot/new00path/0",
		},
		{
			target: "/#slash/somepath/0",
			want:   "/##slash/somepath/0",
		},
		{
			target: "##slash/somepath/0",
			want:   "/#slash####slash/somepath/0",
		},
	}
	for _, tc := range tests {
		result := escapeTarget(tc.target)
		if lastKey, ok := results[result]; ok {
			t.Errorf("Key %s and last key %s use the same path %s", tc.target, lastKey, result)
		}
		results[result] = tc.target
		if result != tc.want {
			t.Errorf("Test escapeTarget for \"%s\" want \"%s\" but got \"%s\"", tc.target, tc.want, result)
		}
	}
}

func TestCacheAndMetaPath(t *testing.T) {
	diskCache := &diskCache{
		dir: "/cachedir",
	}
	tests := []struct {
		target string
		blk    uint32
		cache  string
		meta   string
		size   string
		disk   string
	}{
		{
			target: "/testtarget",
			blk:    0,
			cache:  "/cachedir/testtarget/0",
			meta:   "/cachedir/testtarget/meta",
			size:   "/cachedir/testtarget/size",
			disk:   "/testtarget",
		},
		{
			target: "testtarget",
			blk:    0,
			cache:  "/cachedir/#slashtesttarget/0",
			meta:   "/cachedir/#slashtesttarget/meta",
			size:   "/cachedir/#slashtesttarget/size",
			disk:   "/#slashtesttarget",
		},
		{
			target: "//testtarget",
			blk:    0,
			cache:  "/cachedir/#slash#slash#slashtesttarget/0",
			meta:   "/cachedir/#slash#slash#slashtesttarget/meta",
			size:   "/cachedir/#slash#slash#slashtesttarget/size",
			disk:   "/#slash#slash#slashtesttarget",
		},
		{
			target: "/./testtarget",
			blk:    0,
			cache:  "/cachedir/#dot/testtarget/0",
			meta:   "/cachedir/#dot/testtarget/meta",
			size:   "/cachedir/#dot/testtarget/size",
			disk:   "/#dot/testtarget",
		},
		{
			target: "/../testtarget",
			blk:    0,
			cache:  "/cachedir/#dot#dot/testtarget/0",
			meta:   "/cachedir/#dot#dot/testtarget/meta",
			size:   "/cachedir/#dot#dot/testtarget/size",
			disk:   "/#dot#dot/testtarget",
		},
		{
			target: "/testtarget/",
			blk:    0,
			cache:  "/cachedir/testtarget#slash/0",
			meta:   "/cachedir/testtarget#slash/meta",
			size:   "/cachedir/testtarget#slash/size",
			disk:   "/testtarget#slash",
		},
		{
			target: "#dot/testtarget/..",
			blk:    0,
			cache:  "/cachedir/#slash##dot/testtarget/#dot#dot/0",
			meta:   "/cachedir/#slash##dot/testtarget/#dot#dot/meta",
			size:   "/cachedir/#slash##dot/testtarget/#dot#dot/size",
			disk:   "/#slash##dot/testtarget/#dot#dot",
		},
	}
	for _, tc := range tests {
		key := CacheKey{tc.target, tc.blk}
		cache := diskCache.cachePath(key)
		meta, size, disk := diskCache.metaPath(key)
		if cache != tc.cache {
			t.Errorf("Test cachePath for \"%v\" want \"%s\" but got \"%s\"", key, tc.meta, meta)
		}
		if meta != tc.meta {
			t.Errorf("Test metaPath meta for \"%v\" want \"%s\" but got \"%s\"", key, tc.meta, meta)
		}
		if size != tc.size {
			t.Errorf("Test metaPath size for \"%v\" want \"%s\" but got \"%s\"", key, tc.size, size)
		}
		if disk != tc.disk {
			t.Errorf("Test metaPath disk for \"%v\" want \"%s\" but got \"%s\"", key, tc.disk, disk)
		}
	}
}

func TestReplaceLast(t *testing.T) {
	tests := []struct {
		original string
		old      string
		new      string
		expected string
	}{
		{"test/..", "/..", "/dotdot", "test/dotdot"},
		{"test/../..", "/..", "/dotdot", "test/../dotdot"},
		{"../test/..", "/..", "/dotdot", "../test/dotdot"},
		{"test/file.txt", "/..", "/dotdot", "test/file.txt"},
		{"", "/..", "/dotdot", ""},
		{"test/..test/..", "/..", "/dotdot", "test/..test/dotdot"},
	}

	for _, test := range tests {
		result := replaceLast(test.original, test.old, test.new)
		if result != test.expected {
			t.Errorf("replaceLast(%q, %q, %q) = %q; expected %q", test.original, test.old, test.new, result, test.expected)
		}
	}
}

func TestFlushPage(t *testing.T) {
	cacheWrites := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cache_writes_total",
		Help: "Total number of cache writes",
	})
	cacheWriteBytes := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cache_write_bytes_total",
		Help: "Total number of bytes written to cache",
	})
	cacheWriteHist := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "cache_write_duration_seconds",
		Help:    "Histogram for the duration of cache writes",
		Buckets: prometheus.DefBuckets,
	})

	cache := &diskCache{
		cacheWrites:     cacheWrites,
		cacheWriteBytes: cacheWriteBytes,
		cacheWriteHist:  cacheWriteHist,
	}

	tempDir, err := os.MkdirTemp("", "test-flushpage")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	path := filepath.Join(tempDir, "0")
	key := CacheKey{
		Target: tempDir,
		Blk:    0,
	}
	data := []byte("test data")

	err = cache.flushPage(key, data)
	require.NoError(t, err)

	writtenData, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, data, writtenData, "The data written to the file should match the input data")

	_, err = os.Stat(path + ".tmp")
	require.True(t, os.IsNotExist(err), "The temporary file should not exist anymore")
}

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	target, blk := "testTarget", uint32(1)
	key := CacheKey{target, blk}

	newCache := func(t *testing.T) (*diskCache, *MockRemoteCache) {
		mockRCache := new(MockRemoteCache)
		cache := &diskCache{
			pages: make(map[CacheKey]*Page),
			keys:  make(map[CacheKey]cacheItem),
			store: &store{
				rcache: mockRCache,
			},
			dir: dir,
		}
		return cache, mockRCache
	}

	createFile := func(t *testing.T, cache *diskCache) {
		t.Helper()
		cachePath := cache.cachePath(key)
		require.NoError(t, os.MkdirAll(filepath.Dir(cachePath), os.ModePerm))
		f, err := os.Create(cachePath)
		require.NoError(t, err)
		f.Close()
		t.Cleanup(func() { os.Remove(cachePath) })
	}

	t.Run("page cache hit - announce not called", func(t *testing.T) {
		cache, mockRCache := newCache(t)
		page := &Page{}
		cache.pages[key] = page

		p, rc, err := cache.load(target, blk, true)
		require.NoError(t, err)
		require.NotNil(t, p)
		require.Nil(t, rc)

		mockRCache.AssertNotCalled(t, "announce")
	})

	t.Run("tracked key with file - atime updated, announce not called", func(t *testing.T) {
		cache, mockRCache := newCache(t)
		oldAtime := uint32(1)
		cache.keys[key] = cacheItem{size: 4096, atime: oldAtime}
		createFile(t, cache)

		_, rc, err := cache.load(target, blk, true)
		require.NoError(t, err)
		require.NotNil(t, rc)

		// announce must NOT be called for tracked (already indexed) files
		mockRCache.AssertNotCalled(t, "announce")
		// atime must be refreshed
		updated := cache.keys[key]
		assert.Greater(t, updated.atime, oldAtime, "atime should be updated")
	})

	t.Run("untracked key with file - announce called", func(t *testing.T) {
		cache, mockRCache := newCache(t)
		// key is NOT in cache.keys (untracked)
		createFile(t, cache)
		mockRCache.On("announce", target, uint64(0), blk).Return()

		_, rc, err := cache.load(target, blk, true)
		require.NoError(t, err)
		require.NotNil(t, rc)

		// announce must be called for untracked files found on disk
		mockRCache.AssertCalled(t, "announce", target, uint64(0), blk)
	})

	t.Run("tracked key no file - key removed, announce not called", func(t *testing.T) {
		cache, mockRCache := newCache(t)
		cache.keys[key] = cacheItem{size: 4096, atime: 1}
		cache.used = 4096
		// no file on disk

		_, rc, err := cache.load(target, blk, true)
		require.Error(t, err)
		require.Nil(t, rc)

		// stale key must be removed
		_, ok := cache.keys[key]
		assert.False(t, ok, "stale key should be removed when file is missing")
		assert.Equal(t, int64(0), cache.used, "used should be decremented")
		mockRCache.AssertNotCalled(t, "announce")
	})

	t.Run("untracked key no file - returns error, announce not called", func(t *testing.T) {
		cache, mockRCache := newCache(t)
		// neither pages nor keys contain the key, no file on disk

		_, rc, err := cache.load(target, blk, true)
		require.Error(t, err)
		require.Nil(t, rc)

		mockRCache.AssertNotCalled(t, "announce")
	})
}

func TestCache(t *testing.T) {
	dir := t.TempDir()
	cache, mockRCache := newTestCache(t, dir)

	target := "target1"
	blk := uint32(1)
	key := CacheKey{target, blk}
	page := &Page{Data: []byte("mock data")}
	len := uint64(0)

	mockRCache.On("announce", target, len, uint32(1)).Once()

	t.Run("cache with capacity 0", func(t *testing.T) {
		cache.capacity = 0
		cache.burstCap = 0
		cache.cache(target, 1, page)

		assert.NotContains(t, cache.pages, key)

		mockRCache.AssertNotCalled(t, "announce")
	})

	t.Run("already in cache.pages", func(t *testing.T) {
		cache.capacity = 512
		cache.burstCap = 1024
		cache.pages[key] = page
		assert.Contains(t, cache.pages, key)
		assert.Equal(t, cache.pages[key], page)

		cache.cache(target, 1, page)

		mockRCache.AssertNotCalled(t, "announce")
	})

	t.Run("cache and send to pending", func(t *testing.T) {
		cache.capacity = 512
		cache.burstCap = 1024
		cache.pages = make(map[CacheKey]*Page)

		cache.cache(target, 1, page)

		cache.Lock()
		queued := append(append([]pendingCache{}, cache.hashhitpending...), cache.nonhitpending...)
		cache.Unlock()
		require.Len(t, queued, 1, "Expected pending cache item, but none was queued.")
		assert.Equal(t, queued[0].key, key)
		assert.Equal(t, queued[0].page, page)

		assert.Contains(t, cache.pages, key)
		assert.Equal(t, cache.pages[key], page)
		mockRCache.AssertExpectations(t)
	})
}

// newQueueCache builds a minimal diskCache exercising only the writeback queue
// (pushPending/popPending) without a serf ring or on-disk directory.
func newQueueCache(limit int) *diskCache {
	c := &diskCache{
		pages:                 make(map[CacheKey]*Page),
		hashhitpending:        make([]pendingCache, 0, limit),
		nonhitpending:         make([]pendingCache, 0, limit),
		pendingLimit:          limit,
		cacheDrops:            prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheDropHashHit:      prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheDropHashMiss:     prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheDropBytes:        prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheDropPreempts:     prometheus.NewCounter(prometheus.CounterOpts{}),
		cacheDropPreemptBytes: prometheus.NewCounter(prometheus.CounterOpts{}),
	}
	c.pendingCond = sync.NewCond(c)
	return c
}

func TestPushPending(t *testing.T) {
	// add registers a fresh page for name in cache.pages (mirroring cache()),
	// returning the key and page so tests can inspect its refcount afterwards.
	add := func(c *diskCache, name string) (CacheKey, *Page) {
		k := CacheKey{name, 0}
		p := NewPage([]byte(name))
		c.pages[k] = p
		return k, p
	}
	refs := func(p *Page) int32 { return atomic.LoadInt32(&p.refs) }
	// ok drops the preempted-key return so bool-only assertions stay one-liners.
	ok := func(queued bool, _ *CacheKey) bool { return queued }

	t.Run("enqueue into the matching queue when not full", func(t *testing.T) {
		c := newQueueCache(10)
		c.Lock()
		defer c.Unlock()
		kh, ph := add(c, "h")
		kn, pn := add(c, "n")
		assert.True(t, ok(c.pushPending(pendingCache{kh, ph}, true)))
		assert.True(t, ok(c.pushPending(pendingCache{kn, pn}, false)))
		require.Len(t, c.hashhitpending, 1)
		require.Len(t, c.nonhitpending, 1)
		assert.Equal(t, kh, c.hashhitpending[0].key)
		assert.Equal(t, kn, c.nonhitpending[0].key)
		assert.EqualValues(t, 1, refs(ph))
		assert.EqualValues(t, 1, refs(pn))
		assert.EqualValues(t, 0, testutil.ToFloat64(c.cacheDrops))
	})

	t.Run("hash-hit preempts the newest hash-miss when full", func(t *testing.T) {
		c := newQueueCache(2)
		c.Lock()
		defer c.Unlock()
		k1, p1 := add(c, "n1")
		k2, p2 := add(c, "n2")
		require.True(t, ok(c.pushPending(pendingCache{k1, p1}, false)))
		require.True(t, ok(c.pushPending(pendingCache{k2, p2}, false))) // full: [n1, n2]
		kh, ph := add(c, "h")
		queued, revoked := c.pushPending(pendingCache{kh, ph}, true)
		assert.True(t, queued)
		// The evicted block was announced at its enqueue, so its key must be
		// surfaced for the caller to revoke.
		require.NotNil(t, revoked)
		assert.Equal(t, k2, *revoked)
		// The newest hash-miss (n2) is evicted; the head (n1) is protected.
		require.Len(t, c.nonhitpending, 1)
		assert.Equal(t, k1, c.nonhitpending[0].key)
		require.Len(t, c.hashhitpending, 1)
		assert.Equal(t, kh, c.hashhitpending[0].key)
		assert.EqualValues(t, 0, refs(p2), "evicted page should be released")
		assert.EqualValues(t, 1, refs(p1))
		assert.EqualValues(t, 1, refs(ph))
		assert.NotContains(t, c.pages, k2)
		assert.EqualValues(t, 1, testutil.ToFloat64(c.cacheDrops))
		assert.EqualValues(t, 1, testutil.ToFloat64(c.cacheDropHashMiss))
		assert.EqualValues(t, 0, testutil.ToFloat64(c.cacheDropHashHit))
		assert.EqualValues(t, len("n2"), testutil.ToFloat64(c.cacheDropBytes))
		// Preempt sub-metrics: a subset of the hash-miss drops.
		assert.EqualValues(t, 1, testutil.ToFloat64(c.cacheDropPreempts))
		assert.EqualValues(t, len("n2"), testutil.ToFloat64(c.cacheDropPreemptBytes))
	})

	t.Run("drop the new hash-miss when full", func(t *testing.T) {
		c := newQueueCache(1)
		c.Lock()
		defer c.Unlock()
		k1, p1 := add(c, "n1")
		require.True(t, ok(c.pushPending(pendingCache{k1, p1}, false))) // full
		k2, p2 := add(c, "n2")
		queued, revoked := c.pushPending(pendingCache{k2, p2}, false)
		assert.False(t, queued)
		// A brand-new block is never announced, so nothing to revoke.
		assert.Nil(t, revoked)
		require.Len(t, c.nonhitpending, 1)
		assert.Equal(t, k1, c.nonhitpending[0].key, "queued block must not be disturbed")
		assert.EqualValues(t, 0, refs(p2))
		assert.NotContains(t, c.pages, k2)
		assert.EqualValues(t, 1, testutil.ToFloat64(c.cacheDrops))
		assert.EqualValues(t, 1, testutil.ToFloat64(c.cacheDropHashMiss))
		// Dropping a brand-new block is not a preemption.
		assert.EqualValues(t, 0, testutil.ToFloat64(c.cacheDropPreempts))
	})

	t.Run("drop the new hash-hit when full and all queued are hash-hit", func(t *testing.T) {
		c := newQueueCache(1)
		c.Lock()
		defer c.Unlock()
		k1, p1 := add(c, "h1")
		require.True(t, ok(c.pushPending(pendingCache{k1, p1}, true))) // full, no hash-miss to evict
		k2, p2 := add(c, "h2")
		queued, revoked := c.pushPending(pendingCache{k2, p2}, true)
		assert.False(t, queued)
		assert.Nil(t, revoked)
		require.Len(t, c.hashhitpending, 1)
		assert.Equal(t, k1, c.hashhitpending[0].key)
		assert.EqualValues(t, 0, refs(p2))
		assert.EqualValues(t, 1, testutil.ToFloat64(c.cacheDrops))
		assert.EqualValues(t, 1, testutil.ToFloat64(c.cacheDropHashHit))
		assert.EqualValues(t, 0, testutil.ToFloat64(c.cacheDropPreempts))
	})
}

func TestPopPending(t *testing.T) {
	add := func(c *diskCache, name string) (CacheKey, *Page) {
		k := CacheKey{name, 0}
		p := NewPage([]byte(name))
		c.pages[k] = p
		return k, p
	}

	t.Run("drains hash-hit before hash-miss", func(t *testing.T) {
		c := newQueueCache(10)
		kn, pn := add(c, "n")
		kh, ph := add(c, "h")
		c.Lock()
		queued, _ := c.pushPending(pendingCache{kn, pn}, false)
		require.True(t, queued)
		queued, _ = c.pushPending(pendingCache{kh, ph}, true)
		require.True(t, queued)
		c.Unlock()
		assert.Equal(t, kh, c.popPending().key, "hash-hit must be drained first")
		assert.Equal(t, kn, c.popPending().key)
	})

	t.Run("pops hash-miss when no hash-hit queued", func(t *testing.T) {
		c := newQueueCache(10)
		kn, pn := add(c, "n")
		c.Lock()
		queued, _ := c.pushPending(pendingCache{kn, pn}, false)
		require.True(t, queued)
		c.Unlock()
		assert.Equal(t, kn, c.popPending().key)
	})

	t.Run("blocks until a block is pushed", func(t *testing.T) {
		c := newQueueCache(10)
		got := make(chan pendingCache, 1)
		go func() { got <- c.popPending() }()
		// Give the goroutine time to enter Cond.Wait.
		time.Sleep(50 * time.Millisecond)
		select {
		case <-got:
			t.Fatal("popPending returned before any block was queued")
		default:
		}
		k, p := add(c, "n")
		c.Lock()
		queued, _ := c.pushPending(pendingCache{k, p}, false)
		require.True(t, queued)
		c.Unlock()
		select {
		case w := <-got:
			assert.Equal(t, k, w.key)
		case <-time.After(2 * time.Second):
			t.Fatal("popPending did not wake after a block was queued")
		}
	})
}

// When serf is unavailable every block is treated as a hash-miss, so the
// writeback queue degenerates to a single FIFO and preemption never fires.
func TestCacheSerfNilDegradesToFIFO(t *testing.T) {
	cache, mockRCache := newTestCache(t, t.TempDir())
	cache.store.serf = nil
	cache.capacity = 512
	cache.burstCap = 1024
	cache.pages = make(map[CacheKey]*Page)

	target := "target1"
	mockRCache.On("announce", target, uint64(0), uint32(1)).Once()
	mockRCache.On("announce", target, uint64(0), uint32(2)).Once()

	cache.cache(target, 1, NewPage([]byte("a")))
	cache.cache(target, 2, NewPage([]byte("b")))

	cache.Lock()
	defer cache.Unlock()
	assert.Empty(t, cache.hashhitpending, "serf=nil must route everything to the hash-miss queue")
	require.Len(t, cache.nonhitpending, 2)
	assert.Equal(t, CacheKey{target, 1}, cache.nonhitpending[0].key)
	assert.Equal(t, CacheKey{target, 2}, cache.nonhitpending[1].key)
	assert.EqualValues(t, 0, testutil.ToFloat64(cache.cacheDropPreempts))
	mockRCache.AssertExpectations(t)
}

// A preempted hash-miss block was announced to peers when it was queued, so
// cache() must revoke it once it is evicted; a hash-hit block that simply
// fills the queue is never a preemption and triggers no revoke.
func TestCachePreemptRevokesEvicted(t *testing.T) {
	target := "target1"
	hk := CacheKey{target, 2}
	hitKey := hk.String()

	t.Run("preemption revokes the evicted hash-miss", func(t *testing.T) {
		cache, mockRCache := newTestCache(t, t.TempDir())
		// blk 1 is a hash-miss, blk 2 is a hash-hit.
		cache.store.serf = &MockSerfService{hashHitFn: func(key string) bool { return key == hitKey }}
		cache.hashhitpending = cache.hashhitpending[:0]
		cache.nonhitpending = cache.nonhitpending[:0]
		cache.pendingLimit = 1

		// The queued miss (blk 1) is announced, then revoked on eviction; the
		// preempting hit (blk 2) is announced.
		mockRCache.On("announce", target, uint64(0), uint32(1)).Once()
		mockRCache.On("revoke", target, uint64(0), uint32(1)).Once()
		mockRCache.On("announce", target, uint64(0), uint32(2)).Once()

		cache.cache(target, 1, NewPage([]byte("miss"))) // fills the queue
		cache.cache(target, 2, NewPage([]byte("hit")))  // preempts blk 1

		cache.Lock()
		require.Len(t, cache.hashhitpending, 1)
		assert.Equal(t, CacheKey{target, 2}, cache.hashhitpending[0].key)
		assert.Empty(t, cache.nonhitpending)
		assert.EqualValues(t, 1, testutil.ToFloat64(cache.cacheDropPreempts))
		cache.Unlock()
		mockRCache.AssertExpectations(t)
	})

	t.Run("dropping a brand-new block does not revoke", func(t *testing.T) {
		cache, mockRCache := newTestCache(t, t.TempDir())
		cache.store.serf = &MockSerfService{hashHitFn: func(key string) bool { return false }}
		cache.hashhitpending = cache.hashhitpending[:0]
		cache.nonhitpending = cache.nonhitpending[:0]
		cache.pendingLimit = 1

		// Only the first (queued) miss is announced; the second miss is dropped
		// on full without ever being announced, so no revoke must fire.
		mockRCache.On("announce", target, uint64(0), uint32(1)).Once()

		cache.cache(target, 1, NewPage([]byte("miss1"))) // fills the queue
		cache.cache(target, 2, NewPage([]byte("miss2"))) // dropped, never announced

		cache.Lock()
		require.Len(t, cache.nonhitpending, 1)
		assert.Equal(t, CacheKey{target, 1}, cache.nonhitpending[0].key)
		assert.EqualValues(t, 0, testutil.ToFloat64(cache.cacheDropPreempts))
		cache.Unlock()
		mockRCache.AssertExpectations(t)
		mockRCache.AssertNotCalled(t, "revoke", target, uint64(0), uint32(2))
	})
}

func TestCheck(t *testing.T) {
	dir := t.TempDir()
	cache, mockRCache := newTestCache(t, dir)

	target := "target1"
	blk := uint32(1)
	page := &Page{Data: []byte("mock data")}

	mockRCache.On("announce", target, uint64(0), blk).Once()

	t.Run("check existing cache", func(t *testing.T) {
		cache.cache(target, blk, page)
		exist := cache.check(target, blk)
		assert.True(t, exist)
		mockRCache.AssertExpectations(t)
	})
	t.Run("check non-existing cache", func(t *testing.T) {
		cache.cache(target, blk, page)
		exist := cache.check(target, uint32(2))
		assert.False(t, exist)
		mockRCache.AssertExpectations(t)
	})
}

func TestGetBlockFile(t *testing.T) {
	dir := t.TempDir()
	err := os.MkdirAll(dir, 0755)
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	t.Run("Success to get", func(t *testing.T) {
		cache, _ := newTestCache(t, dir)
		cache.capacity = 1024 * 1024 * 1024
		target := "target1"
		key := CacheKey{target, 1}
		filep := cache.cachePath(key)
		parentDir := filepath.Dir(filep)
		err := os.MkdirAll(parentDir, 0755)
		assert.NoError(t, err)
		file, err := os.Create(filep)
		assert.NoError(t, err)
		file.Close()

		cache.pages = map[CacheKey]*Page{
			key: {Data: []byte("mock data")},
		}

		file, err = cache.getBlockFile(target, 1)

		assert.Nil(t, err)
		assert.NotNil(t, file)
	})

	t.Run("cleanup", func(t *testing.T) {
		cache, mockRCache := newTestCache(t, dir)
		cache.capacity = 10
		cache.burstCap = 10
		target := "fisrt"
		key := CacheKey{target, 1}
		filep := cache.cachePath(key)
		parentDir := filepath.Dir(filep)
		err := os.MkdirAll(parentDir, 0755)
		assert.NoError(t, err)
		file, err := os.Create(filep)
		assert.NoError(t, err)
		file.Close()

		target2 := "second"
		key2 := CacheKey{target2, 1}
		filep2 := cache.cachePath(key2)
		parentDir2 := filepath.Dir(filep2)
		err = os.MkdirAll(parentDir2, 0755)
		assert.NoError(t, err)
		file, err = os.Create(filep2)
		assert.NoError(t, err)
		file.Close()

		cache.pages = map[CacheKey]*Page{
			key:  {Data: []byte("mock data")},
			key2: {Data: []byte("mock data")},
		}
		key3 := CacheKey{"third", 1}
		cache.keys = make(map[CacheKey]cacheItem)

		file, err = cache.getBlockFile(target, 1)
		assert.Nil(t, err)
		assert.NotNil(t, file)

		// add another newer key3, so key can be removed on add
		ci, ok := cache.keys[key]
		assert.True(t, ok)
		atime := ci.atime
		cache.keys[key3] = cacheItem{size: 8, atime: atime + 10}

		mockRCache.On("revoke", target, uint64(0), uint32(1))
		file, err = cache.getBlockFile(target2, 1)
		assert.Nil(t, err)
		assert.NotNil(t, file)
		mockRCache.AssertExpectations(t)

		file, err = cache.getBlockFile(target, 1)
		assert.Nil(t, file)
		assert.NotNil(t, err)
	})
}

func TestPrepareBlockFile(t *testing.T) {
	dir := t.TempDir()
	cache, _ := newTestCache(t, dir)

	target := "target1"
	blk := uint32(1)
	key := CacheKey{target, blk}

	t.Run("Success to prepare", func(t *testing.T) {
		expectedPattern := fmt.Sprintf("%d_*.tmp", blk)
		file, err := cache.prepareBlockFile(target, blk, uint32(4096))

		assert.Nil(t, err)
		assert.NotNil(t, file)

		name := filepath.Base(file.Name())
		match, err := filepath.Match(expectedPattern, name)

		assert.Nil(t, err)
		assert.True(t, match)
	})

	t.Run("File already committed", func(t *testing.T) {
		tempFile := &os.File{}
		cache.commitMu.Lock()
		cache.toCommit = map[CacheKey]*pendingBlockFile{
			key: {time.Now(), tempFile, 4096},
		}
		cache.commitMu.Unlock()

		file, err := cache.prepareBlockFile(target, blk, 4096)
		assert.Nil(t, err)
		assert.Nil(t, file)
	})
}

func TestCommitBlockFile(t *testing.T) {
	dir := t.TempDir()
	cache, _ := newTestCache(t, dir)

	target := "target1"
	blk := uint32(1)
	key := CacheKey{target, blk}

	targetFilePath := cache.cachePath(key)
	parentDir := filepath.Dir(targetFilePath)
	err := os.MkdirAll(parentDir, 0755)
	assert.NoError(t, err)
	tmpFilePath := targetFilePath + "_tmp"

	t.Run("Success to commit", func(t *testing.T) {
		tmpFile, err := os.Create(tmpFilePath)
		assert.NoError(t, err)
		defer func() {
			tmpFile.Close()
			os.Remove(tmpFilePath)
		}()
		data := "temporary data"
		_, err = tmpFile.Write([]byte(data))
		assert.NoError(t, err)
		cache.commitMu.Lock()
		cache.toCommit[key] = &pendingBlockFile{
			created: time.Now(),
			file:    tmpFile,
			size:    uint32(len(data)),
		}
		cache.commitMu.Unlock()

		err = cache.commitBlockFile(target, blk, true)
		assert.NoError(t, err)
	})

	t.Run("stat size not equal to bf size", func(t *testing.T) {
		tmpFile, err := os.Create(tmpFilePath)
		assert.NoError(t, err)
		defer func() {
			tmpFile.Close()
			os.Remove(tmpFilePath)
		}()

		data := "temporary data"
		_, err = tmpFile.Write([]byte(data))
		assert.NoError(t, err)

		cache.commitMu.Lock()
		cache.toCommit[key] = &pendingBlockFile{
			created: time.Now(),
			file:    tmpFile,
		}
		cache.commitMu.Unlock()

		err = cache.commitBlockFile(target, blk, true)
		assert.Error(t, err)
	})

	t.Run("stat block is 0", func(t *testing.T) {
		tmpFile, err := os.Create(tmpFilePath)
		assert.NoError(t, err)
		defer func() {
			tmpFile.Close()
			os.Remove(tmpFilePath)
		}()

		assert.NoError(t, err)
		cache.commitMu.Lock()
		cache.toCommit[key] = &pendingBlockFile{
			created: time.Now(),
			file:    tmpFile,
		}
		cache.commitMu.Unlock()

		err = cache.commitBlockFile(target, blk, true)
		assert.Error(t, err)
	})
}

func TestDump(t *testing.T) {
	cache := &diskCache{
		keys:    make(map[CacheKey]cacheItem),
		tmpKeys: make(map[CacheKey]cacheItem),
		pages:   make(map[CacheKey]*Page),
	}

	meta := cache.dump()
	assert.Nil(t, meta, "Expected metaInfo to be nil when both keys and tmpKeys are empty")

	cache.keys[CacheKey{Target: "target1", Blk: 1}] = cacheItem{size: 1024, atime: 1234}
	cache.keys[CacheKey{Target: "target1", Blk: 2}] = cacheItem{size: 2048, atime: 1235}
	cache.tmpKeys[CacheKey{Target: "target2", Blk: 1}] = cacheItem{size: 512, atime: 1236}
	cache.pages[CacheKey{Target: "target3", Blk: 1}] = &Page{refs: 1, mmap: false, Data: []byte{1, 2, 3}}

	meta = cache.dump()

	assert.NotNil(t, meta, "Expected metaInfo to be not nil")
	assert.Len(t, meta.Targets, 2, "Expected 2 files in the metaInfo")

	target1BitSet, ok := meta.Targets["target1"]
	assert.True(t, ok, "Expected bitset for target1 to exist")
	assert.True(t, target1BitSet.Blocks.Test(1), "Expected bitset for target1 to have block 1 set")
	assert.True(t, target1BitSet.Blocks.Test(2), "Expected bitset for target1 to have block 2 set")

	_, ok = meta.Targets["target2"]
	assert.False(t, ok, "Expected bitset for target2 not exist")

	target3BitSet, ok := meta.Targets["target3"]
	assert.True(t, ok, "Expected bitset for target3 to exist")
	assert.True(t, target3BitSet.Blocks.Test(1), "Expected bitset for target3 to have block 1 set")
}

func TestDiskClearBurst(t *testing.T) {
	dir := t.TempDir()

	tests := []struct {
		name      string
		capacity  int64
		burstCap  int64
		setupFn   func(cache *diskCache)
		wantCnt   int64
		wantBytes int64
		checkUsed func(t *testing.T, cache *diskCache)
	}{
		{
			name:     "no burst when used <= capacity",
			capacity: 100,
			burstCap: 300,
			setupFn: func(cache *diskCache) {
				cache.used = 80
			},
			wantCnt:   0,
			wantBytes: 0,
		},
		{
			name:     "burst eviction reduces used to capacity",
			capacity: 100,
			burstCap: 300,
			setupFn: func(cache *diskCache) {
				// Two keys of size 100 each: used=200 > capacity=100.
				// key1 has an older atime so it will always be evicted first
				// regardless of map iteration order.
				now := uint32(time.Now().UnixMilli())
				cache.keys[CacheKey{"target", 1}] = cacheItem{size: 100, atime: now - 2000}
				cache.keys[CacheKey{"target", 2}] = cacheItem{size: 100, atime: now}
				cache.used = 200
			},
			wantCnt:   1,
			wantBytes: 100,
			checkUsed: func(t *testing.T, cache *diskCache) {
				assert.LessOrEqual(t, cache.used, cache.capacity)
			},
		},
		{
			// Only 1 key exists: compare mode requires >= 2 keys to form a pair, so no
			// eviction is possible. The RETRY terminates immediately without looping.
			name:     "single key - used > capacity but too few keys to compare, clearBurst returns without eviction",
			capacity: 100,
			burstCap: 300,
			setupFn: func(cache *diskCache) {
				now := uint32(time.Now().UnixMilli())
				cache.keys[CacheKey{"target", 1}] = cacheItem{size: 200, atime: now}
				cache.used = 200
			},
			wantCnt:   0,
			wantBytes: 0,
		},
		{
			// All keys belong to an incomplete target and are always skipped by shouldSkip.
			// Every compare pass makes 0 evictions, so the RETRY terminates without looping.
			name:     "incomplete target - all keys skipped every pass, clearBurst returns without eviction",
			capacity: 100,
			burstCap: 300,
			setupFn: func(cache *diskCache) {
				now := uint32(time.Now().UnixMilli())
				cache.keys[CacheKey{"t", 1}] = cacheItem{size: 100, atime: now - 2000}
				cache.keys[CacheKey{"t", 2}] = cacheItem{size: 100, atime: now}
				cache.used = 200
				// Incomplete target: shouldSkip returns true for all keys of "t".
				cache.targets = make(map[string]*targetItem)
				cache.targets["t"] = &targetItem{atime: time.Now(), length: 200, complete: false}
			},
			wantCnt:   0,
			wantBytes: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache, mockRCache := newTestCache(t, dir)
			cache.capacity = tt.capacity
			cache.burstCap = tt.burstCap
			tt.setupFn(cache)

			if tt.wantCnt > 0 {
				// The older key (blk=1) is always the one evicted
				mockRCache.On("revoke", "target", uint64(0), uint32(1)).Once()
			}

			cnt, bytes := cache.clearBurst()

			assert.Equal(t, tt.wantCnt, cnt)
			assert.Equal(t, tt.wantBytes, bytes)
			if tt.checkUsed != nil {
				tt.checkUsed(t, cache)
			}
			if tt.wantCnt > 0 {
				mockRCache.AssertExpectations(t)
			}
		})
	}
}

func TestDiskRemoveByPrefixKeepMultiplePrefixes(t *testing.T) {
	dir := t.TempDir()
	cache, mockRCache := newTestCache(t, dir)

	keepPageKey := CacheKey{Target: "/modelscope/qwen/Qwen3-4B/page", Blk: 1}
	evictPageKey := CacheKey{Target: "/modelscope/old/Model/page", Blk: 2}
	keepKey := CacheKey{Target: "/modelscope/deepseek/DeepSeek-OCR/key", Blk: 3}
	evictKey := CacheKey{Target: "/modelscope/old/Model/key", Blk: 4}
	keepPage := NewPage(make([]byte, 10))
	evictPage := NewPage(make([]byte, 20))
	keepPage.Acquire()
	evictPage.Acquire()
	cache.pages[keepPageKey] = keepPage
	cache.pages[evictPageKey] = evictPage
	cache.keys[keepKey] = cacheItem{size: 30, atime: uint32(time.Now().UnixMilli())}
	cache.keys[evictKey] = cacheItem{size: 40, atime: uint32(time.Now().UnixMilli())}
	cache.used = 100

	mockRCache.On("revoke", evictPageKey.Target, uint64(0), evictPageKey.Blk).Once()
	mockRCache.On("revoke", evictKey.Target, uint64(0), evictKey.Blk).Once()

	cnt, freed := cache.removeByPrefix(nil, []string{
		"/modelscope/qwen/Qwen3.5-0.4B/",
		"/modelscope/qwen/Qwen3.5-0.8B/",
		"/modelscope/qwen/Qwen3-4B/",
		"/modelscope/deepseek/DeepSeek-OCR/",
		"/modelscope/MiniMax/MiniMax-M2/",
	})

	assert.Equal(t, int64(2), cnt)
	assert.Equal(t, int64(60), freed)
	assert.Contains(t, cache.pages, keepPageKey)
	assert.NotContains(t, cache.pages, evictPageKey)
	assert.Contains(t, cache.keys, keepKey)
	assert.NotContains(t, cache.keys, evictKey)
	assert.Equal(t, int32(2), keepPage.refs)
	assert.Equal(t, int32(2), evictPage.refs)
	mockRCache.AssertExpectations(t)
}

func TestDiskSetCapacity(t *testing.T) {
	dir := t.TempDir()

	tests := []struct {
		name        string
		newCapacity int64
		newBurst    int64
		setupFn     func(cache *diskCache)
		wantCnt     int64
		wantBytes   int64
		checkState  func(t *testing.T, cache *diskCache)
	}{
		{
			name:        "no eviction needed",
			newCapacity: 100,
			newBurst:    200,
			setupFn:     func(cache *diskCache) { cache.used = 50 },
			wantCnt:     0,
			wantBytes:   0,
			checkState: func(t *testing.T, cache *diskCache) {
				assert.Equal(t, int64(100), cache.capacity)
				assert.Equal(t, int64(200), cache.burstCap)
				assert.Equal(t, int64(100), cache.store.config.CacheSize,
					"config.CacheSize should be synced with effective cache size")
				assert.Equal(t, int64(200), cache.store.config.BurstCacheSize,
					"config.BurstCacheSize should be synced with effective burst cache size")
			},
		},
		{
			name:        "eviction triggered by shrink",
			newCapacity: 100,
			newBurst:    100,
			setupFn: func(cache *diskCache) {
				// Two keys of size 100 each: used=200 > new burstCap=100.
				// key1 has an older atime so it will always be evicted first.
				now := uint32(time.Now().UnixMilli())
				cache.keys[CacheKey{"target", 1}] = cacheItem{size: 100, atime: now - 2000}
				cache.keys[CacheKey{"target", 2}] = cacheItem{size: 100, atime: now}
				cache.used = 200
			},
			wantCnt:   1,
			wantBytes: 100,
			checkState: func(t *testing.T, cache *diskCache) {
				assert.LessOrEqual(t, cache.used, cache.burstCap)
				assert.Equal(t, int64(100), cache.store.config.CacheSize,
					"config.CacheSize should be synced with effective cache size")
				assert.Equal(t, int64(100), cache.store.config.BurstCacheSize,
					"config.BurstCacheSize should be synced with effective burst cache size")
			},
		},
		{
			name:        "burstCacheSize < cacheSize: burstCap reset to cacheSize",
			newCapacity: 200,
			newBurst:    50, // 50 < 200, gets reset to 200
			setupFn:     func(cache *diskCache) { cache.used = 50 },
			wantCnt:     0,
			wantBytes:   0,
			checkState: func(t *testing.T, cache *diskCache) {
				assert.Equal(t, int64(200), cache.capacity)
				assert.Equal(t, int64(200), cache.burstCap)
				assert.Equal(t, int64(200), cache.store.config.CacheSize,
					"config.CacheSize should be synced with effective cache size")
				assert.Equal(t, int64(200), cache.store.config.BurstCacheSize,
					"config.BurstCacheSize should be synced with effective burst cache size")
			},
		},
		{
			// Toggle scenario: burst was configured at startup (400), then disabled
			// via burst=0; restore brings it back without eviction.
			name:        "restore burst to startup-configured value",
			newCapacity: CacheSizeKeepCurrent,
			newBurst:    BurstCacheSizeRestore,
			setupFn: func(cache *diskCache) {
				cache.capacity = 100
				cache.burstCap = 100
				cache.store.config.InitialBurstSize = 400
				cache.used = 50
			},
			wantCnt:   0,
			wantBytes: 0,
			checkState: func(t *testing.T, cache *diskCache) {
				assert.Equal(t, int64(100), cache.capacity)
				assert.Equal(t, int64(400), cache.burstCap)
				assert.Equal(t, int64(100), cache.store.config.CacheSize,
					"config.CacheSize should be synced with effective cache size")
				assert.Equal(t, int64(400), cache.store.config.BurstCacheSize,
					"config.BurstCacheSize should be synced with effective burst cache size")
			},
		},
		{
			// Burst was never configured at startup; burstCap=300 is the leftover
			// after shrinking cache size from 300 to 100 with CacheSizeKeepCurrent. Restore
			// resolves to 0 and is normalized up to cache size, i.e. disabled.
			name:        "restore burst never enabled: normalized to cache size",
			newCapacity: CacheSizeKeepCurrent,
			newBurst:    BurstCacheSizeRestore,
			setupFn: func(cache *diskCache) {
				cache.capacity = 100
				cache.burstCap = 300
				cache.store.config.InitialBurstSize = 0
				now := uint32(time.Now().UnixMilli())
				cache.keys[CacheKey{"target", 1}] = cacheItem{size: 100, atime: now - 2000}
				cache.keys[CacheKey{"target", 2}] = cacheItem{size: 100, atime: now}
				cache.used = 200
			},
			wantCnt:   1,
			wantBytes: 100,
			checkState: func(t *testing.T, cache *diskCache) {
				assert.Equal(t, int64(100), cache.capacity)
				assert.Equal(t, int64(100), cache.burstCap)
				assert.LessOrEqual(t, cache.used, cache.burstCap)
				assert.Equal(t, int64(100), cache.store.config.BurstCacheSize,
					"config.BurstCacheSize should be synced with effective burst cache size")
			},
		},
		{
			// Cache size was raised to 500, exceeding the startup burst (400):
			// restore is normalized up to cache size, effectively a no-op.
			name:        "restore burst below current cache size: raised to cache size",
			newCapacity: CacheSizeKeepCurrent,
			newBurst:    BurstCacheSizeRestore,
			setupFn: func(cache *diskCache) {
				cache.capacity = 500
				cache.burstCap = 500
				cache.store.config.InitialBurstSize = 400
				cache.used = 100
			},
			wantCnt:   0,
			wantBytes: 0,
			checkState: func(t *testing.T, cache *diskCache) {
				assert.Equal(t, int64(500), cache.capacity)
				assert.Equal(t, int64(500), cache.burstCap)
			},
		},
		{
			name:        "restore burst when already at initial value: no-op",
			newCapacity: CacheSizeKeepCurrent,
			newBurst:    BurstCacheSizeRestore,
			setupFn: func(cache *diskCache) {
				cache.capacity = 100
				cache.burstCap = 400
				cache.store.config.InitialBurstSize = 400
				cache.used = 50
			},
			wantCnt:   0,
			wantBytes: 0,
			checkState: func(t *testing.T, cache *diskCache) {
				assert.Equal(t, int64(100), cache.capacity)
				assert.Equal(t, int64(400), cache.burstCap)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache, mockRCache := newTestCache(t, dir)
			tt.setupFn(cache)
			if tt.wantCnt > 0 {
				// The older key (blk=1) is always the one evicted
				mockRCache.On("revoke", "target", uint64(0), uint32(1)).Once()
			}
			_, _, cnt, bytes := cache.setCapacity(tt.newCapacity, tt.newBurst)
			assert.Equal(t, tt.wantCnt, cnt)
			assert.Equal(t, tt.wantBytes, bytes)
			if tt.checkState != nil {
				tt.checkState(t, cache)
			}
			if tt.wantCnt > 0 {
				mockRCache.AssertExpectations(t)
			}
		})
	}
}

func TestNewLocalCacheBurstCap(t *testing.T) {
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
				CacheDir:   t.TempDir(),
				CacheSize:  tt.cacheSize,
				DeferBurst: tt.deferBurst,
				BlockSize:  4 << 20,
				BufferSize: 4 << 20,
			}
			// doNewStore resolves both sizes before the backend is created
			config.BurstCacheSize, config.InitialBurstSize = GenBurstSizes(tt.cacheSize, tt.burstSize, tt.deferBurst)
			store := &store{
				config:     config,
				rcache:     NewRCacheMock(),
				counterReg: metric.NewCounterRegistry(),
			}
			c := newLocalCache(config, store, nil).(*diskCache)
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

func TestDiskCacheLockTarget(t *testing.T) {
	dir := t.TempDir()
	cache, mockRCache := newTestCache(t, dir)
	// newTestCache doesn't set the lock-eviction fields; add them. Allow any
	// revoke call (doCleanup -> revokeDel fires for evicted targets).
	cache.lockedBlocks = make(map[string]map[int]int)
	cache.targets = make(map[string]*targetItem)
	cache.evictsSkipLocked = prometheus.NewCounter(prometheus.CounterOpts{})
	cache.evictsLocked = prometheus.NewCounter(prometheus.CounterOpts{})
	mockRCache.On("revoke", mock.Anything, mock.Anything, mock.Anything).Return()

	addKey := func(target string, blk uint32) {
		key := CacheKey{target, blk}
		cache.keys[key] = cacheItem{size: 1024, atime: uint32(time.Now().Unix())}
		cache.used += 1024
	}
	addKey("locked-target", 0)
	addKey("locked-target", 1)
	addKey("plain-target", 0)
	addKey("plain-target", 1)

	cache.lockTarget("locked-target", nil)

	t.Run("capacity eviction skips locked target in all rounds", func(t *testing.T) {
		cache.Lock()
		cache.doCleanup(false, evictionCleanup, nil)
		cache.Unlock()

		// Locked blocks must all survive; the plain target loses blocks
		// (2-random compare mode may leave one unpaired block behind).
		_, ok := cache.keys[CacheKey{"locked-target", 0}]
		assert.True(t, ok)
		_, ok = cache.keys[CacheKey{"locked-target", 1}]
		assert.True(t, ok)
		plainLeft := 0
		for _, blk := range []uint32{0, 1} {
			if _, ok := cache.keys[CacheKey{"plain-target", blk}]; ok {
				plainLeft++
			}
		}
		assert.LessOrEqual(t, plainLeft, 1)
	})

	t.Run("all-locked cleanup terminates without progress", func(t *testing.T) {
		before := len(cache.keys)
		beforeSkipped := testutil.ToFloat64(cache.evictsSkipLocked)
		cache.Lock()
		cache.doCleanup(false, evictionCleanup, nil)
		cache.Unlock()
		assert.Equal(t, before, len(cache.keys))
		// No eviction can make progress here (locked entries are skipped and a
		// lone plain entry never forms a compare pair), so iteration always
		// visits every entry and the skip counter must fire.
		assert.Greater(t, testutil.ToFloat64(cache.evictsSkipLocked), beforeSkipped)
	})

	t.Run("refcount: still locked after first unlock", func(t *testing.T) {
		cache.lockTarget("locked-target", nil)
		cache.unlockTarget("locked-target", nil)
		cache.Lock()
		cache.doCleanup(false, evictionCleanup, nil)
		cache.Unlock()
		_, ok := cache.keys[CacheKey{"locked-target", 0}]
		assert.True(t, ok)
	})

	t.Run("evictable after last unlock", func(t *testing.T) {
		cache.unlockTarget("locked-target", nil)
		cache.Lock()
		assert.Empty(t, cache.lockedBlocks)
		// Zero the goals so doCleanup evicts everything (otherwise it stops
		// early since used is far below the default 1GiB burst cap).
		cache.capacity, cache.burstCap = 0, 0
		cache.doCleanup(false, evictionCleanup, nil)
		remaining := len(cache.keys)
		cache.Unlock()
		assert.LessOrEqual(t, remaining, 1)
	})
}

func TestDiskCacheSelectedBlockLocks(t *testing.T) {
	dir := t.TempDir()
	cache, _ := newTestCache(t, dir)
	cache.lockedBlocks = make(map[string]map[int]int)

	cache.lockTarget("t", []uint32{})
	cache.Lock()
	assert.True(t, cache.isLocked(CacheKey{Target: "t", Blk: 2}))
	cache.Unlock()
	cache.unlockTarget("t", nil)
	assert.Empty(t, cache.lockedBlocks)

	cache.lockTarget("t", []uint32{0})
	cache.lockTarget("t", []uint32{0, 1})
	cache.lockTarget("t", nil)
	cache.Lock()
	assert.True(t, cache.isLocked(CacheKey{Target: "t", Blk: 0}))
	assert.True(t, cache.isLocked(CacheKey{Target: "t", Blk: 1}))
	assert.True(t, cache.isLocked(CacheKey{Target: "t", Blk: 2}))
	cache.Unlock()

	cache.unlockTarget("t", []uint32{})
	cache.Lock()
	assert.True(t, cache.isLocked(CacheKey{Target: "t", Blk: 0}))
	assert.True(t, cache.isLocked(CacheKey{Target: "t", Blk: 1}))
	assert.False(t, cache.isLocked(CacheKey{Target: "t", Blk: 2}))
	cache.Unlock()

	cache.unlockTarget("t", []uint32{0, 1})
	cache.Lock()
	assert.True(t, cache.isLocked(CacheKey{Target: "t", Blk: 0}))
	assert.False(t, cache.isLocked(CacheKey{Target: "t", Blk: 1}))
	cache.Unlock()

	cache.unlockTarget("t", []uint32{0})
	assert.Empty(t, cache.lockedBlocks)
}

func TestDiskCacheLockedDeleteCounter(t *testing.T) {
	dir := t.TempDir()
	cache, mockRCache := newTestCache(t, dir)
	cache.lockedBlocks = make(map[string]map[int]int)
	cache.evictsSkipLocked = prometheus.NewCounter(prometheus.CounterOpts{})
	cache.evictsLocked = prometheus.NewCounter(prometheus.CounterOpts{})
	mockRCache.On("revoke", mock.Anything, mock.Anything, mock.Anything).Return()

	key := CacheKey{"t", 0}
	cache.keys[key] = cacheItem{size: 1024, atime: uint32(time.Now().Unix())}
	cache.used += 1024
	cache.lockTarget("t", nil)

	// Explicit removal proceeds regardless of the lock (unlink-while-mapped)
	// and is counted by blockcache_evicts_locked.
	cnt, _ := cache.removeByPrefix([]string{"t"}, nil)
	assert.Equal(t, int64(1), cnt)
	assert.Equal(t, float64(1), testutil.ToFloat64(cache.evictsLocked))
	cache.unlockTarget("t", nil)
}
