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
	"fmt"
	"io/fs"
	"os"
	"sync"
	"syscall"
	"testing"

	"github.com/aliyun/cachefs/pkg/chunk"
	"github.com/aliyun/cachefs/pkg/meta"
	cachefsmeta "github.com/aliyun/cachefs/pkg/meta/cachefs"
	"github.com/aliyun/cachefs/pkg/metric"
	"github.com/aliyun/cachefs/pkg/srcmgr"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

// mockMeta is a minimal MetaData implementation used by preload tests.
// Only the methods needed by MetaFS and getSourceFS are wired up.
type mockMeta struct {
	root   *cachefsmeta.Inode
	inoMap map[meta.Ino]*cachefsmeta.Inode
	ready  bool
}

func (m *mockMeta) Init() error                         { return nil }
func (m *mockMeta) InitMetrics(_ prometheus.Registerer) {}
func (m *mockMeta) Stop()                               {}
func (m *mockMeta) GetPath(p string) string             { return p }
func (m *mockMeta) SourceReady(_ bool) syscall.Errno    { return 0 }
func (m *mockMeta) MetaReady() syscall.Errno            { return 0 }
func (m *mockMeta) IsMetaReady() bool                   { return m.ready }
func (m *mockMeta) GetRoot() *cachefsmeta.Inode         { return m.root }
func (m *mockMeta) GetInode(ino meta.Ino) (*cachefsmeta.Inode, syscall.Errno) {
	if n, ok := m.inoMap[ino]; ok {
		return n, 0
	}
	return nil, syscall.ENOENT
}
func (m *mockMeta) Lookup(_ meta.Context, _ meta.Ino, _ string, _ *meta.Ino, _ *meta.Attr) syscall.Errno {
	return syscall.ENOSYS
}
func (m *mockMeta) GetAttr(_ meta.Context, ino meta.Ino, attr *meta.Attr) syscall.Errno {
	cino, eno := m.GetInode(ino)
	if eno != 0 {
		return eno
	}
	cached := cino.GetAttr()
	if cached == nil {
		return syscall.ENOENT
	}
	*attr = *cached
	return 0
}
func (m *mockMeta) ReadLink(_ meta.Context, _ meta.Ino, _ *[]byte) syscall.Errno {
	return syscall.ENOSYS
}
func (m *mockMeta) Open(_ meta.Context, _ meta.Ino, _ uint32, _ *meta.Attr) syscall.Errno {
	return syscall.ENOSYS
}
func (m *mockMeta) Opendir(_ meta.Context, _ meta.Ino, _ *meta.Attr) syscall.Errno {
	return syscall.ENOSYS
}
func (m *mockMeta) Close(_ meta.Context, _ meta.Ino) syscall.Errno { return syscall.ENOSYS }
func (m *mockMeta) StatFS(_ meta.Context, _, _, _, _ *uint64) syscall.Errno {
	return syscall.ENOSYS
}
func (m *mockMeta) Readdir(_ meta.Context, _ meta.Ino, _ *[]*meta.Entry) syscall.Errno {
	return syscall.ENOSYS
}

// buildTestTree creates a minimal mockMeta with a non-nil root inode.
// This is sufficient for TestChooseSourceFS which only checks IsMetaReady and
// whether the returned fs.FS is a *MetaFS; it does not need real children.
func buildTestTree() *mockMeta {
	rootAttr := &meta.Attr{Typ: meta.TypeDirectory, Mode: 0755}
	root := cachefsmeta.NewInode(meta.Ino(1), nil, "/", rootAttr.Typ, rootAttr)
	return &mockMeta{
		root:   root,
		ready:  true,
		inoMap: map[meta.Ino]*cachefsmeta.Inode{meta.Ino(1): root},
	}
}

func TestPreloadState(t *testing.T) {
	state := &preloadState{
		stopCh:       make(chan struct{}),
		fileCh:       make(chan *fileInfo, maxEntrySize),
		stageEntries: make(map[string]*fileInfo, maxEntrySize),
	}

	// push maxEntrySize-1 entries, we should stage them
	for i := 0; i < maxEntrySize-1; i++ {
		fi := &fileInfo{
			path: fmt.Sprintf("%d", i),
			size: 10,
		}
		state.pushEntry(fi)
	}
	assert.Equal(t, maxEntrySize-1, len(state.stageEntries))
	assert.Equal(t, 0, len(state.fileCh))

	// push 1 more entry, they should be flushed
	fi := &fileInfo{
		path: "test",
		size: 1,
	}
	state.pushEntry(fi)
	assert.Equal(t, 0, len(state.stageEntries))
	assert.Equal(t, maxEntrySize, len(state.fileCh))

	// push 3 entries, and flush them manually
	state.stageEntries = make(map[string]*fileInfo, maxEntrySize)
	close(state.fileCh)
	state.fileCh = make(chan *fileInfo, maxEntrySize)
	for i := 0; i < maxEntrySize-1; i++ {
		fi := &fileInfo{
			path: fmt.Sprintf("%d", i),
			size: 10,
		}
		state.pushEntry(fi)
	}
	state.flushEntries()
	assert.Equal(t, 0, len(state.stageEntries))
	assert.Equal(t, maxEntrySize-1, len(state.fileCh))

	state.stop()
	assert.Equal(t, true, state.stopped)
}

func TestStopWalking(t *testing.T) {
	state := &preloadState{}
	state.resetStopWalk()

	// Concurrent workers may all decide to stop the round, the signal must be
	// closed exactly once, a second close would panic.
	var waitGroup sync.WaitGroup
	for range 16 {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			state.stopWalking("cache already full")
		}()
	}
	waitGroup.Wait()

	select {
	case <-state.stopWalkCh:
	default:
		t.Fatal("stop walk signal should be closed")
	}

	// A round that stopped doesn't leave the next one unable to stop.
	state.resetStopWalk()
	select {
	case <-state.stopWalkCh:
		t.Fatal("stop walk signal should be armed again after reset")
	default:
	}
	state.stopWalking("preload cancelled")
	<-state.stopWalkCh
}

// pinnedCacheMock reports pins so that handleLoad runs more than one round.
type pinnedCacheMock struct {
	counterMock
}

func (m *pinnedCacheMock) hasPins() bool { return true }

// TestLoadLoopStopWalkPerRound covers a worker that decides to stop after the
// walk of its round has already finished, which happens when a single source
// file is larger than the cache. The next round must start with a stop signal
// of its own, otherwise it could neither stop nor report why it stopped.
func TestLoadLoopStopWalkPerRound(t *testing.T) {
	mock := &pinnedCacheMock{counterMock{
		isCacheFullMock: isCacheFullMock{burstCacheSize: 1000, currentSize: 0},
		counters: map[string]*metric.AtomicCounter{
			chunk.CounterEvictsCleanup: metric.NewCounter(prometheus.CounterOpts{
				Name: chunk.CounterEvictsCleanup,
			}),
			chunk.CounterEvictsPinned: metric.NewCounter(prometheus.CounterOpts{
				Name: chunk.CounterEvictsPinned,
			}),
		},
	}}
	state := &preloadState{
		stopCh:    make(chan struct{}),
		loadCh:    make(chan *loadReq, 1),
		cache:     mock,
		blockSize: 16,
	}
	state.resetStopWalk()
	c := &Cachefs{
		Conf:         &Config{Store: &chunk.StoreConfig{BlockSize: 16}},
		cache:        mock,
		counterReg:   metric.NewCounterRegistry(),
		preloadState: state,
	}

	var armed []bool
	doLoad := func(paths []string) {
		open := true
		select {
		case <-state.stopWalkCh:
			open = false
		default:
		}
		armed = append(armed, open)
		// The walk is over by now, a worker still finds the cache full.
		state.stopWalking("cache already full")
	}
	go c.loadLoop(doLoad)
	defer state.stop()

	req := &loadReq{paths: []string{"/test"}, done: make(chan struct{})}
	state.loadCh <- req
	<-req.done

	// Pinned round, then remaining round, each with a stop signal of its own.
	assert.Equal(t, []bool{true, true}, armed)
}

func TestPreloadReq(t *testing.T) {
	tests := []struct {
		name  string
		paths []string
		want  string
	}{
		{
			name:  "multiple paths",
			paths: []string{"a", "b", "c"},
			want:  `paths: "a b c"`,
		},
		{
			name:  "single path",
			paths: []string{"/tmp/file"},
			want:  `paths: "/tmp/file"`,
		},
		{
			name:  "empty paths",
			paths: []string{},
			want:  `paths: ""`,
		},
		{
			name:  "nil paths",
			paths: nil,
			want:  `paths: ""`,
		},
		{
			name:  "paths with space",
			paths: []string{"dir1", "dir two", "dir3"},
			want:  `paths: "dir1 dir two dir3"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &loadReq{
				paths: tt.paths,
			}

			got := req.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLoadCacheCancel(t *testing.T) {
	tests := []struct {
		name     string
		req      *loadReq
		expected syscall.Errno
	}{
		{
			name: "cancel when no preload in progress",
			req: &loadReq{
				cancel: true,
			},
			expected: 0,
		},
		{
			name: "cancel request with paths",
			req: &loadReq{
				paths:  []string{"/test"},
				cancel: true,
			},
			expected: 0,
		},
		{
			name:     "nil request",
			req:      nil,
			expected: syscall.ENODATA,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Cachefs{
				preloadState: &preloadState{
					stopCh:    make(chan struct{}),
					loadCh:    make(chan *loadReq, 1),
					stopped:   false,
					fileCh:    make(chan *fileInfo, 10),
					fileSet:   make(map[uint64]struct{}),
					initReq:   nil,
					curReq:    nil,
					blkTokens: utils.NewToken(10),
				},
			}

			err := c.loadCache(tt.req)
			assert.Equal(t, tt.expected, err)
		})
	}
}

func TestLoadCacheCancelWithActivePreload(t *testing.T) {
	c := &Cachefs{
		preloadState: &preloadState{
			stopCh:    make(chan struct{}),
			loadCh:    make(chan *loadReq, 1),
			stopped:   false,
			fileCh:    make(chan *fileInfo, 10),
			fileSet:   make(map[uint64]struct{}),
			initReq:   &loadReq{paths: []string{"/init"}},
			curReq:    &loadReq{paths: []string{"/active"}},
			blkTokens: utils.NewToken(10),
		},
	}

	// Test cancel when there is an active preload
	req := &loadReq{
		cancel: true,
	}
	err := c.loadCache(req)
	assert.Equal(t, syscall.Errno(0), err)
	assert.True(t, c.preloadState.cancel.Load())
}

// mockSource implements sourceStore interface for testing
type mockSource struct{}

func (m *mockSource) newSourceFile(target string, length uint64) sourceFileStore {
	return nil
}

func TestGetPreloadSource(t *testing.T) {
	tests := []struct {
		name              string
		sourceDir         string
		mountPoint        string
		subpathEnabled    bool
		expectedDir       string
		expectedUseSource bool
	}{
		{
			name:              "default case - use source",
			sourceDir:         "/source",
			mountPoint:        "/mount",
			subpathEnabled:    false,
			expectedDir:       "/source",
			expectedUseSource: true,
		},
		{
			name:              "subpath enabled - use mount point",
			sourceDir:         "/source",
			mountPoint:        "/mount",
			subpathEnabled:    true,
			expectedDir:       "/mount",
			expectedUseSource: false,
		},
		{
			name:              "empty source dir",
			sourceDir:         "",
			mountPoint:        "/mount",
			subpathEnabled:    false,
			expectedDir:       "",
			expectedUseSource: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metaConf := &cachefsmeta.MetaConfig{
				MountPoint: tt.mountPoint,
				SubPathConfig: cachefsmeta.SourceSubPathConfig{
					Paths: "",
					File:  "",
				},
			}
			if tt.sourceDir != "" {
				metaConf.SourceDirs = []string{tt.sourceDir}
				metaConf.SourceManager = srcmgr.NewSourceManager(metaConf.SourceDirs, nil)
			}
			fs := &Cachefs{
				Conf: &Config{
					Meta: metaConf,
				},
				source: &mockSource{},
			}

			// Set subpath config
			if tt.subpathEnabled {
				fs.Conf.Meta.SubPathConfig.Paths = "/test/path"
			}

			dir, useSource := fs.getPreloadSource()
			if dir != tt.expectedDir {
				t.Errorf("getPreloadSource() dir = %v, expected %v", dir, tt.expectedDir)
				return
			}
			if useSource != tt.expectedUseSource {
				t.Errorf("getPreloadSource() useSource = %v, expected %v", useSource, tt.expectedUseSource)
				return
			}
		})
	}
}

func newTestCachefs(conf *Config, m cachefsmeta.MetaData) *Cachefs {
	return &Cachefs{
		Conf:         conf,
		Meta:         m,
		preloadState: &preloadState{},
		// mountCh == nil means SetMounted() has already been called.
		source: &mockSource{},
	}
}

func TestGetSourceFS(t *testing.T) {
	tree := buildTestTree()
	tests := []struct {
		name            string
		peerMetaTimeout int
		metaReady       bool
		wantMetaFS      bool
		wantUseSource   bool
	}{
		{
			name:            "peer meta ready",
			peerMetaTimeout: 10,
			metaReady:       true,
			wantMetaFS:      true,
			wantUseSource:   false,
		},
		{
			name:            "peer meta not ready",
			peerMetaTimeout: 10,
			metaReady:       false,
			wantMetaFS:      false,
			wantUseSource:   true,
		},
		{
			name:            "peer meta disabled",
			peerMetaTimeout: 0,
			metaReady:       true,
			wantMetaFS:      false,
			wantUseSource:   true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var m *mockMeta
			if tc.metaReady {
				m = &mockMeta{root: tree.root, ready: true, inoMap: tree.inoMap}
			} else {
				m = &mockMeta{ready: false}
			}
			c := newTestCachefs(&Config{
				Meta: &cachefsmeta.MetaConfig{
					SourceDirs:      []string{"/src"},
					SourceManager:   srcmgr.NewSourceManager([]string{"/src"}, nil),
					PeerMetaTimeout: tc.peerMetaTimeout,
				},
			}, m)

			sfs := c.getSourceFS()

			_, isMeta := sfs.(*cachefsmeta.MetaFS)
			assert.Equal(t, tc.wantMetaFS, isMeta)
			assert.NotNil(t, sfs)
			assert.Equal(t, tc.wantUseSource, c.preloadState.useSource)
		})
	}
}

// isCacheFullMock overrides size() and getCacheSize() on mockCacheManager to
// provide controlled values for TestIsCacheFull.
type isCacheFullMock struct {
	mockCacheManager
	currentSize    int64
	cacheSize      int64
	burstCacheSize int64
}

func (m *isCacheFullMock) size() int64                  { return m.currentSize }
func (m *isCacheFullMock) getCacheSize() (int64, int64) { return m.cacheSize, m.burstCacheSize }

func (m *isCacheFullMock) setPinCache(prefixes []string) {}
func (m *isCacheFullMock) isPinned(target string) bool   { return false }
func (m *isCacheFullMock) hasPins() bool                 { return false }

func TestIsCacheFull(t *testing.T) {
	const (
		blockBytes     = uint32(16) // margin (BlockSize in bytes)
		baseCacheBytes = int64(100) // base cache capacity in bytes
		burstCapBytes  = int64(500) // burst cache capacity in bytes
	)
	baseThreshold := baseCacheBytes - int64(blockBytes) // 84
	burstThreshold := burstCapBytes - int64(blockBytes) // 484

	// Each eviction test case gets its own counter to avoid cross-contamination.
	evictingCounter := metric.NewCounter(prometheus.CounterOpts{Name: "test_cleanup_evicting"})
	evictingCounter.Add(5)
	quietCounter := metric.NewCounter(prometheus.CounterOpts{Name: "test_cleanup_quiet"})
	quietCounter.Add(3)

	tests := []struct {
		name                 string
		cacheSize            int64
		burstCacheSize       int64
		blockSize            uint32
		currentSize          int64
		cleanupEvictCounter  *metric.AtomicCounter
		cleanupEvictBaseline float64
		wantFull             bool
	}{
		{
			name:           "empty cache - not full",
			burstCacheSize: burstCapBytes,
			blockSize:      blockBytes,
			currentSize:    0,
			wantFull:       false,
		},
		{
			name:           "below burst threshold - not full",
			burstCacheSize: burstCapBytes,
			blockSize:      blockBytes,
			currentSize:    burstThreshold - 1, // 483
			wantFull:       false,
		},
		{
			name:           "at burst threshold - full",
			burstCacheSize: burstCapBytes,
			blockSize:      blockBytes,
			currentSize:    burstThreshold, // 484
			wantFull:       true,
		},
		{
			name:           "above burst capacity - full",
			burstCacheSize: burstCapBytes,
			blockSize:      blockBytes,
			currentSize:    burstCapBytes, // 500 >= threshold(484)
			wantFull:       true,
		},
		{
			// Regression: used is above base-cache but below burst threshold.
			// The old code compared against CacheSize and would incorrectly
			// report full (200 >= 100-16=84). The correct code uses BurstCacheSize
			// and reports not-full (200 < 500-16=484).
			name:           "burst mode: used above base cache but below burst threshold - not full",
			cacheSize:      baseCacheBytes,
			burstCacheSize: burstCapBytes,
			blockSize:      blockBytes,
			currentSize:    200, // > cacheSize(100) but < burstThreshold(484)
			wantFull:       false,
		},
		{
			name:           "no burst: below threshold - not full",
			cacheSize:      baseCacheBytes,
			burstCacheSize: baseCacheBytes, // burst == cache, no extra burst headroom
			blockSize:      blockBytes,
			currentSize:    baseThreshold - 1, // 83
			wantFull:       false,
		},
		{
			name:           "no burst: at threshold - full",
			burstCacheSize: baseCacheBytes,
			blockSize:      blockBytes,
			currentSize:    baseThreshold, // 84
			wantFull:       true,
		},
		{
			name:                 "capacity not full but cleanup eviction growing - full",
			burstCacheSize:       burstCapBytes,
			blockSize:            blockBytes,
			currentSize:          0,
			cleanupEvictCounter:  evictingCounter,
			cleanupEvictBaseline: 3, // counter is at 5, baseline at 3 → growth detected
			wantFull:             true,
		},
		{
			name:                 "capacity not full and no cleanup eviction - not full",
			burstCacheSize:       burstCapBytes,
			blockSize:            blockBytes,
			currentSize:          0,
			cleanupEvictCounter:  quietCounter,
			cleanupEvictBaseline: quietCounter.Load(), // counter at 3, baseline at 3 → no growth
			wantFull:             false,
		},
		{
			name:           "counter unavailable (daemon mode) - capacity only",
			burstCacheSize: burstCapBytes,
			blockSize:      blockBytes,
			currentSize:    0,
			wantFull:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := &preloadState{
				cache: &isCacheFullMock{
					currentSize:    tt.currentSize,
					cacheSize:      tt.cacheSize,
					burstCacheSize: tt.burstCacheSize,
				},
				blockSize:            tt.blockSize,
				cleanupEvictCounter:  tt.cleanupEvictCounter,
				cleanupEvictBaseline: tt.cleanupEvictBaseline,
			}
			assert.Equal(t, tt.wantFull, state.isCacheFull())
		})
	}
}

// counterMock extends isCacheFullMock with a getCounter that returns a named
// AtomicCounter, so we can test the snapshot path used in handleLoad.
type counterMock struct {
	isCacheFullMock
	counters map[string]*metric.AtomicCounter
}

func (m *counterMock) getCounter(name string) (*metric.AtomicCounter, error) {
	if c, ok := m.counters[name]; ok {
		return c, nil
	}
	return nil, fmt.Errorf("counter %q not available in mock", name)
}

// TestIsCacheFullEvictionSnapshot verifies that the counter name used in
// handleLoad (chunk.CounterEvictsCleanup) matches what the backends register,
// and that the snapshot-then-compare logic detects eviction growth.
func TestIsCacheFullEvictionSnapshot(t *testing.T) {
	cleanupCounter := metric.NewCounter(prometheus.CounterOpts{
		Name: chunk.CounterEvictsCleanup,
	})
	cleanupCounter.Add(10)

	mock := &counterMock{
		isCacheFullMock: isCacheFullMock{
			burstCacheSize: 1000,
			currentSize:    0,
		},
		counters: map[string]*metric.AtomicCounter{
			chunk.CounterEvictsCleanup: cleanupCounter,
		},
	}

	state := &preloadState{
		cache:     mock,
		blockSize: 16,
	}

	// Simulate the snapshot path from handleLoad (line 381-383)
	counter, err := state.cache.getCounter(chunk.CounterEvictsCleanup)
	assert.NoError(t, err)
	state.cleanupEvictCounter = counter
	state.cleanupEvictBaseline = counter.Load()

	// No new eviction since snapshot — should not be full
	assert.False(t, state.isCacheFull())

	// Simulate cleanup eviction
	cleanupCounter.Add(1)
	assert.True(t, state.isCacheFull())
}

func TestShouldSkipHashRing(t *testing.T) {
	tests := []struct {
		name          string
		loadAll       bool
		cacheEviction chunk.CleanupMode
		serf          *MockSerfService // nil means c.serf is nil
		expectedSkip  bool
	}{
		{
			name:          "loadAll requested",
			loadAll:       true,
			cacheEviction: chunk.HashCleanup,
			serf:          &MockSerfService{memberCount: 3},
			expectedSkip:  true,
		},
		{
			name:          "hashring disabled",
			loadAll:       false,
			cacheEviction: chunk.RandomCleanup,
			serf:          &MockSerfService{memberCount: 3},
			expectedSkip:  true,
		},
		{
			name:          "serf is nil",
			loadAll:       false,
			cacheEviction: chunk.HashCleanup,
			serf:          nil,
			expectedSkip:  true,
		},
		{
			name:          "single node",
			loadAll:       false,
			cacheEviction: chunk.HashCleanup,
			serf:          &MockSerfService{memberCount: 1},
			expectedSkip:  true,
		},
		{
			name:          "multi-node with hashring enabled",
			loadAll:       false,
			cacheEviction: chunk.HashCleanup,
			serf:          &MockSerfService{memberCount: 3},
			expectedSkip:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &Cachefs{
				Conf: &Config{
					Store: &chunk.StoreConfig{
						CacheEviction: tt.cacheEviction,
					},
				},
			}
			if tt.serf != nil {
				fs.serf = tt.serf
			}
			req := &loadReq{loadAll: tt.loadAll}
			assert.Equal(t, tt.expectedSkip, fs.shouldSkipHashRing(req))
		})
	}
}

// TestStopOnEviction covers the stopWalkDir predicate returned by
// stopOnEviction, including the daemon-mode fallback: when the per-priority
// eviction counter can't be read, it must degrade to stopOnFull (stop once the
// cache is full) rather than to never-stop, which would churn the cache
// indefinitely.
func TestStopOnEviction(t *testing.T) {
	const counterName = chunk.CounterEvictsPinned
	const reason = "pinned blocks being evicted"

	// newState builds a preloadState whose cache is full/empty and whose named
	// eviction counter is either present (at `counterVal`) or absent.
	newState := func(full bool, withCounter bool, counterVal float64) (*preloadState, *metric.AtomicCounter) {
		var currentSize int64
		if full {
			currentSize = 1000 // >= burstCacheSize-blockSize below
		}
		mock := &counterMock{
			isCacheFullMock: isCacheFullMock{
				burstCacheSize: 1000,
				currentSize:    currentSize,
			},
			counters: map[string]*metric.AtomicCounter{},
		}
		var counter *metric.AtomicCounter
		if withCounter {
			counter = metric.NewCounter(prometheus.CounterOpts{Name: counterName})
			counter.Add(counterVal)
			mock.counters[counterName] = counter
		}
		return &preloadState{cache: mock, blockSize: 16}, counter
	}

	t.Run("counter present: cache not full - never stop", func(t *testing.T) {
		state, counter := newState(false, true, 5)
		pred := state.stopOnEviction(counterName, reason)
		counter.Add(10) // eviction happened, but cache isn't full
		stop, _ := pred()
		assert.False(t, stop)
	})

	t.Run("counter present: cache full but no new eviction - continue", func(t *testing.T) {
		state, _ := newState(true, true, 5)
		pred := state.stopOnEviction(counterName, reason)
		// no counter growth since the predicate captured its baseline
		stop, _ := pred()
		assert.False(t, stop)
	})

	t.Run("counter present: cache full and eviction growing - stop", func(t *testing.T) {
		state, counter := newState(true, true, 5)
		pred := state.stopOnEviction(counterName, reason)
		counter.Add(1) // equal-priority block evicted after baseline
		stop, got := pred()
		assert.True(t, stop)
		assert.Equal(t, reason, got)
	})

	t.Run("counter unavailable: cache not full - continue", func(t *testing.T) {
		state, _ := newState(false, false, 0)
		pred := state.stopOnEviction(counterName, reason)
		stop, _ := pred()
		assert.False(t, stop)
	})

	// Regression for the daemon-mode fallback: with the counter unavailable and
	// the cache full, the predicate must stop (like stopOnFull) instead of the
	// old never-stop behavior that kept churning the cache.
	t.Run("counter unavailable: cache full - stop like stopOnFull", func(t *testing.T) {
		state, _ := newState(true, false, 0)
		pred := state.stopOnEviction(counterName, reason)
		stop, got := pred()
		assert.True(t, stop)
		assert.Equal(t, "cache already full", got)
	})
}

// TestStopOnFull covers the plain cache-full predicate.
func TestStopOnFull(t *testing.T) {
	full := &preloadState{
		cache:     &isCacheFullMock{burstCacheSize: 1000, currentSize: 1000},
		blockSize: 16,
	}
	stop, reason := full.stopOnFull()
	assert.True(t, stop)
	assert.Equal(t, "cache already full", reason)

	notFull := &preloadState{
		cache:     &isCacheFullMock{burstCacheSize: 1000, currentSize: 0},
		blockSize: 16,
	}
	stop, _ = notFull.stopOnFull()
	assert.False(t, stop)
}

// errFS fails every Open with err, simulating a broken source dir replica.
type errFS struct{ err error }

func (e errFS) Open(string) (fs.File, error) { return nil, e.err }

// stubSourceDirFS swaps the sourceDirFS hook: walks on the bad dir hit err,
// everything else uses the real os.DirFS.
func stubSourceDirFS(t *testing.T, badDir string, err error) {
	t.Helper()
	orig := sourceDirFS
	t.Cleanup(func() { sourceDirFS = orig })
	sourceDirFS = func(dir string) fs.FS {
		if dir == badDir {
			return errFS{err}
		}
		return os.DirFS(dir)
	}
}
