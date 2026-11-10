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
	"fmt"
	"os"
	"runtime/debug"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aliyun/cachefs/pkg/metric"
	"github.com/aliyun/cachefs/pkg/rpcsvc"
	"github.com/aliyun/cachefs/pkg/serf"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/willf/bitset"
)

func TestGenBurstSizes(t *testing.T) {
	tests := []struct {
		name            string
		cacheSize       int64
		burstSize       int64
		deferBurst      bool
		wantBurstSize   int64
		wantInitialSize int64
	}{
		{
			name:            "burst disabled",
			cacheSize:       100,
			burstSize:       0,
			wantBurstSize:   100,
			wantInitialSize: 100,
		},
		{
			name:            "burst smaller than cache size",
			cacheSize:       100,
			burstSize:       50,
			wantBurstSize:   100,
			wantInitialSize: 100,
		},
		{
			name:            "burst equal to cache size",
			cacheSize:       100,
			burstSize:       100,
			wantBurstSize:   100,
			wantInitialSize: 100,
		},
		{
			name:            "burst larger than cache size",
			cacheSize:       100,
			burstSize:       200,
			wantBurstSize:   200,
			wantInitialSize: 200,
		},
		{
			name:            "defer burst larger than cache size",
			cacheSize:       100,
			burstSize:       200,
			deferBurst:      true,
			wantBurstSize:   100,
			wantInitialSize: 200,
		},
		{
			name:            "defer burst not larger than cache size",
			cacheSize:       100,
			burstSize:       50,
			deferBurst:      true,
			wantBurstSize:   100,
			wantInitialSize: 100,
		},
	}

	for _, tt := range tests {
		burstSize, initialBurstSize := GenBurstSizes(tt.cacheSize, tt.burstSize, tt.deferBurst)
		assert.Equal(t, tt.wantBurstSize, burstSize, tt.name)
		assert.Equal(t, tt.wantInitialSize, initialBurstSize, tt.name)

		// Feeding the recorded size back returns the same pair, so the command
		// line and the store may both resolve the same config.
		burstSize, initialBurstSize = GenBurstSizes(tt.cacheSize, initialBurstSize, tt.deferBurst)
		assert.Equal(t, tt.wantBurstSize, burstSize, tt.name)
		assert.Equal(t, tt.wantInitialSize, initialBurstSize, tt.name)
	}
}

func TestInitBurstSizes(t *testing.T) {
	tests := []struct {
		name        string
		conf        StoreConfig
		wantBurst   int64
		wantInitial int64
	}{
		{
			name:        "already settled by the command line",
			conf:        StoreConfig{CacheSize: 100, BurstCacheSize: 100, InitialBurstSize: 400, DeferBurst: true},
			wantBurst:   100,
			wantInitial: 400,
		},
		{
			name:        "hand-filled config without initial size",
			conf:        StoreConfig{CacheSize: 100, BurstCacheSize: 400},
			wantBurst:   400,
			wantInitial: 400,
		},
		{
			name:        "hand-filled config deferring burst",
			conf:        StoreConfig{CacheSize: 100, BurstCacheSize: 400, DeferBurst: true},
			wantBurst:   100,
			wantInitial: 400,
		},
		{
			name:        "hand-filled config below cache size",
			conf:        StoreConfig{CacheSize: 100, BurstCacheSize: 50},
			wantBurst:   100,
			wantInitial: 100,
		},
		{
			name:        "no burst sizes at all",
			conf:        StoreConfig{CacheSize: 100},
			wantBurst:   100,
			wantInitial: 100,
		},
	}

	for _, tt := range tests {
		conf := tt.conf
		for i := 0; i < 2; i++ { // idempotent, so a second store may reuse the config
			initBurstSizes(&conf)
			assert.Equal(t, tt.wantBurst, conf.BurstCacheSize, tt.name)
			assert.Equal(t, tt.wantInitial, conf.InitialBurstSize, tt.name)
		}
	}
}

type MockRemoteCache struct {
	mock.Mock
}

func (m *MockRemoteCache) announce(target string, length uint64, nrblk uint32) {
	m.Called(target, length, nrblk)
}

func (m *MockRemoteCache) revoke(target string, length uint64, nrblk uint32) {
	m.Called(target, length, nrblk)
}

func (m *MockRemoteCache) load(ctx context.Context, target string, nrblk, blklen uint32) (*Page, error) {
	args := m.Called(ctx, target, nrblk, blklen)
	if args.Get(0) != nil {
		return args.Get(0).(*Page), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockRemoteCache) setDataReady() {
	m.Called()
}

func (m *MockRemoteCache) stop() {
	m.Called()
}

func (m *MockRemoteCache) addMetaInfo(remoteMeta *metaInfo) (int, int) {
	m.Called(remoteMeta)
	return 0, 0
}

func (m *MockRemoteCache) reconcilePeer(remoteMeta *metaInfo) (int, int) {
	m.Called(remoteMeta)
	return 0, 0
}

func (m *MockRemoteCache) trackerSnapshot(prefixes []string) map[string]map[string]*bitset.BitSet {
	m.Called(prefixes)
	return map[string]map[string]*bitset.BitSet{}
}

type MockLocalCache struct {
	mock.Mock
}

func (m *MockLocalCache) cache(target string, blk uint32, p *Page) {
	m.Called(target, blk, p)
}

func (m *MockLocalCache) load(target string, blk uint32, touchLRU bool) (*Page, ChunkReader, error) {
	args := m.Called(target, blk, touchLRU)

	var page *Page
	if p := args.Get(0); p != nil {
		page = p.(*Page)
	}

	var r ChunkReader
	if rc := args.Get(1); rc != nil {
		r = rc.(ChunkReader)
	}

	return page, r, args.Error(2)
}

func (m *MockLocalCache) check(target string, blk uint32) bool {
	args := m.Called(target, blk)
	return args.Bool(0)
}

func (m *MockLocalCache) removeByPrefix(remove, keep []string) (int64, int64) {
	args := m.Called(remove, keep)
	return args.Get(0).(int64), args.Get(1).(int64)
}

func (m *MockLocalCache) getBlockFile(target string, blk uint32) (*os.File, error) {
	args := m.Called(target, blk)
	file, ok := args.Get(0).(*os.File)
	if !ok {
		return nil, args.Error(1)
	}
	return file, args.Error(1)
}

func (m *MockLocalCache) prepareBlockFile(target string, blk uint32, size uint32) (*os.File, error) {
	args := m.Called(target, blk, size)
	if file, ok := args.Get(0).(*os.File); ok {
		return file, args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockLocalCache) prepareFile(target string, length uint64) error {
	args := m.Called(target, length)
	return args.Error(0)
}

func (m *MockLocalCache) commitFile(target string) error {
	args := m.Called(target)
	return args.Error(0)
}

func (m *MockLocalCache) commitBlockFile(target string, blk uint32, commit bool) error {
	args := m.Called(target, blk, commit)
	return args.Error(0)
}

func (m *MockLocalCache) stats() (int64, int64) {
	args := m.Called()
	return args.Get(0).(int64), args.Get(1).(int64)
}

func (m *MockLocalCache) getCacheState() CacheState {
	args := m.Called()
	if cs, ok := args.Get(0).(CacheState); ok {
		return cs
	}
	return CacheState{}
}

func (m *MockLocalCache) dump() *metaInfo {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*metaInfo)
}

func (m *MockLocalCache) clearBurst() (int64, int64) {
	args := m.Called()
	return args.Get(0).(int64), args.Get(1).(int64)
}

func (m *MockLocalCache) setCapacity(cacheSize, burstCacheSize int64) (int64, int64, int64, int64) {
	args := m.Called(cacheSize, burstCacheSize)
	return args.Get(0).(int64), args.Get(1).(int64), args.Get(2).(int64), args.Get(3).(int64)
}

func (m *MockLocalCache) lockTarget(target string, blocks []uint32) {
	m.Called(target, blocks)
}

func (m *MockLocalCache) unlockTarget(target string, blocks []uint32) {
	m.Called(target, blocks)
}

func unsetEnv(key string) {
	if err := os.Unsetenv(key); err != nil {
		fmt.Printf("Warning: failed to unset environment variable %s\n", key)
	}
}

func TestNewStore_MemoryAllocation(t *testing.T) {
	mockStoreConfig, err := NewStoreConfig()
	assert.NoError(t, err)

	mockStoreConfig.CacheDir = "memory"

	unsetEnv("GOGC")
	unsetEnv("GOMEMLIMIT")

	reg := prometheus.NewRegistry()
	ss := serf.NewSerfService(serf.DefaultSerfConfig(), reg)
	rpc, err := rpcsvc.NewRpcService(&rpcsvc.RpcConfig{Addr: mockStoreConfig.CacheAddr})
	assert.NoError(t, err, "new rpc service")
	store := NewStore(mockStoreConfig, rpc, nil, ss, reg)
	assert.NotNil(t, store)

	if os.Getenv("GOGC") == "" && os.Getenv("GOMEMLIMIT") == "" {
		assert.Equal(t, -1, debug.SetGCPercent(-1))
		assert.Equal(t, int64(1<<30), debug.SetMemoryLimit(1<<30))
	}
}

func TestStore_Load(t *testing.T) {
	mockStoreConfig, err := NewStoreConfig()
	assert.NoError(t, err)

	remoteCache := new(MockRemoteCache)
	localCache := new(MockLocalCache)

	localCache.On("load", "target", uint32(0), true).Return(&Page{}, nil, nil).Once()

	store := &store{
		config:     mockStoreConfig,
		rcache:     remoteCache,
		bcache:     localCache,
		pendingOps: utils.NewPendingOpsManager(500 * time.Millisecond),
	}

	ctx := context.Background()
	page, err := store.Load(ctx, "target", 1024, 0)
	assert.NoError(t, err)
	assert.NotNil(t, page)

	localCache.AssertExpectations(t)
}

func TestStore_ReadBlock(t *testing.T) {
	mockStoreConfig, err := NewStoreConfig()
	assert.NoError(t, err)

	ctx := context.Background()
	remoteCache := new(MockRemoteCache)
	localCache := new(MockLocalCache)

	store := &store{
		config:     mockStoreConfig,
		rcache:     remoteCache,
		bcache:     localCache,
		pendingOps: utils.NewPendingOpsManager(500 * time.Millisecond),
	}
	store.remoteHits = metric.NewCounter(prometheus.CounterOpts{})
	store.remoteMiss = metric.NewCounter(prometheus.CounterOpts{})
	store.remoteHitBytes = metric.NewCounter(prometheus.CounterOpts{})
	store.remoteMissBytes = metric.NewCounter(prometheus.CounterOpts{})
	store.remoteReadHist = prometheus.NewHistogram(prometheus.HistogramOpts{})

	// local cache return page
	localCache.On("load", "target", uint32(0), true).Return(&Page{}, nil, nil).Once()
	reader, err := store.ReadBlock(ctx, "target", 1024, 0)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	localCache.AssertExpectations(t)

	// local cache return reader
	page := NewPage(make([]byte, 10))
	localCache.On("load", "target", uint32(0), true).Return(nil, NewPageReader(page), nil).Once()
	reader, err = store.ReadBlock(ctx, "target", 1024, 0)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	localCache.AssertExpectations(t)

	// local cache miss, remote hit
	localCache.On("load", "target", uint32(0), true).Return(nil, nil, ErrNotFound).Once()
	localCache.On("cache", "target", uint32(0), page).Return().Once()
	remoteCache.On("load", ctx, "target", uint32(0), uint32(1024)).Return(page, nil).Once()
	reader, err = store.ReadBlock(ctx, "target", 1024, 0)
	time.Sleep(time.Millisecond * 100) // wait for adding cache to local cache
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	localCache.AssertExpectations(t)
	remoteCache.AssertExpectations(t)

	// both local and remote miss
	localCache.On("load", "target", uint32(0), true).Return(nil, nil, ErrNotFound).Once()
	remoteCache.On("load", ctx, "target", uint32(0), uint32(1024)).Return(nil, ErrNotFound).Once()
	reader, err = store.ReadBlock(ctx, "target", 1024, 0)
	assert.Error(t, err)
	assert.Nil(t, reader)
	localCache.AssertExpectations(t)
	remoteCache.AssertExpectations(t)
}

func TestStore_Fill(t *testing.T) {
	mockStoreConfig, err := NewStoreConfig()
	assert.NoError(t, err)

	remoteCache := new(MockRemoteCache)
	localCache := new(MockLocalCache)
	data := make([]byte, 4)
	page := NewPage(data)

	ctx := context.Background()
	localCache.On("check", "target", uint32(0)).Return(true).Once()
	localCache.On("check", "target", uint32(1)).Return(false).Once()
	localCache.On("check", "target", uint32(2)).Return(false).Once()
	localCache.On("cache", "target", uint32(1), page).Once()
	remoteCache.On("load", ctx, "target", uint32(1), mockStoreConfig.BlockSize).Return(page, nil).Once()
	remoteCache.On("load", ctx, "target", uint32(2), mockStoreConfig.BlockSize).Return(nil, ErrNotFound).Once()

	store := &store{
		config:     mockStoreConfig,
		rcache:     remoteCache,
		bcache:     localCache,
		pendingOps: utils.NewPendingOpsManager(500 * time.Millisecond),
	}
	store.remoteHits = metric.NewCounter(prometheus.CounterOpts{})
	store.remoteMiss = metric.NewCounter(prometheus.CounterOpts{})
	store.remoteHitBytes = metric.NewCounter(prometheus.CounterOpts{})
	store.remoteMissBytes = metric.NewCounter(prometheus.CounterOpts{})
	store.remoteReadHist = prometheus.NewHistogram(prometheus.HistogramOpts{})

	err = store.Fill(ctx, "target", 1<<30, 0)
	assert.NoError(t, err)

	err = store.Fill(ctx, "target", 1<<30, 1)
	assert.NoError(t, err)

	err = store.Fill(ctx, "target", 1<<30, 2)
	assert.Error(t, err)

	// Wait for a while to let background localCache.cache to be scheduled
	time.Sleep(time.Millisecond * 10)
	localCache.AssertExpectations(t)
	remoteCache.AssertExpectations(t)
}

func TestStore_Cache(t *testing.T) {
	mockStoreConfig, err := NewStoreConfig()
	assert.NoError(t, err)

	localCache := new(MockLocalCache)

	store := &store{
		config: mockStoreConfig,
		bcache: localCache,
	}

	page := &Page{}
	localCache.On("cache", "target", uint32(0), page).Return().Once()

	store.Cache("target", 0, page)

	localCache.AssertExpectations(t)
}

func TestStore_Stop(t *testing.T) {
	mockStoreConfig, err := NewStoreConfig()
	assert.NoError(t, err)

	remoteCache := new(MockRemoteCache)

	remoteCache.On("stop").Return().Once()

	store := &store{
		config: mockStoreConfig,
		rcache: remoteCache,
		stopCh: make(chan struct{}),
	}

	store.Stop()

	remoteCache.AssertExpectations(t)
}

func TestBlockSize(t *testing.T) {
	store := &store{
		config: &StoreConfig{BlockSize: 4096},
	}

	length := uint64(10000)
	var blk uint32 = 1

	size := store.blockSize(length, blk)
	expectedSize := uint32(4096)

	if size != expectedSize {
		t.Errorf("Incorrect block size: got %v, want %v", size, expectedSize)
	}
}

func TestInitMetrics(t *testing.T) {
	store := &store{
		counterReg: metric.NewCounterRegistry(),
	}
	reg := prometheus.NewRegistry()
	store.InitMetrics(reg)

	if store.remoteHits == nil {
		t.Error("Expected remoteHits to be initialized")
	}
	if store.remoteMiss == nil {
		t.Error("Expected remoteMiss to be initialized")
	}
}

func TestChecksumEnabled(t *testing.T) {
	tests := []struct {
		hasher  string
		enabled bool
	}{
		{"crc32", true},
		{"", false},
	}
	for _, tc := range tests {
		store := &store{
			config: &StoreConfig{Hasher: tc.hasher},
		}
		assert.Equal(t, tc.enabled, store.checksumEnabled())
	}
}

func TestParseDigest(t *testing.T) {
	tests := []struct {
		hash  string
		local string
		want1 string
		want2 string
		pass  bool
	}{
		{"crc32:11223344", "crc32", "crc32", "11223344", true},
		{"crc32:11223344", "md5", "", "", false},
		{"crc3211223344", "crc32", "", "", false},
		{"crc32:test:11223344", "crc32", "", "", false},
		{"", "", "", "", true},
		{"", "crc", "", "", false},
		{"crc3211223344", "", "", "", false},
	}
	for _, tc := range tests {
		res1, res2, err := parseDigest(tc.hash, tc.local)
		if res1 != tc.want1 {
			t.Errorf("Test hash \"%s\" local \"%s\": expect hasher %s got %s", tc.hash, tc.local, tc.want1, res1)
		}
		if res2 != tc.want2 {
			t.Errorf("Test hash \"%s\" local \"%s\": expect digest %s got %s", tc.hash, tc.local, tc.want2, res2)
		}
		if (err == nil) != tc.pass {
			t.Errorf("Test hash \"%s\" local \"%s\": expect result pass %v got %s", tc.hash, tc.local, tc.pass, err)
		}
	}
}

func TestEncodeDigest(t *testing.T) {
	tests := []struct {
		digest string
		hasher string
		want   string
	}{
		{"11223344", "crc32", "crc32:11223344"},
		{"abcd1234", "md5", "md5:abcd1234"},
		{"", "crc32", "crc32:"},
		{"11223344", "", ":11223344"},
		{"", "", ":"},
	}

	for _, tc := range tests {
		result := encodeDigest(tc.digest, tc.hasher)
		assert.Equal(t, tc.want, result, "encodeDigest(%q, %q)", tc.digest, tc.hasher)
	}
}

type MockHistogram struct {
	mock.Mock
}

func NewMockHistogram() *MockHistogram {
	return &MockHistogram{}
}

func (m *MockHistogram) Observe(val float64) {
	m.Called(val)
}

func (m *MockHistogram) Desc() *prometheus.Desc {
	args := m.Called()
	return args.Get(0).(*prometheus.Desc)
}

func (m *MockHistogram) Write(out *dto.Metric) error {
	args := m.Called(out)
	return args.Error(0)
}

func (m *MockHistogram) Describe(ch chan<- *prometheus.Desc) {
	m.Called(ch)
}

func (m *MockHistogram) Collect(ch chan<- prometheus.Metric) {
	m.Called(ch)
}

func TestLoadRemote(t *testing.T) {
	mockCache := new(MockRemoteCache)

	store := &store{
		rcache:             mockCache,
		remoteHits:         metric.NewCounter(prometheus.CounterOpts{}),
		remoteMiss:         metric.NewCounter(prometheus.CounterOpts{}),
		remoteHitBytes:     metric.NewCounter(prometheus.CounterOpts{}),
		remoteMissBytes:    metric.NewCounter(prometheus.CounterOpts{}),
		remoteReadHist:     new(MockHistogram),
		remoteReadInFlight: 0,
	}

	ctx := context.Background()
	target := "test-target"
	blk := uint32(1)
	blklen := uint32(4096)

	t.Run("HitCase", func(t *testing.T) {
		page := &Page{}
		mockCache.On("load", ctx, target, blk, blklen).Return(page, nil).Once()

		store.remoteReadHist.(*MockHistogram).On("Observe", mock.Anything).Return()

		result, err := store.loadRemote(ctx, target, blk, blklen)
		assert.NoError(t, err)
		assert.Equal(t, page, result)
		assert.Equal(t, int32(0), store.remoteReadInFlight)

		assert.Equal(t, float64(1), store.remoteHits.Load())
		assert.Equal(t, float64(blklen), store.remoteHitBytes.Load())
		store.remoteReadHist.(*MockHistogram).AssertCalled(t, "Observe", mock.Anything)

		mockCache.AssertExpectations(t)
	})

	t.Run("MissCase", func(t *testing.T) {
		mockCache.On("load", ctx, target, blk, blklen).Return(nil, errors.New("not found")).Once()

		result, err := store.loadRemote(ctx, target, blk, blklen)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, int32(0), store.remoteReadInFlight)

		assert.Equal(t, float64(1), store.remoteMiss.Load())
		assert.Equal(t, float64(blklen), store.remoteMissBytes.Load())

		mockCache.AssertExpectations(t)
	})
}

type MockReader struct {
	mock.Mock
}

func (m *MockReader) Read(p []byte) (n int, err error) {
	args := m.Called(p)
	return args.Int(0), args.Error(1)
}

func (m *MockReader) ReadAt(p []byte, off int64) (n int, err error) {
	args := m.Called(p, off)
	return args.Int(0), args.Error(1)
}

func (m *MockReader) Close() error {
	return m.Called().Error(0)
}

func (m *MockReader) ExportFd() (int, *utils.Memfd, error) {
	args := m.Called()
	memfd, _ := args.Get(1).(*utils.Memfd)
	return args.Int(0), memfd, args.Error(2)
}

func TestStore_DoLoad(t *testing.T) {
	mockStoreConfig, err := NewStoreConfig()
	assert.NoError(t, err)

	remoteCache := new(MockRemoteCache)
	localCache := new(MockLocalCache)
	mockReader := new(MockReader)

	mockHist := new(MockHistogram)

	mockHist.On("Observe", mock.Anything).Return()

	store := &store{
		config:             mockStoreConfig,
		rcache:             remoteCache,
		bcache:             localCache,
		remoteHits:         metric.NewCounter(prometheus.CounterOpts{}),
		remoteMiss:         metric.NewCounter(prometheus.CounterOpts{}),
		remoteHitBytes:     metric.NewCounter(prometheus.CounterOpts{}),
		remoteMissBytes:    metric.NewCounter(prometheus.CounterOpts{}),
		remoteReadHist:     mockHist,
		remoteReadInFlight: 0,
	}

	ctx := context.Background()
	target := "target"
	blk := uint32(0)
	blklen := uint32(1024)

	t.Run("LocalCacheHit", func(t *testing.T) {
		localCache.On("load", target, blk, false).Return(&Page{}, nil, nil).Once()

		page, err := store.doLoad(ctx, target, blk, blklen, false, false)
		assert.NoError(t, err)
		assert.NotNil(t, page)

		localCache.AssertExpectations(t)
	})

	t.Run("LocalCacheExistsButFailsToRead", func(t *testing.T) {
		mockReader.On("ReadAt", mock.Anything, int64(0)).Return(0, errors.New("read error")).Once()
		mockReader.On("Close").Return(nil).Once()
		localCache.On("load", target, blk, false).Return(nil, mockReader, nil).Once()

		page, err := store.doLoad(ctx, target, blk, blklen, false, false)
		assert.Error(t, err)
		assert.Nil(t, page)

		localCache.AssertExpectations(t)
		// Since memory allocated by mmap becomes inaccessible after being released,
		// we no longer verify the calls to mockReader.
	})
}

func TestGenTarget(t *testing.T) {
	tests := []struct {
		prefix   string
		sourceID string
		target   string
		expect   string
	}{
		{"", "", "target", "target"},
		{"", "", "/target", "/target"},
		{"", "", "/target/", "/target/"},
		{"/", "", "/target", "//target"},
		{"/prefix", "", "/target", "/prefix/target"},
		{"/prefix/", "", "/target", "/prefix//target"},
		{"/prefix/", "", "/target/", "/prefix//target/"},
		{"/prefix/2/", "", "/target/", "/prefix/2//target/"},
		// SourceID only
		{"", "mybucket", "/target", "/stag_mybucket/target"},
		{"", "mybucket", "/path/to/file", "/stag_mybucket/path/to/file"},
		// KeyPrefix + SourceID
		{"/prefix", "mybucket", "/target", "/prefix/stag_mybucket/target"},
		{"/prefix", "mybucket", "/path/to/file", "/prefix/stag_mybucket/path/to/file"},
	}

	conf, err := NewStoreConfig()
	assert.NoError(t, err)
	conf.CacheDir = "memory"
	assert := assert.New(t)
	for _, tc := range tests {
		conf.KeyPrefix = tc.prefix
		var sourcePrefix string
		if tc.sourceID != "" {
			sourcePrefix = "/stag_" + tc.sourceID
		}
		store := &store{
			config:       conf,
			sourcePrefix: sourcePrefix,
		}
		id := fmt.Sprintf("genTarget: prefix=%q sourceID=%q target=%q", tc.prefix, tc.sourceID, tc.target)
		result := store.genTarget(tc.target)
		assert.Equal(tc.expect, result, id)
	}
}

func TestStore_KeyPrefix(t *testing.T) {
	conf, err := NewStoreConfig()
	assert.NoError(t, err)
	conf.KeyPrefix = "/prefix"
	conf.CacheDir = "memory"

	rpc, err := rpcsvc.NewRpcService(&rpcsvc.RpcConfig{Addr: conf.CacheAddr})
	assert.NoError(t, err, "new rpc service")
	store := doNewStore(conf, rpc, nil, serf.NewSerfService(serf.DefaultSerfConfig(), nil), nil)
	assert.Equal(t, false, store == nil, "KeyPrefix: create store")

	// first load, no cache
	ctx := context.Background()
	_, err = store.Load(ctx, "/target", 4, 0)
	assert.Equal(t, ErrNotFound, err, "KeyPrefix: first Load")

	// Cache /target, but key prefix should be added
	data := []byte{'a', 'b'}
	blklen := uint32(len(data))
	store.Cache("/target", 0, NewPage(data))
	_, err = store.doLoad(ctx, "/target", 0, blklen, false, false)
	assert.Equal(t, ErrNotFound, err, "KeyPrefix: doLoad /target")

	_, err = store.doLoad(ctx, "/prefix/target", 0, blklen, false, false)
	assert.NoError(t, err, "KeyPrefix: doLoad /prefix/target")

	_, err = store.Load(ctx, "/target", uint64(blklen), 0)
	assert.NoError(t, err, "KeyPrefix: Load /target")

	_, err = store.Load(ctx, "/prefix/target", uint64(blklen), 0)
	assert.Equal(t, ErrNotFound, err, "KeyPrefix: Load /target")
}

func TestStore_SourceID(t *testing.T) {
	newStore := func(t *testing.T, sourceID, keyPrefix string) *store {
		conf, err := NewStoreConfig()
		assert.NoError(t, err)
		conf.SourceID = sourceID
		conf.KeyPrefix = keyPrefix
		conf.CacheDir = "memory"
		rpc, err := rpcsvc.NewRpcService(&rpcsvc.RpcConfig{Addr: conf.CacheAddr})
		assert.NoError(t, err)
		return doNewStore(conf, rpc, nil, serf.NewSerfService(serf.DefaultSerfConfig(), nil), nil)
	}

	t.Run("source id only", func(t *testing.T) {
		store := newStore(t, "mybucket", "")
		assert.Equal(t, "/stag_mybucket", store.sourcePrefix)

		ctx := context.Background()
		_, err := store.Load(ctx, "/target", 4, 0)
		assert.Equal(t, ErrNotFound, err)

		data := []byte{'a', 'b'}
		blklen := uint32(len(data))
		store.Cache("/target", 0, NewPage(data))

		_, err = store.doLoad(ctx, "/target", 0, blklen, false, false)
		assert.Equal(t, ErrNotFound, err, "doLoad without sourcePrefix should not find it")

		_, err = store.doLoad(ctx, "/stag_mybucket/target", 0, blklen, false, false)
		assert.NoError(t, err, "doLoad with sourcePrefix should find it")

		_, err = store.Load(ctx, "/target", uint64(blklen), 0)
		assert.NoError(t, err, "Load goes through genTarget, should find it")
	})

	t.Run("source id with key prefix", func(t *testing.T) {
		store := newStore(t, "mybucket", "/prefix")

		ctx := context.Background()
		data := []byte{'a', 'b'}
		blklen := uint32(len(data))
		store.Cache("/target", 0, NewPage(data))

		_, err := store.doLoad(ctx, "/prefix/stag_mybucket/target", 0, blklen, false, false)
		assert.NoError(t, err, "doLoad with full prefix should find it")

		_, err = store.doLoad(ctx, "/stag_mybucket/target", 0, blklen, false, false)
		assert.Equal(t, ErrNotFound, err, "doLoad with partial prefix should not find it")

		_, err = store.Load(ctx, "/target", uint64(blklen), 0)
		assert.NoError(t, err, "Load goes through genTarget, should find it")
	})
}

func TestStoreConfig_IsMemoryBackend(t *testing.T) {
	tests := []struct {
		name     string
		cacheDir string
		expected bool
	}{
		{
			name:     "memory backend",
			cacheDir: "memory",
			expected: true,
		},
		{
			name:     "file backend with path",
			cacheDir: "/tmp/cache",
			expected: false,
		},
		{
			name:     "file backend with relative path",
			cacheDir: "./cache",
			expected: false,
		},
		{
			name:     "empty cache dir",
			cacheDir: "",
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conf := &StoreConfig{
				CacheDir: tc.cacheDir,
			}
			result := conf.IsMemoryBackend()
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestCachefsStore_PendingCache(t *testing.T) {
	store := &store{
		pendingOps: utils.NewPendingOpsManager(500 * time.Millisecond),
	}

	target := "test-target"
	blk := uint32(10)

	t.Run("AddAndCheckPendingCache", func(t *testing.T) {
		// Initially no pending cache
		cached := store.checkPendingCache(target, blk)
		if cached {
			t.Error("Expected no pending cache, but got cached=true")
			return
		}

		// Add pending cache
		store.addPendingCache(target, blk)
		key := CacheKey{Target: target, Blk: blk}
		if !store.pendingOps.Exists(key.String()) {
			t.Error("Expected pending cache to be added")
			return
		}
	})

	t.Run("RemovePendingCache", func(t *testing.T) {
		target2 := "test-target-2"
		blk2 := uint32(20)

		// Add pending cache
		store.addPendingCache(target2, blk2)
		key := CacheKey{Target: target2, Blk: blk2}

		// Remove pending cache
		store.removePendingCache(target2, blk2)
		if store.pendingOps.Exists(key.String()) {
			t.Error("Expected pending cache to be removed")
			return
		}

		// Remove non-existent key should not panic
		store.removePendingCache("non-existent", 999)
	})

	t.Run("CheckPendingCacheTimeout", func(t *testing.T) {
		target3 := "test-target-3"
		blk3 := uint32(30)

		// Add pending cache but never remove it
		store.addPendingCache(target3, blk3)

		start := time.Now()
		cached := store.checkPendingCache(target3, blk3)
		duration := time.Since(start)

		// Should timeout and return false
		if cached {
			t.Error("Expected timeout with cached=false, but got cached=true")
			return
		}

		// Should take approximately 500ms (timeout duration)
		if duration < 400*time.Millisecond || duration > 600*time.Millisecond {
			t.Errorf("Expected timeout around 500ms, got %v", duration)
			return
		}

		// Clean up
		store.removePendingCache(target3, blk3)
	})

	t.Run("CheckPendingCacheSignaled", func(t *testing.T) {
		target4 := "test-target-4"
		blk4 := uint32(40)

		// Add pending cache
		store.addPendingCache(target4, blk4)

		// Signal after 100ms
		go func() {
			time.Sleep(100 * time.Millisecond)
			store.removePendingCache(target4, blk4)
		}()

		start := time.Now()
		cached := store.checkPendingCache(target4, blk4)
		duration := time.Since(start)

		// Should be signaled and return true
		if !cached {
			t.Error("Expected to be signaled with cached=true, but got cached=false")
			return
		}

		// Should take approximately 100ms (signal time)
		if duration < 50*time.Millisecond || duration > 200*time.Millisecond {
			t.Errorf("Expected signal around 100ms, got %v", duration)
			return
		}
	})
}

func TestCachefsStore_AddToBcache(t *testing.T) {
	mockBCache := new(MockLocalCache)
	store := &store{
		bcache:     mockBCache,
		pendingOps: utils.NewPendingOpsManager(500 * time.Millisecond),
	}

	target := "test-target"
	blk := uint32(5)
	data := make([]byte, 4096)
	page := NewPage(data)
	page.Acquire() // Acquire before passing to addToBcache

	mockBCache.On("cache", target, blk, page).Return().Once()

	// Call addToBcache
	store.addToBcache(target, blk, page)

	// Verify pending cache was added
	key := CacheKey{Target: target, Blk: blk}
	if !store.pendingOps.Exists(key.String()) {
		t.Error("Expected pending cache to be added by addToBcache")
		return
	}

	// Wait for goroutine to complete
	time.Sleep(100 * time.Millisecond)

	// Verify bcache.cache was called
	mockBCache.AssertExpectations(t)

	// Verify pending cache was removed
	if store.pendingOps.Exists(key.String()) {
		t.Error("Expected pending cache to be removed after addToBcache completes")
		return
	}
}

func TestCachefsStore_ConcurrentPendingCache(t *testing.T) {
	mockBCache := new(MockLocalCache)
	store := &store{
		bcache:     mockBCache,
		pendingOps: utils.NewPendingOpsManager(500 * time.Millisecond),
	}

	target := "test-target-concurrent"
	blk := uint32(100)
	data := make([]byte, 4096)
	page := NewPage(data)
	page.Acquire()

	// Mock bcache to simulate slow caching operation
	mockBCache.On("cache", target, blk, page).Run(func(args mock.Arguments) {
		time.Sleep(200 * time.Millisecond)
	}).Return().Once()

	// Start first addToBcache (simulates first load)
	store.addToBcache(target, blk, page)

	// Small delay to ensure first addToBcache has added pending cache
	time.Sleep(10 * time.Millisecond)

	// Start second checkPendingCache (simulates concurrent load)
	done := make(chan bool, 1)
	go func() {
		cached := store.checkPendingCache(target, blk)
		done <- cached
	}()

	// Wait for result
	select {
	case cached := <-done:
		// Should be signaled by removePendingCache
		if !cached {
			t.Error("Expected concurrent check to be signaled with cached=true")
			return
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("Concurrent checkPendingCache timed out unexpectedly")
		return
	}

	// Wait for background goroutine to complete
	time.Sleep(100 * time.Millisecond)
	mockBCache.AssertExpectations(t)
}

func TestStoreConfig_RdmaEnabled(t *testing.T) {
	tests := []struct {
		name          string
		transport     string
		cacheRootXprt string
		expected      bool
	}{
		{
			name:          "TCP transport only",
			transport:     "tcp",
			cacheRootXprt: "tcp",
			expected:      false,
		},
		{
			name:          "RDMA transport",
			transport:     "rdma",
			cacheRootXprt: "tcp",
			expected:      true,
		},
		{
			name:          "RDMA transport with cacheroot",
			transport:     "rdma",
			cacheRootXprt: "rdma",
			expected:      true,
		},
		{
			name:          "RDMA prefix is not RDMA",
			transport:     "rdma-v2",
			cacheRootXprt: "tcp",
			expected:      false,
		},
		{
			name:          "CacheRootXprt RDMA only",
			transport:     "tcp",
			cacheRootXprt: "rdma",
			expected:      true,
		},
		{
			name:          "CacheRootXprt RDMA prefix is not RDMA",
			transport:     "tcp",
			cacheRootXprt: "rdma-v2",
			expected:      false,
		},
		{
			name:          "Empty transport and cacheroot",
			transport:     "",
			cacheRootXprt: "",
			expected:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conf := &StoreConfig{
				Transport:     tc.transport,
				CacheRootXprt: tc.cacheRootXprt,
			}
			result := conf.RdmaEnabled()
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestStore_ClearBurst(t *testing.T) {
	mockStoreConfig, err := NewStoreConfig()
	assert.NoError(t, err)

	localCache := new(MockLocalCache)
	s := &store{
		config: mockStoreConfig,
		bcache: localCache,
	}

	// Verify that ClearBurst delegates to bcache.clearBurst and returns its result
	localCache.On("clearBurst").Return(int64(5), int64(1024)).Once()

	cnt, bytes := s.ClearBurst()

	assert.Equal(t, int64(5), cnt)
	assert.Equal(t, int64(1024), bytes)
	localCache.AssertExpectations(t)
}

func TestStore_GetCacheSize(t *testing.T) {
	tests := []struct {
		name      string
		cacheSize int64
		burstSize int64
	}{
		{
			name:      "returns configured cache and burst size",
			cacheSize: int64(100 << 20),
			burstSize: int64(200 << 20),
		},
		{
			name:      "returns zero burst when burst is disabled",
			cacheSize: int64(512 << 20),
			burstSize: 0,
		},
		{
			name:      "reflects values written atomically by SetCacheSize",
			cacheSize: int64(256 << 20),
			burstSize: int64(256 << 20), // burst == cache (no extra burst headroom)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStoreConfig, err := NewStoreConfig()
			assert.NoError(t, err)

			// Populate config fields with atomic writes, mirroring what SetCacheSize does.
			atomic.StoreInt64(&mockStoreConfig.CacheSize, tt.cacheSize)
			atomic.StoreInt64(&mockStoreConfig.BurstCacheSize, tt.burstSize)

			s := &store{config: mockStoreConfig}

			gotCache, gotBurst := s.GetCacheSize()

			assert.Equal(t, tt.cacheSize, gotCache,
				"GetCacheSize should return the atomically stored CacheSize")
			assert.Equal(t, tt.burstSize, gotBurst,
				"GetCacheSize should return the atomically stored BurstCacheSize")
		})
	}
}

func TestStore_SetCacheSize(t *testing.T) {
	tests := []struct {
		name              string
		reqCache          int64 // input cache size (bytes)
		reqBurst          int64 // input burst size (bytes)
		backendNewCache   int64 // effective values returned by backend after normalization
		backendNewBurst   int64
		backendEvictCnt   int64
		backendEvictBytes int64
	}{
		{
			name:              "config synced with requested values",
			reqCache:          int64(100 << 20),
			reqBurst:          int64(200 << 20),
			backendNewCache:   int64(100 << 20),
			backendNewBurst:   int64(200 << 20),
			backendEvictCnt:   3,
			backendEvictBytes: 512,
		},
		{
			name:              "config synced with normalized burst (burst < cache, reset to cache)",
			reqCache:          int64(300 << 20),
			reqBurst:          int64(100 << 20), // burst < cache, backend normalizes to 300MiB
			backendNewCache:   int64(300 << 20),
			backendNewBurst:   int64(300 << 20), // effective burst equals cache after normalization
			backendEvictCnt:   0,
			backendEvictBytes: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStoreConfig, err := NewStoreConfig()
			assert.NoError(t, err)

			localCache := new(MockLocalCache)
			s := &store{
				config: mockStoreConfig,
				bcache: localCache,
			}

			localCache.On("setCapacity", tt.reqCache, tt.reqBurst).
				Return(tt.backendNewCache, tt.backendNewBurst, tt.backendEvictCnt, tt.backendEvictBytes).Once()

			newCache, newBurst, cnt, bytes := s.SetCacheSize(tt.reqCache, tt.reqBurst)

			// Verify return values
			assert.Equal(t, tt.backendNewCache, newCache)
			assert.Equal(t, tt.backendNewBurst, newBurst)
			assert.Equal(t, tt.backendEvictCnt, cnt)
			assert.Equal(t, tt.backendEvictBytes, bytes)
			localCache.AssertExpectations(t)
		})
	}
}
