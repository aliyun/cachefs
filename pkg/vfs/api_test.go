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
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aliyun/cachefs/pkg/chunk"
	chunkcommon "github.com/aliyun/cachefs/pkg/chunk/common"
	"github.com/aliyun/cachefs/pkg/meta"
	cachefsmeta "github.com/aliyun/cachefs/pkg/meta/cachefs"
	"github.com/aliyun/cachefs/pkg/metric"
	pb "github.com/aliyun/cachefs/pkg/ttrpc/cachefs"
	"github.com/aliyun/cachefs/pkg/vfs/apiclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/willf/bitset"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewApiServer(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")

	fs := &Cachefs{}
	server, err := newApiServer(socketPath, fs, nil)
	assert.NoError(t, err)
	assert.NotNil(t, server)
	assert.Equal(t, socketPath, server.socket)
	assert.NotNil(t, server.ttrpcServer)
	assert.NotNil(t, server.listener)
	assert.NotNil(t, server.fdTransfer)
	assert.Equal(t, socketPath+".fd", server.fdSocketPath)
	server.stop()
	assert.Nil(t, server.ttrpcServer)
	assert.Nil(t, server.listener)
	// fdTransfer outlives stop() on purpose: in-flight handlers still reach it.
	require.NotNil(t, server.fdTransfer)
	server.fdTransfer.Lock()
	assert.True(t, server.fdTransfer.stopped)
	server.fdTransfer.Unlock()
}

func TestNewApiClient(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")

	// Test successful creation
	fs := &Cachefs{}
	server, err := newApiServer(socketPath, fs, nil)
	assert.NoError(t, err)
	server.serve()
	client, err := apiclient.NewApiClient(socketPath)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	assert.Equal(t, socketPath, client.SockPath)
	assert.Equal(t, socketPath+".fd", client.FdSockPath)
	assert.NotNil(t, client.TtrpcSvc)
	assert.NotNil(t, client.TtrpcConn)
	server.stop()
	client.Close()

	// Test with non-existent socket, creation should fail
	client, err = apiclient.NewApiClient("/some/non-existent/file.sock")
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestApiServerGetPid(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")

	fs := &Cachefs{}
	server, err := newApiServer(socketPath, fs, nil)
	require.NoError(t, err)
	defer server.stop()

	ctx := context.Background()
	req := &pb.GetPidRequest{}
	resp, err := server.GetPid(ctx, req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, int64(os.Getpid()), resp.Pid)
}

func TestApiServerUnmount(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")

	fs := &Cachefs{
		Conf: &Config{
			Meta: &cachefsmeta.MetaConfig{
				MountPoint: tmpDir,
			},
		},
	}
	server, err := newApiServer(socketPath, fs, nil)
	require.NoError(t, err)
	defer server.stop()

	ctx := context.Background()
	req := &pb.UnmountRequest{
		Name: "test-client",
		Pid:  int64(os.Getpid()),
	}

	// This test will trigger unmount timeout since we don't have real mount
	done := make(chan struct{})
	go func() {
		_, err := server.Unmount(ctx, req)
		assert.NoError(t, err)
		close(done)
	}()

	// Wait for a short time to ensure the unmount process starts
	select {
	case <-done:
		// Unmount completed successfully
	case <-time.After(100 * time.Millisecond):
		// Unmount is in progress, which is expected behavior
	}
}

func TestApiServerDataReady(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")

	fs := &Cachefs{
		Conf: &Config{
			Meta: &cachefsmeta.MetaConfig{},
		},
		cache: &mockCacheManager{},
	}
	server, err := newApiServer(socketPath, fs, nil)
	require.NoError(t, err)
	defer server.stop()

	ctx := context.Background()
	req := &pb.DataReadyRequest{}

	// This should fail because we're not in sourceless meta consumer mode
	_, err = server.DataReady(ctx, req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not in sourceless meta consumer mode")
}

func TestApiServerGetServerInfo(t *testing.T) {
	newSocketPath := func() string {
		return filepath.Join(os.TempDir(), fmt.Sprintf("cachefs-info-%d.sock", time.Now().UnixNano()))
	}

	t.Run("returns configured snapshot without setting cache size", func(t *testing.T) {
		cache := &mockCacheManager{cacheSize: 64 << 20, burstCacheSize: 128 << 20}
		server, err := newApiServer(newSocketPath(), &Cachefs{
			Conf:  &Config{Store: &chunk.StoreConfig{BlockSize: 4 << 20}, ShareReserve: 32 << 20},
			cache: cache,
		}, nil)
		require.NoError(t, err)
		defer server.stop()

		resp, err := server.GetServerInfo(context.Background(), &pb.GetServerInfoRequest{})
		require.NoError(t, err)
		assert.Equal(t, uint32(4<<20), resp.GetBlockSize())
		assert.Equal(t, int64(64<<20), resp.GetCacheSize())
		assert.Equal(t, int64(128<<20), resp.GetBurstCacheSize())
		assert.Equal(t, int64(96<<20), resp.GetShareCap())
		assert.Equal(t, int64(os.Getpid()), resp.GetPid())
		assert.Equal(t, 0, cache.setCacheSizeCalls)
	})

	t.Run("returns zero snapshot without configuration", func(t *testing.T) {
		server, err := newApiServer(newSocketPath(), &Cachefs{}, nil)
		require.NoError(t, err)
		defer server.stop()

		resp, err := server.GetServerInfo(context.Background(), &pb.GetServerInfoRequest{})
		require.NoError(t, err)
		assert.Zero(t, resp.GetBlockSize())
		assert.Zero(t, resp.GetCacheSize())
		assert.Zero(t, resp.GetBurstCacheSize())
		assert.Zero(t, resp.GetShareCap())
		assert.Equal(t, int64(os.Getpid()), resp.GetPid())
	})
}

func TestApiServerSetCacheSize_InvalidSentinels(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")

	fs := &Cachefs{
		Conf:  &Config{},
		cache: &mockCacheManager{},
	}
	server, err := newApiServer(socketPath, fs, nil)
	require.NoError(t, err)
	defer server.stop()

	tests := []struct {
		name    string
		req     *pb.SetCacheSizeRequest
		wantErr string
	}{
		{
			name: "invalid cache size sentinel",
			req: &pb.SetCacheSizeRequest{
				CacheSize:      -3,
				BurstCacheSize: chunkcommon.CacheSizeKeepCurrent,
			},
			wantErr: "invalid cache size sentinel: -3",
		},
		{
			name: "invalid burst cache size sentinel",
			req: &pb.SetCacheSizeRequest{
				CacheSize:      chunkcommon.CacheSizeKeepCurrent,
				BurstCacheSize: -3,
			},
			wantErr: "invalid burst cache size sentinel: -3",
		},
		{
			name: "valid keep current and restore sentinels",
			req: &pb.SetCacheSizeRequest{
				CacheSize:      chunkcommon.CacheSizeKeepCurrent,
				BurstCacheSize: chunkcommon.BurstCacheSizeRestore,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := server.SetCacheSize(context.Background(), tt.req)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestApiServerSetCacheSizeReportsShareCap(t *testing.T) {
	fs := &Cachefs{
		Conf:  &Config{ShareReserve: 1024},
		cache: &mockCacheManager{cacheSize: 1024, burstCacheSize: 2048},
	}
	server, err := newApiServer(filepath.Join(t.TempDir(), "test.sock"), fs, nil)
	require.NoError(t, err)
	defer server.stop()

	resp, err := server.SetCacheSize(context.Background(), &pb.SetCacheSizeRequest{CacheSize: 2048, BurstCacheSize: 4096})
	require.NoError(t, err)
	assert.Equal(t, int64(3072), resp.GetShareCap())
}

func TestFmtSetCacheSizeArg(t *testing.T) {
	tests := []struct {
		name     string
		value    int64
		expected string
	}{
		{
			name:     "keep current sentinel",
			value:    chunkcommon.CacheSizeKeepCurrent,
			expected: "keep",
		},
		{
			name:     "restore burst sentinel",
			value:    chunkcommon.BurstCacheSizeRestore,
			expected: "restore",
		},
		{
			name:     "zero bytes",
			value:    0,
			expected: "0MB",
		},
		{
			name:     "regular size in bytes",
			value:    int64(2048 << 20),
			expected: "2048MB",
		},
		{
			name:     "sub-megabyte rounds down to zero",
			value:    int64(1 << 19),
			expected: "0MB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, fmtSetCacheSizeArg(tt.value))
		})
	}
}

// burstCacheManager reports a fixed positive burst cache size so GetServerInfo
// (and the auto-limit constructors built on it) observe a dynamic burst to size a
// limiter from. The default mockCacheManager reports zero capacity,
// which models a daemon without dynamic cache sizing.
type burstCacheManager struct {
	mockCacheManager
	burst int64
}

func (m *burstCacheManager) setCacheSize(cacheSize, burstCacheSize int64) (int64, int64, int64, int64) {
	return cacheSize, m.burst, 0, 0
}

func (m *burstCacheManager) getCacheSize() (int64, int64) {
	return 0, m.burst
}

// startAutoLimitServer starts an in-process API server backed by cache and returns
// its socket path, so the auto-limit constructors can probe GetServerInfo over a
// real ttrpc connection. The server is torn down when the test finishes.
func startAutoLimitServer(t *testing.T, cache cacheReader) string {
	t.Helper()
	socketPath := filepath.Join(t.TempDir(), "test.sock")
	fs := &Cachefs{Conf: &Config{}, cache: cache}
	server, err := newApiServer(socketPath, fs, nil)
	require.NoError(t, err)
	server.serve()
	t.Cleanup(server.stop)
	time.Sleep(10 * time.Millisecond)
	return socketPath
}

func TestNewApiClientAutoLimit(t *testing.T) {
	t.Run("sizes limiter from daemon burst", func(t *testing.T) {
		sock := startAutoLimitServer(t, &burstCacheManager{burst: 4096})
		client, err := apiclient.NewApiClientAutoLimit(sock)
		require.NoError(t, err)
		defer client.Close()
		assert.Equal(t, int64(4096), client.CacheLimit())
	})

	t.Run("unlimited when daemon reports no dynamic burst", func(t *testing.T) {
		// Default mock echoes CacheSizeKeepCurrent (-1) -> burst <= 0 -> no limiter.
		sock := startAutoLimitServer(t, &mockCacheManager{})
		client, err := apiclient.NewApiClientAutoLimit(sock)
		require.NoError(t, err)
		defer client.Close()
		assert.Equal(t, int64(0), client.CacheLimit())
	})

	t.Run("connection failure", func(t *testing.T) {
		client, err := apiclient.NewApiClientAutoLimit("/non/existent/socket.sock")
		assert.Error(t, err)
		assert.Nil(t, client)
	})
}

func TestNewAutoCacheLimiter(t *testing.T) {
	t.Run("returns limiter sized to daemon burst", func(t *testing.T) {
		sock := startAutoLimitServer(t, &burstCacheManager{burst: 8192})
		lim, err := apiclient.NewAutoCacheLimiter(sock)
		require.NoError(t, err)
		require.NotNil(t, lim)
		assert.Equal(t, int64(8192), lim.Cap())
	})

	t.Run("nil limiter when daemon reports no dynamic burst", func(t *testing.T) {
		sock := startAutoLimitServer(t, &mockCacheManager{})
		lim, err := apiclient.NewAutoCacheLimiter(sock)
		require.NoError(t, err)
		assert.Nil(t, lim)
	})

	t.Run("connection failure", func(t *testing.T) {
		lim, err := apiclient.NewAutoCacheLimiter("/non/existent/socket.sock")
		assert.Error(t, err)
		assert.Nil(t, lim)
	})
}

// mockCacheManager implements the cacheReader interface for testing. The state,
// snapshot and peerStats fields let tests drive getCacheState(),
// trackerSnapshot() and getPeerLoadStats() with controlled data; their zero
// values keep the default empty behavior.
type mockCacheManager struct {
	state             chunk.CacheState
	snapshot          map[string]map[string]*bitset.BitSet
	localSnapshot     map[string]map[string]*bitset.BitSet
	peerStats         chunk.PeerLoadStats
	cacheSize         int64
	burstCacheSize    int64
	setCacheSizeCalls int
	unlockedTarget    string
	unlockedBlocks    []uint32
}

func (m *mockCacheManager) readFromStore(ctx context.Context, target string, fileSize uint64, blk uint32, chunkSize uint32, forMmap bool) (chunk.ChunkReader, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *mockCacheManager) fillStore(ctx meta.Context, target string, length uint64, blk uint32) error {
	return nil
}

func (m *mockCacheManager) addToStore(ctx meta.Context, target string, length uint64, blk uint32, page *chunk.Page, cache bool) error {
	return fmt.Errorf("not implemented")
}

func (m *mockCacheManager) check(target string, blk uint32) bool {
	return false
}

func (m *mockCacheManager) size() int64 {
	return 0
}

func (m *mockCacheManager) dataReady() {}

func (m *mockCacheManager) removeByPrefix(remove, keep []string) (int64, int64) {
	return 0, 0
}

func (m *mockCacheManager) stop() {}

func (m *mockCacheManager) isHashHit(target string, blk uint32) bool {
	return false
}

func (m *mockCacheManager) clearBurst() (int64, int64) {
	return 0, 0
}

func (m *mockCacheManager) setCacheSize(cacheSize, burstCacheSize int64) (int64, int64, int64, int64) {
	m.setCacheSizeCalls++
	return cacheSize, burstCacheSize, 0, 0
}

func (m *mockCacheManager) getCacheSize() (int64, int64) {
	return m.cacheSize, m.burstCacheSize
}

func (m *mockCacheManager) getCounter(name string) (*metric.AtomicCounter, error) {
	return nil, fmt.Errorf("counter %q not available in mock", name)
}

func (m *mockCacheManager) getCacheState() chunk.CacheState       { return m.state }
func (m *mockCacheManager) getPeerLoadStats() chunk.PeerLoadStats { return m.peerStats }
func (m *mockCacheManager) trackerSnapshot(prefixes []string, local bool) map[string]map[string]*bitset.BitSet {
	snap := m.snapshot
	if local {
		snap = m.localSnapshot
	}
	if snap != nil {
		return snap
	}
	return map[string]map[string]*bitset.BitSet{}
}

func (m *mockCacheManager) setPinCache(prefixes []string) {}
func (m *mockCacheManager) isPinned(target string) bool   { return false }
func (m *mockCacheManager) hasPins() bool                 { return false }

func (m *mockCacheManager) lockTarget(target string, blocks []uint32) {}
func (m *mockCacheManager) unlockTarget(target string, blocks []uint32) {
	m.unlockedTarget = target
	m.unlockedBlocks = blocks
}

func TestGetFdSocketPath(t *testing.T) {
	tests := []struct {
		name     string
		sockPath string
		expected string
	}{
		{
			name:     "simple path",
			sockPath: "/tmp/test.sock",
			expected: "/tmp/test.sock.fd",
		},
		{
			name:     "relative path",
			sockPath: "./test.sock",
			expected: "./test.sock.fd",
		},
		{
			name:     "path with extension",
			sockPath: "/run/cachefs/api.socket",
			expected: "/run/cachefs/api.socket.fd",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getFdSocketPath(tt.sockPath)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetBlockRange(t *testing.T) {
	const blockSize = 4
	largeBlockSize := ^uint32(0)
	tests := []struct {
		name        string
		fileSize    uint64
		blockSize   uint32
		rangeValue  *pb.FileRange
		shareBlocks []shareBlock
		bytes       int64
		wantCode    codes.Code
	}{
		{"whole file", 10, blockSize, nil, []shareBlock{{index: 0, bytes: 4}, {index: 1, bytes: 4}, {index: 2, bytes: 2}}, 10, codes.OK},
		{"first byte", 10, blockSize, &pb.FileRange{Start: 0, Length: 1}, []shareBlock{{index: 0, bytes: 4}}, 4, codes.OK},
		{"crosses block boundary", 10, blockSize, &pb.FileRange{Start: 3, Length: 2}, []shareBlock{{index: 0, bytes: 4}, {index: 1, bytes: 4}}, 8, codes.OK},
		{"aligned block", 10, blockSize, &pb.FileRange{Start: 4, Length: 4}, []shareBlock{{index: 1, bytes: 4}}, 4, codes.OK},
		{"short tail", 10, blockSize, &pb.FileRange{Start: 8, Length: 2}, []shareBlock{{index: 2, bytes: 2}}, 2, codes.OK},
		{"empty file range", 0, blockSize, &pb.FileRange{Start: 8, Length: 1}, nil, 0, codes.OK},
		{"zero length", 10, blockSize, &pb.FileRange{Start: 0, Length: 0}, nil, 0, codes.InvalidArgument},
		{"overflow", 10, blockSize, &pb.FileRange{Start: ^uint64(0), Length: 1}, nil, 0, codes.InvalidArgument},
		{"past eof", 10, blockSize, &pb.FileRange{Start: 9, Length: 2}, nil, 0, codes.InvalidArgument},
		{"unrepresentable bytes", uint64(largeBlockSize) * (uint64(largeBlockSize) + 1), largeBlockSize, nil, nil, 0, codes.InvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			br, err := getBlockRange(tt.fileSize, tt.blockSize, tt.rangeValue)
			assert.Equal(t, tt.wantCode, status.Code(err))
			if tt.wantCode != codes.OK {
				return
			}
			require.NotNil(t, br)
			assert.Equal(t, tt.shareBlocks, br.shareBlocks)
			assert.Equal(t, tt.bytes, br.bytes)
		})
	}
}

func TestFmtFileRange(t *testing.T) {
	// Proto text marshaling drops zero-valued fields; the log rendering must
	// keep a zero start visible instead.
	tests := []struct {
		name       string
		rangeValue *pb.FileRange
		want       string
	}{
		{"nil means whole file", nil, "full file"},
		{"zero range", &pb.FileRange{}, "[0,0)"},
		{"zero start stays visible", &pb.FileRange{Start: 0, Length: 242}, "[0,242)"},
		{"zero start large range", &pb.FileRange{Start: 0, Length: 3221225472}, "[0,3221225472)"},
		{"non-zero start", &pb.FileRange{Start: 4194000, Length: 1000}, "[4194000,4195000)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, fmtFileRange(tt.rangeValue))
		})
	}
}

func TestApiServerGetFileChunks_EmptyFileRange(t *testing.T) {
	const blockSize = 4
	inode := meta.Ino(100)
	file := cachefsmeta.NewInode(inode, nil, "/empty", meta.TypeFile, &meta.Attr{Typ: meta.TypeFile})
	server, err := newApiServer(filepath.Join(t.TempDir(), "test.sock"), &Cachefs{
		Conf: &Config{Store: &chunk.StoreConfig{BlockSize: blockSize}},
		Meta: &mockMeta{inoMap: map[meta.Ino]*cachefsmeta.Inode{inode: file}},
	}, nil)
	require.NoError(t, err)
	defer server.stop()

	tests := []struct {
		name       string
		rangeValue *pb.FileRange
		wantCode   codes.Code
	}{
		{"whole empty file", nil, codes.OK},
		{"nonempty range", &pb.FileRange{Start: 0, Length: 1}, codes.OK},
		{"zero length range", &pb.FileRange{Start: 0, Length: 0}, codes.InvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := server.GetFileChunks(context.Background(), &pb.GetFileChunksRequest{
				Inode: uint64(inode),
				Range: tt.rangeValue,
			})
			assert.Equal(t, tt.wantCode, status.Code(err))
			if tt.wantCode != codes.OK {
				return
			}
			require.NotNil(t, resp)
			assert.Equal(t, uint64(0), resp.GetFileSize())
			assert.Equal(t, uint32(blockSize), resp.GetBlockSize())
			assert.Empty(t, resp.GetChunks())
		})
	}
}

func TestApiServerGetFileChunks_InvalidInode(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")

	fs := &Cachefs{}
	server, err := newApiServer(socketPath, fs, nil)
	require.NoError(t, err)
	defer server.stop()

	ctx := context.Background()

	// Test with special inode (should fail)
	req := &pb.GetFileChunksRequest{
		Inode: uint64(logInode),
	}
	_, err = server.GetFileChunks(ctx, req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot get chunks for special nodes")
}

func TestApiServerGetRequestID(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")

	fs := &Cachefs{}
	server, err := newApiServer(socketPath, fs, nil)
	require.NoError(t, err)
	defer server.stop()

	// Test request ID increment
	id1 := server.getRequestID()
	id2 := server.getRequestID()
	id3 := server.getRequestID()

	assert.Equal(t, uint32(1), id1)
	assert.Equal(t, uint32(2), id2)
	assert.Equal(t, uint32(3), id3)
	assert.True(t, id2 > id1)
	assert.True(t, id3 > id2)
}

func TestApiServerClearCache_EmptyInodes(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")

	fs := &Cachefs{
		Conf: &Config{},
	}
	server, err := newApiServer(socketPath, fs, nil)
	require.NoError(t, err)
	defer server.stop()

	ctx := context.Background()
	req := &pb.ClearCacheRequest{
		Paths: []string{}, // Empty paths list
	}

	resp, err := server.ClearCache(ctx, req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, uint64(0), resp.ClearCount)
	assert.Equal(t, uint64(0), resp.ClearBytes)
}

func TestApiServerLoadCache_EmptyPaths(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")

	fs := &Cachefs{}
	server, err := newApiServer(socketPath, fs, nil)
	require.NoError(t, err)
	defer server.stop()

	ctx := context.Background()
	req := &pb.LoadCacheRequest{
		Paths: []string{}, // Empty paths list
	}

	resp, err := server.LoadCache(ctx, req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, uint64(0), resp.LoadCount)
	assert.Equal(t, uint64(0), resp.LoadBytes)
}

func TestApiServerSourceReady_NotSupported(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")

	fs := &Cachefs{
		Conf: &Config{
			Meta: &cachefsmeta.MetaConfig{},
		},
	}
	server, err := newApiServer(socketPath, fs, nil)
	require.NoError(t, err)
	defer server.stop()

	ctx := context.Background()
	req := &pb.SourceReadyRequest{Background: false}

	// This should fail because we're not in sourceless meta provider mode
	_, err = server.SourceReady(ctx, req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not in sourceless meta provider mode")
}

func TestApiServerClearCache_FuseLayerMode(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")

	fs := &Cachefs{
		Conf: &Config{
			DaemonSocket: "test", // This makes it fuse layer mode
		},
	}
	server, err := newApiServer(socketPath, fs, nil)
	require.NoError(t, err)
	defer server.stop()

	ctx := context.Background()
	req := &pb.ClearCacheRequest{
		Paths: []string{"/"},
	}

	// This should fail because fuse layer mode doesn't support clear cache
	_, err = server.ClearCache(ctx, req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "clear cache not supported in fuse layer mode")
}

func TestApiServerMetricsInitialization(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")

	fs := &Cachefs{}
	server, err := newApiServer(socketPath, fs, nil)
	require.NoError(t, err)
	defer server.stop()

	// Check metrics are initialized
	assert.NotNil(t, server.loadCacheCount)
	assert.NotNil(t, server.clearCacheCount)
	assert.NotNil(t, server.cancelLoadCount)
	assert.NotNil(t, server.sourceReadyCount)
	assert.NotNil(t, server.dataReadyCount)
	assert.NotNil(t, server.getPidCount)
	assert.NotNil(t, server.unmountCount)
	assert.NotNil(t, server.getFileCount)

	// Check initial flight metrics are zero
	assert.Equal(t, int32(0), server.loadCacheFlight)
	assert.Equal(t, int32(0), server.clearCacheFlight)
	assert.Equal(t, int32(0), server.cancelLoadFlight)
	assert.Equal(t, int32(0), server.sourceReadyFlight)
	assert.Equal(t, int32(0), server.dataReadyFlight)
	assert.Equal(t, int32(0), server.getPidFlight)
	assert.Equal(t, int32(0), server.unmountFlight)
	assert.Equal(t, int32(0), server.getFileFlight)

	// Check initial request counter
	assert.Equal(t, uint32(0), server.requestCounter)
}

func TestApiServerLoadCacheCancel(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")

	fs := &Cachefs{}
	server, err := newApiServer(socketPath, fs, nil)
	require.NoError(t, err)
	defer server.stop()

	ctx := context.Background()

	// Test cancel with empty paths
	// When preloadState is nil, it returns EAGAIN error
	req := &pb.LoadCacheRequest{
		Cancel: true,
	}
	_, err = server.LoadCache(ctx, req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "resource temporarily unavailable")
}

func TestApiServerLoadCacheCancelWithBroadcast(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")

	fs := &Cachefs{}
	server, err := newApiServer(socketPath, fs, nil)
	require.NoError(t, err)
	defer server.stop()

	ctx := context.Background()

	// Test cancel with broadcast (should fail since serf is nil)
	req := &pb.LoadCacheRequest{
		Cancel:    true,
		Broadcast: true,
	}
	_, err = server.LoadCache(ctx, req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "serf service not available")
}

// partialMockCachefsStore embeds chunk.Store to obtain zero-value stubs
// for all unimplemented methods. Only GetCacheSize is explicitly overridden.
type partialMockCachefsStore struct {
	chunk.Store
	cacheSize int64
	burstSize int64
}

func (m *partialMockCachefsStore) GetCacheSize() (int64, int64) {
	return m.cacheSize, m.burstSize
}

func TestStoreReader_GetCacheSize(t *testing.T) {
	tests := []struct {
		name      string
		cacheSize int64
		burstSize int64
	}{
		{
			name:      "delegates to store and returns cache and burst size",
			cacheSize: int64(100 << 20),
			burstSize: int64(200 << 20),
		},
		{
			name:      "delegates to store with zero burst",
			cacheSize: int64(512 << 20),
			burstSize: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStore := &partialMockCachefsStore{
				cacheSize: tt.cacheSize,
				burstSize: tt.burstSize,
			}
			sr := &storeReader{store: mockStore}

			gotCache, gotBurst := sr.getCacheSize()

			assert.Equal(t, tt.cacheSize, gotCache,
				"storeReader.getCacheSize should delegate to store.GetCacheSize")
			assert.Equal(t, tt.burstSize, gotBurst,
				"storeReader.getCacheSize should delegate to store.GetCacheSize")
		})
	}
}

func TestDaemonReader_GetCacheSize(t *testing.T) {
	tests := []struct {
		name      string
		cacheSize int64
		burstSize int64
	}{
		{
			name:      "returns initial config values",
			cacheSize: int64(128 << 20),
			burstSize: int64(256 << 20),
		},
		{
			name:      "returns updated atomic config values",
			cacheSize: int64(1024 << 20),
			burstSize: int64(2048 << 20),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storeConfig := &chunk.StoreConfig{}
			// Simulate atomic writes as performed by store.SetCacheSize.
			atomic.StoreInt64(&storeConfig.CacheSize, tt.cacheSize)
			atomic.StoreInt64(&storeConfig.BurstCacheSize, tt.burstSize)

			fs := &Cachefs{
				Conf: &Config{
					Store: storeConfig,
				},
			}
			dr := &daemonReader{fs: fs}

			gotCache, gotBurst := dr.getCacheSize()

			assert.Equal(t, tt.cacheSize, gotCache,
				"daemonReader.getCacheSize should return atomic-loaded Conf.Store.CacheSize")
			assert.Equal(t, tt.burstSize, gotBurst,
				"daemonReader.getCacheSize should return atomic-loaded Conf.Store.BurstCacheSize")
		})
	}
}
