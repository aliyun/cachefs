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

package apiclient

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unsafe"

	pb "github.com/aliyun/cachefs/pkg/ttrpc/cachefs"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/containerd/ttrpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// mockSvc is a lightweight mock of pb.CachefsApiService that records the last
// request received and returns configurable responses.
type mockSvc struct {
	lastLoadCacheReq         *pb.LoadCacheRequest
	lastClearCacheReq        *pb.ClearCacheRequest
	lastSetCacheSizeReq      *pb.SetCacheSizeRequest
	lastGetServerInfoReq     *pb.GetServerInfoRequest
	lastGetPeersReq          *pb.GetPeersRequest
	lastUnmountReq           *pb.UnmountRequest
	lastSourceReadyReq       *pb.SourceReadyRequest
	lastGetFileChunksReq     *pb.GetFileChunksRequest
	lastReleaseFileChunksReq *pb.ReleaseFileChunksRequest
	getServerInfoCalls       int
	setCacheSizeCalls        int

	loadCacheResp     *pb.LoadCacheResponse
	clearCacheResp    *pb.ClearCacheResponse
	getServerInfoResp *pb.GetServerInfoResponse
	setCacheSizeResp  *pb.SetCacheSizeResponse
	getPeersResp      *pb.GetPeersResponse
	getPidResp        *pb.GetPidResponse
	sourceReadyResp   *pb.SourceReadyResponse

	getServerInfoErr error
	err              error
}

func (m *mockSvc) LoadCache(_ context.Context, req *pb.LoadCacheRequest) (*pb.LoadCacheResponse, error) {
	m.lastLoadCacheReq = req
	if m.err != nil {
		return nil, m.err
	}
	if m.loadCacheResp != nil {
		return m.loadCacheResp, nil
	}
	return &pb.LoadCacheResponse{}, nil
}

func (m *mockSvc) ClearCache(_ context.Context, req *pb.ClearCacheRequest) (*pb.ClearCacheResponse, error) {
	m.lastClearCacheReq = req
	if m.err != nil {
		return nil, m.err
	}
	if m.clearCacheResp != nil {
		return m.clearCacheResp, nil
	}
	return &pb.ClearCacheResponse{}, nil
}

func (m *mockSvc) SourceReady(_ context.Context, req *pb.SourceReadyRequest) (*pb.SourceReadyResponse, error) {
	m.lastSourceReadyReq = req
	if m.err != nil {
		return nil, m.err
	}
	if m.sourceReadyResp != nil {
		return m.sourceReadyResp, nil
	}
	return &pb.SourceReadyResponse{}, nil
}

func (m *mockSvc) MetaReady(_ context.Context, _ *pb.MetaReadyRequest) (*pb.MetaReadyResponse, error) {
	return &pb.MetaReadyResponse{}, m.err
}

func (m *mockSvc) DataReady(_ context.Context, _ *pb.DataReadyRequest) (*pb.DataReadyResponse, error) {
	return &pb.DataReadyResponse{}, m.err
}

func (m *mockSvc) GetPid(_ context.Context, _ *pb.GetPidRequest) (*pb.GetPidResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.getPidResp != nil {
		return m.getPidResp, nil
	}
	return &pb.GetPidResponse{}, nil
}

func (m *mockSvc) Unmount(_ context.Context, req *pb.UnmountRequest) (*pb.UnmountResponse, error) {
	m.lastUnmountReq = req
	return &pb.UnmountResponse{}, m.err
}

func (m *mockSvc) GetFileChunks(_ context.Context, req *pb.GetFileChunksRequest) (*pb.GetFileChunksResponse, error) {
	m.lastGetFileChunksReq = req
	return &pb.GetFileChunksResponse{}, m.err
}

func (m *mockSvc) ReleaseFileChunks(_ context.Context, req *pb.ReleaseFileChunksRequest) (*pb.ReleaseFileChunksResponse, error) {
	m.lastReleaseFileChunksReq = req
	return &pb.ReleaseFileChunksResponse{}, m.err
}

func (m *mockSvc) GetPeers(_ context.Context, req *pb.GetPeersRequest) (*pb.GetPeersResponse, error) {
	m.lastGetPeersReq = req
	if m.err != nil {
		return nil, m.err
	}
	if m.getPeersResp != nil {
		return m.getPeersResp, nil
	}
	return &pb.GetPeersResponse{}, nil
}

func (m *mockSvc) GetServerInfo(_ context.Context, req *pb.GetServerInfoRequest) (*pb.GetServerInfoResponse, error) {
	m.lastGetServerInfoReq = req
	m.getServerInfoCalls++
	if m.getServerInfoErr != nil {
		return nil, m.getServerInfoErr
	}
	if m.err != nil {
		return nil, m.err
	}
	if m.getServerInfoResp != nil {
		return m.getServerInfoResp, nil
	}
	return &pb.GetServerInfoResponse{}, nil
}

func (m *mockSvc) SetCacheSize(_ context.Context, req *pb.SetCacheSizeRequest) (*pb.SetCacheSizeResponse, error) {
	m.lastSetCacheSizeReq = req
	m.setCacheSizeCalls++
	if m.err != nil {
		return nil, m.err
	}
	if m.setCacheSizeResp != nil {
		return m.setCacheSizeResp, nil
	}
	return &pb.SetCacheSizeResponse{}, nil
}

func (m *mockSvc) GetLoadStats(context.Context, *pb.GetLoadStatsRequest) (*pb.GetLoadStatsResponse, error) {
	return &pb.GetLoadStatsResponse{}, m.err
}

// newTestClient returns an ApiClient wired to the given mock service, bypassing
// the real socket connection.
func newTestClient(svc pb.CachefsApiService) *ApiClient {
	return &ApiClient{
		SockPath:   "/tmp/test.sock",
		FdSockPath: "/tmp/test.sock.fd",
		TtrpcSvc:   svc,
	}
}

func startMockServer(t *testing.T, svc pb.CachefsApiService) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "cachefs-api-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socketPath := filepath.Join(dir, "test.sock")
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)
	server, err := ttrpc.NewServer()
	require.NoError(t, err)
	pb.RegisterCachefsApiService(server, svc)
	go func() {
		_ = server.Serve(context.Background(), listener)
	}()
	t.Cleanup(func() {
		_ = server.Close()
		_ = listener.Close()
	})
	return socketPath
}

func TestNewApiClient(t *testing.T) {
	t.Run("connection failure with non-existent socket", func(t *testing.T) {
		client, err := NewApiClient("/non/existent/socket.sock")
		assert.Error(t, err)
		assert.Nil(t, client)
		assert.Contains(t, err.Error(), "failed to connect to CacheFS API server")
	})
}

func TestCachedServerInfo(t *testing.T) {
	populated := ServerInfo{BlockSize: 4 << 20, CacheSize: 1024, BurstCacheSize: 4096, Pid: 1234, ShareCap: 2048}
	tests := []struct {
		name     string
		snapshot *ServerInfo
		want     ServerInfo
		wantErr  error
	}{
		{
			name:    "nil snapshot",
			wantErr: errServerInfoUnavailable,
		},
		{
			name:     "zero snapshot",
			snapshot: &ServerInfo{},
		},
		{
			name:     "populated snapshot",
			snapshot: &populated,
			want:     populated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &ApiClient{serverInfo: tt.snapshot}
			got, err := client.cachedServerInfo()
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				assert.Equal(t, ServerInfo{}, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestAutoLimitUsesConstructionProbe(t *testing.T) {
	probeFailure := func(t *testing.T) (*mockSvc, string) {
		t.Helper()
		mock := &mockSvc{
			getServerInfoErr: errors.New("GetServerInfo failed"),
		}
		return mock, startMockServer(t, mock)
	}

	t.Run("normal client tolerates probe failure", func(t *testing.T) {
		mock, socketPath := probeFailure(t)
		client, err := NewApiClient(socketPath)
		require.NoError(t, err)
		defer client.Close()
		assert.Equal(t, 1, mock.getServerInfoCalls)
		assert.Zero(t, mock.setCacheSizeCalls)
	})

	t.Run("auto client returns unavailable without retry", func(t *testing.T) {
		mock, socketPath := probeFailure(t)
		client, err := NewApiClientAutoLimit(socketPath)
		assert.ErrorIs(t, err, errServerInfoUnavailable)
		assert.Nil(t, client)
		assert.Equal(t, 1, mock.getServerInfoCalls)
	})

	t.Run("shared limiter returns unavailable without retry", func(t *testing.T) {
		mock, socketPath := probeFailure(t)
		limiter, err := NewAutoCacheLimiter(socketPath)
		assert.ErrorIs(t, err, errServerInfoUnavailable)
		assert.Nil(t, limiter)
		assert.Equal(t, 1, mock.getServerInfoCalls)
	})

	t.Run("zero-capacity snapshot remains valid", func(t *testing.T) {
		mock := &mockSvc{}
		client, err := NewApiClientAutoLimit(startMockServer(t, mock))
		require.NoError(t, err)
		defer client.Close()
		assert.Equal(t, int64(0), client.CacheLimit())
		assert.Equal(t, 1, mock.getServerInfoCalls)
		assert.Zero(t, mock.setCacheSizeCalls)
	})

	t.Run("zero-capacity snapshot returns nil shared limiter", func(t *testing.T) {
		mock := &mockSvc{}
		limiter, err := NewAutoCacheLimiter(startMockServer(t, mock))
		require.NoError(t, err)
		assert.Nil(t, limiter)
		assert.Equal(t, 1, mock.getServerInfoCalls)
		assert.Zero(t, mock.setCacheSizeCalls)
	})
}

func TestClose(t *testing.T) {
	t.Run("nil TtrpcConn is a no-op", func(t *testing.T) {
		c := &ApiClient{TtrpcConn: nil}
		assert.NoError(t, c.Close())
	})

	t.Run("clears TtrpcConn after close", func(t *testing.T) {
		mock := &mockSvc{}
		c := newTestClient(mock)
		// Inject a real (closed) connection to test the nil-clear path.
		// We call Close on a client without a real conn; the nil guard ensures no panic.
		c.TtrpcConn = nil
		assert.NoError(t, c.Close())
		assert.Nil(t, c.TtrpcConn)
	})
}

func TestParseInode(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    uint64
		expectError bool
	}{
		{"valid inode zero", "0", 0, false},
		{"valid inode one", "1", 1, false},
		{"valid large inode", "18446744073709551615", ^uint64(0), false},
		{"empty string", "", 0, true},
		{"negative number", "-1", 0, true},
		{"non-numeric", "abc", 0, true},
		{"overflow", "18446744073709551616", 0, true},
		{"float string", "1.5", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseInode(tt.input)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestLoadCacheOptions(t *testing.T) {
	tests := []struct {
		name     string
		opts     []LoadCacheOption
		expected LoadCacheOptions
	}{
		{
			"no options yields zero value",
			nil,
			LoadCacheOptions{},
		},
		{
			"Background true",
			[]LoadCacheOption{LoadCacheWithBackground(true)},
			LoadCacheOptions{Background: true},
		},
		{
			"Broadcast true",
			[]LoadCacheOption{LoadCacheWithBroadcast(true)},
			LoadCacheOptions{Broadcast: true},
		},
		{
			"LoadAll true",
			[]LoadCacheOption{LoadCacheWithLoadAll(true)},
			LoadCacheOptions{LoadAll: true},
		},
		{
			"Force true",
			[]LoadCacheOption{LoadCacheWithForce(true)},
			LoadCacheOptions{Force: true},
		},
		{
			"Cancel true",
			[]LoadCacheOption{LoadCacheWithCancel(true)},
			LoadCacheOptions{Cancel: true},
		},
		{
			"multiple options combined",
			[]LoadCacheOption{LoadCacheWithBackground(true), LoadCacheWithBroadcast(true), LoadCacheWithForce(true)},
			LoadCacheOptions{Background: true, Broadcast: true, Force: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &LoadCacheOptions{}
			for _, o := range tt.opts {
				o(opts)
			}
			assert.Equal(t, tt.expected, *opts)
		})
	}
}

func TestClearCacheOptions(t *testing.T) {
	tests := []struct {
		name     string
		opts     []ClearCacheOption
		expected ClearCacheOptions
	}{
		{"no options yields zero value", nil, ClearCacheOptions{}},
		{"Broadcast true", []ClearCacheOption{ClearCacheWithBroadcast(true)}, ClearCacheOptions{Broadcast: true}},
		{"ClearBurst true", []ClearCacheOption{ClearCacheWithClearBurst(true)}, ClearCacheOptions{ClearBurst: true}},
		{
			"both options combined",
			[]ClearCacheOption{ClearCacheWithBroadcast(true), ClearCacheWithClearBurst(true)},
			ClearCacheOptions{Broadcast: true, ClearBurst: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &ClearCacheOptions{}
			for _, o := range tt.opts {
				o(opts)
			}
			assert.Equal(t, tt.expected, *opts)
		})
	}
}

func TestSetCacheSizeOptions(t *testing.T) {
	t.Run("no options yields zero value", func(t *testing.T) {
		opts := &SetCacheSizeOptions{}
		assert.Equal(t, SetCacheSizeOptions{}, *opts)
	})
	t.Run("Broadcast true", func(t *testing.T) {
		opts := &SetCacheSizeOptions{}
		SetCacheSizeWithBroadcast(true)(opts)
		assert.True(t, opts.Broadcast)
	})
}

func TestGetPid(t *testing.T) {
	t.Run("returns pid from response", func(t *testing.T) {
		mock := &mockSvc{getPidResp: &pb.GetPidResponse{Pid: 12345}}
		c := newTestClient(mock)
		pid, err := c.GetPid()
		require.NoError(t, err)
		assert.Equal(t, int64(12345), pid)
	})

	t.Run("propagates service error", func(t *testing.T) {
		mock := &mockSvc{err: errors.New("rpc error")}
		c := newTestClient(mock)
		_, err := c.GetPid()
		assert.Error(t, err)
	})
}

func TestGetPeers(t *testing.T) {
	tests := []struct {
		name          string
		raw, port     bool
		mockResp      *pb.GetPeersResponse
		mockErr       error
		expectedPeers []string
		expectError   bool
	}{
		{
			"returns peers list",
			false, false,
			&pb.GetPeersResponse{Peers: []string{"10.0.0.1", "10.0.0.2"}},
			nil,
			[]string{"10.0.0.1", "10.0.0.2"},
			false,
		},
		{
			"raw mode passes raw=true to service",
			true, false,
			&pb.GetPeersResponse{},
			nil,
			nil,
			false,
		},
		{
			"port mode passes port=true to service",
			false, true,
			&pb.GetPeersResponse{Peers: []string{"10.0.0.1:9000"}},
			nil,
			[]string{"10.0.0.1:9000"},
			false,
		},
		{
			"propagates service error",
			false, false,
			nil,
			errors.New("serf unavailable"),
			nil,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockSvc{getPeersResp: tt.mockResp, err: tt.mockErr}
			c := newTestClient(mock)
			peers, err := c.GetPeers(tt.raw, tt.port)
			if tt.expectError {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expectedPeers, peers)
			assert.Equal(t, tt.raw, mock.lastGetPeersReq.Raw)
			assert.Equal(t, tt.port, mock.lastGetPeersReq.Port)
		})
	}
}

func TestUnmount(t *testing.T) {
	t.Run("passes name to service", func(t *testing.T) {
		mock := &mockSvc{}
		c := newTestClient(mock)
		require.NoError(t, c.Unmount("test-client"))
		assert.Equal(t, "test-client", mock.lastUnmountReq.Name)
		assert.NotZero(t, mock.lastUnmountReq.Pid)
	})

	t.Run("propagates service error", func(t *testing.T) {
		mock := &mockSvc{err: errors.New("unmount failed")}
		c := newTestClient(mock)
		assert.Error(t, c.Unmount("test"))
	})
}

func TestLoadCache(t *testing.T) {
	tests := []struct {
		name          string
		paths         []string
		opts          []LoadCacheOption
		mockResp      *pb.LoadCacheResponse
		mockErr       error
		expectedCount uint64
		expectedBytes uint64
		expectError   bool
		checkReq      func(t *testing.T, req *pb.LoadCacheRequest)
	}{
		{
			name:  "background load returns counts",
			paths: []string{"/data"},
			opts:  []LoadCacheOption{LoadCacheWithBackground(true)},
			mockResp: &pb.LoadCacheResponse{
				LoadCount: 5,
				LoadBytes: 1024,
			},
			expectedCount: 5,
			expectedBytes: 1024,
			checkReq: func(t *testing.T, req *pb.LoadCacheRequest) {
				assert.Equal(t, []string{"/data"}, req.Paths)
				assert.True(t, req.Background)
				assert.False(t, req.Broadcast)
			},
		},
		{
			name:  "cancel preload sends empty paths",
			paths: []string{},
			opts:  []LoadCacheOption{LoadCacheWithCancel(true)},
			checkReq: func(t *testing.T, req *pb.LoadCacheRequest) {
				assert.True(t, req.Cancel)
			},
		},
		{
			name:  "broadcast load passes flag",
			paths: []string{"/"},
			opts:  []LoadCacheOption{LoadCacheWithBroadcast(true), LoadCacheWithForce(true)},
			checkReq: func(t *testing.T, req *pb.LoadCacheRequest) {
				assert.True(t, req.Broadcast)
				assert.True(t, req.Force)
			},
		},
		{
			name:        "propagates service error",
			paths:       []string{"/"},
			mockErr:     errors.New("load failed"),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockSvc{loadCacheResp: tt.mockResp, err: tt.mockErr}
			c := newTestClient(mock)
			count, bytes, err := c.LoadCache(tt.paths, tt.opts...)
			if tt.expectError {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expectedCount, count)
			assert.Equal(t, tt.expectedBytes, bytes)
			if tt.checkReq != nil {
				tt.checkReq(t, mock.lastLoadCacheReq)
			}
		})
	}
}

func TestClearCache(t *testing.T) {
	tests := []struct {
		name          string
		paths         []string
		opts          []ClearCacheOption
		mockResp      *pb.ClearCacheResponse
		mockErr       error
		expectedCount uint64
		expectedBytes uint64
		expectError   bool
		checkReq      func(t *testing.T, req *pb.ClearCacheRequest)
	}{
		{
			name:  "clears paths and returns counts",
			paths: []string{"/data"},
			mockResp: &pb.ClearCacheResponse{
				ClearCount: 10,
				ClearBytes: 4096,
			},
			expectedCount: 10,
			expectedBytes: 4096,
			checkReq: func(t *testing.T, req *pb.ClearCacheRequest) {
				assert.Equal(t, []string{"/data"}, req.Paths)
				assert.False(t, req.ClearBurst)
			},
		},
		{
			name:  "clear burst passes flag",
			paths: []string{"/"},
			opts:  []ClearCacheOption{ClearCacheWithClearBurst(true)},
			checkReq: func(t *testing.T, req *pb.ClearCacheRequest) {
				assert.True(t, req.ClearBurst)
			},
		},
		{
			name:  "broadcast clear passes flag",
			paths: []string{"/"},
			opts:  []ClearCacheOption{ClearCacheWithBroadcast(true)},
			checkReq: func(t *testing.T, req *pb.ClearCacheRequest) {
				assert.True(t, req.Broadcast)
			},
		},
		{
			name:        "propagates service error",
			paths:       []string{"/"},
			mockErr:     errors.New("clear failed"),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockSvc{clearCacheResp: tt.mockResp, err: tt.mockErr}
			c := newTestClient(mock)
			count, bytes, err := c.ClearCache(tt.paths, tt.opts...)
			if tt.expectError {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expectedCount, count)
			assert.Equal(t, tt.expectedBytes, bytes)
			if tt.checkReq != nil {
				tt.checkReq(t, mock.lastClearCacheReq)
			}
		})
	}
}

func TestMetaReady(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		c := newTestClient(&mockSvc{})
		assert.NoError(t, c.MetaReady())
	})

	t.Run("propagates service error", func(t *testing.T) {
		c := newTestClient(&mockSvc{err: errors.New("not ready")})
		assert.Error(t, c.MetaReady())
	})
}

func TestDataReady(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		c := newTestClient(&mockSvc{})
		assert.NoError(t, c.DataReady())
	})

	t.Run("propagates service error", func(t *testing.T) {
		c := newTestClient(&mockSvc{err: errors.New("not ready")})
		assert.Error(t, c.DataReady())
	})
}

func TestSourceReady(t *testing.T) {
	tests := []struct {
		name          string
		background    bool
		mockResp      *pb.SourceReadyResponse
		mockErr       error
		expectedCount uint64
		expectedBytes uint64
		expectError   bool
	}{
		{
			name:          "foreground returns load counts",
			background:    false,
			mockResp:      &pb.SourceReadyResponse{LoadCount: 3, LoadBytes: 512},
			expectedCount: 3,
			expectedBytes: 512,
		},
		{
			name:       "background returns zero counts",
			background: true,
			mockResp:   &pb.SourceReadyResponse{LoadCount: 0, LoadBytes: 0},
		},
		{
			name:        "propagates service error",
			mockErr:     errors.New("source not ready"),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockSvc{sourceReadyResp: tt.mockResp, err: tt.mockErr}
			c := newTestClient(mock)
			count, bytes, err := c.SourceReady(tt.background)
			if tt.expectError {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expectedCount, count)
			assert.Equal(t, tt.expectedBytes, bytes)
			assert.Equal(t, tt.background, mock.lastSourceReadyReq.Background)
		})
	}
}

func TestSetCacheSize(t *testing.T) {
	tests := []struct {
		name               string
		cacheSize          int64
		burstCacheSize     int64
		opts               []SetCacheSizeOption
		mockResp           *pb.SetCacheSizeResponse
		mockErr            error
		initialServerInfo  *ServerInfo
		expectedServerInfo *ServerInfo
		expectError        bool
		checkReq           func(t *testing.T, req *pb.SetCacheSizeRequest)
		checkResp          func(t *testing.T, cache, burst, shareCap int64, evictCnt, evictBytes uint64)
	}{
		{
			name:           "keep cache size with sentinel -1",
			cacheSize:      -1,
			burstCacheSize: 0,
			mockResp: &pb.SetCacheSizeResponse{
				CacheSize:      1024 * 1024 * 1024,
				BurstCacheSize: 0,
				EvictCount:     5,
				EvictBytes:     2048,
				ShareCap:       512,
			},
			checkReq: func(t *testing.T, req *pb.SetCacheSizeRequest) {
				assert.Equal(t, int64(-1), req.CacheSize)
				assert.Equal(t, int64(0), req.BurstCacheSize)
				assert.False(t, req.Broadcast)
			},
			checkResp: func(t *testing.T, cache, burst, shareCap int64, evictCnt, evictBytes uint64) {
				assert.Equal(t, int64(1024*1024*1024), cache)
				assert.Equal(t, int64(512), shareCap)
				assert.Equal(t, uint64(5), evictCnt)
				assert.Equal(t, uint64(2048), evictBytes)
			},
		},
		{
			name:           "broadcast flag is forwarded",
			cacheSize:      512,
			burstCacheSize: -1,
			opts:           []SetCacheSizeOption{SetCacheSizeWithBroadcast(true)},
			checkReq: func(t *testing.T, req *pb.SetCacheSizeRequest) {
				assert.True(t, req.Broadcast)
			},
		},
		{
			name:           "creates server info when no snapshot exists",
			cacheSize:      2048,
			burstCacheSize: 4096,
			mockResp:       &pb.SetCacheSizeResponse{CacheSize: 2048, BurstCacheSize: 4096, ShareCap: 2048},
			expectedServerInfo: &ServerInfo{
				CacheSize:      2048,
				BurstCacheSize: 4096,
				ShareCap:       2048,
			},
		},
		{
			name:           "refreshes capacities and preserves block size and PID",
			cacheSize:      2048,
			burstCacheSize: 4096,
			mockResp:       &pb.SetCacheSizeResponse{CacheSize: 2048, BurstCacheSize: 4096, ShareCap: 2048},
			initialServerInfo: &ServerInfo{
				BlockSize:      4 << 20,
				CacheSize:      1024,
				BurstCacheSize: 1024,
				Pid:            1234,
			},
			expectedServerInfo: &ServerInfo{
				BlockSize:      4 << 20,
				CacheSize:      2048,
				BurstCacheSize: 4096,
				Pid:            1234,
				ShareCap:       2048,
			},
		},
		{
			name:           "propagates service error returns -1 sentinel",
			cacheSize:      100,
			burstCacheSize: 200,
			mockErr:        errors.New("set cache failed"),
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockSvc{setCacheSizeResp: tt.mockResp, err: tt.mockErr}
			c := newTestClient(mock)
			c.serverInfo = tt.initialServerInfo
			cache, burst, shareCap, evictCnt, evictBytes, err := c.SetCacheSize(tt.cacheSize, tt.burstCacheSize, tt.opts...)
			if tt.expectError {
				assert.Error(t, err)
				assert.Equal(t, int64(-1), cache)
				assert.Equal(t, int64(-1), burst)
				assert.Equal(t, int64(-1), shareCap)
				return
			}
			require.NoError(t, err)
			if tt.checkReq != nil {
				tt.checkReq(t, mock.lastSetCacheSizeReq)
			}
			if tt.checkResp != nil {
				tt.checkResp(t, cache, burst, shareCap, evictCnt, evictBytes)
			}
			if tt.expectedServerInfo != nil {
				info, err := c.cachedServerInfo()
				require.NoError(t, err)
				assert.Equal(t, *tt.expectedServerInfo, info)
			}
		})
	}
}

// newLimitedClient builds a bare ApiClient wired to a limiter, without any
// network connection, so the byte-based admission control can be exercised in
// isolation.
func newLimitedClient(lim *CacheLimiter) *ApiClient {
	return &ApiClient{limiter: lim, mappings: make(map[uintptr]*mappedRegion)}
}

func TestNewCacheLimiter(t *testing.T) {
	lim := NewCacheLimiter(1024)
	require.NotNil(t, lim)
	assert.Equal(t, int64(1024), lim.Cap())
}

func TestGetLimitBytes(t *testing.T) {
	for _, tt := range []struct {
		name string
		info ServerInfo
		want int64
	}{
		{"uses share cap", ServerInfo{ShareCap: 1024, BurstCacheSize: 4096}, 1024},
		{"share cap without burst", ServerInfo{ShareCap: 1024}, 1024},
		{"zero share cap", ServerInfo{BurstCacheSize: 4096}, 0},
		{"negative share cap", ServerInfo{ShareCap: -1, BurstCacheSize: 4096}, 0},
		{"zero capacities", ServerInfo{}, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, getLimitBytes(tt.info))
		})
	}
}

func TestAutoLimitUsesShareCap(t *testing.T) {
	t.Run("sizes client limiter from daemon share cap", func(t *testing.T) {
		svc := &mockSvc{getServerInfoResp: &pb.GetServerInfoResponse{BurstCacheSize: 4096, ShareCap: 1024}}
		client, err := NewApiClientAutoLimit(startMockServer(t, svc))
		require.NoError(t, err)
		defer client.Close()
		assert.Equal(t, int64(1024), client.CacheLimit())
	})

	t.Run("burst cache size alone installs no limiter", func(t *testing.T) {
		svc := &mockSvc{getServerInfoResp: &pb.GetServerInfoResponse{BurstCacheSize: 4096}}
		client, err := NewApiClientAutoLimit(startMockServer(t, svc))
		require.NoError(t, err)
		defer client.Close()
		assert.Equal(t, int64(0), client.CacheLimit())
	})

	t.Run("sizes shared limiter from daemon share cap", func(t *testing.T) {
		svc := &mockSvc{getServerInfoResp: &pb.GetServerInfoResponse{BurstCacheSize: 8192, ShareCap: 2048}}
		lim, err := NewAutoCacheLimiter(startMockServer(t, svc))
		require.NoError(t, err)
		require.NotNil(t, lim)
		assert.Equal(t, int64(2048), lim.Cap())
	})
}

func TestCacheLimit(t *testing.T) {
	// No limiter -> unlimited, reported as 0.
	c := &ApiClient{mappings: make(map[uintptr]*mappedRegion)}
	assert.Equal(t, int64(0), c.CacheLimit())

	// With a limiter, CacheLimit reports its capacity.
	c.limiter = NewCacheLimiter(2048)
	assert.Equal(t, int64(2048), c.CacheLimit())
}

func TestGetServerInfo(t *testing.T) {
	t.Run("returns GetServerInfo snapshot", func(t *testing.T) {
		mock := &mockSvc{getServerInfoResp: &pb.GetServerInfoResponse{
			BlockSize:      4 << 20,
			CacheSize:      1024,
			BurstCacheSize: 4096,
			Pid:            1234,
			ShareCap:       2048,
		}}
		c := newTestClient(mock)
		info, err := c.GetServerInfo()
		require.NoError(t, err)
		assert.Equal(t, ServerInfo{BlockSize: 4 << 20, CacheSize: 1024, BurstCacheSize: 4096, Pid: 1234, ShareCap: 2048}, info)
		assert.NotNil(t, mock.lastGetServerInfoReq)
		assert.Nil(t, mock.lastSetCacheSizeReq)
	})

	t.Run("propagates error", func(t *testing.T) {
		c := newTestClient(&mockSvc{getServerInfoErr: errors.New("get server info failed")})
		_, err := c.GetServerInfo()
		assert.Error(t, err)
	})
}

func TestAcquireCacheNoLimiter(t *testing.T) {
	// Without a limiter, acquire/release are no-ops and must never block or panic.
	c := &ApiClient{mappings: make(map[uintptr]*mappedRegion)}
	assert.NoError(t, c.acquireCache(context.Background(), 1<<30))
	c.releaseCache(1 << 30)
}

func TestAcquireCacheNonPositive(t *testing.T) {
	lim := NewCacheLimiter(100)
	c := newLimitedClient(lim)

	// A hint <= 0 skips limiting and must not consume any budget.
	assert.NoError(t, c.acquireCache(context.Background(), 0))
	assert.NoError(t, c.acquireCache(context.Background(), -5))
	c.releaseCache(0)
	c.releaseCache(-5)

	// Full capacity is still available afterwards.
	assert.NoError(t, c.acquireCache(context.Background(), 100))
}

func TestAcquireCacheOversizedRejected(t *testing.T) {
	lim := NewCacheLimiter(100)
	c := newLimitedClient(lim)

	// A single request larger than the capacity is rejected up front rather than
	// blocking forever (semaphore.Acquire(n > cap) would never return).
	err := c.acquireCache(context.Background(), 101)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds cache limit")

	// Rejection must not have consumed any budget.
	assert.NoError(t, c.acquireCache(context.Background(), 100))
}

func TestAcquireCacheBlocksWhenExhausted(t *testing.T) {
	lim := NewCacheLimiter(100)
	c := newLimitedClient(lim)
	require.NoError(t, c.acquireCache(context.Background(), 100))

	// Budget exhausted: a further acquire blocks until something is released.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := c.acquireCache(ctx, 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "acquire cache limit")

	// Releasing frees the budget so the next acquire succeeds.
	c.releaseCache(100)
	require.NoError(t, c.acquireCache(context.Background(), 100))
}

func TestAcquireReleaseBalance(t *testing.T) {
	lim := NewCacheLimiter(100)
	c := newLimitedClient(lim)

	// Repeated balanced acquire/release cycles must not leak budget.
	for i := 0; i < 5; i++ {
		require.NoError(t, c.acquireCache(context.Background(), 100))
		c.releaseCache(100)
	}
	require.NoError(t, c.acquireCache(context.Background(), 100))
}

func TestCloseReleasesReservedKeepsSharedLimiter(t *testing.T) {
	lim := NewCacheLimiter(100)
	c1 := newLimitedClient(lim)
	c2 := newLimitedClient(lim) // shares the same limiter

	// c1 reserves 40 bytes for a live mapping (data nil so Close skips munmap).
	require.NoError(t, c1.acquireCache(context.Background(), 40))
	c1.mappings[uintptr(1)] = &mappedRegion{data: nil, reserved: 40}

	// Only 60 bytes remain, so c2 cannot reserve the full capacity yet.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.Error(t, c2.acquireCache(ctx, 100))

	// Closing c1 returns its 40 bytes to the shared limiter and clears mappings,
	// but must not destroy the limiter that c2 still uses.
	require.NoError(t, c1.Close())
	assert.Equal(t, 0, len(c1.mappings))

	// c2 can now reserve the full capacity through the intact shared limiter.
	require.NoError(t, c2.acquireCache(context.Background(), 100))
	assert.Equal(t, int64(100), lim.Cap())
}

// The refund on a non-error return is the easy one to lose: an empty file maps
// nothing, so MmapFileByInode returns successfully and its reservation must go
// back to the limiter anyway.
func TestMmapFileByInodeRefundsEmptyFile(t *testing.T) {
	lim := NewCacheLimiter(100)
	c := newLimitedClient(lim)
	c.TtrpcSvc = &mockSvc{} // GetFileChunks reports FileSize 0

	data, err := c.MmapFileByInode(1, 100)
	require.NoError(t, err)
	assert.Empty(t, data)

	// Nothing is mapped, so the full capacity must be available again.
	require.NoError(t, c.acquireCache(context.Background(), 100))
}

func TestMmapFileByInodeRefundsOnError(t *testing.T) {
	lim := NewCacheLimiter(100)
	c := newLimitedClient(lim)
	c.TtrpcSvc = &mockSvc{err: errors.New("get file chunks failed")}

	_, err := c.MmapFileByInode(1, 100)
	require.Error(t, err)

	require.NoError(t, c.acquireCache(context.Background(), 100))
}

func TestMmapCanonicalAndLegacyOptions(t *testing.T) {
	t.Run("canonical options configure mmap", func(t *testing.T) {
		svc := &mockSvc{}
		c := newTestClient(svc)

		data, err := c.MmapFileByInode(1, 10,
			FileChunkFdWithWaitTimeout(1500*time.Millisecond),
			FileChunkFdWithRange(2, 4))
		require.NoError(t, err)
		assert.Empty(t, data)
		require.NotNil(t, svc.lastGetFileChunksReq)
		assert.Equal(t, uint32(1500), svc.lastGetFileChunksReq.GetWaitTimeoutMs())
		require.NotNil(t, svc.lastGetFileChunksReq.GetRange())
		assert.Equal(t, uint64(2), svc.lastGetFileChunksReq.GetRange().GetStart())
		assert.Equal(t, uint64(4), svc.lastGetFileChunksReq.GetRange().GetLength())
	})

	t.Run("deprecated aliases and helpers remain compatible", func(t *testing.T) {
		legacyOptions := MmapOptions{}
		legacyOpts := []MmapOption{
			MmapWithWaitTimeout(2 * time.Second),
			MmapWithRange(3, 5),
		}
		for _, opt := range legacyOpts {
			opt(&legacyOptions)
		}
		assert.Equal(t, 2*time.Second, legacyOptions.WaitTimeout)
		require.NotNil(t, legacyOptions.Range)
		assert.Equal(t, &FileRange{Start: 3, Length: 5}, legacyOptions.Range)

		svc := &mockSvc{}
		c := newTestClient(svc)
		data, err := c.MmapFileByInode(1, 10, legacyOpts...)
		require.NoError(t, err)
		assert.Empty(t, data)
		require.NotNil(t, svc.lastGetFileChunksReq)
		assert.Equal(t, uint32(2000), svc.lastGetFileChunksReq.GetWaitTimeoutMs())
		require.NotNil(t, svc.lastGetFileChunksReq.GetRange())
		assert.Equal(t, uint64(3), svc.lastGetFileChunksReq.GetRange().GetStart())
		assert.Equal(t, uint64(5), svc.lastGetFileChunksReq.GetRange().GetLength())
	})
}

func TestMmapRangeRequestAndValidation(t *testing.T) {
	svc := &mockSvc{}
	c := newTestClient(svc)

	data, err := c.MmapFileByInode(1, 10, FileChunkFdWithRange(2, 4))
	require.NoError(t, err)
	assert.Empty(t, data)
	require.NotNil(t, svc.lastGetFileChunksReq)
	require.NotNil(t, svc.lastGetFileChunksReq.GetRange())
	assert.Equal(t, uint64(2), svc.lastGetFileChunksReq.GetRange().GetStart())
	assert.Equal(t, uint64(4), svc.lastGetFileChunksReq.GetRange().GetLength())

	_, err = c.MmapFileByInode(1, 10, FileChunkFdWithRange(0, 0))
	require.Error(t, err)
	_, err = c.MmapFileByInode(1, 10, FileChunkFdWithRange(9, 2))
	require.Error(t, err)
	_, err = c.MmapFileByInode(1, -1, FileChunkFdWithRange(^uint64(0), 1))
	require.Error(t, err)
}

func TestShareReservation(t *testing.T) {
	tests := []struct {
		name       string
		sizeHint   int64
		fileRange  *FileRange
		serverInfo *ServerInfo
		want       int64
	}{
		{"full mapping", 10, nil, &ServerInfo{BlockSize: 4}, 10},
		{"selected range", 10, &FileRange{Start: 3, Length: 2}, &ServerInfo{BlockSize: 4}, 8},
		{"short tail", 10, &FileRange{Start: 8, Length: 2}, &ServerInfo{BlockSize: 4}, 2},
		{"unknown block size", 10, &FileRange{Start: 3, Length: 2}, nil, 10},
		{"zero block size", 10, &FileRange{Start: 3, Length: 2}, &ServerInfo{}, 10},
		{"unknown file size", -1, &FileRange{Start: 3, Length: 2}, &ServerInfo{BlockSize: 4}, -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ApiClient{serverInfo: tt.serverInfo}
			got, err := c.shareReservation(tt.sizeHint, tt.fileRange)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMaxAllowedRange(t *testing.T) {
	const (
		mib      = uint64(1) << 20
		fileSize = int64(0x1f41000000)
		shareCap = int64(13856 * mib)
	)
	info := &ServerInfo{BlockSize: uint32(128 * mib), BurstCacheSize: int64(20000 * mib), ShareCap: shareCap}

	tests := []struct {
		name       string
		fileSize   int64
		start      uint64
		length     uint64
		serverInfo *ServerInfo
		want       FileRange
		wantErr    string
	}{
		{"aligned start", fileSize, 0, uint64(shareCap), info, FileRange{0, 13824 * mib}, ""},
		{"next aligned start", fileSize, 13824 * mib, uint64(shareCap), info, FileRange{13824 * mib, 13824 * mib}, ""},
		{"unaligned start", fileSize, 100 * mib, uint64(shareCap), info, FileRange{100 * mib, 13724 * mib}, ""},
		{"fitting range unchanged", fileSize, 13824 * mib, 32 * mib, info, FileRange{13824 * mib, 32 * mib}, ""},
		{"unaligned fitting range unchanged", fileSize, 100 * mib, 13700 * mib, info, FileRange{100 * mib, 13700 * mib}, ""},
		{"clamped to EOF", fileSize, 0x1e60000000, uint64(shareCap), info, FileRange{0x1e60000000, 0xe1000000}, ""},
		{"short EOF block fits", int64(13856 * mib), 0, uint64(shareCap), info, FileRange{0, 13856 * mib}, ""},
		{"no limit", fileSize, 0, 1 << 62, &ServerInfo{BlockSize: uint32(128 * mib)}, FileRange{0, uint64(fileSize)}, ""},
		{"block exceeds limit", fileSize, 0, mib, &ServerInfo{BlockSize: uint32(128 * mib), ShareCap: int64(64 * mib)}, FileRange{}, "exceeds cache limit"},
		{"zero file size", 0, 0, mib, info, FileRange{}, "file size must be positive"},
		{"zero length", fileSize, 0, 0, info, FileRange{}, "length must be positive"},
		{"start at EOF", fileSize, uint64(fileSize), mib, info, FileRange{}, "start 134234505216 must be less than file size 134234505216"},
		{"no server info", fileSize, 0, mib, nil, FileRange{}, errServerInfoUnavailable.Error()},
		{"unknown block size", fileSize, 0, mib, &ServerInfo{ShareCap: shareCap}, FileRange{}, "block size is unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ApiClient{serverInfo: tt.serverInfo}
			got, err := c.MaxAllowedRange(tt.fileSize, tt.start, tt.length)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("sequential whole file", func(t *testing.T) {
		c := &ApiClient{serverInfo: info}
		var ranges []FileRange
		for start := uint64(0); start < uint64(fileSize); {
			r, err := c.MaxAllowedRange(fileSize, start, uint64(shareCap))
			require.NoError(t, err)
			require.Equal(t, start, r.Start)
			span, err := utils.GetRangeBlockBytes(uint64(fileSize), r.Start, r.Length, info.BlockSize)
			require.NoError(t, err)
			require.LessOrEqual(t, span, shareCap)
			ranges = append(ranges, r)
			start += r.Length
		}
		require.Len(t, ranges, 10)
		assert.Equal(t, FileRange{0x1b00000000, 13824 * mib}, ranges[8])
		assert.Equal(t, FileRange{0x1e60000000, 0xe1000000}, ranges[9])
	})
}

func TestRangeLimiterReservation(t *testing.T) {
	t.Run("mmap uses selected span", func(t *testing.T) {
		lim := NewCacheLimiter(8)
		svc := &mockSvc{}
		c := newLimitedClient(lim)
		c.TtrpcSvc = svc
		c.serverInfo = &ServerInfo{BlockSize: 4}

		data, err := c.MmapFileByInode(1, 10, MmapWithRange(3, 2))
		require.NoError(t, err)
		assert.Empty(t, data)
		require.NotNil(t, svc.lastGetFileChunksReq)
		require.NoError(t, c.acquireCache(context.Background(), 8))
	})

	t.Run("mmap falls back to full size", func(t *testing.T) {
		lim := NewCacheLimiter(8)
		svc := &mockSvc{}
		c := newLimitedClient(lim)
		c.TtrpcSvc = svc

		_, err := c.MmapFileByInode(1, 10, MmapWithRange(3, 2))
		require.Error(t, err)
		assert.Nil(t, svc.lastGetFileChunksReq)
	})

	t.Run("mmap refunds selected span on rpc error", func(t *testing.T) {
		lim := NewCacheLimiter(8)
		c := newLimitedClient(lim)
		c.TtrpcSvc = &mockSvc{err: errors.New("get file chunks failed")}
		c.serverInfo = &ServerInfo{BlockSize: 4}

		_, err := c.MmapFileByInode(1, 10, MmapWithRange(3, 2))
		require.Error(t, err)
		require.NoError(t, c.acquireCache(context.Background(), 8))
	})

	t.Run("direct FD uses selected span", func(t *testing.T) {
		svc := &chunkFdsSvc{getErr: errors.New("stop after admission")}
		c := newLimitedClient(NewCacheLimiter(40))
		c.TtrpcSvc = svc
		c.serverInfo = &ServerInfo{BlockSize: 64}

		// File 100 with block size 64: range [70,90) spans only block 1 (36
		// bytes), which fits the cap; the full file would not.
		_, err := c.GetFileChunkFdsByInode(1, 100, FileChunkFdWithRange(70, 20))
		require.ErrorContains(t, err, "stop after admission")
		require.NoError(t, c.acquireCache(context.Background(), 40))
	})

	t.Run("direct FD falls back to full size", func(t *testing.T) {
		svc := &chunkFdsSvc{}
		c := newLimitedClient(NewCacheLimiter(40))
		c.TtrpcSvc = svc

		_, err := c.GetFileChunkFdsByInode(1, 100, FileChunkFdWithRange(70, 20))
		require.ErrorContains(t, err, "exceeds cache limit")
		assert.Nil(t, svc.getRequest())
		require.NoError(t, c.acquireCache(context.Background(), 40))
	})
}

func mmapTestFD(t *testing.T, data string) int {
	t.Helper()
	fd, err := unix.MemfdCreate("cachefs-range-test", 0)
	require.NoError(t, err)
	_, err = unix.Write(fd, []byte(data))
	require.NoError(t, err)
	return fd
}

func TestCloseFDs(t *testing.T) {
	fds := []int{mmapTestFD(t, "first"), -1, mmapTestFD(t, "second")}

	closeFDs(fds)

	for _, fd := range []int{fds[0], fds[2]} {
		_, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		assert.ErrorIs(t, err, unix.EBADF)
	}
}

func TestMmapChunksRangeView(t *testing.T) {
	// MAP_FIXED stitching needs page-aligned chunk sizes; use the host page
	// size so the test runs on 64k-page kernels too.
	blockSize := os.Getpagesize()
	c := &ApiClient{}
	first, second := strings.Repeat("a", blockSize), strings.Repeat("b", blockSize)
	resp := &pb.GetFileChunksResponse{
		FileSize:  uint64(2 * blockSize),
		BlockSize: uint32(blockSize),
		Chunks: []*pb.FileChunk{
			{BlockIndex: 0, Size: uint32(blockSize)},
			{BlockIndex: 1, Size: uint32(blockSize)},
		},
	}
	fds := []int{mmapTestFD(t, first), mmapTestFD(t, second)}

	base, view, err := c.mmapChunks(fds, resp, &FileRange{Start: uint64(blockSize - 1), Length: 5})
	require.NoError(t, err)
	defer func() { require.NoError(t, unix.Munmap(base)) }()
	assert.Equal(t, "abbbb", string(view))
	assert.Len(t, base, 2*blockSize)
	for _, fd := range fds {
		_, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		assert.ErrorIs(t, err, unix.EBADF)
	}
}

func TestMmapChunksRangeViewFromFullResponse(t *testing.T) {
	// MAP_FIXED stitching needs page-aligned chunk sizes; use the host page
	// size so the test runs on 64k-page kernels too.
	blockSize := os.Getpagesize()
	c := &ApiClient{}
	first, second := strings.Repeat("a", blockSize), strings.Repeat("b", blockSize)
	resp := &pb.GetFileChunksResponse{
		FileSize:  uint64(2 * blockSize),
		BlockSize: uint32(blockSize),
		Chunks: []*pb.FileChunk{
			{BlockIndex: 0, Size: uint32(blockSize)},
			{BlockIndex: 1, Size: uint32(blockSize)},
		},
	}
	fds := []int{mmapTestFD(t, first), mmapTestFD(t, second)}

	base, view, err := c.mmapChunks(fds, resp, &FileRange{Start: uint64(blockSize + 1), Length: 5})
	require.NoError(t, err)
	defer func() { require.NoError(t, unix.Munmap(base)) }()
	assert.Equal(t, "bbbbb", string(view))
	assert.Len(t, base, blockSize)
	for _, fd := range fds {
		_, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		assert.ErrorIs(t, err, unix.EBADF)
	}
}

// A munmap failure racing Close must not re-insert the entry into the drained
// ledger: nobody would release it ever again. The connection and the limiter
// budget get their remaining exits instead.
func TestMunmapFileFailureAfterCloseReleasesResources(t *testing.T) {
	lim := NewCacheLimiter(64)
	c := newLimitedClient(lim)
	require.NoError(t, c.acquireCache(context.Background(), 64))
	require.NoError(t, c.Close())

	// munmap requires a page-aligned address; two candidates one byte apart
	// cannot both be aligned, so one of them fails with EINVAL for sure.
	raw := make([]byte, 8192)
	data := raw[1:65]
	if uintptr(unsafe.Pointer(&data[0]))%4096 == 0 {
		data = raw[2:66]
	}
	left, right := net.Pipe()
	defer func() { _ = right.Close() }()
	dataPtr := uintptr(unsafe.Pointer(&data[0]))
	c.mappings[dataPtr] = &mappedRegion{data: data, base: data, reserved: 64, conn: left}

	err := c.MunmapFile(data)
	require.Error(t, err)

	c.mappingMu.Lock()
	_, tracked := c.mappings[dataPtr]
	c.mappingMu.Unlock()
	assert.False(t, tracked, "entry must not be re-inserted after Close")

	// The connection got its remaining cleanup exit: the peer sees EOF.
	_ = right.SetReadDeadline(time.Now().Add(time.Second))
	_, err = right.Read(make([]byte, 1))
	assert.ErrorIs(t, err, io.EOF)

	// The reserved budget went back to the (shared) limiter.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	assert.NoError(t, c.acquireCache(ctx, 64))
}

// A mapping registered after Close would be orphaned in the swapped ledger, so
// MmapFileByInode tears the fresh mapping down instead of tracking it.
func TestMmapFileAfterCloseIsRejected(t *testing.T) {
	const blockSize = 4096 // single chunk: MAP_FIXED lands at base+0 on any page size
	requestID := uint32(77)
	socket, transferDone := startChunkFdTransferServer(t, []int{mmapTestFD(t, strings.Repeat("x", blockSize))})
	svc := &chunkFdsSvc{getResp: &pb.GetFileChunksResponse{
		FileSize:   blockSize,
		BlockSize:  blockSize,
		Chunks:     []*pb.FileChunk{{BlockIndex: 0, Size: blockSize}},
		RequestId:  requestID,
		FdRespSize: fdResponseSize(t, requestID),
	}}
	c := newLimitedClient(NewCacheLimiter(blockSize))
	c.TtrpcSvc = svc
	c.FdSockPath = socket
	require.NoError(t, c.Close())

	_, err := c.MmapFileByInode(1, blockSize)
	require.ErrorContains(t, err, "client is closed")

	c.mappingMu.Lock()
	tracked := len(c.mappings)
	c.mappingMu.Unlock()
	assert.Zero(t, tracked)
	// Daemon-side cleanup used the connection EOF fallback, not the RPC.
	assert.Equal(t, 0, svc.releaseCallCount())
	require.NoError(t, <-transferDone)
	// The reservation went back to the limiter.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, c.acquireCache(ctx, blockSize))
}
