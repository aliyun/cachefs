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
	"strings"
	"sync"
	"testing"

	"github.com/aliyun/cachefs/pkg/rpcsvc"
	"github.com/aliyun/cachefs/pkg/transfer"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func hasRDMADevices() bool {
	entries, err := os.ReadDir("/sys/class/infiniband")
	if err != nil {
		return false
	}
	return len(entries) > 0
}

// getRDMADevices returns all directory entries excluding "." and ".."
// Returns empty slice if readdir fails.
func getRDMADevices() []string {
	entries, err := os.ReadDir("/sys/class/infiniband")
	if err != nil {
		return []string{}
	}

	result := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		// Skip special directory
		// entries
		if name == "." || name == ".." {
			continue
		}
		result = append(result, name)
	}
	return result
}

func TestNewTransport(t *testing.T) {
	assert := assert.New(t)
	tests := []struct {
		transport      string
		cacherootXprt  string
		nics           []string
		cacherootAddrs []string
		cacherootNics  []string
		result         bool
	}{
		{"TCP", "", []string{"lo"}, []string{}, []string{}, true},
		{"tcp", "", []string{"lo"}, []string{}, []string{}, true},
		{"tcp", "", []string{}, []string{}, []string{}, true},
		{"none", "", []string{"lo"}, []string{}, []string{}, false},
		{"tcp", "", []string{"lo"}, []string{"127.0.0.1:8811"}, []string{}, true},
		{"tcp", "tcp", []string{"lo"}, []string{"127.0.0.1:8811"}, []string{}, true},
		{"none", "tcp", []string{"lo"}, []string{"127.0.0.1:8811"}, []string{}, false},
		{"tcp", "", []string{"lo"}, []string{"127.0.0.1:8811"}, []string{"lo"}, false},
		{"tcp", "tcp", []string{"lo"}, []string{"127.0.0.1:8811"}, []string{"lo"}, true},
		{"rdma", "tcp", []string{}, []string{"127.0.0.1:8811"}, []string{"lo"}, hasRDMADevices()},
		{"rdma", "tcp", getRDMADevices(), []string{"127.0.0.1:8811"}, []string{"lo"}, hasRDMADevices()},
		{"tcp", "rdma", []string{}, []string{"127.0.0.1:8811"}, []string{}, hasRDMADevices()},
		{"tcp", "rdma", []string{}, []string{"127.0.0.1:8811"}, getRDMADevices(), hasRDMADevices()},
		{"tcp", "rdma", []string{"lo"}, []string{"127.0.0.1:8811"}, []string{}, hasRDMADevices()},
		{"tcp", "rdma", []string{"lo"}, []string{"127.0.0.1:8811"}, getRDMADevices(), hasRDMADevices()},
	}

	cfg := &remoteCacheConfig{
		localIP:   "127.0.0.1",
		localPort: 7733,
	}

	for i, tc := range tests {
		name := fmt.Sprintf("test case %d", i)
		cfg.transport = strings.ToLower(tc.transport)
		cfg.cacherootXprt = tc.cacherootXprt
		cfg.transportNics = tc.nics
		cfg.cacherootAddrs = tc.cacherootAddrs
		cfg.cacherootNics = tc.cacherootNics
		cfg.rdmaFallbackTcp = false
		rpc, err := rpcsvc.NewRpcService(&rpcsvc.RpcConfig{Addr: "127.0.0.1:0"})
		assert.NoError(err, "new rpc service")
		store := &store{rpc: rpc}
		trans, err := newTransport(cfg, store, nil)
		if err == nil {
			trans.stop()
		}
		if tc.result {
			assert.NoError(err, name)
		} else {
			assert.Error(err, name)
		}
	}
}

func TestNewTransportRdmaFallback(t *testing.T) {
	assert := assert.New(t)
	rdma := hasRDMADevices()

	tests := []struct {
		name            string
		transport       string
		rdmaFallbackTcp bool
		cacherootAddrs  []string
		cacherootXprt   string
		cacherootNics   []string
		result          bool
		errContains     string
	}{
		// RDMA + fallback on: with RDMA succeeds directly, without RDMA fallback to TCP
		{"RdmaFallbackEnabled", "rdma", true, nil, "", nil, true, ""},
		// RDMA + fallback off: with RDMA succeeds, without RDMA fails
		{"RdmaFallbackDisabled", "rdma", false, nil, "", nil, rdma, ""},
		// TCP + fallback on: always succeeds, no fallback needed
		{"TcpWithFallbackEnabled", "tcp", true, nil, "", nil, true, ""},
		// RDMA + fallback on + cacheroot RDMA: with RDMA all pass,
		// without RDMA transferSvc fallback ok but cacheroot fails
		{"CacherootRdmaNoFallback", "rdma", true,
			[]string{"127.0.0.1:8811"}, "rdma", []string{},
			rdma, "cacheroot"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &remoteCacheConfig{
				localIP:         "127.0.0.1",
				localPort:       7743,
				transport:       tc.transport,
				rdmaFallbackTcp: tc.rdmaFallbackTcp,
				cacherootAddrs:  tc.cacherootAddrs,
				cacherootXprt:   tc.cacherootXprt,
				cacherootNics:   tc.cacherootNics,
			}
			rpc, err := rpcsvc.NewRpcService(&rpcsvc.RpcConfig{Addr: "127.0.0.1:0"})
			assert.NoError(err, "new rpc service")
			store := &store{rpc: rpc}
			trans, err := newTransport(cfg, store, nil)
			if trans != nil {
				trans.stop()
			}
			if tc.result {
				assert.NoError(err, tc.name)
			} else {
				assert.Error(err, tc.name)
				if tc.errContains != "" {
					assert.Contains(err.Error(), tc.errContains, tc.name)
				}
			}
		})
	}
}

type transferDownload struct {
	addr  string
	svcID uint32
	reqID transfer.RequestID
}

// mockTransferService implements transfer.TransferService for testing.
type mockTransferService struct {
	startServerErr error
	addr           string
	serverID       uint32
	stopCalls      int
	downloads      []transferDownload
	registerCalls  int

	// hook of the last unclaimed registration, for control-plane tests to
	// observe Unregister.
	mu               sync.Mutex
	onRelease        func()
	unregisterReqIDs []transfer.RequestID
}

func (m *mockTransferService) StartServer() error { return m.startServerErr }
func (m *mockTransferService) StopServer() error {
	m.stopCalls++
	return nil
}
func (m *mockTransferService) ServerAddrs() []string {
	addr := m.addr
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	return []string{addr}
}
func (m *mockTransferService) SelectServerAddr() (string, error) {
	return m.ServerAddrs()[0], nil
}
func (m *mockTransferService) ServerID() uint32 {
	if m.serverID == 0 {
		return 1
	}
	return m.serverID
}
func (m *mockTransferService) Register(_ string, _ []byte, onRelease func()) (transfer.RequestID, error) {
	m.mu.Lock()
	m.registerCalls++
	m.onRelease = onRelease
	m.mu.Unlock()
	return 1, nil
}
func (m *mockTransferService) Unregister(reqID transfer.RequestID) {
	m.mu.Lock()
	m.unregisterReqIDs = append(m.unregisterReqIDs, reqID)
	onRelease := m.onRelease
	m.onRelease = nil
	m.mu.Unlock()
	if onRelease != nil {
		onRelease()
	}
}
func (m *mockTransferService) Download(_ context.Context, addr string, svcID uint32, reqID transfer.RequestID, _ []byte) error {
	m.mu.Lock()
	m.downloads = append(m.downloads, transferDownload{addr: addr, svcID: svcID, reqID: reqID})
	m.mu.Unlock()
	return nil
}
func (m *mockTransferService) EvictPeer(_ []string) int { return 0 }

func TestNewTransportCreatesDualTCP(t *testing.T) {
	origFactory := newTransferService
	defer func() { newTransferService = origFactory }()
	var configs []*transfer.TransferConfig
	services := []*mockTransferService{{addr: "rdma:7744"}, {addr: "tcp:7746"}}
	newTransferService = func(cfg *transfer.TransferConfig, _ prometheus.Registerer) (transfer.TransferService, error) {
		configs = append(configs, cfg.DeepCopy())
		return services[len(configs)-1], nil
	}
	rpc, err := rpcsvc.NewRpcService(&rpcsvc.RpcConfig{Addr: "127.0.0.1:0"})
	require.NoError(t, err)
	defer rpc.Stop()
	xprt, err := newTransport(&remoteCacheConfig{
		localIP: "127.0.0.1", localPort: 7743, transport: transfer.RDMATransport,
		transportNics: []string{"rdma0", "rdma1"}, rdmaFallbackTcp: true, rdmaGroup: "region-a",
	}, &store{rpc: rpc}, nil)
	require.NoError(t, err)
	trans := xprt.(*transport)
	defer trans.stop()
	require.Len(t, configs, 2)
	assert.Equal(t, transfer.RDMATransport, configs[0].Transport)
	assert.Equal(t, transfer.TCPTransport, configs[1].Transport)
	assert.Empty(t, configs[1].TransportNics)
	assert.Equal(t, configs[0].BasePort+len(configs[0].TransportNics), configs[1].BasePort)
	assert.NotSame(t, trans.service, trans.fallbackService)
}

func TestNewTransportInitialRDMAFallback(t *testing.T) {
	origFactory := newTransferService
	defer func() { newTransferService = origFactory }()
	var configs []*transfer.TransferConfig
	tcp := &mockTransferService{addr: "tcp:7744"}
	newTransferService = func(cfg *transfer.TransferConfig, _ prometheus.Registerer) (transfer.TransferService, error) {
		configs = append(configs, cfg.DeepCopy())
		if len(configs) == 1 {
			return nil, errors.New("rdma initialization failed")
		}
		return tcp, nil
	}
	rpc, err := rpcsvc.NewRpcService(&rpcsvc.RpcConfig{Addr: "127.0.0.1:0"})
	require.NoError(t, err)
	defer rpc.Stop()
	xprt, err := newTransport(&remoteCacheConfig{
		localIP: "127.0.0.1", localPort: 7743, transport: transfer.RDMATransport,
		transportNics: []string{"rdma0"}, rdmaFallbackTcp: true,
	}, &store{rpc: rpc}, nil)
	require.NoError(t, err)
	trans := xprt.(*transport)
	assert.True(t, trans.rdmaDisabled)
	assert.Same(t, trans.service, trans.fallbackService)
	require.Len(t, configs, 2)
	assert.Equal(t, transfer.TCPTransport, configs[1].Transport)
	assert.Empty(t, configs[1].TransportNics)
	require.NoError(t, trans.stop())
	assert.Equal(t, 1, tcp.stopCalls)
}

func TestNewTransportInitialRDMAFallbackCleanup(t *testing.T) {
	origFactory := newTransferService
	defer func() { newTransferService = origFactory }()
	calls := 0
	tcp := &mockTransferService{addr: "tcp:7744"}
	newTransferService = func(_ *transfer.TransferConfig, _ prometheus.Registerer) (transfer.TransferService, error) {
		calls++
		switch calls {
		case 1:
			return nil, errors.New("rdma initialization failed")
		case 2:
			return tcp, nil
		default:
			return nil, errors.New("cacheroot initialization failed")
		}
	}
	rpc, err := rpcsvc.NewRpcService(&rpcsvc.RpcConfig{Addr: "127.0.0.1:0"})
	require.NoError(t, err)
	defer rpc.Stop()
	_, err = newTransport(&remoteCacheConfig{
		localIP: "127.0.0.1", localPort: 7743, transport: transfer.RDMATransport,
		rdmaFallbackTcp: true, cacherootAddrs: []string{"peer:1"}, cacherootXprt: transfer.RDMATransport,
	}, &store{rpc: rpc}, nil)
	require.Error(t, err)
	assert.Equal(t, 1, tcp.stopCalls)
}

func TestUpdateRdmaPeer(t *testing.T) {
	trans := &transport{config: &remoteCacheConfig{}}
	trans.updateRdmaPeer("peer", "region-a")
	assert.True(t, trans.isRdmaPeer("unknown"))

	trans.config.rdmaGroup = "region-a"
	assert.False(t, trans.isRdmaPeer("peer"))
	trans.updateRdmaPeer("peer", "region-a")
	assert.True(t, trans.isRdmaPeer("peer"))
	trans.updateRdmaPeer("peer", "region-b")
	assert.False(t, trans.isRdmaPeer("peer"))
	trans.updateRdmaPeer("peer", "region-a")
	trans.updateRdmaPeer("peer", "")
	assert.False(t, trans.isRdmaPeer("peer"))
	trans.updateRdmaPeer("peer", "Region-A")
	assert.False(t, trans.isRdmaPeer("peer"))
	assert.True(t, trans.useFallback("peer", false))
	trans.updateRdmaPeer("peer", "region-a")
	assert.False(t, trans.useFallback("peer", false))
	trans.updateRdmaPeer("peer", "region-b")
	assert.True(t, trans.useFallback("peer", false))
	assert.False(t, trans.useFallback("peer", true))
	trans.rdmaDisabled = true
	assert.True(t, trans.useFallback("unknown", false))
}

func TestInitTransferService(t *testing.T) {
	assert := assert.New(t)

	origFactory := newTransferService
	defer func() { newTransferService = origFactory }()

	tcfg := &transfer.TransferConfig{
		Transport:   transfer.TCPTransport,
		TransportIP: "127.0.0.1",
		BasePort:    7750,
	}

	// Case 1: NewTransferService fails
	t.Run("NewTransferServiceFail", func(t *testing.T) {
		newTransferService = func(_ *transfer.TransferConfig, _ prometheus.Registerer) (transfer.TransferService, error) {
			return nil, errors.New("create failed")
		}
		svc, err := initTransferService(tcfg, nil, true)
		assert.Nil(svc)
		assert.Error(err)
		assert.Contains(err.Error(), "New")
		assert.Contains(err.Error(), "create failed")
	})

	// Case 2: NewTransferService succeeds, StartServer fails
	t.Run("StartServerFail", func(t *testing.T) {
		service := &mockTransferService{startServerErr: errors.New("bind failed")}
		newTransferService = func(_ *transfer.TransferConfig, _ prometheus.Registerer) (transfer.TransferService, error) {
			return service, nil
		}
		svc, err := initTransferService(tcfg, nil, true)
		assert.Nil(svc)
		assert.Error(err)
		assert.Contains(err.Error(), "Start")
		assert.Contains(err.Error(), "bind failed")
		assert.Equal(1, service.stopCalls)
	})

	// Case 3: NewTransferService succeeds, needStart=false skips StartServer
	t.Run("NeedStartFalse", func(t *testing.T) {
		newTransferService = func(_ *transfer.TransferConfig, _ prometheus.Registerer) (transfer.TransferService, error) {
			return &mockTransferService{startServerErr: errors.New("should not be called")}, nil
		}
		svc, err := initTransferService(tcfg, nil, false)
		assert.NotNil(svc)
		assert.NoError(err)
	})

	// Case 4: All succeed
	t.Run("AllSucceed", func(t *testing.T) {
		newTransferService = func(_ *transfer.TransferConfig, _ prometheus.Registerer) (transfer.TransferService, error) {
			return &mockTransferService{}, nil
		}
		svc, err := initTransferService(tcfg, nil, true)
		assert.NotNil(svc)
		assert.NoError(err)
	})
}

func TestGetMetaDump(t *testing.T) {
	tests := []struct {
		name       string
		dumpResult *metaInfo
		localAddr  string
		blockSize  uint32
		want       *metaInfo
	}{
		{
			name:       "bcache dump returns nil",
			dumpResult: nil,
			localAddr:  "192.168.1.1:8080",
			blockSize:  4194304,
			want:       nil,
		},
		{
			name: "bcache dump returns valid metaInfo",
			dumpResult: &metaInfo{
				Targets: map[string]*targetMeta{
					"/data/file1": {
						Length: 8388608,
						Blocks: nil,
					},
					"/data/file2": {
						Length: 4194304,
						Blocks: nil,
					},
				},
			},
			localAddr: "10.0.0.1:9000",
			blockSize: 4194304,
			want: &metaInfo{
				SrcAddr:   "10.0.0.1:9000",
				BlockSize: 4194304,
				Targets: map[string]*targetMeta{
					"/data/file1": {
						Length: 8388608,
						Blocks: nil,
					},
					"/data/file2": {
						Length: 4194304,
						Blocks: nil,
					},
				},
			},
		},
		{
			name: "bcache dump returns metaInfo with block-level targets",
			dumpResult: &metaInfo{
				Targets: map[string]*targetMeta{
					"/data/file3": {
						Length: 0,
						Blocks: nil, // will be set in test
					},
				},
			},
			localAddr: "172.16.0.1:7000",
			blockSize: 1048576,
			want: &metaInfo{
				SrcAddr:   "172.16.0.1:7000",
				BlockSize: 1048576,
				Targets: map[string]*targetMeta{
					"/data/file3": {
						Length: 0,
						Blocks: nil,
					},
				},
			},
		},
		{
			name: "bcache dump returns empty targets",
			dumpResult: &metaInfo{
				Targets: map[string]*targetMeta{},
			},
			localAddr: "192.168.1.100:6000",
			blockSize: 2097152,
			want: &metaInfo{
				SrcAddr:   "192.168.1.100:6000",
				BlockSize: 2097152,
				Targets:   map[string]*targetMeta{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock bcache
			mockBcache := &MockLocalCache{}
			mockBcache.On("dump").Return(tt.dumpResult)

			// Create transport with mock bcache
			trans := &transport{
				config: &remoteCacheConfig{
					localAddr: tt.localAddr,
					blockSize: tt.blockSize,
				},
				store: &store{
					bcache: mockBcache,
				},
			}

			// Call getMetaDump
			got := trans.getMetaDump()

			// Verify result
			if tt.want == nil {
				assert.Nil(t, got)
			} else {
				assert.NotNil(t, got)
				assert.Equal(t, tt.want.SrcAddr, got.SrcAddr)
				assert.Equal(t, tt.want.BlockSize, got.BlockSize)
				assert.Equal(t, len(tt.want.Targets), len(got.Targets))
				for key, wantTarget := range tt.want.Targets {
					gotTarget, ok := got.Targets[key]
					assert.True(t, ok, "target %s should exist", key)
					assert.Equal(t, wantTarget.Length, gotTarget.Length)
					if wantTarget.Blocks == nil {
						assert.Nil(t, gotTarget.Blocks)
					} else {
						assert.Equal(t, wantTarget.Blocks, gotTarget.Blocks)
					}
				}
			}

			mockBcache.AssertExpectations(t)
		})
	}
}
