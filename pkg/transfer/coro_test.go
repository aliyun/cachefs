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

package transfer

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveTransportLimit(t *testing.T) {
	const total = 400 * MbtoB
	const single = 180 * MbtoB
	tests := []struct {
		name        string
		totalLimit  int64
		singleLimit int64
		nicCount    int
		want        int64
	}{
		{"single nic uses single-limit", total, single, 1, single},
		{"multi nic uses total-limit", total, single, 4, total},
		{"multi nic only single set is unlimited", 0, single, 4, 0},
		{"single nic only single set", 0, single, 1, single},
		{"single nic no single falls back to total", total, 0, 1, total},
		{"multi nic no single uses total", total, 0, 4, total},
		{"nothing set is unlimited", 0, 0, 1, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &TransferConfig{TotalLimit: tc.totalLimit, SingleLimit: tc.singleLimit}
			assert.Equal(t, tc.want, resolveTransportLimit(cfg, tc.nicCount))
		})
	}
}

func TestNewCoroTransfer(t *testing.T) {
	t.Run("TCPTransport", func(t *testing.T) {
		cfg := &TransferConfig{
			Transport:   TCPTransport,
			TransportIP: "127.0.0.1",
			BasePort:    7800,
			PeerTimeout: time.Second * 30,
		}

		ct, err := newCoroTransfer(cfg, nil)
		assert.NoError(t, err)
		assert.NotNil(t, ct)

		coroSvc := ct.(*coroTransfer)
		assert.NotNil(t, coroSvc.config)
		assert.NotEqual(t, uint32(0), coroSvc.id)
		assert.False(t, coroSvc.rdma)
		assert.NotNil(t, coroSvc.nextReqID)
		assert.NotNil(t, coroSvc.clients)
		assert.NotNil(t, coroSvc.buffers)

		// Start server to enable server-side functionality
		err = coroSvc.StartServer()
		assert.NoError(t, err)
		assert.NotEmpty(t, coroSvc.ServerAddrs())

		// Clean up
		err = coroSvc.StopServer()
		assert.NoError(t, err)
	})

	t.Run("WithRateLimits", func(t *testing.T) {
		cfg := &TransferConfig{
			Transport:     TCPTransport,
			TransportIP:   "127.0.0.1",
			BasePort:      7801,
			PeerTimeout:   time.Second * 30,
			UploadLimit:   1024 * 1024,
			DownloadLimit: 2048 * 1024,
			TotalLimit:    2048 * 1024,
		}

		ct, err := newCoroTransfer(cfg, nil)
		assert.NoError(t, err)
		assert.NotNil(t, ct)

		coroSvc := ct.(*coroTransfer)
		assert.NotNil(t, coroSvc.upLimit)
		assert.NotNil(t, coroSvc.downLimit)
		assert.NotNil(t, coroSvc.totalLimit)

		// Start and stop server
		err = coroSvc.StartServer()
		assert.NoError(t, err)
		err = coroSvc.StopServer()
		assert.NoError(t, err)
	})

	t.Run("WithoutRateLimits", func(t *testing.T) {
		cfg := &TransferConfig{
			Transport:     TCPTransport,
			TransportIP:   "127.0.0.1",
			BasePort:      7802,
			PeerTimeout:   time.Second * 30,
			UploadLimit:   0,
			DownloadLimit: 0,
			TotalLimit:    0,
		}

		ct, err := newCoroTransfer(cfg, nil)
		assert.NoError(t, err)
		assert.NotNil(t, ct)

		coroSvc := ct.(*coroTransfer)
		assert.Nil(t, coroSvc.upLimit)
		assert.Nil(t, coroSvc.downLimit)
		assert.Nil(t, coroSvc.totalLimit)

		// Start and stop server
		err = coroSvc.StartServer()
		assert.NoError(t, err)
		err = coroSvc.StopServer()
		assert.NoError(t, err)
	})

	t.Run("ClientOnlyMode", func(t *testing.T) {
		// Test creating service without starting server (client-only mode)
		cfg := &TransferConfig{
			Transport:   TCPTransport,
			TransportIP: "127.0.0.1",
			BasePort:    7803,
			PeerTimeout: time.Second * 30,
		}

		ct, err := newCoroTransfer(cfg, nil)
		assert.NoError(t, err)
		assert.NotNil(t, ct)

		coroSvc := ct.(*coroTransfer)

		// Without StartServer, ServerAddrs should be empty
		assert.Empty(t, coroSvc.ServerAddrs())

		// But client-side functionality should work
		reqID := coroSvc.getReqID()
		assert.Equal(t, RequestID(1), reqID)

		// Register should still work (buffer management)
		testData := []byte("client only data")
		reqID, err = coroSvc.Register("client-key", testData, nil)
		assert.NoError(t, err)
		assert.NotEqual(t, RequestID(0), reqID)

		// Clean up (StopServer can be called even without StartServer)
		err = coroSvc.StopServer()
		assert.NoError(t, err)
	})
}

func TestCoroTransfer_GetReqID(t *testing.T) {
	ct := &coroTransfer{
		nextReqID: &atomic.Uint32{},
	}

	// Test sequential request ID generation
	reqID1 := ct.getReqID()
	reqID2 := ct.getReqID()
	reqID3 := ct.getReqID()

	assert.Equal(t, RequestID(1), reqID1)
	assert.Equal(t, RequestID(2), reqID2)
	assert.Equal(t, RequestID(3), reqID3)

	// Test concurrent generation
	const numGoroutines = 100
	results := make([]RequestID, numGoroutines)
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			results[idx] = ct.getReqID()
			done <- true
		}(i)
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Check all IDs are unique
	idSet := make(map[RequestID]bool)
	for _, id := range results {
		assert.False(t, idSet[id], "Duplicate request ID found: %d", id)
		idSet[id] = true
	}
}

func TestCoroTransfer_BufferManagement(t *testing.T) {
	t.Run("RegisterAndGetBuffer", func(t *testing.T) {
		ct := &coroTransfer{
			config:    &TransferConfig{},
			nextReqID: &atomic.Uint32{},
			buffers:   make(map[RequestID]*transferBuffer),
		}

		// Register a buffer
		testData := []byte("test buffer data")
		reqID, err := ct.Register("test-key", testData, nil)
		assert.NoError(t, err)
		assert.NotEqual(t, RequestID(0), reqID)

		// Get buffer - should succeed
		buf, err := ct.getBuffer(reqID)
		assert.NoError(t, err)
		assert.NotNil(t, buf)
		assert.Equal(t, "test-key", buf.key)
		assert.Equal(t, testData, buf.data)
		assert.False(t, buf.atime.IsZero())
	})

	t.Run("GetNonExistentBuffer", func(t *testing.T) {
		ct := &coroTransfer{
			buffers: make(map[RequestID]*transferBuffer),
		}

		buf, err := ct.getBuffer(RequestID(999))
		assert.Error(t, err)
		assert.Nil(t, buf)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("UnregisterBuffer", func(t *testing.T) {
		ct := &coroTransfer{
			config:    &TransferConfig{},
			nextReqID: &atomic.Uint32{},
			buffers:   make(map[RequestID]*transferBuffer),
		}

		// Register and unregister
		reqID, _ := ct.Register("test-key", []byte("data"), nil)
		ct.Unregister(reqID)

		// Should not be found after unregister
		buf, err := ct.getBuffer(reqID)
		assert.Error(t, err)
		assert.Nil(t, buf)
	})

	t.Run("UnregisterLocked", func(t *testing.T) {
		ct := &coroTransfer{
			buffers: make(map[RequestID]*transferBuffer),
		}

		reqID := RequestID(1)
		ct.buffers[reqID] = &transferBuffer{key: "test"}

		ct.buffersMu.Lock()
		ct.unregisterLocked(reqID)
		ct.buffersMu.Unlock()

		assert.Empty(t, ct.buffers)
	})
}

func TestCoroTransfer_InitRDMA(t *testing.T) {
	// Mock RDMA device discovery so the test does not need real RDMA hardware.
	// initRDMA only builds bindings; it does not start servers or touch hardware.
	orig := getAllRDMADevs
	getAllRDMADevs = func() []string { return []string{"mlx5_0", "mlx5_1"} }
	defer func() { getAllRDMADevs = orig }()

	cases := []struct {
		name     string
		nics     []string
		wantErr  bool
		wantNics []string
	}{
		{"all specified found", []string{"mlx5_0", "mlx5_1"}, false, []string{"mlx5_0", "mlx5_1"}},
		{"partial found uses subset", []string{"mlx5_0", "nosuchdev"}, false, []string{"mlx5_0"}},
		{"duplicate specified deduped", []string{"mlx5_0", "mlx5_0", "mlx5_1"}, false, []string{"mlx5_0", "mlx5_1"}},
		{"duplicate with missing deduped", []string{"mlx5_1", "nosuchdev", "mlx5_1"}, false, []string{"mlx5_1"}},
		{"none found returns error", []string{"nosuchdev"}, true, nil},
		{"no nics specified uses all", []string{}, false, []string{"mlx5_0", "mlx5_1"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ct := &coroTransfer{
				config: &TransferConfig{
					TransportIP:   "127.0.0.1",
					TransportNics: tc.nics,
				},
			}
			err := ct.initRDMA()
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.wantNics, ct.nics)
			assert.Equal(t, tc.wantNics, ct.clientNics)
		})
	}
}

func TestCoroTransfer_InitTCP(t *testing.T) {
	t.Run("NoNICsSpecified", func(t *testing.T) {
		ct := &coroTransfer{
			config: &TransferConfig{
				TransportIP:   "127.0.0.1",
				TransportNics: []string{},
			},
		}

		err := ct.initTCP()
		assert.NoError(t, err)
		assert.Equal(t, []string{"127.0.0.1"}, ct.nics)
		assert.Nil(t, ct.clientNics, "OS picks the outgoing NIC when none is specified")
	})

	t.Run("WithLoopbackNIC", func(t *testing.T) {
		ct := &coroTransfer{
			config: &TransferConfig{
				TransportIP:   "127.0.0.1",
				TransportNics: []string{"lo"},
			},
		}

		err := ct.initTCP()
		assert.NoError(t, err)
		assert.NotEmpty(t, ct.nics)
		// server and client sides use the same NIC IPs
		assert.Equal(t, ct.nics, ct.clientNics)
	})

	t.Run("InvalidNIC", func(t *testing.T) {
		ct := &coroTransfer{
			config: &TransferConfig{
				TransportIP:   "127.0.0.1",
				TransportNics: []string{"non-existent-nic"},
			},
		}

		err := ct.initTCP()
		// Should return error for non-existent NIC
		assert.Error(t, err)
	})
}

func TestCoroTransfer_ServerIDAndAddrs(t *testing.T) {
	t.Run("ServerID", func(t *testing.T) {
		ct := &coroTransfer{
			id: 12345,
		}

		assert.Equal(t, uint32(12345), ct.ServerID())
	})

	t.Run("ServerAddrsBeforeStart", func(t *testing.T) {
		cfg := &TransferConfig{
			Transport:   TCPTransport,
			TransportIP: "127.0.0.1",
			BasePort:    7820,
			PeerTimeout: time.Second * 30,
		}

		ct, err := newCoroTransfer(cfg, nil)
		assert.NoError(t, err)

		coroSvc := ct.(*coroTransfer)

		// Before StartServer, addresses should be empty
		addrs := coroSvc.ServerAddrs()
		assert.Empty(t, addrs)

		err = coroSvc.StopServer()
		assert.NoError(t, err)
	})

	t.Run("ServerAddrsAfterStart", func(t *testing.T) {
		cfg := &TransferConfig{
			Transport:   TCPTransport,
			TransportIP: "127.0.0.1",
			BasePort:    7821,
			PeerTimeout: time.Second * 30,
		}

		ct, err := newCoroTransfer(cfg, nil)
		assert.NoError(t, err)

		coroSvc := ct.(*coroTransfer)

		// Start server
		err = coroSvc.StartServer()
		assert.NoError(t, err)

		// After StartServer, addresses should not be empty
		addrs := coroSvc.ServerAddrs()
		assert.NotEmpty(t, addrs)
		t.Logf("Server listening on: %v", addrs)

		err = coroSvc.StopServer()
		assert.NoError(t, err)
	})
}

func TestCoroTransfer_AddDelGet(t *testing.T) {
	t.Run("AddAndGetCoroTransfer", func(t *testing.T) {
		cfg := &TransferConfig{
			Transport:   TCPTransport,
			TransportIP: "127.0.0.1",
			BasePort:    7830,
			PeerTimeout: time.Second * 30,
		}

		ct, err := newCoroTransfer(cfg, nil)
		assert.NoError(t, err)

		coroSvc := ct.(*coroTransfer)
		svcID := coroSvc.id

		// Should be able to get the service (newCoroTransfer already adds it)
		gotCt, err := getCoroTransfer(svcID)
		assert.NoError(t, err)
		assert.Equal(t, coroSvc, gotCt)

		// Clean up
		err = coroSvc.StopServer()
		assert.NoError(t, err)

		// Should not be found after StopServer (which calls delCoroTransfer)
		gotCt, err = getCoroTransfer(svcID)
		assert.Error(t, err)
		assert.Nil(t, gotCt)
	})

	t.Run("AddDuplicateID", func(t *testing.T) {
		ct1 := &coroTransfer{id: 101}

		err := addCoroTransfer(101, ct1)
		assert.NoError(t, err)

		// Try to add again with same ID
		ct2 := &coroTransfer{id: 101}
		err = addCoroTransfer(101, ct2)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "has been initialized")

		// Clean up
		delCoroTransfer(101)
	})

	t.Run("GetNonExistent", func(t *testing.T) {
		gotCt, err := getCoroTransfer(999999)
		assert.Error(t, err)
		assert.Nil(t, gotCt)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("DeleteCoroTransfer", func(t *testing.T) {
		ct := &coroTransfer{id: 102}

		err := addCoroTransfer(102, ct)
		assert.NoError(t, err)

		// Delete
		delCoroTransfer(102)

		// Should not be found after deletion
		gotCt, err := getCoroTransfer(102)
		assert.Error(t, err)
		assert.Nil(t, gotCt)
	})
}

func TestCoroTransfer_StopServer(t *testing.T) {
	cfg := &TransferConfig{
		Transport:   TCPTransport,
		TransportIP: "127.0.0.1",
		BasePort:    7850,
		PeerTimeout: time.Second * 30,
	}

	ct, err := newCoroTransfer(cfg, nil)
	assert.NoError(t, err)

	coroSvc := ct.(*coroTransfer)

	// Register some buffers (can be done before StartServer)
	reqID1, err := ct.Register("key1", []byte("data1"), nil)
	assert.NoError(t, err)
	reqID2, err := ct.Register("key2", []byte("data2"), nil)
	assert.NoError(t, err)

	// Verify buffers exist
	coroSvc.buffersMu.Lock()
	initialBufferCount := len(coroSvc.buffers)
	coroSvc.buffersMu.Unlock()
	assert.Equal(t, 2, initialBufferCount)

	// Start server to enable server-side functionality
	err = ct.StartServer()
	assert.NoError(t, err)

	// Verify server is started
	assert.NotEmpty(t, coroSvc.ServerAddrs())
	assert.NotNil(t, coroSvc.rpcServers)

	// Stop server
	err = ct.StopServer()
	assert.NoError(t, err)

	// Verify buffers are cleared
	coroSvc.buffersMu.Lock()
	bufferCount := len(coroSvc.buffers)
	coroSvc.buffersMu.Unlock()
	assert.Equal(t, 0, bufferCount)

	// Verify clients are cleared
	coroSvc.clientMu.Lock()
	clientCount := len(coroSvc.clients)
	coroSvc.clientMu.Unlock()
	assert.Equal(t, 0, clientCount)

	// Verify RPC servers are nil
	assert.Nil(t, coroSvc.rpcServers)

	// Verify service is removed from global map
	_, err = getCoroTransfer(coroSvc.id)
	assert.Error(t, err)

	// These should not be accessible after stop
	_, err = coroSvc.getBuffer(reqID1)
	assert.Error(t, err)
	_, err = coroSvc.getBuffer(reqID2)
	assert.Error(t, err)
}

func TestCoroTransferClient(t *testing.T) {
	t.Run("TouchAndGetAtime", func(t *testing.T) {
		initialTime := time.Now().Add(-time.Hour)
		client := &coroTransferClient{
			atime: initialTime,
		}

		// Get initial atime
		oldAtime := client.getAtime()
		assert.Equal(t, initialTime, oldAtime)

		// Wait a bit and touch
		time.Sleep(10 * time.Millisecond)
		client.touch()

		// New atime should be later
		newAtime := client.getAtime()
		assert.True(t, newAtime.After(oldAtime))
		assert.True(t, newAtime.After(initialTime))
	})

	t.Run("MultipleTouches", func(t *testing.T) {
		client := &coroTransferClient{
			atime: time.Now(),
		}

		var prevTime time.Time
		for i := 0; i < 5; i++ {
			time.Sleep(5 * time.Millisecond)
			client.touch()
			currentTime := client.getAtime()
			if i > 0 {
				assert.True(t, currentTime.After(prevTime) || currentTime.Equal(prevTime))
			}
			prevTime = currentTime
		}
	})
}

func TestCoroTransfer_TotalLimit(t *testing.T) {
	cfg := &TransferConfig{
		Transport:     TCPTransport,
		TransportIP:   "127.0.0.1",
		BasePort:      7903,
		PeerTimeout:   time.Second * 30,
		UploadLimit:   0,
		DownloadLimit: 0,
		TotalLimit:    1 * MbtoB, // 1Mbps
	}

	ct, err := newCoroTransfer(cfg, nil)
	assert.NoError(t, err)
	assert.NotNil(t, ct)

	coroSvc := ct.(*coroTransfer)
	assert.NotNil(t, coroSvc.totalLimit)

	// Consume initial bucket capacity first (bucket starts full with capacity tokens)
	// 1Mbps = 125,000 bytes/s
	coroSvc.totalLimit.Wait(int64(125000))

	// Test that totalLimit bucket waits for tokens after initial capacity is consumed
	start := time.Now()
	want := int64(31250) // 1/4 of 125,000
	coroSvc.totalLimit.Wait(want)
	duration := time.Since(start)

	// Should take approximately 250ms for 31250 bytes at 125,000 bytes/sec (1Mbps)
	// Allow some tolerance (200ms-300ms)
	assert.True(t, duration > 200*time.Millisecond, "Expected duration > 200ms, got %v", duration)
	assert.True(t, duration < 300*time.Millisecond, "Expected duration < 300ms, got %v", duration)

	err = coroSvc.StopServer()
	assert.NoError(t, err)
}

func baseCoroConfig() *TransferConfig {
	return &TransferConfig{
		Transport:    TCPTransport,
		TransportIP:  "127.0.0.1",
		BasePort:     7711,
		PeerTimeout:  30 * time.Second,
		RdmaBufCount: 4,
	}
}

func TestCoroTransfer_EnvConfig(t *testing.T) {
	cases := []struct {
		name            string
		envKey          string
		envVal          string
		wantIdleTimeout int
		wantRdmaMaxBuf  int
	}{
		{
			name:            "default values",
			wantIdleTimeout: defIdleTime,
			wantRdmaMaxBuf:  6, // RdmaBufCount(4) + 2
		},
		{
			name:            "override idle timeout",
			envKey:          "CFS_CORO_RPC_IDLE_TIMEOUT",
			envVal:          "30",
			wantIdleTimeout: 30,
			wantRdmaMaxBuf:  6,
		},
		{
			name:            "invalid idle timeout falls back to default",
			envKey:          "CFS_CORO_RPC_IDLE_TIMEOUT",
			envVal:          "notanumber",
			wantIdleTimeout: defIdleTime,
			wantRdmaMaxBuf:  6,
		},
		{
			name:            "non-positive idle timeout falls back to default",
			envKey:          "CFS_CORO_RPC_IDLE_TIMEOUT",
			envVal:          "0",
			wantIdleTimeout: defIdleTime,
			wantRdmaMaxBuf:  6,
		},
		{
			name:            "negative idle timeout falls back to default",
			envKey:          "CFS_CORO_RPC_IDLE_TIMEOUT",
			envVal:          "-1",
			wantIdleTimeout: defIdleTime,
			wantRdmaMaxBuf:  6,
		},
		{
			name:            "override rdma max buffer",
			envKey:          "RDMA_MAX_BUFFER_COUNT",
			envVal:          "16",
			wantIdleTimeout: defIdleTime,
			wantRdmaMaxBuf:  16,
		},
		{
			name:            "invalid rdma max buffer falls back to default",
			envKey:          "RDMA_MAX_BUFFER_COUNT",
			envVal:          "notanumber",
			wantIdleTimeout: defIdleTime,
			wantRdmaMaxBuf:  6,
		},
		{
			name:            "non-positive rdma max buffer falls back to default",
			envKey:          "RDMA_MAX_BUFFER_COUNT",
			envVal:          "0",
			wantIdleTimeout: defIdleTime,
			wantRdmaMaxBuf:  6,
		},
		{
			name:            "negative rdma max buffer falls back to default",
			envKey:          "RDMA_MAX_BUFFER_COUNT",
			envVal:          "-1",
			wantIdleTimeout: defIdleTime,
			wantRdmaMaxBuf:  6,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.envKey != "" {
				t.Setenv(tc.envKey, tc.envVal)
			}
			svc, err := newCoroTransfer(baseCoroConfig(), nil)
			require.NoError(t, err)
			require.NotNil(t, svc)
			ct := svc.(*coroTransfer)
			assert.Equal(t, tc.wantIdleTimeout, ct.idleTimeoutSec)
			assert.Equal(t, tc.wantRdmaMaxBuf, ct.rdmaMaxBuffer)
			_ = ct.StopServer()
		})
	}
}

func TestCoroTransfer_ReleaseHook(t *testing.T) {
	newTransfer := func() *coroTransfer {
		return &coroTransfer{
			config:    &TransferConfig{Transport: TCPTransport, PeerTimeout: time.Second * 30},
			buffers:   make(map[RequestID]*transferBuffer),
			nextReqID: &atomic.Uint32{},
		}
	}

	t.Run("Unregister runs the hook exactly once", func(t *testing.T) {
		ct := newTransfer()
		released := 0
		reqID, err := ct.Register("key", []byte("data"), func() { released++ })
		assert.NoError(t, err)
		assert.Equal(t, 0, released, "registering must not release anything")

		ct.Unregister(reqID)
		assert.Equal(t, 1, released)

		// A duplicate unregister, e.g. an error path racing the upload
		// completion, must not release the buffer twice.
		ct.Unregister(reqID)
		assert.Equal(t, 1, released)
	})

	t.Run("hook is optional", func(t *testing.T) {
		ct := newTransfer()
		reqID, err := ct.Register("key", []byte("data"), nil)
		assert.NoError(t, err)
		ct.Unregister(reqID)
		_, err = ct.getBuffer(reqID)
		assert.Error(t, err)
	})

	t.Run("claim transfers hook ownership", func(t *testing.T) {
		ct := newTransfer()
		released := 0
		reqID, err := ct.Register("key", []byte("data"), func() { released++ })
		assert.NoError(t, err)

		buf, err := ct.getBuffer(reqID)
		assert.NoError(t, err)
		assert.NotNil(t, buf)
		assert.NotContains(t, ct.buffers, reqID)

		ct.Unregister(reqID)
		assert.Equal(t, 0, released)
		buf.onRelease()
		assert.Equal(t, 1, released)
	})

	t.Run("claim and unregister race", func(t *testing.T) {
		const attempts = 1000
		for i := 0; i < attempts; i++ {
			ct := newTransfer()
			var released atomic.Int32
			reqID, err := ct.Register("key", []byte("data"), func() { released.Add(1) })
			assert.NoError(t, err)

			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				buf, err := ct.getBuffer(reqID)
				if err == nil && buf.onRelease != nil {
					buf.onRelease()
				}
			}()
			go func() {
				defer wg.Done()
				<-start
				ct.Unregister(reqID)
			}()
			close(start)
			wg.Wait()

			assert.Equal(t, int32(1), released.Load())
			_, err = ct.getBuffer(reqID)
			assert.Error(t, err)
		}
	})
}

// TestIsStaleConnError verifies that isStaleConnError matches coro_rpc's stable
// "not connected" message (errc::not_connected) and ignores other IO-error categories.
func TestIsStaleConnError(t *testing.T) {
	tests := []struct {
		name string
		msg  string
		want bool
	}{
		// Should match: coro_rpc errc::not_connected stable message.
		{"not_connected embedded in load error", "load failed: 2, not connected", true},
		{"not_connected standalone", "not connected", true},
		// connection_refused also maps to errc::not_connected on C++ side.
		{"connection_refused mapped to not_connected", "load failed: 2, not connected", true},
		// Should NOT match.
		{"connection refused raw", "load failed: 1, connection refused", false},
		{"io_error broken pipe", "load failed: 5, io_error: broken pipe", false},
		{"server side error", "load failed: 1001, buffer not found", false},
		{"data size mismatch", "data size mismatch: expected 1024, got 512", false},
		{"empty string", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := errors.New(tc.msg)
			assert.Equal(t, tc.want, isStaleConnError(err))
		})
	}
}

// TestDownloadWithRetry covers the retry logic in downloadWithRetry:
//   - success on first try (no retry)
//   - non-stale error is returned immediately without retry
//   - stale ENOTCONN triggers exactly one retry on the same pool
//   - if the retry also fails, the second error is returned
func TestDownloadWithRetry(t *testing.T) {
	const addr = "127.0.0.1:19000"
	// With no client NIC configured, the key produced by getClient is the
	// bare address.
	const clientKey = addr

	// newCT builds a minimal coroTransfer wired for TCP.
	newCT := func() *coroTransfer {
		return &coroTransfer{
			config: &TransferConfig{
				Transport:   TCPTransport,
				TransportIP: "127.0.0.1",
				PeerTimeout: 30 * time.Second,
			},
			clients: make(map[string]*coroTransferClient),
		}
	}

	// fakeClient returns a coroTransferClient whose pool is nil; the test's
	// loadFn ignores the pool value entirely.
	fakeClient := func() *coroTransferClient {
		return &coroTransferClient{pool: nil, atime: time.Now()}
	}

	t.Run("SuccessOnFirstTry", func(t *testing.T) {
		ct := newCT()
		ct.clients[clientKey] = fakeClient()

		calls := 0
		err := ct.downloadWithRetry(addr, 100, func(_ unsafe.Pointer) error {
			calls++
			return nil
		})

		assert.NoError(t, err)
		assert.Equal(t, 1, calls, "loadFn must be called exactly once on success")

		// The client must remain in the map (not invalidated).
		ct.clientMu.Lock()
		_, ok := ct.clients[clientKey]
		ct.clientMu.Unlock()
		assert.True(t, ok)
	})

	t.Run("NonStaleErrorNoRetry", func(t *testing.T) {
		ct := newCT()
		ct.clients[clientKey] = fakeClient()

		nonStaleErr := errors.New("load failed: 5, io_error: broken pipe")
		calls := 0
		err := ct.downloadWithRetry(addr, 100, func(_ unsafe.Pointer) error {
			calls++
			return nonStaleErr
		})

		assert.ErrorIs(t, err, nonStaleErr, "must return the original error")
		assert.Equal(t, 1, calls, "non-stale errors must not trigger retry")

		// The client must NOT have been invalidated.
		ct.clientMu.Lock()
		_, ok := ct.clients[clientKey]
		ct.clientMu.Unlock()
		assert.True(t, ok, "client must remain in map after non-stale error")
	})

	t.Run("StaleErrorThenSuccess", func(t *testing.T) {
		ct := newCT()
		ct.clients[clientKey] = fakeClient()

		calls := 0
		err := ct.downloadWithRetry(addr, 100, func(_ unsafe.Pointer) error {
			calls++
			if calls == 1 {
				return errors.New("load failed: 2, not connected")
			}
			return nil
		})

		assert.NoError(t, err)
		assert.Equal(t, 2, calls, "loadFn must be called twice: once stale, once retry")

		// The same client pool must remain in the map; C++ handles reconnect internally.
		ct.clientMu.Lock()
		_, ok := ct.clients[clientKey]
		ct.clientMu.Unlock()
		assert.True(t, ok, "client must remain in map after retry")
	})

	t.Run("MultipleStaleClientsDrained", func(t *testing.T) {
		// Even if the C++ pool had N stale clients, FlushClientPool drains them
		// all atomically before the retry. So loadFn is called exactly twice:
		// once (stale), flush, once (fresh reconnect).
		// In this unit test the pool is nil so FlushClientPool is a no-op;
		// we verify that the second call succeeds regardless of how many stale
		// errors would have been seen in a real pool.
		ct := newCT()
		ct.clients[clientKey] = fakeClient()

		calls := 0
		err := ct.downloadWithRetry(addr, 100, func(_ unsafe.Pointer) error {
			calls++
			if calls == 1 {
				return errors.New("load failed: 2, not connected")
			}
			return nil
		})

		assert.NoError(t, err)
		// FlushClientPool drains all stale entries atomically; loadFn is called
		// exactly twice (first stale, then fresh reconnect), not N+1 times.
		assert.Equal(t, 2, calls, "loadFn must be called exactly twice: stale + flush + reconnect")
	})

	t.Run("StaleErrorThenAnotherError", func(t *testing.T) {
		ct := newCT()
		ct.clients[clientKey] = fakeClient()

		sentinelErr := errors.New("load failed: 1001, server error on retry")
		calls := 0
		err := ct.downloadWithRetry(addr, 100, func(_ unsafe.Pointer) error {
			calls++
			if calls == 1 {
				return errors.New("load failed: 2, not connected")
			}
			return sentinelErr
		})

		assert.ErrorIs(t, err, sentinelErr, "must return the error from the retry call")
		assert.Equal(t, 2, calls, "must retry exactly once")
	})

}

// TestEvictPeer verifies EvictPeer flushes idle connections matching the given IPs.
func TestEvictPeer(t *testing.T) {
	// newCTWithClients builds a coroTransfer pre-populated with fake clients.
	// Each client has a nil pool (FlushClientPool is a no-op for nil, so the
	// test validates routing/filtering logic without a real C++ pool).
	newCTWithClients := func(entries map[string]string) *coroTransfer {
		ct := &coroTransfer{
			config:  &TransferConfig{Transport: RDMATransport},
			clients: make(map[string]*coroTransferClient),
		}
		for key, addr := range entries {
			ct.clients[key] = &coroTransferClient{pool: nil, addr: addr, atime: time.Now()}
		}
		return ct
	}

	t.Run("EmptyIPList", func(t *testing.T) {
		ct := newCTWithClients(map[string]string{
			"k1": "10.0.0.1:7800",
		})
		n := ct.EvictPeer(nil)
		assert.Equal(t, 0, n, "nil peerIPs must return 0")

		n = ct.EvictPeer([]string{})
		assert.Equal(t, 0, n, "empty peerIPs must return 0")
	})

	t.Run("NilPoolSkipped", func(t *testing.T) {
		// pool == nil → FlushClientPool is skipped; must not crash.
		ct := newCTWithClients(map[string]string{
			"k1": "10.0.0.1:7800",
		})
		assert.NotPanics(t, func() {
			ct.EvictPeer([]string{"10.0.0.1"})
		})
	})

	t.Run("MatchesAllPortsForSameIP", func(t *testing.T) {
		// Clients on different ports of the same IP must all be visited.
		visited := map[string]bool{}
		ct := &coroTransfer{
			config:  &TransferConfig{Transport: RDMATransport},
			clients: make(map[string]*coroTransferClient),
		}
		addrs := []string{
			"172.24.138.21:7798",
			"172.24.138.21:7799",
			"172.24.138.21:7800",
		}
		for i, a := range addrs {
			addr := a
			ct.clients[addr+string(rune('0'+i))] = &coroTransferClient{
				pool:  nil,
				addr:  addr,
				atime: time.Now(),
			}
			visited[addr] = false
		}
		// Verify IP extraction works for all three addrs.
		for _, a := range addrs {
			ip, _, err := net.SplitHostPort(a)
			assert.NoError(t, err)
			assert.Equal(t, "172.24.138.21", ip)
		}
		// EvictPeer with nil pools returns 0 but must not crash.
		n := ct.EvictPeer([]string{"172.24.138.21"})
		assert.Equal(t, 0, n, "nil pools contribute 0 to flush count")
	})

	t.Run("NonMatchingIPsIgnored", func(t *testing.T) {
		ct := newCTWithClients(map[string]string{
			"k1": "10.0.0.1:7800",
			"k2": "10.0.0.2:7800",
			"k3": "10.0.0.3:7800",
		})
		// Evict only 10.0.0.1 – others must be untouched (pool nil → 0 anyway,
		// but the client map must remain intact).
		n := ct.EvictPeer([]string{"10.0.0.1"})
		assert.Equal(t, 0, n)
		// All three clients still present in the map.
		ct.clientMu.Lock()
		assert.Len(t, ct.clients, 3)
		ct.clientMu.Unlock()
	})

	t.Run("MultipleIPsInOneBatch", func(t *testing.T) {
		ct := newCTWithClients(map[string]string{
			"k1": "10.0.0.1:7800",
			"k2": "10.0.0.2:7800",
			"k3": "10.0.0.3:7800",
		})
		// Evict 10.0.0.1 and 10.0.0.2 in one call.
		n := ct.EvictPeer([]string{"10.0.0.1", "10.0.0.2"})
		assert.Equal(t, 0, n, "nil pools: 0 flushed")
		// 10.0.0.3 client still present.
		ct.clientMu.Lock()
		assert.Len(t, ct.clients, 3, "EvictPeer must not remove entries from the map")
		ct.clientMu.Unlock()
	})

	t.Run("InvalidAddrSkipped", func(t *testing.T) {
		// A client with a malformed addr must be silently skipped.
		ct := &coroTransfer{
			config:  &TransferConfig{Transport: RDMATransport},
			clients: make(map[string]*coroTransferClient),
		}
		ct.clients["bad"] = &coroTransferClient{pool: nil, addr: "not-valid", atime: time.Now()}
		ct.clients["good"] = &coroTransferClient{pool: nil, addr: "10.0.0.1:7800", atime: time.Now()}
		assert.NotPanics(t, func() {
			ct.EvictPeer([]string{"10.0.0.1"})
		})
	})
}
