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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransferConfig(t *testing.T) {
	// Default
	defaultCfg := DefaultTransferConfig()

	assert.Equal(t, TCPTransport, defaultCfg.Transport)
	assert.Equal(t, 7712, defaultCfg.BasePort)
	assert.Equal(t, time.Second*30, defaultCfg.PeerTimeout)
	assert.Equal(t, 128<<10, defaultCfg.RdmaBufSize)
	assert.Equal(t, 16, defaultCfg.RdmaBufCount)
	assert.Equal(t, uint64(1<<30), defaultCfg.RdmaMaxMem)

	// RdmaEnabled
	for _, tc := range []struct {
		transport string
		rdma      bool
	}{{RDMATransport, true}, {TCPTransport, false}, {"unknown", false}} {
		assert.Equal(t, tc.rdma, IsRdmaTransport(tc.transport), tc.transport)
		assert.Equal(t, tc.rdma, (&TransferConfig{Transport: tc.transport}).RdmaEnabled(), tc.transport)
	}

	// DeepCopy
	original := &TransferConfig{
		Transport:     RDMATransport,
		TransportNics: []string{"eth0", "eth1"},
		TransportIP:   "192.168.1.1",
		BasePort:      8000,
		UploadLimit:   1000,
		DownloadLimit: 2000,
		PeerTimeout:   time.Second * 10,
		RdmaBufSize:   64 << 10,
		RdmaBufCount:  32,
		RdmaMaxMem:    2 << 30,
	}

	copied := original.DeepCopy()

	// Check that values are equal
	assert.Equal(t, original.Transport, copied.Transport)
	assert.Equal(t, original.TransportIP, copied.TransportIP)
	assert.Equal(t, original.BasePort, copied.BasePort)
	assert.Equal(t, original.UploadLimit, copied.UploadLimit)
	assert.Equal(t, original.DownloadLimit, copied.DownloadLimit)
	assert.Equal(t, original.PeerTimeout, copied.PeerTimeout)
	assert.Equal(t, original.RdmaBufSize, copied.RdmaBufSize)
	assert.Equal(t, original.RdmaBufCount, copied.RdmaBufCount)
	assert.Equal(t, original.RdmaMaxMem, copied.RdmaMaxMem)

	// Check that slices are different instances but have same content
	assert.Equal(t, original.TransportNics, copied.TransportNics)
	assert.NotSame(t, original.TransportNics, copied.TransportNics)

	// Modify original and ensure copy is unaffected
	original.TransportNics[0] = "eth2"
	assert.Equal(t, "eth0", copied.TransportNics[0])
}

func TestNewTransferService(t *testing.T) {
	t.Run("TCPTransport", func(t *testing.T) {
		cfg := &TransferConfig{
			Transport:     TCPTransport,
			TransportIP:   "127.0.0.1",
			BasePort:      7712,
			PeerTimeout:   time.Second * 30,
			UploadLimit:   0,
			DownloadLimit: 0,
		}

		svc, err := NewTransferService(cfg, nil)
		assert.NoError(t, err)
		assert.NotNil(t, svc)
		err = svc.StartServer()
		assert.NoError(t, err)

		// Verify the service implements both server and client interfaces
		assert.Implements(t, (*TransferServer)(nil), svc)
		assert.Implements(t, (*TransferClient)(nil), svc)

		// Clean up
		if svc != nil {
			_ = svc.StopServer()
		}
	})

	t.Run("UnsupportedTransport", func(t *testing.T) {
		cfg := &TransferConfig{
			Transport:   "unsupported",
			TransportIP: "127.0.0.1",
			BasePort:    7714,
		}

		svc, err := NewTransferService(cfg, nil)

		assert.Error(t, err)
		assert.Nil(t, svc)
		assert.Contains(t, err.Error(), "not supported")
	})

	t.Run("RDMATransport", func(t *testing.T) {
		cfg := &TransferConfig{
			Transport:    RDMATransport,
			TransportIP:  "127.0.0.1",
			BasePort:     7715,
			PeerTimeout:  time.Second * 30,
			RdmaBufSize:  128 << 10,
			RdmaBufCount: 16,
			RdmaMaxMem:   1 << 30,
		}

		svc, err := NewTransferService(cfg, nil)

		if err != nil {
			// RDMA not available - expected to fail without fallback
			assert.Nil(t, svc)
			t.Logf("RDMA not available (expected): %v", err)
		} else {
			assert.NotNil(t, svc)
			err := svc.StartServer()
			assert.NoError(t, err)
			_ = svc.StopServer()
		}
	})

	t.Run("WithCustomNICs", func(t *testing.T) {
		cfg := &TransferConfig{
			Transport:     TCPTransport,
			TransportNics: []string{"lo"},
			BasePort:      7718,
			PeerTimeout:   time.Second * 30,
		}

		svc, err := NewTransferService(cfg, nil)
		assert.NoError(t, err)
		err = svc.StartServer()
		assert.NoError(t, err)

		// If service was created successfully, verify and clean up
		if svc != nil {
			assert.NotNil(t, svc)
			addrs := svc.ServerAddrs()
			assert.NotEmpty(t, addrs)
			t.Logf("Server addresses: %v", addrs)
			_ = svc.StopServer()
		}
	})
}

func TestIsValidTransport(t *testing.T) {
	valid := []string{TCPTransport, RDMATransport}
	for _, trans := range valid {
		cfg := &TransferConfig{Transport: trans}
		assert.True(t, cfg.IsValidTransport(), trans)
	}

	invalid := []string{"", "unsupported", "udp", "TCP", "RDMA", "tcp-", "rdma-", "none"}
	for _, trans := range invalid {
		cfg := &TransferConfig{Transport: trans}
		assert.False(t, cfg.IsValidTransport(), trans)
	}
}

func TestTransferConfig_Validate(t *testing.T) {
	cases := []struct {
		name    string
		cfg     TransferConfig
		wantErr string
	}{
		{"plain tcp", TransferConfig{Transport: TCPTransport}, ""},
		{"plain rdma", TransferConfig{Transport: RDMATransport}, ""},
		{"unsupported transport", TransferConfig{Transport: "udp"}, "not supported"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestTransferConfig_DeepCopy(t *testing.T) {
	orig := &TransferConfig{
		Transport:     TCPTransport,
		TransportNics: []string{"eth0", "eth1"},
	}
	cp := orig.DeepCopy()

	// Verify values are equal
	assert.Equal(t, orig.Transport, cp.Transport)
	assert.Equal(t, orig.TransportNics, cp.TransportNics)

	// Verify deep copy: modifying copy does not affect original
	cp.TransportNics[0] = "lo"
	assert.Equal(t, "eth0", orig.TransportNics[0], "modifying copy TransportNics must not affect original")

	// Nil slices: DeepCopy allocates empty (non-nil) slices for them.
	origNil := &TransferConfig{}
	cpNil := origNil.DeepCopy()
	assert.Empty(t, cpNil.TransportNics)
}

func TestSortedNics(t *testing.T) {
	cases := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "empty",
			input:    nil,
			expected: []string{},
		},
		{
			name:     "TCP default single IP",
			input:    []string{"127.0.0.1"},
			expected: []string{"127.0.0.1"},
		},
		{
			name:     "duplicates are deduped",
			input:    []string{"mlx1", "mlx0", "mlx1"},
			expected: []string{"mlx0", "mlx1"},
		},
		{
			name:     "result is sorted regardless of input order",
			input:    []string{"mlx2", "mlx0", "mlx1"},
			expected: []string{"mlx0", "mlx1", "mlx2"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sortedNics(tc.input)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestSupportedTransport(t *testing.T) {
	assert.Equal(t, []string{TCPTransport, RDMATransport}, SupportedTransport())
}
