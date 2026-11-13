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
	"sync"
	"testing"
	"time"

	"github.com/aliyun/cachefs/pkg/metric"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/willf/bitset"
)

func TestAnnounce(t *testing.T) {
	r := &peerCache{
		config:          &remoteCacheConfig{},
		pendingAnnounce: make(map[string]*pendingTargetMeta),
	}
	target := "test-file"
	nrblk := uint32(10)

	r.announce(target, 0, nrblk)

	ptm, exists := r.pendingAnnounce[target]
	if !exists {
		t.Errorf("Expected to have pending file info for target '%s'", target)
		return
	}

	if !ptm.target.Blocks.Test(uint(nrblk)) {
		t.Errorf("Expected block number '%d' to be set in the bitset", nrblk)
	}
}

func TestRevoke(t *testing.T) {
	r := &peerCache{
		config:        &remoteCacheConfig{},
		pendingRevoke: make(map[string]*pendingTargetMeta),
	}
	target := "test-file"
	nrblk := uint32(10)

	r.revoke(target, 0, nrblk)

	ptm, exists := r.pendingRevoke[target]
	if !exists {
		t.Errorf("Expected to have pending file info for revoke in target '%s'", target)
		return
	}

	if !ptm.target.Blocks.Test(uint(nrblk)) {
		t.Errorf("Expected block number '%d' to be set in the bitset", nrblk)
	}
}

func TestConfigChecksumEnabled(t *testing.T) {
	tests := []struct {
		hasher  string
		enabled bool
	}{
		{"crc32", true},
		{"", false},
	}

	config := &remoteCacheConfig{}
	for _, tc := range tests {
		config.hasher = tc.hasher
		assert.Equal(t, tc.enabled, config.checksumEnabled())
	}
}

func createMetaInfo() *metaInfo {
	return &metaInfo{
		SrcAddr:   "127.0.0.1",
		BlockSize: 4096,
		Targets: map[string]*targetMeta{
			"file1": {
				Length: 12345,
				Blocks: bitset.New(3),
			},
		},
	}
}

// Only run through
func TestServeAnnounce(t *testing.T) {
	cache := &peerCache{
		config:        &remoteCacheConfig{},
		announceCh:    make(chan *metaInfo),
		stopCh:        make(chan struct{}),
		metaMap:       make(map[string]map[uint32]map[string]struct{}),
		announceReady: make(chan struct{}),
	}

	go cache.serveAnnounce()

	meta := createMetaInfo()
	cache.announceCh <- meta

	time.Sleep(100 * time.Millisecond)

	cache.metaMu.Lock()
	defer cache.metaMu.Unlock()
	_, exists := cache.metaMap["file1"]
	assert.False(t, exists)
}

// Only run through
func TestServeRevoke(t *testing.T) {
	cache := &peerCache{
		config:   &remoteCacheConfig{},
		revokeCh: make(chan *metaInfo),
		stopCh:   make(chan struct{}),
		metaMap:  make(map[string]map[uint32]map[string]struct{}),
	}

	go cache.serveRevoke()

	meta := createMetaInfo()
	cache.revokeCh <- meta
	time.Sleep(100 * time.Millisecond)
}

type peerLoadTransport struct {
	err       error
	rdmaPeers map[string]struct{}
	loads     int
}

func (t *peerLoadTransport) load(context.Context, string, string, uint32, uint32, *Page, string, bool) error {
	t.loads++
	return t.err
}
func (t *peerLoadTransport) updateRdmaPeer(string, string) {}
func (t *peerLoadTransport) isRdmaPeer(addr string) bool {
	_, ok := t.rdmaPeers[addr]
	return ok
}
func (t *peerLoadTransport) exchangeTracker(context.Context, string) error { return nil }
func (t *peerLoadTransport) stop() error                                   { return nil }

func TestFindRdmaCandidates(t *testing.T) {
	for _, tc := range []struct {
		name           string
		same, other, n int
	}{
		{"no same-group peers", 0, 20, 10},
		{"fewer same-group peers than limit", 3, 20, 10},
		{"same-group peers equal limit", 10, 20, 10},
		{"same-group peers exceed limit", 30, 20, 10},
		{"total peers below limit", 2, 3, 10},
		{"custom limit", 3, 20, 2},
		{"unlimited with zero limit", 3, 20, 0},
		{"unlimited with negative limit", 3, 20, -1},
		{"only local peer", 0, 0, 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			groups := map[string]string{"local:1": "region-a"}
			rdmaPeers := map[string]struct{}{"local:1": {}}
			hosts := map[string]struct{}{"local:1": {}}
			for i := 0; i < tc.same+tc.other; i++ {
				addr := fmt.Sprintf("peer-%d:1", i)
				hosts[addr] = struct{}{}
				if i < tc.same {
					groups[addr] = "region-a"
					rdmaPeers[addr] = struct{}{}
				} else if i%2 == 0 {
					groups[addr] = "region-b"
				}
			}
			r := &peerCache{
				config:    &remoteCacheConfig{localAddr: "local:1", transport: "rdma", rdmaGroup: "region-a"},
				transport: &peerLoadTransport{rdmaPeers: rdmaPeers},
				metaMap:   map[string]map[uint32]map[string]struct{}{"target": {0: hosts}},
			}
			wantSame := tc.same
			wantTotal := tc.same + tc.other
			if tc.n > 0 {
				wantSame = min(wantSame, tc.n)
				wantTotal = min(wantTotal, tc.n)
			}
			for attempt := 0; attempt < 20; attempt++ {
				got := r.findRdmaCandidates("target", 0, tc.n)
				assert.Len(t, got, wantTotal)
				seen := map[string]bool{}
				for i, addr := range got {
					assert.False(t, seen[addr])
					seen[addr] = true
					assert.Equal(t, i < wantSame, groups[addr] == "region-a")
					assert.NotEqual(t, "local:1", addr)
				}
			}
			assert.Empty(t, r.findRdmaCandidates("missing", 0, tc.n))
			assert.Empty(t, r.findRdmaCandidates("target", 1, tc.n))
		})
	}

	t.Run("blacklist checked at load", func(t *testing.T) {
		groups := map[string]string{"same-0:1": "region-a", "same-1:1": "region-a"}
		hosts := map[string]struct{}{"same-0:1": {}, "same-1:1": {}, "other:1": {}}
		blacklists := map[string]time.Time{"same-0:1": time.Now(), "same-1:1": time.Now()}
		transport := &peerLoadTransport{err: errors.New("miss"), rdmaPeers: map[string]struct{}{"same-0:1": {}, "same-1:1": {}}}
		r := &peerCache{
			config:    &remoteCacheConfig{transport: "rdma", rdmaGroup: "region-a", blacklistTime: time.Minute},
			transport: transport, store: &store{remoteBlacklist: metric.NewCounter(prometheus.CounterOpts{Name: "blacklist"})},
			metaMap: map[string]map[uint32]map[string]struct{}{"target": {0: hosts}}, blacklists: blacklists,
		}
		got := r.loadCandidates("target", 0)
		if !assert.Len(t, got, 3) {
			return
		}
		for i, addr := range got[:2] {
			assert.Equal(t, "region-a", groups[addr])
			assert.Contains(t, blacklists, addr, i)
		}
		assert.Equal(t, "other:1", got[2])
		_, err := r.loadFromPeer(context.Background(), "target", 0, 1, got, "", false)
		assert.ErrorIs(t, err, ErrNotFound)
		assert.Equal(t, 1, transport.loads)
		assert.Equal(t, float64(2), r.store.remoteBlacklist.Load())
	})
}

func TestFindCandidates(t *testing.T) {
	metaMap := map[string]map[uint32]map[string]struct{}{
		"target1": {
			1: {
				"addr1":     {},
				"addr2":     {},
				"localhost": {},
			},
		},
	}

	cfg := &remoteCacheConfig{
		localAddr: "localhost",
	}
	peerCacheInstance := &peerCache{
		metaMap: metaMap,
		config:  cfg,
	}

	t.Run("TestWithValidData", func(t *testing.T) {
		expected := []string{"addr1", "addr2"}
		result := peerCacheInstance.findCandidates("target1", 1, 2, true)
		if len(result) != len(expected) {
			t.Errorf("expected length %d, got %d", len(expected), len(result))
			return
		}

		for _, addr := range result {
			found := false
			for _, expectedAddr := range expected {
				if addr == expectedAddr {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("unexpected candidate %s", addr)
			}
		}
	})

	t.Run("TestWithNoCandidates1", func(t *testing.T) {
		result := peerCacheInstance.findCandidates("target2", 1, 2, true)
		if len(result) != 0 {
			t.Errorf("expected empty result, got %v", result)
		}
	})

	t.Run("TestWithNoCandidates2", func(t *testing.T) {
		result := peerCacheInstance.findCandidates("target1", 2, 2, true)
		if len(result) != 0 {
			t.Errorf("expected empty result, got %v", result)
		}
	})

	t.Run("TestWithExcludedLocalAddr", func(t *testing.T) {
		result := peerCacheInstance.findCandidates("target1", 1, 3, true)
		if len(result) != 2 {
			t.Errorf("expected 2 candidates, got %d", len(result))
		}
		for _, addr := range result {
			if addr == peerCacheInstance.config.localAddr {
				t.Errorf("local address should be excluded, but got %s", addr)
			}
		}
	})

	t.Run("TestWithIncludeLocalAddr", func(t *testing.T) {
		result := peerCacheInstance.findCandidates("target1", 1, 3, false)
		if len(result) != 3 {
			t.Errorf("expected 3 candidates, got %d", len(result))
		}
		found := false
		for _, addr := range result {
			if addr == peerCacheInstance.config.localAddr {
				found = true
			}
		}
		if !found {
			t.Error("local address should not be excluded")
		}
	})

	t.Run("TestWithUnlimited1", func(t *testing.T) {
		result := peerCacheInstance.findCandidates("target1", 1, 0, true)
		if len(result) != 2 {
			t.Errorf("expected 2 candidates, got %d", len(result))
		}
	})

	t.Run("TestWithUnlimited2", func(t *testing.T) {
		result := peerCacheInstance.findCandidates("target1", 1, 0, false)
		if len(result) != 3 {
			t.Errorf("expected 3 candidates, got %d", len(result))
		}
	})

	t.Run("GroupPolicyDoesNotAffectLookup", func(t *testing.T) {
		r := &peerCache{
			config:  &remoteCacheConfig{transport: "rdma", rdmaGroup: "region-a"},
			metaMap: map[string]map[uint32]map[string]struct{}{"target": {0: {"a": {}, "b": {}, "c": {}}}},
		}
		assert.Len(t, r.findCandidates("target", 0, 2, true), 2)
		assert.ElementsMatch(t, []string{"a", "b", "c"}, r.findCandidates("target", 0, 0, true))
	})
}

func TestPeerBlacklist(t *testing.T) {
	addr1 := "localhost:7711"
	addr2 := "localhost:7712"
	addr3 := "localhost:7713"
	timeout := time.Second

	config := &remoteCacheConfig{
		blacklistTime: timeout,
	}
	pcache := &peerCache{
		config:     config,
		blacklists: make(map[string]time.Time),
	}

	// Add peer addr to blacklist, and they should be in blacklist
	pcache.addBlacklist(addr1)
	assert.Equal(t, true, pcache.inBlacklist(addr1))
	pcache.addBlacklist(addr2)
	assert.Equal(t, true, pcache.inBlacklist(addr2))

	// Sleep 1s to let peers in blacklist expire
	time.Sleep(timeout)

	// Peers expired, but not deleted yet
	assert.Equal(t, 2, len(pcache.blacklists))
	assert.Equal(t, false, pcache.inBlacklist(addr1))

	// A inBlacklist check would delete this expired addr
	assert.Equal(t, 1, len(pcache.blacklists))

	// GC blacklist, nothing should be in blacklist, except the newly added addr
	go pcache.gcBlacklist()
	time.Sleep(50 * time.Microsecond)
	pcache.addBlacklist(addr3)
	assert.Equal(t, 1, len(pcache.blacklists))
	assert.Equal(t, false, pcache.inBlacklist(addr2))
	assert.Equal(t, true, pcache.inBlacklist(addr3))

	// Add addr3 again after expired, it should be still in blacklist
	time.Sleep(timeout)
	assert.Equal(t, 1, len(pcache.blacklists))
	pcache.addBlacklist(addr3)
	assert.Equal(t, 1, len(pcache.blacklists))
	assert.Equal(t, true, pcache.inBlacklist(addr3))

	// blacklistTime is 0, disable blacklist
	config = &remoteCacheConfig{
		blacklistTime: 0,
	}
	pcache = &peerCache{
		config:     config,
		blacklists: make(map[string]time.Time),
	}
	pcache.addBlacklist(addr1)
	assert.Equal(t, false, pcache.inBlacklist(addr1))
	pcache.addBlacklist(addr2)
	assert.Equal(t, false, pcache.inBlacklist(addr2))
}

func TestCountBlacklistSkip(t *testing.T) {
	oldInterval := blacklistSkipLogInterval
	blacklistSkipLogInterval = 50 * time.Millisecond
	defer func() { blacklistSkipLogInterval = oldInterval }()

	// skipCounts and lastSkipLog are intentionally left zero to cover lazy init
	pcache := &peerCache{
		config:     &remoteCacheConfig{blacklistTime: time.Minute},
		blacklists: make(map[string]time.Time),
	}

	// counts accumulate within the interval, no flush yet
	pcache.countBlacklistSkip("addr1")
	pcache.countBlacklistSkip("addr1")
	pcache.countBlacklistSkip("addr2")
	assert.Equal(t, int64(2), pcache.skipCounts["addr1"])
	assert.Equal(t, int64(1), pcache.skipCounts["addr2"])

	// after the interval elapsed, the next skip is counted into the current
	// window and flushed together, so counts reset to empty for the new window
	time.Sleep(100 * time.Millisecond)
	pcache.countBlacklistSkip("addr1")
	assert.Equal(t, 0, len(pcache.skipCounts))
}

func TestRemoteCacheConfigDeepCopy(t *testing.T) {
	// Create original config with non-zero values
	original := &remoteCacheConfig{
		blockSize:      4096,
		localIP:        "192.168.1.100",
		localPort:      9000,
		transport:      "tcp",
		transportNics:  []string{"eth0", "eth1"},
		cacherootAddrs: []string{"192.168.1.101:9000", "192.168.1.102:9000"},
		cacherootNics:  []string{"eth2"},
		blacklistTime:  10 * time.Minute,
	}

	// Make a copy and modify the original
	copied := original.deepCopy()

	// Test basic value copy
	if copied.blockSize != original.blockSize {
		t.Errorf("blockSize not copied correctly: got %d want %d",
			copied.blockSize, original.blockSize)
	}

	// Test slice deep copy
	t.Run("SliceIndependence", func(t *testing.T) {
		// Modify original slices
		original.transportNics[0] = "modified"
		original.cacherootAddrs = append(original.cacherootAddrs, "new.address")
		original.cacherootNics[0] = "changed"

		// Check copied slices remain unchanged
		if copied.transportNics[0] == "modified" {
			t.Error("transportNics slice was shallow copied")
		}
		if len(copied.cacherootAddrs) != 2 {
			t.Error("cacherootAddrs slice was shallow copied")
		}
		if copied.cacherootNics[0] == "changed" {
			t.Error("cacherootNics slice was shallow copied")
		}
	})

	// Test time.Duration fields
	t.Run("TimeFields", func(t *testing.T) {
		if copied.blacklistTime != 10*time.Minute {
			t.Error("blacklistTime not copied correctly")
		}
	})

	// Test string fields
	t.Run("StringFields", func(t *testing.T) {
		if copied.localIP != original.localIP {
			t.Error("localIP not copied correctly")
		}
		if copied.transport != original.transport {
			t.Error("transport not copied correctly")
		}
	})

	// Test zero value handling
	t.Run("ZeroValues", func(t *testing.T) {
		zeroConfig := &remoteCacheConfig{}
		copiedZero := zeroConfig.deepCopy()

		if copiedZero.localPort != 0 {
			t.Error("zero value not preserved in copy")
		}
		if len(copiedZero.transportNics) != 0 {
			t.Error("empty slice not handled properly")
		}
	})
}

func TestPeerCacheFromLocal(t *testing.T) {
	cache := &peerCache{
		config: &remoteCacheConfig{
			localAddr: "127.0.0.1:1234",
		},
	}
	assert.True(t, cache.fromLocal("127.0.0.1:1234"))
	assert.False(t, cache.fromLocal("192.168.1.1:5678"))
}

func TestPeerCacheValidateMetaInfo(t *testing.T) {
	cache := &peerCache{
		config: &remoteCacheConfig{
			blockSize: 1024,
		},
	}

	// Test valid meta info
	validInfo := &metaInfo{
		SrcAddr:   "192.168.1.1:5678",
		BlockSize: 1024,
	}
	assert.NoError(t, cache.validateMetaInfo(validInfo))

	// Test invalid meta info - no SrcAddr
	invalidInfo := &metaInfo{
		SrcAddr:   "",
		BlockSize: 1024,
	}
	assert.Error(t, cache.validateMetaInfo(invalidInfo))

	// Test invalid meta info - wrong block size
	invalidInfo2 := &metaInfo{
		SrcAddr:   "192.168.1.1:5678",
		BlockSize: 2048,
	}
	assert.Error(t, cache.validateMetaInfo(invalidInfo2))
}

func TestPeerCacheMetaInfo(t *testing.T) {
	cache := &peerCache{
		config: &remoteCacheConfig{
			localAddr: "127.0.0.1:1234",
			blockSize: 1024,
		},
		metaMap:   make(map[string]map[uint32]map[string]struct{}),
		peerMap:   make(map[string]map[string]*bitset.BitSet),
		targetMap: make(map[string]*targetInfo),
	}

	info := &metaInfo{
		SrcAddr:   "192.168.1.1:5678",
		BlockSize: 1024,
		Targets:   make(map[string]*targetMeta),
	}

	// Test block-based target
	blocks := bitset.New(10)
	blocks.Set(1)
	blocks.Set(3)
	info.Targets["block_target"] = &targetMeta{
		Blocks: blocks,
		Length: 0,
	}

	// Test whole target
	info.Targets["whole_target"] = &targetMeta{
		Blocks: nil,
		Length: 4096,
	}

	// Test adding meta info
	cache.addMetaInfo(info)

	// Verify block target was added
	assert.Contains(t, cache.metaMap, "block_target")
	assert.Contains(t, cache.metaMap["block_target"], uint32(1))
	assert.Contains(t, cache.metaMap["block_target"], uint32(3))
	assert.Contains(t, cache.metaMap["block_target"][1], "192.168.1.1:5678")

	// Verify whole target was added
	assert.Contains(t, cache.targetMap, "whole_target")
	assert.Contains(t, cache.targetMap["whole_target"].nodes, "192.168.1.1:5678")
	assert.Equal(t, uint64(4096), cache.targetMap["whole_target"].length)

	// Test deleting meta info
	cache.delMetaInfo(info)

	// Verify block target was deleted
	assert.NotContains(t, cache.metaMap["block_target"], uint32(1))
	assert.NotContains(t, cache.metaMap["block_target"], uint32(3))

	// Verify whole target was deleted
	assert.NotContains(t, cache.targetMap, "whole_target")
}

func TestPendingTargetMeta(t *testing.T) {
	var mu sync.Mutex
	pending := make(map[string]*pendingTargetMeta)

	// Add a block to a target
	addPendingEvents(&mu, pending, "test_target", 0, 5)
	assert.Contains(t, pending, "test_target")
	assert.NotNil(t, pending["test_target"].target.Blocks)
	assert.True(t, pending["test_target"].target.Blocks.Test(5))

	// Add another block to the same target
	addPendingEvents(&mu, pending, "test_target", 0, 10)
	assert.True(t, pending["test_target"].target.Blocks.Test(5))
	assert.True(t, pending["test_target"].target.Blocks.Test(10))

	// Add whole target
	addPendingEvents(&mu, pending, "whole_target", 1024, 0)
	assert.Contains(t, pending, "whole_target")
	assert.Equal(t, uint64(1024), pending["whole_target"].target.Length)
	assert.Nil(t, pending["whole_target"].target.Blocks)
}

func TestAddMetaInfo(t *testing.T) {
	tests := []struct {
		name          string
		existing      map[string]map[uint32]map[string]struct{}
		incoming      *metaInfo
		wantTargets   int
		wantBlocks    int
		verifyMetaMap func(t *testing.T, metaMap map[string]map[uint32]map[string]struct{})
	}{
		{
			name:        "empty SrcAddr",
			existing:    map[string]map[uint32]map[string]struct{}{},
			incoming:    &metaInfo{SrcAddr: "", BlockSize: 4194304},
			wantTargets: 0,
			wantBlocks:  0,
		},
		{
			name:        "nil incoming",
			existing:    map[string]map[uint32]map[string]struct{}{},
			incoming:    nil,
			wantTargets: 0,
			wantBlocks:  0,
		},
		{
			name:     "add new target with whole file",
			existing: map[string]map[uint32]map[string]struct{}{},
			incoming: &metaInfo{
				SrcAddr:   "10.0.0.1:9000",
				BlockSize: 4194304,
				Targets: map[string]*targetMeta{
					"/data/file1": {
						Length: 4194304, // 1 block
						Blocks: nil,
					},
				},
			},
			wantTargets: 1, // 1 target in loop
			wantBlocks:  1, // 1 block
			verifyMetaMap: func(t *testing.T, metaMap map[string]map[uint32]map[string]struct{}) {
				assert.Contains(t, metaMap, "/data/file1")
				assert.Contains(t, metaMap["/data/file1"][0], "10.0.0.1:9000")
			},
		},
		{
			name:     "add new target with specific blocks",
			existing: map[string]map[uint32]map[string]struct{}{},
			incoming: &metaInfo{
				SrcAddr:   "10.0.0.2:9000",
				BlockSize: 4194304,
				Targets: map[string]*targetMeta{
					"/data/file2": {
						Length: 0,
						Blocks: func() *bitset.BitSet {
							bs := bitset.New(3)
							bs.Set(0)
							bs.Set(2)
							return bs
						}(),
					},
				},
			},
			wantTargets: 1, // 1 target in loop
			wantBlocks:  2, // 2 blocks (0 and 2)
			verifyMetaMap: func(t *testing.T, metaMap map[string]map[uint32]map[string]struct{}) {
				assert.Contains(t, metaMap, "/data/file2")
				assert.Contains(t, metaMap["/data/file2"][0], "10.0.0.2:9000")
				assert.Contains(t, metaMap["/data/file2"][2], "10.0.0.2:9000")
				assert.NotContains(t, metaMap["/data/file2"], 1)
			},
		},
		{
			name: "merge with existing data - same target different blocks",
			existing: map[string]map[uint32]map[string]struct{}{
				"/data/file1": {
					0: {"10.0.0.1:9000": {}},
				},
			},
			incoming: &metaInfo{
				SrcAddr:   "10.0.0.2:9000",
				BlockSize: 4194304,
				Targets: map[string]*targetMeta{
					"/data/file1": {
						Length: 0,
						Blocks: func() *bitset.BitSet {
							bs := bitset.New(2)
							bs.Set(0)
							bs.Set(1)
							return bs
						}(),
					},
				},
			},
			wantTargets: 1, // 1 target in loop
			wantBlocks:  2, // 2 blocks (0 and 1)
			verifyMetaMap: func(t *testing.T, metaMap map[string]map[uint32]map[string]struct{}) {
				// Block 0 should have both addresses
				assert.Contains(t, metaMap["/data/file1"][0], "10.0.0.1:9000")
				assert.Contains(t, metaMap["/data/file1"][0], "10.0.0.2:9000")
				// Block 1 is new
				assert.Contains(t, metaMap["/data/file1"][1], "10.0.0.2:9000")
			},
		},
		{
			name: "add duplicate address - should not duplicate",
			existing: map[string]map[uint32]map[string]struct{}{
				"/data/file1": {
					0: {"10.0.0.1:9000": {}},
					1: {"10.0.0.1:9000": {}},
				},
			},
			incoming: &metaInfo{
				SrcAddr:   "10.0.0.1:9000",
				BlockSize: 4194304,
				Targets: map[string]*targetMeta{
					"/data/file1": {
						Length: 0,
						Blocks: func() *bitset.BitSet {
							bs := bitset.New(3)
							bs.Set(0)
							bs.Set(1)
							bs.Set(2)
							return bs
						}(),
					},
				},
			},
			wantTargets: 1, // 1 target in loop
			wantBlocks:  3, // Should count all blocks, even if address exists
			verifyMetaMap: func(t *testing.T, metaMap map[string]map[uint32]map[string]struct{}) {
				// All blocks should have the address (no duplicates in map)
				assert.Len(t, metaMap["/data/file1"][0], 1)
				assert.Contains(t, metaMap["/data/file1"][0], "10.0.0.1:9000")
				assert.Len(t, metaMap["/data/file1"][1], 1)
				assert.Contains(t, metaMap["/data/file1"][1], "10.0.0.1:9000")
				assert.Len(t, metaMap["/data/file1"][2], 1)
				assert.Contains(t, metaMap["/data/file1"][2], "10.0.0.1:9000")
			},
		},
		{
			name:     "add multiple targets",
			existing: map[string]map[uint32]map[string]struct{}{},
			incoming: &metaInfo{
				SrcAddr:   "10.0.0.3:9000",
				BlockSize: 4194304,
				Targets: map[string]*targetMeta{
					"/data/file1": {
						Length: 8388608, // 2 blocks
						Blocks: nil,
					},
					"/data/file2": {
						Length: 4194304, // 1 block
						Blocks: nil,
					},
				},
			},
			wantTargets: 2, // 2 targets in loop
			wantBlocks:  3, // 3 blocks total (2 + 1)
			verifyMetaMap: func(t *testing.T, metaMap map[string]map[uint32]map[string]struct{}) {
				assert.Contains(t, metaMap, "/data/file1")
				assert.Contains(t, metaMap, "/data/file2")
				assert.Contains(t, metaMap["/data/file1"][0], "10.0.0.3:9000")
				assert.Contains(t, metaMap["/data/file1"][1], "10.0.0.3:9000")
				assert.Contains(t, metaMap["/data/file2"][0], "10.0.0.3:9000")
			},
		},
		{
			name:     "duplicate addresses across multiple blocks",
			existing: map[string]map[uint32]map[string]struct{}{},
			incoming: &metaInfo{
				SrcAddr:   "10.0.0.1:9000",
				BlockSize: 4194304,
				Targets: map[string]*targetMeta{
					"/data/file1": {
						Length: 0,
						Blocks: func() *bitset.BitSet {
							bs := bitset.New(5)
							bs.Set(0)
							bs.Set(1)
							bs.Set(2)
							bs.Set(3)
							bs.Set(4)
							return bs
						}(),
					},
					"/data/file2": {
						Length: 0,
						Blocks: func() *bitset.BitSet {
							bs := bitset.New(2)
							bs.Set(0)
							bs.Set(1)
							return bs
						}(),
					},
				},
			},
			wantTargets: 2, // 2 targets in loop
			wantBlocks:  7, // 7 blocks total (5 + 2)
			verifyMetaMap: func(t *testing.T, metaMap map[string]map[uint32]map[string]struct{}) {
				// Verify all blocks have the address
				for blk := uint32(0); blk < 5; blk++ {
					assert.Contains(t, metaMap["/data/file1"][blk], "10.0.0.1:9000")
				}
				for blk := uint32(0); blk < 2; blk++ {
					assert.Contains(t, metaMap["/data/file2"][blk], "10.0.0.1:9000")
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &peerCache{
				config: &remoteCacheConfig{
					blockSize: 4194304,
				},
				metaMap:   tc.existing,
				peerMap:   make(map[string]map[string]*bitset.BitSet),
				targetMap: make(map[string]*targetInfo),
			}

			targets, blocks := r.addMetaInfo(tc.incoming)
			assert.Equal(t, tc.wantTargets, targets)
			assert.Equal(t, tc.wantBlocks, blocks)

			if tc.verifyMetaMap != nil {
				tc.verifyMetaMap(t, r.metaMap)
			}
		})
	}
}

// newReconcileCache builds a peerCache with blockSize 1024 suitable for
// reconcilePeer/cleanupMember tests.
func newReconcileCache() *peerCache {
	return &peerCache{
		config: &remoteCacheConfig{
			localAddr: "127.0.0.1:1234",
			blockSize: 1024,
		},
		metaMap:   make(map[string]map[uint32]map[string]struct{}),
		peerMap:   make(map[string]map[string]*bitset.BitSet),
		targetMap: make(map[string]*targetInfo),
	}
}

// bitsetOf builds a bitset with the given block indices set.
func bitsetOf(idx ...uint) *bitset.BitSet {
	bs := bitset.New(0)
	for _, i := range idx {
		bs.Set(i)
	}
	return bs
}

// blockMeta builds a metaInfo announcing block targets from src.
func blockMeta(src string, targets map[string]*bitset.BitSet) *metaInfo {
	info := &metaInfo{SrcAddr: src, BlockSize: 1024, Targets: make(map[string]*targetMeta)}
	for target, bs := range targets {
		info.Targets[target] = &targetMeta{Blocks: bs, Length: 0}
	}
	return info
}

// assertPeerMapConsistent verifies peerMap is an exact reverse view of metaMap:
// every (host, target, block) recorded in one appears in the other, with no empty
// containers left dangling.
func assertPeerMapConsistent(t *testing.T, r *peerCache) {
	t.Helper()
	for target, blockMap := range r.metaMap {
		assert.NotEmpty(t, blockMap, "metaMap target %s has empty block map", target)
		for blk, hosts := range blockMap {
			assert.NotEmpty(t, hosts, "metaMap %s block %d has empty host set", target, blk)
			for host := range hosts {
				tm, ok := r.peerMap[host]
				assert.True(t, ok, "peerMap missing host %s", host)
				bs, ok := tm[target]
				assert.True(t, ok, "peerMap[%s] missing target %s", host, target)
				if ok {
					assert.True(t, bs.Test(uint(blk)), "peerMap[%s][%s] missing block %d", host, target, blk)
				}
			}
		}
	}
	for host, tm := range r.peerMap {
		assert.NotEmpty(t, tm, "peerMap host %s has empty target map", host)
		for target, bs := range tm {
			assert.False(t, bs.None(), "peerMap[%s][%s] is an empty bitset", host, target)
			for i, e := bs.NextSet(0); e; i, e = bs.NextSet(i + 1) {
				hosts, ok := r.metaMap[target][uint32(i)]
				assert.True(t, ok, "metaMap missing %s block %d for host %s", target, i, host)
				_, ok = hosts[host]
				assert.True(t, ok, "metaMap[%s][%d] missing host %s", target, i, host)
			}
		}
	}
}

func TestSetPeerBlock(t *testing.T) {
	const A = "192.168.1.1:5678"
	const B = "192.168.1.2:5678"

	t.Run("creates both views", func(t *testing.T) {
		r := newReconcileCache()
		r.setPeerBlock(A, "t1", 3)

		assert.Contains(t, r.metaMap["t1"][3], A)
		assert.True(t, r.peerMap[A]["t1"].Test(3))
		assert.Equal(t, uint(1), r.peerMap[A]["t1"].Count())
		assertPeerMapConsistent(t, r)
	})

	t.Run("idempotent", func(t *testing.T) {
		r := newReconcileCache()
		r.setPeerBlock(A, "t1", 3)
		r.setPeerBlock(A, "t1", 3)

		assert.Len(t, r.metaMap["t1"][3], 1)
		assert.Equal(t, uint(1), r.peerMap[A]["t1"].Count())
		assertPeerMapConsistent(t, r)
	})

	t.Run("multiple hosts share a block", func(t *testing.T) {
		r := newReconcileCache()
		r.setPeerBlock(A, "t1", 0)
		r.setPeerBlock(B, "t1", 0)

		assert.Len(t, r.metaMap["t1"][0], 2)
		assert.True(t, r.peerMap[A]["t1"].Test(0))
		assert.True(t, r.peerMap[B]["t1"].Test(0))
		assertPeerMapConsistent(t, r)
	})

	t.Run("multiple blocks and targets for one host", func(t *testing.T) {
		r := newReconcileCache()
		r.setPeerBlock(A, "t1", 0)
		r.setPeerBlock(A, "t1", 5)
		r.setPeerBlock(A, "t2", 2)

		assert.Equal(t, uint(2), r.peerMap[A]["t1"].Count())
		assert.True(t, r.peerMap[A]["t1"].Test(0))
		assert.True(t, r.peerMap[A]["t1"].Test(5))
		assert.True(t, r.peerMap[A]["t2"].Test(2))
		assert.Contains(t, r.metaMap["t1"][0], A)
		assert.Contains(t, r.metaMap["t1"][5], A)
		assert.Contains(t, r.metaMap["t2"][2], A)
		assertPeerMapConsistent(t, r)
	})
}

func TestClearPeerBlock(t *testing.T) {
	const A = "192.168.1.1:5678"
	const B = "192.168.1.2:5678"

	t.Run("prunes empty containers on last block", func(t *testing.T) {
		r := newReconcileCache()
		r.setPeerBlock(A, "t1", 3)
		r.clearPeerBlock(A, "t1", 3)

		// metaMap fully pruned: block -> target.
		assert.NotContains(t, r.metaMap, "t1")
		// peerMap fully pruned: target -> host.
		assert.NotContains(t, r.peerMap, A)
		assertPeerMapConsistent(t, r)
	})

	t.Run("keeps other host owning the same block", func(t *testing.T) {
		r := newReconcileCache()
		r.setPeerBlock(A, "t1", 0)
		r.setPeerBlock(B, "t1", 0)
		r.clearPeerBlock(A, "t1", 0)

		assert.NotContains(t, r.metaMap["t1"][0], A)
		assert.Contains(t, r.metaMap["t1"][0], B)
		assert.NotContains(t, r.peerMap, A)
		assert.True(t, r.peerMap[B]["t1"].Test(0))
		assertPeerMapConsistent(t, r)
	})

	t.Run("keeps host's other blocks", func(t *testing.T) {
		r := newReconcileCache()
		r.setPeerBlock(A, "t1", 0)
		r.setPeerBlock(A, "t1", 5)
		r.clearPeerBlock(A, "t1", 0)

		assert.NotContains(t, r.metaMap["t1"], uint32(0))
		assert.Contains(t, r.metaMap["t1"][5], A)
		assert.False(t, r.peerMap[A]["t1"].Test(0))
		assert.True(t, r.peerMap[A]["t1"].Test(5))
		assert.Equal(t, uint(1), r.peerMap[A]["t1"].Count())
		assertPeerMapConsistent(t, r)
	})

	t.Run("keeps host's other targets", func(t *testing.T) {
		r := newReconcileCache()
		r.setPeerBlock(A, "t1", 0)
		r.setPeerBlock(A, "t2", 0)
		r.clearPeerBlock(A, "t1", 0)

		assert.NotContains(t, r.metaMap, "t1")
		assert.Contains(t, r.metaMap["t2"][0], A)
		assert.NotContains(t, r.peerMap[A], "t1")
		assert.True(t, r.peerMap[A]["t2"].Test(0))
		assertPeerMapConsistent(t, r)
	})

	t.Run("no-op on unknown host/target/block", func(t *testing.T) {
		r := newReconcileCache()
		r.setPeerBlock(A, "t1", 0)

		r.clearPeerBlock(B, "t1", 0) // unknown host
		r.clearPeerBlock(A, "t2", 0) // unknown target
		r.clearPeerBlock(A, "t1", 9) // unknown block

		// Original state untouched.
		assert.Contains(t, r.metaMap["t1"][0], A)
		assert.True(t, r.peerMap[A]["t1"].Test(0))
		assertPeerMapConsistent(t, r)
	})
}

func TestReconcilePeer(t *testing.T) {
	const A = "192.168.1.1:5678"
	const B = "192.168.1.2:5678"

	t.Run("skip local", func(t *testing.T) {
		r := newReconcileCache()
		targets, added := r.reconcilePeer(blockMeta(r.config.localAddr, map[string]*bitset.BitSet{"t1": bitsetOf(1)}))
		assert.Equal(t, 0, targets)
		assert.Equal(t, 0, added)
		assert.Empty(t, r.metaMap)
		assert.Empty(t, r.peerMap)
	})

	t.Run("add to empty", func(t *testing.T) {
		r := newReconcileCache()
		targets, added := r.reconcilePeer(blockMeta(A, map[string]*bitset.BitSet{"t1": bitsetOf(1, 3)}))
		assert.Equal(t, 1, targets)
		assert.Equal(t, 2, added)
		assert.Contains(t, r.metaMap["t1"][1], A)
		assert.Contains(t, r.metaMap["t1"][3], A)
		assert.True(t, r.peerMap[A]["t1"].Test(1))
		assert.True(t, r.peerMap[A]["t1"].Test(3))
		assert.Equal(t, uint(2), r.peerMap[A]["t1"].Count())
		assertPeerMapConsistent(t, r)
	})

	t.Run("remove stale blocks", func(t *testing.T) {
		r := newReconcileCache()
		r.reconcilePeer(blockMeta(A, map[string]*bitset.BitSet{"t1": bitsetOf(1, 3)}))
		// Peer now only holds block 1; block 3 is stale and must be dropped.
		targets, added := r.reconcilePeer(blockMeta(A, map[string]*bitset.BitSet{"t1": bitsetOf(1)}))
		assert.Equal(t, 1, targets)
		assert.Equal(t, 0, added)
		assert.Contains(t, r.metaMap["t1"][1], A)
		assert.NotContains(t, r.metaMap["t1"], uint32(3))
		assert.False(t, r.peerMap[A]["t1"].Test(3))
		assertPeerMapConsistent(t, r)
	})

	t.Run("whole target expand then drop", func(t *testing.T) {
		r := newReconcileCache()
		info := &metaInfo{SrcAddr: A, BlockSize: 1024, Targets: map[string]*targetMeta{
			"t1": {Blocks: bitsetOf(1), Length: 0},
			"t2": {Blocks: nil, Length: 4096}, // 4 blocks (0-3)
		}}
		r.reconcilePeer(info)
		assert.Contains(t, r.targetMap, "t2")
		assert.Contains(t, r.targetMap["t2"].nodes, A)
		for i := uint32(0); i < 4; i++ {
			assert.Contains(t, r.metaMap["t2"][i], A)
		}
		assertPeerMapConsistent(t, r)

		// Peer no longer reports t2; it must be purged from metaMap, peerMap and targetMap.
		r.reconcilePeer(blockMeta(A, map[string]*bitset.BitSet{"t1": bitsetOf(1)}))
		assert.NotContains(t, r.metaMap, "t2")
		assert.NotContains(t, r.targetMap, "t2")
		assert.NotContains(t, r.peerMap[A], "t2")
		assertPeerMapConsistent(t, r)
	})

	t.Run("length mismatch skips target", func(t *testing.T) {
		r := newReconcileCache()
		// Local already knows t3 with a different length, owned by B.
		r.targetMap["t3"] = &targetInfo{length: 2048, nodes: map[string]struct{}{B: {}}}
		info := &metaInfo{SrcAddr: A, BlockSize: 1024, Targets: map[string]*targetMeta{
			"t3": {Blocks: nil, Length: 4096},
		}}
		targets, added := r.reconcilePeer(info)
		assert.Equal(t, 1, targets)
		assert.Equal(t, 0, added)
		// A must not be added to the mismatched target.
		assert.NotContains(t, r.targetMap["t3"].nodes, A)
		assert.Nil(t, r.peerMap[A])
		assert.Equal(t, uint64(2048), r.targetMap["t3"].length)
		assertPeerMapConsistent(t, r)
	})

	t.Run("multi-peer isolation", func(t *testing.T) {
		r := newReconcileCache()
		r.reconcilePeer(blockMeta(A, map[string]*bitset.BitSet{"t1": bitsetOf(1)}))
		r.reconcilePeer(blockMeta(B, map[string]*bitset.BitSet{"t1": bitsetOf(1)}))
		// A drops everything; B's ownership of the shared block must survive.
		r.reconcilePeer(&metaInfo{SrcAddr: A, BlockSize: 1024, Targets: map[string]*targetMeta{}})
		assert.NotContains(t, r.metaMap["t1"][1], A)
		assert.Contains(t, r.metaMap["t1"][1], B)
		assert.NotContains(t, r.peerMap, A)
		assert.Contains(t, r.peerMap, B)
		assertPeerMapConsistent(t, r)
	})
}

func TestCleanupMemberPeerMap(t *testing.T) {
	const A = "192.168.1.1:5678"
	const B = "192.168.1.2:5678"
	r := newReconcileCache()
	// A holds t1 blocks {1,2} and the whole target t2 (length 4096 -> 4 blocks).
	r.reconcilePeer(&metaInfo{SrcAddr: A, BlockSize: 1024, Targets: map[string]*targetMeta{
		"t1": {Blocks: bitsetOf(1, 2), Length: 0},
		"t2": {Blocks: nil, Length: 4096},
	}})
	// B shares block 2 of t1 and additionally holds block 3.
	r.reconcilePeer(blockMeta(B, map[string]*bitset.BitSet{"t1": bitsetOf(2, 3)}))

	rmblk, rmtarget := r.cleanupMember(A)
	assert.Equal(t, 6, rmblk)    // t1{1,2}=2 + t2{0,1,2,3}=4
	assert.Equal(t, 1, rmtarget) // whole target t2

	// A is fully gone.
	assert.NotContains(t, r.peerMap, A)
	assert.NotContains(t, r.metaMap, "t2")
	assert.NotContains(t, r.targetMap, "t2")
	assert.NotContains(t, r.metaMap["t1"], uint32(1)) // only A had block 1

	// B is untouched.
	assert.Contains(t, r.metaMap["t1"][2], B)
	assert.Contains(t, r.metaMap["t1"][3], B)
	assert.NotContains(t, r.metaMap["t1"][2], A)
	assert.Contains(t, r.peerMap, B)
	assertPeerMapConsistent(t, r)
}
