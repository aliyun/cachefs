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
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	ss "github.com/aliyun/cachefs/pkg/serf"
	"github.com/eryugey/serf/serf"
	"github.com/stretchr/testify/assert"
	"github.com/willf/bitset"
)

func TestIsValidUserEvent(t *testing.T) {
	tests := []struct {
		eventName string
		want      bool
	}{
		{announceEvent, true},
		{revokeEvent, true},
		{"invalid-user-event", false},
	}

	for _, tt := range tests {
		t.Run(tt.eventName, func(t *testing.T) {
			got := isValidUserEvent(tt.eventName)
			if got != tt.want {
				t.Errorf("isValidUserEvent(%q) = %v, want %v", tt.eventName, got, tt.want)
			}
		})
	}
}

type MockSerfService struct {
	members map[string]bool
	// hashHitFn, when set, decides HashHit per key; nil keeps the default (true).
	hashHitFn func(key string) bool
}

func (m *MockSerfService) BroadCast(name string, payload []byte) error {
	return nil
}

func (m *MockSerfService) RegisterUser(user *ss.SerfUser) error {
	return nil
}

func (m *MockSerfService) GetMembers(raw bool) []string {
	if m.members == nil {
		return nil
	}
	addrs := make([]string, 0, len(m.members))
	for addr := range m.members {
		addrs = append(addrs, addr)
	}
	return addrs
}

func (m *MockSerfService) GetMemberCount() int {
	return 1
}

func (m *MockSerfService) MemberExists(memAddr string) bool {
	if m.members == nil {
		return true // Default to true for testing
	}
	return m.members[memAddr]
}

func (m *MockSerfService) Group() string {
	return ""
}

func (m *MockSerfService) LocalAddr() string {
	return "127.0.0.1:1234"
}

func (m *MockSerfService) HashHit(key string) bool {
	if m.hashHitFn != nil {
		return m.hashHitFn(key)
	}
	return true
}

func (m *MockSerfService) GetHashOwner(key string) (string, string, error) {
	return "", "", nil
}

func (m *MockSerfService) Start() error {
	return nil
}

func (m *MockSerfService) Stop() {
}

func TestHandleSerfUserEvent(t *testing.T) {
	// Create test peerCache
	cache := &peerCache{
		config: &remoteCacheConfig{
			localAddr: "127.0.0.1:1234",
		},
		serfUser: &ss.SerfUser{
			UserEventCh: make(chan serf.UserEvent, 10),
		},
		serf: &MockSerfService{
			members: map[string]bool{
				"192.168.1.1:5678": true,
			},
		},
		announceCh: make(chan *metaInfo, 10),
		revokeCh:   make(chan *metaInfo, 10),
		stopCh:     make(chan struct{}),
	}

	go cache.handleSerfUserEvent()

	// Prepare test data
	info := &metaInfo{
		SrcAddr:   "192.168.1.1:5678",
		BlockSize: 1024,
		Targets: map[string]*targetMeta{
			"test_target": {Length: 1024},
		},
	}
	payload, _ := json.Marshal(info)

	// Test announce event
	cache.serfUser.UserEventCh <- serf.UserEvent{
		Name:    announceEvent,
		Payload: payload,
	}
	time.Sleep(10 * time.Millisecond)
	assert.Len(t, cache.announceCh, 1)

	// Test revoke event
	cache.serfUser.UserEventCh <- serf.UserEvent{
		Name:    revokeEvent,
		Payload: payload,
	}
	time.Sleep(10 * time.Millisecond)
	assert.Len(t, cache.revokeCh, 1)

	// Test unknown event
	cache.serfUser.UserEventCh <- serf.UserEvent{
		Name:    "unknown-event",
		Payload: payload,
	}
	time.Sleep(10 * time.Millisecond)
	// Lengths should remain unchanged
	assert.Len(t, cache.announceCh, 1)
	assert.Len(t, cache.revokeCh, 1)

	// Test event from local (should be ignored)
	localInfo := &metaInfo{
		SrcAddr:   "127.0.0.1:1234",
		BlockSize: 1024,
		Targets:   map[string]*targetMeta{"test": {Length: 1024}},
	}
	localPayload, _ := json.Marshal(localInfo)
	cache.serfUser.UserEventCh <- serf.UserEvent{
		Name:    announceEvent,
		Payload: localPayload,
	}
	time.Sleep(10 * time.Millisecond)
	// Length should remain unchanged
	assert.Len(t, cache.announceCh, 1)

	// Test invalid payload
	cache.serfUser.UserEventCh <- serf.UserEvent{
		Name:    announceEvent,
		Payload: []byte("invalid json"),
	}
	time.Sleep(10 * time.Millisecond)
	// Length should remain unchanged
	assert.Len(t, cache.announceCh, 1)

	// Test event source not in member list
	nonMemberInfo := &metaInfo{
		SrcAddr:   "192.168.99.99:5678", // Not in member list
		BlockSize: 1024,
		Targets:   map[string]*targetMeta{"test": {Length: 1024}},
	}
	nonMemberPayload, _ := json.Marshal(nonMemberInfo)
	cache.serfUser.UserEventCh <- serf.UserEvent{
		Name:    announceEvent,
		Payload: nonMemberPayload,
	}
	time.Sleep(10 * time.Millisecond)
	// Length should remain unchanged
	assert.Len(t, cache.announceCh, 1)

	// Test channel full scenario
	fullCache := &peerCache{
		config: &remoteCacheConfig{
			localAddr: "127.0.0.1:1234",
		},
		serfUser: &ss.SerfUser{
			UserEventCh: make(chan serf.UserEvent, 10),
		},
		serf: &MockSerfService{
			members: map[string]bool{
				"192.168.1.1:5678": true,
			},
		},
		announceCh: make(chan *metaInfo, 1), // Small capacity
		stopCh:     make(chan struct{}),
	}

	go fullCache.handleSerfUserEvent()

	// Fill the channel
	fullCache.announceCh <- &metaInfo{}

	// Send multiple events - should not block or panic
	for i := 0; i < 3; i++ {
		fullCache.serfUser.UserEventCh <- serf.UserEvent{
			Name:    announceEvent,
			Payload: payload,
		}
	}
	time.Sleep(10 * time.Millisecond)

	// Test stop signal
	close(cache.stopCh)
	time.Sleep(10 * time.Millisecond)
}

func TestHandleSerfMemberJoinEvent(t *testing.T) {
	cache := &peerCache{
		config: &remoteCacheConfig{
			localAddr: "127.0.0.1:1234",
		},
		serfUser: &ss.SerfUser{
			MemberJoinCh: make(chan []ss.SerfMember, 10),
		},
		transport:      &mockExchangeTransport{},
		seeders:        make(map[string]struct{}),
		exchangeCh:     make(chan string, exchangeTrackerChSize),
		exchangeDoneCh: make(chan struct{}),
		stopCh:         make(chan struct{}),
	}

	go cache.handleSerfMemberJoinEvent()

	// Test adding new seeder members
	members := []ss.SerfMember{
		{Name: "seeder", Addr: "192.168.1.1:5678", Role: ss.SerfRoleSeeder},
		{Name: "init", Addr: "192.168.1.2:5678", Role: ss.SerfRoleInit},
		{Name: "localaddr", Addr: "127.0.0.1:1234", Role: ss.SerfRoleSeeder},
	}

	cache.serfUser.MemberJoinCh <- members
	time.Sleep(10 * time.Millisecond)

	// Verify only seeder was added
	cache.seederMu.Lock()
	_, exists := cache.seeders["192.168.1.1:5678"]
	cache.seederMu.Unlock()
	assert.True(t, exists)

	// Verify non-seeder and local address were not added as seeders
	cache.seederMu.Lock()
	_, exists = cache.seeders["192.168.1.2:5678"]
	_, localExists := cache.seeders["127.0.0.1:1234"]
	cache.seederMu.Unlock()
	assert.False(t, exists)
	assert.False(t, localExists)

	// Verify both seeder and non-seeder addresses were pushed to exchangeCh
	// (all non-local members, not just seeders)
	var exchangeAddrs []string
collectDone:
	for {
		select {
		case addr := <-cache.exchangeCh:
			exchangeAddrs = append(exchangeAddrs, addr)
		default:
			break collectDone
		}
	}
	assert.Equal(t, 2, len(exchangeAddrs)) // seeder + initnode, local excluded
	assert.Contains(t, exchangeAddrs, "192.168.1.1:5678")
	assert.Contains(t, exchangeAddrs, "192.168.1.2:5678")

	// Test duplicate addition of same seeder
	cache.serfUser.MemberJoinCh <- []ss.SerfMember{
		{Name: "dup seeder", Addr: "192.168.1.1:5678", Role: ss.SerfRoleSeeder},
	}
	time.Sleep(10 * time.Millisecond)

	// Verify no duplicate was added
	cache.seederMu.Lock()
	assert.Len(t, cache.seeders, 1)
	cache.seederMu.Unlock()

	// Test empty member list
	cache.serfUser.MemberJoinCh <- []ss.SerfMember{}
	time.Sleep(10 * time.Millisecond)
	// Should not affect existing seeders
	cache.seederMu.Lock()
	assert.Len(t, cache.seeders, 1)
	cache.seederMu.Unlock()

	close(cache.stopCh)
	time.Sleep(10 * time.Millisecond)
}

func TestHandleSerfMemberJoinEventExchange(t *testing.T) {
	tests := []struct {
		name          string
		closeDoneCh   bool
		exchangeChCap int
		members       []ss.SerfMember
		wantExchangeN int
		wantAtMost    bool // true: use LessOrEqual instead of Equal
	}{
		{
			name:          "join window open - members pushed to exchangeCh",
			closeDoneCh:   false,
			exchangeChCap: exchangeTrackerChSize,
			members: []ss.SerfMember{
				{Name: "seeder", Addr: "10.0.0.1:9000", Role: ss.SerfRoleSeeder},
				{Name: "init", Addr: "10.0.0.2:9000", Role: ss.SerfRoleInit},
			},
			wantExchangeN: 2, // both seeder and init are pushed
		},
		{
			name:          "join window closed - members not pushed to exchangeCh",
			closeDoneCh:   true,
			exchangeChCap: exchangeTrackerChSize,
			members: []ss.SerfMember{
				{Name: "seeder", Addr: "10.0.0.3:9000", Role: ss.SerfRoleSeeder},
				{Name: "init", Addr: "10.0.0.4:9000", Role: ss.SerfRoleInit},
			},
			wantExchangeN: 0, // exchangeDoneCh closed, nothing pushed
		},
		{
			name:          "local member excluded from exchangeCh",
			closeDoneCh:   false,
			exchangeChCap: exchangeTrackerChSize,
			members: []ss.SerfMember{
				{Name: "local", Addr: "127.0.0.1:1234", Role: ss.SerfRoleSeeder},
				{Name: "remote", Addr: "10.0.0.5:9000", Role: ss.SerfRoleSeeder},
			},
			wantExchangeN: 1, // only remote, local excluded
		},
		{
			name:          "channel full - excess members dropped",
			closeDoneCh:   false,
			exchangeChCap: 1, // tiny channel
			members: []ss.SerfMember{
				{Name: "a", Addr: "10.0.0.10:9000", Role: ss.SerfRoleSeeder},
				{Name: "b", Addr: "10.0.0.11:9000", Role: ss.SerfRoleSeeder},
				{Name: "c", Addr: "10.0.0.12:9000", Role: ss.SerfRoleSeeder},
			},
			wantExchangeN: 1, // at most 1, rest dropped
			wantAtMost:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exchangeDoneCh := make(chan struct{})
			cache := &peerCache{
				config: &remoteCacheConfig{
					localAddr: "127.0.0.1:1234",
				},
				serfUser: &ss.SerfUser{
					MemberJoinCh: make(chan []ss.SerfMember, 10),
				},
				transport:      &mockExchangeTransport{},
				seeders:        make(map[string]struct{}),
				exchangeCh:     make(chan string, tt.exchangeChCap),
				exchangeDoneCh: exchangeDoneCh,
				stopCh:         make(chan struct{}),
			}

			if tt.closeDoneCh {
				close(exchangeDoneCh)
			}

			go cache.handleSerfMemberJoinEvent()

			cache.serfUser.MemberJoinCh <- tt.members
			time.Sleep(20 * time.Millisecond)

			// Drain exchangeCh and count
			var gotN int
			for {
				select {
				case <-cache.exchangeCh:
					gotN++
				default:
					goto done
				}
			}
		done:
			if tt.wantAtMost {
				assert.LessOrEqual(t, gotN, tt.wantExchangeN)
			} else {
				assert.Equal(t, tt.wantExchangeN, gotN)
			}

			close(cache.stopCh)
			time.Sleep(10 * time.Millisecond)
		})
	}
}

// mockExchangeTransport records exchangeTracker calls for testing.
type mockExchangeTransport struct {
	mu    sync.Mutex
	addrs []string
	err   error
}

func (m *mockExchangeTransport) load(ctx context.Context, addr, target string, blk, blklen uint32, page *Page, group string, cacheroot bool) error {
	return nil
}

func (m *mockExchangeTransport) updateRdmaPeer(string, string) {}
func (m *mockExchangeTransport) isRdmaPeer(string) bool        { return true }

func (m *mockExchangeTransport) exchangeTracker(ctx context.Context, addr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addrs = append(m.addrs, addr)
	return m.err
}

func (m *mockExchangeTransport) stop() error {
	return nil
}

func (m *mockExchangeTransport) getAddrs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]string, len(m.addrs))
	copy(cp, m.addrs)
	return cp
}

func TestServeExchangeTracker(t *testing.T) {
	tests := []struct {
		name      string
		addrs     []string
		err       error
		wantAddrs []string
		exitBy    string // "done" to close exchangeDoneCh, "stop" to close stopCh; empty for normal close after processing
	}{
		{
			name:      "single address",
			addrs:     []string{"10.0.0.1:9000"},
			wantAddrs: []string{"10.0.0.1:9000"},
		},
		{
			name:      "multiple addresses",
			addrs:     []string{"10.0.0.1:9000", "10.0.0.2:9000", "10.0.0.3:9000"},
			wantAddrs: []string{"10.0.0.1:9000", "10.0.0.2:9000", "10.0.0.3:9000"},
		},
		{
			name:      "exchangeTracker error does not block",
			addrs:     []string{"10.0.0.1:9000", "10.0.0.2:9000"},
			err:       fmt.Errorf("rpc error"),
			wantAddrs: []string{"10.0.0.1:9000", "10.0.0.2:9000"},
		},
		{
			name:      "exit on stopCh closed",
			exitBy:    "stop",
			wantAddrs: nil, // worker exits before any addresses are pushed
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mt := &mockExchangeTransport{err: tt.err}
			exchangeCh := make(chan string, exchangeTrackerChSize)
			exchangeDoneCh := make(chan struct{})
			stopCh := make(chan struct{})
			cache := &peerCache{
				config:         &remoteCacheConfig{},
				transport:      mt,
				exchangeCh:     exchangeCh,
				exchangeDoneCh: exchangeDoneCh,
				stopCh:         stopCh,
			}

			go cache.serveExchangeTracker(0)

			switch tt.exitBy {
			case "stop":
				close(stopCh)
				time.Sleep(20 * time.Millisecond)
				exchangeCh <- "10.0.0.1:9000"
				time.Sleep(20 * time.Millisecond)
				assert.Empty(t, mt.getAddrs())
			default:
				// Push addresses into the channel
				for _, addr := range tt.addrs {
					exchangeCh <- addr
				}
				// Wait for worker to process all addresses
				time.Sleep(50 * time.Millisecond)
				time.Sleep(10 * time.Millisecond)
				close(stopCh)
				assert.Equal(t, tt.wantAddrs, mt.getAddrs())
			}
		})
	}
}

func TestCollectMeta(t *testing.T) {
	tests := []struct {
		name                string
		closeStopBeforeJoin bool // close stopCh before JoinedCh
		wantDoneChClosed    bool
	}{
		{
			name:             "join completes - exchangeDoneCh closed",
			wantDoneChClosed: true,
		},
		{
			name:                "stopCh before join - exchangeDoneCh not closed",
			closeStopBeforeJoin: true,
			wantDoneChClosed:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			joinedCh := make(chan struct{})
			exchangeDoneCh := make(chan struct{})
			stopCh := make(chan struct{})

			cache := &peerCache{
				config: &remoteCacheConfig{
					localAddr: "127.0.0.1:1234",
					blockSize: 4096,
				},
				serfUser: &ss.SerfUser{
					JoinedCh: joinedCh,
				},
				serf:           &MockSerfService{},
				exchangeDoneCh: exchangeDoneCh,
				stopCh:         stopCh,
			}

			go cache.collectMeta()

			if tt.closeStopBeforeJoin {
				close(stopCh)
				time.Sleep(20 * time.Millisecond)

				select {
				case <-exchangeDoneCh:
					t.Errorf("exchangeDoneCh should not be closed when stopCh is triggered before join")
					return
				default:
				}
				return
			}

			close(joinedCh)

			if tt.wantDoneChClosed {
				select {
				case <-exchangeDoneCh:
					// success
				case <-time.After(ss.CoalescePeriod*2 + time.Second):
					t.Errorf("exchangeDoneCh was not closed within expected time")
					return
				}
			}

			close(stopCh)
		})
	}
}

func TestRandomPeers(t *testing.T) {
	tests := []struct {
		name    string
		serf    ss.SerfService
		n       int
		wantLen int
	}{
		{
			name:    "nil serf returns none",
			serf:    nil,
			n:       3,
			wantLen: 0,
		},
		{
			name:    "empty member list returns none",
			serf:    &MockSerfService{},
			n:       3,
			wantLen: 0,
		},
		{
			name: "only local member returns none",
			serf: &MockSerfService{
				members: map[string]bool{"127.0.0.1:1234": true},
			},
			n:       3,
			wantLen: 0,
		},
		{
			name: "non-positive n returns none",
			serf: &MockSerfService{
				members: map[string]bool{
					"127.0.0.1:1234": true,
					"10.0.0.1:9000":  true,
				},
			},
			n:       0,
			wantLen: 0,
		},
		{
			name: "fewer peers than fanout returns all remote",
			serf: &MockSerfService{
				members: map[string]bool{
					"127.0.0.1:1234": true,
					"10.0.0.1:9000":  true,
				},
			},
			n:       3,
			wantLen: 1,
		},
		{
			name: "more peers than fanout capped at n",
			serf: &MockSerfService{
				members: map[string]bool{
					"127.0.0.1:1234": true,
					"10.0.0.1:9000":  true,
					"10.0.0.2:9000":  true,
					"10.0.0.3:9000":  true,
					"10.0.0.4:9000":  true,
				},
			},
			n:       3,
			wantLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := &peerCache{
				config: &remoteCacheConfig{
					localAddr: "127.0.0.1:1234",
				},
				serf: tt.serf,
			}

			peers := cache.randomPeers(tt.n)
			assert.Len(t, peers, tt.wantLen)

			seen := make(map[string]bool)
			for _, addr := range peers {
				assert.NotEqual(t, "127.0.0.1:1234", addr, "must exclude local")
				assert.False(t, seen[addr], "peers must be distinct")
				seen[addr] = true
			}
		})
	}
}

func TestExchangeScaleInterval(t *testing.T) {
	base := 10 * time.Second

	tests := []struct {
		name     string
		n        int
		expected time.Duration
	}{
		{"below threshold - 1 node", 1, base},
		{"below threshold - 16 nodes", 16, base},
		{"at threshold - 32 nodes", 32, base},
		{"above threshold - 33 nodes", 33, 2 * base},
		{"above threshold - 64 nodes", 64, 2 * base},
		{"above threshold - 65 nodes", 65, 3 * base},
		{"above threshold - 128 nodes", 128, 3 * base},
		{"above threshold - 129 nodes", 129, 4 * base},
		{"above threshold - 256 nodes", 256, 4 * base},
		{"above threshold - 257 nodes", 257, 5 * base},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := exchangeScaleInterval(base, tt.n)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestPeriodicExchange(t *testing.T) {
	tests := []struct {
		name         string
		interval     time.Duration
		members      map[string]bool
		wantExchange bool
	}{
		{
			name:         "enabled - periodic exchange triggers",
			interval:     50 * time.Millisecond,
			members:      map[string]bool{"127.0.0.1:1234": true, "10.0.0.1:9000": true},
			wantExchange: true,
		},
		{
			name:         "disabled - no periodic exchange",
			interval:     0,
			members:      map[string]bool{"10.0.0.1:9000": true},
			wantExchange: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exchangeCh := make(chan string, exchangeTrackerChSize)
			stopCh := make(chan struct{})

			cache := &peerCache{
				config: &remoteCacheConfig{
					localAddr:    "127.0.0.1:1234",
					exchangeTime: tt.interval,
				},
				serf: &MockSerfService{
					members: tt.members,
				},
				exchangeCh: exchangeCh,
				stopCh:     stopCh,
			}

			if tt.wantExchange {
				go cache.periodicExchange()

				select {
				case addr := <-exchangeCh:
					assert.NotEmpty(t, addr)
					assert.NotEqual(t, "127.0.0.1:1234", addr)
				case <-time.After(200 * time.Millisecond):
					t.Errorf("periodicExchange did not push any address within expected time")
					return
				}
			} else {
				// periodicExchange should not be started when interval is 0
				select {
				case <-exchangeCh:
					t.Errorf("should not receive any address when periodic exchange is disabled")
					return
				case <-time.After(150 * time.Millisecond):
					// success: no exchange triggered
				}
			}

			close(stopCh)
			time.Sleep(20 * time.Millisecond)
		})
	}
}

func TestHandleSerfMemberLeaveEvent(t *testing.T) {
	cache := &peerCache{
		config: &remoteCacheConfig{
			localAddr: "127.0.0.1:1234",
		},
		serfUser: &ss.SerfUser{
			MemberLeaveCh: make(chan []ss.SerfMember, 10),
		},
		transport: &mockExchangeTransport{},
		metaMap:   make(map[string]map[uint32]map[string]struct{}),
		peerMap:   make(map[string]map[string]*bitset.BitSet),
		targetMap: make(map[string]*targetInfo),
		seeders:   make(map[string]struct{}),
		stopCh:    make(chan struct{}),
	}

	// Add test data (via setPeerBlock so metaMap and peerMap stay consistent).
	cache.setPeerBlock("192.168.1.1:5678", "test_target", 0)
	cache.setPeerBlock("192.168.1.2:5678", "test_target", 0)

	cache.targetMap["test_target"] = &targetInfo{
		length: 1024,
		nodes: map[string]struct{}{
			"192.168.1.1:5678": {},
			"192.168.1.2:5678": {},
		},
	}

	cache.seeders["192.168.1.1:5678"] = struct{}{}
	cache.seeders["192.168.1.2:5678"] = struct{}{}

	go cache.handleSerfMemberLeaveEvent()

	// Test member leave event
	members := []ss.SerfMember{
		{Addr: "192.168.1.1:5678"},
		{Addr: "192.168.99.99:5678"}, // Member not in any maps
	}

	cache.serfUser.MemberLeaveCh <- members
	time.Sleep(10 * time.Millisecond)

	// Verify member's metadata was cleaned up
	cache.metaMu.Lock()
	_, exists := cache.metaMap["test_target"][0]["192.168.1.1:5678"]
	cache.metaMu.Unlock()
	assert.False(t, exists)

	cache.targetMu.Lock()
	_, exists = cache.targetMap["test_target"].nodes["192.168.1.1:5678"]
	cache.targetMu.Unlock()
	assert.False(t, exists)

	// Verify seeder was removed
	cache.seederMu.Lock()
	_, exists = cache.seeders["192.168.1.1:5678"]
	cache.seederMu.Unlock()
	assert.False(t, exists)

	// Verify other members were unaffected
	cache.metaMu.Lock()
	_, exists = cache.metaMap["test_target"][0]["192.168.1.2:5678"]
	cache.metaMu.Unlock()
	assert.True(t, exists)

	cache.targetMu.Lock()
	_, exists = cache.targetMap["test_target"].nodes["192.168.1.2:5678"]
	cache.targetMu.Unlock()
	assert.True(t, exists)

	cache.seederMu.Lock()
	_, exists = cache.seeders["192.168.1.2:5678"]
	cache.seederMu.Unlock()
	assert.True(t, exists)

	// Test empty member list
	cache.serfUser.MemberLeaveCh <- []ss.SerfMember{}
	time.Sleep(10 * time.Millisecond)
	// Should not affect existing data
	cache.seederMu.Lock()
	assert.Len(t, cache.seeders, 1)
	cache.seederMu.Unlock()

	close(cache.stopCh)
	time.Sleep(10 * time.Millisecond)
}

func TestHandleSerfMemberRdmaGroup(t *testing.T) {
	for _, group := range []string{"", "region-a"} {
		cache := &peerCache{
			config: &remoteCacheConfig{localAddr: "127.0.0.1:1234", rdmaGroup: group},
			serfUser: &ss.SerfUser{
				MemberJoinCh: make(chan []ss.SerfMember, 10), MemberLeaveCh: make(chan []ss.SerfMember, 10),
			},
			transport: &transport{config: &remoteCacheConfig{rdmaGroup: group}},
			metaMap:   make(map[string]map[uint32]map[string]struct{}), peerMap: make(map[string]map[string]*bitset.BitSet),
			targetMap: make(map[string]*targetInfo), seeders: make(map[string]struct{}),
			exchangeCh: make(chan string, exchangeTrackerChSize), exchangeDoneCh: make(chan struct{}), stopCh: make(chan struct{}),
		}
		go cache.handleSerfMemberJoinEvent()
		go cache.handleSerfMemberLeaveEvent()
		cache.serfUser.MemberJoinCh <- []ss.SerfMember{
			{Name: "same", Addr: "10.0.0.1:9000", RdmaGroup: "region-a"},
			{Name: "other", Addr: "10.0.0.2:9000", RdmaGroup: "region-b"},
			{Name: "untagged", Addr: "10.0.0.3:9000"},
			{Name: "local", Addr: "127.0.0.1:1234", RdmaGroup: "region-a"},
		}
		for i := 0; i < 3; i++ {
			addr := <-cache.exchangeCh
			assert.Equal(t, group == "" || addr == "10.0.0.1:9000", cache.transport.isRdmaPeer(addr), addr)
		}
		cache.serfUser.MemberJoinCh <- []ss.SerfMember{{Name: "same", Addr: "10.0.0.1:9000"}}
		<-cache.exchangeCh
		assert.Equal(t, group == "", cache.transport.isRdmaPeer("10.0.0.1:9000"))
		cache.serfUser.MemberJoinCh <- []ss.SerfMember{{Name: "same", Addr: "10.0.0.1:9000", RdmaGroup: "region-a"}}
		<-cache.exchangeCh
		assert.True(t, cache.transport.isRdmaPeer("10.0.0.1:9000"))
		cache.serfUser.MemberLeaveCh <- []ss.SerfMember{{Name: "same", Addr: "10.0.0.1:9000"}}
		if group == "" {
			assert.True(t, cache.transport.isRdmaPeer("10.0.0.1:9000"))
		} else {
			assert.Eventually(t, func() bool { return !cache.transport.isRdmaPeer("10.0.0.1:9000") }, time.Second, time.Millisecond)
		}
		close(cache.stopCh)
	}
}

func TestCleanupMember(t *testing.T) {
	cache := &peerCache{
		metaMap:   make(map[string]map[uint32]map[string]struct{}),
		peerMap:   make(map[string]map[string]*bitset.BitSet),
		targetMap: make(map[string]*targetInfo),
	}

	// Add test data (via setPeerBlock so metaMap and peerMap stay consistent):
	// test_target block 0 owned by .1 and .2, block 1 owned by .1 only.
	cache.setPeerBlock("192.168.1.1:5678", "test_target", 0)
	cache.setPeerBlock("192.168.1.2:5678", "test_target", 0)
	cache.setPeerBlock("192.168.1.1:5678", "test_target", 1)

	cache.targetMap["test_target"] = &targetInfo{
		length: 1024,
		nodes: map[string]struct{}{
			"192.168.1.1:5678": {},
			"192.168.1.2:5678": {},
		},
	}

	// Add another target with only the member to be deleted
	cache.setPeerBlock("192.168.1.1:5678", "target2", 0)
	cache.targetMap["target2"] = &targetInfo{
		length: 2048,
		nodes: map[string]struct{}{
			"192.168.1.1:5678": {},
		},
	}

	// Perform cleanup
	rmblk, rmtarget := cache.cleanupMember("192.168.1.1:5678")

	// Verify return values
	assert.Equal(t, 3, rmblk)    // Deleted from 3 blocks
	assert.Equal(t, 2, rmtarget) // Deleted from 2 targets

	// Verify correct data structure cleanup
	cache.metaMu.Lock()
	// test_target block 0 should still exist but only with 192.168.1.2:5678
	assert.Len(t, cache.metaMap["test_target"][0], 1)
	assert.Contains(t, cache.metaMap["test_target"][0], "192.168.1.2:5678")

	// test_target block 1 should be deleted (no other hosts)
	assert.NotContains(t, cache.metaMap["test_target"], uint32(1))

	// target2 should be completely deleted (no other hosts)
	assert.NotContains(t, cache.metaMap, "target2")
	cache.metaMu.Unlock()

	cache.targetMu.Lock()
	// test_target should still exist but only with 192.168.1.2:5678
	assert.Len(t, cache.targetMap["test_target"].nodes, 1)
	assert.Contains(t, cache.targetMap["test_target"].nodes, "192.168.1.2:5678")

	// target2 should be completely deleted
	assert.NotContains(t, cache.targetMap, "target2")
	cache.targetMu.Unlock()

	// Test cleanup of non-existent member
	rmblk, rmtarget = cache.cleanupMember("192.168.99.99:5678")
	assert.Equal(t, 0, rmblk)
	assert.Equal(t, 0, rmtarget)
}
