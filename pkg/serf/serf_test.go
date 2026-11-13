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

package serf

import (
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/aliyun/cachefs/pkg/consistent"
	"github.com/eryugey/serf/serf"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func TestIsValidEventType(t *testing.T) {
	tests := []struct {
		typ  string
		want bool
	}{
		{eventTypeUser, true},
		{eventTypeMemberJoin, true},
		{eventTypeMemberLeave, true},
		{eventTypeMemberFailed, true},
		{"invalid-event-type", false},
	}

	for _, tt := range tests {
		t.Run(tt.typ, func(t *testing.T) {
			got := isValidEventType(tt.typ)
			if got != tt.want {
				t.Errorf("isValidEventType(%q) = %v, want %v", tt.typ, got, tt.want)
			}
		})
	}
}

func TestFormatMemberAddr(t *testing.T) {
	tests := []struct {
		member *serf.Member
		want   string
	}{
		{
			&serf.Member{
				Addr: net.ParseIP("127.0.0.1"),
				Tags: map[string]string{
					keyCachePort: "8080",
				},
			},
			"127.0.0.1:8080",
		},
		{
			&serf.Member{
				Addr: net.ParseIP("192.168.1.1"),
				Tags: map[string]string{
					keyCachePort: "9999",
				},
			},
			"192.168.1.1:9999",
		},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := formatMemberAddr(tt.member)
			if got != tt.want {
				t.Errorf("formatMemberAddr() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIsSerfInitNode(t *testing.T) {
	tests := []struct {
		name       string
		member     *serf.Member
		isInitNode bool
	}{
		{
			"initnode",
			&serf.Member{
				Tags: map[string]string{
					keySerfRole: "initnode",
				},
			},
			true,
		},
		{
			"init",
			&serf.Member{
				Tags: map[string]string{
					keySerfRole: "init",
				},
			},
			false,
		},
		{
			"sometag",
			&serf.Member{
				Tags: map[string]string{
					"sometag": "initnode",
				},
			},
			false,
		},
		{
			"notag",
			&serf.Member{},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isSerfInitNode(tt.member)
			if got != tt.isInitNode {
				t.Errorf("isSerfInitNode(%v) = %v, want %v", tt.member.Tags, got, tt.isInitNode)
			}
		})
	}
}

func TestIsSerfGroup(t *testing.T) {
	tests := []struct {
		group  string
		member *serf.Member
		expect bool
	}{
		{
			"",
			&serf.Member{},
			true,
		},
		{
			"",
			&serf.Member{
				Tags: map[string]string{
					keySerfGroup: "",
				},
			},
			true,
		},
		{
			"group1",
			&serf.Member{
				Tags: map[string]string{
					keySerfGroup: "group1",
				},
			},
			true,
		},
		{
			"group2",
			&serf.Member{
				Tags: map[string]string{
					keySerfGroup: "group3",
				},
			},
			false,
		},
	}
	for _, tc := range tests {
		group, same := isSerfGroup(tc.group, tc.member)
		if same != tc.expect {
			t.Errorf("test for group \"%s\" member %v failed", tc.group, *tc.member)
		}
		if same && group != tc.group {
			t.Errorf("wrong group name \"%s\" for group \"%s\" member %v", group, tc.group, *tc.member)
		}
	}
}

func TestInSameSerfGroup(t *testing.T) {
	tests := []struct {
		group  string
		member *serf.Member
		expect bool
	}{
		{
			"",
			&serf.Member{},
			true,
		},
		{
			"",
			&serf.Member{
				Tags: map[string]string{
					keySerfRole: "initnode",
				},
			},
			false,
		},
		{
			"",
			&serf.Member{
				Tags: map[string]string{
					keySerfGroup: "",
				},
			},
			true,
		},
		{
			"group1",
			&serf.Member{
				Tags: map[string]string{
					keySerfGroup: "group1",
				},
			},
			true,
		},
		{
			"group2",
			&serf.Member{
				Tags: map[string]string{
					keySerfGroup: "group2",
					keySerfRole:  "initnode",
				},
			},
			false,
		},
		{
			"group3",
			&serf.Member{
				Tags: map[string]string{
					keySerfGroup: "group4",
				},
			},
			false,
		},
	}

	for _, tt := range tests {
		cfg := &SerfConfig{
			Group: tt.group,
		}
		ss := &serfService{
			config: cfg,
		}
		got := ss.inSameSerfGroup(tt.member)
		if got != tt.expect {
			t.Errorf("inSameSerfGroup(%v) = %v, want %v", *tt.member, got, tt.expect)
		}
	}
}

func newTestSerfConfig() *SerfConfig {
	cfg := DefaultSerfConfig()
	cfg.LeaveDelay = 10 * time.Microsecond
	cfg.CacheAddr = "127.0.0.1:7788"
	cfg.CachePort = 7788
	cfg.BindAddr = "127.0.0.1"
	cfg.BindPort = 0
	cfg.NodeCount = VirtualNodeCount
	return cfg
}

func TestNewSerfService(t *testing.T) {
	cfg := newTestSerfConfig()
	ss := NewSerfService(cfg, nil)
	_, ok := ss.(*serfService)
	assert.Equal(t, true, ok, "cast to *serfService w/o reg")

	reg := prometheus.NewRegistry()
	ss = NewSerfService(cfg, reg)
	_, ok = ss.(*serfService)
	assert.Equal(t, true, ok, "cast to *serfService with reg")
}

func TestSerfServiceStartStop(t *testing.T) {
	cfg := newTestSerfConfig()
	ss := NewSerfService(cfg, nil)
	svc, ok := ss.(*serfService)
	assert.Equal(t, true, ok, "cast to *serfService")

	err := ss.Start()
	assert.NoError(t, err, "start serf service")
	if cfg.HashRing {
		if svc.hashring.Consistent == nil {
			t.Error("hashring should not be nil")
		}
	} else {
		if svc.hashring.Consistent != nil {
			t.Error("hashring should be nil")
		}
	}

	// Give it some time to handle member join event, as we enable event coalesce
	time.Sleep(GossipInterval * 2)
	members := ss.GetMembers(false)
	assert.Equal(t, []string{"127.0.0.1:7788"}, members, "cached members")
	members = ss.GetMembers(true)
	assert.Equal(t, []string{"127.0.0.1:7788"}, members, "raw members")

	ok = ss.MemberExists("127.0.0.1:7788")
	assert.Equal(t, true, ok, "127.0.0.1:7788 exists")
	ok = ss.MemberExists("127.0.0.1:7789")
	assert.Equal(t, false, ok, "127.0.0.1:7789 not exists")

	ok = ss.HashHit("test key")
	assert.Equal(t, true, ok, "hash hit")

	ss.Stop()
}

func TestRegisterUser(t *testing.T) {
	cfg := newTestSerfConfig()
	cfg.RdmaGroup = "region-a"
	ss := NewSerfService(cfg, nil)
	svc, ok := ss.(*serfService)
	assert.Equal(t, true, ok, "cast to *serfService")

	err := ss.RegisterUser(nil)
	assert.NoError(t, err, "nil user")

	user1 := &SerfUser{
		Name:          "user1",
		UserEvents:    []string{"event1", "event2"},
		MemberJoinCh:  make(chan []SerfMember, 2),
		MemberLeaveCh: make(chan []SerfMember, 2),
		UserEventCh:   make(chan serf.UserEvent, 2),
		ReadyCh:       make(chan struct{}),
		JoinedCh:      make(chan struct{}, 1),
	}

	// Register first user
	err = ss.RegisterUser(user1)
	assert.NoError(t, err, "register first user")
	assert.Equal(t, map[string]*SerfUser{"user1": user1}, svc.users, "actual users")
	assert.Equal(t, map[string]*SerfUser{"event1": user1, "event2": user1}, svc.events, "actual user events")

	// Register duplicated user, should fail
	err = ss.RegisterUser(user1)
	assert.Equal(t, errors.New("serf user \"user1\" already registered"), err, "double register")

	// Register user with empty name, should fail
	user1.Name = ""
	err = ss.RegisterUser(user1)
	assert.Equal(t, errors.New("serf user has empty name"), err, "empty name")
	user1.Name = "user1"

	// Register user with empty events, should fail
	user1.UserEvents = []string{}
	err = ss.RegisterUser(user1)
	assert.Equal(t, errors.New("no serf user events"), err, "empty user events")
	user1.UserEvents = []string{"event1", "event2"}

	user2 := &SerfUser{
		Name:          "user2",
		UserEvents:    []string{"event1", "event3"},
		MemberJoinCh:  make(chan []SerfMember, 2),
		MemberLeaveCh: make(chan []SerfMember, 2),
		UserEventCh:   make(chan serf.UserEvent, 2),
		ReadyCh:       make(chan struct{}),
		JoinedCh:      make(chan struct{}, 1),
	}

	// Register user with duplicated events, should fail
	err = ss.RegisterUser(user2)
	assert.Equal(t, errors.New("serf event event1 already registered"), err, "duplicated user events")
	_, ok = svc.users[user2.Name]
	assert.Equal(t, false, ok, "should not contain user2")
	assert.Equal(t, map[string]*SerfUser{"event1": user1, "event2": user1}, svc.events, "cleanup duplicated user events should not delete existing event")

	// Broadcast before start, should fail
	err = ss.BroadCast("event1", []byte("event1 payload"))
	assert.Equal(t, errors.New("serf service not started yet"), err, "broadcast before start")

	// Check ready channel
	go ss.Start()
	user1.WaitReady()
	select {
	case <-user1.ReadyCh:
	default:
		t.Error("notify ready failed")
	}

	// Check member join event
	hostname, _ := os.Hostname()
	timer := time.NewTimer(time.Millisecond * 300)
	select {
	case <-timer.C:
		t.Error("member join event timed out")
	case members := <-user1.MemberJoinCh:
		assert.Equal(t, []SerfMember{{Name: hostname, Addr: "127.0.0.1:7788", RdmaGroup: "region-a"}}, members, "member join")
	}
	assert.Equal(t, map[string]string{"127.0.0.1:7788": hostname}, svc.members, "members after join")

	// Broadcast a message
	err = ss.BroadCast("event2", []byte("event2 payload"))
	assert.NoError(t, err, "broadcast after start")

	// Broadcast non-registered event
	err = ss.BroadCast("event3", []byte("event3 payload"))
	assert.Equal(t, errors.New("Serf user event \"event3\" not registered"), err, "broadcast non-registered event name")

	ss.Stop()
}

func TestMemberHashKey(t *testing.T) {
	tests := []struct {
		name        string
		useNodeName bool
		memberName  string
		memberAddr  string
		want        string
	}{
		{
			name:        "addr mode - returns addr",
			useNodeName: false,
			memberName:  "node-1",
			memberAddr:  "10.0.0.1:9000",
			want:        "10.0.0.1:9000",
		},
		{
			name:        "node-name mode - returns name",
			useNodeName: true,
			memberName:  "node-1",
			memberAddr:  "10.0.0.1:9000",
			want:        "node-1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ss := &serfService{useNodeName: tt.useNodeName}
			got := ss.memberHashKey(tt.memberName, tt.memberAddr)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestAddDelMemberUpdatesNameMap(t *testing.T) {
	ss := &serfService{
		config:  &SerfConfig{HashRing: false},
		members: make(map[string]string),
	}

	ss.addMember("node-1", "10.0.0.1:9000")
	ss.addMember("node-2", "10.0.0.2:9000")
	assert.Equal(t, map[string]string{
		"10.0.0.1:9000": "node-1",
		"10.0.0.2:9000": "node-2",
	}, ss.members, "both members tracked")

	ss.delMember("node-1", "10.0.0.1:9000")
	assert.Equal(t, map[string]string{
		"10.0.0.2:9000": "node-2",
	}, ss.members, "member removed by addr")
}

func TestAddDelMemberHashring(t *testing.T) {
	tests := []struct {
		name        string
		useNodeName bool
		memberName  string
		memberAddr  string
		wantHashKey string
	}{
		{
			name:        "addr mode - ring key is addr",
			useNodeName: false,
			memberName:  "node-1",
			memberAddr:  "10.0.0.1:9000",
			wantHashKey: "10.0.0.1:9000",
		},
		{
			name:        "node-name mode - ring key is node name",
			useNodeName: true,
			memberName:  "node-1",
			memberAddr:  "10.0.0.1:9000",
			wantHashKey: "node-1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ss := &serfService{
				config:        &SerfConfig{HashRing: true},
				useNodeName:   tt.useNodeName,
				members:       make(map[string]string),
				hashring:      consistent.NewHashRing(VirtualNodeCount),
				hashKeyToAddr: make(map[string]string),
			}

			ss.addMember(tt.memberName, tt.memberAddr)
			assert.Equal(t, []string{tt.wantHashKey}, ss.hashring.Members(),
				"ring should contain correct key after add")
			if tt.useNodeName {
				assert.Equal(t, tt.memberAddr, ss.hashKeyToAddr[tt.wantHashKey],
					"reverse map should resolve ring key to member addr")
			}

			ss.delMember(tt.memberName, tt.memberAddr)
			assert.Empty(t, ss.hashring.Members(), "ring should be empty after del")
			assert.Empty(t, ss.hashKeyToAddr[tt.wantHashKey],
				"reverse map entry should be cleared after del")
		})
	}
}

func TestGetHashOwner(t *testing.T) {
	// newStarted builds a directly-constructed serfService that passes the
	// "service started" guard without a real serf cluster: GetHashOwner never
	// calls serfCluster methods, it only checks it is non-nil.
	newStarted := func(useNodeName, hashRing bool) *serfService {
		ss := &serfService{
			config:        &SerfConfig{HashRing: hashRing},
			useNodeName:   useNodeName,
			members:       make(map[string]string),
			hashKeyToAddr: make(map[string]string),
			stopCh:        make(chan struct{}),
			serfCluster:   &serf.Serf{},
		}
		if hashRing {
			ss.hashring = consistent.NewHashRing(VirtualNodeCount)
		}
		return ss
	}

	t.Run("not started returns error", func(t *testing.T) {
		ss := &serfService{config: &SerfConfig{HashRing: true}}
		hashKey, memAddr, err := ss.GetHashOwner("key")
		assert.Error(t, err)
		assert.Empty(t, hashKey)
		assert.Empty(t, memAddr)
	})

	t.Run("hash ring disabled returns error", func(t *testing.T) {
		ss := newStarted(false, false)
		_, _, err := ss.GetHashOwner("key")
		assert.Error(t, err)
	})

	t.Run("addr mode - hashKey equals memAddr", func(t *testing.T) {
		ss := newStarted(false, true)
		// Single member: the ring resolves any key to it deterministically.
		ss.addMember("node-1", "10.0.0.1:9000")
		hashKey, memAddr, err := ss.GetHashOwner("some-block-key")
		assert.NoError(t, err)
		assert.Equal(t, "10.0.0.1:9000", hashKey, "addr mode ring key is the addr")
		assert.Equal(t, "10.0.0.1:9000", memAddr, "addr mode resolves to same addr")
	})

	t.Run("node-name mode - resolves memAddr via reverse map", func(t *testing.T) {
		ss := newStarted(true, true)
		ss.addMember("node-a", "10.0.0.1:9000")
		hashKey, memAddr, err := ss.GetHashOwner("some-block-key")
		assert.NoError(t, err)
		assert.Equal(t, "node-a", hashKey, "node-name mode ring key is the node name")
		assert.Equal(t, "10.0.0.1:9000", memAddr, "reverse map resolves name to addr")
	})

	t.Run("node-name mode - unresolved key returns hashKey and error", func(t *testing.T) {
		ss := newStarted(true, true)
		// Orphan key on the ring with no reverse-map entry (e.g. a stale ring
		// state); serf should still report who the owner is but signal it can't
		// be resolved to a cache addr.
		ss.hashring.AddToHashring("orphan-node")
		hashKey, memAddr, err := ss.GetHashOwner("some-block-key")
		assert.Error(t, err)
		assert.Equal(t, "orphan-node", hashKey, "should still return the ring key")
		assert.Empty(t, memAddr, "unresolved owner has no addr")
	})
}

func TestSerfServiceNodeNameMode(t *testing.T) {
	cfg := newTestSerfConfig()
	cfg.NodeName = "my-node-1"

	ss := NewSerfService(cfg, nil)
	svc, ok := ss.(*serfService)
	assert.True(t, ok)
	assert.True(t, svc.useNodeName, "useNodeName should be true when NodeName is configured")

	err := ss.Start()
	assert.NoError(t, err, "start serf service in node-name mode")

	// Give it some time to handle the self member-join event
	time.Sleep(GossipInterval * 2)

	// localKey should be the node name, not ip:port
	assert.Equal(t, "my-node-1", svc.localKey, "localKey should be node name")

	// members should contain both name and addr for the node
	svc.memberMu.Lock()
	info, exists := svc.members["127.0.0.1:7788"]
	svc.memberMu.Unlock()
	assert.True(t, exists, "member entry should exist")
	assert.Equal(t, "my-node-1", info, "member entry should carry node name")

	// members should still track addr for MemberExists / RPC
	assert.True(t, ss.MemberExists("127.0.0.1:7788"), "addr-based MemberExists should still work")

	// GetMembers should still return addrs
	members := ss.GetMembers(false)
	assert.Equal(t, []string{"127.0.0.1:7788"}, members, "GetMembers should return addrs")

	// HashHit should use node name as hash key
	assert.True(t, ss.HashHit("test key"), "HashHit should succeed using node name")

	// updateHashRing in node-name mode should keep HashHit working
	svc.updateHashRing()
	assert.True(t, ss.HashHit("test key"), "HashHit should succeed after updateHashRing")

	ss.Stop()
}

func TestGetMemberRole(t *testing.T) {
	tests := []struct {
		name     string
		member   *serf.Member
		expected string
	}{
		{
			name: "member has role tag",
			member: &serf.Member{
				Tags: map[string]string{
					keySerfRole: "agent",
				},
			},
			expected: "agent",
		},
		{
			name: "member has no role tag",
			member: &serf.Member{
				Tags: map[string]string{
					"other": "value",
				},
			},
			expected: "",
		},
		{
			name: "member has empty role value",
			member: &serf.Member{
				Tags: map[string]string{
					keySerfRole: "",
				},
			},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getMemberRole(tt.member)
			if got != tt.expected {
				t.Errorf("getMemberRole() = %q, want %q", got, tt.expected)
			}
		})
	}
}
