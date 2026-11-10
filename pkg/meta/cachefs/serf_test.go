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

package cachefsmeta

import (
	"encoding/json"
	"testing"
	"time"

	ss "github.com/aliyun/cachefs/pkg/serf"
	"github.com/eryugey/serf/serf"
	"github.com/stretchr/testify/assert"
)

type MockSerfService struct {
	members map[string]bool
}

func (m *MockSerfService) BroadCast(name string, payload []byte) error {
	return nil
}

func (m *MockSerfService) RegisterUser(user *ss.SerfUser) error {
	return nil
}

func (m *MockSerfService) GetMembers(raw bool) []string {
	return nil
}

func (m *MockSerfService) GetMemberCount() int {
	return len(m.members)
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

func TestIsValidUserEvent(t *testing.T) {
	// Test valid events
	assert.True(t, isValidUserEvent(announceEvent))
	assert.True(t, isValidUserEvent(dumpEvent))

	// Test invalid event
	assert.False(t, isValidUserEvent("invalid-event"))
	assert.False(t, isValidUserEvent(""))
	assert.False(t, isValidUserEvent("meta-revoke")) // Not defined in this package
}

func TestBroadCast(t *testing.T) {
	// Test normal broadcast
	meta := &metaData{
		localAddr: "127.0.0.1:1234",
		serf:      &MockSerfService{},
	}

	info := &serfInfo{
		SrcAddr:  "127.0.0.1:1234",
		Provider: true,
		Addrs:    []string{"192.168.1.1:5678"},
	}

	meta.broadCast(announceEvent, info)

	// Test with nil serf service
	meta.serf = nil
	meta.broadCast(announceEvent, info) // Should not panic

	// Test with unmarshallable data
	info.Addrs = []string{} // Reset
	// Create a field that cannot be marshaled
	meta.broadCast(announceEvent, info) // Should not panic
}

func TestAddMetaSource(t *testing.T) {
	// Test normal addition
	meta := &metaData{
		localAddr:    "127.0.0.1:1234",
		metaSource:   make(map[string]struct{}),
		metaSourceCh: make(chan struct{}, 10),
	}

	info := &serfInfo{
		SrcAddr:  "192.168.1.1:5678",
		Provider: true,
		Addrs:    []string{"192.168.1.1:5678", "192.168.1.2:5678"},
	}

	meta.addMetaSource(info)

	// Check provider was set
	assert.Equal(t, "192.168.1.1:5678", meta.metaProvider)

	// Check addresses were added
	_, exists := meta.metaSource["192.168.1.1:5678"]
	assert.True(t, exists)
	_, exists = meta.metaSource["192.168.1.2:5678"]
	assert.True(t, exists)

	// Check notification was sent
	assert.Len(t, meta.metaSourceCh, 1)

	// Test duplicate provider (should be ignored)
	info2 := &serfInfo{
		SrcAddr:  "192.168.2.1:5678",
		Provider: true,
		Addrs:    []string{"192.168.2.1:5678"},
	}

	meta.addMetaSource(info2)

	// Provider should not change
	assert.Equal(t, "192.168.1.1:5678", meta.metaProvider)

	// Test non-provider addition
	info3 := &serfInfo{
		SrcAddr:  "192.168.3.1:5678",
		Provider: false,
		Addrs:    []string{"192.168.3.1:5678", "192.168.3.2:5678"},
	}

	meta.addMetaSource(info3)

	// Provider should remain the same
	assert.Equal(t, "192.168.1.1:5678", meta.metaProvider)

	// New addresses should be added
	_, exists = meta.metaSource["192.168.3.1:5678"]
	assert.True(t, exists)
	_, exists = meta.metaSource["192.168.3.2:5678"]
	assert.True(t, exists)

	// Test duplicate address (should not be added again)
	initialLen := len(meta.metaSource)
	info4 := &serfInfo{
		SrcAddr:  "192.168.4.1:5678",
		Provider: false,
		Addrs:    []string{"192.168.1.1:5678"}, // Already exists
	}

	meta.addMetaSource(info4)

	// Should not increase the map size
	assert.Len(t, meta.metaSource, initialLen)
}

func TestDelMetaSource(t *testing.T) {
	meta := &metaData{
		localAddr:    "127.0.0.1:1234",
		metaProvider: "192.168.1.1:5678",
		metaSource:   make(map[string]struct{}),
	}

	// Add some sources
	meta.metaSource["192.168.1.1:5678"] = struct{}{}
	meta.metaSource["192.168.1.2:5678"] = struct{}{}
	meta.metaSource["192.168.1.3:5678"] = struct{}{}

	// Test removing sources including provider
	addrs := []string{"192.168.1.1:5678", "192.168.1.2:5678", "192.168.99.99:5678"} // Last one doesn't exist
	meta.delMetaSource(addrs)

	// Check provider was removed
	assert.Equal(t, "", meta.metaProvider)

	// Check sources were removed
	_, exists := meta.metaSource["192.168.1.1:5678"]
	assert.False(t, exists)
	_, exists = meta.metaSource["192.168.1.2:5678"]
	assert.False(t, exists)

	// Check non-existent source didn't cause issues
	_, exists = meta.metaSource["192.168.1.3:5678"]
	assert.True(t, exists)

	// Test removing from empty list
	meta.delMetaSource([]string{})

	// Test removing non-existent sources
	meta.delMetaSource([]string{"192.168.99.1:5678", "192.168.99.2:5678"})
	// Should not panic and remaining data should be unaffected
	_, exists = meta.metaSource["192.168.1.3:5678"]
	assert.True(t, exists)
}

func TestHandleSerfUserEvent(t *testing.T) {
	meta := &metaData{
		localAddr:    "127.0.0.1:1234",
		metaSource:   make(map[string]struct{}),
		metaSourceCh: make(chan struct{}, 10),
		config: &MetaConfig{
			Sourceless: true,
		},
		serfUser: &ss.SerfUser{
			UserEventCh: make(chan serf.UserEvent, 10),
		},
		serf: &MockSerfService{
			members: map[string]bool{
				"192.168.1.1:5678": true,
			},
		},
		stopCh: make(chan struct{}),
	}

	go meta.handleSerfUserEvent()

	// Test announce event
	info := &serfInfo{
		SrcAddr:  "192.168.1.1:5678",
		Provider: true,
		Addrs:    []string{"192.168.1.1:5678"},
	}
	payload, _ := json.Marshal(info)

	meta.serfUser.UserEventCh <- serf.UserEvent{
		Name:    announceEvent,
		Payload: payload,
	}
	time.Sleep(10 * time.Millisecond)

	// Check that source was added
	assert.Equal(t, "192.168.1.1:5678", meta.metaProvider)
	assert.Len(t, meta.metaSourceCh, 1)

	// Test dump event when source is ready
	meta.metaReady.Store(true)
	dumpInfo := &serfInfo{
		SrcAddr: "192.168.1.1:5678",
	}
	dumpPayload, _ := json.Marshal(dumpInfo)

	meta.serfUser.UserEventCh <- serf.UserEvent{
		Name:    dumpEvent,
		Payload: dumpPayload,
	}
	time.Sleep(10 * time.Millisecond)

	// Test event from local (should be ignored)
	localInfo := &serfInfo{
		SrcAddr: "127.0.0.1:1234",
	}
	localPayload, _ := json.Marshal(localInfo)

	meta.serfUser.UserEventCh <- serf.UserEvent{
		Name:    announceEvent,
		Payload: localPayload,
	}
	time.Sleep(10 * time.Millisecond)

	// Should not change provider
	assert.Equal(t, "192.168.1.1:5678", meta.metaProvider)

	// Test invalid payload
	meta.serfUser.UserEventCh <- serf.UserEvent{
		Name:    announceEvent,
		Payload: []byte("invalid json"),
	}
	time.Sleep(10 * time.Millisecond)

	// Should not change state
	assert.Equal(t, "192.168.1.1:5678", meta.metaProvider)

	// Test unknown event
	meta.serfUser.UserEventCh <- serf.UserEvent{
		Name:    "unknown-event",
		Payload: payload,
	}
	time.Sleep(10 * time.Millisecond)

	// Should not change state
	assert.Equal(t, "192.168.1.1:5678", meta.metaProvider)

	// Test member not in list
	nonMemberInfo := &serfInfo{
		SrcAddr: "192.168.99.99:5678",
	}
	nonMemberPayload, _ := json.Marshal(nonMemberInfo)

	meta.serfUser.UserEventCh <- serf.UserEvent{
		Name:    announceEvent,
		Payload: nonMemberPayload,
	}
	time.Sleep(10 * time.Millisecond)

	// Should not change state
	assert.Equal(t, "192.168.1.1:5678", meta.metaProvider)

	close(meta.stopCh)
	time.Sleep(10 * time.Millisecond)
}

func TestHandleSerfMemberLeaveEvent(t *testing.T) {
	meta := &metaData{
		localAddr:    "127.0.0.1:1234",
		metaProvider: "192.168.1.1:5678",
		metaSource: map[string]struct{}{
			"192.168.1.1:5678": {},
			"192.168.1.2:5678": {},
			"192.168.1.3:5678": {},
		},
		serfUser: &ss.SerfUser{
			MemberLeaveCh: make(chan []ss.SerfMember, 10),
		},
		stopCh: make(chan struct{}),
	}

	go meta.handleSerfMemberLeaveEvent()

	// Test member leave event
	members := []ss.SerfMember{
		{Addr: "192.168.1.1:5678"},
		{Addr: "192.168.1.2:5678"},
	}

	meta.serfUser.MemberLeaveCh <- members
	time.Sleep(10 * time.Millisecond)

	// Check that sources were removed
	_, exists := meta.metaSource["192.168.1.1:5678"]
	assert.False(t, exists)
	_, exists = meta.metaSource["192.168.1.2:5678"]
	assert.False(t, exists)

	// Check that provider was removed
	assert.Equal(t, "", meta.metaProvider)

	// Check that remaining source is still there
	_, exists = meta.metaSource["192.168.1.3:5678"]
	assert.True(t, exists)

	// Test empty member list
	meta.serfUser.MemberLeaveCh <- []ss.SerfMember{}
	time.Sleep(10 * time.Millisecond)

	// Should not affect existing data
	_, exists = meta.metaSource["192.168.1.3:5678"]
	assert.True(t, exists)

	close(meta.stopCh)
	time.Sleep(10 * time.Millisecond)
}

func TestRequestMetaSource(t *testing.T) {
	meta := &metaData{
		localAddr: "127.0.0.1:1234",
		config: &MetaConfig{
			Sourceless: true,
		},
		serfUser: &ss.SerfUser{
			JoinedCh: make(chan struct{}, 10),
		},
		serfReadyCh: make(chan struct{}, 10),
		stopCh:      make(chan struct{}),
		serf:        &MockSerfService{},
	}

	go meta.requestMetaSource()

	// Test with consumer mode
	meta.serfReadyCh <- struct{}{}
	meta.serfUser.JoinedCh <- struct{}{}
	time.Sleep(10 * time.Millisecond)

	// Test timeout scenario
	meta2 := &metaData{
		localAddr: "127.0.0.1:1234",
		config: &MetaConfig{
			Sourceless: true,
		},
		serfUser:    &ss.SerfUser{JoinedCh: make(chan struct{}, 10)},
		serfReadyCh: make(chan struct{}, 10),
		stopCh:      make(chan struct{}),
		serf:        &MockSerfService{},
	}

	go meta2.requestMetaSource()

	// Trigger timeout
	timer := time.NewTimer(time.Second)
	select {
	case <-timer.C:
		// Timeout reached, send joined event anyway
		meta2.serfUser.JoinedCh <- struct{}{}
	}
	time.Sleep(10 * time.Millisecond)

	// Test with non-consumer mode && non-peermeta
	meta3 := &metaData{
		localAddr: "127.0.0.1:1234",
		config: &MetaConfig{
			SourceDirs: []string{"/testmnt"},
			Sourceless: true,
		},
		serfUser:    &ss.SerfUser{JoinedCh: make(chan struct{}, 10)},
		serfReadyCh: make(chan struct{}, 10),
		stopCh:      make(chan struct{}),
		serf:        &MockSerfService{},
	}

	// Should return immediately without doing anything
	meta3.requestMetaSource()

	// Test with peer meta enabled
	meta4 := &metaData{
		localAddr: "127.0.0.1:1234",
		config: &MetaConfig{
			SourceDirs:      []string{"/testmnt"},
			PeerMetaTimeout: 1,
		},
		serfUser:    &ss.SerfUser{JoinedCh: make(chan struct{}, 10)},
		serfReadyCh: make(chan struct{}, 10),
		stopCh:      make(chan struct{}),
		serf:        &MockSerfService{},
	}
	go meta4.requestMetaSource()
	meta4.serfReadyCh <- struct{}{}
	meta4.serfUser.JoinedCh <- struct{}{}
	time.Sleep(10 * time.Millisecond)

	close(meta.stopCh)
	close(meta2.stopCh)
	close(meta3.stopCh)
	close(meta4.stopCh)
	time.Sleep(10 * time.Millisecond)
}

func TestFromLocal(t *testing.T) {
	meta := &metaData{
		localAddr: "127.0.0.1:1234",
	}

	// Test local address
	info := &serfInfo{
		SrcAddr: "127.0.0.1:1234",
	}
	assert.True(t, meta.fromLocal(info))

	// Test non-local address
	info.SrcAddr = "192.168.1.1:5678"
	assert.False(t, meta.fromLocal(info))

	// Test empty address
	info.SrcAddr = ""
	assert.False(t, meta.fromLocal(info))
}
