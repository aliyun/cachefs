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
	"encoding/json"
	"syscall"
	"testing"
	"time"

	ss "github.com/aliyun/cachefs/pkg/serf"
	"github.com/eryugey/serf/serf"
	"github.com/stretchr/testify/assert"
)

type MockSerfService struct {
	members       map[string]bool
	cachedMembers []string
	rawMembers    []string
	memberCount   int
	broadcastErr  error
	lastBroadcast struct {
		name    string
		payload []byte
	}
}

func (m *MockSerfService) BroadCast(name string, payload []byte) error {
	m.lastBroadcast.name = name
	m.lastBroadcast.payload = payload
	return m.broadcastErr
}

func (m *MockSerfService) RegisterUser(user *ss.SerfUser) error {
	return nil
}

func (m *MockSerfService) GetMembers(raw bool) []string {
	if raw {
		return m.rawMembers
	}
	return m.cachedMembers
}

func (m *MockSerfService) GetMemberCount() int {
	if m.memberCount > 0 {
		return m.memberCount
	}
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
	tests := []struct {
		name      string
		eventName string
		want      bool
	}{
		{
			name:      "valid preload event",
			eventName: preloadEvent,
			want:      true,
		},
		{
			name:      "invalid event",
			eventName: "invalid-event",
			want:      false,
		},
		{
			name:      "empty event name",
			eventName: "",
			want:      false,
		},
		{
			name:      "unknown event",
			eventName: "unknown",
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidUserEvent(tt.eventName)
			assert.Equal(t, tt.want, got, "isValidUserEvent(%q) = %v, want %v", tt.eventName, got, tt.want)
		})
	}
}

func Test_BroadCast(t *testing.T) {
	tests := []struct {
		name        string
		setupCache  func() *Cachefs
		typ         string
		info        *serfInfo
		wantErr     bool
		expectedErr error
	}{
		{
			name: "successful broadcast",
			setupCache: func() *Cachefs {
				return &Cachefs{
					serf: &MockSerfService{},
				}
			},
			typ: preloadEvent,
			info: &serfInfo{
				SrcAddr: "192.168.1.1:5678",
				Paths:   []string{"/test"},
				LoadAll: true,
				Force:   false,
			},
			wantErr: false,
		},
		{
			name: "nil serf service",
			setupCache: func() *Cachefs {
				return &Cachefs{
					serf: nil,
				}
			},
			typ: preloadEvent,
			info: &serfInfo{
				SrcAddr: "192.168.1.1:5678",
				Paths:   []string{"/test"},
			},
			wantErr:     true,
			expectedErr: syscall.EAGAIN,
		},
		{
			name: "broadcast failure",
			setupCache: func() *Cachefs {
				return &Cachefs{
					serf: &MockSerfService{
						broadcastErr: syscall.EIO,
					},
				}
			},
			typ: preloadEvent,
			info: &serfInfo{
				SrcAddr: "192.168.1.1:5678",
				Paths:   []string{"/test"},
			},
			wantErr:     true,
			expectedErr: syscall.EIO,
		},
		{
			name: "empty paths",
			setupCache: func() *Cachefs {
				return &Cachefs{
					serf: &MockSerfService{},
				}
			},
			typ: preloadEvent,
			info: &serfInfo{
				SrcAddr: "192.168.1.1:5678",
				Paths:   []string{},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := tt.setupCache()
			err := cache.broadCast(tt.typ, tt.info)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.expectedErr != nil {
					assert.Equal(t, tt.expectedErr, err)
				}
			} else {
				assert.NoError(t, err)
				// Verify broadcast was called with correct data
				if cache.serf != nil {
					mockSerf := cache.serf.(*MockSerfService)
					assert.Equal(t, tt.typ, mockSerf.lastBroadcast.name)
					// Verify payload can be unmarshaled
					var info serfInfo
					err := json.Unmarshal(mockSerf.lastBroadcast.payload, &info)
					assert.NoError(t, err)
					assert.Equal(t, tt.info.SrcAddr, info.SrcAddr)
					assert.Equal(t, tt.info.Paths, info.Paths)
				}
			}
		})
	}
}

func Test_FromLocal(t *testing.T) {
	tests := []struct {
		name  string
		cache *Cachefs
		info  *serfInfo
		want  bool
	}{
		{
			name: "local address matches",
			cache: &Cachefs{
				serf: &MockSerfService{},
			},
			info: &serfInfo{
				SrcAddr: "127.0.0.1:1234",
			},
			want: true,
		},
		{
			name: "remote address",
			cache: &Cachefs{
				serf: &MockSerfService{},
			},
			info: &serfInfo{
				SrcAddr: "192.168.1.1:5678",
			},
			want: false,
		},
		{
			name: "nil serf service",
			cache: &Cachefs{
				serf: nil,
			},
			info: &serfInfo{
				SrcAddr: "127.0.0.1:1234",
			},
			want: false,
		},
		{
			name: "empty address",
			cache: &Cachefs{
				serf: &MockSerfService{},
			},
			info: &serfInfo{
				SrcAddr: "",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cache.fromLocal(tt.info)
			assert.Equal(t, tt.want, got, "fromLocal() = %v, want %v", got, tt.want)
		})
	}
}

func Test_HandleSerfUserEvent(t *testing.T) {
	tests := []struct {
		name        string
		setupCache  func() *Cachefs
		sendEvents  func(*Cachefs)
		checkResult func(*testing.T, *Cachefs)
		waitTime    time.Duration
	}{
		{
			name: "nil serfUser - returns immediately",
			setupCache: func() *Cachefs {
				return &Cachefs{
					serfUser: nil,
				}
			},
			sendEvents: func(c *Cachefs) {
				// No events to send
			},
			checkResult: func(t *testing.T, c *Cachefs) {
				// Should return immediately without panic
			},
			waitTime: 10 * time.Millisecond,
		},
		{
			name: "event from local - ignored",
			setupCache: func() *Cachefs {
				cache := &Cachefs{
					serf: &MockSerfService{
						members: map[string]bool{
							"127.0.0.1:1234": true,
						},
					},
					serfUser: &ss.SerfUser{
						UserEventCh: make(chan serf.UserEvent, 10),
					},
				}
				return cache
			},
			sendEvents: func(c *Cachefs) {
				info := &serfInfo{
					SrcAddr: "127.0.0.1:1234", // Local address
					Paths:   []string{"/test"},
				}
				payload, _ := json.Marshal(info)
				c.serfUser.UserEventCh <- serf.UserEvent{
					Name:    preloadEvent,
					Payload: payload,
				}
			},
			checkResult: func(t *testing.T, c *Cachefs) {
				// Event should be ignored
			},
			waitTime: 50 * time.Millisecond,
		},
		{
			name: "event from non-member - ignored",
			setupCache: func() *Cachefs {
				cache := &Cachefs{
					serf: &MockSerfService{
						members: map[string]bool{
							"192.168.1.1:5678": true,
						},
					},
					serfUser: &ss.SerfUser{
						UserEventCh: make(chan serf.UserEvent, 10),
					},
				}
				return cache
			},
			sendEvents: func(c *Cachefs) {
				info := &serfInfo{
					SrcAddr: "192.168.99.99:5678", // Not in member list
					Paths:   []string{"/test"},
				}
				payload, _ := json.Marshal(info)
				c.serfUser.UserEventCh <- serf.UserEvent{
					Name:    preloadEvent,
					Payload: payload,
				}
			},
			checkResult: func(t *testing.T, c *Cachefs) {
				// Event should be ignored
			},
			waitTime: 50 * time.Millisecond,
		},
		{
			name: "invalid event name - ignored",
			setupCache: func() *Cachefs {
				cache := &Cachefs{
					serf: &MockSerfService{},
					serfUser: &ss.SerfUser{
						UserEventCh: make(chan serf.UserEvent, 10),
					},
				}
				return cache
			},
			sendEvents: func(c *Cachefs) {
				info := &serfInfo{
					SrcAddr: "192.168.1.1:5678",
					Paths:   []string{"/test"},
				}
				payload, _ := json.Marshal(info)
				c.serfUser.UserEventCh <- serf.UserEvent{
					Name:    "invalid-event",
					Payload: payload,
				}
			},
			checkResult: func(t *testing.T, c *Cachefs) {
				// Event should be ignored
			},
			waitTime: 50 * time.Millisecond,
		},
		{
			name: "nil payload - ignored",
			setupCache: func() *Cachefs {
				cache := &Cachefs{
					serf: &MockSerfService{},
					serfUser: &ss.SerfUser{
						UserEventCh: make(chan serf.UserEvent, 10),
					},
				}
				return cache
			},
			sendEvents: func(c *Cachefs) {
				c.serfUser.UserEventCh <- serf.UserEvent{
					Name:    preloadEvent,
					Payload: nil,
				}
			},
			checkResult: func(t *testing.T, c *Cachefs) {
				// Event should be ignored
			},
			waitTime: 50 * time.Millisecond,
		},
		{
			name: "invalid JSON payload - ignored",
			setupCache: func() *Cachefs {
				cache := &Cachefs{
					serf: &MockSerfService{},
					serfUser: &ss.SerfUser{
						UserEventCh: make(chan serf.UserEvent, 10),
					},
				}
				return cache
			},
			sendEvents: func(c *Cachefs) {
				c.serfUser.UserEventCh <- serf.UserEvent{
					Name:    preloadEvent,
					Payload: []byte("invalid json"),
				}
			},
			checkResult: func(t *testing.T, c *Cachefs) {
				// Event should be ignored
			},
			waitTime: 50 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := tt.setupCache()

			if cache.serfUser != nil {
				// Run handler in goroutine
				done := make(chan struct{})
				go func() {
					defer func() {
						if r := recover(); r != nil {
							t.Errorf("handleSerfUserEvent panicked: %v", r)
						}
						close(done)
					}()
					// Run for a short time
					timeout := time.After(tt.waitTime)
					go cache.handleSerfUserEvent()
					<-timeout
				}()

				// Send events
				tt.sendEvents(cache)

				// Wait for processing
				time.Sleep(tt.waitTime)
			} else {
				// For nil serfUser case, just call the function
				cache.handleSerfUserEvent()
			}

			// Check results
			tt.checkResult(t, cache)
		})
	}
}

func TestSerfInfo_Marshaling(t *testing.T) {
	tests := []struct {
		name string
		info *serfInfo
	}{
		{
			name: "complete info",
			info: &serfInfo{
				SrcAddr: "192.168.1.1:5678",
				Paths:   []string{"/test", "/test2"},
				LoadAll: true,
				Force:   false,
			},
		},
		{
			name: "minimal info",
			info: &serfInfo{
				SrcAddr: "127.0.0.1:1234",
				Paths:   []string{},
			},
		},
		{
			name: "single path",
			info: &serfInfo{
				SrcAddr: "10.0.0.1:9999",
				Paths:   []string{"/"},
				LoadAll: false,
				Force:   true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal
			data, err := json.Marshal(tt.info)
			assert.NoError(t, err)
			assert.NotEmpty(t, data)

			// Unmarshal
			var decoded serfInfo
			err = json.Unmarshal(data, &decoded)
			assert.NoError(t, err)

			// Compare
			assert.Equal(t, tt.info.SrcAddr, decoded.SrcAddr)
			assert.Equal(t, tt.info.Paths, decoded.Paths)
			assert.Equal(t, tt.info.LoadAll, decoded.LoadAll)
			assert.Equal(t, tt.info.Force, decoded.Force)
		})
	}
}
