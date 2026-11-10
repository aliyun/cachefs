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

	"github.com/eryugey/serf/serf"
	"github.com/stretchr/testify/assert"
)

func TestNewUserEventCoalescer(t *testing.T) {
	// Test creating new user event coalescer
	serfService := &MockSerfService{}
	coalescer := newUserEventCoalescer("127.0.0.1:1234", serfService)

	assert.NotNil(t, coalescer)

	// Check that the coalescer has correct initial values
	coalescerImpl := coalescer.(*userEventCoalescer)
	assert.Equal(t, "127.0.0.1:1234", coalescerImpl.localAddr)
	assert.Equal(t, serfService, coalescerImpl.serf)
	assert.Equal(t, 0, coalescerImpl.announceCount)
	assert.Equal(t, 0, coalescerImpl.dumpCount)
	assert.Nil(t, coalescerImpl.announce)
	assert.Nil(t, coalescerImpl.dump)
}

func TestUserEventCoalescerFromLocal(t *testing.T) {
	coalescer := &userEventCoalescer{
		localAddr: "127.0.0.1:1234",
	}

	// Test local address
	info := &serfInfo{
		SrcAddr: "127.0.0.1:1234",
	}
	assert.True(t, coalescer.fromLocal(info))

	// Test non-local address
	info.SrcAddr = "192.168.1.1:5678"
	assert.False(t, coalescer.fromLocal(info))

	// Test empty address
	info.SrcAddr = ""
	assert.False(t, coalescer.fromLocal(info))
}

func TestUserEventCoalescerHandle(t *testing.T) {
	coalescer := &userEventCoalescer{}

	// Test valid events
	validEvents := []string{announceEvent, dumpEvent}
	for _, eventName := range validEvents {
		event := serf.UserEvent{
			Name: eventName,
		}
		assert.True(t, coalescer.Handle(event))
	}

	// Test invalid event
	event := serf.UserEvent{
		Name: "invalid-event",
	}
	assert.False(t, coalescer.Handle(event))

	// Test empty event name
	event.Name = ""
	assert.False(t, coalescer.Handle(event))
}

func TestUserEventCoalescerCoalesce(t *testing.T) {
	coalescer := &userEventCoalescer{
		localAddr: "127.0.0.1:1234",
		serf: &MockSerfService{
			members: map[string]bool{
				"192.168.1.1:5678": true,
				"192.168.1.2:5678": true,
				"192.168.1.3:5678": true,
				"192.168.2.1:5678": true,
				"192.168.2.2:5678": true,
			},
		},
	}

	// Test announce event coalescing
	info1 := &serfInfo{
		SrcAddr:  "192.168.1.1:5678",
		Provider: false,
		Addrs:    []string{},
	}
	payload1, _ := json.Marshal(info1)

	event1 := serf.UserEvent{
		Name:    announceEvent,
		Payload: payload1,
		LTime:   10,
	}

	coalescer.Coalesce(event1)

	// Check state after first announce
	assert.Equal(t, 1, coalescer.announceCount)
	assert.Equal(t, serf.LamportTime(10), coalescer.announceLTime)
	assert.NotNil(t, coalescer.announce)
	assert.Equal(t, "192.168.1.1:5678", coalescer.announce.SrcAddr)

	// Test second announce event (non-provider)
	info2 := &serfInfo{
		SrcAddr:  "192.168.1.2:5678",
		Provider: false,
		Addrs:    []string{},
	}
	payload2, _ := json.Marshal(info2)

	event2 := serf.UserEvent{
		Name:    announceEvent,
		Payload: payload2,
		LTime:   15,
	}

	coalescer.Coalesce(event2)

	// Check state after second announce
	assert.Equal(t, 2, coalescer.announceCount)
	assert.Equal(t, serf.LamportTime(15), coalescer.announceLTime)  // Should be updated to higher LTime
	assert.Equal(t, "192.168.1.1:5678", coalescer.announce.SrcAddr) // Should remain the first one
	assert.Equal(t, []string{"192.168.1.1:5678", "192.168.1.2:5678"}, coalescer.announce.Addrs)

	// Test provider announce event
	info3 := &serfInfo{
		SrcAddr:  "192.168.1.3:5678",
		Provider: true,
		Addrs:    []string{},
	}
	payload3, _ := json.Marshal(info3)

	event3 := serf.UserEvent{
		Name:    announceEvent,
		Payload: payload3,
		LTime:   20,
	}

	coalescer.Coalesce(event3)

	// Check state after provider announce
	assert.Equal(t, 3, coalescer.announceCount)
	assert.Equal(t, serf.LamportTime(20), coalescer.announceLTime)
	assert.Equal(t, "192.168.1.3:5678", coalescer.announce.SrcAddr)
	assert.Equal(t, []string{"192.168.1.1:5678", "192.168.1.2:5678"}, coalescer.announce.Addrs)

	// Test dump event coalescing
	dumpInfo := &serfInfo{
		SrcAddr: "192.168.2.1:5678",
	}
	dumpPayload, _ := json.Marshal(dumpInfo)

	dump := serf.UserEvent{
		Name:    dumpEvent,
		Payload: dumpPayload,
		LTime:   25,
	}

	coalescer.Coalesce(dump)

	// Check state after dump
	assert.Equal(t, 1, coalescer.dumpCount)
	assert.Equal(t, serf.LamportTime(25), coalescer.dumpLTime)
	assert.NotNil(t, coalescer.dump)
	assert.Equal(t, "192.168.2.1:5678", coalescer.dump.SrcAddr)

	// Test second dump event with earlier LTime
	dumpInfo2 := &serfInfo{
		SrcAddr: "192.168.2.2:5678",
	}
	dumpPayload2, _ := json.Marshal(dumpInfo2)

	dumpEvent2 := serf.UserEvent{
		Name:    dumpEvent,
		Payload: dumpPayload2,
		LTime:   15, // Earlier than previous
	}

	coalescer.Coalesce(dumpEvent2)

	// Check state after second dump
	assert.Equal(t, 2, coalescer.dumpCount)
	assert.Equal(t, serf.LamportTime(25), coalescer.dumpLTime)  // Should remain unchanged
	assert.Equal(t, "192.168.2.1:5678", coalescer.dump.SrcAddr) // Should remain unchanged

	// Test event from local (should be ignored)
	localInfo := &serfInfo{
		SrcAddr: "127.0.0.1:1234",
	}
	localPayload, _ := json.Marshal(localInfo)

	localEvent := serf.UserEvent{
		Name:    announceEvent,
		Payload: localPayload,
		LTime:   30,
	}

	// State should remain unchanged
	prevAnnounceCount := coalescer.announceCount
	prevDumpCount := coalescer.dumpCount

	coalescer.Coalesce(localEvent)

	assert.Equal(t, prevAnnounceCount, coalescer.announceCount)
	assert.Equal(t, prevDumpCount, coalescer.dumpCount)

	// Test invalid payload (should not panic)
	invalidEvent := serf.UserEvent{
		Name:    announceEvent,
		Payload: []byte("invalid json"),
		LTime:   30,
	}

	coalescer.Coalesce(invalidEvent)

	// Counts should remain unchanged
	assert.Equal(t, prevAnnounceCount, coalescer.announceCount)
	assert.Equal(t, prevDumpCount, coalescer.dumpCount)

	// Test event from non-member: MemberExists check is deferred to Flush(), so
	// Coalesce() should still accumulate the event even if the sender is not yet
	// in the member list (e.g., its member-join event shares the same CoalescePeriod).
	nonMemberInfo := &serfInfo{
		SrcAddr: "192.168.99.99:5678",
	}
	nonMemberPayload, _ := json.Marshal(nonMemberInfo)

	nonMemberEvent := serf.UserEvent{
		Name:    announceEvent,
		Payload: nonMemberPayload,
		LTime:   30,
	}

	coalescer.Coalesce(nonMemberEvent)

	// announceCount should increase because the check is now deferred to Flush()
	assert.Equal(t, prevAnnounceCount+1, coalescer.announceCount)
	assert.Equal(t, prevDumpCount, coalescer.dumpCount)
}

func TestUserEventCoalescerFlush(t *testing.T) {
	allMembers := &MockSerfService{
		members: map[string]bool{
			"192.168.1.1:5678": true,
			"192.168.2.1:5678": true,
		},
	}
	partialMembers := &MockSerfService{
		members: map[string]bool{
			// announce sender (192.168.1.1) has left; dump sender is still alive
			"192.168.2.1:5678": true,
		},
	}

	tests := []struct {
		name          string
		coalescer     *userEventCoalescer
		wantEvents    int
		wantEventName string // expected name of the single event when wantEvents == 1
	}{
		{
			name: "both members alive forwards all events",
			coalescer: &userEventCoalescer{
				announceCount: 3,
				dumpCount:     2,
				announceLTime: 15,
				dumpLTime:     25,
				serf:          allMembers,
				announce: &serfInfo{
					SrcAddr:  "192.168.1.1:5678",
					Provider: true,
					Addrs:    []string{"192.168.1.2:5678"},
				},
				dump: &serfInfo{SrcAddr: "192.168.2.1:5678"},
			},
			wantEvents: 2,
		},
		{
			name: "all members alive forwards all events",
			coalescer: &userEventCoalescer{
				announceCount: 1,
				dumpCount:     1,
				announceLTime: 10,
				dumpLTime:     20,
				serf:          allMembers,
				announce:      &serfInfo{SrcAddr: "192.168.1.1:5678"},
				dump:          &serfInfo{SrcAddr: "192.168.2.1:5678"},
			},
			wantEvents: 2,
		},
		{
			// Verify that events from members that left before the CoalescePeriod
			// window expired are suppressed at Flush time.
			name: "left member event is filtered at flush",
			coalescer: &userEventCoalescer{
				announceCount: 1,
				dumpCount:     1,
				announceLTime: 10,
				dumpLTime:     20,
				serf:          partialMembers,
				announce:      &serfInfo{SrcAddr: "192.168.1.1:5678"}, // left
				dump:          &serfInfo{SrcAddr: "192.168.2.1:5678"}, // alive
			},
			wantEvents:    1,
			wantEventName: dumpEvent,
		},
		{
			name: "nil announce and dump sends nothing",
			coalescer: &userEventCoalescer{
				serf: allMembers,
			},
			wantEvents: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			outChan := make(chan serf.Event, 10)
			tc.coalescer.Flush(outChan)
			assert.Len(t, outChan, tc.wantEvents)
			if tc.wantEventName != "" && len(outChan) > 0 {
				evt := (<-outChan).(serf.UserEvent)
				assert.Equal(t, tc.wantEventName, evt.Name)
			}
		})
	}

	// Verify counters are reset after Flush
	c := &userEventCoalescer{
		announceCount: 3,
		dumpCount:     2,
		announceLTime: 15,
		dumpLTime:     25,
		serf:          allMembers,
		announce:      &serfInfo{SrcAddr: "192.168.1.1:5678"},
		dump:          &serfInfo{SrcAddr: "192.168.2.1:5678"},
	}
	c.Flush(make(chan serf.Event, 10))
	assert.Equal(t, 0, c.announceCount)
	assert.Equal(t, 0, c.dumpCount)
	assert.Equal(t, serf.LamportTime(0), c.announceLTime)
	assert.Equal(t, serf.LamportTime(0), c.dumpLTime)
	assert.Nil(t, c.announce)
	assert.Nil(t, c.dump)
}
