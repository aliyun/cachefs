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

package cachefsmeta

import (
	"encoding/json"
	"testing"

	"github.com/eryugey/serf/serf"
	"github.com/stretchr/testify/assert"

	ss "github.com/aliyun/cachefs/pkg/serf"
)

func TestMetaEventFilter(t *testing.T) {
	liveAddr := "192.168.1.1:5678"
	liveAddr2 := "192.168.2.2:5678"
	deadAddr := "192.168.99.99:5678"

	mockSerf := &MockSerfService{
		members: map[string]bool{
			liveAddr:  true,
			liveAddr2: true,
		},
	}

	announcePayload := func(srcAddr string) []byte {
		info := &serfInfo{SrcAddr: srcAddr}
		b, _ := json.Marshal(info)
		return b
	}

	tests := []struct {
		name       string
		input      []*serf.BufferedUserEvents
		wantLen    int
		wantLTimes []serf.LamportTime
		wantEvents [][]string
	}{
		{
			name:    "empty buffer returns empty result",
			input:   []*serf.BufferedUserEvents{},
			wantLen: 0,
		},
		{
			name:       "nil entries are skipped",
			input:      []*serf.BufferedUserEvents{nil, makeMetaEntry(1, "other-event"), nil},
			wantLen:    1,
			wantLTimes: []serf.LamportTime{1},
		},
		{
			name:    "dumpEvent is always removed",
			input:   []*serf.BufferedUserEvents{makeMetaEntry(1, dumpEvent)},
			wantLen: 0,
		},
		{
			name:       "announceEvent from live member is retained",
			input:      []*serf.BufferedUserEvents{makeMetaEntryWithPayload(1, announceEvent, announcePayload(liveAddr))},
			wantLen:    1,
			wantEvents: [][]string{{announceEvent}},
		},
		{
			name:    "announceEvent from dead member is removed",
			input:   []*serf.BufferedUserEvents{makeMetaEntryWithPayload(1, announceEvent, announcePayload(deadAddr))},
			wantLen: 0,
		},
		{
			name:       "other module events are retained",
			input:      []*serf.BufferedUserEvents{makeMetaEntry(1, "other-module-event")},
			wantLen:    1,
			wantEvents: [][]string{{"other-module-event"}},
		},
		{
			name: "mixed entry: dumpEvent removed, dead announceEvent removed, live announceEvent and others retained",
			input: []*serf.BufferedUserEvents{
				makeMetaEntryWithPayloadAndNames(1,
					[]string{dumpEvent, announceEvent, "other-event", announceEvent},
					[][]byte{nil, announcePayload(deadAddr), nil, announcePayload(liveAddr)},
				),
			},
			wantLen:    1,
			wantLTimes: []serf.LamportTime{1},
			wantEvents: [][]string{{"other-event", announceEvent}},
		},
		{
			name: "entry dropped when all its events are filtered",
			input: []*serf.BufferedUserEvents{
				makeMetaEntryWithPayloadAndNames(1,
					[]string{dumpEvent, announceEvent},
					[][]byte{nil, announcePayload(deadAddr)},
				),
				makeMetaEntry(2, "other-event"),
			},
			wantLen:    1,
			wantLTimes: []serf.LamportTime{2},
		},
		{
			name: "LTime of retained entries is preserved",
			input: []*serf.BufferedUserEvents{
				makeMetaEntry(42, "other-event"),
				makeMetaEntry(99, "another-event"),
			},
			wantLen:    2,
			wantLTimes: []serf.LamportTime{42, 99},
		},
		{
			name:    "announceEvent with malformed payload is removed",
			input:   []*serf.BufferedUserEvents{makeMetaEntryWithPayload(1, announceEvent, []byte("invalid json"))},
			wantLen: 0,
		},
		{
			name:    "announceEvent with nil payload is removed",
			input:   []*serf.BufferedUserEvents{makeMetaEntryWithPayload(1, announceEvent, nil)},
			wantLen: 0,
		},
		{
			// Two entries with the same SrcAddr: only the first entry (LTime=1) is kept.
			name: "duplicate announces from same SrcAddr deduped, first occurrence wins",
			input: []*serf.BufferedUserEvents{
				makeMetaEntryWithPayload(1, announceEvent, announcePayload(liveAddr)),
				makeMetaEntryWithPayload(2, announceEvent, announcePayload(liveAddr)),
			},
			wantLen:    1,
			wantLTimes: []serf.LamportTime{1},
			wantEvents: [][]string{{announceEvent}},
		},
		{
			// Two events in the same entry with the same SrcAddr: first one wins.
			name: "duplicate announces within same entry deduped, first event wins",
			input: []*serf.BufferedUserEvents{
				makeMetaEntryWithPayloadAndNames(1,
					[]string{announceEvent, announceEvent},
					[][]byte{announcePayload(liveAddr), announcePayload(liveAddr)},
				),
			},
			wantLen:    1,
			wantLTimes: []serf.LamportTime{1},
			wantEvents: [][]string{{announceEvent}},
		},
		{
			// Different SrcAddrs are independent: both are retained.
			name: "announces from different SrcAddrs both retained",
			input: []*serf.BufferedUserEvents{
				makeMetaEntryWithPayload(1, announceEvent, announcePayload(liveAddr)),
				makeMetaEntryWithPayload(2, announceEvent, announcePayload(liveAddr2)),
			},
			wantLen:    2,
			wantLTimes: []serf.LamportTime{1, 2},
		},
	}

	f := newMetaEventFilter(mockSerf)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := f.Filter(tt.input)
			assert.Len(t, got, tt.wantLen)
			for i, lt := range tt.wantLTimes {
				assert.Equal(t, lt, got[i].LTime)
			}
			for i, names := range tt.wantEvents {
				assert.Equal(t, names, metaEventNames(got[i]))
			}
		})
	}
}

func TestMetaEventFilterNilSerf(t *testing.T) {
	announcePayload := func(srcAddr string) []byte {
		info := &serfInfo{SrcAddr: srcAddr}
		b, _ := json.Marshal(info)
		return b
	}

	// With nil serf, announceEvent is never filtered (filterAnnounce returns false)
	f := newMetaEventFilter(nil)
	input := []*serf.BufferedUserEvents{
		makeMetaEntryWithPayload(1, announceEvent, announcePayload("192.168.99.99:5678")),
		makeMetaEntry(2, dumpEvent),
	}
	got := f.Filter(input)
	assert.Len(t, got, 1)
	assert.Equal(t, serf.LamportTime(1), got[0].LTime)
	assert.Equal(t, []string{announceEvent}, metaEventNames(got[0]))
}

// makeMetaEntry builds a BufferedUserEvents with the given LTime and event
// names (no payloads).
func makeMetaEntry(ltime serf.LamportTime, names ...string) *serf.BufferedUserEvents {
	events := make([]serf.BufferedUserEvent, len(names))
	for i, n := range names {
		events[i] = serf.BufferedUserEvent{Name: n}
	}
	return &serf.BufferedUserEvents{LTime: ltime, Events: events}
}

// makeMetaEntryWithPayload builds a single-event BufferedUserEvents with the
// given LTime, event name, and payload.
func makeMetaEntryWithPayload(ltime serf.LamportTime, name string, payload []byte) *serf.BufferedUserEvents {
	return &serf.BufferedUserEvents{
		LTime: ltime,
		Events: []serf.BufferedUserEvent{
			{Name: name, Payload: payload},
		},
	}
}

// makeMetaEntryWithPayloadAndNames builds a BufferedUserEvents with the given
// LTime and per-event names and payloads.
func makeMetaEntryWithPayloadAndNames(ltime serf.LamportTime, names []string, payloads [][]byte) *serf.BufferedUserEvents {
	events := make([]serf.BufferedUserEvent, len(names))
	for i, n := range names {
		events[i] = serf.BufferedUserEvent{Name: n}
		if i < len(payloads) && payloads[i] != nil {
			events[i].Payload = payloads[i]
		}
	}
	return &serf.BufferedUserEvents{LTime: ltime, Events: events}
}

// metaEventNames extracts the event names from a BufferedUserEvents entry.
func metaEventNames(entry *serf.BufferedUserEvents) []string {
	names := make([]string, len(entry.Events))
	for i, ev := range entry.Events {
		names[i] = ev.Name
	}
	return names
}

func TestFilterAnnounce(t *testing.T) {
	liveAddr := "192.168.1.1:5678"
	deadAddr := "192.168.99.99:5678"

	mockSerf := &MockSerfService{
		members: map[string]bool{
			liveAddr: true,
		},
	}

	validPayload := func(addr string) []byte {
		b, _ := json.Marshal(&serfInfo{SrcAddr: addr})
		return b
	}

	tests := []struct {
		name    string
		serf    ss.SerfService
		payload []byte
		want    bool // true = remove
	}{
		{
			name:    "nil serf always keeps the event",
			serf:    nil,
			payload: validPayload(deadAddr),
			want:    false,
		},
		{
			name:    "live member is kept",
			serf:    mockSerf,
			payload: validPayload(liveAddr),
			want:    false,
		},
		{
			name:    "dead member is removed",
			serf:    mockSerf,
			payload: validPayload(deadAddr),
			want:    true,
		},
		{
			name:    "malformed payload is removed",
			serf:    mockSerf,
			payload: []byte("not-json"),
			want:    true,
		},
		{
			name:    "nil payload is removed",
			serf:    mockSerf,
			payload: nil,
			want:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newMetaEventFilter(tt.serf)
			assert.Equal(t, tt.want, f.filterAnnounce(tt.payload))
		})
	}
}

func TestSrcAddrFromPayload(t *testing.T) {
	f := newMetaEventFilter(nil)

	validPayload := func(addr string) []byte {
		b, _ := json.Marshal(&serfInfo{SrcAddr: addr})
		return b
	}

	tests := []struct {
		name    string
		payload []byte
		want    string
	}{
		{
			name:    "valid payload returns SrcAddr",
			payload: validPayload("192.168.1.1:5678"),
			want:    "192.168.1.1:5678",
		},
		{
			name:    "malformed payload returns empty string",
			payload: []byte("not-json"),
			want:    "",
		},
		{
			name:    "nil payload returns empty string",
			payload: nil,
			want:    "",
		},
		{
			name:    "empty SrcAddr field returns empty string",
			payload: validPayload(""),
			want:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, f.srcAddrFromPayload(tt.payload))
		})
	}
}
