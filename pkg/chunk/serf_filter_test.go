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

package chunk

import (
	"testing"

	"github.com/eryugey/serf/serf"
	"github.com/stretchr/testify/assert"
)

// makeEntry builds a BufferedUserEvents with the given LTime and event names.
func makeEntry(ltime serf.LamportTime, names ...string) *serf.BufferedUserEvents {
	events := make([]serf.BufferedUserEvent, len(names))
	for i, n := range names {
		events[i] = serf.BufferedUserEvent{Name: n}
	}
	return &serf.BufferedUserEvents{LTime: ltime, Events: events}
}

// eventNames extracts the event names from a BufferedUserEvents entry.
func eventNames(entry *serf.BufferedUserEvents) []string {
	names := make([]string, len(entry.Events))
	for i, ev := range entry.Events {
		names[i] = ev.Name
	}
	return names
}

func TestStoreEventFilter(t *testing.T) {
	tests := []struct {
		name       string
		input      []*serf.BufferedUserEvents
		wantLen    int
		wantLTimes []serf.LamportTime
		wantEvents [][]string // per-entry expected event names, nil means skip check
	}{
		{
			name:    "empty buffer returns empty result",
			input:   []*serf.BufferedUserEvents{},
			wantLen: 0,
		},
		{
			name:       "nil entries are skipped",
			input:      []*serf.BufferedUserEvents{nil, makeEntry(1, dataEvent), nil},
			wantLen:    1,
			wantLTimes: []serf.LamportTime{1},
		},
		{
			name:    "announceEvent is removed",
			input:   []*serf.BufferedUserEvents{makeEntry(1, announceEvent)},
			wantLen: 0,
		},
		{
			name:    "revokeEvent is removed",
			input:   []*serf.BufferedUserEvents{makeEntry(1, revokeEvent)},
			wantLen: 0,
		},
		{
			name:    "compAnnounceEvent is removed",
			input:   []*serf.BufferedUserEvents{makeEntry(1, compAnnounceEvent)},
			wantLen: 0,
		},
		{
			name:    "compRevokeEvent is removed",
			input:   []*serf.BufferedUserEvents{makeEntry(1, compRevokeEvent)},
			wantLen: 0,
		},
		{
			name:       "dataEvent is retained",
			input:      []*serf.BufferedUserEvents{makeEntry(1, dataEvent)},
			wantLen:    1,
			wantEvents: [][]string{{dataEvent}},
		},
		{
			name:       "other module events are retained",
			input:      []*serf.BufferedUserEvents{makeEntry(1, "other-module-event")},
			wantLen:    1,
			wantEvents: [][]string{{"other-module-event"}},
		},
		{
			name: "store events removed from mixed entry, non-store events retained",
			input: []*serf.BufferedUserEvents{
				makeEntry(1, announceEvent, dataEvent, "other-event", revokeEvent),
			},
			wantLen:    1,
			wantLTimes: []serf.LamportTime{1},
			wantEvents: [][]string{{dataEvent, "other-event"}},
		},
		{
			name: "entry dropped when all its events are store events",
			input: []*serf.BufferedUserEvents{
				makeEntry(1, announceEvent, revokeEvent),
				makeEntry(2, dataEvent),
			},
			wantLen:    1,
			wantLTimes: []serf.LamportTime{2},
		},
		{
			name: "LTime of retained entries is preserved",
			input: []*serf.BufferedUserEvents{
				makeEntry(42, dataEvent),
				makeEntry(99, "other-event"),
			},
			wantLen:    2,
			wantLTimes: []serf.LamportTime{42, 99},
		},
	}

	f := &storeEventFilter{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := f.Filter(tt.input)
			assert.Len(t, got, tt.wantLen)
			for i, lt := range tt.wantLTimes {
				assert.Equal(t, lt, got[i].LTime)
			}
			for i, names := range tt.wantEvents {
				assert.Equal(t, names, eventNames(got[i]))
			}
		})
	}
}
