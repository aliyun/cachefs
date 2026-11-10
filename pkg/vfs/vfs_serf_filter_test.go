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

package vfs

import (
	"testing"

	"github.com/eryugey/serf/serf"
	"github.com/stretchr/testify/assert"
)

func TestVfsEventFilter(t *testing.T) {
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
			input:      []*serf.BufferedUserEvents{nil, makeVfsEntry(1, "other-event"), nil},
			wantLen:    1,
			wantLTimes: []serf.LamportTime{1},
		},
		{
			name:    "preloadEvent is removed",
			input:   []*serf.BufferedUserEvents{makeVfsEntry(1, preloadEvent)},
			wantLen: 0,
		},
		{
			name:    "clearEvent is removed",
			input:   []*serf.BufferedUserEvents{makeVfsEntry(1, clearEvent)},
			wantLen: 0,
		},
		{
			name:       "other module events are retained",
			input:      []*serf.BufferedUserEvents{makeVfsEntry(1, "other-module-event")},
			wantLen:    1,
			wantEvents: [][]string{{"other-module-event"}},
		},
		{
			name: "VFS events removed from mixed entry, non-VFS events retained",
			input: []*serf.BufferedUserEvents{
				makeVfsEntry(1, preloadEvent, "other-event", clearEvent),
			},
			wantLen:    1,
			wantLTimes: []serf.LamportTime{1},
			wantEvents: [][]string{{"other-event"}},
		},
		{
			name: "entry dropped when all its events are VFS events",
			input: []*serf.BufferedUserEvents{
				makeVfsEntry(1, preloadEvent, clearEvent),
				makeVfsEntry(2, "other-event"),
			},
			wantLen:    1,
			wantLTimes: []serf.LamportTime{2},
		},
		{
			name: "LTime of retained entries is preserved",
			input: []*serf.BufferedUserEvents{
				makeVfsEntry(42, "other-event"),
				makeVfsEntry(99, "another-event"),
			},
			wantLen:    2,
			wantLTimes: []serf.LamportTime{42, 99},
		},
	}

	f := &vfsEventFilter{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := f.Filter(tt.input)
			assert.Len(t, got, tt.wantLen)
			for i, lt := range tt.wantLTimes {
				assert.Equal(t, lt, got[i].LTime)
			}
			for i, names := range tt.wantEvents {
				assert.Equal(t, names, vfsEventNames(got[i]))
			}
		})
	}
}

// makeVfsEntry builds a BufferedUserEvents with the given LTime and event names.
func makeVfsEntry(ltime serf.LamportTime, names ...string) *serf.BufferedUserEvents {
	events := make([]serf.BufferedUserEvent, len(names))
	for i, n := range names {
		events[i] = serf.BufferedUserEvent{Name: n}
	}
	return &serf.BufferedUserEvents{LTime: ltime, Events: events}
}

// vfsEventNames extracts the event names from a BufferedUserEvents entry.
func vfsEventNames(entry *serf.BufferedUserEvents) []string {
	names := make([]string, len(entry.Events))
	for i, ev := range entry.Events {
		names[i] = ev.Name
	}
	return names
}
