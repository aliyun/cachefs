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
	"github.com/eryugey/serf/serf"
)

// vfsEventFilter implements serf.EventBufferFilterer for the VFS module. It removes preloadEvent,
// clearEvent and setCacheEvent from the event buffer, retaining all other events. This prevents
// VFS-specific events from being exchanged with peers during Push-Pull synchronization.
type vfsEventFilter struct{}

var vfsFilteredEvents = map[string]struct{}{
	preloadEvent:  {},
	clearEvent:    {},
	setCacheEvent: {},
}

func (f *vfsEventFilter) Filter(eventBuffer []*serf.BufferedUserEvents) []*serf.BufferedUserEvents {
	result := make([]*serf.BufferedUserEvents, 0, len(eventBuffer))
	for _, entry := range eventBuffer {
		if entry == nil {
			continue
		}
		retained := make([]serf.BufferedUserEvent, 0, len(entry.Events))
		for _, ev := range entry.Events {
			if _, filtered := vfsFilteredEvents[ev.Name]; !filtered {
				retained = append(retained, ev)
			}
		}
		if len(retained) == 0 {
			continue
		}
		result = append(result, &serf.BufferedUserEvents{
			LTime:  entry.LTime,
			Events: retained,
		})
	}
	return result
}
