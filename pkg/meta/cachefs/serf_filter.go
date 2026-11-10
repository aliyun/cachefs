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

	ss "github.com/aliyun/cachefs/pkg/serf"
	"github.com/eryugey/serf/serf"
)

// metaEventFilter implements serf.EventBufferFilterer for the meta module.
// It removes dumpEvent unconditionally, and removes announceEvent when the
// source address no longer exists in the serf cluster. This prevents stale
// meta events from being exchanged with peers during Push-Pull synchronization.
type metaEventFilter struct {
	serf ss.SerfService
}

func newMetaEventFilter(serf ss.SerfService) *metaEventFilter {
	return &metaEventFilter{serf: serf}
}

var metaFilteredEvents = map[string]struct{}{
	dumpEvent: {},
}

// Filter removes meta-specific events from the event buffer:
//   - dumpEvent is always removed (request-response, no need to sync).
//   - announceEvent is removed when the source member no longer exists in
//     the cluster (stale announce from a departed node).
func (f *metaEventFilter) Filter(eventBuffer []*serf.BufferedUserEvents) []*serf.BufferedUserEvents {
	seenAddrs := make(map[string]struct{})
	result := make([]*serf.BufferedUserEvents, 0, len(eventBuffer))
	for _, entry := range eventBuffer {
		if entry == nil {
			continue
		}
		retained := make([]serf.BufferedUserEvent, 0, len(entry.Events))
		for _, ev := range entry.Events {
			if _, filtered := metaFilteredEvents[ev.Name]; filtered {
				continue
			}
			if ev.Name == announceEvent {
				if f.filterAnnounce(ev.Payload) {
					continue
				}
				// Deduplicate: keep only one announce per SrcAddr.
				if addr := f.srcAddrFromPayload(ev.Payload); addr != "" {
					if _, seen := seenAddrs[addr]; seen {
						continue
					}
					seenAddrs[addr] = struct{}{}
				}
			}
			retained = append(retained, ev)
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

// Filter out events with source member no longer exists in the serf cluster.
func (f *metaEventFilter) filterAnnounce(payload []byte) bool {
	if f.serf == nil {
		return false
	}
	info := &serfInfo{}
	if err := json.Unmarshal(payload, info); err != nil {
		// Malformed payload, remove it
		return true
	}
	return !f.serf.MemberExists(info.SrcAddr)
}

func (f *metaEventFilter) srcAddrFromPayload(payload []byte) string {
	info := &serfInfo{}
	if err := json.Unmarshal(payload, info); err != nil {
		return ""
	}
	return info.SrcAddr
}
