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

package serf

import (
	"github.com/eryugey/serf/serf"
)

// Filter implements serf.EventBufferFilterer. It applies each registered
// SerfUser's Filter in sequence, passing the output of one filter as the input
// to the next. Each user's filter only removes events it owns; events belonging
// to other users are left untouched. The final result is the eventBuffer after
// all per-user filters have been applied.
// If no user has a filter configured, the full eventBuffer is returned as-is.
func (s *serfService) Filter(eventBuffer []*serf.BufferedUserEvents) []*serf.BufferedUserEvents {
	s.userMu.RLock()
	users := make([]*SerfUser, 0, len(s.users))
	for _, u := range s.users {
		if u.Filter != nil {
			users = append(users, u)
		}
	}
	s.userMu.RUnlock()

	// If no user has a filter, return the full buffer unchanged.
	if len(users) == 0 {
		return eventBuffer
	}

	// Apply each user's filter in sequence: the output of one filter becomes
	// the input of the next. Each filter only shrinks the buffer, never grows it.
	result := eventBuffer
	for _, u := range users {
		result = u.Filter.Filter(result)
	}
	return result
}
