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
	"testing"

	"github.com/eryugey/serf/serf"
	"github.com/stretchr/testify/assert"
)

// mockFilter is a test implementation of serf.EventBufferFilterer that
// returns only the entries whose LTime is in the allowed set.
type mockFilter struct {
	allowed map[serf.LamportTime]bool
}

func newMockFilter(ltimes ...serf.LamportTime) *mockFilter {
	m := &mockFilter{allowed: make(map[serf.LamportTime]bool)}
	for _, lt := range ltimes {
		m.allowed[lt] = true
	}
	return m
}

func (f *mockFilter) Filter(buf []*serf.BufferedUserEvents) []*serf.BufferedUserEvents {
	var result []*serf.BufferedUserEvents
	for _, entry := range buf {
		if entry != nil && f.allowed[entry.LTime] {
			result = append(result, entry)
		}
	}
	return result
}

// newTestSerfService builds a minimal serfService with the given users
// pre-registered, without starting the underlying serf cluster.
func newTestSerfService(users ...*SerfUser) *serfService {
	ss := &serfService{
		config: DefaultSerfConfig(),
		users:  make(map[string]*SerfUser),
		events: make(map[string]*SerfUser),
	}
	for _, u := range users {
		ss.users[u.Name] = u
		for _, ev := range u.UserEvents {
			ss.events[ev] = u
		}
	}
	return ss
}

// makeBuffer constructs a slice of *serf.BufferedUserEvents with the given
// LTimes. nil entries can be represented by passing 0 (ltime 0 is not used
// in production but tests nil-entry handling explicitly via the nil variant).
func makeBuffer(ltimes ...serf.LamportTime) []*serf.BufferedUserEvents {
	buf := make([]*serf.BufferedUserEvents, len(ltimes))
	for i, lt := range ltimes {
		buf[i] = &serf.BufferedUserEvents{LTime: lt}
	}
	return buf
}

// TestFilterNoUsers verifies that when no users are registered, the full
// eventBuffer is returned unchanged.
func TestFilterNoUsers(t *testing.T) {
	ss := newTestSerfService()
	buf := makeBuffer(1, 2, 3)
	got := ss.Filter(buf)
	assert.Equal(t, buf, got, "no users: full buffer should be returned")
}

// TestFilterNoUserFilter verifies that when all registered users have no
// Filter set, the full eventBuffer is returned unchanged.
func TestFilterNoUserFilter(t *testing.T) {
	u := &SerfUser{Name: "u1", UserEvents: []string{"ev1"}}
	ss := newTestSerfService(u)
	buf := makeBuffer(1, 2, 3)
	got := ss.Filter(buf)
	assert.Equal(t, buf, got, "users without filter: full buffer should be returned")
}

// TestFilterSingleUser verifies that a single user's filter is applied
// correctly, returning only the entries allowed by that filter.
func TestFilterSingleUser(t *testing.T) {
	buf := makeBuffer(1, 2, 3, 4, 5)
	u := &SerfUser{
		Name:       "u1",
		UserEvents: []string{"ev1"},
		Filter:     newMockFilter(1, 3, 5),
	}
	ss := newTestSerfService(u)

	got := ss.Filter(buf)
	assert.Len(t, got, 3, "should return 3 filtered entries")
	assert.Equal(t, serf.LamportTime(1), got[0].LTime)
	assert.Equal(t, serf.LamportTime(3), got[1].LTime)
	assert.Equal(t, serf.LamportTime(5), got[2].LTime)
}

// TestFilterMultipleUsersChained verifies that when multiple users each have a
// filter, they are applied in sequence: the output of one filter is passed as
// input to the next. Each filter only removes its own events; events owned by
// other users are left untouched. The net result is the intersection of what
// all filters collectively retain.
func TestFilterMultipleUsersChained(t *testing.T) {
	// Buffer has 5 events. u1 owns ev1/ev2/ev3 and keeps LTimes 1 and 2.
	// u2 owns ev4/ev5 and keeps LTime 4. Neither filter touches the other's events.
	// After chaining: u1 removes LTime 3 (its own); u2 removes LTime 5 (its own).
	// Final result: {1, 2, 4}.
	buf := makeBuffer(1, 2, 3, 4, 5)

	// u1's filter keeps LTimes 1, 2, 4, 5 (removes LTime 3 which belongs to u1)
	u1 := &SerfUser{
		Name:       "u1",
		UserEvents: []string{"ev1", "ev2", "ev3"},
		Filter:     newMockFilter(1, 2, 4, 5),
	}
	// u2's filter keeps LTimes 1, 2, 4 (removes LTime 5 which belongs to u2)
	u2 := &SerfUser{
		Name:       "u2",
		UserEvents: []string{"ev4", "ev5"},
		Filter:     newMockFilter(1, 2, 4),
	}
	ss := newTestSerfService(u1, u2)

	got := ss.Filter(buf)
	assert.Len(t, got, 3, "chained filters should yield 3 entries")
	ltimes := make(map[serf.LamportTime]bool)
	for _, entry := range got {
		ltimes[entry.LTime] = true
	}
	assert.True(t, ltimes[1], "LTime 1 should be present")
	assert.True(t, ltimes[2], "LTime 2 should be present")
	assert.True(t, ltimes[4], "LTime 4 should be present")
	assert.False(t, ltimes[3], "LTime 3 should be removed by u1's filter")
	assert.False(t, ltimes[5], "LTime 5 should be removed by u2's filter")
}

// TestFilterMixedUsers verifies that only users with a non-nil Filter
// contribute to the result; users without a filter are ignored.
func TestFilterMixedUsers(t *testing.T) {
	buf := makeBuffer(1, 2, 3)
	uWithFilter := &SerfUser{
		Name:       "u1",
		UserEvents: []string{"ev1"},
		Filter:     newMockFilter(2),
	}
	uNoFilter := &SerfUser{
		Name:       "u2",
		UserEvents: []string{"ev2"},
		// Filter intentionally nil
	}
	ss := newTestSerfService(uWithFilter, uNoFilter)

	got := ss.Filter(buf)
	assert.Len(t, got, 1, "only entries from user with filter")
	assert.Equal(t, serf.LamportTime(2), got[0].LTime)
}

// TestFilterEmptyBuffer verifies that an empty eventBuffer returns an empty
// result regardless of what filters are configured.
func TestFilterEmptyBuffer(t *testing.T) {
	u := &SerfUser{
		Name:       "u1",
		UserEvents: []string{"ev1"},
		Filter:     newMockFilter(1, 2, 3),
	}
	ss := newTestSerfService(u)

	got := ss.Filter([]*serf.BufferedUserEvents{})
	assert.Empty(t, got, "empty buffer should return empty result")
}

// TestFilterChainedOutputIsInput verifies that the output of each filter is
// passed as the input to the next filter in the chain (not the original buffer).
func TestFilterChainedOutputIsInput(t *testing.T) {
	// Buffer: LTimes 1..5. u1 removes LTime 5; u2 then receives {1,2,3,4} and removes LTime 4.
	// If filters received the original buffer, u2 would still see LTime 5; in chain mode it won't.
	buf := makeBuffer(1, 2, 3, 4, 5)

	u1 := &SerfUser{
		Name:       "u1",
		UserEvents: []string{"ev5"},
		Filter:     newMockFilter(1, 2, 3, 4), // removes LTime 5
	}
	u2 := &SerfUser{
		Name:       "u2",
		UserEvents: []string{"ev4"},
		Filter:     newMockFilter(1, 2, 3), // removes LTime 4; never sees LTime 5
	}
	ss := newTestSerfService(u1, u2)

	got := ss.Filter(buf)
	assert.Len(t, got, 3, "chain should yield 3 entries")
	for _, entry := range got {
		if entry.LTime == 4 || entry.LTime == 5 {
			t.Errorf("LTime %d should have been removed", entry.LTime)
		}
	}
}
