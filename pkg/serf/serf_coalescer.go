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

package serf

import (
	"github.com/eryugey/serf/serf"
)

func (s *serfService) Handle(e serf.Event) bool {
	// Only handle serf.EventUser messages
	if e.EventType() != serf.EventUser {
		return false
	}

	userEvent := e.(serf.UserEvent)
	eventName := userEvent.Name
	s.eventMu.RLock()
	serfUser, ok := s.events[eventName]
	s.eventMu.RUnlock()
	if !ok {
		return false
	}
	if serfUser.UserEventCoalescer == nil {
		return false
	}
	return serfUser.UserEventCoalescer.Handle(e)
}

func (s *serfService) Coalesce(e serf.Event) {
	userEvent := e.(serf.UserEvent)
	eventName := userEvent.Name
	if userEvent.Payload == nil {
		logger.Warnf("serf: event %s has no payload, skip coalescing", eventName)
		return
	}

	s.eventMu.RLock()
	serfUser, ok := s.events[eventName]
	s.eventMu.RUnlock()
	if !ok {
		return
	}
	if serfUser.UserEventCoalescer == nil {
		return
	}
	serfUser.UserEventCoalescer.Coalesce(e)
	return
}

func (s *serfService) Flush(outChan chan<- serf.Event) {
	s.userMu.RLock()
	users := make([]*SerfUser, 0, len(s.users))
	for _, serfUser := range s.users {
		users = append(users, serfUser)
	}
	s.userMu.RUnlock()

	for _, serfUser := range users {
		if serfUser.UserEventCoalescer == nil {
			continue
		}
		serfUser.UserEventCoalescer.Flush(outChan)
	}
	return
}
