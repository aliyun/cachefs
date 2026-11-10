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

	ss "github.com/aliyun/cachefs/pkg/serf"
	"github.com/eryugey/serf/serf"
)

type userEventCoalescer struct {
	serf          ss.SerfService
	localAddr     string
	announceCount int
	dumpCount     int
	announceLTime serf.LamportTime
	dumpLTime     serf.LamportTime
	announce      *serfInfo
	dump          *serfInfo
}

func newUserEventCoalescer(addr string, serf ss.SerfService) serf.Coalescer {
	return &userEventCoalescer{serf: serf, localAddr: addr}
}

func (c *userEventCoalescer) fromLocal(info *serfInfo) bool {
	return info.SrcAddr == c.localAddr
}

func (c *userEventCoalescer) Handle(e serf.Event) bool {
	// EventType has been checked by serf service, only check user event here.
	user := e.(serf.UserEvent)
	return isValidUserEvent(user.Name)
}

func (c *userEventCoalescer) Coalesce(e serf.Event) {
	user := e.(serf.UserEvent)
	info := &serfInfo{}

	// Payload has been checked by serf service
	if err := json.Unmarshal(user.Payload, info); err != nil {
		logger.Warnf("unmarshal user event request: %s", err)
		return
	}
	if c.fromLocal(info) {
		return
	}

	switch user.Name {
	case announceEvent:
		if c.announce == nil {
			c.announce = info
		}

		// Use meta provider as main src addr, otherwise append current addr to addr list
		addrs := c.announce.Addrs
		if info.Provider {
			c.announce = info
			c.announce.Addrs = addrs
		} else {
			c.announce.Addrs = append(addrs, info.SrcAddr)
		}
		if c.announceLTime < user.LTime {
			c.announceLTime = user.LTime
		}
		c.announceCount++
	case dumpEvent:
		// Merge multiple dump events into one
		if c.dump == nil {
			c.dump = info
		}
		if c.dumpLTime < user.LTime {
			c.dumpLTime = user.LTime
		}
		c.dumpCount++
	}
}

func (c *userEventCoalescer) Flush(outChan chan<- serf.Event) {
	// Defer the member-existence check to Flush() rather than Coalesce() because
	// member-join events share the same CoalescePeriod as user events. Checking
	// MemberExists() during Coalesce() would incorrectly drop events from nodes
	// that joined within the same coalesce window (their member-join event has not
	// been processed yet at that point). By the time Flush() is called the window
	// has expired and all member-join events have been delivered, so the check is safe.
	if c.announce != nil && c.serf.MemberExists(c.announce.SrcAddr) {
		payload, err := json.Marshal(c.announce)
		if err != nil {
			logger.Warnf("Failed to marshal announce info \"%v\": %s", c.announce, err)
		} else {
			outChan <- serf.UserEvent{
				LTime:    c.announceLTime,
				Name:     announceEvent,
				Payload:  payload,
				Coalesce: true,
			}
		}
	}

	if c.dump != nil && c.serf.MemberExists(c.dump.SrcAddr) {
		payload, err := json.Marshal(c.dump)
		if err != nil {
			logger.Warnf("Failed to marshal dump info \"%v\": %s", c.dump, err)
		} else {
			outChan <- serf.UserEvent{
				LTime:    c.dumpLTime,
				Name:     dumpEvent,
				Payload:  payload,
				Coalesce: true,
			}
		}
	}

	if c.announceCount != 0 || c.dumpCount != 0 {
		logger.Debugf("Meta coalescer handled %d announce events, %d dump events",
			c.announceCount, c.dumpCount)
	}

	c.announceCount = 0
	c.dumpCount = 0
	c.announceLTime = 0
	c.dumpLTime = 0
	c.announce = nil
	c.dump = nil
}
