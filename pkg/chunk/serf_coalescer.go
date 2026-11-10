/*
 * Copyright 2024 Alibaba Cloud, Inc. or its affiliates.
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
	"encoding/json"

	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/eryugey/serf/serf"
	"github.com/willf/bitset"
)

type userEventCoalescer struct {
	peer          *peerCache
	announceCount int
	revokeCount   int
	dataCount     int
	announceLTime serf.LamportTime
	revokeLTime   serf.LamportTime
	dataLTime     serf.LamportTime
	announces     map[string]*metaInfo
	revokes       map[string]*metaInfo
	data          *metaInfo
}

func newUserEventCoalescer(peer *peerCache) serf.Coalescer {
	return &userEventCoalescer{
		peer:      peer,
		announces: make(map[string]*metaInfo),
		revokes:   make(map[string]*metaInfo),
	}
}

func (c *userEventCoalescer) Handle(e serf.Event) bool {
	// EventType has been checked by serf service, only check user event here.
	user := e.(serf.UserEvent)
	return isValidUserEvent(user.Name)
}

func (c *userEventCoalescer) Coalesce(e serf.Event) {
	user := e.(serf.UserEvent)
	info := &metaInfo{}

	// Payload has been checked by serf service
	if err := json.Unmarshal(user.Payload, info); err != nil {
		logger.Warnf("unmarshal user event request: %s", err)
		return
	}
	if c.peer.fromLocal(info.SrcAddr) {
		return
	}
	if !c.peer.serf.MemberExists(info.SrcAddr) {
		return
	}
	if err := c.peer.validateMetaInfo(info); err != nil {
		logger.Warnf("Skip invalid %s meta info: %s", user.Name, err)
		return
	}

	announces, ok := c.announces[info.SrcAddr]
	if !ok {
		announces = &metaInfo{
			SrcAddr:   info.SrcAddr,
			BlockSize: info.BlockSize,
			Targets:   make(map[string]*targetMeta),
		}
		c.announces[info.SrcAddr] = announces
	}
	revokes, ok := c.revokes[info.SrcAddr]
	if !ok {
		revokes = &metaInfo{
			SrcAddr:   info.SrcAddr,
			BlockSize: info.BlockSize,
			Targets:   make(map[string]*targetMeta),
		}
		c.revokes[info.SrcAddr] = revokes
	}
	switch user.Name {
	case announceEvent, compAnnounceEvent:
		// Merge new announce events with existing events
		for target, remoteTm := range info.Targets {
			if remoteTm.Blocks != nil {
				localTm, ok := announces.Targets[target]
				if !ok {
					announces.Targets[target] = &targetMeta{
						Blocks: remoteTm.Blocks.Clone(),
					}
				} else {
					if localTm.Length == 0 {
						localTm.Length = remoteTm.Length
					}
					localTm.Blocks.InPlaceUnion(remoteTm.Blocks)
				}
			} else if remoteTm.Length != 0 {
				localTm, ok := announces.Targets[target]
				if !ok {
					blkcnt := utils.GetBlockCount(remoteTm.Length, announces.BlockSize)
					announces.Targets[target] = &targetMeta{
						Length: remoteTm.Length,
						Blocks: bitset.New(uint(blkcnt)).ClearAll().Complement(),
					}
				} else {
					localTm.Length = remoteTm.Length
					localTm.Blocks.ClearAll().Complement()
				}
			}
		}

		// New announce events would remove revoke events
		for target, remoteTm := range info.Targets {
			if remoteTm.Blocks != nil {
				localTm, ok := revokes.Targets[target]
				if ok {
					localTm.Blocks.InPlaceIntersection(localTm.Blocks.Intersection(remoteTm.Blocks).Complement())
				}
			}
		}

		if c.announceLTime < user.LTime {
			c.announceLTime = user.LTime
		}
		c.announceCount++
	case revokeEvent, compRevokeEvent:
		// Merge new revoke events with existing events
		for target, remoteTm := range info.Targets {
			if remoteTm.Blocks != nil {
				localTm, ok := revokes.Targets[target]
				if !ok {
					revokes.Targets[target] = &targetMeta{
						Blocks: remoteTm.Blocks.Clone(),
					}
				} else {
					if localTm.Length == 0 {
						localTm.Length = remoteTm.Length
					}
					localTm.Blocks.InPlaceUnion(remoteTm.Blocks)
				}
			}
		}

		// New revoke events would remove announce events
		for target, remoteTm := range info.Targets {
			if remoteTm.Blocks != nil {
				localTm, ok := announces.Targets[target]
				if ok {
					localTm.Blocks.InPlaceIntersection(localTm.Blocks.Intersection(remoteTm.Blocks).Complement())
				}
			}
		}

		if c.revokeLTime < user.LTime {
			c.revokeLTime = user.LTime
		}
		c.revokeCount++
	case dataEvent:
		if c.dataLTime < user.LTime {
			c.dataLTime = user.LTime
		}

		// Merge multiple data ready events into one
		if c.data == nil {
			c.data = &metaInfo{
				SrcAddr:   info.SrcAddr,
				BlockSize: info.BlockSize,
			}
		}
		c.dataCount++
	}
}

func (c *userEventCoalescer) Flush(outChan chan<- serf.Event) {
	for _, info := range c.announces {
		if len(info.Targets) > 0 {
			payload, err := json.Marshal(info)
			if err != nil {
				logger.Warnf("Failed to marshal announce info \"%v\": %s", info, err)
				continue
			}
			outChan <- serf.UserEvent{
				LTime:    c.announceLTime,
				Name:     announceEvent,
				Payload:  payload,
				Coalesce: true,
			}
		}
	}

	for _, info := range c.revokes {
		if len(info.Targets) > 0 {
			payload, err := json.Marshal(info)
			if err != nil {
				logger.Warnf("Failed to marshal revoke info \"%v\": %s", info, err)
				continue
			}
			outChan <- serf.UserEvent{
				LTime:    c.revokeLTime,
				Name:     revokeEvent,
				Payload:  payload,
				Coalesce: true,
			}
		}
	}

	if c.data != nil {
		payload, err := json.Marshal(c.data)
		if err != nil {
			logger.Warnf("Failed to marshal data ready info \"%v\": %s", c.data, err)
		} else {
			outChan <- serf.UserEvent{
				LTime:    c.dataLTime,
				Name:     dataEvent,
				Payload:  payload,
				Coalesce: true,
			}
		}
	}
	logger.Debugf("Coalesced %d announce events, %d revoke events, %d data ready events",
		c.announceCount, c.revokeCount, c.dataCount)
	c.announceCount = 0
	c.revokeCount = 0
	c.dataCount = 0

	c.announceLTime = 0
	c.revokeLTime = 0
	c.dataLTime = 0
	c.announces = make(map[string]*metaInfo)
	c.revokes = make(map[string]*metaInfo)
	c.data = nil
}
