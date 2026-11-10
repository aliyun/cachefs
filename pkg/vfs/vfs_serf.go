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

package vfs

import (
	"encoding/json"
	"syscall"
	"time"
)

const (
	preloadEvent  string = "vfs-preload"
	clearEvent    string = "vfs-clear"
	setCacheEvent string = "vfs-set-cache"
)

var supportedSerfEvents = []string{preloadEvent, clearEvent, setCacheEvent}

type serfInfo struct {
	SrcAddr        string
	BTime          time.Time // Instance birth time, we'll ignore info before our local btime
	Paths          []string
	LoadAll        bool
	Force          bool
	Cancel         bool
	ClearBurst     bool
	CacheSize      int64
	BurstCacheSize int64
}

func isValidUserEvent(eventName string) bool {
	switch eventName {
	case preloadEvent:
		return true
	case clearEvent:
		return true
	case setCacheEvent:
		return true
	default:
		return false
	}
}

func (c *Cachefs) broadCast(typ string, info *serfInfo) error {
	if c.serf == nil {
		logger.Warnf("Serf cluster not initialized, skip broadcast %s - %v", typ, info)
		return syscall.EAGAIN
	}

	info.BTime = time.Now().UTC()
	payload, err := json.Marshal(info)
	if err != nil {
		logger.Warnf("Failed to marshal serf info %s:%v: %s", typ, info, err)
		return err
	}

	logger.Debugf("Broadcast %s - %v to cluster, payload size %d", typ, info, len(payload))
	if err := c.serf.BroadCast(typ, payload); err != nil {
		logger.Warnf("Failed to send serf user event %s:%v: %s", typ, info, err)
		return err
	}
	return nil
}

func (c *Cachefs) handleSerfUserEvent() {
	if c.serfUser == nil {
		return
	}
	for {
		select {
		case <-c.stopCh:
			return
		case userEvent := <-c.serfUser.UserEventCh:
			eventName := userEvent.Name
			payload := userEvent.Payload
			if payload == nil {
				logger.Warnf("serf event %q has no payload, skip", eventName)
				continue
			}
			if !isValidUserEvent(eventName) {
				logger.Warnf("Unknown serf user event: %s", eventName)
				continue
			}

			info := &serfInfo{}
			if err := json.Unmarshal(payload, info); err != nil {
				logger.Warnf("Unmarshal serf event request: %s", err)
				continue
			}
			if c.fromLocal(info) {
				continue
			}
			if !c.serf.MemberExists(info.SrcAddr) {
				logger.Debugf("Ignore serf user event %s from %s, which is not in our member list", eventName, info.SrcAddr)
				continue
			}
			// Ignore requests sent before our birth time, we only honor requests sent
			// when we're running.
			if info.BTime.Before(c.birthTime) {
				logger.Debugf("Ignore old serf user event %s from %s", eventName, info.SrcAddr)
				continue
			}

			switch eventName {
			case preloadEvent:
				if info.Cancel {
					logger.Infof("Received preload cancellation broadcast from %s", info.SrcAddr)
				} else {
					logger.Infof("Received preload broadcast from %s, paths: %v", info.SrcAddr, info.Paths)
				}
				req := &loadReq{
					paths:   info.Paths,
					loadAll: info.LoadAll,
					force:   info.Force,
					cancel:  info.Cancel,
				}
				err := c.loadCache(req)
				if err != 0 {
					if err == syscall.EINPROGRESS {
						logger.Infof("Handle broadcast preload request failed: %s", err)
					} else {
						logger.Warnf("Handle broadcast preload request failed: %s", err)
					}
				}
			case clearEvent:
				logger.Infof("Received clear cache broadcast from %s, paths: %v, clear_burst: %v", info.SrcAddr, info.Paths, info.ClearBurst)
				if info.ClearBurst {
					count, bytes := c.cache.clearBurst()
					logger.Infof("Shrink burst from broadcast: evicted %d blocks %dMiB", count, bytes>>20)
				} else {
					// Convert paths to cache prefixes
					prefix := c.pathsToCachePrefixes(info.Paths)
					if len(prefix) > 0 {
						count, bytes := c.cache.removeByPrefix(prefix, nil)
						logger.Infof("Cleared %d blocks %dMiB cache for broadcast paths: %v", count, bytes>>20, info.Paths)
					}
				}
			case setCacheEvent:
				logger.Infof("Received set-cache broadcast from %s: cacheSize=%dMB burstSize=%dMB",
					info.SrcAddr, info.CacheSize>>20, info.BurstCacheSize>>20)
				_, newBurstSize, _, _ := c.cache.setCacheSize(info.CacheSize, info.BurstCacheSize)
				if c.api != nil {
					c.api.quota.setCap(shareCap(newBurstSize, c.api.shareReserve))
				}
			default:
				logger.Warnf("Unknown VFS event: %s", eventName)
			}
		}
	}
}

func (c *Cachefs) fromLocal(info *serfInfo) bool {
	if c.serf == nil {
		return false
	}
	return info.SrcAddr == c.serf.LocalAddr()
}
