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
	"os"
	"strconv"
	"time"
)

func (m *metaData) attrSweepInterval() time.Duration {
	interval := max(time.Duration(m.config.ReaddirTimeout/2)*time.Second, 5*time.Second)
	if value := os.Getenv("CFS_META_SWEEP_INTERVAL"); value != "" {
		override, err := strconv.Atoi(value)
		if err != nil || override <= 0 {
			logger.Warnf("Invalid CFS_META_SWEEP_INTERVAL %q, use %s", value, interval)
		} else {
			interval = time.Duration(override) * time.Second
			logger.Infof("Metadata attr sweep interval set to %s by CFS_META_SWEEP_INTERVAL", interval)
		}
	}
	return interval
}

// Sourceless and peer-meta modes disable attr sweeping so dump/load can rely on non-nil attrs and
// valid types.
func (m *metaData) attrSweepEnabled() bool {
	return m.config.AttrCleanup && m.config.ReaddirTimeout >= 0 &&
		!m.config.Sourceless && !m.config.PeerMeta()
}

func (m *metaData) startAttrSweeper() {
	if !m.attrSweepEnabled() {
		return
	}
	logger.Info("Metadata attr cleanup enabled")
	go m.sweepAttrs(m.stopCh)
}

func (m *metaData) sweepAttrs(stopCh <-chan struct{}) {
	interval := m.attrSweepInterval()
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-stopCh:
			return
		case <-timer.C:
			m.doSweepAttrs()
			timer.Reset(interval)
		}
	}
}

func (m *metaData) doSweepAttrs() int {
	if !m.attrSweepEnabled() {
		return 0
	}
	timeout := time.Duration(m.config.ReaddirTimeout) * time.Second
	evicted := 0
	for i := range m.inodeShards {
		shard := &m.inodeShards[i]
		shard.Lock()
		for _, cino := range shard.inodes {
			cached := cino.attrCache.Load()
			if cached == nil || !isStaleAttr(cached, timeout) {
				continue
			}
			// Do not clear a newer attribute published after this Load.
			if cino.attrCache.CompareAndSwap(cached, nil) {
				m.cachedAttrs.Add(-1)
				evicted++
			}
		}
		shard.Unlock()
	}
	return evicted
}
