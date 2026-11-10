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
	"time"
)

const (
	announceEvent string = "meta-announce"
	dumpEvent     string = "meta-dump"
)

type serfInfo struct {
	SrcAddr  string   // message comes from SrcAddr
	Provider bool     // meta provider
	Addrs    []string `json:",omitempty"`
}

func isValidUserEvent(eventName string) bool {
	switch eventName {
	case announceEvent, dumpEvent:
		return true
	default:
		return false
	}
}

func (m *metaData) broadCast(typ string, info *serfInfo) {
	if m.serf == nil {
		return
	}

	payload, err := json.Marshal(info)
	if err != nil {
		logger.Warnf("Failed to marshal meta info %s:%v: %s", typ, info, err)
		return
	}

	if m.serf == nil {
		logger.Warnf("Serf cluster not initialized, skip broadcast %s - %v", typ, info)
		return
	}
	logger.Debugf("Broadcast %s - %v to cluster, payload size %d", typ, info, len(payload))
	if err := m.serf.BroadCast(typ, payload); err != nil {
		logger.Warnf("Failed to send serf user event %s:%v: %s", typ, info, err)
		return
	}
	return
}

// Add sourceless meta source info
func (m *metaData) addMetaSource(info *serfInfo) {
	added := make([]string, 0, len(info.Addrs)+1)
	m.metaLock.Lock()
	if info.Provider && m.metaProvider != "" && m.metaProvider != info.SrcAddr {
		m.metaLock.Unlock()
		// There should be only one meta provider in cluster
		logger.Warnf("Ignore duplicated meta provider %s, already have %s", info.SrcAddr, m.metaProvider)
		return
	} else if info.Provider {
		m.metaProvider = info.SrcAddr
		added = append(added, info.SrcAddr)
	}

	for _, addr := range info.Addrs {
		if _, ok := m.metaSource[addr]; !ok {
			m.metaSource[addr] = struct{}{}
			added = append(added, addr)
		}
	}
	m.metaLock.Unlock()

	if len(added) > 0 {
		select {
		case m.metaSourceCh <- struct{}{}:
		default:
		}
		cnt := len(added)
		if cnt > 5 {
			logger.Infof("Node %v and %d more announced full filesystem meta, meta provider %v",
				added[:5], cnt-5, info.Provider)
		} else {
			logger.Infof("Node %v announced full filesystem meta, meta provider %v",
				added, info.Provider)
		}
	}
}

func (m *metaData) delMetaSource(addrs []string) {
	provider := ""
	removed := []string{}
	m.metaLock.Lock()
	for _, addr := range addrs {
		if _, ok := m.metaSource[addr]; ok {
			removed = append(removed, addr)
			delete(m.metaSource, addr)
		}
		if addr == m.metaProvider {
			provider = addr
			m.metaProvider = ""
		}
	}
	m.metaLock.Unlock()
	if len(removed) > 0 {
		logger.Infof("Removed %v from meta source", removed)
	}
	if provider != "" {
		logger.Infof("Meta provider %s removed", provider)
	}
}

func (m *metaData) announceMetaSource() {
	info := &serfInfo{
		SrcAddr:  m.localAddr,
		Provider: m.config.MetaProvider(),
	}
	m.broadCast(announceEvent, info)
}

// Receive serf user events and process events.
func (m *metaData) handleSerfUserEvent() {
	for {
		select {
		case <-m.stopCh:
			return
		case userEvent := <-m.serfUser.UserEventCh:
			// Name and Payload has been checked by serf service
			eventName := userEvent.Name
			payload := userEvent.Payload
			info := &serfInfo{}
			if err := json.Unmarshal(payload, info); err != nil {
				logger.Warnf("Unmarshal user event request: %s", err)
				continue
			}
			if m.fromLocal(info) {
				continue
			}
			if !m.serf.MemberExists(info.SrcAddr) {
				logger.Debugf("Ignore serf user event %s from %s, which is not in our member list", eventName, info.SrcAddr)
				continue
			}
			if !isValidUserEvent(eventName) {
				logger.Warnf("Unknown serf user event: %s", eventName)
				continue
			}
			switch eventName {
			case announceEvent:
				m.addMetaSource(info)
			case dumpEvent:
				if m.metaReady.Load() {
					m.announceMetaSource()
				}
			default:
				logger.Warnf("Unknown meta event: %s", eventName)
			}
		}
	}
}

func (m *metaData) handleSerfMemberLeaveEvent() {
	for {
		select {
		case <-m.stopCh:
			return
		case members := <-m.serfUser.MemberLeaveCh:
			memAddrs := make([]string, 0, len(members))
			for _, mem := range members {
				memAddrs = append(memAddrs, mem.Addr)
			}
			m.delMetaSource(memAddrs)
		}
	}
}

// On newly start up, send dump request in sourceless meta consumer mode or peer meta enabled to get
// meta sources
func (m *metaData) requestMetaSource() {
	if !m.config.MetaConsumer() && !m.config.PeerMeta() {
		return
	}

	// wait serf service to be ready for 1s
	t := time.NewTimer(time.Second)
	select {
	case <-t.C:
		logger.Warn("Waiting for serf service timed out, send meta ready anyway")
	case <-m.serfReadyCh:
	}

	select {
	case <-m.stopCh:
		return
	case <-m.serfUser.JoinedCh:
		info := &serfInfo{
			SrcAddr: m.localAddr,
		}
		m.broadCast(dumpEvent, info)
	}
}

func (m *metaData) fromLocal(info *serfInfo) bool {
	return info.SrcAddr == m.localAddr
}
