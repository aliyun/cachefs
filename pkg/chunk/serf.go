/*
 * Copyright 2023 Alibaba Cloud, Inc. or its affiliates.
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
	"context"
	"encoding/json"
	"math"
	"math/rand"
	"sync"
	"time"

	ss "github.com/aliyun/cachefs/pkg/serf"
	"github.com/aliyun/cachefs/pkg/utils"
)

const (
	announceEvent string = "store-announce"
	revokeEvent   string = "store-revoke"
	dataEvent     string = "store-dataready"

	// to be compatible with old events
	compAnnounceEvent string = "announce"
	compRevokeEvent   string = "revoke"

	// the minimum number of peers before we start scaling the periodic exchange interval
	exchangeScaleThreshold = 32
)

// number of peers to exchange metaMap with per periodic round, overridable via
// CFS_EXCHANGE_FANOUT. A higher fanout shortens the metaMap staleness window at
// the cost of more exchange RPCs.
var exchangeFanout = 3

var supportedSerfEvents = []string{
	announceEvent,
	revokeEvent,
	dataEvent,
	compAnnounceEvent,
	compRevokeEvent,
}

func init() {
	exchangeFanout = utils.GetEnvInt("CFS_EXCHANGE_FANOUT", exchangeFanout, 1)
}

func isValidUserEvent(eventName string) bool {
	switch eventName {
	case announceEvent, revokeEvent, dataEvent:
		return true
	case compAnnounceEvent, compRevokeEvent:
		return true
	default:
		return false
	}
}

func (r *peerCache) broadCast(typ string, info *metaInfo) {
	payload, err := json.Marshal(info)
	if err != nil {
		logger.Warnf("Failed to marshal meta info %s:%v: %s", typ, info, err)
		return
	}

	if r.serf == nil {
		logger.Warnf("Serf cluster not initialized, skip broadcast %s - %v", typ, info)
		return
	}
	logger.Debugf("Broadcast %s - %v to cluster, payload size %d", typ, info, len(payload))
	if err := r.serf.BroadCast(typ, payload); err != nil {
		logger.Warnf("Failed to send serf user event %s:%v: %s", typ, info, err)
	}
}

// Receive serf user events and process events.
func (r *peerCache) handleSerfUserEvent() {
	sendInfo := func(name string, ch chan *metaInfo, info *metaInfo) {
		select {
		case ch <- info:
		default:
			logger.Warnf("%s channel full, dropping events", name)
		}
	}
	for {
		select {
		case <-r.stopCh:
			return
		case userEvent := <-r.serfUser.UserEventCh:
			// Name and Payload has been checked by serf service
			eventName := userEvent.Name
			payload := userEvent.Payload
			info := &metaInfo{}
			if err := json.Unmarshal(payload, info); err != nil {
				logger.Warnf("Unmarshal user event request: %s", err)
				continue
			}
			if r.fromLocal(info.SrcAddr) {
				continue
			}
			if !r.serf.MemberExists(info.SrcAddr) {
				logger.Debugf("Ignore serf user event %s from %s, which is not in our member list", eventName, info.SrcAddr)
				continue
			}

			// Send meta info to channel
			switch eventName {
			case announceEvent, compAnnounceEvent:
				sendInfo(eventName, r.announceCh, info)
			case revokeEvent, compRevokeEvent:
				sendInfo(eventName, r.revokeCh, info)
			case dataEvent:
				go r.addSeeders([]string{info.SrcAddr})
			default:
				logger.Warnf("Unknown user event name \"%s\"", eventName)
			}
		}
	}
}

func (r *peerCache) handleSerfMemberJoinEvent() {
	for {
		select {
		case <-r.stopCh:
			return
		case members := <-r.serfUser.MemberJoinCh:
			seederAddrs := make([]string, 0, len(members))
			allAddrs := make([]string, 0, len(members))
			for _, mem := range members {
				if r.fromLocal(mem.Addr) {
					continue
				}
				allAddrs = append(allAddrs, mem.Addr)
				// Record group before exchange so the first known blocks use it.
				r.transport.updateRdmaPeer(mem.Addr, mem.RdmaGroup)
				if mem.Role != ss.SerfRoleSeeder {
					continue
				}
				seederAddrs = append(seederAddrs, mem.Addr)
			}
			if len(seederAddrs) > 0 {
				r.addSeeders(seederAddrs)
			}

			// During the initial join phase, immediately exchange metaMap with
			// all newly discovered members (not just seeders). Once the join is
			// complete, further member-join events are driven by gossip and the
			// remote node will initiate the exchange itself.
			select {
			case <-r.exchangeDoneCh:
				// join completed, skip
				logger.Debugf("Exchange done, skip exchange with %v", allAddrs)
			default:
				for _, addr := range allAddrs {
					select {
					case r.exchangeCh <- addr:
						logger.Debugf("Add member %s to exchange channel", addr)
					default:
						logger.Warnf("ExchangeTracker channel full, skipping peer %s", addr)
					}
				}
			}
		}
	}
}

// handle both member leave and member failed events by cleaning up any associated metadata.
func (r *peerCache) handleSerfMemberLeaveEvent() {
	for {
		select {
		case <-r.stopCh:
			return
		case members := <-r.serfUser.MemberLeaveCh:
			start := time.Now()
			rmblk := 0
			rmtarget := 0
			rmseeder := 0
			for _, mem := range members {
				b, t := r.cleanupMember(mem.Addr)
				rmblk += b
				rmtarget += t
				r.transport.updateRdmaPeer(mem.Addr, "")
			}
			r.seederMu.Lock()
			for _, mem := range members {
				if _, ok := r.seeders[mem.Addr]; !ok {
					continue
				}
				delete(r.seeders, mem.Addr)
				rmseeder += 1
			}
			r.seederMu.Unlock()
			logger.Infof("Handle %d left members in %s, removed %d blocks, %d targets, %d seeders",
				len(members), time.Since(start), rmblk, rmtarget, rmseeder)
		}
	}
}

// Clean up metadata associated with the member that left.
func (r *peerCache) cleanupMember(memAddr string) (int, int) {
	rmblk := 0
	rmtarget := 0
	r.targetMu.Lock()
	r.metaMu.Lock()
	// peerMap gives the exact set of blocks this member held, so we touch only its own
	// footprint instead of scanning the whole metaMap.
	for target, bs := range r.peerMap[memAddr] {
		for i, e := bs.NextSet(0); e; i, e = bs.NextSet(i + 1) {
			logger.Debugf("delete %s from %s block %d", memAddr, target, i)
			rmblk++
			r.clearPeerBlock(memAddr, target, uint32(i))
		}
	}
	delete(r.peerMap, memAddr)
	r.metaMu.Unlock()

	for target, tinfo := range r.targetMap {
		if _, exists := tinfo.nodes[memAddr]; exists {
			logger.Debugf("delete %s from %s target map", memAddr, target)
			rmtarget++
			delete(tinfo.nodes, memAddr)
		}
		// Remove target if no more peers are associated with it.
		if len(tinfo.nodes) == 0 {
			delete(r.targetMap, target)
		}
	}
	r.targetMu.Unlock()
	return rmblk, rmtarget
}

// serveExchangeTracker is a worker goroutine that processes ExchangeTracker
// requests from the exchangeCh channel. Multiple workers run concurrently
// to handle exchanges in parallel. Workers exit when stopCh is closed.
func (r *peerCache) serveExchangeTracker(id int) {
	logger.Debugf("Exchange tracker worker %d started", id)
	for {
		select {
		case <-r.stopCh:
			return
		case addr := <-r.exchangeCh:
			ctx, cancel := context.WithTimeout(context.Background(), exchangeTrackerTimeout)
			if err := r.transport.exchangeTracker(ctx, addr); err != nil {
				logger.Infof("ExchangeTracker with %s (worker %d): %s", addr, id, err)
			}
			cancel()
		}
	}
}

// On newly start up, wait for serf join to complete and then close exchangeDoneCh
// so that handleSerfMemberJoinEvent stops pushing new members to the exchange channel.
func (r *peerCache) collectMeta() {
	// Close exchangeDoneCh to signal that the initial join has completed, subsequent
	// member-join events will skip pushing to exchangeCh
	var closeOnce sync.Once
	closeExchangeDone := func() {
		closeOnce.Do(func() {
			close(r.exchangeDoneCh)
		})
	}

	select {
	case <-r.stopCh:
		return
	case <-r.serfUser.JoinedCh:
		// Wait for member-join events that are still being coalesced by serf
		// before closing exchangeDoneCh. Serf's CoalescePeriod determines how
		// long events are buffered before dispatch, so we sleep for that
		// duration to ensure all pending member-join events from the initial
		// join have been delivered to our MemberJoinCh.
		time.Sleep(ss.CoalescePeriod * 2)
		closeExchangeDone()
	case <-time.After(time.Minute):
		logger.Info("Waiting for JoinedCh timed out (1min), closing exchangeDoneCh anyway")
		closeExchangeDone()
	}
}

// scales the exchange interval based on the number of peers, following the same formula as
// memberlist's pushPullScale: interval stays unchanged below the threshold, then grows as
// Ceil(log2(n) - log2(threshold)) + 1 beyond it. This prevents network saturation as the cluster
// size grows.
func exchangeScaleInterval(interval time.Duration, n int) time.Duration {
	if n <= exchangeScaleThreshold {
		return interval
	}
	multiplier := math.Ceil(math.Log2(float64(n))-math.Log2(exchangeScaleThreshold)) + 1.0
	return time.Duration(multiplier) * interval
}

// periodically selects up to fanout random peers and triggers an ExchangeTracker
// with each via exchangeCh.
func (r *peerCache) periodicExchange() {
	interval := r.config.exchangeTime

	for {
		// Scale the interval based on current peer count
		tickTime := exchangeScaleInterval(interval, r.serf.GetMemberCount())
		select {
		case <-r.stopCh:
			return
		case <-time.After(tickTime):
			for _, addr := range r.randomPeers(exchangeFanout) {
				select {
				case r.exchangeCh <- addr:
				default:
					logger.Warnf("ExchangeTracker channel full, skipping periodic exchange with peer %s", addr)
				}
			}
		}
	}
}

// randomPeers selects up to n distinct random peer addresses from the serf
// member list, excluding the local address. Returns fewer than n (or nil) when
// not enough peers are available.
func (r *peerCache) randomPeers(n int) []string {
	if r.serf == nil || n <= 0 {
		return nil
	}
	members := r.serf.GetMembers(false)
	peers := make([]string, 0, len(members))
	for _, addr := range members {
		if !r.fromLocal(addr) {
			peers = append(peers, addr)
		}
	}
	if len(peers) == 0 {
		return nil
	}
	rand.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
	if len(peers) > n {
		peers = peers[:n]
	}
	return peers
}
