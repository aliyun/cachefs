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
	"bytes"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/eryugey/serf/serf"
	"github.com/hashicorp/logutils"
	"github.com/hashicorp/memberlist"
)

type SerfConfig struct {
	LoggerName string // Name in logger, default to 'cachefs'
	CacheAddr  string // Address that identifies this serf node, including port
	CachePort  int    // Port that cachefs uses
	BindAddr   string // Address that serf lintens on
	BindPort   int    // Port that serf listens on
	JoinAddr   string
	JoinCmd    string
	MaxRetry   uint32
	Role       string
	Group      string
	NodeName   string
	HashRing   bool
	NodeCount  int
	Reconnect  time.Duration
	LeaveDelay time.Duration
}

func DefaultSerfConfig() *SerfConfig {
	return &SerfConfig{
		BindPort:   7946,
		MaxRetry:   10,
		HashRing:   true,
		NodeCount:  VirtualNodeCount,
		Reconnect:  20 * time.Second,
		LeaveDelay: 5 * time.Second,
	}
}

func setupSerfConfig(cfg *SerfConfig, coalescer serf.Coalescer, filterer serf.EventBufferFilterer, eventCh chan serf.Event) *serf.Config {
	conf := serf.DefaultConfig()
	memberlistConf := memberlist.DefaultLANConfig()

	level := strings.ToUpper(logger.Level.String())
	if level == "TRACE" || level == "DEBUG" {
		level = "INFO"
	}
	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"TRACE", "DEBUG", "INFO", "WARN", "ERROR"},
		MinLevel: logutils.LogLevel(level),
		Writer:   os.Stderr,
	}

	// Filter push/pull error due to serf group mismatch, see comments for
	// groupMismatchLogFilter for details.
	logOutput := &groupMismatchLogFilter{w: filter}

	// Add local cachefs port to serf tags for use by other peers.
	tags := map[string]string{
		keyCachePort: strconv.Itoa(cfg.CachePort),
	}
	if cfg.Role != "" {
		tags[keySerfRole] = cfg.Role
	}
	if cfg.Group != "" {
		tags[keySerfGroup] = cfg.Group
	}
	logger.Infof("Initialize serf with tags: %v", tags)

	// make serf listens on the same IP of cache server
	memberlistConf.BindAddr = cfg.BindAddr
	memberlistConf.GossipInterval = GossipInterval
	memberlistConf.LogOutput = logOutput
	memberlistConf.HandoffQueueDepth = EventQueueDepth
	memberlistConf.BindPort = cfg.BindPort
	memberlistConf.AdvertisePort = cfg.BindPort

	// RetransmitMult is the multiplier for the number of retransmissions that are attempted for
	// messages broadcasted over gossip, which is default to 4. The actual count of
	// retransmissions is calculated using the formula, where N is count of members:
	//
	// Retransmits = RetransmitMult * Ceil(log(N+1))
	memberlistConf.RetransmitMult = 3

	conf.EventBuffer = EventQueueDepth
	conf.MaxQueueDepth = EventQueueDepth
	conf.QueueDepthWarning = EventQueueDepth * 3 / 4
	conf.UserEventSizeLimit = serf.UserEventSizeLimit
	conf.MemberlistConfig = memberlistConf
	conf.EventCh = eventCh
	conf.LogOutput = logOutput
	conf.Tags = tags
	conf.Merge = newSerfMergeDelegate(cfg.Group)
	if len(cfg.NodeName) != 0 {
		conf.NodeName = cfg.NodeName
	}

	// User event coalesce setups. Serf is able to coalesce multiple events into single events
	// in order to reduce the amount of noise that is sent along the EventCh.
	//
	// UserCoalescePeriod specifies the time duration to coalesce events.
	// For example, if this is set to 5 seconds, then all events received
	// within 5 seconds that can be coalesced will be.
	//
	// UserQuiescentPeriod specifies the duration of time where if no events
	// are received, coalescence immediately happens. For example, if
	// CoalscePeriod is set to 10 seconds but QuiscentPeriod is set to 2
	// seconds, then the events will be coalesced and dispatched if no
	// new events are received within 2 seconds of the last event. Otherwise,
	// every event will always be delayed by at least 10 seconds.
	//
	// UserEventCoalescer is an optional user defined Coalescer interface implementation.
	if coalescer != nil {
		conf.UserCoalescePeriod = GossipInterval * 2
		conf.UserQuiescentPeriod = GossipInterval
		conf.UserEventCoalescer = coalescer
	}

	// Similar to UserCoalescePeriod, but works for serf events like member join and leave.
	conf.CoalescePeriod = GossipInterval * 2
	conf.QuiescentPeriod = GossipInterval

	// EventBufferFilter is an optional filter for selecting which user events to include
	// in the LocalState during Push-Pull synchronization. When set, only the filtered
	// events will be exchanged with other peers.
	if filterer != nil {
		conf.EventBufferFilter = filterer
	}

	// SkipBroadcastIfAlone skips queuing user event gossip broadcasts when this is the
	// only node in the cluster. New members receive past events through push-pull
	// synchronization (subject to EventBufferFilter).
	conf.SkipBroadcastIfAlone = true

	// Serf will reconnect to members in Failed status, and keep such members for
	// `ReconnectTimeout` time duration, and default value is 24 hours, which is too long, as IP
	// might be re-used by nodes in other serf clusters. Let's lower it and remove failed
	// members faster.
	conf.ReconnectTimeout = cfg.Reconnect

	// Serf will keep around nodes that gracefully left as tombstones for syncing state with
	// other Serf nodes, and default value is 24 hours. We don't need this state kept for that
	// long, lower the timeout and reduce memory usage and sync message size.
	conf.TombstoneTimeout = 5 * time.Minute

	// LeavePropagateDelay is for our leave message to propagate through the cluster. In
	// particular, we want to stay up long enough to service any probes from other nodes before
	// they learn about us leaving and stop probing. Otherwise, we risk getting node failures as
	// we leave.
	conf.LeavePropagateDelay = cfg.LeaveDelay

	return conf
}

// groupMismatchLogFilter wraps an io.Writer and suppresses [ERR] lines from
// memberlist that are caused by expected serf group mismatch rejections.
// These lines look like:
//
//	[ERR] memberlist: Failed push/pull merge: member not in same group: ...
//
// They are noisy because memberlist logs at ERR level for every incoming
// push/pull connection that is rejected by NotifyMerge. Our serfJoin already
// emits a summary at INFO level, so per-connection ERRs add no value.
type groupMismatchLogFilter struct {
	w io.Writer
}

func (f *groupMismatchLogFilter) Write(p []byte) (n int, err error) {
	if bytes.Contains(p, []byte("Failed push/pull merge")) &&
		bytes.Contains(p, []byte(ErrGroupMismatch.Error())) {
		return len(p), nil
	}
	return f.w.Write(p)
}
