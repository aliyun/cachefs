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

package vfs

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aliyun/cachefs/pkg/chunk"
	"github.com/aliyun/cachefs/pkg/metric"
	"github.com/aliyun/cachefs/pkg/utils"
)

// SourceLoadStats holds this node's source-read counters, accumulated since
// process start and never reset.
type SourceLoadStats struct {
	// Reads that returned a full block; failures go to Errors instead.
	Reads    uint64 `json:"reads"`
	Bytes    uint64 `json:"bytes"`
	Errors   uint64 `json:"errors"`
	InFlight int64  `json:"in_flight"`

	// Peer-load retries in sourceless meta-consumer mode; zero otherwise.
	SourcelessRetry      uint64 `json:"sourceless_retry"`
	SourcelessRetryBytes uint64 `json:"sourceless_retry_bytes"`

	// Observed alongside Reads on the success path, so Latency.Count tracks Reads.
	Latency metric.HistSnapshot `json:"latency"`
}

// LoadStats is the data-loading report returned by the GetLoadStats API.
type LoadStats struct {
	UptimeSeconds int64               `json:"uptime_seconds"`
	Source        SourceLoadStats     `json:"source"`
	Peer          chunk.PeerLoadStats `json:"peer"`
}

func (v *Cachefs) collectLoadStats() LoadStats {
	return LoadStats{
		UptimeSeconds: int64(metric.Uptime().Seconds()),
		Source: SourceLoadStats{
			Reads:                uint64(sourceReads.Load()),
			Bytes:                uint64(sourceReadBytes.Load()),
			Errors:               uint64(sourceReadErrors.Load()),
			InFlight:             int64(atomic.LoadInt32(&sourceReadInFlight)),
			SourcelessRetry:      uint64(sourcelessRetry.Load()),
			SourcelessRetryBytes: uint64(sourcelessRetryBytes.Load()),
			Latency:              metric.ReadHist(sourceReadHist),
		},
		Peer: v.cache.getPeerLoadStats(),
	}
}

// Format renders the report as two independent records: a summary to judge the run at a glance, and
// a detail line to walk through field by field.
//
// The summary contains a percent sign, so callers must log it with logger.Info and never hand it to
// a printf-style logger as the format string.
func (s LoadStats) Format() (summary, detail string) {
	return s.formatSummary(), s.formatDetail()
}

func (s LoadStats) formatSummary() string {
	since := time.Duration(s.UptimeSeconds) * time.Second
	total := s.Source.Reads + s.Peer.Hits
	if total == 0 {
		return fmt.Sprintf("Load stats since mount (%s): no blocks loaded yet", since)
	}

	peerPct := float64(s.Peer.Hits) * 100 / float64(total)
	return fmt.Sprintf(
		"Load stats since mount (%s): loaded %d blocks %s from source, %d blocks %s from peers (peer %.1f%% of %d blocks)",
		since,
		s.Source.Reads, utils.FormatBytesShort(s.Source.Bytes),
		s.Peer.Hits, utils.FormatBytesShort(s.Peer.HitBytes),
		peerPct, total)
}

func (s LoadStats) formatDetail() string {
	var b strings.Builder
	b.WriteString("Load stats detail: source[")
	fmt.Fprintf(&b, "reads=%d bytes=%s errors=%d inflight=%d retry=%d retry_bytes=%s %s",
		s.Source.Reads, utils.FormatBytesShort(s.Source.Bytes),
		s.Source.Errors, s.Source.InFlight,
		s.Source.SourcelessRetry, utils.FormatBytesShort(s.Source.SourcelessRetryBytes),
		formatLatency(s.Source.Latency))
	b.WriteString("] peer[")
	fmt.Fprintf(&b, "hits=%d miss=%d hit_bytes=%s miss_bytes=%s timeout=%d blacklist=%d cksum_err=%d inflight=%d %s",
		s.Peer.Hits, s.Peer.Miss,
		utils.FormatBytesShort(s.Peer.HitBytes), utils.FormatBytesShort(s.Peer.MissBytes),
		s.Peer.Timeouts, s.Peer.Blacklisted, s.Peer.ChecksumErrors, s.Peer.InFlight,
		formatLatency(s.Peer.Latency))
	b.WriteString("]")
	return b.String()
}

// formatLatency renders a latency histogram as one group. "ok" means the figures cover successful
// operations only, which is all these histograms observe.
func formatLatency(h metric.HistSnapshot) string {
	if h.Count == 0 {
		// Printing avg=0s p50<=0s would read as "everything was instant" when nothing succeeded.
		return "ok(n=0)"
	}
	return fmt.Sprintf("ok(n=%d avg=%s p50%s p90%s p99%s)",
		h.Count, h.FormatAvg(),
		h.FormatQuantile(0.5), h.FormatQuantile(0.9), h.FormatQuantile(0.99))
}
