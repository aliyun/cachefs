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

package metric

import (
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// HistBucket is one bucket of a histogram snapshot.
type HistBucket struct {
	UpperBound float64 `json:"le"`

	// Cumulative count, following Prometheus semantics: observations <=
	// UpperBound. Not a per-bucket count.
	Count uint64 `json:"n"`
}

// HistSnapshot is a point-in-time copy of a histogram, including its buckets.
//
// Buckets holds the finite buckets only; the +Inf bucket stays implicit, so
// Count - Buckets[len-1].Count is the number of observations past the widest
// finite bound. Quantile relies on that gap, and leaving +Inf out also keeps the
// struct JSON-encodable, since JSON has no infinity.
type HistSnapshot struct {
	Count   uint64       `json:"count"`
	Sum     float64      `json:"sum"` // seconds
	Buckets []HistBucket `json:"buckets"`
}

// ReadHist copies a histogram's current count, sum and buckets. The object is
// read directly rather than looked up by name in a registry.
func ReadHist(h prometheus.Histogram) HistSnapshot {
	if h == nil {
		return HistSnapshot{}
	}
	var m dto.Metric
	if err := h.Write(&m); err != nil {
		logger.Warnf("read histogram: %s", err)
		return HistSnapshot{}
	}
	dh := m.GetHistogram()
	if dh == nil {
		return HistSnapshot{}
	}
	snap := HistSnapshot{
		Count:   dh.GetSampleCount(),
		Sum:     dh.GetSampleSum(),
		Buckets: make([]HistBucket, 0, len(dh.GetBucket())),
	}
	for _, b := range dh.GetBucket() {
		if math.IsInf(b.GetUpperBound(), +1) {
			// Write() emits finite buckets only, except that it appends an
			// explicit +Inf one when an exemplar is attached to it. JSON cannot
			// encode infinity, so letting that through would fail the marshal of
			// the whole report.
			continue
		}
		snap.Buckets = append(snap.Buckets, HistBucket{
			UpperBound: b.GetUpperBound(),
			Count:      b.GetCumulativeCount(),
		})
	}
	return snap
}

// Quantile returns the upper bound of the first bucket whose cumulative count
// reaches q of all observations. ok is false when there are no observations.
//
// No interpolation: both histograms this serves use exponential buckets, so
// interpolating within one would invent precision the buckets never had. Render
// the result as an upper bound ("p99<=1.29s") to keep that visible.
//
// Returns +Inf when the quantile lands past the widest finite bucket. Clamping to
// that bucket would report "p99<=168s" for an observation that took ten minutes,
// exactly when someone is reading the report to investigate a stall.
func (h HistSnapshot) Quantile(q float64) (float64, bool) {
	if h.Count == 0 || len(h.Buckets) == 0 {
		return 0, false
	}
	// Round up, so p50 of a single observation is that observation's bucket
	// rather than the empty prefix before it.
	rank := uint64(math.Ceil(q * float64(h.Count)))
	if rank == 0 {
		rank = 1
	}
	for _, b := range h.Buckets {
		if b.Count >= rank {
			return b.UpperBound, true
		}
	}
	return math.Inf(+1), true // inside the implicit +Inf bucket
}

// FormatQuantile renders a quantile as a bound, since the value is a bucket edge
// rather than a measurement:
//
//	"<=15ms"  the quantile sits in a bucket bounded by 15ms
//	">168s"   it overflowed the widest bucket; the real value is unbounded
//	"-"       no observations. Rendering "0s" here would read as "every request
//	          was instant" when in fact none succeeded.
func (h HistSnapshot) FormatQuantile(q float64) string {
	v, ok := h.Quantile(q)
	if !ok {
		return "-"
	}
	if math.IsInf(v, +1) {
		// Name the widest bound we have, so the reader sees where measurement
		// stopped instead of a bare "Inf".
		widest := h.Buckets[len(h.Buckets)-1].UpperBound
		return ">" + FormatSeconds(widest)
	}
	return "<=" + FormatSeconds(v)
}

// FormatSeconds renders a second-valued duration compactly for single-line
// reports: 500µs, 15ms, 1.29s, 168s.
func FormatSeconds(sec float64) string {
	d := time.Duration(sec * float64(time.Second))
	if d < time.Millisecond {
		return d.Truncate(time.Microsecond).String()
	}
	return d.Truncate(time.Millisecond).String()
}

// Avg returns Sum/Count as a duration, or zero when there are no observations.
func (h HistSnapshot) Avg() time.Duration {
	if h.Count == 0 {
		return 0
	}
	return time.Duration(h.Sum / float64(h.Count) * float64(time.Second))
}

// FormatAvg renders the average, or "-" when there is nothing to average. This
// is an exact figure, not a bound, so it carries no <= prefix.
func (h HistSnapshot) FormatAvg() string {
	if h.Count == 0 {
		return "-"
	}
	d := h.Avg()
	if d < 10*time.Millisecond {
		return d.Truncate(10 * time.Microsecond).String()
	}
	return d.Truncate(time.Millisecond).String()
}

// Uptime returns how long this process has been running. Shares its origin with
// the uptime gauge, so a report and a scrape cannot disagree.
func Uptime() time.Duration {
	return time.Since(start)
}
