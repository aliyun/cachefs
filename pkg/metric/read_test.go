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
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestReadHist(t *testing.T) {
	h := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "test_hist",
		Buckets: []float64{1, 2, 4},
	})
	for _, v := range []float64{0.5, 1.5, 1.7, 3, 100} {
		h.Observe(v)
	}

	got := ReadHist(h)
	if got.Count != 5 {
		t.Errorf("Count = %d, want 5", got.Count)
	}
	if want := 0.5 + 1.5 + 1.7 + 3 + 100; math.Abs(got.Sum-want) > 1e-9 {
		t.Errorf("Sum = %v, want %v", got.Sum, want)
	}

	// Buckets are cumulative and hold the finite buckets only: Prometheus keeps
	// its +Inf bucket implicit and so does the snapshot. The 100 observation is
	// therefore in Count but in no bucket, leaving the last cumulative count (4)
	// short of Count (5). Quantile depends on spotting that gap.
	want := []HistBucket{
		{UpperBound: 1, Count: 1}, // 0.5
		{UpperBound: 2, Count: 3}, // + 1.5, 1.7
		{UpperBound: 4, Count: 4}, // + 3
	}
	if len(got.Buckets) != len(want) {
		// Return rather than fall through: the loop below indexes by want's length.
		t.Errorf("Buckets = %v, want %v", got.Buckets, want)
		return
	}
	for i := range want {
		if got.Buckets[i] != want[i] {
			t.Errorf("Buckets[%d] = %v, want %v", i, got.Buckets[i], want[i])
		}
	}
	if overflow := got.Count - got.Buckets[len(got.Buckets)-1].Count; overflow != 1 {
		t.Errorf("Count - last bucket = %d, want 1 (the 100s observation)", overflow)
	}
}

func TestReadHistDropsInfBucket(t *testing.T) {
	// Write() normally emits finite buckets only, but it appends an explicit +Inf
	// bucket when an exemplar is attached to it. That bound has to be dropped:
	// JSON cannot encode infinity, so it would fail the marshal of the whole
	// report rather than degrade it. Feed a dto.Metric with such a bucket
	// directly, since only ObserveWithExemplar produces one and nothing in the
	// tree calls it yet.
	h := &infBucketHist{buckets: []float64{1, 2}, counts: []uint64{1, 3}, count: 5, sum: 42}

	got := ReadHist(h)
	if got.Count != 5 || got.Sum != 42 {
		t.Errorf("Count/Sum = %d/%v, want 5/42", got.Count, got.Sum)
	}
	for _, b := range got.Buckets {
		if math.IsInf(b.UpperBound, +1) {
			// Break, not return: the Marshal check below then fails too, which is
			// the consequence this bound actually has.
			t.Errorf("Buckets kept a +Inf bound: %v", got.Buckets)
			break
		}
	}
	if len(got.Buckets) != 2 {
		t.Errorf("Buckets = %v, want the 2 finite ones", got.Buckets)
	}
	if _, err := json.Marshal(got); err != nil {
		t.Errorf("Marshal: %s", err)
	}
}

// A prometheus.Histogram whose Write emits a trailing +Inf bucket, as the real
// one does once an exemplar is attached to it.
type infBucketHist struct {
	prometheus.Histogram
	buckets []float64
	counts  []uint64
	count   uint64
	sum     float64
}

func (h *infBucketHist) Write(out *dto.Metric) error {
	dh := &dto.Histogram{
		SampleCount: &h.count,
		SampleSum:   &h.sum,
	}
	for i, ub := range h.buckets {
		bound, cnt := ub, h.counts[i]
		dh.Bucket = append(dh.Bucket, &dto.Bucket{UpperBound: &bound, CumulativeCount: &cnt})
	}
	inf, cnt := math.Inf(+1), h.count
	dh.Bucket = append(dh.Bucket, &dto.Bucket{UpperBound: &inf, CumulativeCount: &cnt})
	out.Histogram = dh
	return nil
}

func TestReadHistNil(t *testing.T) {
	// A nil histogram must not panic: in fuse-layer mode and in unit tests a
	// bare struct can carry nil metric fields.
	if got := ReadHist(nil); got.Count != 0 || got.Sum != 0 || got.Buckets != nil {
		t.Errorf("ReadHist(nil) = %+v, want zero value", got)
	}
}

// Ten observations, nine of them in finite buckets (cumulative 2/5/9) and one
// past the widest bound, sitting in the implicit +Inf bucket. This is the shape
// ReadHist actually produces.
var quantileSnap = HistSnapshot{
	Count: 10,
	Buckets: []HistBucket{
		{UpperBound: 1, Count: 2},
		{UpperBound: 2, Count: 5},
		{UpperBound: 4, Count: 9},
	},
}

func TestHistSnapshotQuantile(t *testing.T) {
	cases := []struct {
		name string
		q    float64
		want float64
		ok   bool
	}{
		// rank = ceil(q*10); first bucket with cumulative >= rank wins.
		{"p10 rank1 first bucket", 0.10, 1, true},
		{"p20 rank2 exactly fills first bucket", 0.20, 1, true},
		// The boundary that off-by-one errors get wrong: rank 3 exceeds the
		// first bucket's 2, so it must move to the second, not stay.
		{"p21 rank3 crosses into second", 0.21, 2, true},
		{"p50 rank5 exactly fills second", 0.50, 2, true},
		{"p51 rank6 crosses into third", 0.51, 4, true},
		{"p90 rank9 exactly fills third", 0.90, 4, true},
		// The one that must not be clamped: rank 10 is beyond every finite
		// bucket. Answering 4 here would claim a tail latency the histogram
		// cannot vouch for -- the observation may have taken any amount of time.
		{"p91 rank10 overflows to +Inf", 0.91, math.Inf(+1), true},
		{"p100 rank10 overflows to +Inf", 1.00, math.Inf(+1), true},
		// q=0 gives rank 0, which every bucket satisfies vacuously; clamped to
		// rank 1 so the answer is the lowest bucket rather than nonsense.
		{"p0 clamps to rank1", 0, 1, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := quantileSnap.Quantile(c.q)
			if ok != c.ok || got != c.want {
				t.Errorf("Quantile(%v) = (%v, %v), want (%v, %v)", c.q, got, ok, c.want, c.ok)
			}
		})
	}
}

func TestHistSnapshotFormatQuantile(t *testing.T) {
	cases := []struct {
		name string
		snap HistSnapshot
		q    float64
		want string
	}{
		{"in range renders as upper bound", quantileSnap, 0.50, "<=2s"},
		// Overflow must read as "greater than the widest bound we measured",
		// not as that bound, and not as "+Inf" with no scale attached.
		{"overflow renders as greater-than", quantileSnap, 1.00, ">4s"},
		// An empty histogram must not render as 0s: that reads as "every
		// request was instant" when in fact none of them succeeded.
		{"no observations renders as dash", HistSnapshot{}, 0.50, "-"},
		{"sub-millisecond bound", HistSnapshot{
			Count:   1,
			Buckets: []HistBucket{{UpperBound: 0.0005, Count: 1}},
		}, 0.50, "<=500µs"},
		{"fractional second bound is truncated", HistSnapshot{
			Count:   1,
			Buckets: []HistBucket{{UpperBound: 1.2909, Count: 1}},
		}, 0.50, "<=1.29s"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.snap.FormatQuantile(c.q); got != c.want {
				t.Errorf("FormatQuantile(%v) = %q, want %q", c.q, got, c.want)
			}
		})
	}
}

func TestHistSnapshotFormatAvg(t *testing.T) {
	cases := []struct {
		name string
		snap HistSnapshot
		want string
	}{
		{"no observations", HistSnapshot{}, "-"},
		// The case that matters. These are the real figures from a 60MiB read off
		// a local-directory source: every sample landed under the first bucket
		// bound, so all quantiles collapsed onto it and the average was the only
		// number left carrying information. Rounding it to "4ms" would discard it.
		{"sub-10ms keeps two decimals", HistSnapshot{Count: 15, Sum: 0.068924254}, "4.59ms"},
		{"sub-millisecond", HistSnapshot{Count: 4, Sum: 0.002}, "500µs"},
		// Past 10ms the extra digits stop mattering; nearby bucket bounds are
		// coarser than a millisecond anyway.
		{"above 10ms truncates to ms", HistSnapshot{Count: 3, Sum: 0.0456}, "15ms"},
		{"seconds", HistSnapshot{Count: 2, Sum: 2.5819}, "1.29s"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.snap.FormatAvg(); got != c.want {
				t.Errorf("FormatAvg() = %q, want %q", got, c.want)
			}
		})
	}
}

func TestHistSnapshotJSONRoundTrip(t *testing.T) {
	// JSON has no infinity, so a +Inf bucket bound would make Marshal fail and
	// take the whole RPC response with it. Keeping +Inf implicit is what makes
	// the snapshot encodable; pin that.
	b, err := json.Marshal(ReadHist(func() prometheus.Histogram {
		h := prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "test_hist_json",
			Buckets: []float64{1, 2},
		})
		h.Observe(0.5)
		h.Observe(1000) // overflows into the implicit +Inf bucket
		return h
	}()))
	if err != nil {
		// Marshal failing is the whole point of the test; b is nil, so comparing it
		// below would only add a second, derived error.
		t.Errorf("Marshal: %s", err)
		return
	}
	want := `{"count":2,"sum":1000.5,"buckets":[{"le":1,"n":1},{"le":2,"n":1}]}`
	if string(b) != want {
		t.Errorf("Marshal =\n  %s\nwant\n  %s", b, want)
	}
}

func TestHistSnapshotQuantileEmpty(t *testing.T) {
	// No observations: ok must be false so callers render "-" instead of a
	// meaningless 0s, which would read as "everything was instant".
	for _, snap := range []HistSnapshot{
		{},
		{Count: 0, Buckets: []HistBucket{{UpperBound: 1, Count: 0}}},
		{Count: 5}, // count without buckets
	} {
		if got, ok := snap.Quantile(0.5); ok {
			t.Errorf("Quantile on %+v = (%v, true), want ok=false", snap, got)
		}
	}
}

func TestHistSnapshotAvg(t *testing.T) {
	cases := []struct {
		name string
		snap HistSnapshot
		want time.Duration
	}{
		{"empty", HistSnapshot{}, 0},
		{"sum without count", HistSnapshot{Sum: 3}, 0},
		{"1.5s over 3", HistSnapshot{Count: 3, Sum: 1.5}, 500 * time.Millisecond},
		{"sub-millisecond", HistSnapshot{Count: 4, Sum: 0.002}, 500 * time.Microsecond},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.snap.Avg(); got != c.want {
				t.Errorf("Avg() = %v, want %v", got, c.want)
			}
		})
	}
}

func TestAtomicCounterLoadMatchesGather(t *testing.T) {
	// The load-stats report reads counters via AtomicCounter.Load while .stats
	// and /metrics read the same counters via the registry Gather path. If the
	// two ever disagree the report is lying, so pin them together here.
	c := NewCounter(prometheus.CounterOpts{Name: "test_counter"})
	c.Inc()
	c.Add(2.5)

	if got := c.Load(); got != 3.5 {
		t.Errorf("Load() = %v, want 3.5", got)
	}

	var m dto.Metric
	if err := c.Write(&m); err != nil {
		// Return: with no value written, the comparison below would report a
		// disagreement that is really just this error.
		t.Errorf("Write: %s", err)
		return
	}
	if got := m.GetCounter().GetValue(); got != c.Load() {
		t.Errorf("Gather path = %v, Load() = %v; must agree", got, c.Load())
	}
}

func TestUptime(t *testing.T) {
	// Uptime must share its origin with the uptime gauge, so a report and a
	// scrape taken at the same moment cannot disagree.
	seconds, ok := testUptimeGaugeSeconds(t)
	if !ok {
		return
	}
	gauge := time.Duration(seconds * float64(time.Second))
	got := Uptime()
	if got <= 0 {
		t.Errorf("Uptime() = %v, want > 0", got)
		return
	}
	if d := got - gauge; d < -time.Second || d > time.Second {
		t.Errorf("Uptime() = %v but uptime gauge = %v; different origins", got, gauge)
	}
}

// The uptime gauge is registered anonymously inside initPackageMetrics, so it can
// only be reached the way a scrape reaches it: through a registry.
//
// ok is false when the gauge could not be read, and the caller must stop instead
// of treating the value as zero: a test process has only been up for
// milliseconds, so a zero would land inside the tolerance above and the check
// would pass having compared Uptime() against nothing.
func testUptimeGaugeSeconds(t *testing.T) (float64, bool) {
	t.Helper()
	reg := prometheus.NewRegistry()
	initPackageMetrics(reg)
	mfs, err := reg.Gather()
	if err != nil {
		t.Errorf("Gather: %s", err)
		return 0, false
	}
	for _, mf := range mfs {
		if mf.GetName() == "uptime" {
			return mf.GetMetric()[0].GetGauge().GetValue(), true
		}
	}
	t.Error("no uptime gauge registered by initPackageMetrics")
	return 0, false
}
