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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aliyun/cachefs/pkg/chunk"
	"github.com/aliyun/cachefs/pkg/metric"
	pb "github.com/aliyun/cachefs/pkg/ttrpc/cachefs"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A fully populated report, used by the formatting and JSON tests so both see
// the same figures.
func sampleLoadStats() LoadStats {
	return LoadStats{
		UptimeSeconds: 3723, // 1h2m3s
		Source: SourceLoadStats{
			Reads:                12345,
			Bytes:                48 << 30,
			Errors:               2,
			InFlight:             3,
			SourcelessRetry:      0,
			SourcelessRetryBytes: 0,
			Latency: metric.HistSnapshot{
				Count:   12345,
				Sum:     185.1,
				Buckets: []metric.HistBucket{{UpperBound: 0.015, Count: 8000}, {UpperBound: 0.05, Count: 12000}, {UpperBound: 1.3, Count: 12345}},
			},
		},
		Peer: chunk.PeerLoadStats{
			Hits:           8123,
			Miss:           412,
			HitBytes:       31 << 30,
			MissBytes:      1717986918,
			Timeouts:       1180,
			Blacklisted:    7,
			ChecksumErrors: 0,
			InFlight:       1,
			Latency: metric.HistSnapshot{
				Count:   8123,
				Sum:     40.6,
				Buckets: []metric.HistBucket{{UpperBound: 0.002, Count: 5000}, {UpperBound: 0.008, Count: 7500}, {UpperBound: 0.064, Count: 8123}},
			},
		},
	}
}

func TestLoadStatsFormatSummary(t *testing.T) {
	summary, _ := sampleLoadStats().Format()
	assert.Equal(t,
		"Load stats since mount (1h2m3s): loaded 12345 blocks 48.00GiB from source, "+
			"8123 blocks 31.00GiB from peers (peer 39.7% of 20468 blocks)",
		summary)

	// The summary carries a percent sign, so it must never be handed to a
	// printf-style logger as the format string. Pin that the sign is really
	// there, so the logger.Info call site keeps its reason to exist.
	assert.Contains(t, summary, "%")
	assert.NotContains(t, fmt.Sprintf("%s", summary), "MISSING")
}

func TestLoadStatsFormatDetail(t *testing.T) {
	_, detail := sampleLoadStats().Format()
	assert.Equal(t,
		"Load stats detail: "+
			"source[reads=12345 bytes=48.00GiB errors=2 inflight=3 retry=0 retry_bytes=0B "+
			"ok(n=12345 avg=14ms p50<=15ms p90<=50ms p99<=1.3s)] "+
			"peer[hits=8123 miss=412 hit_bytes=31.00GiB miss_bytes=1.60GiB timeout=1180 "+
			"blacklist=7 cksum_err=0 inflight=1 "+
			// avg keeps two decimals below 10ms: it is the only unbucketed figure
			// here, and the one that still says something when the quantiles
			// collapse onto a single bucket bound.
			"ok(n=8123 avg=4.99ms p50<=2ms p90<=8ms p99<=64ms)]",
		detail)
}

func TestLoadStatsFormatSingleLine(t *testing.T) {
	// Both records must stay on one line. The logger only prefixes the start of
	// a message and appends [file:line] after all of it, so an embedded newline
	// leaves later lines unprefixed and log collectors that split on newlines
	// tear the record into unparseable fragments.
	summary, detail := sampleLoadStats().Format()
	assert.NotContains(t, summary, "\n")
	assert.NotContains(t, detail, "\n")
}

func TestLoadStatsFormatDetailTokens(t *testing.T) {
	// Every key=value must be a single whitespace-delimited token so awk/cut can
	// split the detail line on spaces. Byte sizes are the risk here: the shared
	// byte formatter used to emit "48.00 GiB" with a space in the middle.
	_, detail := sampleLoadStats().Format()
	body := strings.TrimPrefix(detail, "Load stats detail: ")
	for _, field := range strings.Fields(body) {
		if !strings.Contains(field, "=") {
			continue
		}
		value := field[strings.Index(field, "=")+1:]
		value = strings.TrimRight(value, "]")
		assert.NotEmpty(t, value, "field %q has an empty value, so a space split it", field)
	}
	assert.NotContains(t, detail, " GiB")
	assert.NotContains(t, detail, " B ")
}

func TestLoadStatsFormatEmpty(t *testing.T) {
	summary, detail := LoadStats{UptimeSeconds: 5}.Format()

	// Nothing loaded is a different statement from "0% came from peers": there
	// is no ratio yet, and no conclusion to draw about whether P2P works.
	assert.Equal(t, "Load stats since mount (5s): no blocks loaded yet", summary)
	assert.NotContains(t, summary, "%")

	// With no successful reads the latency groups must not claim anything.
	// Rendering avg=0s p50<=0s here would read as "every read was instant" when
	// in fact none happened.
	assert.Contains(t, detail, "source[reads=0 bytes=0B errors=0 inflight=0 retry=0 retry_bytes=0B ok(n=0)]")
	assert.Contains(t, detail, "ok(n=0)]")
	assert.NotContains(t, detail, "p50")
}

func TestLoadStatsFormatPeerOnly(t *testing.T) {
	// The all-peer case is the good outcome, and 100.0% is the number someone
	// checks for. Make sure the denominator does not accidentally include
	// source reads that never happened.
	s := LoadStats{UptimeSeconds: 60}
	s.Peer.Hits = 10
	s.Peer.HitBytes = 10 << 20
	summary, _ := s.Format()
	assert.Contains(t, summary, "(peer 100.0% of 10 blocks)")
	assert.Contains(t, summary, "loaded 0 blocks 0B from source")
}

func TestLoadStatsFormatOverflowQuantile(t *testing.T) {
	// A latency past the widest bucket must render as ">bound", not as the bound
	// itself. This is the case where an honest report matters most: someone is
	// reading it precisely because something took pathologically long.
	s := LoadStats{UptimeSeconds: 60}
	s.Source.Reads = 2
	s.Source.Latency = metric.HistSnapshot{
		Count:   2,
		Sum:     600,
		Buckets: []metric.HistBucket{{UpperBound: 168, Count: 1}},
	}
	_, detail := s.Format()
	assert.Contains(t, detail, "p99>2m48s")
	assert.NotContains(t, detail, "p99<=")
}

func TestLoadStatsJSON(t *testing.T) {
	// This payload is the RPC's wire format, and Go field names must not leak
	// into it: an explicit json tag on every field is what lets the struct be
	// renamed without breaking clients. Pinning the exact bytes is the only
	// thing that actually enforces that, since no Go consumer unmarshals it.
	b, err := json.Marshal(sampleLoadStats())
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"uptime_seconds": 3723,
		"source": {
			"reads": 12345,
			"bytes": 51539607552,
			"errors": 2,
			"in_flight": 3,
			"sourceless_retry": 0,
			"sourceless_retry_bytes": 0,
			"latency": {
				"count": 12345,
				"sum": 185.1,
				"buckets": [{"le":0.015,"n":8000},{"le":0.05,"n":12000},{"le":1.3,"n":12345}]
			}
		},
		"peer": {
			"hits": 8123,
			"miss": 412,
			"hit_bytes": 33285996544,
			"miss_bytes": 1717986918,
			"timeouts": 1180,
			"blacklisted": 7,
			"checksum_errors": 0,
			"in_flight": 1,
			"latency": {
				"count": 8123,
				"sum": 40.6,
				"buckets": [{"le":0.002,"n":5000},{"le":0.008,"n":7500},{"le":0.064,"n":8123}]
			}
		}
	}`, string(b))
}

// newLoadStatsTestServer starts an apiServer for fs over a throwaway socket and
// stops it when the test ends.
func newLoadStatsTestServer(t *testing.T, fs *Cachefs) *apiServer {
	t.Helper()
	server, err := newApiServer(filepath.Join(t.TempDir(), "test.sock"), fs, nil)
	require.NoError(t, err)
	t.Cleanup(server.stop)
	return server
}

// A mount with non-zero peer counters, so a report taken from it is
// distinguishable from a zeroed one.
func newLoadStatsTestFS() *Cachefs {
	return &Cachefs{
		Conf:  &Config{},
		cache: &mockCacheManager{peerStats: chunk.PeerLoadStats{Hits: 7, HitBytes: 1 << 20}},
	}
}

func TestApiServerGetLoadStats(t *testing.T) {
	server := newLoadStatsTestServer(t, newLoadStatsTestFS())

	resp, err := server.GetLoadStats(context.Background(), &pb.GetLoadStatsRequest{})
	require.NoError(t, err)

	// text is the detail line verbatim, so clients in any language get a
	// printable report without parsing anything.
	assert.Contains(t, resp.GetText(), "Load stats detail:")
	assert.Contains(t, resp.GetText(), "hits=7")

	var got LoadStats
	require.NoError(t, json.Unmarshal(resp.GetPayload(), &got))
	assert.Equal(t, uint64(7), got.Peer.Hits)
	assert.Equal(t, uint64(1<<20), got.Peer.HitBytes)
}

func TestApiServerGetLoadStats_Log(t *testing.T) {
	server := newLoadStatsTestServer(t, newLoadStatsTestFS())

	var buf bytes.Buffer
	out, lvl := logger.Out, logger.GetLevel()
	logger.Logger.SetOutput(&buf)
	logger.Logger.SetLevel(logrus.InfoLevel)
	defer func() {
		logger.Logger.SetOutput(out)
		logger.Logger.SetLevel(lvl)
	}()

	_, err := server.GetLoadStats(context.Background(), &pb.GetLoadStatsRequest{Log: true})
	require.NoError(t, err)

	logged := buf.String()
	assert.Contains(t, logged, "Load stats since mount")
	assert.Contains(t, logged, "Load stats detail:")

	// The summary carries a percent sign, so the call site has to be
	// logger.Info, which does not interpret it as a format string. Switching to
	// Infof would leave printf's failure markers in the log.
	assert.NotContains(t, logged, "MISSING")
	assert.NotContains(t, logged, "%!")

	// Two records, each on its own line: the logger only prefixes the start of a
	// message, so an embedded newline would leave a fragment with no timestamp.
	var records int
	for _, line := range strings.Split(strings.TrimRight(logged, "\n"), "\n") {
		if strings.Contains(line, "Load stats") {
			records++
		}
	}
	assert.Equal(t, 2, records, "expected exactly 2 single-line records, got:\n%s", logged)
}

func TestApiServerGetLoadStats_FuseLayerMode(t *testing.T) {
	// No cache layer on purpose: the guard has to reject before anything reads a
	// counter, so a regression that collects first would panic here rather than
	// quietly return a zeroed report.
	server := newLoadStatsTestServer(t, &Cachefs{
		Conf: &Config{
			DaemonSocket: "test", // makes it fuse layer mode
		},
	})

	// Must refuse rather than answer with zeros: peer loads happen in the daemon
	// process, so an all-zero report from here would be read as "P2P is broken".
	resp, err := server.GetLoadStats(context.Background(), &pb.GetLoadStatsRequest{})
	assert.Nil(t, resp)
	require.Error(t, err)
	assert.Equal(t, codes.Unimplemented, status.Code(err),
		"the code must be machine-checkable so clients can tell daemon mode from a real failure")
}
