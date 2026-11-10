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

// Package stats provides a terminal-based real-time stats display engine for
// CacheFS and related tools. It reads Prometheus-format metrics over HTTP and
// renders them as a tabular, optionally colorized view.
package stats

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/cachefs/pkg/utils"
)

var logger = utils.GetLogger("stats")

const (
	BLACK = 30 + iota
	RED
	GREEN
	YELLOW
	BLUE
	MAGENTA
	CYAN
	WHITE
	DEFAULT = "00"
)

const (
	RESET_SEQ      = "\033[0m"
	COLOR_SEQ      = "\033[1;" // %dm
	COLOR_DARK_SEQ = "\033[0;" // %dm
	UNDERLINE_SEQ  = "\033[4m"
)

type StatsWatcher struct {
	Colorful    bool
	Interval    uint
	Path        string
	MemoryCache bool
	Prefault    bool
	header      string
	sections    []*section
}

// NewStatsWatcher creates a StatsWatcher for use by external binaries.
func NewStatsWatcher(colorful bool, interval uint) *StatsWatcher {
	return &StatsWatcher{Colorful: colorful, Interval: interval}
}

// GetHeader returns the formatted header string produced by FormatHeader.
func (w *StatsWatcher) GetHeader() string { return w.header }

func (w *StatsWatcher) colorize(msg string, color int, dark bool, underline bool) string {
	if !w.Colorful || msg == "" || msg == " " {
		return msg
	}
	var cseq, useq string
	if dark {
		cseq = COLOR_DARK_SEQ
	} else {
		cseq = COLOR_SEQ
	}
	if underline {
		useq = UNDERLINE_SEQ
	}
	return fmt.Sprintf("%s%s%dm%s%s", useq, cseq, color, msg, RESET_SEQ)
}

const (
	metricByte = 1 << iota
	metricCount
	metricTime
	metricCPU
	metricGauge
	metricCounter
	metricHist
	metricTimestamp
)

type item struct {
	nick string // must be size <= 5
	name string
	typ  uint8
}

type section struct {
	name  string
	items []*item
}

func (w *StatsWatcher) BuildCacheSchema(schema string, verbosity uint) {
	for _, r := range schema {
		var s section
		switch r {
		case 'T':
			s.name = "timestamp"
			s.items = append(s.items, &item{"date", "date", metricTimestamp})
			s.items = append(s.items, &item{"time", "time", metricTimestamp})
		case 't':
			s.name = "time"
			s.items = append(s.items, &item{"time", "short", metricTimestamp})
		case 'u':
			s.name = "usage"
			s.items = append(s.items, &item{"cpu", "cachefs_cpu_usage", metricCPU | metricCounter})
			s.items = append(s.items, &item{"mem", "cachefs_memory", metricByte | metricGauge})
			s.items = append(s.items, &item{"page", "cachefs_alloc_mem_size", metricByte | metricGauge})
		case 'f':
			s.name = "fuse"
			s.items = append(s.items, &item{"ops", "cachefs_fuse_ops_durations_histogram_seconds", metricTime | metricHist})
			s.items = append(s.items, &item{"read", "cachefs_fuse_read_size_bytes_sum", metricByte | metricCounter})
		case 'm':
			s.name = "meta"
			s.items = append(s.items, &item{"ops", "cachefs_meta_ops_durations_histogram_seconds", metricTime | metricHist})
		case 'p':
			s.name = "blockpool"
			s.items = append(s.items, &item{"pool", "cachefs_block_pool_bytes", metricByte | metricGauge})
			s.items = append(s.items, &item{"reuse", "cachefs_block_pool_reuse_bytes", metricByte | metricCounter})
			if verbosity > 0 {
				s.items = append(s.items, &item{"reu_c", "cachefs_block_pool_reuse", metricCount | metricCounter})
			}
			s.items = append(s.items, &item{"raloc", "cachefs_block_pool_realloc_bytes", metricByte | metricCounter})
			if verbosity > 0 {
				s.items = append(s.items, &item{"ral_c", "cachefs_block_pool_realloc", metricCount | metricCounter})
			}
			if w.Prefault {
				s.items = append(s.items, &item{"preft", "cachefs_block_pool_prefault_bytes", metricByte | metricCounter})
				if verbosity > 1 {
					s.items = append(s.items, &item{"discd", "cachefs_block_pool_prefault_discard_bytes", metricByte | metricCounter})
				}
			}
		case 'c':
			s.name = "cache"
			s.items = append(s.items, &item{"size", "cachefs_cache_bytes", metricByte | metricGauge})
			s.items = append(s.items, &item{"hits", "cachefs_cache_hits", metricCount | metricCounter})
			s.items = append(s.items, &item{"read", "cachefs_cache_hit_bytes", metricByte | metricCounter})
			if verbosity > 0 {
				s.items = append(s.items, &item{"miss", "cachefs_cache_miss", metricCount | metricCounter})
				s.items = append(s.items, &item{"misSZ", "cachefs_cache_miss_bytes", metricByte | metricCounter})
			}

			// disk cache writeback bandwidth (not applicable to the memory backend)
			if !w.MemoryCache {
				s.items = append(s.items, &item{"write", "cachefs_blockcache_writes", metricCount | metricCounter})
				s.items = append(s.items, &item{"wsize", "cachefs_blockcache_write_bytes", metricByte | metricCounter})
			}

			s.items = append(s.items, &item{"rhits", "cachefs_remote_hits", metricCount | metricCounter})
			s.items = append(s.items, &item{"rread", "cachefs_remote_hit_bytes", metricByte | metricCounter})
			s.items = append(s.items, &item{"r_flt", "cachefs_remote_read_flight", metricGauge})
			if verbosity > 0 {
				s.items = append(s.items, &item{"rmiss", "cachefs_remote_miss", metricCount | metricCounter})
				s.items = append(s.items, &item{"rmiSZ", "cachefs_remote_miss_bytes", metricByte | metricCounter})
			}
		case 's':
			s.name = "source"
			s.items = append(s.items, &item{"read", "cachefs_source_reads", metricCount | metricCounter})
			s.items = append(s.items, &item{"size", "cachefs_source_read_bytes", metricByte | metricCounter})
			s.items = append(s.items, &item{"inflt", "cachefs_source_read_flight", metricGauge})
		case 'S':
			s.name = "server"
			s.items = append(s.items, &item{"ups", "cachefs_remote_server_upload_count", metricCount | metricCounter})
			s.items = append(s.items, &item{"size", "cachefs_remote_server_upload_bytes", metricByte | metricCounter})
			if verbosity > 0 {
				s.items = append(s.items, &item{"miss", "cachefs_remote_server_miss_count", metricCount | metricCounter})
				s.items = append(s.items, &item{"misSZ", "cachefs_remote_server_miss_bytes", metricByte | metricCounter})
			}
		case 'g':
			s.name = "go"
			s.items = append(s.items, &item{"alloc", "cachefs_go_memstats_alloc_bytes", metricByte | metricGauge})
			s.items = append(s.items, &item{"sys", "cachefs_go_memstats_sys_bytes", metricByte | metricGauge})
		default:
			fmt.Printf("Warning: no item defined for %c\n", r)
			continue
		}
		w.sections = append(w.sections, &s)
	}
	if len(w.sections) == 0 {
		logger.Fatalln("no section to watch, please check the schema string")
	}
}

func (w *StatsWatcher) BuildDaemonSchema(schema string, verbosity uint) {
	for _, r := range schema {
		var s section
		switch r {
		case 'T':
			s.name = "timestamp"
			s.items = append(s.items, &item{"date", "date", metricTimestamp})
			s.items = append(s.items, &item{"time", "time", metricTimestamp})
		case 't':
			s.name = "time"
			s.items = append(s.items, &item{"time", "short", metricTimestamp})
		case 'u':
			s.name = "usage"
			s.items = append(s.items, &item{"cpu", "cachefsd_cpu_usage", metricCPU | metricCounter})
			s.items = append(s.items, &item{"mem", "cachefsd_memory", metricByte | metricGauge})
			s.items = append(s.items, &item{"page", "cachefsd_alloc_mem_size", metricByte | metricGauge})
		case 'c':
			s.name = "localcache"
			s.items = append(s.items, &item{"hits", "cachefsd_daemon_hits", metricCount | metricCounter})
			s.items = append(s.items, &item{"read", "cachefsd_daemon_hit_bytes", metricByte | metricCounter})
			if verbosity > 0 {
				s.items = append(s.items, &item{"miss", "cachefsd_daemon_miss", metricCount | metricCounter})
				s.items = append(s.items, &item{"misSZ", "cachefsd_daemon_miss_bytes", metricByte | metricCounter})
			}
			s.items = append(s.items, &item{"write", "cachefsd_blockcache_writes", metricCount | metricCounter})
			s.items = append(s.items, &item{"wsize", "cachefsd_blockcache_write_bytes", metricByte | metricCounter})
			s.items = append(s.items, &item{"evict", "cachefsd_blockcache_evicts", metricCount | metricCounter})
			s.items = append(s.items, &item{"clnts", "cachefsd_client_count", metricGauge})
			if verbosity > 0 {
				s.items = append(s.items, &item{"blks", "cachefsd_cache_blocks", metricGauge})
				s.items = append(s.items, &item{"size", "cachefsd_cache_bytes", metricByte | metricGauge})
			}
		case 'r':
			s.name = "remotecache"
			s.items = append(s.items, &item{"hits", "cachefsd_remote_hits", metricCount | metricCounter})
			s.items = append(s.items, &item{"read", "cachefsd_remote_hit_bytes", metricByte | metricCounter})
			s.items = append(s.items, &item{"inflt", "cachefsd_remote_read_flight", metricGauge})
			if verbosity > 0 {
				s.items = append(s.items, &item{"miss", "cachefsd_remote_miss", metricCount | metricCounter})
				s.items = append(s.items, &item{"misSZ", "cachefsd_remote_miss_bytes", metricByte | metricCounter})
			}
		case 'S':
			s.name = "server"
			s.items = append(s.items, &item{"ups", "cachefsd_remote_server_upload_count", metricCount | metricCounter})
			s.items = append(s.items, &item{"size", "cachefsd_remote_server_upload_bytes", metricByte | metricCounter})
			if verbosity > 0 {
				s.items = append(s.items, &item{"miss", "cachefsd_remote_server_miss_count", metricCount | metricCounter})
				s.items = append(s.items, &item{"misSZ", "cachefsd_remote_server_miss_bytes", metricByte | metricCounter})
			}
		case 'g':
			s.name = "go"
			s.items = append(s.items, &item{"alloc", "cachefsd_go_memstats_alloc_bytes", metricByte | metricGauge})
			s.items = append(s.items, &item{"sys", "cachefsd_go_memstats_sys_bytes", metricByte | metricGauge})
		default:
			fmt.Printf("Warning: no item defined for %c\n", r)
			continue
		}
		w.sections = append(w.sections, &s)
	}
	if len(w.sections) == 0 {
		logger.Fatalln("no section to watch, please check the schema string")
	}
}

func padding(name string, width int, char byte) string {
	pad := width - len(name)
	if pad < 0 {
		pad = 0
		name = name[0:width]
	}
	prefix := (pad + 1) / 2
	buf := make([]byte, width)
	for i := 0; i < prefix; i++ {
		buf[i] = char
	}
	copy(buf[prefix:], name)
	for i := prefix + len(name); i < width; i++ {
		buf[i] = char
	}
	return string(buf)
}

// FormatHeader builds and stores the table header string.
func (w *StatsWatcher) FormatHeader() {
	headers := make([]string, len(w.sections))
	subHeaders := make([]string, len(w.sections))
	for i, s := range w.sections {
		subs := make([]string, 0, len(s.items))
		var timestampWidth int
		for _, it := range s.items {
			if it.typ&metricTimestamp != 0 {
				switch it.name {
				case "date":
					subs = append(subs, w.colorize(padding("date", 10, ' '), BLUE, false, true))
					timestampWidth += 10
				case "time":
					subs = append(subs, w.colorize(padding("time", 8, ' '), BLUE, false, true))
					timestampWidth += 8
				case "short":
					subs = append(subs, w.colorize(padding("time", 8, ' '), BLUE, false, true))
					timestampWidth = 8
				}
				continue
			}
			subs = append(subs, w.colorize(padding(it.nick, 5, ' '), BLUE, false, true))
			if it.typ&metricHist != 0 {
				if it.typ&metricTime != 0 {
					subs = append(subs, w.colorize(" lat ", BLUE, false, true))
				} else {
					subs = append(subs, w.colorize(" avg ", BLUE, false, true))
				}
			}
		}
		var width int
		if len(s.items) > 0 && s.items[0].typ&metricTimestamp != 0 {
			if len(s.items) > 1 {
				width = timestampWidth + len(s.items) - 1
			} else {
				width = timestampWidth
			}
		} else {
			width = 6*len(subs) - 1
		}
		subHeaders[i] = strings.Join(subs, " ")
		headers[i] = w.colorize(padding(s.name, width, '-'), BLUE, true, false)
	}
	w.header = fmt.Sprintf("%s\n%s", strings.Join(headers, " "),
		strings.Join(subHeaders, w.colorize("|", BLUE, true, false)))
}

func (w *StatsWatcher) formatU64(v float64, dark, isByte bool) string {
	if v <= 0.0 {
		return w.colorize("   0 ", BLACK, false, false)
	}
	var vi uint64
	var unit string
	var color int
	switch vi = uint64(v); {
	case vi < 10000:
		if isByte {
			unit = "B"
		} else {
			unit = " "
		}
		color = RED
	case vi>>10 < 10000:
		vi, unit, color = vi>>10, "K", YELLOW
	case vi>>20 < 10000:
		vi, unit, color = vi>>20, "M", GREEN
	case vi>>30 < 10000:
		vi, unit, color = vi>>30, "G", BLUE
	case vi>>40 < 10000:
		vi, unit, color = vi>>40, "T", MAGENTA
	default:
		vi, unit, color = vi>>50, "P", CYAN
	}
	return w.colorize(fmt.Sprintf("%4d", vi), color, dark, false) +
		w.colorize(unit, BLACK, false, false)
}

func (w *StatsWatcher) formatTime(v float64, dark bool) string {
	var ret string
	var color int
	switch {
	case v <= 0.0:
		ret, color, dark = "   0 ", BLACK, false
	case v < 10.0:
		ret, color = fmt.Sprintf("%4.2f ", v), GREEN
	case v < 100.0:
		ret, color = fmt.Sprintf("%4.1f ", v), YELLOW
	case v < 10000.0:
		ret, color = fmt.Sprintf("%4.f ", v), RED
	default:
		ret, color = fmt.Sprintf("%1.e", v), MAGENTA
	}
	return w.colorize(ret, color, dark, false)
}

func (w *StatsWatcher) formatCPU(v float64, dark bool) string {
	var ret string
	var color int
	switch v = v * 100.0; {
	case v <= 0.0:
		ret, color = " 0.0", WHITE
	case v < 30.0:
		ret, color = fmt.Sprintf("%4.1f", v), GREEN
	case v < 100.0:
		ret, color = fmt.Sprintf("%4.1f", v), YELLOW
	default:
		ret, color = fmt.Sprintf("%4.f", v), RED
	}
	return w.colorize(ret, color, dark, false) +
		w.colorize("%", BLACK, false, false)
}

// PrintDiff renders one row of stats using the diff between left and right metric maps.
func (w *StatsWatcher) PrintDiff(left, right map[string]float64, dark bool) {
	if !w.Colorful && dark {
		return
	}
	values := make([]string, len(w.sections))
	for i, s := range w.sections {
		vals := make([]string, 0, len(s.items))
		for _, it := range s.items {
			switch it.typ & 0xF0 {
			case metricTimestamp:
				now := time.Now()
				switch it.name {
				case "date":
					vals = append(vals, now.Format("2006-01-02"))
				case "time", "short":
					vals = append(vals, now.Format("15:04:05"))
				}
			case metricGauge:
				if it.typ&metricByte != 0 {
					vals = append(vals, w.formatU64(right[it.name], dark, true))
				} else {
					vals = append(vals, w.formatU64(right[it.name], dark, false))
				}
			case metricCounter:
				v := (right[it.name] - left[it.name])
				if !dark {
					v /= float64(w.Interval)
				}
				if it.typ&metricByte != 0 {
					vals = append(vals, w.formatU64(v, dark, true))
				} else if it.typ&metricCPU != 0 {
					vals = append(vals, w.formatCPU(v, dark))
				} else {
					vals = append(vals, w.formatU64(v, dark, false))
				}
			case metricHist:
				count := right[it.name+"_total"] - left[it.name+"_total"]
				var avg float64
				if count > 0.0 {
					cost := right[it.name+"_sum"] - left[it.name+"_sum"]
					if it.typ&metricTime != 0 {
						cost *= 1000
					}
					avg = cost / count
				}
				if !dark {
					count /= float64(w.Interval)
				}
				vals = append(vals, w.formatU64(count, dark, false), w.formatTime(avg, dark))
			}
		}
		values[i] = strings.Join(vals, " ")
	}
	if w.Colorful && dark {
		fmt.Printf("%s\r", strings.Join(values, w.colorize("|", BLUE, true, false)))
	} else {
		fmt.Printf("%s\n", strings.Join(values, w.colorize("|", BLUE, true, false)))
	}
}

// ReadStats fetches and parses Prometheus-format metrics from the given HTTP
// URL or .stats file path. Returns nil on error.
func ReadStats(path string) map[string]float64 {
	var reader io.Reader
	if !strings.HasSuffix(path, ".stats") {
		resp, err := http.Get(path)
		if err != nil {
			logger.Warnf("http Get %s: %s", path, err)
			return nil
		}
		defer resp.Body.Close()
		reader = resp.Body
	} else {
		f, err := os.Open(path)
		if err != nil {
			logger.Warnf("open %s: %s", path, err)
			return nil
		}
		defer f.Close()
		reader = f
	}
	d, err := io.ReadAll(reader)
	if err != nil {
		logger.Warnf("read %s: %s", path, err)
		return nil
	}
	result := make(map[string]float64)
	for _, line := range strings.Split(string(d), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 {
			result[fields[0]], err = strconv.ParseFloat(fields[1], 64)
			if err != nil {
				logger.Warnf("parse %s: %s", fields[1], err)
			}
		}
	}
	return result
}
