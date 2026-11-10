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

package cmd

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	pb "github.com/aliyun/cachefs/pkg/rpc/cacheroot"
	"github.com/aliyun/cachefs/pkg/rpcsvc"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/urfave/cli/v2"
)

func cmdCacheroot() *cli.Command {
	return &cli.Command{
		Name:     "cacheroot",
		Category: "TOOL",
		Usage:    "Query the CacheFS cache root cluster",
		Description: `
Query cache progress and per-instance cache state from a cache root cluster.

Examples:
# Cache ratio of one or more paths
$ cachefs cacheroot ratio /data/dir1 /data/dir2 --addr cacheroot.nlb:80

# Per-instance cache state of the whole cluster
$ cachefs cacheroot stats --addr cacheroot.nlb:80
`,
		Subcommands: []*cli.Command{
			cmdCacherootRatio(),
			cmdCacherootStats(),
		},
	}
}

func cacherootAddrFlag() cli.Flag {
	return &cli.StringFlag{
		Name:    "addr",
		Aliases: []string{"a"},
		Value:   fmt.Sprintf("127.0.0.1:%d", defCacherootTrackerPort),
		Usage:   "address (host:port) of any cache root instance or the cluster NLB",
	}
}

func cmdCacherootRatio() *cli.Command {
	return &cli.Command{
		Name:      "ratio",
		Action:    cacherootRatio,
		ArgsUsage: "PATH [PATH ...]",
		Usage:     "Show cache progress (cached/total) of the given paths",
		Flags: []cli.Flag{
			cacherootAddrFlag(),
			cacherootFormatFlag(),
			cacherootTimeoutFlag(),
			&cli.BoolFlag{
				Name:    "relative",
				Aliases: []string{"r"},
				Usage:   "treat PATH as relative to the mount point instead of a local absolute path",
			},
			&cli.BoolFlag{
				Name:    "list-nodes",
				Aliases: []string{"l"},
				Usage:   "also break down the cached amount per cluster node",
			},
		},
	}
}

func cmdCacherootStats() *cli.Command {
	return &cli.Command{
		Name:   "stats",
		Action: cacherootStats,
		Usage:  "Show per-instance cache state of the whole cluster",
		Flags: []cli.Flag{
			cacherootAddrFlag(),
			cacherootFormatFlag(),
			cacherootTimeoutFlag(),
		},
	}
}

func cacherootFormatFlag() cli.Flag {
	return &cli.StringFlag{
		Name:    "format",
		Aliases: []string{"f"},
		Value:   "plain",
		Usage:   "output format: plain, json or csv (json/csv emit raw, unformatted values)",
	}
}

func cacherootTimeoutFlag() cli.Flag {
	return &cli.UintFlag{
		Name:    "timeout",
		Aliases: []string{"t"},
		Usage:   "RPC timeout in seconds (0 means no timeout)",
	}
}

func cacherootContext(c *cli.Context) (context.Context, context.CancelFunc) {
	timeout := c.Uint("timeout")
	if timeout == 0 {
		return context.Background(), func() {}
	}
	return context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
}

func ratioPct(cached, total uint64) float64 {
	if total == 0 {
		return 0
	}
	return float64(cached) * 100 / float64(total)
}

func cacherootRatio(c *cli.Context) error {
	if c.NArg() == 0 {
		return fmt.Errorf("at least one PATH is required")
	}

	var paths []string
	if c.Bool("relative") {
		paths = c.Args().Slice()
	} else {
		// Convert local absolute paths to paths relative to the mount point.
		absPaths, err := getAbsolutePaths(c.Args().Slice())
		if err != nil {
			return err
		}
		if paths, err = convertToRelativePaths(absPaths); err != nil {
			return err
		}
	}

	conn, err := rpcsvc.Dial(c.String("addr"))
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := cacherootContext(c)
	defer cancel()
	req := &pb.GetCacheRatioRequest{
		Paths:   paths,
		Verbose: c.Bool("list-nodes"),
	}
	resp, err := pb.NewCacheRootClusterClient(conn).GetCacheRatio(ctx, req)
	if err != nil {
		return err
	}

	format := c.String("format")
	switch format {
	case "json":
		rows := make([]ratioRow, 0, len(resp.GetRatios()))
		for _, r := range resp.GetRatios() {
			var nodes []nodeRow
			if len(r.GetNodes()) > 0 {
				nodes = make([]nodeRow, 0, len(r.GetNodes()))
				for _, n := range r.GetNodes() {
					nodes = append(nodes, nodeRow{
						Node:         n.GetNode(),
						CachedBlocks: n.GetCachedBlocks(),
						CachedBytes:  n.GetCachedBytes(),
					})
				}
			}
			rows = append(rows, ratioRow{
				Path:         r.GetPath(),
				Ok:           r.GetOk(),
				Error:        r.GetError(),
				TotalBlocks:  r.GetTotalBlocks(),
				TotalBytes:   r.GetTotalBytes(),
				CachedBlocks: r.GetCachedBlocks(),
				CachedBytes:  r.GetCachedBytes(),
				Nodes:        nodes,
			})
		}
		return writeJSON(rows)
	case "csv":
		w := csv.NewWriter(os.Stdout)
		// The node column is empty on the per-path summary row; --list-nodes adds one row
		// per node (its cached_*), repeating total_* so consumers can compute the percentage.
		_ = w.Write([]string{"path", "node", "ok", "error", "total_blocks", "total_bytes", "cached_blocks", "cached_bytes"})
		for _, r := range resp.GetRatios() {
			_ = w.Write([]string{
				r.GetPath(), "", strconv.FormatBool(r.GetOk()), r.GetError(),
				strconv.FormatUint(r.GetTotalBlocks(), 10), strconv.FormatUint(r.GetTotalBytes(), 10),
				strconv.FormatUint(r.GetCachedBlocks(), 10), strconv.FormatUint(r.GetCachedBytes(), 10),
			})
			for _, n := range r.GetNodes() {
				_ = w.Write([]string{
					r.GetPath(), n.GetNode(), strconv.FormatBool(r.GetOk()), r.GetError(),
					strconv.FormatUint(r.GetTotalBlocks(), 10), strconv.FormatUint(r.GetTotalBytes(), 10),
					strconv.FormatUint(n.GetCachedBlocks(), 10), strconv.FormatUint(n.GetCachedBytes(), 10),
				})
			}
		}
		w.Flush()
		return w.Error()
	case "plain", "":
	default:
		return fmt.Errorf("unsupported format %q: use plain, json or csv", format)
	}

	type row struct {
		path, blocks, blockPct, bytes, bytePct string
	}
	// Header participates in width calculation so columns stay aligned.
	head := row{path: "PATH", blocks: "BLOCKS", blockPct: "BLOCK%", bytes: "BYTES", bytePct: "BYTE%"}
	rows := []row{head}
	for _, r := range resp.GetRatios() {
		// Silently skip paths that could not be resolved on the server.
		if !r.GetOk() {
			continue
		}
		rows = append(rows, row{
			path:     r.GetPath(),
			blocks:   fmt.Sprintf("%d/%d", r.GetCachedBlocks(), r.GetTotalBlocks()),
			blockPct: fmt.Sprintf("%.2f%%", cachePct(r.GetCachedBlocks(), r.GetTotalBlocks())),
			bytes:    fmt.Sprintf("%s/%s", utils.FormatBytesShort(r.GetCachedBytes()), utils.FormatBytesShort(r.GetTotalBytes())),
			bytePct:  fmt.Sprintf("%.2f%%", cachePct(r.GetCachedBytes(), r.GetTotalBytes())),
		})
		// --list-nodes: one indented sub-row per node, its cached amount over the path total.
		for _, n := range r.GetNodes() {
			rows = append(rows, row{
				path:     "  " + n.GetNode(),
				blocks:   fmt.Sprintf("%d/%d", n.GetCachedBlocks(), r.GetTotalBlocks()),
				blockPct: fmt.Sprintf("%.2f%%", cachePct(n.GetCachedBlocks(), r.GetTotalBlocks())),
				bytes:    fmt.Sprintf("%s/%s", utils.FormatBytesShort(n.GetCachedBytes()), utils.FormatBytesShort(r.GetTotalBytes())),
				bytePct:  fmt.Sprintf("%.2f%%", cachePct(n.GetCachedBytes(), r.GetTotalBytes())),
			})
		}
	}
	var wPath, wBlocks, wBlockPct, wBytes, wBytePct int
	for _, r := range rows {
		wPath = max(wPath, len(r.path))
		wBlocks = max(wBlocks, len(r.blocks))
		wBlockPct = max(wBlockPct, len(r.blockPct))
		wBytes = max(wBytes, len(r.bytes))
		wBytePct = max(wBytePct, len(r.bytePct))
	}

	for _, r := range rows {
		fmt.Printf("%-*s  %*s  %*s  %*s  %*s\n",
			wPath, r.path, wBlocks, r.blocks, wBlockPct, r.blockPct, wBytes, r.bytes, wBytePct, r.bytePct)
	}
	return nil
}

// Like ratioPct but treats an empty target (total == 0, e.g. empty files or
// directories) as fully cached.
func cachePct(cached, total uint64) float64 {
	if total == 0 {
		return 100
	}
	return ratioPct(cached, total)
}

func cacherootStats(c *cli.Context) error {
	conn, err := rpcsvc.Dial(c.String("addr"))
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := cacherootContext(c)
	defer cancel()
	resp, err := pb.NewCacheRootClusterClient(conn).GetCacheStats(ctx, &pb.GetCacheStatsRequest{})
	if err != nil {
		return err
	}

	format := c.String("format")
	switch format {
	case "json":
		rows := make([]statsRow, 0, len(resp.GetInstances()))
		for _, ins := range resp.GetInstances() {
			st := ins.GetState()
			rows = append(rows, statsRow{
				Node:          ins.GetNode(),
				Ok:            ins.GetOk(),
				Error:         ins.GetError(),
				Capacity:      st.GetCapacity(),
				Used:          st.GetUsed(),
				HashhitBlocks: st.GetHashhitBlocks(),
				HashhitBytes:  st.GetHashhitBytes(),
				CachedBlocks:  st.GetCachedBlocks(),
				CachedBytes:   st.GetCachedBytes(),
				PinnedBlocks:  st.GetPinnedBlocks(),
				PinnedBytes:   st.GetPinnedBytes(),
			})
		}
		return writeJSON(rows)
	case "csv":
		w := csv.NewWriter(os.Stdout)
		_ = w.Write([]string{"node", "ok", "error", "capacity", "used", "hashhit_blocks", "hashhit_bytes", "cached_blocks", "cached_bytes", "pinned_blocks", "pinned_bytes"})
		for _, ins := range resp.GetInstances() {
			st := ins.GetState()
			_ = w.Write([]string{
				ins.GetNode(), strconv.FormatBool(ins.GetOk()), ins.GetError(),
				strconv.FormatUint(st.GetCapacity(), 10), strconv.FormatUint(st.GetUsed(), 10),
				strconv.FormatUint(st.GetHashhitBlocks(), 10), strconv.FormatUint(st.GetHashhitBytes(), 10),
				strconv.FormatUint(st.GetCachedBlocks(), 10), strconv.FormatUint(st.GetCachedBytes(), 10),
				strconv.FormatUint(st.GetPinnedBlocks(), 10), strconv.FormatUint(st.GetPinnedBytes(), 10),
			})
		}
		w.Flush()
		return w.Error()
	case "plain", "":
	default:
		return fmt.Errorf("unsupported format %q: use plain, json or csv", format)
	}

	fmt.Printf("%-24s %10s %10s %12s %13s %12s %14s %12s %13s %8s %10s %8s %10s\n",
		"NODE", "CAPACITY", "USED",
		"HASHHIT_BLK", "HASHHIT_BYTES", "CACHED_BLK", "CACHED_BYTES",
		"PINNED_BLK", "PINNED_BYTES",
		"HIT%_BLK", "HIT%_BYTES", "PIN%_BLK", "PIN%_BYTES")
	for _, ins := range resp.GetInstances() {
		if !ins.GetOk() {
			continue
		}
		st := ins.GetState()
		fmt.Printf("%-24s %10s %10s %12d %13s %12d %14s %12d %13s %7.2f%% %9.2f%% %7.2f%% %9.2f%%\n",
			ins.GetNode(),
			utils.FormatBytesShort(st.GetCapacity()), utils.FormatBytesShort(st.GetUsed()),
			st.GetHashhitBlocks(), utils.FormatBytesShort(st.GetHashhitBytes()),
			st.GetCachedBlocks(), utils.FormatBytesShort(st.GetCachedBytes()),
			st.GetPinnedBlocks(), utils.FormatBytesShort(st.GetPinnedBytes()),
			ratioPct(st.GetHashhitBlocks(), st.GetCachedBlocks()),
			ratioPct(st.GetHashhitBytes(), st.GetCachedBytes()),
			ratioPct(st.GetPinnedBlocks(), st.GetCachedBlocks()),
			ratioPct(st.GetPinnedBytes(), st.GetCachedBytes()))
	}
	return nil
}

// ratioRow / statsRow carry the raw, unformatted values emitted by the json and
// csv output formats.
type ratioRow struct {
	Path         string    `json:"path"`
	Ok           bool      `json:"ok"`
	Error        string    `json:"error,omitempty"`
	TotalBlocks  uint64    `json:"total_blocks"`
	TotalBytes   uint64    `json:"total_bytes"`
	CachedBlocks uint64    `json:"cached_blocks"`
	CachedBytes  uint64    `json:"cached_bytes"`
	Nodes        []nodeRow `json:"nodes,omitempty"` // per-node breakdown, only with --list-nodes
}

// nodeRow is a per-node share of a path's cached data (raw amounts; the percentage is
// computed against the path's total by the plain formatter or the json/csv consumer).
type nodeRow struct {
	Node         string `json:"node"`
	CachedBlocks uint64 `json:"cached_blocks"`
	CachedBytes  uint64 `json:"cached_bytes"`
}

type statsRow struct {
	Node          string `json:"node"`
	Ok            bool   `json:"ok"`
	Error         string `json:"error,omitempty"`
	Capacity      uint64 `json:"capacity"`
	Used          uint64 `json:"used"`
	HashhitBlocks uint64 `json:"hashhit_blocks"`
	HashhitBytes  uint64 `json:"hashhit_bytes"`
	CachedBlocks  uint64 `json:"cached_blocks"`
	CachedBytes   uint64 `json:"cached_bytes"`
	PinnedBlocks  uint64 `json:"pinned_blocks"`
	PinnedBytes   uint64 `json:"pinned_bytes"`
}

func writeJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
