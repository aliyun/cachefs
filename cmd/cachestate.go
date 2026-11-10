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
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"

	pb "github.com/aliyun/cachefs/pkg/rpc/cacheroot"
	"github.com/aliyun/cachefs/pkg/rpcsvc"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/urfave/cli/v2"
)

// Default cachePort served by the per-instance CachefsState service.
const defCachePort = 7711

func cmdCachestate() *cli.Command {
	return &cli.Command{
		Name:     "cachestate",
		Category: "INSPECTOR",
		Usage:    "Query the per-instance CachefsState service of a single CacheFS node",
		Description: `
Query a single CacheFS instance's CachefsState service (served on cachePort).

Examples:
# Per-instance cache state of one node
$ cachefs cachestate state --addr 127.0.0.1:7711

# Local tracker (which peers hold which blocks) of one node, filtered by path
$ cachefs cachestate tracker /data/dir1 --addr 127.0.0.1:7711
`,
		Subcommands: []*cli.Command{
			cmdCachestateState(),
			cmdCachestateTracker(),
		},
	}
}

func cachestateAddrFlag() cli.Flag {
	return &cli.StringFlag{
		Name:    "addr",
		Aliases: []string{"a"},
		Value:   fmt.Sprintf("127.0.0.1:%d", defCachePort),
		Usage:   "address (host:port) of the CacheFS instance's CachefsState service",
	}
}

func cmdCachestateState() *cli.Command {
	return &cli.Command{
		Name:   "state",
		Action: cachestateState,
		Usage:  "Show the per-instance cache state of one node",
		Flags: []cli.Flag{
			cachestateAddrFlag(),
			cacherootFormatFlag(),
			cacherootTimeoutFlag(),
		},
	}
}

func cmdCachestateTracker() *cli.Command {
	return &cli.Command{
		Name:      "tracker",
		Action:    cachestateTracker,
		ArgsUsage: "[PATH ...]",
		Usage:     "Show the tracker (which nodes hold which blocks) of one node",
		Flags: []cli.Flag{
			cachestateAddrFlag(),
			cacherootFormatFlag(),
			cacherootTimeoutFlag(),
			&cli.BoolFlag{
				Name:    "relative",
				Aliases: []string{"r"},
				Usage:   "treat PATH as relative to the mount point instead of a local absolute path",
			},
			&cli.BoolFlag{
				Name:    "local",
				Aliases: []string{"l"},
				Usage:   "show only this node's own cache distribution instead of the cluster tracker",
			},
		},
	}
}

func cachestateState(c *cli.Context) error {
	conn, err := rpcsvc.Dial(c.String("addr"))
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := cacherootContext(c)
	defer cancel()
	st, err := pb.NewCachefsStateClient(conn).GetCacheState(ctx, &pb.GetCacheStateRequest{})
	if err != nil {
		return err
	}

	node := c.String("addr")
	format := c.String("format")
	switch format {
	case "json":
		return writeJSON(statsRow{
			Node:          node,
			Ok:            true,
			Capacity:      st.GetCapacity(),
			Used:          st.GetUsed(),
			HashhitBlocks: st.GetHashhitBlocks(),
			HashhitBytes:  st.GetHashhitBytes(),
			CachedBlocks:  st.GetCachedBlocks(),
			CachedBytes:   st.GetCachedBytes(),
			PinnedBlocks:  st.GetPinnedBlocks(),
			PinnedBytes:   st.GetPinnedBytes(),
		})
	case "csv":
		w := csv.NewWriter(os.Stdout)
		_ = w.Write([]string{"node", "capacity", "used", "hashhit_blocks", "hashhit_bytes", "cached_blocks", "cached_bytes", "pinned_blocks", "pinned_bytes"})
		_ = w.Write([]string{
			node,
			strconv.FormatUint(st.GetCapacity(), 10), strconv.FormatUint(st.GetUsed(), 10),
			strconv.FormatUint(st.GetHashhitBlocks(), 10), strconv.FormatUint(st.GetHashhitBytes(), 10),
			strconv.FormatUint(st.GetCachedBlocks(), 10), strconv.FormatUint(st.GetCachedBytes(), 10),
			strconv.FormatUint(st.GetPinnedBlocks(), 10), strconv.FormatUint(st.GetPinnedBytes(), 10),
		})
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
	fmt.Printf("%-24s %10s %10s %12d %13s %12d %14s %12d %13s %7.2f%% %9.2f%% %7.2f%% %9.2f%%\n",
		node,
		utils.FormatBytesShort(st.GetCapacity()), utils.FormatBytesShort(st.GetUsed()),
		st.GetHashhitBlocks(), utils.FormatBytesShort(st.GetHashhitBytes()),
		st.GetCachedBlocks(), utils.FormatBytesShort(st.GetCachedBytes()),
		st.GetPinnedBlocks(), utils.FormatBytesShort(st.GetPinnedBytes()),
		ratioPct(st.GetHashhitBlocks(), st.GetCachedBlocks()),
		ratioPct(st.GetHashhitBytes(), st.GetCachedBytes()),
		ratioPct(st.GetPinnedBlocks(), st.GetCachedBlocks()),
		ratioPct(st.GetPinnedBytes(), st.GetCachedBytes()))
	return nil
}

func cachestateTracker(c *cli.Context) error {
	var paths []string
	if c.NArg() > 0 {
		if c.Bool("relative") {
			paths = c.Args().Slice()
		} else {
			absPaths, err := getAbsolutePaths(c.Args().Slice())
			if err != nil {
				return err
			}
			if paths, err = convertToRelativePaths(absPaths); err != nil {
				return err
			}
		}
	}

	conn, err := rpcsvc.Dial(c.String("addr"))
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := cacherootContext(c)
	defer cancel()
	req := &pb.GetTrackerInfoRequest{Paths: paths, Local: c.Bool("local")}
	resp, err := pb.NewCachefsStateClient(conn).GetTrackerInfo(ctx, req)
	if err != nil {
		return err
	}

	format := c.String("format")
	switch format {
	case "json":
		rows := make([]trackerTargetRow, 0, len(resp.GetTargets()))
		for _, t := range resp.GetTargets() {
			nodes := make([]trackerNodeRow, 0, len(t.GetNodes()))
			for _, n := range t.GetNodes() {
				blocks := n.GetBlocks()
				nodes = append(nodes, trackerNodeRow{
					Node:   n.GetNode(),
					Blocks: len(blocks),
					Ranges: compactRanges(blocks),
				})
			}
			rows = append(rows, trackerTargetRow{Target: t.GetTarget(), Nodes: nodes})
		}
		return writeJSON(rows)
	case "csv":
		w := csv.NewWriter(os.Stdout)
		_ = w.Write([]string{"target", "node", "blocks", "ranges"})
		for _, t := range resp.GetTargets() {
			for _, n := range t.GetNodes() {
				blocks := n.GetBlocks()
				_ = w.Write([]string{
					t.GetTarget(), n.GetNode(),
					strconv.Itoa(len(blocks)), compactRanges(blocks),
				})
			}
		}
		w.Flush()
		return w.Error()
	case "plain", "":
	default:
		return fmt.Errorf("unsupported format %q: use plain, json or csv", format)
	}

	type row struct{ target, node, blocks, ranges string }
	head := row{target: "TARGET", node: "NODE", blocks: "BLOCKS", ranges: "RANGES"}
	rows := []row{head}
	for _, t := range resp.GetTargets() {
		for _, n := range t.GetNodes() {
			blocks := n.GetBlocks()
			rows = append(rows, row{
				target: t.GetTarget(),
				node:   n.GetNode(),
				blocks: strconv.Itoa(len(blocks)),
				ranges: compactRanges(blocks),
			})
		}
	}
	var wTarget, wNode, wBlocks int
	for _, r := range rows {
		wTarget = max(wTarget, len(r.target))
		wNode = max(wNode, len(r.node))
		wBlocks = max(wBlocks, len(r.blocks))
	}
	for _, r := range rows {
		fmt.Printf("%-*s  %-*s  %*s  %s\n", wTarget, r.target, wNode, r.node, wBlocks, r.blocks, r.ranges)
	}
	return nil
}

// compactRanges renders an ascending list of block numbers as comma-separated
// ranges, e.g. [0,1,2,3,5,7,8] -> "0-3,5,7-8".
func compactRanges(blocks []uint32) string {
	if len(blocks) == 0 {
		return ""
	}
	var b strings.Builder
	start := blocks[0]
	prev := blocks[0]
	flush := func(lo, hi uint32) {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		if lo == hi {
			b.WriteString(strconv.FormatUint(uint64(lo), 10))
		} else {
			b.WriteString(strconv.FormatUint(uint64(lo), 10))
			b.WriteByte('-')
			b.WriteString(strconv.FormatUint(uint64(hi), 10))
		}
	}
	for _, blk := range blocks[1:] {
		if blk == prev+1 {
			prev = blk
			continue
		}
		flush(start, prev)
		start = blk
		prev = blk
	}
	flush(start, prev)
	return b.String()
}

// trackerTargetRow / trackerNodeRow carry the json output of the tracker subcommand.
type trackerTargetRow struct {
	Target string           `json:"target"`
	Nodes  []trackerNodeRow `json:"nodes"`
}

type trackerNodeRow struct {
	Node   string `json:"node"`
	Blocks int    `json:"blocks"`           // number of blocks this node holds
	Ranges string `json:"ranges,omitempty"` // held block numbers as compact ranges, e.g. "0,4,14-15"
}
