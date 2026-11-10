/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
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

package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aliyun/cachefs/cmd/stats"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/aliyun/cachefs/pkg/vfs"
	"github.com/urfave/cli/v2"
)

func cmdStats() *cli.Command {
	return &cli.Command{
		Name:      "stats",
		Action:    statsAction,
		Category:  "INSPECTOR",
		Usage:     "Show real time performance statistics of CacheFS",
		ArgsUsage: "MOUNTPOINT",
		Description: `
This is a tool that reads Prometheus metrics and shows real time statistics of the target mount point.

Examples:
$ cachefs stats /mnt/cfs

# More metrics
$ cachefs stats /mnt/cfs -l 1

# With timestamp (full datetime)
$ cachefs stats /mnt/cfs --schema TufmpcsS

# With time only (no date)
$ cachefs stats /mnt/cfs --schema tufmpcsS

# cachefsd metrics
$ cachefs stats 127.0.0.1:9678/metrics`,

		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "schema",
				Value: "tufmpcsS",
				Usage: "schema string that controls the output sections (T: timestamp, t: time, u: usage, f: fuse, m: meta, p: blockpool, c: cache, s: source, S: server, g: go)",
			},
			&cli.UintFlag{
				Name:  "interval",
				Value: 1,
				Usage: "interval in seconds between each update",
			},
			&cli.UintFlag{
				Name:    "verbosity",
				Aliases: []string{"l"},
				Usage:   "verbosity level, 0 or 1 is enough for most cases",
			},
		},
	}
}

func statsAction(ctx *cli.Context) error {
	interval := ctx.Uint("interval")
	if interval == 0 {
		return cli.Exit("--interval must be at least 1", 1)
	}
	watcher := &stats.StatsWatcher{
		Colorful: !ctx.Bool("no-color") && utils.SupportANSIColor(os.Stdout.Fd()),
		Interval: interval,
	}

	setup(ctx, 1)
	mp := ctx.Args().First()
	schema := ctx.String("schema")

	if _, err := os.Stat(mp); err == nil {
		inode, err := utils.GetFileInode(mp)
		if err != nil {
			logger.Fatalf("lookup inode for %s: %s", mp, err)
		}
		if inode != 1 {
			logger.Fatalf("path %s is not a mount point", mp)
		}
		watcher.Path = path.Join(mp, ".stats")
		if !ctx.IsSet("schema") {
			schema = "tufmpcsS"
		}
		// The memory backend has no disk writeback, so hide its write bandwidth.
		if contents, err := os.ReadFile(path.Join(mp, ".config")); err == nil {
			cfsCfg := vfs.Config{}
			if err := json.Unmarshal(contents, &cfsCfg); err == nil && cfsCfg.Store != nil {
				watcher.MemoryCache = cfsCfg.Store.IsMemoryBackend()
				watcher.Prefault = cfsCfg.BufferPrefault
			}
		}
		watcher.BuildCacheSchema(schema, ctx.Uint("verbosity"))
	} else {
		watcher.Path = mp
		if !strings.HasPrefix(mp, "http://") {
			watcher.Path = "http://" + mp
		}
		if !ctx.IsSet("schema") {
			schema = "tucrS"
		}
		watcher.BuildDaemonSchema(schema, ctx.Uint("verbosity"))
	}
	watcher.FormatHeader()

	var tick uint
	var start, last, current map[string]float64
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	current = stats.ReadStats(watcher.Path)
	start = current
	last = current
	for {
		if tick%(watcher.Interval*30) == 0 {
			fmt.Println(watcher.GetHeader())
		}
		if tick%watcher.Interval == 0 {
			watcher.PrintDiff(start, current, false)
			start = current
		} else {
			watcher.PrintDiff(last, current, true)
		}
		last = current
		tick++
		<-ticker.C
		current = stats.ReadStats(watcher.Path)
	}
}
