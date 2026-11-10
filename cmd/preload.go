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

package cmd

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	cfsmeta "github.com/aliyun/cachefs/pkg/meta/cachefs"
	"github.com/aliyun/cachefs/pkg/progress"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/aliyun/cachefs/pkg/vfs/apiclient"
	"github.com/urfave/cli/v2"
)

func cmdPreload() *cli.Command {
	return &cli.Command{
		Name:      "preload",
		Action:    preload,
		Category:  "TOOL",
		Usage:     "Build cache for target directories/files",
		ArgsUsage: "[PATH ...]",
		Description: `
This command provides a faster way to actively build cache for the target files.

Examples:
# Preload all files in cachefs
$ cachefs preload /mnt/cachefs

# Preload specified dir/files
$ cat preload.txt
/mnt/cachefs/testdir
/mnt/cachefs/testdir2/testdir3
/mnt/cachefs/testdir4/testfile
$ cachefs preload --file preload.txt`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "file",
				Usage: "file containing a list of paths",
			},
			&cli.BoolFlag{
				Name:    "background",
				Aliases: []string{"d"},
				Usage:   "run in background, command return immediately",
			},
			&cli.BoolFlag{
				Name:    "broadcast",
				Aliases: []string{"b"},
				Usage:   "broadcast preload request to all nodes in serf cluster",
			},
			&cli.BoolFlag{
				Name:    "all",
				Aliases: []string{"a"},
				Value:   true,
				Usage:   "scan & load all specified dirs/files, old cache will be evicted if cache is full",
			},
			&cli.BoolFlag{
				Name:    "noall",
				Aliases: []string{"A"},
				Value:   false,
				Usage:   "disable --all option",
			},
			&cli.BoolFlag{
				Name:    "force",
				Aliases: []string{"f"},
				Value:   true,
				Usage:   "load all blocks even if internal resource is unavailable temporarily, wait for resource to be ready",
			},
			&cli.BoolFlag{
				Name:    "noforce",
				Aliases: []string{"F"},
				Value:   false,
				Usage:   "skip blocks if internal resource is unavailable temporarily",
			},
			&cli.StringFlag{
				Name:    "api-server-sock",
				Aliases: []string{"s"},
				Usage:   "use cachefs API server to send command instead of control file",
			},
			&cli.BoolFlag{
				Name:    "cancel",
				Aliases: []string{"c"},
				Usage:   "cancel the current preload request",
			},
		},
	}
}

// send preload command to controller file
func sendPreload(cf *os.File, batch []string, background, broadcast, loadAll, forceLoad, cancel bool, dspin *progress.DoubleSpinner) {
	paths := strings.Join(batch, "\n")
	var back, bcast, all, force, canc uint8
	if background {
		back = 1
	}
	if broadcast {
		bcast = 1
	}
	if loadAll {
		all = 1
	}
	if forceLoad {
		force = 1
	}
	if cancel {
		canc = 1
	}

	// Total buffer size:
	// cmd: 4
	// payload size: 4
	// paths size: 4
	// paths: len(paths)
	// background: 1
	// broadcast: 1
	// all: 1
	// force: 1
	// cancel: 1
	// padding: 3
	wb := utils.NewBuffer(8 + 4 + 8 + uint32(len(paths)))
	wb.Put32(cfsmeta.CmdPreload)
	wb.Put32(4 + 8 + uint32(len(paths)))
	wb.Put32(uint32(len(paths)))
	wb.Put([]byte(paths))
	wb.Put8(back)
	wb.Put8(bcast)
	wb.Put8(all)
	wb.Put8(force)
	wb.Put8(canc)
	// paddings
	wb.Put16(0)
	wb.Put8(0)

	if _, err := cf.Write(wb.Bytes()); err != nil {
		logger.Fatalf("Write message: %s", err)
	}
	if dspin != nil {
		if errno := readProgress(cf, func(count, bytes uint64) {
			dspin.SetCurrent(int64(count), int64(bytes))
		}); errno != 0 {
			logger.Fatalf("Preload failed: %s", errno)
		}
	}
}

func sendPreloadApiReq(sockPath string, batch []string, background, broadcast, loadAll, forceLoad, cancel bool) (int64, int64, error) {
	client, err := apiclient.NewApiClient(sockPath)
	if err != nil {
		return 0, 0, err
	}
	defer client.Close()

	opts := []apiclient.LoadCacheOption{}
	if background {
		opts = append(opts, apiclient.LoadCacheWithBackground(true))
	}
	if broadcast {
		opts = append(opts, apiclient.LoadCacheWithBroadcast(true))
	}
	if loadAll {
		opts = append(opts, apiclient.LoadCacheWithLoadAll(true))
	}
	if forceLoad {
		opts = append(opts, apiclient.LoadCacheWithForce(true))
	}
	if cancel {
		opts = append(opts, apiclient.LoadCacheWithCancel(true))
	}

	count, bytes, err := client.LoadCache(batch, opts...)
	if err != nil {
		return -1, -1, err
	}
	return int64(count), int64(bytes), nil
}

func getPaths(ctx *cli.Context) ([]string, error) {
	var paths []string
	for _, p := range ctx.Args().Slice() {
		if abs, err := filepath.Abs(p); err == nil {
			paths = append(paths, abs)
		} else {
			return nil, fmt.Errorf("Failed to get absolute path of %s: %s", p, err)
		}
	}
	if fname := ctx.String("file"); fname != "" {
		fd, err := os.Open(fname)
		if err != nil {
			return nil, fmt.Errorf("Failed to open file %s: %s", fname, err)
		}
		defer fd.Close()
		scanner := bufio.NewScanner(fd)
		for scanner.Scan() {
			if p := strings.TrimSpace(scanner.Text()); p != "" {
				if abs, e := filepath.Abs(p); e == nil {
					paths = append(paths, abs)
				} else {
					logger.Warnf("Skipped path %s because it fails to get absolute path: %s", p, e)
				}
			}
		}
		if err = scanner.Err(); err != nil {
			return nil, fmt.Errorf("Reading file %s failed: %s", fname, err)
		}
	}
	return paths, nil
}

func preload(ctx *cli.Context) error {
	setup(ctx, 0)
	broadcast := ctx.Bool("broadcast")
	cancel := ctx.Bool("cancel")
	sockPath := ctx.String("api-server-sock")
	// handle cancel request first if sockPath is specified,
	// as cancel is independent of path argument
	if cancel && sockPath != "" {
		_, _, err := sendPreloadApiReq(sockPath, []string{}, false, broadcast, false, false, true)
		if err == nil {
			logger.Info("Cancel preload done")
		} else {
			logger.Errorf("Cancel preload failed: %s", err)
		}
		return nil
	}

	if ctx.IsSet("all") && ctx.IsSet("noall") {
		logger.Fatal("Both --all and --noall are specified")
	}
	if ctx.IsSet("force") && ctx.IsSet("noforce") {
		logger.Fatal("Both --force and --noforce are specified")
	}

	paths, err := getPaths(ctx)
	if err != nil {
		logger.Fatal(err.Error())
	}
	if len(paths) == 0 {
		logger.Infof("Nothing to preload")
		return nil
	}

	// Find mount point and convert to relative paths
	batch, err := convertToRelativePaths(paths)
	if err != nil {
		logger.Fatal(err.Error())
	}

	// Open controller file for control file mode
	first := paths[0]
	controller := openController(first)
	if controller == nil {
		logger.Fatalf("open control file for %s", first)
	}
	defer controller.Close()

	background := ctx.Bool("background")
	loadAll := ctx.Bool("all") && !ctx.Bool("noall")
	force := ctx.Bool("force") && !ctx.Bool("noforce")

	var prog *progress.Progress
	var dspin *progress.DoubleSpinner
	if sockPath == "" && !cancel {
		prog = progress.NewProgress(background, true)
		dspin = prog.AddDoubleSpinner("Preload")
	}

	var count, bytes int64
	if sockPath == "" {
		sendPreload(controller, batch, background, broadcast, loadAll, force, cancel, dspin)
		if prog != nil {
			prog.Done()
		}
	} else {
		count, bytes, err = sendPreloadApiReq(sockPath, batch, background, broadcast, loadAll, force, false)
	}
	if !background {
		if sockPath == "" && dspin != nil {
			count, bytes = dspin.Current()
		}
		if cancel {
			if err == nil {
				logger.Info("Cancel preload done")
			} else {
				logger.Errorf("Cancel preload failed: %s", err)
			}
		} else {
			if err == nil {
				logger.Infof("Successfully loaded %d files (%d bytes)", count, bytes)
			} else {
				logger.Errorf("Preload failed: %s", err)
			}
		}
	}
	return nil
}
