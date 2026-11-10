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
	"os"
	"strings"

	cfsmeta "github.com/aliyun/cachefs/pkg/meta/cachefs"
	"github.com/aliyun/cachefs/pkg/progress"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/urfave/cli/v2"
)

func cmdClear() *cli.Command {
	return &cli.Command{
		Name:      "clear",
		Action:    clear,
		Category:  "TOOL",
		Usage:     "Clear cache for target directories/files",
		ArgsUsage: "[PATH ...]",
		Description: `
This command provides a way to clear cache for the target dirs/files.

Examples:
# Clear all cache in cachefs
$ cachefs clear /mnt/cachefs

# Clear cache on specified dir/files
$ cat clear.txt
/mnt/cachefs/testdir
/mnt/cachefs/testdir2/testdir3
/mnt/cachefs/testdir4/testfile
$ cachefs clear -f clear.txt

# Clear burst cache back to cache-size
$ cachefs clear --clear-burst /mnt/cachefs

# Clear burst cache on all nodes in serf cluster
$ cachefs clear --clear-burst --broadcast /mnt/cachefs`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "file",
				Aliases: []string{"f"},
				Usage:   "file containing a list of paths",
			},
			&cli.BoolFlag{
				Name:    "broadcast",
				Aliases: []string{"b"},
				Usage:   "broadcast clear cache request to all nodes in serf cluster",
			},
			&cli.BoolFlag{
				Name:    "clear-burst",
				Aliases: []string{"B"},
				Usage:   "clear burst cache back to cache-size; a mount point argument is still required to locate the control file, paths are otherwise ignored",
			},
		},
	}
}

// send clear command to controller file
func sendClear(cf *os.File, relativePaths []string, broadcast bool, clearBurst bool, count, bytes *int64, dspin *progress.DoubleSpinner) {
	paths := strings.Join(relativePaths, "\n")
	logger.Infof("Clear cache of paths: %v, clear_burst: %v", relativePaths, clearBurst)

	var bcast uint8
	if broadcast {
		bcast = 1
	}
	var cburst uint8
	if clearBurst {
		cburst = 1
	}

	// Total buffer size:
	// cmd: 4
	// payload size: 4
	// paths size: 4
	// paths: len(paths)
	// broadcast: 1
	// clear_burst: 1
	// padding: 6
	wb := utils.NewBuffer(8 + 4 + 8 + uint32(len(paths)))
	wb.Put32(cfsmeta.CmdClear)
	wb.Put32(4 + 8 + uint32(len(paths)))
	wb.Put32(uint32(len(paths)))
	wb.Put([]byte(paths))
	wb.Put8(bcast)
	wb.Put8(cburst)
	// paddings
	for i := 0; i < 6; i++ {
		wb.Put8(0)
	}

	if _, err := cf.Write(wb.Bytes()); err != nil {
		logger.Fatalf("Write message: %s", err)
	}
	if errno := readProgress(cf, func(cnt, size uint64) {
		*count = int64(cnt)
		*bytes = int64(size)
		if dspin != nil {
			dspin.SetCurrent(int64(cnt), int64(size))
		}
	}); errno != 0 {
		logger.Fatalf("Clear failed: %s", errno)
	}
}

func clear(ctx *cli.Context) error {
	setup(ctx, 0)
	clearBurst := ctx.Bool("clear-burst")
	broadcast := ctx.Bool("broadcast")

	// Get paths
	paths, err := getPaths(ctx)
	if err != nil {
		logger.Fatal(err.Error())
	}

	// Paths required in normal clear mode
	if len(paths) == 0 && !clearBurst {
		logger.Infof("Nothing to clear")
		return nil
	}

	// Get absolute paths for finding mount point
	var absPaths []string
	var relativePaths []string
	if len(paths) > 0 {
		absPaths, err = getAbsolutePaths(paths)
		if err != nil {
			logger.Fatalf("Failed to get absolute paths: %s", err)
		}

		// Find mount point and convert to relative paths
		relativePaths, err = convertToRelativePaths(absPaths)
		if err != nil {
			logger.Fatalf("Failed to convert paths: %s", err)
		}
	}

	if len(absPaths) == 0 {
		logger.Fatalf("requires a mount point argument")
	}

	// Open controller file
	first := absPaths[0]
	controller := openController(first)
	if controller == nil {
		logger.Fatalf("open control file for %s", first)
	}
	defer controller.Close()

	// Execute clear operation
	var count, bytes int64
	progressLabel := "Clear cache"
	if clearBurst {
		progressLabel = "Clear burst cache"
		relativePaths = nil // clear-burst ignores paths
	}

	// Use control file with progress display
	prog := progress.NewProgress(false, true)
	dspin := prog.AddDoubleSpinner(progressLabel)
	sendClear(controller, relativePaths, broadcast, clearBurst, &count, &bytes, dspin)
	prog.Done()
	count, bytes = dspin.Current()

	// Log result
	if clearBurst {
		if count == 0 && bytes == 0 {
			logger.Info("Clear burst: no burst cache to clear")
		} else {
			logger.Infof("Successfully cleared burst cache: evicted %d blocks %s", count, utils.FormatBytes(uint64(bytes)))
		}
	} else {
		logger.Infof("Successfully cleared %d blocks %s", count, utils.FormatBytes(uint64(bytes)))
	}

	return nil
}
