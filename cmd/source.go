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

	cfsmeta "github.com/aliyun/cachefs/pkg/meta/cachefs"
	"github.com/aliyun/cachefs/pkg/progress"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/aliyun/cachefs/pkg/vfs/apiclient"
	"github.com/urfave/cli/v2"
)

func cmdSource() *cli.Command {
	return &cli.Command{
		Name:      "source-ready",
		Action:    source,
		Category:  "TOOL",
		Usage:     "Set source ready state for sourceless CacheFS",
		ArgsUsage: "MOUNTPOINT",
		Description: `
This command notifies cachefs in sourceless mode that source dir is ready,
also cache all source dir metadata & data and announce to cachefs cluster
that this cachefs is the metadata and data source node. Once this command
returns, it means all metadata and data are loaded & cached & announced.

Note that cachefs cache size must hold all data in source dir.

Examples:
# mount cachefs in sourceless mode but with not-ready source dir specified
$ cachefs --source-dir /mnt/data --sourceless ... cachefs /mnt/cachefs

# write data to /mnt/data, once done, notify cachefs source is ready
$ cachefs source-ready /mnt/cachefs`,

		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "background",
				Aliases: []string{"d"},
				Usage:   "run in background, command return immediately",
			},
			&cli.StringFlag{
				Name:    "api-server-sock",
				Aliases: []string{"s"},
				Usage:   "use cachefs API server to send command instead of control file",
			},
		},
	}
}

// send source-ready command to controller file
func sendSourceReady(cf *os.File, background bool, dspin *progress.DoubleSpinner) {
	var back uint8
	wb := utils.NewBuffer(8 + 4)
	wb.Put32(cfsmeta.CmdSourceReady)
	wb.Put32(4)
	if background {
		back = 1
	}
	wb.Put8(back)
	// padding
	for i := 0; i < 3; i++ {
		wb.Put8(0)
	}
	if _, err := cf.Write(wb.Bytes()); err != nil {
		logger.Fatalf("Write message: %s", err)
	}
	if errno := readProgress(cf, func(count, bytes uint64) {
		dspin.SetCurrent(int64(count), int64(bytes))
	}); errno != 0 {
		logger.Fatalf("Set source ready failed: %s", errno)
	}
}

func sendSourceReadyApiReq(sockPath string, background bool) (int64, int64, error) {
	client, err := apiclient.NewApiClient(sockPath)
	if err != nil {
		return 0, 0, err
	}
	defer client.Close()
	count, bytes, err := client.SourceReady(background)
	if err != nil {
		return 0, 0, err
	}
	return int64(count), int64(bytes), nil
}

func source(ctx *cli.Context) error {
	sockPath := ctx.String("api-server-sock")
	background := ctx.Bool("background")

	var controller *os.File
	if sockPath == "" {
		setup(ctx, 1, true)
		mp := ctx.Args().First()
		controller = openController(mp)
		if controller == nil {
			logger.Fatalf("open control file for %s", mp)
		}
		defer controller.Close()
	} else {
		setup(ctx, 0, true)
	}

	var count, bytes int64
	var prog *progress.Progress
	var dspin *progress.DoubleSpinner
	if sockPath == "" {
		prog = progress.NewProgress(background, true)
		dspin = prog.AddDoubleSpinner("source-ready")
		sendSourceReady(controller, background, dspin)
		prog.Done()
	} else {
		var err error
		count, bytes, err = sendSourceReadyApiReq(sockPath, background)
		if err != nil {
			logger.Fatalf("Set source ready failed: %s", err)
		}
	}
	if !background {
		if sockPath == "" {
			count, bytes = dspin.Current()
		}
		logger.Infof("Set source ready done, loaded %d files (%d bytes) successfully", count, bytes)
	}
	return nil
}
