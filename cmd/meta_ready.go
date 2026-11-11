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
	"time"

	cfsmeta "github.com/aliyun/cachefs/pkg/meta/cachefs"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/aliyun/cachefs/pkg/vfs/apiclient"
	"github.com/urfave/cli/v2"
)

func cmdMetaReady() *cli.Command {
	return &cli.Command{
		Name:      "meta-ready",
		Action:    metaReady,
		Category:  "TOOL",
		Usage:     "Notify cachefs that source dir metadata is fully cached and can be used as meta source",
		ArgsUsage: "MOUNTPOINT",
		Description: `
This command notifies cachefs in peer meta mode that the in-memory metadata is
fully cached and this node can serve as a metadata source for other peers.

Examples:
# After mounting cachefs in peer meta mode and scanning all source metadata:
$ cachefs meta-ready /mnt/cachefs`,

		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "api-server-sock",
				Aliases: []string{"s"},
				Usage:   "use cachefs API server to send command instead of control file",
			},
		},
	}
}

// sendMetaReady sends a meta-ready command via the .control file.
func sendMetaReady(cf *utils.Buffer) {
	cf.Put32(cfsmeta.CmdMetaReady)
	cf.Put32(0) // no payload
}

func metaReady(ctx *cli.Context) error {
	sockPath := ctx.String("api-server-sock")

	if sockPath == "" {
		setup(ctx, 1, true)
		mp := ctx.Args().First()
		controller := openController(mp)
		if controller == nil {
			logger.Fatalf("open control file for %s", mp)
		}
		defer controller.Close()

		start := time.Now()
		wb := utils.NewBuffer(8)
		sendMetaReady(wb)
		if _, err := controller.Write(wb.Bytes()); err != nil {
			logger.Fatalf("Write meta-ready command: %s", err)
		}
		if errno := readProgress(controller, func(_, _ uint64) {}); errno != 0 {
			logger.Fatalf("Set meta ready failed: %s", errno)
		}
		logger.Infof("Set meta ready in %s", time.Since(start))
		return nil
	}

	// Use API server
	setup(ctx, 0, true)
	start := time.Now()
	client, err := apiclient.NewApiClient(sockPath)
	if err != nil {
		logger.Fatalf("Create API client failed: %s", err)
	}
	defer client.Close()
	if err = client.MetaReady(); err != nil {
		logger.Fatalf("Set meta ready failed: %s", err)
	}
	logger.Infof("Set meta ready in %s", time.Since(start))
	return nil
}
