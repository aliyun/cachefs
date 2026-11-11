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
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"

	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/aliyun/cachefs/pkg/version"
	"github.com/erikdubbelboer/gspt"
	"github.com/google/gops/agent"
	"github.com/google/uuid"
	"github.com/pyroscope-io/client/pyroscope"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

var logger = utils.GetLogger("cachefs")

func Main(args []string) error {
	// we have to call this because gspt removes all arguments
	gspt.SetProcTitle(strings.Join(os.Args, " "))
	cli.VersionFlag = &cli.BoolFlag{
		Name: "version", Aliases: []string{"V"},
		Usage: "print version only",
	}
	app := &cli.App{
		Name:                 "cachefs",
		Usage:                "A distributed file system that could accelerate POSIX directory with distributed cache",
		Version:              version.Version(),
		Copyright:            "Apache License 2.0",
		HideHelpCommand:      true,
		EnableBashCompletion: true,
		Flags:                globalFlags(),
		Commands: []*cli.Command{
			cmdVersion(),
			cmdStats(),
			cmdProfile(),
			cmdCache(),
			cmdDaemon(),
			cmdPreload(),
			cmdClear(),
			cmdSource(),
			cmdMetaReady(),
			cmdApiClient(),
			cmdCacheroot(),
			cmdCachestate(),
		},
	}

	return app.Run(utils.ReorderOptions(app, args))
}

// Check number of positional arguments, set logger level and setup agent if needed.
// defaultNoAgent turns the debug agents off by default for short-lived commands;
// an explicit --no-agent=false still enables them.
func setup(c *cli.Context, n int, defaultNoAgent bool) {
	if c.NArg() < n {
		fmt.Printf("ERROR: This command requires at least %d arguments\n", n)
		fmt.Printf("USAGE:\n   cachefs %s [command options] %s\n", c.Command.Name, c.Command.ArgsUsage)
		os.Exit(1)
	}

	if c.Bool("trace") {
		utils.SetLogLevel(logrus.TraceLevel)
	} else if c.Bool("verbose") {
		utils.SetLogLevel(logrus.DebugLevel)
	} else if c.Bool("quiet") {
		utils.SetLogLevel(logrus.WarnLevel)
	} else {
		utils.SetLogLevel(logrus.InfoLevel)
	}
	if c.Bool("no-color") {
		utils.DisableLogColor()
	}
	logID := c.String("log-id")
	if logID != "" {
		if logID == "random" {
			logID = uuid.New().String()
		}
		utils.SetLogID("[" + logID + "] ")
	}

	noAgent := c.Bool("no-agent")
	if defaultNoAgent && !c.IsSet("no-agent") {
		noAgent = true
	}
	if !noAgent {
		go func() {
			// Bind explicitly instead of calling ListenAndServe: the port is
			// picked by racing up the range, and ListenAndServe never reveals
			// which attempt won. Logging the resolved address is the only way an
			// operator can aim curl/pprof at the right process.
			var ln net.Listener
			var err error
			for port := 6060; port < 6100; port++ {
				ln, err = net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
				if err == nil {
					break
				}
			}
			if ln == nil {
				logger.Warnf("Pprof agent not started: no free port in range 6060-6099 (last error: %s)", err)
				return
			}
			logger.Infof("Pprof agent listening on http://%s/debug/pprof/", ln.Addr())
			if err := http.Serve(ln, nil); err != nil {
				logger.Warnf("Pprof agent on %s stopped: %s", ln.Addr(), err)
			}
		}()
		go func() {
			for port := 6070; port < 6100; port++ {
				_ = agent.Listen(agent.Options{Addr: fmt.Sprintf("127.0.0.1:%d", port)})
			}
		}()
	}

	if c.IsSet("pyroscope") {
		tags := make(map[string]string)
		appName := fmt.Sprintf("cachefs.%s", c.Command.Name)
		if c.Command.Name == "mount" {
			tags["mountpoint"] = c.Args().Get(1)
		}
		if hostname, err := os.Hostname(); err == nil {
			tags["hostname"] = hostname
		}
		tags["pid"] = strconv.Itoa(os.Getpid())
		tags["version"] = version.Version()

		if _, err := pyroscope.Start(pyroscope.Config{
			ApplicationName: appName,
			ServerAddress:   c.String("pyroscope"),
			Logger:          logger,
			Tags:            tags,
			AuthToken:       os.Getenv("PYROSCOPE_AUTH_TOKEN"),
			ProfileTypes:    pyroscope.DefaultProfileTypes,
		}); err != nil {
			logger.Errorf("start pyroscope agent: %v", err)
		}
	}
}
