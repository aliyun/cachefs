/*
 * Copyright 2024 Alibaba Cloud, Inc. or its affiliates.
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
	"errors"
	"io/fs"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/urfave/cli/v2"

	"github.com/aliyun/cachefs/pkg/chunk"
	"github.com/aliyun/cachefs/pkg/metric"
	"github.com/aliyun/cachefs/pkg/rpcsvc"
	"github.com/aliyun/cachefs/pkg/serf"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/juicedata/godaemon"
)

func cmdDaemon() *cli.Command {
	compoundFlags := [][]cli.Flag{
		commonFlags(),
		daemonFlags(),
	}
	return &cli.Command{
		Name:      "daemon",
		Action:    daemon,
		Category:  "SERVICE",
		Usage:     "Start cachefs cache daemon",
		ArgsUsage: "",
		Description: `
Start cache daemon for cachefs fuse, and cachefs fuse mount connects to this daemon
for cached data via unix domain socket.

Examples:
1. Start cachefs daemon

# Start cachefs daemon and store cache data in /mnt/cache,
# default unix domain socket path is /var/run/cachefsd/cachefsd.sock
$ cachefs daemon --cache-dir /mnt/cache

# Join to an existing cluster by specifying a peer node addr
$ cachefs daemon --cache-dir /mnt/cache --serf-join 192.168.1.2

# Change default cache size to 1G, daemon will use at most cache-size in cache-dir
$ cachefs daemon --cache-dir /mnt/cache --cache-size 1024 --serf-join 192.168.1.2

# Listen on the given unix domain socket path
$ cachefs daemon --cache-dir /mnt/cache --listen /run/cachefsd.sock


2. Start cachefs fuse mount and connect to the daemon

# Mount cachefs and use /mnt/slow as data source, connects to daemon via specified uds path
$ cachefs cache --source-dir /mnt/slow --socket /var/run/cachefsd/cachefsd.sock cachefs /mnt/cachefs
`,
		Flags: expandFlags(compoundFlags),
	}
}

func daemonize(c *cli.Context) error {
	var attrs godaemon.DaemonAttr
	logfile := c.String("log")
	attrs.OnExit = func(stage int) error {
		if stage != 0 {
			return nil
		}
		return nil
	}

	// the current dir will be changed to root in daemon
	if godaemon.Stage() == 0 {
		var err error
		attrs.Stdout, err = os.OpenFile(logfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			logger.Errorf("open log file %s: %s", logfile, err)
		}
	}
	_, _, err := godaemon.MakeDaemon(&attrs)
	return err
}

func runAsDaemon(c *cli.Context) {
	// The default log to syslog is only in daemon mode.
	utils.InitLoggers(!c.Bool("no-syslog"))
	err := daemonize(c)
	if err != nil {
		logger.Fatalf("Failed to make daemon: %s", err)
	}
}

func genDaemonStoreConf(c *cli.Context) *chunk.StoreConfig {
	cacheDir, isCacheDirNew, err := processPath(c, "cache-dir", true)
	if err != nil {
		logger.Fatal(err.Error())
	}
	cfg, err := genStoreConf(c, cacheDir)
	if err != nil {
		logger.Fatal(err.Error())
	}

	// require at least 1G cache size
	if cfg.CacheSize < 1024 {
		logger.Fatal("require at least 1GiB cache size")
	}
	cfg.Socket = c.String("listen")

	baseDir := path.Dir(cfg.Socket)
	if err := os.MkdirAll(baseDir, 0755); err != nil && !errors.Is(err, fs.ErrExist) {
		if isCacheDirNew {
			if err := os.Remove(cacheDir); err != nil {
				logger.Warnf("Failed to clean up cache directory: %s", err)
			}
		}
		logger.Fatal(err)
	}
	return cfg
}

func handleSignal(stop func(), done chan struct{}) {
	// Go will catch all the signals
	signal.Ignore(syscall.SIGPIPE)
	signalChan := make(chan os.Signal, 10)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	go func() {
		for {
			sig := <-signalChan
			logger.Infof("Received signal %s, exiting...", sig.String())
			stop()
			done <- struct{}{}
		}
	}()
}

func peersHandler(ss serf.SerfService, raw bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		members := ss.GetMembers(raw)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(members); err != nil {
			http.Error(w, "Unable to encode JSON", http.StatusInternalServerError)
		}
	}
}

func daemon(c *cli.Context) error {
	logger.Infof("Run CacheFS daemon version %s", c.App.Version)
	done := make(chan struct{})
	setup(c, 0)

	registerer, registry := metric.InitRegister("cachefsd_", "", "")

	// Create serf service
	storeConf := genDaemonStoreConf(c)
	serfConf, err := genSerfConf(c, storeConf)
	if err != nil {
		logger.Fatal(err.Error())
	}
	rpcConf := genRpcConf(storeConf)
	rpcService, err := rpcsvc.NewRpcService(rpcConf)
	if err != nil {
		logger.Fatal(err.Error())
	}

	// Create cachefs daemon
	serfService := serf.NewSerfService(serfConf, registerer)
	cachefsd := chunk.NewDaemon(storeConf, rpcService, serfService, registerer)

	if c.Bool("background") && os.Getenv("CFS_FOREGROUND") == "" {
		runAsDaemon(c)
	}

	handleSignal(func() {
		cachefsd.Stop()
		rpcService.Stop()
		serfService.Stop()
	}, done)
	initMetrics(c, registry, serfService)

	if err := syscall.Setpriority(syscall.PRIO_PROCESS, os.Getpid(), -19); err != nil {
		logger.Warnf("setpriority: %s", err)
	}

	// Start RPC service before serf service, to make local service ready before
	// broadcasting states to cluster.
	rpcService.Serve()

	// Start serf service after all users registered themselves to serf service
	if err := serfService.Start(); err != nil {
		logger.Fatalf("Start serf service: %s", err)
	}
	if err := cachefsd.Serve(); err != nil {
		logger.Fatalf("Run cachefs daemon failed: %s", err)
	}
	liveAddr := c.String("liveness")
	liveSet := c.IsSet("liveness")
	if err := utils.LivenessProbe(liveAddr, !liveSet); err != nil {
		logger.Fatal(err.Error())
	}
	<-done
	return nil
}
