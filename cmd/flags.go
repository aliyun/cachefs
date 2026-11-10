/*
 * JuiceFS, Copyright 2022 Juicedata, Inc.
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
	"path"
	"strings"

	"github.com/aliyun/cachefs/pkg/chunk"
	"github.com/aliyun/cachefs/pkg/serf"
	"github.com/aliyun/cachefs/pkg/transfer"
	"github.com/aliyun/cachefs/pkg/vfs"
	"github.com/urfave/cli/v2"
)

const defaultSocketPath = "/var/run/cachefsd/cachefsd.sock"

func defaultLogDir() string {
	var logDir = "/var/log"
	if os.Getuid() != 0 {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			logger.Fatalf("%v", err)
			return ""
		}
		logDir = path.Join(homeDir, ".cachefs")
	}
	return logDir
}

func globalFlags() []cli.Flag {
	return []cli.Flag{
		&cli.BoolFlag{
			Name:    "verbose",
			Aliases: []string{"debug", "v"},
			Usage:   "enable debug log",
		},
		&cli.BoolFlag{
			Name:    "quiet",
			Aliases: []string{"q"},
			Usage:   "show warning and errors only",
		},
		&cli.BoolFlag{
			Name:  "trace",
			Usage: "enable trace log",
		},
		&cli.StringFlag{
			Name:  "log-id",
			Usage: "append the given log id in log, use \"random\" to use random uuid",
		},
		&cli.BoolFlag{
			Name:  "no-agent",
			Usage: "disable pprof (:6060) and gops (:6070) agent",
		},
		&cli.StringFlag{
			Name:  "pyroscope",
			Usage: "pyroscope address",
		},
		&cli.BoolFlag{
			Name:  "no-color",
			Usage: "disable colors",
		},
	}
}

func expandFlags(compoundFlags [][]cli.Flag) []cli.Flag {
	var flags []cli.Flag
	for _, flag := range compoundFlags {
		flags = append(flags, flag...)
	}
	return flags
}

func commonFlags() []cli.Flag {
	return []cli.Flag{
		&cli.BoolFlag{
			Name:    "d",
			Aliases: []string{"background"},
			Usage:   "run in background",
		},
		&cli.BoolFlag{
			Name:  "no-syslog",
			Usage: "disable syslog",
		},
		&cli.IntFlag{
			Name:  "block-size",
			Value: 64,
			Usage: "size of block in MiB",
		},
		&cli.StringFlag{
			Name:  "cache-dir",
			Value: "memory",
			Usage: "cache dir or \"memory\"",
		},
		&cli.IntFlag{
			Name:  "cache-size",
			Value: 128 << 10,
			Usage: "size of cached objects in MiB",
		},
		&cli.IntFlag{
			Name:  "burst-cache-size",
			Value: 0,
			Usage: "burst cache size in MiB, allows cache to temporarily exceed cache-size up to this limit (0=disabled)",
		},
		&cli.BoolFlag{
			Name:    "defer-burst",
			EnvVars: []string{"CFS_DEFER_BURST"},
			Usage:   "record burst-cache-size at startup but do not enable it until SetCacheSize API restores it",
		},
		&cli.IntFlag{
			Name:  "go-gc",
			Usage: "Go GC percent like GOGC, negative value disables GC; GOGC env takes precedence",
		},
		&cli.Int64Flag{
			Name:  "go-memlimit",
			Usage: "Go memory limit in MiB like GOMEMLIMIT; GOMEMLIMIT env takes precedence",
		},
		&cli.IntFlag{
			Name:  "writeback-buffer",
			Value: 256,
			Usage: "size of disk cache writeback buffer in MiB",
		},
		&cli.IntFlag{
			Name:  "writeback-worker",
			Value: 8,
			Usage: "number of disk cache writeback workers",
		},
		&cli.Float64Flag{
			Name:  "free-space-ratio",
			Value: 0.1,
			Usage: "min free space (ratio)",
		},
		&cli.BoolFlag{
			Name:  "cleanup-cache",
			Usage: "cleanup cache dir on exit",
		},
		&cli.StringFlag{
			Name:  "checksum",
			Usage: "checksum cache block after P2P transfer, supported algorithms: crc32, md5",
		},
		&cli.Int64Flag{
			Name:  "upload-limit",
			Value: 0,
			Usage: "bandwidth limit for upload in Mbps, actual upload limit is min(upload-limit, total-limit) if total-limit is also set",
		},
		&cli.Int64Flag{
			Name:  "download-limit",
			Value: 0,
			Usage: "bandwidth limit for download in Mbps, actual download limit is min(download-limit, total-limit) if total-limit is also set",
		},
		&cli.Int64Flag{
			Name:  "total-limit",
			Value: 0,
			Usage: "total bandwidth limit in Mbps shared by both upload and download",
		},
		&cli.Int64Flag{
			Name:  "single-nic-limit",
			Value: 0,
			Usage: "total bandwidth limit in Mbps used instead of total-limit when only a single transport NIC is in use (e.g. multiple RDMA NICs fallback to single TCP NIC)",
		},
		&cli.IntFlag{
			Name:  "peer-timeout",
			Value: 30,
			Usage: "the max number of seconds to download data from peer",
		},
		&cli.IntFlag{
			Name:  "blacklist-time",
			Value: 180,
			Usage: "Max seconds for a peer, which timed out for data loading, to be in peer blacklist",
		},
		&cli.IntFlag{
			Name:  "exchange-interval",
			Value: 30,
			Usage: "Interval in seconds for periodic P2P tracker exchange with a random peer, 0 to disable",
		},
		&cli.StringFlag{
			Name:  "cache-nic",
			Usage: "Network interface used as control-plane of cache server and Serf service, default to device that has default route",
		},
		&cli.StringFlag{
			Name:  "transport",
			Value: transfer.TCPTransport,
			Usage: "Underlying transport used to transfer data between peers, supports " + strings.Join(chunk.SupportedTransport(), " "),
		},
		&cli.StringFlag{
			Name:  "transport-nic",
			Usage: "Comma- or space-separated list of network interfaces (e.g., eth0,eth1 or \"eth0 eth1\") used as data-plane of cache server, default to --cache-nic for TCP transport, and all available RDMA devices for RDMA transport",
		},
		&cli.IntFlag{
			Name:  "cache-port",
			Value: 7711,
			Usage: "Port for cachefs cache server",
		},
		&cli.StringFlag{
			Name:  "serf-join",
			Usage: "Addresses of initial serf nodes to join with, separate with space, e.g. \"host1 host2\"",
		},
		&cli.StringFlag{
			Name:  "serf-memberlist-cmd",
			Usage: "command that outputs existing member IP list, IPs should be separated by space or newline",
		},
		&cli.IntFlag{
			Name:  "serf-maxretry",
			Value: 10,
			Usage: `Sets the number of attempts after the first fixed phase of 5s intervals for 6 times.
	This parameter controls second phase attempts at 1-minute intervals, defaults to 10 attempts.`,
		},
		&cli.IntFlag{
			Name:  "serf-port",
			Value: 7946,
			Usage: "Port for serf to construct P2P network",
		},
		&cli.BoolFlag{
			Name:  "serf-initnode",
			Usage: "indicate this node works as init node in P2P cluster, and won't be counted in hashring ",
		},
		&cli.StringFlag{
			Name:  "serf-group",
			Usage: "specify which group serf can join, will ignore nodes with different group",
		},
		&cli.IntFlag{
			Name:  "serf-reconnect-timeout",
			Value: 20,
			Usage: "reconnect failed serf members before this timeout time in second",
		},
		&cli.IntFlag{
			Name:  "serf-leave-delay",
			Value: 1,
			Usage: "delay N seconds to let serf leave message to propagate through the cluster before exit",
		},
		&cli.StringFlag{
			Name:  "serf-node-name",
			Usage: "Specify the Serf node name for the current instance. This is particularly useful when running multiple instances on the same machine to avoid name conflicts within the Serf cluster.",
		},
		&cli.IntFlag{
			Name:  "virtual-node-count",
			Value: serf.VirtualNodeCount,
			Usage: "Virtual node count used in consistent hash ring",
		},
		&cli.StringFlag{
			Name:  "metrics",
			Value: "127.0.0.1:9678",
			Usage: "address to export metrics",
		},
		&cli.StringFlag{
			Name:  "liveness",
			Value: "127.0.0.1:7954",
			Usage: "address for liveness probe",
		},
		&cli.StringFlag{
			Name:  "cache-eviction",
			Value: "hash",
			Usage: "choose the cache eviction policy, available: hash, 2-random",
		},
		&cli.StringFlag{
			Name:  "cache-expire",
			Value: "0",
			Usage: "Duration after which unused cached blocks are evicted (0=never). Accepts: float in seconds (10, 10.5), duration strings (1h30m), or day-based (2d12h).",
		},
		&cli.StringFlag{
			Name:  "thp-policy",
			Value: "default",
			Usage: "Transparent hugepage policy, values can be one of \"always\", \"madvise\" and \"default\"",
		},
		&cli.IntFlag{
			Name:  "rdma-buffer-size",
			Value: 256,
			Usage: "Size of buffers used by RDMA transport in KiB, must be power of 2",
		},
		&cli.IntFlag{
			Name:  "rdma-buffer-count",
			Value: 8,
			Usage: "Number of buffers used by RDMA transport in per-connection to remote server",
		},
		&cli.IntFlag{
			Name:  "rdma-max-mem",
			Value: 2048,
			Usage: "Max memory usage of buffers used by RDMA transport in MiB",
		},
		&cli.BoolFlag{
			Name:  "rdma-fallback-tcp",
			Usage: "enables fallback to 'tcp' if RDMA initialization fails",
		},
		&cli.IntFlag{
			Name:  "cacheroot-port",
			Value: 0,
			Usage: "Port for local cachefs P2P cache root tracker info server, 0 means disable",
		},
		&cli.StringFlag{
			Name:  "cacheroot-tracker",
			Usage: "Addresses of server used as alternative P2P tracker info sources when cache miss in local serf cluster, separate with space, e.g. \"host1:8811 host2:8811\"; if no port is specified, port 80 is used by default",
		},
		&cli.StringFlag{
			Name:  "cacheroot-transport",
			Value: transfer.TCPTransport,
			Usage: "Transport used to load data from cacheroot servers, similar to \"--transport\"",
		},
		&cli.StringFlag{
			Name:  "cacheroot-nic",
			Usage: "Comma- or space-separated list of network interfaces (e.g., eth0,eth1 or \"eth0 eth1\") used as data-plane to cacheroot server, default to --cache-nic for TCP transport, and all available RDMA devices for RDMA transport",
		},
		&cli.StringFlag{
			Name:  "key-prefix",
			Usage: "Add prefix to internal cache keys",
		},
	}
}

func cacheFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:  "log",
			Value: path.Join(defaultLogDir(), "cachefs.log"),
			Usage: "path of log file when running in background",
		},
		&cli.StringFlag{
			Name:  "o",
			Usage: "other FUSE options",
		},
		&cli.StringFlag{
			Name:  "source-dir",
			Usage: "source dir, required unless --sourceless is used",
		},
		&cli.StringFlag{
			Name:    "extra-source-dirs",
			EnvVars: []string{"CFS_EXTRA_SOURCE_DIRS"},
			Usage:   "optional extra source dirs separated by spaces or commas, used as fallback replicas of --source-dir in the given order",
		},
		&cli.BoolFlag{
			Name:  "source-direct-io",
			Usage: "use direct IO reading data from source dir, falls back to buffered IO if the source filesystem does not support O_DIRECT",
		},
		&cli.BoolFlag{
			Name:  "cache-read-direct-io",
			Usage: "use direct IO reading cached blocks from cache dir, falls back to buffered IO if the cache filesystem does not support O_DIRECT",
		},
		&cli.BoolFlag{
			Name:  "cache-write-direct-io",
			Usage: "use direct IO writing cached blocks to cache dir, falls back to buffered IO if the cache filesystem does not support O_DIRECT",
		},
		&cli.StringFlag{
			Name:  "source-subpath",
			Usage: "only specified subpaths within source dir are accessible, subpaths must be started with \"/\", multiple subpaths are separated by space",
		},
		&cli.StringFlag{
			Name:  "source-subpath-file",
			Usage: "path of file that contains accessible subpaths within source dir, one subpath per-line in file, this option overrides --source-subpath",
		},
		&cli.StringFlag{
			Name:  "source-subpath-cmd",
			Usage: "command to run to get accessible subpaths within source dir, one subpath per line; highest priority, overrides --source-subpath-file and --source-subpath",
		},
		&cli.IntFlag{
			Name:  "source-subpath-reload",
			Value: 0,
			Usage: "Seconds between reloading subpaths from --source-subpath-cmd or --source-subpath-file, 0 means disable reloading",
		},
		&cli.BoolFlag{
			Name:  "sourceless",
			Usage: "start without source dir, load fs meta from other peers, or with source dir specified and notified by source-ready subcommand that source dir is ready",
		},
		&cli.IntFlag{
			Name:  "source-peermeta",
			Value: 0,
			Usage: "Try best to load source dir filesystem metadata from other peers within given timeout in seconds, to avoid going back to source",
		},
		&cli.StringFlag{
			Name:  "peermeta-mode",
			Value: "auto",
			Usage: "Peer meta ready mode: 'auto' sets meta ready automatically after mount; 'manual' requires explicit meta-ready command",
		},
		&cli.IntFlag{
			Name:  "prefetch",
			Value: 24,
			Usage: "prefetch N blocks in advance and set the block LRU window size",
		},
		&cli.IntFlag{
			Name:  "prefetch-workers",
			Usage: "number of workers to do prefetch, use the same number set by --prefetch if 0",
		},
		&cli.StringFlag{
			Name:  "preload",
			Usage: "preload given files from source or peer in given paths, only support \"/\" for now.",
		},
		&cli.IntFlag{
			Name:  "preload-workers",
			Value: 2,
			Usage: "Number of files to preload concurrently from source or peer",
		},
		&cli.IntFlag{
			Name:  "preload-interval",
			Value: 0,
			Usage: "Preload interval in seconds, <= 0 means do preload once",
		},
		&cli.BoolFlag{
			Name:  "force-preload",
			Usage: "Force preload to proceed even if prefetch channel is full",
		},
		&cli.StringFlag{
			Name:  "pin-cache",
			Usage: "Space-separated list of directory paths to pin in cache. Paths must start with \"/\".",
		},
		&cli.StringFlag{
			Name:  "pin-cache-file",
			Usage: "Path to file containing directory paths to pin, one per line (overrides --pin-cache).",
		},
		&cli.StringFlag{
			Name:  "pin-cache-cmd",
			Usage: "Command to run to get directory paths to pin, one per line; highest priority, overrides --pin-cache-file and --pin-cache.",
		},
		&cli.IntFlag{
			Name:  "pin-cache-reload",
			Value: 0,
			Usage: "Seconds between reloading pin paths from --pin-cache-cmd or --pin-cache-file, 0 means disable reloading",
		},
		&cli.IntFlag{
			Name:  "share-reserve",
			Usage: "reserved burst cache in MiB for regular read traffic, not available to vfs API path; default to prefetch x block size, 0 means no reservation.",
		},
		&cli.IntFlag{
			Name:  "buffer-size",
			Value: 0,
			Usage: "total size in MB of idle warm blocks, rounded down to whole blocks, 0 disables the block pool",
		},
		&cli.BoolFlag{
			Name:    "buffer-reuse",
			Value:   true,
			EnvVars: []string{"CFS_BUFFER_REUSE"},
			Usage:   "reuse released blocks in the block pool",
		},
		&cli.BoolFlag{
			Name:    "buffer-prefault",
			Value:   true,
			EnvVars: []string{"CFS_BUFFER_PREFAULT"},
			Usage:   "asynchronously preallocate and prefault idle blocks",
		},
		&cli.IntFlag{
			Name:  "buffer-timeout",
			Value: 30,
			Usage: "seconds idle warm blocks are retained after the last allocation, 0 means no timeout",
		},
		&cli.IntFlag{
			Name:  "readdir-cache",
			Value: -1,
			Usage: "readdir cache timeout in seconds, -1 means no timeout",
		},
		&cli.BoolFlag{
			Name:    "attr-cleanup",
			EnvVars: []string{"CFS_ATTR_CLEANUP"},
			Usage:   "clean up expired inode attributes, requires readdir-cache >= 0",
		},
		&cli.IntFlag{
			Name:  "negative-timeout",
			Usage: "cache timeout for non-existed entries, -1 means no timeout, default to readdir-cache",
		},
		&cli.IntFlag{
			Name:  "metaop-timeout",
			Value: 0,
			Usage: "timeout in seconds of meta data operations on source dir, and return cached meta if any on either timeout or error, currently only affects readdir, 0 means no timeout and return actual result or error",
		},
		&cli.BoolFlag{
			Name:  "no-pagecache",
			Value: false,
			Usage: "disable fuse page cache",
		},
		&cli.BoolFlag{
			Name:  "unsafe-cache-key",
			Usage: "Use file path as cache key without file length and mtime info, so files with same path but different attrs share the same cache even if their content differ",
		},
		&cli.StringFlag{
			Name:  "socket",
			Usage: "Unix domain socket path of cachefsd to connect to",
		},
		&cli.BoolFlag{
			Name:  "enable-api-server",
			Value: false,
			Usage: "Start vfs level API server",
		},
		&cli.StringFlag{
			Name:    "api-server-sock",
			Aliases: []string{"s"},
			Value:   vfs.DefAPIServerSock,
			Usage:   "Socket path vfs level API server listens on",
		},
		&cli.StringFlag{
			Name:  "source-id",
			Usage: "Name that identifies an source uniquely, e.g. an OSS bucket name, cachefsd uses this ID to separate cached data, cachefs fuse mounts with the same source ID share the same set of cache. Must not contain \"/\"",
		},
	}
}

func daemonFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:  "log",
			Value: path.Join(defaultLogDir(), "cachefsd.log"),
			Usage: "path of log file when running in background",
		},
		&cli.StringFlag{
			Name:  "listen",
			Value: defaultSocketPath,
			Usage: "Unix domain socket path to listen on",
		},
	}
}
