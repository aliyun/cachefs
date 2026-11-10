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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"

	chunkcommon "github.com/aliyun/cachefs/pkg/chunk/common"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/aliyun/cachefs/pkg/vfs/apiclient"
	vfscommon "github.com/aliyun/cachefs/pkg/vfs/common"
	"github.com/urfave/cli/v2"
	"golang.org/x/sys/unix"
)

func cmdApiClient() *cli.Command {
	return &cli.Command{
		Name:     "apiclient",
		Category: "TOOL",
		Usage:    "Client to CacheFS VFS level API server, for testing purpose",
		Description: `
A client to cachefs VFS API service for testing and management.

Examples:
# Get pid of cachefs
$ cachefs apiclient getpid

# Unmount cachefs
$ cachefs apiclient umount

# Get file content by path for direct access
$ cachefs apiclient getfile /mnt/cachefs/testfile

# Save file content to local file
$ cachefs apiclient getfile /mnt/cachefs/testfile -o ./savedfile

# Preload files/directories
$ cachefs apiclient preload /mnt/cachefs/testdir

# Broadcast preload to all nodes in serf cluster
$ cachefs apiclient preload /mnt/cachefs/testdir --broadcast

# Shrink burst cache back to cache-size
$ cachefs apiclient clear --clear-burst

# Clear burst cache on all nodes in serf cluster
$ cachefs apiclient clear --clear-burst --broadcast

# Set cache size to 200 MiB
$ cachefs apiclient set-cache --cache-size 200

# Set cache size and burst cache size
$ cachefs apiclient set-cache --cache-size 200 --burst-cache-size 400

# Set cache size and broadcast to cluster
$ cachefs apiclient set-cache --cache-size 200 --broadcast

# Show a snapshot of server-information, such as PID, block size, and cache configuration
$ cachefs apiclient server-info

# Show how data has been loaded (source vs peers) since mount
$ cachefs apiclient loadstats

# Also record the report in the mount's log, so it survives the process exiting
$ cachefs apiclient loadstats --log
`,
		Subcommands: []*cli.Command{
			cmdGetpid(),
			cmdUnmount(),
			cmdMetaReadyApi(),
			cmdDataReady(),
			cmdGetFile(),
			cmdPreloadApi(),
			cmdClearApi(),
			cmdSetCacheApi(),
			cmdServerInfo(),
			cmdGetPeers(),
			cmdLoadStats(),
		},
	}
}

func sockFlag() cli.Flag {
	return &cli.StringFlag{
		Name:    "api-server-sock",
		Aliases: []string{"s"},
		Value:   apiclient.DefAPIServerSock,
		Usage:   "Unix domain socket path of cachefs API service",
	}
}

func cmdGetPeers() *cli.Command {
	return &cli.Command{
		Name:   "getpeers",
		Action: apiGetPeers,
		Usage:  "Get peer addresses in the serf cluster",
		Flags: []cli.Flag{
			sockFlag(),
			&cli.BoolFlag{
				Name:    "raw",
				Aliases: []string{"r"},
				Usage:   "return live members from the underlying serf cluster instead of the cached list",
			},
			&cli.BoolFlag{
				Name:    "port",
				Aliases: []string{"p"},
				Usage:   "include port information in the returned peer addresses",
			},
		},
	}
}

func cmdGetpid() *cli.Command {
	return &cli.Command{
		Name:   "getpid",
		Action: apiGetpid,
		Usage:  "Getpid of cachefs",
		Flags:  []cli.Flag{sockFlag()},
	}
}

func cmdLoadStats() *cli.Command {
	return &cli.Command{
		Name:   "loadstats",
		Action: apiLoadStats,
		Usage:  "Show data loading stats: how much came from source vs from peers, with latency distribution",
		Flags: []cli.Flag{
			sockFlag(),
			&cli.BoolFlag{
				Name:    "log",
				Aliases: []string{"l"},
				Usage:   "also write the report to the mount process log, so it outlives the process",
			},
			&cli.BoolFlag{
				Name:  "json",
				Usage: "print the raw JSON payload instead of the rendered report",
			},
		},
	}
}

func cmdUnmount() *cli.Command {
	return &cli.Command{
		Name:   "umount",
		Action: apiUnmount,
		Usage:  "Unmount cachefs",
		Flags: []cli.Flag{sockFlag(), &cli.StringFlag{
			Name:  "name",
			Value: "apiclient",
			Usage: "Set unmount request Name",
		}},
	}
}

func cmdMetaReadyApi() *cli.Command {
	return &cli.Command{
		Name:   "meta-ready",
		Action: apiMetaReady,
		Usage:  "Notify cachefs that source dir metadata is fully cached and can be used as meta source",
		Flags:  []cli.Flag{sockFlag()},
	}
}

func cmdDataReady() *cli.Command {
	return &cli.Command{
		Name:   "data-ready",
		Action: apiDataReady,
		Usage:  "Notify cachefs that all data has been read and cached",
		Flags:  []cli.Flag{sockFlag()},
	}
}

func cmdSetCacheApi() *cli.Command {
	return &cli.Command{
		Name:   "set-cache",
		Action: apiSetCache,
		Usage:  "Set cache size via API server",
		Flags: []cli.Flag{
			sockFlag(),
			&cli.IntFlag{
				Name:    "cache-size",
				Aliases: []string{"c"},
				Value:   -1,
				Usage:   "new cache size in MiB (-1 = keep current)",
			},
			&cli.IntFlag{
				Name:    "burst-cache-size",
				Aliases: []string{"B"},
				Value:   -1,
				Usage:   "new burst cache size in MiB (-1 = keep current, auto-adjusted if < cache-size)",
			},
			&cli.BoolFlag{
				Name:  "restore-burst-cache",
				Usage: "restore burst cache size to the startup-configured value (no-op if burst cache was never enabled)",
			},
			&cli.BoolFlag{
				Name:    "broadcast",
				Aliases: []string{"b"},
				Usage:   "broadcast set-cache request to all nodes in serf cluster",
			},
		},
		Description: `Set cache size dynamically via API server.
If only --cache-size is given and the new value exceeds the current burst size, burst size is automatically raised to match.
If no flags are set, current values are kept unchanged.

Examples:
  # Set only cache size (burst size unchanged unless it would violate burst >= cache)
  cachefs apiclient set-cache --cache-size 200

  # Set both cache and burst cache size
  cachefs apiclient set-cache --cache-size 200 --burst-cache-size 400

  # Set only burst cache size
  cachefs apiclient set-cache --burst-cache-size 400

  # Restore burst cache size to the startup-configured value
  cachefs apiclient set-cache --restore-burst-cache

  # Set cache size and broadcast to cluster
  cachefs apiclient set-cache --cache-size 200 --broadcast
`,
	}
}

func cmdServerInfo() *cli.Command {
	return &cli.Command{
		Name:   "server-info",
		Action: apiServerInfo,
		Usage:  "Show a snapshot of server-information, such as PID, block size, and cache configuration",
		Flags:  []cli.Flag{sockFlag()},
	}
}

func cmdGetFile() *cli.Command {
	return &cli.Command{
		Name:   "getfile",
		Action: apiGetFile,
		Usage:  "Get file content by path or inode using mmap SDK",
		Flags: []cli.Flag{
			sockFlag(),
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "output file path for a single file (default: stdout)",
			},
			&cli.StringFlag{
				Name:    "output-dir",
				Aliases: []string{"O"},
				Usage:   "output directory for multiple files; each file is written to <dir>/<path-relative-to-mount>. Required when 2+ paths are given.",
			},
			&cli.BoolFlag{
				Name:  "no-cache-limit",
				Usage: "disable the cache limiter (by default getfile probes the daemon's burst cache size and limits concurrent mmap'd bytes to it)",
			},
			&cli.IntFlag{
				Name:  "wait-timeout",
				Value: 0,
				Usage: "max seconds to wait for the daemon to admit the mmap request into cache budget, 0 means wait forever",
			},
			&cli.Uint64Flag{
				Name:  "range-start",
				Usage: "start byte offset; requires --range-length",
			},
			&cli.Uint64Flag{
				Name:  "range-length",
				Usage: "number of bytes per range; without --range-start copies the whole file in consecutive ranges",
			},
			&cli.BoolFlag{
				Name:  "fd",
				Usage: "copy through direct cache block FDs instead of the mmap SDK path",
			},
		},
		ArgsUsage: "[FILE_PATH ...]",
		Description: `Get file content using CacheFS mmap SDK for zero-copy access.

By default getfile probes the daemon's burst cache size and installs a cache
limiter of that size, so the bytes mmap'd concurrently never exceed the burst
cache. Pass --no-cache-limit to disable it.

Give 2+ paths to fetch them at the same time; each output is written under
-O/--output-dir (a single -o file cannot hold several outputs). With -v
(global verbose flag) each file reports its mmap time, followed by a latency
summary — a file that waited on cache budget shows a markedly larger mmap time.

Examples:
  # Get file by path and output to stdout
  cachefs apiclient getfile /mnt/cachefs/testfile

  # Get file by path and save to local file
  cachefs apiclient getfile /mnt/cachefs/testfile -o ./savedfile

  # Copy a byte range from a file
  cachefs apiclient getfile /mnt/cachefs/testfile --range-start 1024 --range-length 4096 -o ./savedrange

  # Get multiple files at once
  cachefs apiclient getfile -O /tmp/out /mnt/cachefs/f1 /mnt/cachefs/f2 -v

  # Get file without the cache limiter
  cachefs apiclient getfile /mnt/cachefs/testfile --no-cache-limit
`,
	}
}

func cmdPreloadApi() *cli.Command {
	return &cli.Command{
		Name:      "preload",
		Action:    apiPreload,
		Usage:     "Preload files/directories using API server",
		ArgsUsage: "[PATH ...]",
		Flags: []cli.Flag{
			sockFlag(),
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
				Name:  "all",
				Usage: "scan & load all specified dirs/files",
				Value: true,
			},
			&cli.BoolFlag{
				Name:  "force",
				Usage: "load all blocks even if internal resource is unavailable temporarily",
				Value: true,
			},
			&cli.BoolFlag{
				Name:    "relative",
				Aliases: []string{"r"},
				Usage:   "use relative paths instead of absolute paths",
			},
			&cli.BoolFlag{
				Name:    "cancel",
				Aliases: []string{"c"},
				Usage:   "cancel the current preload request",
			},
		},
		Description: `Preload files/directories to cache using API server.

Examples:
  # Preload a directory
  cachefs apiclient preload /mnt/cachefs/testdir

  # Preload multiple paths in background
  cachefs apiclient preload /mnt/cachefs/dir1 /mnt/cachefs/dir2 --background

  # Broadcast preload request to all nodes in serf cluster
  cachefs apiclient preload /mnt/cachefs/testdir --broadcast

  # Cancel the current preload request
  cachefs apiclient preload --cancel

  # Use relative paths
  cachefs apiclient preload /testdir --relative
`,
	}
}

func cmdClearApi() *cli.Command {
	return &cli.Command{
		Name:      "clear",
		Action:    apiClear,
		Usage:     "Clear cache via API server",
		ArgsUsage: "[PATH ...]",
		Flags: []cli.Flag{
			sockFlag(),
			&cli.BoolFlag{
				Name:    "clear-burst",
				Aliases: []string{"B"},
				Usage:   "clear burst cache back to cache-size (PATH args are ignored)",
			},
			&cli.BoolFlag{
				Name:    "broadcast",
				Aliases: []string{"b"},
				Usage:   "broadcast clear cache request to all nodes in serf cluster",
			},
			&cli.BoolFlag{
				Name:    "relative",
				Aliases: []string{"r"},
				Usage:   "use relative paths instead of absolute paths",
			},
		},
		Description: `Clear cache via API server.

Examples:
  # Clear cache for a specific path
  cachefs apiclient clear /mnt/cachefs/testdir

  # Clear cache for multiple paths
  cachefs apiclient clear /mnt/cachefs/dir1 /mnt/cachefs/dir2

  # Clear cache using relative paths
  cachefs apiclient clear /testdir --relative

  # Shrink burst cache back to cache-size
  cachefs apiclient clear --clear-burst

  # Broadcast clear burst to all nodes in serf cluster
  cachefs apiclient clear --clear-burst --broadcast
`,
	}
}

func apiGetpid(c *cli.Context) error {
	client, err := apiclient.NewApiClient(c.String("api-server-sock"))
	if err != nil {
		return err
	}
	defer client.Close()

	pid, err := client.GetPid()
	if err != nil {
		return err
	}
	logger.Infof("Get cachefs PID: %d", pid)
	return nil
}

func apiServerInfo(c *cli.Context) error {
	client, err := apiclient.NewApiClient(c.String("api-server-sock"))
	if err != nil {
		return err
	}
	defer client.Close()

	info, err := client.GetServerInfo()
	if err != nil {
		return err
	}
	fmt.Printf("pid: %d\nblock_size: %d\ncache_size: %d\nburst_cache_size: %d\nshare_cap: %d\n",
		info.Pid, info.BlockSize, info.CacheSize, info.BurstCacheSize, info.ShareCap)
	return nil
}

func apiGetPeers(c *cli.Context) error {
	client, err := apiclient.NewApiClient(c.String("api-server-sock"))
	if err != nil {
		return err
	}
	defer client.Close()

	raw := c.Bool("raw")
	port := c.Bool("port")
	peers, err := client.GetPeers(raw, port)
	if err != nil {
		return err
	}
	if len(peers) == 0 {
		return nil
	}
	for _, peer := range peers {
		fmt.Println(peer)
	}
	return nil
}

func apiUnmount(c *cli.Context) error {
	client, err := apiclient.NewApiClient(c.String("api-server-sock"))
	if err != nil {
		return err
	}
	defer client.Close()
	err = client.Unmount(c.String("name"))
	if err != nil && !strings.Contains(err.Error(), "ttrpc: closed") {
		return err
	}
	logger.Info("Cachefs unmounted")
	return nil
}

func apiMetaReady(c *cli.Context) error {
	start := time.Now()
	client, err := apiclient.NewApiClient(c.String("api-server-sock"))
	if err != nil {
		return err
	}
	defer client.Close()
	err = client.MetaReady()
	if err != nil {
		return err
	}
	logger.Infof("Set meta ready in %s", time.Since(start))
	return nil
}

func apiDataReady(c *cli.Context) error {
	client, err := apiclient.NewApiClient(c.String("api-server-sock"))
	if err != nil {
		return err
	}
	err = client.DataReady()
	if err != nil {
		return err
	}
	logger.Info("Set data ready")
	return nil
}

func apiLoadStats(c *cli.Context) error {
	client, err := apiclient.NewApiClient(c.String("api-server-sock"))
	if err != nil {
		return err
	}
	defer client.Close()

	text, payload, err := client.GetLoadStats(c.Bool("log"))
	if err != nil {
		return err
	}
	// Straight to stdout, not through logger: --json output is meant to be piped
	// into jq, and the rendered report shouldn't carry a log prefix either.
	if c.Bool("json") {
		fmt.Println(string(payload))
	} else {
		fmt.Println(text)
	}
	return nil
}

// getfileOutputPath maps an input path to <outDir>/<path relative to its
// CacheFS mount>, falling back to stripping the leading '/' when the mount
// point can't be resolved (so distinct inputs never collide).
func getfileOutputPath(outDir, path string) string {
	mount, err := vfscommon.FindMountPoint(path)
	if err != nil {
		mount = ""
	}
	mount = strings.TrimRight(mount, "/")
	if mount != "" && strings.HasPrefix(path, mount+"/") {
		rel := strings.TrimPrefix(path, mount+"/")
		return filepath.Join(outDir, rel)
	}
	return filepath.Join(outDir, strings.TrimLeft(path, "/"))
}

type rangeTask struct {
	start  uint64
	length uint64
}

type mappedRange struct {
	task    rangeTask
	data    []byte
	err     error
	release func() error
}

type rangeProducer struct {
	mu          sync.Mutex
	next        uint64
	end         uint64
	rangeLength uint64
}

func newRangeProducer(fileSize int64, rangeLength uint64) (*rangeProducer, error) {
	if rangeLength == 0 {
		return nil, fmt.Errorf("range length must be positive")
	}
	if fileSize < 0 {
		return nil, fmt.Errorf("file size must not be negative")
	}
	return &rangeProducer{end: uint64(fileSize), rangeLength: rangeLength}, nil
}

func (p *rangeProducer) nextTask() (rangeTask, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.next == p.end {
		return rangeTask{}, false
	}
	length := min(p.rangeLength, p.end-p.next)
	task := rangeTask{start: p.next, length: length}
	p.next += length
	return task, true
}

const maxRangeWorkers = 32

func rangeWorkerLimit() int {
	if workers := runtime.GOMAXPROCS(0); workers > 0 {
		return min(workers, maxRangeWorkers)
	}
	return 1
}

func rangeWorkerCount(fileSize int64, rangeLength uint64) int {
	if fileSize <= 0 || rangeLength == 0 {
		return 1
	}
	rangeCount := (uint64(fileSize)-1)/rangeLength + 1
	return min(rangeWorkerLimit(), int(min(rangeCount, uint64(maxRangeWorkers))))
}

func acquireRangeTask(producer *rangeProducer, limiter chan struct{}, done <-chan struct{}) (rangeTask, bool) {
	select {
	case limiter <- struct{}{}:
	case <-done:
		return rangeTask{}, false
	}
	select {
	case <-done:
		<-limiter
		return rangeTask{}, false
	default:
	}
	task, ok := producer.nextTask()
	if !ok {
		<-limiter
	}
	return task, ok
}

type getfileRead struct {
	data    []byte
	release func() error
}

// fileRangeNote renders the requested range as a verbose-log suffix.
func fileRangeNote(fileRange *apiclient.FileRange) string {
	if fileRange == nil {
		return ""
	}
	return fmt.Sprintf(", range [%d, %d)", fileRange.Start, fileRange.Start+fileRange.Length)
}

type getfileReader func(string, *apiclient.FileRange) (getfileRead, error)

func mmapGetfileReader(client *apiclient.ApiClient, opts []apiclient.FileChunkFdOption) getfileReader {
	return func(path string, fileRange *apiclient.FileRange) (getfileRead, error) {
		rangeOpts := append([]apiclient.FileChunkFdOption{}, opts...)
		if fileRange != nil {
			rangeOpts = append(rangeOpts, apiclient.FileChunkFdWithRange(fileRange.Start, fileRange.Length))
		}
		data, err := client.MmapFile(path, rangeOpts...)
		if err != nil {
			return getfileRead{}, err
		}
		return getfileRead{
			data: data,
			release: func() error {
				return client.MunmapFile(data)
			},
		}, nil
	}
}

// mappedChunkFds owns one direct-FD receipt whose chunks are assembled into
// one contiguous read-only mapping — the same assembly MmapFile builds, done
// here from the batch's borrowed FDs. The selected bytes live in page cache
// the kernel can reclaim instead of a heap buffer.
type mappedChunkFds struct {
	batch    *apiclient.ChunkFds
	fileData []byte
	view     []byte
}

func mapChunkFds(batch *apiclient.ChunkFds) (*mappedChunkFds, error) {
	chunks := batch.Chunks()
	var mapSize, viewLen uint64
	for _, chunk := range chunks {
		mapSize += uint64(chunk.Size)
		viewLen += uint64(chunk.DataLen)
	}
	mapped := &mappedChunkFds{batch: batch}
	if mapSize == 0 {
		return mapped, nil
	}
	if mapSize > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("selected block span is too large to mmap")
	}
	fileData, err := unix.Mmap(-1, 0, int(mapSize), unix.PROT_NONE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate virtual address space: %v", err)
	}
	var offset uint64
	for _, chunk := range chunks {
		targetAddr := unsafe.Pointer(uintptr(unsafe.Pointer(&fileData[0])) + uintptr(offset))
		_, err := unix.MmapPtr(chunk.FD(), 0, targetAddr, uintptr(chunk.Size), unix.PROT_READ, unix.MAP_SHARED|unix.MAP_FIXED)
		if err != nil {
			_ = unix.Munmap(fileData)
			return nil, fmt.Errorf("failed to mmap chunk %d: %v", chunk.BlockIndex, err)
		}
		offset += uint64(chunk.Size)
	}
	mapped.fileData = fileData
	mapped.view = fileData[chunks[0].DataOffset : uint64(chunks[0].DataOffset)+viewLen]
	return mapped, nil
}

func (m *mappedChunkFds) release() error {
	var unmapErr error
	if m.fileData != nil {
		unmapErr = unix.Munmap(m.fileData)
	}
	closeErr := m.batch.Close()
	if unmapErr != nil {
		return unmapErr
	}
	return closeErr
}

func fdGetfileReader(client *apiclient.ApiClient, opts []apiclient.FileChunkFdOption) getfileReader {
	return func(path string, fileRange *apiclient.FileRange) (getfileRead, error) {
		rangeOpts := append([]apiclient.FileChunkFdOption{}, opts...)
		if fileRange != nil {
			rangeOpts = append(rangeOpts, apiclient.FileChunkFdWithRange(fileRange.Start, fileRange.Length))
		}
		batch, err := client.GetFileChunkFds(path, rangeOpts...)
		if err != nil {
			return getfileRead{}, err
		}
		mapped, mapErr := mapChunkFds(batch)
		if mapErr != nil {
			_ = batch.Close()
			return getfileRead{}, mapErr
		}
		return getfileRead{data: mapped.view, release: mapped.release}, nil
	}
}

func runRanges(reader getfileReader, path string, fileSize int64, rangeLength uint64, limiter chan struct{}, workers int, output io.Writer) error {
	producer, err := newRangeProducer(fileSize, rangeLength)
	if err != nil || fileSize == 0 {
		return err
	}
	if workers < 1 {
		workers = rangeWorkerCount(fileSize, rangeLength)
	}

	results := make(chan mappedRange, workers)
	done := make(chan struct{})
	var once sync.Once
	var firstErr error
	fail := func(err error) {
		once.Do(func() {
			firstErr = err
			close(done)
		})
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				task, ok := acquireRangeTask(producer, limiter, done)
				if !ok {
					return
				}
				read, readErr := reader(path, &apiclient.FileRange{Start: task.start, Length: task.length})
				if readErr != nil {
					<-limiter
					select {
					case results <- mappedRange{err: readErr}:
					case <-done:
					}
					return
				}
				result := mappedRange{
					task: task,
					data: read.data,
					release: func() error {
						if releaseErr := read.release(); releaseErr != nil {
							// Keep the permit because the resource may remain live until client cleanup.
							return releaseErr
						}
						<-limiter
						return nil
					},
				}
				select {
				case results <- result:
				case <-done:
					if unmapErr := result.release(); unmapErr != nil {
						logger.Warnf("Failed to munmap file %s: %v", path, unmapErr)
					}
					return
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	pending := make(map[uint64]mappedRange)
	next := uint64(0)
	for result := range results {
		if result.err != nil {
			fail(result.err)
			continue
		}
		if firstErr != nil {
			if unmapErr := result.release(); unmapErr != nil {
				logger.Warnf("Failed to munmap file %s: %v", path, unmapErr)
			}
			continue
		}
		pending[result.task.start] = result
		for {
			nextResult, ok := pending[next]
			if !ok {
				break
			}
			n, writeErr := output.Write(nextResult.data)
			if writeErr == nil && n != len(nextResult.data) {
				writeErr = io.ErrShortWrite
			}
			unmapErr := nextResult.release()
			delete(pending, next)
			if writeErr != nil {
				fail(writeErr)
				break
			}
			if unmapErr != nil {
				fail(unmapErr)
				break
			}
			next += nextResult.task.length
		}
	}
	for _, result := range pending {
		if unmapErr := result.release(); unmapErr != nil {
			logger.Warnf("Failed to munmap file %s: %v", path, unmapErr)
		}
	}
	return firstErr
}

func getfileCopyRanges(reader getfileReader, path, outPath string, verbose bool, rangeLength uint64, limiter chan struct{}, workers int, mode string) (int64, float64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, 0, err
	}
	if dir := filepath.Dir(outPath); dir != "" {
		if err = os.MkdirAll(dir, 0755); err != nil {
			return 0, 0, err
		}
	}
	output, err := os.CreateTemp(filepath.Dir(outPath), ".cachefs-getfile-*")
	if err != nil {
		return 0, 0, err
	}
	tempPath := output.Name()
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = output.Close()
			_ = os.Remove(tempPath)
		}
	}()
	if err = output.Chmod(0644); err != nil {
		return 0, 0, err
	}
	if err = output.Truncate(info.Size()); err != nil {
		return 0, 0, err
	}
	if info.Size() == 0 {
		if err = output.Close(); err != nil {
			return 0, 0, err
		}
		if err = os.Rename(tempPath, outPath); err != nil {
			return 0, 0, err
		}
		removeTemp = false
		return 0, 0, nil
	}

	start := time.Now()
	if err = runRanges(reader, path, info.Size(), rangeLength, limiter, workers, output); err != nil {
		return 0, float64(time.Since(start).Microseconds()) / 1000.0, err
	}
	if err = output.Close(); err != nil {
		return 0, float64(time.Since(start).Microseconds()) / 1000.0, err
	}
	if err = os.Rename(tempPath, outPath); err != nil {
		return 0, float64(time.Since(start).Microseconds()) / 1000.0, err
	}
	removeTemp = false
	mmapMs := float64(time.Since(start).Microseconds()) / 1000.0
	if verbose {
		fmt.Fprintf(os.Stderr, "%s: %d bytes, range %s %.1f ms -> %s\n", path, info.Size(), mode, mmapMs, outPath)
	}
	return info.Size(), mmapMs, nil
}

func getfileWriteRangesToStdout(reader getfileReader, path string, rangeLength uint64, limiter chan struct{}) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	return runRanges(reader, path, info.Size(), rangeLength, limiter, 0, os.Stdout)
}

// getfileReadOne reads path, writes its bytes to outPath, and returns
// (nbytes, readMs). The reader owns its backing resource until release, so mmap
// and direct-FD copies follow the same output and concurrent-range path.
func getfileReadOne(reader getfileReader, path, outPath string, verbose bool, fileRange *apiclient.FileRange, rangeLength uint64, limiter chan struct{}, rangeWorkers int, mode string) (int64, float64, error) {
	if rangeLength != 0 {
		return getfileCopyRanges(reader, path, outPath, verbose, rangeLength, limiter, rangeWorkers, mode)
	}

	start := time.Now()
	read, err := reader(path, fileRange)
	if err != nil {
		return 0, 0, err
	}
	readMs := float64(time.Since(start).Microseconds()) / 1000.0
	defer func() {
		if e := read.release(); e != nil {
			logger.Warnf("Failed to release file %s: %v", path, e)
		}
	}()
	if dir := filepath.Dir(outPath); dir != "" {
		if err = os.MkdirAll(dir, 0755); err != nil {
			return 0, 0, err
		}
	}
	if err = os.WriteFile(outPath, read.data, 0644); err != nil {
		return 0, readMs, err
	}
	nbytes := int64(len(read.data))
	if verbose {
		fmt.Fprintf(os.Stderr, "%s: %d bytes, %s %.1f ms%s -> %s\n", path, nbytes, mode, readMs, fileRangeNote(fileRange), outPath)
	}
	return nbytes, readMs, nil
}

func apiGetFile(c *cli.Context) error {
	sockPath := c.String("api-server-sock")
	noCacheLimit := c.Bool("no-cache-limit")
	verbose := c.Bool("verbose")
	output := c.String("output")
	outputDir := c.String("output-dir")
	rangeStartSet := c.IsSet("range-start")
	rangeLengthSet := c.IsSet("range-length")
	if rangeStartSet && !rangeLengthSet {
		return fmt.Errorf("--range-start requires --range-length")
	}
	if rangeLengthSet && c.Uint64("range-length") == 0 {
		return fmt.Errorf("--range-length must be positive")
	}
	rangeCopyLength := uint64(0)
	if rangeLengthSet && !rangeStartSet {
		rangeCopyLength = c.Uint64("range-length")
	}

	paths := c.Args().Slice()
	if len(paths) == 0 {
		return fmt.Errorf("at least one file path must be specified")
	}

	var client *apiclient.ApiClient
	var err error
	if noCacheLimit {
		client, err = apiclient.NewApiClient(sockPath)
	} else {
		// The SDK probes the daemon's share cap and installs a limiter.
		client, err = apiclient.NewApiClientAutoLimit(sockPath)
	}
	if err != nil {
		return fmt.Errorf("failed to create CacheFS client: %v", err)
	}
	defer client.Close()

	if !noCacheLimit {
		if limit := client.CacheLimit(); limit > 0 {
			logger.Infof("Cache limiter enabled: cap %s", utils.FormatBytes(uint64(limit)))
		} else {
			logger.Warnf("daemon does not report a share cap; cache limiter disabled, use --no-cache-limit to silence this")
		}
	}

	var opts []apiclient.FileChunkFdOption
	if wt := c.Int("wait-timeout"); wt > 0 {
		waitTimeout := time.Duration(wt) * time.Second
		opts = append(opts, apiclient.FileChunkFdWithWaitTimeout(waitTimeout))
	}
	var fileRange *apiclient.FileRange
	if rangeStartSet {
		fileRange = &apiclient.FileRange{Start: c.Uint64("range-start"), Length: c.Uint64("range-length")}
	}
	reader := mmapGetfileReader(client, opts)
	mode := "mmap"
	if c.Bool("fd") {
		reader = fdGetfileReader(client, opts)
		mode = "direct FD"
	}
	var rangeLimiter chan struct{}
	if rangeCopyLength != 0 {
		rangeLimiter = make(chan struct{}, rangeWorkerLimit())
	}

	// Concurrent mode: 2+ paths, one goroutine each, written under --output-dir.
	if len(paths) >= 2 {
		if output != "" {
			return fmt.Errorf("use -O/--output-dir for multiple files, not -o")
		}
		if outputDir == "" {
			return fmt.Errorf("-O/--output-dir is required for multiple files")
		}

		type result struct {
			path  string
			bytes int64
			ms    float64
			err   error
		}
		results := make(chan result, len(paths))
		wallStart := time.Now()
		for _, p := range paths {
			go func(path string) {
				outPath := getfileOutputPath(outputDir, path)
				n, ms, e := getfileReadOne(reader, path, outPath, verbose, fileRange, rangeCopyLength, rangeLimiter, 1, mode)
				results <- result{path, n, ms, e}
			}(p)
		}

		var latencies []float64
		var totalBytes int64
		failed := 0
		for i := 0; i < len(paths); i++ {
			r := <-results
			if r.err != nil {
				fmt.Fprintf(os.Stderr, "%s: error: %v\n", r.path, r.err)
				failed++
			} else {
				totalBytes += r.bytes
				latencies = append(latencies, r.ms)
			}
		}
		wallMs := float64(time.Since(wallStart).Microseconds()) / 1000.0

		if verbose || failed > 0 {
			sort.Float64s(latencies)
			min, med, max := 0.0, 0.0, 0.0
			if n := len(latencies); n > 0 {
				min = latencies[0]
				med = latencies[n/2]
				max = latencies[n-1]
			}
			fmt.Fprintf(os.Stderr, "concurrent getfile: %d ok, %d failed, cap=%d bytes, total %d bytes, wall %.1f ms, %s ms min/median/max = %.1f/%.1f/%.1f\n",
				len(latencies), failed, client.CacheLimit(), totalBytes, wallMs, mode, min, med, max)
		}
		if failed > 0 {
			return fmt.Errorf("%d of %d getfile requests failed", failed, len(paths))
		}
		return nil
	}

	// Single path.
	if output != "" && outputDir != "" {
		return fmt.Errorf("-o and -O/--output-dir are mutually exclusive")
	}
	path := paths[0]
	if outputDir != "" {
		outPath := getfileOutputPath(outputDir, path)
		_, _, err := getfileReadOne(reader, path, outPath, verbose, fileRange, rangeCopyLength, rangeLimiter, 0, mode)
		return err
	}
	if rangeCopyLength != 0 {
		if output != "" {
			_, _, err := getfileCopyRanges(reader, path, output, verbose, rangeCopyLength, rangeLimiter, 0, mode)
			return err
		}
		return getfileWriteRangesToStdout(reader, path, rangeCopyLength, rangeLimiter)
	}

	// Single-file behavior: write to the -o file or stdout.
	readStart := time.Now()
	read, err := reader(path, fileRange)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %v", path, err)
	}
	readMs := float64(time.Since(readStart).Microseconds()) / 1000.0
	defer func() {
		if err := read.release(); err != nil {
			logger.Warnf("Failed to release file: %v", err)
		}
	}()

	if verbose {
		fmt.Fprintf(os.Stderr, "%s: %d bytes, %s %.1f ms%s\n", path, len(read.data), mode, readMs, fileRangeNote(fileRange))
	}
	if output != "" {
		if err = os.WriteFile(output, read.data, 0644); err != nil {
			return fmt.Errorf("failed to write to file %s: %v", output, err)
		}
		logger.Infof("File content saved to: %s (%d bytes)", output, len(read.data))
	} else {
		if _, err = os.Stdout.Write(read.data); err != nil {
			return fmt.Errorf("failed to write to stdout: %v", err)
		}
	}
	return nil
}

func apiClear(c *cli.Context) error {
	client, err := apiclient.NewApiClient(c.String("api-server-sock"))
	if err != nil {
		return err
	}
	defer client.Close()

	clearBurst := c.Bool("clear-burst")
	broadcast := c.Bool("broadcast")

	opts := []apiclient.ClearCacheOption{}
	if broadcast {
		opts = append(opts, apiclient.ClearCacheWithBroadcast(true))
	}

	var paths []string
	if clearBurst {
		opts = append(opts, apiclient.ClearCacheWithClearBurst(true))
	} else {
		// Normal clear mode: at least one path required
		if c.NArg() == 0 {
			return fmt.Errorf("at least one path must be specified")
		}
		if c.Bool("relative") {
			paths = c.Args().Slice()
		} else {
			absPaths, err := getAbsolutePaths(c.Args().Slice())
			if err != nil {
				return err
			}
			if len(absPaths) == 0 {
				return fmt.Errorf("no valid paths to clear")
			}
			paths, err = convertToRelativePaths(absPaths)
			if err != nil {
				return err
			}
		}
	}

	count, bytes, err := client.ClearCache(paths, opts...)
	if err != nil {
		return err
	}
	if clearBurst {
		if count == 0 && bytes == 0 {
			logger.Infof("Clear burst: no burst cache to clear")
		} else {
			logger.Infof("Successfully cleared burst cache: evicted %d blocks %s", count, utils.FormatBytes(bytes))
		}
	} else {
		logger.Infof("Successfully cleared %d blocks %s", count, utils.FormatBytes(bytes))
	}
	return nil
}

func apiSetCache(c *cli.Context) error {
	cacheSizeMiB := c.Int("cache-size")
	burstSizeMiB := c.Int("burst-cache-size")
	restoreBurst := c.Bool("restore-burst-cache")
	broadcast := c.Bool("broadcast")

	if restoreBurst && c.IsSet("burst-cache-size") {
		return fmt.Errorf("--restore-burst-cache conflicts with --burst-cache-size")
	}

	client, err := apiclient.NewApiClient(c.String("api-server-sock"))
	if err != nil {
		return err
	}
	defer client.Close()

	opts := []apiclient.SetCacheSizeOption{}
	if broadcast {
		opts = append(opts, apiclient.SetCacheSizeWithBroadcast(true))
	}

	// CacheSizeKeepCurrent means "keep current value"; non-negative values are in MiB -> convert to bytes
	var cacheSize, burstSize int64
	if cacheSizeMiB >= 0 {
		cacheSize = int64(cacheSizeMiB) << 20
	} else {
		cacheSize = chunkcommon.CacheSizeKeepCurrent
	}
	if restoreBurst {
		burstSize = chunkcommon.BurstCacheSizeRestore
	} else if burstSizeMiB >= 0 {
		burstSize = int64(burstSizeMiB) << 20
	} else {
		burstSize = chunkcommon.CacheSizeKeepCurrent
	}

	newCacheSize, newBurstSize, newShareCap, evictCnt, evictBytes, err := client.SetCacheSize(cacheSize, burstSize, opts...)
	if err != nil {
		return err
	}

	var parts []string
	if cacheSizeMiB >= 0 {
		parts = append(parts, fmt.Sprintf("cache=%dMiB", cacheSizeMiB))
	}
	if restoreBurst {
		parts = append(parts, "burst=restore")
	} else if burstSizeMiB >= 0 {
		parts = append(parts, fmt.Sprintf("burst=%dMiB", burstSizeMiB))
	}
	var msg string
	if len(parts) > 0 {
		msg = strings.Join(parts, " ")
	} else {
		msg = "(no change)"
	}
	newCacheMiB := newCacheSize >> 20
	newBurstMiB := newBurstSize >> 20
	newShareMiB := newShareCap >> 20
	if evictCnt > 0 {
		logger.Infof("Cache size updated: %s; effective cache=%dMiB burst=%dMiB share_cap=%dMiB; evicted %d blocks (%s)",
			msg, newCacheMiB, newBurstMiB, newShareMiB, evictCnt, utils.FormatBytes(evictBytes))
	} else {
		logger.Infof("Cache size updated: %s; effective cache=%dMiB burst=%dMiB share_cap=%dMiB",
			msg, newCacheMiB, newBurstMiB, newShareMiB)
	}
	return nil
}

func apiPreload(c *cli.Context) error {
	client, err := apiclient.NewApiClient(c.String("api-server-sock"))
	if err != nil {
		return fmt.Errorf("failed to create CacheFS client: %v", err)
	}
	defer client.Close()

	opts := []apiclient.LoadCacheOption{}
	broadcast := c.Bool("broadcast")
	if broadcast {
		opts = append(opts, apiclient.LoadCacheWithBroadcast(true))
	}
	// Handle cancel request first
	if c.Bool("cancel") {
		logger.Info("Cancelling current preload request")
		opts = append(opts, apiclient.LoadCacheWithCancel(true))
		_, _, err := client.LoadCache(nil, opts...)
		if err != nil {
			return fmt.Errorf("cancel preload failed: %v", err)
		}
		logger.Info("Cancel preload done")
		return nil
	}

	if c.NArg() == 0 {
		return fmt.Errorf("at least one path must be specified")
	}

	var relativePaths []string
	if c.Bool("relative") {
		relativePaths = c.Args().Slice()
	} else {
		// Get absolute paths
		absPaths, err := getAbsolutePaths(c.Args().Slice())
		if err != nil {
			return err
		}

		if len(absPaths) == 0 {
			return fmt.Errorf("no valid paths to preload")
		}

		// Find mount point and convert to relative paths
		relativePaths, err = convertToRelativePaths(absPaths)
		if err != nil {
			return err
		}
	}

	background := c.Bool("background")
	loadAll := c.Bool("all")
	force := c.Bool("force")

	logger.Infof("Preload paths: %v, background=%v, broadcast=%v, loadAll=%v, force=%v",
		relativePaths, background, broadcast, loadAll, force)

	if background {
		opts = append(opts, apiclient.LoadCacheWithBackground(true))
	}
	if loadAll {
		opts = append(opts, apiclient.LoadCacheWithLoadAll(true))
	}
	if force {
		opts = append(opts, apiclient.LoadCacheWithForce(true))
	}

	count, bytes, err := client.LoadCache(relativePaths, opts...)
	if err != nil {
		return fmt.Errorf("preload failed: %v", err)
	}

	if background {
		logger.Info("Preload request submitted in background")
	} else {
		logger.Infof("Preload completed: %d files, %s", count, utils.FormatBytes(bytes))
	}

	return nil
}
