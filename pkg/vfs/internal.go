/*
 * Copyright 2023 Alibaba Cloud, Inc. or its affiliates.
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

package vfs

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aliyun/cachefs/pkg/meta"
	cfsmeta "github.com/aliyun/cachefs/pkg/meta/cachefs"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/aliyun/cachefs/pkg/vfs/common"
	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

const (
	minInternalNode = common.MinInternalNode
	logInode        = common.LogInode
	controlInode    = common.ControlInode
	statsInode      = common.StatsInode
	configInode     = common.ConfigInode
	cachefsInode    = common.CachefsInode
)

type internalNode struct {
	inode Ino
	name  string
	attr  *Attr
}

var internalNodes = []*internalNode{
	{logInode, ".accesslog", &Attr{Mode: 0400}},
	{controlInode, ".control", &Attr{Mode: 0600}},
	{statsInode, ".stats", &Attr{Mode: 0444}},
	{configInode, ".config", &Attr{Mode: 0400}},
	{cachefsInode, ".cachefs", &Attr{Mode: 0400}},
}

func init() {
	uid := uint32(os.Getuid())
	gid := uint32(os.Getgid())
	now := time.Now().Unix()
	for _, v := range internalNodes {
		v.attr.Typ = meta.TypeFile
		v.attr.Nlink = 1
		v.attr.Uid = uid
		v.attr.Gid = gid
		v.attr.Atime = now
		v.attr.Mtime = now
		v.attr.Ctime = now
		v.attr.Full = true
	}
}

func IsSpecialNode(ino Ino) bool {
	return ino >= minInternalNode
}

func getInternalNode(ino Ino) *internalNode {
	for _, n := range internalNodes {
		if ino == n.inode {
			return n
		}
	}
	return nil
}

func getInternalNodeByName(name string) *internalNode {
	if name[0] != '.' {
		return nil
	}
	for _, n := range internalNodes {
		if name == n.name {
			return n
		}
	}
	return nil
}

func collectMetrics(registry *prometheus.Registry) []byte {
	if registry == nil {
		return []byte("")
	}
	mfs, err := registry.Gather()
	if err != nil {
		logger.Errorf("collect metrics: %s", err)
		return nil
	}
	w := bytes.NewBuffer(nil)
	format := func(v float64) string {
		return strconv.FormatFloat(v, 'f', -1, 64)
	}
	for _, mf := range mfs {
		for _, m := range mf.Metric {
			var name string = *mf.Name
			for _, l := range m.Label {
				if *l.Name != "mp" && *l.Name != "vol_name" {
					name += "_" + *l.Value
				}
			}
			switch *mf.Type {
			case io_prometheus_client.MetricType_GAUGE:
				_, _ = fmt.Fprintf(w, "%s %s\n", name, format(*m.Gauge.Value))
			case io_prometheus_client.MetricType_COUNTER:
				_, _ = fmt.Fprintf(w, "%s %s\n", name, format(*m.Counter.Value))
			case io_prometheus_client.MetricType_HISTOGRAM:
				_, _ = fmt.Fprintf(w, "%s_total %d\n", name, *m.Histogram.SampleCount)
				_, _ = fmt.Fprintf(w, "%s_sum %s\n", name, format(*m.Histogram.SampleSum))
			case io_prometheus_client.MetricType_SUMMARY:
			}
		}
	}
	return w.Bytes()
}

func writeProgress(count, bytes *uint64, out io.Writer, done chan struct{}) {
	wb := utils.NewBuffer(17)
	wb.Put8(meta.CPROGRESS)
	if bytes == nil {
		bytes = new(uint64)
	}
	ticker := time.NewTicker(time.Millisecond * 300)
	for {
		select {
		case <-ticker.C:
			wb.Put64(atomic.LoadUint64(count))
			wb.Put64(atomic.LoadUint64(bytes))
			_, _ = out.Write(wb.Bytes())
			wb.Seek(1)
		case <-done:
			ticker.Stop()
			if *count > 0 || *bytes > 0 {
				wb.Put64(atomic.LoadUint64(count))
				wb.Put64(atomic.LoadUint64(bytes))
				_, _ = out.Write(wb.Bytes())
			}
			return
		}
	}
}

func (c *Cachefs) handleCmdPreload(ctx meta.Context, r *utils.Buffer, out io.Writer) {
	paths := strings.Split(string(r.Get(int(r.Get32()))), "\n")
	background := r.Get8()
	broadcast := r.Get8()
	loadAll := r.Get8()
	force := r.Get8()
	cancel := r.Get8()

	// consume paddings
	_ = r.Get32()

	// Broadcast to cluster first if requested
	if broadcast == 1 {
		if c.serf == nil {
			logger.Warnf("Broadcast preload requested but serf service not available")
			_, _ = out.Write([]byte{byte(syscall.EAGAIN & 0xff)})
			return
		}
		info := &serfInfo{
			SrcAddr: c.serf.LocalAddr(),
			Paths:   paths,
			LoadAll: loadAll == 1,
			Force:   force == 1,
			Cancel:  cancel == 1,
		}
		err := c.broadCast(preloadEvent, info)
		if err != nil {
			logger.Warnf("Broadcast preload failed: %s", err)
			_, _ = out.Write([]byte{byte(syscall.EIO & 0xff)})
			return
		}
		if cancel == 1 {
			logger.Info("Successfully broadcast preload cancellation request to cluster")
		} else {
			logger.Infof("Successfully broadcast preload request to cluster, paths: %v", paths)
		}
	}

	// Always perform local preload
	req := &loadReq{
		paths:   paths,
		loadAll: loadAll == 1,
		force:   force == 1,
		cancel:  cancel == 1,
	}
	if background == 0 {
		var count, bytes uint64
		done := make(chan struct{})
		req.ctx = ctx
		req.count = &count
		req.bytes = &bytes
		req.done = done
		err := c.loadCache(req)
		if err != 0 {
			logger.Warnf("Preload command failed: %s", err)
			_, _ = out.Write([]byte{byte(err & 0xff)})
			return
		}
		writeProgress(&count, &bytes, out, done)
	} else {
		req.ctx = meta.NewContext(ctx.Pid(), ctx.Uid(), ctx.Gids())
		err := c.loadCache(req)
		if err != 0 {
			logger.Warnf("Background preload command failed: %s", err)
			_, _ = out.Write([]byte{byte(err & 0xff)})
			return
		}
	}
	_, _ = out.Write([]byte{0})
}

func (c *Cachefs) handleCmdClear(ctx meta.Context, r *utils.Buffer, out io.Writer) {
	if c.Conf.IsFuseLayerMode() {
		// TODO: Fuse layer mode doesn't support clear cache yet
		_, _ = out.Write([]byte{byte(syscall.ENOTSUP & 0xff)})
		return
	}

	// Read paths from buffer
	pathsSize := r.Get32()
	var paths []string
	if pathsSize > 0 {
		pathsBytes := r.Get(int(pathsSize))
		pathsStr := string(pathsBytes)
		paths = strings.Split(pathsStr, "\n")
	}

	// Read broadcast flag
	broadcast := r.Get8()
	// Read clear_burst flag
	clearBurst := r.Get8()
	// consume paddings
	for i := 0; i < 6; i++ {
		_ = r.Get8()
	}

	// Handle clear burst mode: paths are ignored
	if clearBurst == 1 {
		if pathsSize > 0 {
			logger.Info("Paths are ignored in --clear-burst mode")
		}
		paths = nil
	} else {
		// Normal clear mode: paths required
		if pathsSize == 0 {
			_, _ = out.Write([]byte{0})
			return
		}
	}

	// Broadcast to cluster if requested
	if broadcast == 1 {
		if c.serf == nil {
			logger.Warnf("Broadcast clear cache requested but serf service not available")
			_, _ = out.Write([]byte{byte(syscall.EAGAIN & 0xff)})
			return
		}
		info := &serfInfo{
			SrcAddr:    c.serf.LocalAddr(),
			Paths:      paths,
			ClearBurst: clearBurst == 1,
		}
		if err := c.broadCast(clearEvent, info); err != nil {
			logger.Warnf("Broadcast clear cache failed: %s", err)
			_, _ = out.Write([]byte{byte(syscall.EIO & 0xff)})
			return
		}
		if clearBurst == 1 {
			logger.Info("Successfully broadcast clear burst request to cluster")
		} else {
			logger.Infof("Successfully broadcast clear cache request to cluster, paths: %v", paths)
		}
	}

	// Perform local clear operation
	var count, bytes uint64
	start := time.Now()
	done := make(chan struct{})

	go func() {
		var cnt, size int64
		if clearBurst == 1 {
			// Clear burst cache
			cnt, size = c.cache.clearBurst()
		} else {
			// Clear normal cache by prefix
			prefix := c.pathsToCachePrefixes(paths)
			if len(prefix) > 0 {
				cnt, size = c.cache.removeByPrefix(prefix, nil)
			}
		}
		count = uint64(cnt)
		bytes = uint64(size)
		close(done)
	}()

	writeProgress(&count, &bytes, out, done)

	// Log result
	if clearBurst == 1 {
		logger.Infof("Clear burst: evicted %d blocks %dMiB in %s", count, bytes>>20, time.Since(start))
	} else {
		prefix := c.pathsToCachePrefixes(paths)
		logger.Infof("Evicted %d blocks %dMiB cache from paths: %v, with prefix: %v, in %s",
			count, bytes>>20, paths, prefix, time.Since(start))
	}

	_, _ = out.Write([]byte{0})
}

func (c *Cachefs) handleCmdSourceReady(ctx meta.Context, r *utils.Buffer, out io.Writer) {
	var count, bytes uint64
	if !c.Conf.Meta.MetaProvider() {
		logger.Warn("Set source ready: not meta provider in sourceless mode")
		_, _ = out.Write([]byte{byte(syscall.ENOTSUP & 0xff)})
		return
	}
	err := c.Meta.SourceReady(true)
	if err != 0 {
		logger.Warnf("Prepare set source ready failed: %s", err)
		_, _ = out.Write([]byte{byte(err & 0xff)})
		return
	}

	// Get background config and consume paddings
	background := r.Get8()
	for i := 0; i < 3; i++ {
		_ = r.Get8()
	}

	// Load from root to walk all entries to populate filesystem meta.
	start := time.Now()
	done := make(chan struct{})
	req := &loadReq{
		paths:   []string{"/"},
		count:   &count,
		bytes:   &bytes,
		loadAll: true,
		force:   true,
		done:    done,
	}
	if background == 0 {
		req.ctx = ctx
		go writeProgress(&count, &bytes, out, done)
	} else {
		req.ctx = meta.NewContext(ctx.Pid(), ctx.Uid(), ctx.Gids())
	}

	err = c.loadCache(req)
	if err != 0 {
		logger.Warnf("Load data while set source ready failed: %s", err)
		_, _ = out.Write([]byte{byte(err & 0xff)})
		return
	}

	sourceReady := func() syscall.Errno {
		// Load data done, announce meta data
		err := c.Meta.SourceReady(false)
		if err != 0 {
			logger.Warnf("Set source ready failed: %s", err)
		} else {
			logger.Infof("Set source ready in %ss", time.Since(start))
		}
		return err
	}

	// Run background
	if background == 1 {
		_, _ = out.Write([]byte{0})
		go func() {
			<-done
			_ = sourceReady()
		}()
		return
	}

	select {
	case <-req.ctx.Done():
		logger.Info("Set source ready cancelled")
		_, _ = out.Write([]byte{byte(syscall.EINTR & 0xff)})
		return
	case <-done:
	}
	err = sourceReady()
	_, _ = out.Write([]byte{byte(err & 0xff)})
}

func (c *Cachefs) handleCmdMetaReady(ctx meta.Context, r *utils.Buffer, out io.Writer) {
	err := c.Meta.MetaReady()
	_, _ = out.Write([]byte{byte(err & 0xff)})
}

func (c *Cachefs) handleInternalMsg(ctx meta.Context, cmd uint32, r *utils.Buffer, out io.Writer) {
	switch cmd {
	case cfsmeta.CmdPreload:
		c.handleCmdPreload(ctx, r, out)
	case cfsmeta.CmdClear:
		c.handleCmdClear(ctx, r, out)
	case cfsmeta.CmdSourceReady:
		c.handleCmdSourceReady(ctx, r, out)
	case cfsmeta.CmdMetaReady:
		c.handleCmdMetaReady(ctx, r, out)
	default:
		logger.Warnf("Unknown control message type: %d", cmd)
		_, _ = out.Write([]byte{byte(syscall.EINVAL & 0xff)})
	}
}
