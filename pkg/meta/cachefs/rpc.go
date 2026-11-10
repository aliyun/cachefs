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

package cachefsmeta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	pb "github.com/aliyun/cachefs/pkg/rpc/meta"
	"github.com/aliyun/cachefs/pkg/rpcsvc"
	"github.com/aliyun/cachefs/pkg/utils"
	"google.golang.org/grpc"
)

func (m *metaData) waitAndLoadMeta() (rerr error) {
	if !m.config.MetaConsumer() && !m.config.PeerMeta() {
		return errors.New("Not in sourceless meta consumer mode nor peer meta mode")
	}

	// In peer meta mode the whole wait+load pipeline is best-effort: use
	// PeerMetaTimeout as a single deadline covering both phases so that a
	// slow load cannot stall startup indefinitely.
	ctx := context.Background()
	if m.config.PeerMeta() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx,
			time.Second*time.Duration(m.config.PeerMetaTimeout))
		defer cancel()
	}

	start := time.Now()
	var candidates []string
	var todel []string
	var metaAddr string
	var loadedMeta *dumpedCachefs
	defer func() {
		m.delMetaSource(todel)
		if rerr != nil {
			return
		}
		m.dumpedMeta.Store(loadedMeta)
		m.metaReady.Store(true)
		m.announceMetaSource()
		logger.Infof("Wait and load meta from %s in %s, %d inodes %dMiB data (%d)",
			metaAddr, time.Since(start), m.totalInode, m.totalSize>>20, m.totalSize)
	}()

WAIT:
	logger.Info("Wait for filesystem meta announcements")
	if m.config.PeerMeta() {
		// Load meta from peer is best-effort in peer meta mode, as we can
		// fallback to source dir. Use the shared deadline so that the wait
		// phase and the load phase together respect PeerMetaTimeout.
		select {
		case <-m.metaSourceCh:
		case <-ctx.Done():
			return fmt.Errorf("wait for meta source timed out: %w", ctx.Err())
		}
	} else if m.config.MetaConsumer() {
		// Load meta from peer is mandatory in sourceless meta consumer mode,
		// as there's no source dir
		<-m.metaSourceCh
	}

	// Try nodes other than meta provider first, to avoid overloading meta provider node.
	m.metaLock.Lock()
	candidates = make([]string, 0, len(m.metaSource))
	todel = make([]string, 0, len(m.metaSource))
	for addr := range m.metaSource {
		candidates = append(candidates, addr)
	}
	m.metaLock.Unlock()

	doLoad := func(addr string) (*dumpedCachefs, error) {
		client, err := m.getClient(addr)
		if err != nil {
			return nil, err
		}
		return client.loadMeta(ctx, m.serfGroup)
	}

	for _, addr := range candidates {
		if addr == m.localAddr {
			continue // skip loading from ourselves
		}
		meta, err := doLoad(addr)
		if err != nil {
			logger.Warnf("Load meta from %s: %s", addr, err)
			todel = append(todel, addr)
			continue
		}
		err = m.loadMeta(meta)
		if err != nil {
			return fmt.Errorf("Restore meta failed: %s", err)
		}
		loadedMeta = meta
		metaAddr = addr
		return nil
	}

	// Try meta provider at last
	if m.metaProvider == "" {
		goto WAIT
	}
	meta, err := doLoad(m.metaProvider)
	if err != nil {
		todel = append(todel, m.metaProvider)
		logger.Warnf("Load meta from meta provider %s: %s", m.metaProvider, err)
		goto WAIT
	}
	err = m.loadMeta(meta)
	if err != nil {
		return fmt.Errorf("Restore meta failed: %s", err)
	}
	loadedMeta = meta
	metaAddr = m.metaProvider
	return nil
}

func (m *metaData) LoadMeta(ctx context.Context, req *pb.LoadMetaReq) (*pb.LoadMetaRes, error) {
	group := req.GetSerfGroup()
	addr := req.GetAddr()
	m.loadMetaCount.Inc()

	if !m.metaReady.Load() {
		return nil, fmt.Errorf("meta not ready")
	}

	logger.Infof("Cachefs meta server LoadMeta: request from %s", addr)
	if group != m.serfGroup {
		return nil, fmt.Errorf("serf group mismatch: from \"%s\" to \"%s\"", group, m.serfGroup)
	}
	meta, err := m.dumpMeta()
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(meta)
	if err != nil {
		logger.Warnf("Failed to marshal dumped meta: %s", err)
		return nil, err
	}
	return &pb.LoadMetaRes{Meta: payload}, nil
}

type metaClient struct {
	grpcClient pb.CachefsMetaClient
	conn       *grpc.ClientConn
	addr       string
	atime      time.Time
}

func (c *metaClient) loadMeta(ctx context.Context, group string) (*dumpedCachefs, error) {
	req := &pb.LoadMetaReq{
		Addr:      c.addr,
		SerfGroup: group,
	}
	res, err := c.grpcClient.LoadMeta(ctx, req)
	if err != nil {
		return nil, err
	}
	payload := res.GetMeta()
	meta := &dumpedCachefs{}
	if err := json.Unmarshal(payload, meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func (c *metaClient) stop() error {
	return c.conn.Close()
}

func (m *metaData) newMetaClient(addr, localAddr string, opts ...grpc.DialOption) (*metaClient, error) {
	conn, err := rpcsvc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}
	client := &metaClient{
		grpcClient: pb.NewCachefsMetaClient(conn),
		conn:       conn,
		addr:       localAddr,
		atime:      time.Now(),
	}
	return client, nil
}

func (m *metaData) getClient(addr string) (*metaClient, error) {
	m.clientMu.Lock()
	defer m.clientMu.Unlock()

	var err error
	client, ok := m.clients[addr]
	if ok {
		client.atime = time.Now()
		return client, nil
	}
	client, err = m.newMetaClient(addr, m.rpc.Addr())
	if err != nil {
		return nil, err
	}
	m.clients[addr] = client
	return client, nil
}

func (m *metaData) gcClients() {
	for {
		select {
		case <-m.stopCh:
			return
		default:
		}

		now := time.Now()
		m.clientMu.Lock()
		for addr, client := range m.clients {
			if client.atime.Add(time.Second * 10).Before(now) {
				if err := client.stop(); err != nil {
					logger.Warnf("Stop metaData client to %s: %s", addr, err)
				}
				delete(m.clients, addr)
				logger.Debugf("GC meta client to %s", addr)
			}
		}
		m.clientMu.Unlock()
		utils.SleepWithJitter(time.Minute * 10)
	}
}
