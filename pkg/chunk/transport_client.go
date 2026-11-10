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

package chunk

import (
	"context"
	"fmt"
	"time"

	pb "github.com/aliyun/cachefs/pkg/rpc/cachefs"
	"github.com/aliyun/cachefs/pkg/rpcsvc"
	"github.com/aliyun/cachefs/pkg/transfer"
	"google.golang.org/grpc"
)

type transportClient struct {
	config     *remoteCacheConfig
	service    transfer.TransferClient
	grpcClient pb.CachefsCacheClient
	conn       *grpc.ClientConn
	atime      time.Time
}

func newTransportClient(addr string, cfg *remoteCacheConfig, service transfer.TransferClient, opts ...grpc.DialOption) (*transportClient, error) {
	conn, err := rpcsvc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}
	client := &transportClient{
		config:     cfg,
		service:    service,
		grpcClient: pb.NewCachefsCacheClient(conn),
		conn:       conn,
		atime:      time.Now(),
	}
	return client, nil
}

func (c *transportClient) touch() {
	c.atime = time.Now()
}

func (c *transportClient) getAtime() time.Time {
	return c.atime
}

func (c *transportClient) stop() error {
	return c.conn.Close()
}

func (c *transportClient) load(ctx context.Context, target string, blk uint32, page *Page, group string) error {
	stream, err := c.grpcClient.Load(ctx)
	if err != nil {
		return err
	}

	// Send request to peer
	req := &pb.LoadRequest{
		Target:         target,
		BlockSize:      c.config.blockSize,
		BlockNum:       blk,
		Len:            uint32(len(page.Data)),
		SerfGroup:      c.config.serfGroup,
		CacheRootGroup: group,
	}
	err = stream.Send(req)
	if err != nil {
		return err
	}

	// Get real download address and optional data checksum digest
	res, err := stream.Recv()
	if err != nil {
		return err
	}

	// Parse and save hasher & digest of this block if there's any
	if c.config.checksumEnabled() {
		hash := res.GetHash()
		hasher, hashVal, err := parseDigest(hash, c.config.hasher)
		if err != nil {
			return fmt.Errorf("parse digest failed: %s", err)
		}
		page.Digest = hashVal
		page.Hasher = c.config.hasher
		logger.Debugf("Cachefs transport client load %s block %d with checksum %s:%s", req.Target, req.BlockNum, hasher, hashVal)
	}

	// Download data from peer via transfer service
	downAddr := res.GetAddr()
	reqID := transfer.RequestID(res.GetReqId())
	svcID := res.GetSvcId()
	err = c.service.Download(ctx, downAddr, svcID, reqID, page.Data)
	if err != nil {
		return err
	}

	// Download done, send empty request to notify server
	_ = stream.Send(&pb.LoadRequest{})
	return nil
}

func (c *transportClient) exchangeTracker(ctx context.Context, localMetaInfo []byte) ([]byte, error) {
	req := &pb.ExchangeTrackerRequest{
		SerfGroup: c.config.serfGroup,
		MetaInfo:  localMetaInfo,
	}
	res, err := c.grpcClient.ExchangeTracker(ctx, req)
	if err != nil {
		return nil, err
	}
	return res.GetMetaInfo(), nil
}
