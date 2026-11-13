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
	transport  *transport
	grpcClient pb.CachefsCacheClient
	conn       *grpc.ClientConn
	atime      time.Time
}

func newTransportClient(addr string, transport *transport, opts ...grpc.DialOption) (*transportClient, error) {
	conn, err := rpcsvc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}
	client := &transportClient{
		transport:  transport,
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

func (c *transportClient) load(ctx context.Context, target string, blk uint32, page *Page, group string, cacheroot, useFallback bool) error {
	config := c.transport.config
	primary, fallback, err := c.transport.servicesFor(cacheroot)
	if err != nil {
		return err
	}
	if useFallback && fallback == nil {
		return fmt.Errorf("fallback to TCP not available: no local TCP service")
	}
	stream, err := c.grpcClient.Load(ctx)
	if err != nil {
		return err
	}

	req := &pb.LoadRequest{
		Target:           target,
		BlockSize:        config.blockSize,
		BlockNum:         blk,
		Len:              uint32(len(page.Data)),
		SerfGroup:        config.serfGroup,
		CacheRootGroup:   group,
		SupportsFallback: fallback != nil,
		UseFallback:      useFallback,
	}
	if err = stream.Send(req); err != nil {
		return err
	}

	res, err := stream.Recv()
	if err != nil {
		return err
	}

	if config.checksumEnabled() {
		hash := res.GetHash()
		hasher, hashVal, err := parseDigest(hash, config.hasher)
		if err != nil {
			return fmt.Errorf("parse digest failed: %s", err)
		}
		page.Digest = hashVal
		page.Hasher = config.hasher
		logger.Debugf("Cachefs transport client load %s block %d with checksum %s:%s", req.Target, req.BlockNum, hasher, hashVal)
	}

	service := primary
	downAddr := res.GetAddr()
	if useFallback {
		if res.GetFallbackAddr() == "" {
			return fmt.Errorf("fallback to TCP not available: no TCP address")
		}
		service = fallback
		downAddr = res.GetFallbackAddr()
	} else if downAddr == "" {
		if res.GetFallbackAddr() == "" {
			return fmt.Errorf("fallback to TCP not available: no TCP address")
		}
		if fallback == nil {
			return fmt.Errorf("fallback to TCP not available: no local TCP service")
		}
		service = fallback
		downAddr = res.GetFallbackAddr()
	}

	reqID := transfer.RequestID(res.GetReqId())
	if err = service.Download(ctx, downAddr, res.GetSvcId(), reqID, page.Data); err != nil {
		return err
	}

	_ = stream.Send(&pb.LoadRequest{})
	return nil
}

func (c *transportClient) exchangeTracker(ctx context.Context, localMetaInfo []byte) ([]byte, error) {
	req := &pb.ExchangeTrackerRequest{
		SerfGroup: c.transport.config.serfGroup,
		MetaInfo:  localMetaInfo,
	}
	res, err := c.grpcClient.ExchangeTracker(ctx, req)
	if err != nil {
		return nil, err
	}
	return res.GetMetaInfo(), nil
}
