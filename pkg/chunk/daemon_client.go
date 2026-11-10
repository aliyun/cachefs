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

package chunk

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aliyun/cachefs/pkg/utils"
)

type DaemonClient interface {
	// Get `blk` block of `target`
	GetBlock(ctx context.Context, target string, blk, blklen uint32) (*Page, error)

	// Get a reader that reads `blk` block of `target`
	ReadBlock(ctx context.Context, target string, blk, blklen uint32) (ChunkReader, error)

	// Fill cache of `blk` block of `target`
	FillBlock(ctx context.Context, target string, blk, blklen uint32) error

	// Put `blk` block of `target`
	PutBlock(ctx context.Context, target string, blk, blklen uint32, page *Page) error

	// Prepare and mark `target` as a whole
	Prepare(ctx context.Context, target string, length uint64) error

	// Commit `target` after writing full data
	Commit(ctx context.Context, target string) error

	// Get block size of daemon
	GetBlockSize(ctx context.Context) (uint32, error)

	// Stop daemon client
	Stop()
}

type daemonClient struct {
	daemonConn unixConn
	daemonAddr *net.UnixAddr
	connKey    []byte
	blockSize  uint32
	reqSeq     *atomic.Uint32 // request seq number

	reqMu           sync.Mutex
	requests        map[uint32]*daemonReq
	abortedRequests map[uint32]*daemonReq
	reqCh           chan *daemonReq
	stopCh          chan struct{}
	reqTimeout      time.Duration

	sendReady chan struct{}
	recvReady chan struct{}
}

func NewDaemonClient(socket string, blockSize uint32, reqTimeout time.Duration) (DaemonClient, error) {
	addr := net.UnixAddr{Name: socket, Net: "unix"}
	client := &daemonClient{
		daemonAddr:      &addr,
		blockSize:       blockSize,
		reqSeq:          &atomic.Uint32{},
		requests:        make(map[uint32]*daemonReq),
		abortedRequests: make(map[uint32]*daemonReq),
		reqCh:           make(chan *daemonReq, 1024),
		stopCh:          make(chan struct{}),
		sendReady:       make(chan struct{}),
		recvReady:       make(chan struct{}),
		reqTimeout:      reqTimeout,
	}
	client.reqSeq.Store(0)
	if err := client.connect(); err != nil {
		logger.Warnf("Initial connection failed: %s", err)
	}

	go client.recvResponses()
	go client.sendRequests()
	if blockSize == 0 {
		// Wait for send and receive to be ready.
		<-client.sendReady
		<-client.recvReady
		blksiz, err := client.GetBlockSize(context.Background())
		if err != nil {
			client.Stop()
			return nil, err
		}
		logger.Infof("Daemon client block size %dMiB", blksiz>>20)
		client.blockSize = blksiz
	}
	return client, nil
}

// Connect to cachefs daemon
func (c *daemonClient) connect() error {
	oldConn := c.daemonConn
	newConn, err := net.DialUnix("unix", nil, c.daemonAddr)
	if err != nil {
		return err
	}
	connKey := c.connKey
	if connKey == nil {
		connKey, err = getConnKey()
		if err != nil {
			return err
		}
		c.connKey = connKey
	}
	_, err = newConn.Write(connKey)
	if err != nil {
		return err
	}
	if oldConn != nil {
		oldConn.Close()
	}

	// set no read deadline, read will not time out
	if err := newConn.SetReadDeadline(time.Time{}); err != nil {
		logger.Warnf("Set connection read deadline to zero: %s", err)
		newConn.Close()
		return err
	}

	logger.Infof("Connected to cachefs daemon at %s", c.daemonAddr)
	c.daemonConn = newConn
	return nil
}

// dispatch received response to the according request based on ID.
func (c *daemonClient) dispatchResponse(resp *Resp) {
	id := resp.Header.Id
	aborted := false
	c.reqMu.Lock()
	req, ok := c.requests[id]
	if !ok {
		req, ok = c.abortedRequests[id]
		if ok {
			aborted = true
			delete(c.abortedRequests, id)
		}
	}
	c.reqMu.Unlock()
	if !ok {
		logger.Warnf("dispatchResponse: request not found for ID %d", id)
		if resp.File != nil {
			resp.File.Close()
		}
	} else {
		if aborted {
			logger.Debugf("dispatchResponse: request aborted for ID %d", id)
			if resp.File != nil {
				resp.File.Close()
			}
		} else {
			req.respCh <- resp
		}
	}
}

func (c *daemonClient) abortRequests(err error) {
	c.reqMu.Lock()
	defer c.reqMu.Unlock()
	for _, req := range c.requests {
		req.respCh <- err
	}
}

func (c *daemonClient) sendRequests() {
	close(c.sendReady)
	for {
		select {
		case <-c.stopCh:
			return
		case req := <-c.reqCh:
			err := writeRequest(c.daemonConn, req.req)
			if err != nil {
				req.respCh <- err
			}
		}
	}
}

func (c *daemonClient) recvResponses() {
	close(c.recvReady)
	for {
		select {
		case <-c.stopCh:
			return
		default:
			resp, err := readResponse(c.daemonConn)
			if err == nil {
				c.dispatchResponse(resp)
				continue
			}

			// not connected yet, try connect again
			if err == errNotConnected {
				time.Sleep(100 * time.Millisecond)
				c.connect()
				continue
			}

			// if err is net.OpError, daemon restarts or crashed? abort all pending
			// requests then wait and try reconnect
			if _, ok := err.(*net.OpError); ok {
				c.abortRequests(err)
				time.Sleep(100 * time.Millisecond)
				c.connect()
				continue
			}

			// for other internal errors, close current connection and reconnect
			logger.Warnf("Recv response failed: %s", err)
			oldConn := c.daemonConn
			c.daemonConn = nil
			oldConn.Close()
			c.abortRequests(err)
			c.connect()
		}
	}
}

func (c *daemonClient) addRequest(id uint32, req *daemonReq) {
	c.reqMu.Lock()
	c.requests[id] = req
	c.reqMu.Unlock()
}

func (c *daemonClient) delRequest(id uint32) {
	c.reqMu.Lock()
	delete(c.requests, id)
	c.reqMu.Unlock()
}

func (c *daemonClient) abortRequest(id uint32) {
	c.reqMu.Lock()
	defer c.reqMu.Unlock()
	dreq, ok := c.requests[id]
	if !ok {
		return
	}
	delete(c.requests, id)
	c.abortedRequests[id] = dreq
}

func (c *daemonClient) buildRequest(typ uint8, target string, length uint64, blk, blklen uint32) *daemonReq {
	id := c.reqSeq.Add(1)
	// header length will be set before sent by writeRequest()
	header := &MsgHdr{
		Type: typ,
		Id:   id,
	}
	info := []*ReqInfo{
		&ReqInfo{
			Target:    target,
			Length:    length,
			BlockNum:  blk,
			Size:      blklen,
			BlockSize: c.blockSize,
		},
	}
	req := &daemonReq{
		req:    &Req{header, info},
		respCh: make(chan any, 1),
	}
	c.addRequest(id, req)
	return req
}

// Get `blk` block of `target`
func (c *daemonClient) GetBlock(ctx context.Context, target string, blk, blklen uint32) (page *Page, rerr error) {
	logger.Debugf("GetBlock %s block %d from daemon", target, blk)

	req := c.buildRequest(REQ_GETBLOCK, target, 0, blk, blklen)
	defer c.delRequest(req.req.Header.Id)

	// send out request
	c.reqCh <- req

	// wait for response
	timer := time.NewTimer(c.reqTimeout)
	select {
	case <-timer.C:
		c.abortRequest(req.req.Header.Id)
		return nil, fmt.Errorf("request %d timed out", req.req.Header.Id)
	case <-ctx.Done():
		c.abortRequest(req.req.Header.Id)
		return nil, ctx.Err()
	case data := <-req.respCh:
		if err, ok := data.(error); ok {
			return nil, err
		}
		resp := data.(*Resp)
		if resp.Header.Status == STS_NOTFOUND {
			return nil, ErrNotFound
		} else if resp.Header.Status == STS_FAIL {
			return nil, errors.New(resp.Info.Message)
		}
		if resp.File == nil {
			logger.Warnf("GetBlock target %s block %d has no cache file fd", target, blk)
			return nil, ErrNotFound
		}

		var err error
		page, err = AllocPage(int(blklen))
		if err != nil {
			resp.File.Close()
			return nil, err
		}
		defer func() {
			resp.File.Close()
			if rerr != nil {
				page.Release()
			}
		}()
		n, err := resp.File.ReadAt(page.Data, 0)
		if err != nil {
			return nil, err
		}
		if n != int(blklen) {
			return nil, fmt.Errorf("data mismatch (%d != %d)", n, blklen)
		}
		return page, nil
	}
}

// Get a reader that reads `blk` block of `target`
func (c *daemonClient) ReadBlock(ctx context.Context, target string, blk, blklen uint32) (ChunkReader, error) {
	logger.Debugf("ReadBlock %s block %d from daemon", target, blk)

	req := c.buildRequest(REQ_GETBLOCK, target, 0, blk, blklen)
	defer c.delRequest(req.req.Header.Id)

	// send out request
	c.reqCh <- req

	// wait for response
	timer := time.NewTimer(c.reqTimeout)
	select {
	case <-timer.C:
		c.abortRequest(req.req.Header.Id)
		return nil, fmt.Errorf("request %d timed out", req.req.Header.Id)
	case <-ctx.Done():
		c.abortRequest(req.req.Header.Id)
		return nil, ctx.Err()
	case data := <-req.respCh:
		if err, ok := data.(error); ok {
			return nil, err
		}
		resp := data.(*Resp)
		if resp.Header.Status == STS_NOTFOUND {
			return nil, ErrNotFound
		} else if resp.Header.Status == STS_FAIL {
			return nil, errors.New(resp.Info.Message)
		}
		if resp.File == nil {
			logger.Warnf("ReadBlock target %s block %d has no cache file fd", target, blk)
			return nil, ErrNotFound
		}
		return newOsFileReader(resp.File), nil
	}
}

// Fill cache of `blk` block of `target`
func (c *daemonClient) FillBlock(ctx context.Context, target string, blk, blklen uint32) error {
	logger.Debugf("FillBlock %s block %d from daemon", target, blk)

	req := c.buildRequest(REQ_FILLBLOCK, target, 0, blk, blklen)
	defer c.delRequest(req.req.Header.Id)

	// send out request
	c.reqCh <- req

	// wait for response
	timer := time.NewTimer(c.reqTimeout)
	select {
	case <-timer.C:
		c.abortRequest(req.req.Header.Id)
		return fmt.Errorf("request %d timed out", req.req.Header.Id)
	case <-ctx.Done():
		c.abortRequest(req.req.Header.Id)
		return ctx.Err()
	case data := <-req.respCh:
		if err, ok := data.(error); ok {
			return err
		}
		resp := data.(*Resp)
		if resp.Header.Status == STS_NOTFOUND {
			return ErrNotFound
		} else if resp.Header.Status == STS_FAIL {
			return errors.New(resp.Info.Message)
		}
		return nil
	}
}

func (c *daemonClient) PutBlock(ctx context.Context, target string, blk, blklen uint32, page *Page) (rerr error) {
	logger.Debugf("PutBlock %s block %d to daemon", target, blk)

	release := false
	commitSize := blklen
	req := c.buildRequest(REQ_PUTBLOCK, target, 0, blk, blklen)
	defer func() {
		c.delRequest(req.req.Header.Id)
		if release {
			// release block after delete previous PutBlock to avoid nested req.
			if err := c.CmtBlock(ctx, target, blk, commitSize); err != nil {
				logger.Warnf("CmtBlock target %s block %d: %s", target, blk, err)
				rerr = err
			}
		}
	}()

	// send out request
	c.reqCh <- req

	// wait for response
	timer := time.NewTimer(c.reqTimeout)
	select {
	case <-timer.C:
		c.abortRequest(req.req.Header.Id)
		return fmt.Errorf("request %d timed out", req.req.Header.Id)
	case <-ctx.Done():
		c.abortRequest(req.req.Header.Id)
		return ctx.Err()
	case data := <-req.respCh:
		if err, ok := data.(error); ok {
			return err
		}
		resp := data.(*Resp)
		status := resp.Header.Status
		if status == STS_OK {
			if resp.File == nil {
				return errors.New("no cache file fd")
			}
			defer resp.File.Close()
			release = true

			// Write data to cache file
			n, err := resp.File.WriteAt(page.Data, 0)
			if err != nil {
				commitSize = 0
				return err
			}
			if n != int(blklen) {
				commitSize = 0
				return fmt.Errorf("write cache file expect %d, got %d", blklen, n)
			}
			return nil
		} else if status == STS_EXISTS {
			return nil
		} else {
			return errors.New(resp.Info.Message)
		}
	}
}

func (c *daemonClient) CmtBlock(ctx context.Context, target string, blk, blklen uint32) error {
	logger.Debugf("CmtBlock %s block %d to daemon", target, blk)
	req := c.buildRequest(REQ_CMTBLOCK, target, 0, blk, blklen)
	defer c.delRequest(req.req.Header.Id)

	// send out request
	c.reqCh <- req

	// wait for response
	timer := time.NewTimer(c.reqTimeout)
	select {
	case <-timer.C:
		c.abortRequest(req.req.Header.Id)
		return fmt.Errorf("request %d timed out", req.req.Header.Id)
	case <-ctx.Done():
		c.abortRequest(req.req.Header.Id)
		return ctx.Err()
	case data := <-req.respCh:
		if err, ok := data.(error); ok {
			return err
		}
		resp := data.(*Resp)
		if resp.Header.Status != STS_OK {
			return errors.New(resp.Info.Message)
		} else {
			return nil
		}
	}
}

func (c *daemonClient) Prepare(ctx context.Context, target string, length uint64) error {
	logger.Debugf("Prepare %s length %d to daemon", target, length)
	req := c.buildRequest(REQ_PREPARE, target, length, 0, 0)
	defer c.delRequest(req.req.Header.Id)

	// send out request
	c.reqCh <- req

	// wait for response
	timer := time.NewTimer(c.reqTimeout)
	select {
	case <-timer.C:
		c.abortRequest(req.req.Header.Id)
		return fmt.Errorf("request %d timed out", req.req.Header.Id)
	case <-ctx.Done():
		c.abortRequest(req.req.Header.Id)
		return ctx.Err()
	case data := <-req.respCh:
		if err, ok := data.(error); ok {
			return err
		}
		resp := data.(*Resp)
		sts := resp.Header.Status
		if sts != STS_OK {
			if sts == STS_EXISTS {
				return ErrExists
			} else {
				return errors.New(resp.Info.Message)
			}
		} else {
			return nil
		}
	}
}

func (c *daemonClient) Commit(ctx context.Context, target string) error {
	logger.Debugf("Commit %s to daemon", target)
	req := c.buildRequest(REQ_COMMIT, target, 0, 0, 0)
	defer c.delRequest(req.req.Header.Id)

	// send out request
	c.reqCh <- req

	// wait for response
	timer := time.NewTimer(c.reqTimeout)
	select {
	case <-timer.C:
		c.abortRequest(req.req.Header.Id)
		return fmt.Errorf("request %d timed out", req.req.Header.Id)
	case <-ctx.Done():
		c.abortRequest(req.req.Header.Id)
		return ctx.Err()
	case data := <-req.respCh:
		if err, ok := data.(error); ok {
			return err
		}
		resp := data.(*Resp)
		sts := resp.Header.Status
		if sts != STS_OK {
			if sts == STS_NOTFOUND {
				return ErrNotFound
			} else {
				return errors.New(resp.Info.Message)
			}
		} else {
			return nil
		}
	}
}

func (c *daemonClient) GetBlockSize(ctx context.Context) (uint32, error) {
	logger.Debug("GetBlockSize from daemon")
	if c.blockSize != 0 {
		return c.blockSize, nil
	}
	req := c.buildRequest(REQ_GETBLKSIZ, "", 0, 0, 0)
	defer c.delRequest(req.req.Header.Id)

	// send out request
	c.reqCh <- req

	// wait for response
	timer := time.NewTimer(c.reqTimeout)
	select {
	case <-timer.C:
		c.abortRequest(req.req.Header.Id)
		return 0, fmt.Errorf("request %d timed out", req.req.Header.Id)
	case <-ctx.Done():
		c.abortRequest(req.req.Header.Id)
		return 0, ctx.Err()
	case data := <-req.respCh:
		if err, ok := data.(error); ok {
			return 0, err
		}
		resp := data.(*Resp)
		if resp.Header.Status != STS_OK {
			return 0, errors.New(resp.Info.Message)
		} else {
			return uint32(resp.Info.Size), nil
		}
	}
}

func (c *daemonClient) Stop() {
	close(c.stopCh)
	c.stopCh = nil
	if c.daemonConn != nil {
		c.daemonConn.Close()
		c.daemonConn = nil
	}
}

// generate UDS connection key to identify a connection to daemon
func getConnKey() ([]byte, error) {
	buf := make([]byte, 0, InitKeySize)
	buffer := bytes.NewBuffer(buf)
	key := uint64(os.Getpid())
	err := binary.Write(buffer, binary.LittleEndian, key)
	return buffer.Bytes(), err
}

func writeRequest(conn unixConn, req *Req) error {
	if conn == nil {
		return errNotConnected
	}
	payload, err := json.Marshal(&req.Info)
	if err != nil {
		return err
	}
	length := len(payload)
	if length > int(math.MaxUint16) {
		return fmt.Errorf("request too long: %d", length)
	}
	req.Header.Length = uint16(length)

	// Allocate buffer for the whole message
	buf := utils.Alloc(HeaderSize + length)
	defer utils.Free(buf)

	// Build message header
	err = req.Header.ToBytes(buf[:HeaderSize])
	if err != nil {
		return fmt.Errorf("build header slice: %s", err)
	}

	// Build message body
	bodyBuf := buf[HeaderSize:]
	_ = append(bodyBuf[:0], payload...)

	// Send request message out
	n, err := conn.Write(buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return fmt.Errorf("writeRequest: partial write, expect %d, written %d", len(buf), n)
	}
	return nil
}

func readResponse(conn unixConn) (*Resp, error) {
	if conn == nil {
		return nil, errNotConnected
	}

	// read header
	header, file, err := readHeader(conn)
	if err != nil {
		return nil, err
	}
	if !header.IsResponse() {
		return nil, fmt.Errorf("read response got no response type: %d (header %v)", header.Type, header)
	}

	// read response body
	length := int(header.Length)
	buf := utils.Alloc(length)
	defer utils.Free(buf)

	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	if n != length {
		return nil, fmt.Errorf("read response expect %d got %d", length, n)
	}

	var info RespInfo
	if err := json.Unmarshal(buf, &info); err != nil {
		return nil, err
	}
	return &Resp{header, &info, file}, nil
}
