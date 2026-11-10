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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"net"
	"os"
	"sync"
	"time"

	"github.com/aliyun/cachefs/pkg/rpcsvc"
	"github.com/aliyun/cachefs/pkg/serf"
	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sys/unix"
)

var errNotConnected = errors.New("not connected to daemon")

type Daemon interface {
	Serve() error
	Stop()
}

type daemon struct {
	store  *store
	stopCh chan struct{}

	listener    *net.UnixListener
	connections map[uint64]unixConn
	connMu      sync.Mutex

	cacheHits      prometheus.Counter
	cacheMiss      prometheus.Counter
	cacheHitBytes  prometheus.Counter
	cacheMissBytes prometheus.Counter
}

type daemonReq struct {
	req    *Req
	respCh chan any
}

type daemonResp struct {
	resp  *Resp
	errCh chan error
}

func NewDaemon(config *StoreConfig, rpc rpcsvc.RpcService, serf serf.SerfService, reg prometheus.Registerer) Daemon {
	if config.IsMemoryBackend() {
		// Allocate cache memory via memfd so we could share cache via memfd with daemon
		// client to avoid extra memory copy.
		PageMemfd = true
	}
	store := doNewStore(config, rpc, nil, serf, reg)
	daemon := &daemon{
		store:       store,
		stopCh:      make(chan struct{}),
		connections: make(map[uint64]unixConn),
	}
	daemon.InitMetrics(reg)
	return daemon
}

type unixConn interface {
	net.Conn
	ReadMsgUnix(b, oob []byte) (n, oobn int, flags int, addr *net.UnixAddr, err error)
	WriteMsgUnix(b, oob []byte, addr *net.UnixAddr) (n, oobn int, err error)
}

// read message header from given connection
func readHeader(conn unixConn) (*MsgHdr, *os.File, error) {
	hdrBuf := utils.Alloc(HeaderSize)
	defer utils.Free(hdrBuf)
	oob := utils.Alloc(unix.CmsgSpace(4))
	defer utils.Free(oob)

	var file *os.File
	n, oobn, _, _, err := conn.ReadMsgUnix(hdrBuf, oob)
	if err != nil {
		return nil, nil, err
	}
	if n != HeaderSize {
		return nil, nil, fmt.Errorf("read header expect %d got %d", HeaderSize, n)
	}

	scms, err := unix.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		logger.Warnf("read header parse control message: %s", err)
		return nil, nil, err
	}
	if len(scms) > 0 {
		fds, err := unix.ParseUnixRights(&scms[0])
		if err != nil {
			logger.Warnf("read header parse unix rights: %s", err)
			return nil, nil, err
		}
		fd := fds[0]
		file = os.NewFile(uintptr(fd), "cachefile")
	}

	header := ParseMessageHeader(hdrBuf)
	if !header.IsValid() {
		if file != nil {
			file.Close()
		}
		logger.Warnf("readHeader: got invalid header: type %d, status %d, length %d, id %d",
			header.Type, header.Status, header.Length, header.Id)
		return nil, nil, fmt.Errorf("Invalid header: %v", *header)
	}
	return header, file, nil
}

// read request message from given connection
func readRequest(conn unixConn) (*Req, error) {
	// read header, request messages have no fd
	header, _, err := readHeader(conn)
	if err != nil {
		return nil, err
	}
	if !header.IsRequest() {
		return nil, fmt.Errorf("read request got no request type: %d (header %v)", header.Type, header)
	}

	// read request body
	length := int(header.Length)
	buf := utils.Alloc(length)
	defer utils.Free(buf)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	if n != length {
		return nil, fmt.Errorf("read request expect %d got %d", length, n)
	}
	var info []*ReqInfo
	if err := json.Unmarshal(buf, &info); err != nil {
		return nil, err
	}
	return &Req{header, info}, nil
}

func writeResponse(conn unixConn, resp *Resp) error {
	payload, err := json.Marshal(&resp.Info)
	if err != nil {
		return err
	}
	length := len(payload)
	if length > int(math.MaxUint16) {
		return fmt.Errorf("response too long: %d", length)
	}
	resp.Header.Length = uint16(length)

	// Allocate buffer for the whole message
	buf := utils.Alloc(HeaderSize + length)
	defer utils.Free(buf)

	// Build message header
	err = resp.Header.ToBytes(buf[:HeaderSize])
	if err != nil {
		return fmt.Errorf("build header slice: %s", err)
	}

	// Build message body
	bodyBuf := buf[HeaderSize:]
	_ = append(bodyBuf[:0], payload...)

	// Send response message out
	var oob []byte
	if resp.File != nil {
		oob = unix.UnixRights(int(resp.File.Fd()))
	}
	n, oobn, err := conn.WriteMsgUnix(buf, oob, nil)
	if err != nil {
		return err
	}
	if n != len(buf) || oobn != len(oob) {
		return fmt.Errorf("partial write, expect %d, written %d, oob expect %d, written %d",
			len(buf), n, len(oob), oobn)
	}
	return nil
}

func (d *daemon) InitMetrics(reg prometheus.Registerer) {
	d.cacheHits = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "daemon_hits",
		Help: "cache hits from daemon requests",
	})
	d.cacheMiss = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "daemon_miss",
		Help: "cache miss from daemon requests",
	})
	d.cacheHitBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "daemon_hit_bytes",
		Help: "requested bytes from daemon requests",
	})
	d.cacheMissBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "daemon_miss_bytes",
		Help: "missed bytes from daemon requests",
	})
	if reg == nil {
		return
	}

	reg.MustRegister(d.cacheHits)
	reg.MustRegister(d.cacheHitBytes)
	reg.MustRegister(d.cacheMiss)
	reg.MustRegister(d.cacheMissBytes)
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "client_count",
			Help: "number of clients",
		},
		func() float64 {
			d.connMu.Lock()
			defer d.connMu.Unlock()
			return float64(len(d.connections))
		}))
}

func (d *daemon) getConnection(key uint64) (unixConn, bool) {
	d.connMu.Lock()
	defer d.connMu.Unlock()
	conn, ok := d.connections[key]
	return conn, ok
}

func (d *daemon) delConnection(key uint64) {
	d.connMu.Lock()
	conn, ok := d.connections[key]
	if !ok {
		d.connMu.Unlock()
		return
	}
	delete(d.connections, key)
	d.connMu.Unlock()
	conn.Close()
	logger.Infof("Deleted connection from pid %d", key)
}

func (d *daemon) handleRequest(key uint64, req *Req, respCh chan *daemonResp) {
	var info *RespInfo
	var file *os.File
	var status uint8

	_, ok := d.getConnection(key)
	if !ok {
		logger.Warnf("Handle request cannot find connection from pid %d", key)
		return
	}

	header := MsgHdr{}
	header.Id = req.Header.Id
	switch req.Header.Type {
	case REQ_GETBLOCK:
		header.Type = RESP_GETBLOCK
		info, file, status = d.GetBlock(req)
	case REQ_FILLBLOCK:
		header.Type = RESP_FILLBLOCK
		info, status = d.FillBlock(req)
	case REQ_PUTBLOCK:
		header.Type = RESP_PUTBLOCK
		info, file, status = d.PutBlock(req)
	case REQ_CMTBLOCK:
		header.Type = RESP_CMTBLOCK
		info, status = d.CmtBlock(req)
	case REQ_PREPARE:
		header.Type = RESP_PREPARE
		info, status = d.Prepare(req)
	case REQ_COMMIT:
		header.Type = RESP_COMMIT
		info, status = d.Commit(req)
	case REQ_GETBLKSIZ:
		header.Type = RESP_GETBLKSIZ
		info, status = d.GetBlockSize()
	default:
		panic(fmt.Errorf("Invalid request type %d", req.Header.Type))
	}
	header.Status = status

	defer func() {
		if file != nil {
			err := file.Close()
			if err != nil {
				logger.Warnf("Close sent file: %s", err)
			}
		}
	}()

	errCh := make(chan error)
	respw := &daemonResp{
		resp:  &Resp{&header, info, file},
		errCh: errCh,
	}

	// write out response and check write error
	respCh <- respw
	err := <-errCh
	if err != nil {
		logger.Warnf("Write response to connection from pid %d failed: %s", key, err)
		d.delConnection(key)
	}
}

func (d *daemon) handleNewConn(conn unixConn) {
	// first get connection key (pid) from client to identify which cachefs fuse it's from
	err := conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err != nil {
		logger.Warnf("Set connection read deadline: %s", err)
		conn.Close()
		return
	}
	msg := make([]byte, InitKeySize)
	n, err := conn.Read(msg)
	if err != nil {
		logger.Warnf("Read initial message from new connection: %s", err)
		conn.Close()
		return
	}
	if n != 8 {
		logger.Warnf("Read initial message expect 8 bytes got %d bytes, drop connection", n)
		conn.Close()
		return
	}
	key := binary.LittleEndian.Uint64(msg)
	d.connMu.Lock()
	if _, ok := d.connections[key]; ok {
		logger.Warnf("Connection from pid %d already exists", key)
		conn.Close()
		d.connMu.Unlock()
		return
	} else {
		d.connections[key] = conn
	}
	d.connMu.Unlock()
	logger.Infof("Connection from pid %d initlaized", key)

	// set no read deadline, read will not time out
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		logger.Warnf("Set connection read deadline to zero: %s", err)
		conn.Close()
		return
	}

	// start a goroutine to receive and write response to connection
	respCh := make(chan *daemonResp, 64)
	go func() {
		for {
			select {
			case <-d.stopCh:
				return
			case resp := <-respCh:
				resp.errCh <- writeResponse(conn, resp.resp)
			}
		}
	}()

	// read & handle requests from cachefs fuse
	for {
		select {
		case <-d.stopCh:
			// Serve() will close all connections, just stop here.
			return
		default:
			req, err := readRequest(conn)
			if err == nil {
				go d.handleRequest(key, req, respCh)
			} else {
				logger.Infof("Read request from connection %d failed: %s", key, err)
				d.delConnection(key)
				return
			}
		}
	}
}

func (d *daemon) Serve() error {
	if err := os.Remove(d.store.config.Socket); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	addr, err := net.ResolveUnixAddr("unix", d.store.config.Socket)
	if err != nil {
		return err
	}
	listener, err := net.ListenUnix("unix", addr)
	if err != nil {
		return err
	}
	d.listener = listener
	logger.Infof("Daemon cache listening on %s for new clients", d.store.config.Socket)

	go func() {
		for {
			conn, err := listener.AcceptUnix()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					logger.Info("Stop accepting new connections.")
					return
				} else {
					logger.Warnf("Accept connection: %s", err)
				}
			} else {
				go d.handleNewConn(conn)
			}
		}
	}()
	return nil
}

func (d *daemon) Stop() {
	if d.stopCh != nil {
		close(d.stopCh)
		d.stopCh = nil
	}

	// stop accepting new connections
	d.listener.Close()
	d.listener = nil

	// close all existing connections
	d.connMu.Lock()
	for _, conn := range d.connections {
		conn.Close()
	}
	d.connections = nil
	d.connMu.Unlock()

	d.store.Stop()
	if err := os.Remove(d.store.config.Socket); err != nil && !errors.Is(err, fs.ErrNotExist) {
		logger.Warn(err)
	}
}

func GetCacheConnKey(target string, blk uint32) string {
	return fmt.Sprintf("%s:%d", target, blk)
}
