/*
 * Copyright 2026 Alibaba Cloud, Inc. or its affiliates.
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
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Share-path coded errors. ttrpc carries gRPC status codes across the wire, so
// SDKs can dispatch on codes instead of string-matching.
var (
	// ErrShareWaitTimeout: admission wait exceeded wait_timeout_ms;
	// retryable with backoff.
	ErrShareWaitTimeout = status.Error(codes.DeadlineExceeded,
		"timed out waiting for shared-bytes quota admission")
)

// shareTooLargeErr builds the "shared blocks too large for the share budget"
// error with the actual sizes so the caller can see why it was rejected. Code
// is FailedPrecondition: retrying cannot help until the configuration changes.
func shareTooLargeErr(bytes, budget int64) error {
	return status.Error(codes.FailedPrecondition,
		fmt.Sprintf("shared block bytes %d exceed share cap %d (burst cache size minus share reserve)",
			bytes, budget))
}

// shareBlock represents one selected cache block and its actual byte size. The final file block
// can be shorter than the configured block size. The targets map maintains refcount.
type shareBlock struct {
	index    uint32
	bytes    int64
	refcount int32
}

// shareTarget tracks the currently shared blocks of one target.
type shareTarget struct {
	blocks map[uint32]*shareBlock
}

// shareLease records the exact block references granted to one GetFileChunks request. It is opaque
// to callers other than passing it to release.
type shareLease struct {
	target string
	blocks []shareBlock
}

// shareWaiter is one queued admission request.
type shareWaiter struct {
	target       string
	blocks       []shareBlock
	requestBytes int64
	lease        *shareLease
	ready        chan struct{}
	err          error
	elem         *list.Element
	enqueued     time.Time
}

// shareQuota bounds the total bytes of share-locked blocks below the burst
// cache budget, with strict-FIFO admission waiting for requests that add new
// bytes. The chunk layer owns eviction protection; this vfs-layer quota owns
// byte accounting.
type shareQuota struct {
	mu           sync.Mutex
	targets      map[string]*shareTarget // key: vfs-layer target
	total        int64                   // sum of unique shared block bytes
	cap          int64                   // live shareCap = burstCap - shareReserve
	waiters      *list.List              // FIFO admission wait queue of *shareWaiter
	sharedBlocks int64                   // number of unique shared blocks, maintained incrementally
	waitingBytes int64                   // sum of queued waiters' selected block bytes
	blockSize    int64
}

func newShareQuota(cap, blockSize int64) (*shareQuota, error) {
	if blockSize <= 0 {
		return nil, fmt.Errorf("share quota blockSize must be positive, got %d", blockSize)
	}
	if cap < 0 {
		return nil, fmt.Errorf("share quota cap must be non-negative, got %d", cap)
	}
	return &shareQuota{
		targets:   make(map[string]*shareTarget),
		cap:       cap,
		waiters:   list.New(),
		blockSize: blockSize,
	}, nil
}

func shareBytes(blocks []shareBlock) int64 {
	var bytes int64
	for _, block := range blocks {
		bytes += block.bytes
	}
	return bytes
}

// shareBlockIndexes extracts block indexes from given shareBlock slice.
func shareBlockIndexes(blocks []shareBlock) []uint32 {
	if blocks == nil {
		return nil
	}
	indexes := make([]uint32, len(blocks))
	for index, block := range blocks {
		indexes[index] = block.index
	}
	return indexes
}

func copyShareBlocks(blocks []shareBlock) []shareBlock {
	return append([]shareBlock(nil), blocks...)
}

// newBytes returns the bytes this block set would add to the global quota. Callers must hold q.mu.
func (q *shareQuota) newBytes(target string, blocks []shareBlock) int64 {
	entry := q.targets[target]
	if entry == nil {
		return shareBytes(blocks)
	}

	var bytes int64
	for _, block := range blocks {
		if entry.blocks[block.index] == nil {
			bytes += block.bytes
		}
	}
	return bytes
}

// addLease records one admitted request. New blocks increment the global byte and block totals
// while existing blocks only gain another reference. Callers must hold q.mu.
func (q *shareQuota) addLease(target string, blocks []shareBlock) *shareLease {
	entry := q.targets[target]
	if entry == nil {
		entry = &shareTarget{blocks: make(map[uint32]*shareBlock)}
		q.targets[target] = entry
	}
	for _, block := range blocks {
		if existing := entry.blocks[block.index]; existing != nil {
			existing.refcount++
			continue
		}
		entry.blocks[block.index] = &shareBlock{
			index:    block.index,
			bytes:    block.bytes,
			refcount: 1,
		}
		q.total += block.bytes
		q.sharedBlocks++
	}
	return &shareLease{target: target, blocks: copyShareBlocks(blocks)}
}

// admit waits until selected blocks may be share-locked, counting only blocks that are not already
// held by another request. deadline zero means wait forever. On success the caller must pair the
// returned lease with exactly one release.
// Returns the time spent queued (0 when admitted immediately) so the caller can log/observe the
// admission wait.
func (q *shareQuota) admit(ctx context.Context, target string, blocks []shareBlock, deadline time.Time) (*shareLease, time.Duration, error) {
	q.mu.Lock()
	newBytes := q.newBytes(target, blocks)
	if newBytes > q.cap {
		// Never satisfiable: reject instead of enqueueing — an impossible waiter would
		// wedge the FIFO head forever. Also closes the race where the cap shrinks between
		// the handler's pre-check and this enqueue.
		q.mu.Unlock()
		return nil, 0, shareTooLargeErr(newBytes, q.cap)
	}
	if newBytes == 0 {
		// Existing shared blocks never consume more quota, so this request may join them
		// immediately without waiting behind unrelated new-byte work.
		lease := q.addLease(target, blocks)
		q.mu.Unlock()
		return lease, 0, nil
	}

	// Admit immediately only when no one is queued — strict FIFO: a new request must not jump
	// ahead of waiters already in line, even if it fits. Without this check, a continuous
	// stream of small requests could keep total high enough that a queued large request never
	// gets admitted.
	if q.waiters.Len() == 0 && q.total+newBytes <= q.cap {
		lease := q.addLease(target, blocks)
		q.mu.Unlock()
		return lease, 0, nil
	}

	// No room: enqueue and wait.
	w := &shareWaiter{
		target:       target,
		blocks:       copyShareBlocks(blocks),
		requestBytes: shareBytes(blocks),
		ready:        make(chan struct{}),
		enqueued:     time.Now(),
	}
	w.elem = q.waiters.PushBack(w)
	q.waitingBytes += w.requestBytes
	q.mu.Unlock()

	var timeout <-chan time.Time
	if !deadline.IsZero() {
		t := time.NewTimer(time.Until(deadline))
		defer t.Stop()
		timeout = t.C
	}
	select {
	case <-w.ready:
		// nil = admitted; non-nil = failed by wakeFront (too large)
		return w.lease, time.Since(w.enqueued), w.err
	case <-timeout:
		lease, err := q.abort(w, ErrShareWaitTimeout)
		return lease, time.Since(w.enqueued), err
	case <-ctx.Done():
		lease, err := q.abort(w, ctx.Err())
		return lease, time.Since(w.enqueued), err
	}
}

// abort: a waiter gives up (timeout / cancel). It MUST re-check ready under q.mu: wakeFront may
// have resolved this waiter concurrently with the timer/ctx firing, and the select in admit may
// have picked the timeout/ctx case even though the waiter was admitted. In that window the
// resolved outcome, including the lease, is returned as-is, so the caller can release the
// admitted lease; returning nil instead would leak a permanent budget reservation.
// close(ready) happens under q.mu, so the locked re-check is exact.
func (q *shareQuota) abort(w *shareWaiter, err error) (*shareLease, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	select {
	case <-w.ready: // resolved in the race window: admitted (lease, nil) or failed (nil, err)
		return w.lease, w.err
	default:
	}
	q.waiters.Remove(w.elem)
	q.waitingBytes -= w.requestBytes
	q.wakeFront() // the new front may now be admissible
	return nil, err
}

// release drops every block reference recorded in one share lease; blocks whose last reference is
// removed leave the budget and wake the next admissible waiters.
func (q *shareQuota) release(lease *shareLease) {
	if lease == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	entry := q.targets[lease.target]
	if entry == nil {
		logger.Warnf("Releasing share quota of unlocked target: %s", lease.target)
		return
	}
	for _, block := range lease.blocks {
		existing := entry.blocks[block.index]
		if existing == nil {
			continue
		}
		existing.refcount--
		if existing.refcount > 0 {
			continue
		}
		q.total -= existing.bytes
		q.sharedBlocks--
		delete(entry.blocks, block.index)
	}
	if len(entry.blocks) == 0 {
		delete(q.targets, lease.target)
	}
	q.wakeFront()
}

// setCap updates the live share cap (burstCap - shareReserve, floored at 0)
// and re-judges the queue front. Waiters made impossible by a shrink are
// failed lazily by wakeFront's impossible-front check.
func (q *shareQuota) setCap(cap int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.cap = cap
	q.wakeFront()
}

func (q *shareQuota) getCap() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.cap
}

// wakeFront runs under q.mu. Never leaves an IMPOSSIBLE waiter at the front, each front is
// admitted, failed, or is a possible-but-waiting front (legitimate head-of-line wait, strict FIFO).
func (q *shareQuota) wakeFront() {
	for {
		el := q.waiters.Front()
		if el == nil {
			return
		}
		w := el.Value.(*shareWaiter)
		newBytes := q.newBytes(w.target, w.blocks)
		if newBytes > q.cap {
			// Impossible under the current cap - reachable only via a runtime cap
			// shrink (admit rejects oversized enqueues). Fail it, otherwise it would
			// wedge the FIFO head forever.
			q.resolve(w, nil, shareTooLargeErr(newBytes, q.cap))
			continue
		}
		if newBytes == 0 || q.total+newBytes <= q.cap {
			q.resolve(w, q.addLease(w.target, w.blocks), nil)
			continue
		}
		return // possible but no room yet: legitimate head-of-line wait
	}
}

// resolve runs under q.mu. Single-close discipline — whoever removes the waiter from the list owns
// the outcome: set lease and err first, then close(ready); the close happens-before makes them
// readable lock-free after <-ready. The one exception is abort: the waiter removes itself and
// returns its own error without closing (it is the receiver).
func (q *shareQuota) resolve(w *shareWaiter, lease *shareLease, err error) {
	q.waiters.Remove(w.elem)
	q.waitingBytes -= w.requestBytes
	w.lease = lease
	w.err = err
	close(w.ready)
}
