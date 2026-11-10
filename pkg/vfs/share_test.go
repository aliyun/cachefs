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
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const shareTestBlockSize = 4 << 20

func shareTestBlocks(values ...int64) []shareBlock {
	blocks := make([]shareBlock, 0, len(values))
	for index, bytes := range values {
		blocks = append(blocks, shareBlock{index: uint32(index), bytes: bytes})
	}
	return blocks
}

func admitShare(t *testing.T, q *shareQuota, target string, blocks []shareBlock) *shareLease {
	t.Helper()
	lease, waited, err := q.admit(context.Background(), target, blocks, time.Time{})
	require.NoError(t, err)
	assert.Zero(t, waited)
	return lease
}

func TestShareBytes(t *testing.T) {
	tests := []struct {
		name   string
		blocks []shareBlock
		want   int64
	}{
		{"nil", nil, 0},
		{"empty", []shareBlock{}, 0},
		{"selected blocks", []shareBlock{{index: 3, bytes: 4}, {index: 7, bytes: 9}, {index: 8, bytes: 2}}, 15},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, shareBytes(tt.blocks))
		})
	}
}

func TestShareBlockIndexes(t *testing.T) {
	tests := []struct {
		name   string
		blocks []shareBlock
		want   []uint32
	}{
		{"nil", nil, nil},
		{"empty", []shareBlock{}, []uint32{}},
		{"selected blocks", []shareBlock{{index: 3, bytes: 4}, {index: 7, bytes: 9}}, []uint32{3, 7}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, shareBlockIndexes(tt.blocks))
		})
	}
}

func TestCopyShareBlocks(t *testing.T) {
	assert.Nil(t, copyShareBlocks(nil))

	blocks := []shareBlock{{index: 2, bytes: 4}, {index: 3, bytes: 8}}
	copy := copyShareBlocks(blocks)
	require.Equal(t, blocks, copy)

	blocks[0].bytes = 16
	assert.Equal(t, int64(4), copy[0].bytes)
}

func TestShareQuotaImmediateAdmit(t *testing.T) {
	q, err := newShareQuota(100, shareTestBlockSize)
	require.NoError(t, err)

	first := admitShare(t, q, "t1", shareTestBlocks(60))
	assert.Equal(t, int64(60), q.total)

	// The same selected block admits free (refcount only, no extra charge).
	second := admitShare(t, q, "t1", shareTestBlocks(60))
	assert.Equal(t, int64(60), q.total)
	assert.Equal(t, int32(2), q.targets["t1"].blocks[0].refcount)

	// First release only drops the block reference.
	q.release(first)
	assert.Equal(t, int64(60), q.total)
	// Last release credits the block.
	q.release(second)
	assert.Equal(t, int64(0), q.total)
	assert.Empty(t, q.targets)
}

func TestShareQuotaRangeOverlapAndLastRelease(t *testing.T) {
	q, err := newShareQuota(200, shareTestBlockSize)
	require.NoError(t, err)

	first := admitShare(t, q, "t", shareTestBlocks(40, 60))
	second := admitShare(t, q, "t", []shareBlock{
		{index: 1, bytes: 60},
		{index: 2, bytes: 20},
	})
	assert.Equal(t, int64(120), q.total)
	assert.Equal(t, int64(3), q.sharedBlocks)
	assert.Equal(t, int32(2), q.targets["t"].blocks[1].refcount)

	q.release(first)
	assert.Equal(t, int64(80), q.total)
	assert.Equal(t, int64(2), q.sharedBlocks)
	assert.Equal(t, int32(1), q.targets["t"].blocks[1].refcount)

	q.release(second)
	assert.Equal(t, int64(0), q.total)
	assert.Equal(t, int64(0), q.sharedBlocks)
	assert.Empty(t, q.targets)
}

func TestShareQuotaTooLarge(t *testing.T) {
	q, err := newShareQuota(100, shareTestBlockSize)
	require.NoError(t, err)
	_, _, err = q.admit(context.Background(), "big", shareTestBlocks(101), time.Time{})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Empty(t, q.targets)
	assert.Equal(t, 0, q.waiters.Len())
}

func TestShareQuotaFIFOWait(t *testing.T) {
	q, err := newShareQuota(100, shareTestBlockSize)
	require.NoError(t, err)
	first := admitShare(t, q, "t1", shareTestBlocks(80))

	done := make(chan error, 1)
	go func() {
		_, waited, err := q.admit(context.Background(), "t2", shareTestBlocks(50), time.Time{})
		if err == nil && waited <= 0 {
			t.Error("expected positive wait duration")
		}
		done <- err
	}()

	// t2 must be queued.
	assert.Eventually(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.waiters.Len() == 1 && q.waitingBytes == 50
	}, time.Second, time.Millisecond)

	// Releasing t1 admits t2.
	q.release(first)
	require.NoError(t, <-done)
	q.mu.Lock()
	assert.Equal(t, int64(50), q.total)
	assert.Equal(t, int64(0), q.waitingBytes)
	q.mu.Unlock()
}

func TestShareQuotaZeroNewBytesBypassWaiter(t *testing.T) {
	q, err := newShareQuota(100, shareTestBlockSize)
	require.NoError(t, err)
	first := admitShare(t, q, "t1", shareTestBlocks(100))

	queued := make(chan error, 1)
	go func() {
		_, _, err := q.admit(context.Background(), "t2", shareTestBlocks(50), time.Time{})
		queued <- err
	}()
	assert.Eventually(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.waiters.Len() == 1
	}, time.Second, time.Millisecond)

	// An overlapping request adds no quota bytes, so it does not wait behind t2.
	second, waited, err := q.admit(context.Background(), "t1", shareTestBlocks(100), time.Time{})
	require.NoError(t, err)
	assert.Zero(t, waited)
	assert.Equal(t, int32(2), q.targets["t1"].blocks[0].refcount)

	q.release(first)
	select {
	case err := <-queued:
		t.Fatalf("queued request admitted while shared block still had a reference: %v", err)
	default:
	}
	q.release(second)
	require.NoError(t, <-queued)
}

func TestShareQuotaWaitTimeout(t *testing.T) {
	q, err := newShareQuota(100, shareTestBlockSize)
	require.NoError(t, err)
	admitShare(t, q, "t1", shareTestBlocks(100))

	start := time.Now()
	_, _, err = q.admit(context.Background(), "t2", shareTestBlocks(10), time.Now().Add(50*time.Millisecond))
	assert.ErrorIs(t, err, ErrShareWaitTimeout)
	assert.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
	// The timed-out waiter must have removed itself.
	assert.Equal(t, 0, q.waiters.Len())
	assert.Equal(t, int64(0), q.waitingBytes)
}

func TestShareQuotaCtxCancel(t *testing.T) {
	q, err := newShareQuota(100, shareTestBlockSize)
	require.NoError(t, err)
	admitShare(t, q, "t1", shareTestBlocks(100))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := q.admit(ctx, "t2", shareTestBlocks(10), time.Time{})
		done <- err
	}()
	assert.Eventually(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.waiters.Len() == 1
	}, time.Second, time.Millisecond)
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
	assert.Equal(t, 0, q.waiters.Len())
}

// TestShareQuotaAbortAfterResolve reproduces the race where the admit select
// picks the timeout/ctx case after wakeFront already resolved the waiter: abort
// must return the admitted lease, not nil, or the budget bytes leak forever.
// The race window is constructed directly (resolve, then abort) since winning
// the real select race is not reproducible.
func TestShareQuotaAbortAfterResolve(t *testing.T) {
	q, err := newShareQuota(100, shareTestBlockSize)
	require.NoError(t, err)
	first := admitShare(t, q, "t1", shareTestBlocks(100))

	w := &shareWaiter{
		target:       "t2",
		blocks:       copyShareBlocks(shareTestBlocks(10)),
		requestBytes: 10,
		ready:        make(chan struct{}),
	}
	q.mu.Lock()
	w.elem = q.waiters.PushBack(w)
	q.waitingBytes += w.requestBytes
	q.mu.Unlock()

	// first's release frees room and wakeFront admits w, exactly as in the race.
	q.release(first)
	select {
	case <-w.ready:
	default:
		t.Error("waiter was not resolved by wakeFront")
		return
	}

	lease, err := q.abort(w, ErrShareWaitTimeout)
	require.NoError(t, err, "the waiter was admitted, so abort must not fail it")
	require.NotNil(t, lease, "abort must hand back the admitted lease")

	// The caller releases it as usual and the budget returns to zero.
	q.release(lease)
	assert.Equal(t, int64(0), q.total)
	assert.Equal(t, 0, len(q.targets))
	assert.Equal(t, 0, q.waiters.Len())
	assert.Equal(t, int64(0), q.waitingBytes)
}

func TestShareQuotaCapShrinkFailsImpossibleFront(t *testing.T) {
	q, err := newShareQuota(100, shareTestBlockSize)
	require.NoError(t, err)
	admitShare(t, q, "t1", shareTestBlocks(90))

	done := make(chan error, 1)
	go func() {
		_, _, err := q.admit(context.Background(), "t2", shareTestBlocks(50), time.Time{})
		done <- err
	}()
	assert.Eventually(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.waiters.Len() == 1
	}, time.Second, time.Millisecond)

	// Shrink below the waiter's new block bytes: wakeFront must fail it, not wedge it.
	q.setCap(40)
	assert.Equal(t, codes.FailedPrecondition, status.Code(<-done))
	assert.Equal(t, 0, q.waiters.Len())
}

func TestShareQuotaSharedBlocksGauge(t *testing.T) {
	q, err := newShareQuota(100*shareTestBlockSize, shareTestBlockSize)
	require.NoError(t, err)
	lease := admitShare(t, q, "t1", []shareBlock{
		{index: 0, bytes: shareTestBlockSize},
		{index: 1, bytes: 1},
	})
	assert.Equal(t, int64(2), q.sharedBlocks)
	q.release(lease)
	assert.Equal(t, int64(0), q.sharedBlocks)
}

func TestShareQuotaFIFONoJumping(t *testing.T) {
	q, err := newShareQuota(100, shareTestBlockSize)
	require.NoError(t, err)
	first := admitShare(t, q, "X", shareTestBlocks(90))

	// A (large) cannot fit and enters the queue.
	doneA := make(chan error, 1)
	go func() {
		_, _, err := q.admit(context.Background(), "A", shareTestBlocks(90), time.Time{})
		doneA <- err
	}()
	assert.Eventually(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.waiters.Len() == 1
	}, time.Second, time.Millisecond)

	// A small request fits current capacity but must not jump ahead of A.
	doneSmall := make(chan error, 1)
	go func() {
		_, _, err := q.admit(context.Background(), "small", shareTestBlocks(10), time.Time{})
		doneSmall <- err
	}()
	assert.Eventually(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.waiters.Len() == 2
	}, time.Second, time.Millisecond)

	// Release X: wakeFront admits A first, then small.
	q.release(first)
	require.NoError(t, <-doneA)
	require.NoError(t, <-doneSmall)
	q.mu.Lock()
	assert.Equal(t, int64(100), q.total)
	q.mu.Unlock()
}

func TestShareCap(t *testing.T) {
	tests := []struct {
		burstCap, reserve, want int64
	}{
		{100, 30, 70},
		{100, 0, 100},   // no reserve: full burst budget
		{100, 100, 0},   // reserve equals burst: zero budget (clamped)
		{100, 150, 0},   // reserve exceeds burst: floored at 0
		{0, 0, 0},       // no burst: zero budget
		{100, -50, 100}, // negative reserve: treated as 0, never above burst
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, shareCap(tt.burstCap, tt.reserve),
			"burstCap=%d reserve=%d", tt.burstCap, tt.reserve)
	}
}

func TestReleaseShare(t *testing.T) {
	tests := []struct {
		name      string
		target    string
		allBlocks bool
		blocks    []shareBlock
		source    string
		want      []uint32
		wantRPC   float64
		wantEOF   float64
		wantExp   float64
	}{
		{"whole target", "t1", true, []shareBlock{{index: 0, bytes: 50}}, shareReleaseRpc, nil, 1, 0, 0},
		{"selected range", "t2", false, []shareBlock{{index: 2, bytes: 20}}, shareReleaseEof, []uint32{2}, 0, 1, 0},
		{"explicit full-file range", "t3", false, []shareBlock{{index: 0, bytes: 10}, {index: 1, bytes: 10}}, shareReleaseExpired, []uint32{0, 1}, 0, 0, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			quota, err := newShareQuota(100, shareTestBlockSize)
			require.NoError(t, err)
			cache := &mockCacheManager{}
			a := &apiServer{
				fs:                   &Cachefs{cache: cache},
				quota:                quota,
				shareHoldHist:        prometheus.NewHistogram(prometheus.HistogramOpts{}),
				shareReleaseRpcCount: prometheus.NewCounter(prometheus.CounterOpts{}),
				shareReleaseEofCount: prometheus.NewCounter(prometheus.CounterOpts{}),
				shareReleaseExpCount: prometheus.NewCounter(prometheus.CounterOpts{}),
			}

			lease := admitShare(t, quota, tt.target, tt.blocks)
			a.releaseShare(&fdTransferInfo{
				target:     tt.target,
				allBlocks:  tt.allBlocks,
				shareLease: lease,
				shareStart: time.Now(),
			}, tt.source)

			assert.Equal(t, tt.target, cache.unlockedTarget)
			assert.Equal(t, tt.want, cache.unlockedBlocks)
			assert.Empty(t, quota.targets)
			assert.Equal(t, int64(0), quota.total)
			assert.Equal(t, tt.wantRPC, testutil.ToFloat64(a.shareReleaseRpcCount))
			assert.Equal(t, tt.wantEOF, testutil.ToFloat64(a.shareReleaseEofCount))
			assert.Equal(t, tt.wantExp, testutil.ToFloat64(a.shareReleaseExpCount))
		})
	}
}

func TestNewShareQuotaValidation(t *testing.T) {
	tests := []struct {
		name      string
		cap       int64
		blockSize int64
		wantErr   bool
	}{
		{"valid", 100, shareTestBlockSize, false},
		{"zero cap allowed (degenerate)", 0, shareTestBlockSize, false},
		{"negative cap", -1, shareTestBlockSize, true},
		{"zero blockSize", 100, 0, true},
		{"negative blockSize", 100, -1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := newShareQuota(tt.cap, tt.blockSize)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, q)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, q)
				assert.Equal(t, tt.cap, q.cap)
				assert.Equal(t, tt.blockSize, q.blockSize)
			}
		})
	}
}

func TestShareTooLargeErr(t *testing.T) {
	err := shareTooLargeErr(12345678, 9876543)
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "12345678")
	assert.Contains(t, err.Error(), "9876543")
}
