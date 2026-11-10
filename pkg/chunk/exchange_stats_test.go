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

package chunk

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExchangeStatsRecord(t *testing.T) {
	stats := newExchangeStats(10, time.Second)

	// Record some exchanges
	stats.record(100*time.Millisecond, 5, 10, 1000, 2000)
	stats.record(200*time.Millisecond, 3, 6, 1500, 2500)
	stats.record(150*time.Millisecond, 4, 8, 1200, 2200)

	stats.mu.Lock()
	assert.Equal(t, 3, stats.count)
	assert.Equal(t, 450*time.Millisecond, stats.totalDuration)
	assert.Equal(t, 12, stats.totalTargets)
	assert.Equal(t, 24, stats.totalBlocks)
	assert.Equal(t, 3700, stats.totalSend)
	assert.Equal(t, 6700, stats.totalRecv)
	stats.mu.Unlock()
}

func TestExchangeStatsMaybeFlushByCount(t *testing.T) {
	stats := newExchangeStats(3, time.Hour) // Long interval, trigger by count

	// Record 3 exchanges (should trigger flush)
	stats.record(100*time.Millisecond, 5, 10, 1000, 2000)
	stats.record(200*time.Millisecond, 3, 6, 1500, 2500)
	stats.record(150*time.Millisecond, 4, 8, 1200, 2200)

	flushed, elapsed, summary := stats.maybeFlush()
	assert.True(t, flushed)
	assert.True(t, elapsed > 0)
	assert.NotNil(t, summary)
	assert.Equal(t, 3, summary.count)
	assert.Equal(t, 150*time.Millisecond, summary.avgDuration)
	assert.InDelta(t, 4.0, summary.avgTargets, 0.01)
	assert.InDelta(t, 8.0, summary.avgBlocks, 0.01)
	assert.InDelta(t, 1233.33, summary.avgSendBytes, 0.01)
	assert.InDelta(t, 2233.33, summary.avgRecvBytes, 0.01)

	// After flush, counters should be reset
	assert.Equal(t, 0, stats.count)
	assert.Equal(t, time.Duration(0), stats.totalDuration)
}

func TestExchangeStatsMaybeFlushByTime(t *testing.T) {
	stats := newExchangeStats(100, 100*time.Millisecond) // Short interval, trigger by time

	// Record 2 exchanges (not enough for count trigger)
	stats.record(100*time.Millisecond, 5, 10, 1000, 2000)
	stats.record(200*time.Millisecond, 3, 6, 1500, 2500)

	// Should not flush yet
	flushed, _, _ := stats.maybeFlush()
	assert.False(t, flushed)

	// Wait for time interval
	time.Sleep(110 * time.Millisecond)

	// Should flush now
	flushed, elapsed, summary := stats.maybeFlush()
	assert.True(t, flushed)
	assert.True(t, elapsed >= 100*time.Millisecond)
	assert.NotNil(t, summary)
	assert.Equal(t, 2, summary.count)
	assert.Equal(t, 150*time.Millisecond, summary.avgDuration)
	assert.InDelta(t, 4.0, summary.avgTargets, 0.01)
	assert.InDelta(t, 8.0, summary.avgBlocks, 0.01)
}

func TestExchangeStatsMaybeFlushNoData(t *testing.T) {
	stats := newExchangeStats(3, time.Second)

	// No records, should not flush
	flushed, _, summary := stats.maybeFlush()
	assert.False(t, flushed)
	assert.Nil(t, summary)
}

func TestExchangeStatsConcurrentRecord(t *testing.T) {
	stats := newExchangeStats(1000, time.Hour)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stats.record(100*time.Millisecond, 1, 2, 100, 200)
		}()
	}
	wg.Wait()

	stats.mu.Lock()
	assert.Equal(t, 100, stats.count)
	assert.Equal(t, 100*time.Millisecond*100, stats.totalDuration)
	assert.Equal(t, 100, stats.totalTargets)
	assert.Equal(t, 200, stats.totalBlocks)
	stats.mu.Unlock()
}

func TestExchangeStatsResetAfterFlush(t *testing.T) {
	stats := newExchangeStats(2, time.Hour)

	// First batch
	stats.record(100*time.Millisecond, 5, 10, 1000, 2000)
	stats.record(200*time.Millisecond, 3, 6, 1500, 2500)

	flushed, _, summary1 := stats.maybeFlush()
	assert.True(t, flushed)
	assert.NotNil(t, summary1)
	assert.Equal(t, 2, summary1.count)

	// Second batch
	stats.record(300*time.Millisecond, 7, 14, 2000, 3000)
	stats.record(400*time.Millisecond, 9, 18, 2500, 3500)

	flushed, _, summary2 := stats.maybeFlush()
	assert.True(t, flushed)
	assert.NotNil(t, summary2)
	assert.Equal(t, 2, summary2.count)
	assert.Equal(t, 350*time.Millisecond, summary2.avgDuration)
	assert.InDelta(t, 8.0, summary2.avgTargets, 0.01)
}

func TestExchangeSummaryValues(t *testing.T) {
	stats := newExchangeStats(2, time.Hour)

	stats.record(100*time.Millisecond, 10, 20, 1000, 2000)
	stats.record(300*time.Millisecond, 20, 40, 3000, 6000)

	_, _, summary := stats.maybeFlush()

	assert.NotNil(t, summary)
	assert.Equal(t, 2, summary.count)
	assert.Equal(t, 200*time.Millisecond, summary.avgDuration)
	assert.Equal(t, 15.0, summary.avgTargets)
	assert.Equal(t, 30.0, summary.avgBlocks)
	assert.Equal(t, 2000.0, summary.avgSendBytes)
	assert.Equal(t, 4000.0, summary.avgRecvBytes)
}

func TestExchangeStatsCountTriggerImmediateFlush(t *testing.T) {
	// Set count threshold to 3, but very long time interval
	stats := newExchangeStats(3, time.Hour)

	// Record 2 exchanges (not enough to trigger)
	stats.record(100*time.Millisecond, 5, 10, 1000, 2000)
	stats.record(200*time.Millisecond, 3, 6, 1500, 2500)

	// Check flushCh should be empty
	select {
	case <-stats.flushCh:
		t.Fatal("flushCh should be empty before reaching threshold")
	default:
		// Expected
	}

	// Record 3rd exchange (should trigger flush via flushCh)
	stats.record(150*time.Millisecond, 4, 8, 1200, 2200)

	// Check flushCh should have signal
	select {
	case <-stats.flushCh:
		// Expected - count threshold reached
	default:
		t.Fatal("flushCh should have signal after reaching count threshold")
	}

	// Verify data is correct
	flushed, _, summary := stats.maybeFlush()
	assert.True(t, flushed)
	assert.NotNil(t, summary)
	assert.Equal(t, 3, summary.count)
	assert.Equal(t, 150*time.Millisecond, summary.avgDuration)
}
