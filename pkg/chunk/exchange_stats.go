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
	"time"
)

const (
	statsFlushCount    = 50
	statsFlushInterval = 5 * time.Second
)

// exchangeSummary holds aggregated statistics for P2P tracker exchanges.
type exchangeSummary struct {
	count        int
	avgDuration  time.Duration
	avgTargets   float64
	avgBlocks    float64
	avgSendBytes float64
	avgRecvBytes float64
}

// exchangeStats collects individual exchange statistics and periodically
// flushes aggregated summaries to avoid log flooding during mass peer joins.
type exchangeStats struct {
	mu            sync.Mutex
	count         int
	totalDuration time.Duration
	totalTargets  int
	totalBlocks   int
	totalSend     int
	totalRecv     int
	startTime     time.Time

	flushCount    int
	flushInterval time.Duration
	stopCh        chan struct{}
	flushCh       chan struct{}
}

func newExchangeStats(flushCount int, flushInterval time.Duration) *exchangeStats {
	return &exchangeStats{
		startTime:     time.Now(),
		flushCount:    flushCount,
		flushInterval: flushInterval,
		stopCh:        make(chan struct{}),
		flushCh:       make(chan struct{}, 1),
	}
}

// record adds a single exchange event to the statistics.
func (s *exchangeStats) record(duration time.Duration, targets, blocks, sendBytes, recvBytes int) {
	s.mu.Lock()
	s.count++
	s.totalDuration += duration
	s.totalTargets += targets
	s.totalBlocks += blocks
	s.totalSend += sendBytes
	s.totalRecv += recvBytes

	// Check if count threshold is reached, trigger flush
	if s.count >= s.flushCount {
		s.mu.Unlock()
		// Non-blocking send to avoid blocking record
		select {
		case s.flushCh <- struct{}{}:
		default:
		}
		return
	}
	s.mu.Unlock()
}

// maybeFlush checks if flush conditions are met and returns the aggregated summary.
// Flush is triggered when either count threshold or time interval is reached.
func (s *exchangeStats) maybeFlush() (bool, time.Duration, *exchangeSummary) {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := s.count
	if count == 0 {
		return false, 0, nil
	}

	elapsed := time.Since(s.startTime)
	shouldFlush := count >= s.flushCount || elapsed >= s.flushInterval

	if !shouldFlush {
		return false, 0, nil
	}

	avgCount := float64(count)
	summary := &exchangeSummary{
		count:        count,
		avgDuration:  s.totalDuration / time.Duration(count),
		avgTargets:   float64(s.totalTargets) / avgCount,
		avgBlocks:    float64(s.totalBlocks) / avgCount,
		avgSendBytes: float64(s.totalSend) / avgCount,
		avgRecvBytes: float64(s.totalRecv) / avgCount,
	}

	// Reset counters
	s.count = 0
	s.totalDuration = 0
	s.totalTargets = 0
	s.totalBlocks = 0
	s.totalSend = 0
	s.totalRecv = 0
	s.startTime = time.Now()

	return true, elapsed, summary
}

// startPrinter periodically checks and prints exchange statistics summary.
// It runs as a background goroutine and stops when stopCh is closed.
// The flushFunc callback receives elapsed time and summary pointer, allowing custom output formatting.
func (s *exchangeStats) startPrinter(flushFunc func(time.Duration, *exchangeSummary)) {
	go func() {
		ticker := time.NewTicker(s.flushInterval)
		defer ticker.Stop()

		// doFlush checks and flushes if needed
		doFlush := func() {
			if flushed, elapsed, summary := s.maybeFlush(); flushed {
				flushFunc(elapsed, summary)
			}
		}

		for {
			select {
			case <-ticker.C:
				// Timer triggered
			case <-s.flushCh:
				// Count threshold triggered
			case <-s.stopCh:
				// Shutdown signal, do final flush
				doFlush()
				return
			}
			doFlush()
		}
	}()
}

// stop signals the printer goroutine to stop and performs a final flush.
func (s *exchangeStats) stop() {
	close(s.stopCh)
}
