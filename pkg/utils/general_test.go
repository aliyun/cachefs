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

package utils

import (
	"testing"
	"time"
)

func TestSleepWithJitter(t *testing.T) {
	d := 100 * time.Millisecond
	before := time.Now()
	SleepWithJitter(d)
	elapsed := time.Since(before)

	// Allow ±5% jitter plus 20ms scheduling slack.
	lo := d - d/20 - 20*time.Millisecond
	hi := d + d/20 + 20*time.Millisecond
	if elapsed < lo || elapsed > hi {
		t.Errorf("SleepWithJitter(%v) slept %v, want [%v, %v]", d, elapsed, lo, hi)
	}
}

func TestSleepWithJitterOrStop(t *testing.T) {
	t.Run("completes normally when stopCh is not closed", func(t *testing.T) {
		d := 100 * time.Millisecond
		stopCh := make(chan struct{})
		before := time.Now()
		stopped := SleepWithJitterOrStop(d, stopCh)
		elapsed := time.Since(before)

		if stopped {
			t.Error("expected stopped=false, got true")
		}
		lo := d - d/20 - 20*time.Millisecond
		hi := d + d/20 + 20*time.Millisecond
		if elapsed < lo || elapsed > hi {
			t.Errorf("elapsed %v, want [%v, %v]", elapsed, lo, hi)
		}
	})

	t.Run("returns immediately when stopCh is already closed", func(t *testing.T) {
		stopCh := make(chan struct{})
		close(stopCh)

		before := time.Now()
		stopped := SleepWithJitterOrStop(10*time.Second, stopCh)
		elapsed := time.Since(before)

		if !stopped {
			t.Error("expected stopped=true, got false")
		}
		if elapsed > 50*time.Millisecond {
			t.Errorf("expected near-instant return, got %v", elapsed)
		}
	})

	t.Run("wakes up early when stopCh is closed mid-sleep", func(t *testing.T) {
		stopCh := make(chan struct{})
		delay := 50 * time.Millisecond
		go func() {
			time.Sleep(delay)
			close(stopCh)
		}()

		before := time.Now()
		stopped := SleepWithJitterOrStop(10*time.Second, stopCh)
		elapsed := time.Since(before)

		if !stopped {
			t.Error("expected stopped=true, got false")
		}
		// Should wake within delay + 50ms scheduling slack, well before the 10s timer.
		hi := delay + 50*time.Millisecond
		if elapsed > hi {
			t.Errorf("expected early wakeup within %v, got %v", hi, elapsed)
		}
	})
}
