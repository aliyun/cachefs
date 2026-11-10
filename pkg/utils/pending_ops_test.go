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
	"fmt"
	"testing"
	"time"
)

func TestPendingOpsManager_AddAndWait(t *testing.T) {
	mgr := NewPendingOpsManager(500 * time.Millisecond)
	key := "test-target/1"

	// Initially no pending operation
	if mgr.Wait(key) {
		t.Error("Expected no pending operation, but Wait returned true")
		return
	}

	// Add pending operation
	mgr.Add(key)
	if !mgr.Exists(key) {
		t.Error("Expected pending operation to exist after Add")
		return
	}

	// Remove pending operation
	mgr.Remove(key)
	if mgr.Exists(key) {
		t.Error("Expected pending operation to not exist after Remove")
		return
	}
}

func TestPendingOpsManager_WaitTimeout(t *testing.T) {
	mgr := NewPendingOpsManager(200 * time.Millisecond)
	key := "test-target/2"

	// Add pending operation but never remove it
	mgr.Add(key)

	start := time.Now()
	result := mgr.Wait(key)
	duration := time.Since(start)

	// Should timeout and return false
	if result {
		t.Error("Expected timeout with result=false, but got result=true")
		return
	}

	// Should take approximately 200ms (timeout duration)
	if duration < 150*time.Millisecond || duration > 300*time.Millisecond {
		t.Errorf("Expected timeout around 200ms, got %v", duration)
		return
	}

	// Clean up
	mgr.Clear()
}

func TestPendingOpsManager_WaitSignaled(t *testing.T) {
	mgr := NewPendingOpsManager(500 * time.Millisecond)
	key := "test-target/3"

	// Add pending operation
	mgr.Add(key)

	// Signal after 100ms
	go func() {
		time.Sleep(100 * time.Millisecond)
		mgr.Remove(key)
	}()

	start := time.Now()
	result := mgr.Wait(key)
	duration := time.Since(start)

	// Should be signaled and return true
	if !result {
		t.Error("Expected to be signaled with result=true, but got result=false")
		return
	}

	// Should take approximately 100ms (signal time)
	if duration < 50*time.Millisecond || duration > 200*time.Millisecond {
		t.Errorf("Expected signal around 100ms, got %v", duration)
		return
	}
}

func TestPendingOpsManager_ConcurrentWaiters(t *testing.T) {
	mgr := NewPendingOpsManager(500 * time.Millisecond)
	key := "test-target/4"

	// Add pending operation
	mgr.Add(key)

	// Start multiple concurrent waiters
	numWaiters := 5
	results := make(chan bool, numWaiters)
	startTime := time.Now()

	for i := 0; i < numWaiters; i++ {
		go func() {
			result := mgr.Wait(key)
			results <- result
		}()
	}

	// Remove pending operation after 150ms
	go func() {
		time.Sleep(150 * time.Millisecond)
		mgr.Remove(key)
	}()

	// Collect results
	for i := 0; i < numWaiters; i++ {
		select {
		case result := <-results:
			// All should be signaled and return true
			if !result {
				t.Errorf("Waiter %d: Expected to be signaled with result=true, but got result=false", i)
				return
			}
		case <-time.After(500 * time.Millisecond):
			t.Errorf("Waiter %d: timed out waiting for result", i)
			return
		}
	}

	duration := time.Since(startTime)
	// All waiters should complete around the same time (150ms)
	if duration < 100*time.Millisecond || duration > 250*time.Millisecond {
		t.Errorf("Expected all waiters to complete around 150ms, got %v", duration)
		return
	}
}

func TestPendingOpsManager_MultiplePendingOps(t *testing.T) {
	mgr := NewPendingOpsManager(500 * time.Millisecond)

	keys := []string{
		"target1/1",
		"target2/2",
		"target3/3",
	}

	// Add all pending operations
	for _, key := range keys {
		mgr.Add(key)
	}

	// Verify count
	if mgr.Count() != len(keys) {
		t.Errorf("Expected %d pending operations, got %d", len(keys), mgr.Count())
		return
	}

	// Remove one by one and verify count
	for i, key := range keys {
		mgr.Remove(key)
		expectedCount := len(keys) - i - 1
		if mgr.Count() != expectedCount {
			t.Errorf("After removing %d operations, expected %d pending operations, got %d", i+1, expectedCount, mgr.Count())
			return
		}
	}
}

func TestPendingOpsManager_ConcurrentAddKeepsSingleOperation(t *testing.T) {
	mgr := NewPendingOpsManager(time.Second)
	key := "test-target/duplicate"
	const callers = 16

	start := make(chan struct{})
	results := make(chan bool, callers)
	for range callers {
		go func() {
			<-start
			results <- mgr.Add(key)
		}()
	}
	close(start)

	owners := 0
	for range callers {
		if <-results {
			owners++
		}
	}
	if owners != 1 {
		t.Errorf("Expected one pending-operation owner, got %d", owners)
		return
	}
	mgr.mu.Lock()
	originalSignal := mgr.pending[key].signal
	mgr.mu.Unlock()

	mgr.Remove(key)
	select {
	case <-originalSignal:
	case <-time.After(time.Second):
		t.Error("Expected Remove to wake waiters on the original pending operation")
		return
	}
}

func TestPendingOpsManager_RemoveNonExistent(t *testing.T) {
	mgr := NewPendingOpsManager(500 * time.Millisecond)
	key := "non-existent/999"

	// Remove non-existent key should not panic
	mgr.Remove(key)

	if mgr.Count() != 0 {
		t.Errorf("Expected 0 pending operations after removing non-existent key, got %d", mgr.Count())
		return
	}
}

func TestPendingOpsManager_Clear(t *testing.T) {
	mgr := NewPendingOpsManager(500 * time.Millisecond)

	keys := []string{
		"target1/1",
		"target2/2",
	}

	for _, key := range keys {
		mgr.Add(key)
	}

	if mgr.Count() != len(keys) {
		t.Errorf("Expected %d pending operations before clear, got %d", len(keys), mgr.Count())
		return
	}

	mgr.Clear()

	if mgr.Count() != 0 {
		t.Errorf("Expected 0 pending operations after clear, got %d", mgr.Count())
		return
	}
}

func TestPendingOpsManager_StringKey(t *testing.T) {
	tests := []struct {
		target string
		blk    uint32
		key    string
	}{
		{"test", 1, "test/1"},
		{"/path/to/file", 123, "/path/to/file/123"},
		{"", 0, "/0"},
	}

	for _, tc := range tests {
		key := fmt.Sprintf("%s/%d", tc.target, tc.blk)
		if key != tc.key {
			t.Errorf("Key format: expected %q, got %q", tc.key, key)
			return
		}
	}
}
