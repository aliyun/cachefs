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
	"sync"
	"time"
)

// PendingOpsManager manages pending operations to avoid duplicate work.
// It provides a mechanism to wait for ongoing operations to complete before
// starting duplicate operations.
type PendingOpsManager struct {
	mu      *sync.Mutex
	pending map[string]*Cond
	timeout time.Duration
}

// NewPendingOpsManager creates a new pending operations manager with the given timeout
func NewPendingOpsManager(timeout time.Duration) *PendingOpsManager {
	return &PendingOpsManager{
		mu:      &sync.Mutex{},
		pending: make(map[string]*Cond),
		timeout: timeout,
	}
}

// Add registers a pending operation for the given key.
// It returns true only for the caller that created the operation.
func (p *PendingOpsManager) Add(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.pending[key]; ok {
		return false
	}
	p.pending[key] = NewCond(p.mu)
	return true
}

// Remove unregisters a pending operation and notifies all waiters
func (p *PendingOpsManager) Remove(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cond, ok := p.pending[key]
	if !ok {
		return
	}
	delete(p.pending, key)
	cond.Broadcast()
}

// Wait checks if an operation is pending for the given key.
// If pending, it waits for the operation to complete or times out.
// Returns true if the operation completed successfully, false if not pending or timed out.
func (p *PendingOpsManager) Wait(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	cond, ok := p.pending[key]
	if !ok {
		// not pending
		return false
	}
	start := time.Now()
	if cond.WaitWithTimeout(p.timeout) {
		logger.Infof("Wait for pending operation %s timed out after %v", key, p.timeout)
		return false
	}
	logger.Debugf("Wait for pending operation %s completed in %s", key, time.Since(start))
	return true
}

// Exists checks if an operation is currently pending for the given key
func (p *PendingOpsManager) Exists(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.pending[key]
	return ok
}

// Count returns the number of currently pending operations
func (p *PendingOpsManager) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pending)
}

// Clear removes all pending operations without notifying waiters
// This should only be used during cleanup/shutdown
func (p *PendingOpsManager) Clear() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pending = make(map[string]*Cond)
}
