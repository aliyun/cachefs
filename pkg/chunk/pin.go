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
	"strings"
	"sync"
)

// CachePinner provides cache pinning operations. Within the same hash-hit class,
// pinned entries are evicted after non-pinned entries; hash-hit status still takes
// precedence over pin status, so non-pinned hash-hit blocks outrank pinned
// non-hash-hit blocks during eviction. Pinned entries are also exempt from
// expiration (--cache-expire), but not from RemoveByPrefix.
type CachePinner interface {
	// Add given prefixes to the pin set (incremental).
	PinCache(prefixes []string)

	// Remove given prefixes from the pin set (incremental).
	UnpinCache(prefixes []string)

	// Replace the entire pin set with the given prefixes. Pass nil or empty to clear all.
	SetPinCache(prefixes []string)

	// Check if a target matches any pinned prefix.
	IsPinned(target string) bool

	// Return whether any prefixes are pinned.
	HasPins() bool
}

type pinCache struct {
	mu       sync.RWMutex
	prefixes map[string]struct{}
}

func newPinCache() *pinCache {
	return &pinCache{
		prefixes: make(map[string]struct{}),
	}
}

func (p *pinCache) isPinned(key string) bool {
	if p == nil {
		return false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	for prefix := range p.prefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func (p *pinCache) hasPins() bool {
	if p == nil {
		return false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.prefixes) > 0
}

func (p *pinCache) add(prefixes []string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, prefix := range prefixes {
		p.prefixes[prefix] = struct{}{}
	}
}

func (p *pinCache) remove(prefixes []string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, prefix := range prefixes {
		delete(p.prefixes, prefix)
	}
}

func (p *pinCache) set(prefixes []string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.prefixes = make(map[string]struct{}, len(prefixes))
	for _, prefix := range prefixes {
		p.prefixes[prefix] = struct{}{}
	}
}

// CachePinner implementation on store.
func (s *store) genTargets(targets []string) []string {
	out := make([]string, len(targets))
	for i, t := range targets {
		out[i] = s.genTarget(t)
	}
	return out
}

func (s *store) PinCache(prefixes []string) {
	s.pinCache.add(s.genTargets(prefixes))
}

func (s *store) UnpinCache(prefixes []string) {
	s.pinCache.remove(s.genTargets(prefixes))
}

func (s *store) SetPinCache(prefixes []string) {
	s.pinCache.set(s.genTargets(prefixes))
}

func (s *store) IsPinned(target string) bool {
	return s.pinCache.isPinned(s.genTarget(target))
}

func (s *store) HasPins() bool {
	return s.pinCache.hasPins()
}
