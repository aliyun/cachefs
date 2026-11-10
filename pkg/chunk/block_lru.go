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

import "container/list"

// blockLRU tracks a bounded set of keys from least to most recent.
type blockLRU struct {
	limit   int
	order   *list.List
	entries map[CacheKey]*list.Element
}

// newBlockLRU creates a bounded LRU; non-positive limits disable protection.
func newBlockLRU(limit int) *blockLRU {
	return &blockLRU{
		limit:   max(0, limit),
		order:   list.New(),
		entries: make(map[CacheKey]*list.Element),
	}
}

// touch inserts key or moves it to the back, removing the oldest key when over the limit.
func (l *blockLRU) touch(key CacheKey) {
	if l == nil || l.limit == 0 {
		return
	}
	if e, ok := l.entries[key]; ok {
		l.order.MoveToBack(e)
		return
	}
	l.entries[key] = l.order.PushBack(key)
	if l.order.Len() > l.limit {
		front := l.order.Front()
		delete(l.entries, front.Value.(CacheKey))
		l.order.Remove(front)
	}
}

// remove deletes key from the protection window if present.
func (l *blockLRU) remove(key CacheKey) {
	if l == nil {
		return
	}
	if e, ok := l.entries[key]; ok {
		delete(l.entries, key)
		l.order.Remove(e)
	}
}

// contains reports whether key is in the protection window.
func (l *blockLRU) contains(key CacheKey) bool {
	if l == nil {
		return false
	}
	_, ok := l.entries[key]
	return ok
}

// len returns the number of keys in the protection window.
func (l *blockLRU) len() int {
	if l == nil {
		return 0
	}
	return l.order.Len()
}

// forEach visits keys oldest to newest and stops when fn returns false.
// fn may remove the current key.
func (l *blockLRU) forEach(fn func(key CacheKey) bool) {
	if l == nil {
		return
	}
	for e := l.order.Front(); e != nil; {
		next := e.Next()
		if !fn(e.Value.(CacheKey)) {
			return
		}
		e = next
	}
}
