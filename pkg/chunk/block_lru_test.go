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
	"testing"

	"github.com/stretchr/testify/assert"
)

func lruTestKeys(l *blockLRU) []CacheKey {
	keys := make([]CacheKey, 0, l.len())
	l.forEach(func(key CacheKey) bool {
		keys = append(keys, key)
		return true
	})
	return keys
}

func TestBlockLRU(t *testing.T) {
	keys := []CacheKey{{"target", 0}, {"target", 1}, {"target", 2}}
	lru := newBlockLRU(2)
	assert.Zero(t, lru.len())

	lru.touch(keys[0])
	lru.touch(keys[1])
	assert.Equal(t, 2, lru.len())
	assert.Equal(t, []CacheKey{keys[0], keys[1]}, lruTestKeys(lru))
	assert.True(t, lru.contains(keys[0]))
	assert.True(t, lru.contains(keys[1]))

	lru.touch(keys[0])
	assert.Equal(t, []CacheKey{keys[1], keys[0]}, lruTestKeys(lru))
	assert.Len(t, lru.entries, 2)

	lru.touch(keys[2])
	assert.Equal(t, 2, lru.len())
	assert.Equal(t, []CacheKey{keys[0], keys[2]}, lruTestKeys(lru))
	assert.False(t, lru.contains(keys[1]))
	assert.Len(t, lru.entries, 2)

	lru.remove(keys[0])
	assert.Equal(t, 1, lru.len())
	assert.Equal(t, []CacheKey{keys[2]}, lruTestKeys(lru))
	assert.False(t, lru.contains(keys[0]))
	lru.remove(keys[1])
	assert.Equal(t, []CacheKey{keys[2]}, lruTestKeys(lru))
}

func TestBlockLRUForEach(t *testing.T) {
	keys := []CacheKey{{"target", 0}, {"target", 1}, {"target", 2}}
	lru := newBlockLRU(3)
	for _, key := range keys {
		lru.touch(key)
	}

	var visited []CacheKey
	lru.forEach(func(key CacheKey) bool {
		visited = append(visited, key)
		return len(visited) < 2
	})
	assert.Equal(t, keys[:2], visited)

	visited = nil
	lru.forEach(func(key CacheKey) bool {
		visited = append(visited, key)
		lru.remove(key)
		return true
	})
	assert.Equal(t, keys, visited)
	assert.Zero(t, lru.len())
	assert.Empty(t, lru.entries)
}

func TestBlockLRUDisabled(t *testing.T) {
	for _, limit := range []int{0, -1} {
		lru := newBlockLRU(limit)
		key := CacheKey{"target", 0}
		lru.touch(key)
		assert.Zero(t, lru.limit)
		assert.Zero(t, lru.len())
		assert.Empty(t, lruTestKeys(lru))
		assert.Empty(t, lru.entries)
		assert.False(t, lru.contains(key))
		lru.remove(key)
	}
}

func TestBlockLRUNilReceiver(t *testing.T) {
	var lru *blockLRU
	key := CacheKey{"target", 0}
	lru.touch(key)
	lru.remove(key)
	assert.False(t, lru.contains(key))
	assert.Zero(t, lru.len())
	called := false
	lru.forEach(func(CacheKey) bool {
		called = true
		return true
	})
	assert.False(t, called)
}
