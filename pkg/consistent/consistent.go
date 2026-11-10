/*
 * Copyright 2024 Alibaba Cloud, Inc. or its affiliates.
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

package consistent

import (
	"github.com/cespare/xxhash/v2"
	"github.com/eryugey/consistent"
)

type xxhash32 struct{}

// Implements consistent.Hasher interface
func (xx *xxhash32) Hash(key string) uint32 {
	h64 := xxhash.Sum64String(key)
	return uint32(h64 ^ (h64 >> 32))
}

type ConsistentHashRing struct {
	*consistent.Consistent
}

func NewHashRing(virtualNodeCount int) *ConsistentHashRing {
	hashRing := consistent.New()
	hashRing.CustomHasher = &xxhash32{}
	if virtualNodeCount > 0 {
		hashRing.NumberOfReplicas = virtualNodeCount
	}
	return &ConsistentHashRing{hashRing}
}

// The native `consistent` library has deficiencies.
// It doesn't validate whether a member already exists in the hashring when performing Add and Remove operations.
// Instead, it directly executes these operations, ultimately altering the `c.count` value, which introduces potential risks.
// Therefore, we have wrapped an additional layer to check for the existence of a member beforehand.
func (chr *ConsistentHashRing) AddToHashring(elt string) bool {
	members := chr.Members()
	if elementExists(members, elt) {
		return false
	}
	chr.Add(elt)
	return true
}

func (chr *ConsistentHashRing) RemoveFromHashring(elt string) bool {
	members := chr.Members()
	if !elementExists(members, elt) {
		return false
	}
	chr.Remove(elt)
	return true
}

func elementExists(members []string, elt string) bool {
	for _, member := range members {
		if member == elt {
			return true
		}
	}
	return false
}
