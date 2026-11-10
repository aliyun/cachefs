/*
 * Copyright 2025 Alibaba Cloud, Inc. or its affiliates.
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
	"testing"
)

func TestNewHashRing(t *testing.T) {
	hashRing := NewHashRing(10)
	if hashRing.NumberOfReplicas != 10 {
		t.Errorf("Expected 10 replicas, got %d", hashRing.NumberOfReplicas)
	}
	if hashRing.CustomHasher == nil {
		t.Error("Expect CustomHasher hash instead of crc32")
	}

	// Invalid replicas should not be set
	hashRing = NewHashRing(0)
	if hashRing.NumberOfReplicas <= 0 {
		t.Errorf("Unexpected replicas: %d", hashRing.NumberOfReplicas)
	}
	hashRing = NewHashRing(-1)
	if hashRing.NumberOfReplicas <= 0 {
		t.Errorf("Unexpected replicas: %d", hashRing.NumberOfReplicas)
	}
}

func TestAddToHashring(t *testing.T) {
	hashRing := NewHashRing(10)

	if !hashRing.AddToHashring("node1") {
		t.Error("Failed to add element 'node1' that did not exist in hashring")
	}

	if hashRing.AddToHashring("node1") {
		t.Error("Added element 'node1' that already exists in hashring")
	}
}

func TestRemoveFromHashring(t *testing.T) {
	hashRing := NewHashRing(10)

	hashRing.AddToHashring("node1")

	if !hashRing.RemoveFromHashring("node1") {
		t.Error("Failed to remove element 'node1' that exists in hashring")
	}

	if hashRing.RemoveFromHashring("node1") {
		t.Error("Removed element 'node1' that does not exist in hashring")
	}
}
