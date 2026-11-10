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

// Counter names registered in the store-layer CounterRegistry.
// Both registration (filebackend/membackend/store) and lookup (vfs/preload)
// must use these constants so a typo becomes a compile error.
const (
	CounterRemoteHits          = "remote_hits"
	CounterRemoteMiss          = "remote_miss"
	CounterRemoteHitBytes      = "remote_hit_bytes"
	CounterRemoteMissBytes     = "remote_miss_bytes"
	CounterEvictsHashHit       = "blockcache_evicts_hashhit"
	CounterEvictsCleanup       = "blockcache_evicts_cleanup"
	CounterEvictsPinned        = "blockcache_evicts_pinned"
	CounterEvictsPinnedHashHit = "blockcache_evicts_pinned_hashhit"
)
