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

// Package common holds parts of the chunk layer's public vocabulary that clients
// need to share with it. It carries no dependencies, so client-side packages can
// speak the same protocol without pulling in the coro_rpc cgo deps of pkg/chunk.
package common

const (
	// CacheSizeKeepCurrent is a SetCacheSize sentinel value that keeps the current
	// cache size or burst cache size unchanged.
	CacheSizeKeepCurrent = -1

	// BurstCacheSizeRestore is a sentinel burstCacheSize value for SetCacheSize:
	// restore the burst cache size configured at startup. The effective value is
	// still normalized to be >= cache size.
	BurstCacheSizeRestore = -2
)
