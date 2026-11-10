/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
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

package metric

import (
	"time"

	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type MetricsInitializer interface {
	InitMetrics(reg prometheus.Registerer)
}

var start = time.Now()

func InitRegister(prefix, mp, name string) (prometheus.Registerer, *prometheus.Registry) {
	registry := prometheus.NewRegistry() // replace default so only cachefs metrics are exposed

	registerer := prometheus.WrapRegistererWithPrefix(prefix, registry)
	if mp != "" {
		registerer = prometheus.WrapRegistererWithPrefix(prefix,
			prometheus.WrapRegistererWith(prometheus.Labels{"mp": mp, "vol_name": name}, registry))
	}

	registerer.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registerer.MustRegister(collectors.NewGoCollector())
	registerer.MustRegister(collectors.NewBuildInfoCollector())

	initPackageMetrics(registerer)

	return registerer, registry
}

func initPackageMetrics(reg prometheus.Registerer) {
	if reg == nil {
		return
	}
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cpu_usage",
		Help: "Accumulated CPU usage in seconds.",
	}, func() float64 {
		ru := utils.GetRusage()
		return ru.GetStime() + ru.GetUtime()
	}))
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "memory",
		Help: "Used memory in bytes.",
	}, func() float64 {
		_, rss := utils.MemoryUsage()
		return float64(rss)
	}))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "alloc_mem_size",
			Help: "allocated memory size by internal memory pool, include released but not freed",
		},
		func() float64 {
			return float64(utils.AllocMemory())
		}))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "block_pool_bytes",
			Help: "resident memory of idle warm blocks currently retained by the block pool",
		},
		func() float64 {
			return float64(utils.GetBlockPoolStats().CachedBytes)
		}))
	reg.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "block_pool_reuse",
			Help: "total block allocations served from the block pool",
		},
		func() float64 {
			return float64(utils.GetBlockPoolStats().ReuseCount)
		}))
	reg.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "block_pool_reuse_bytes",
			Help: "total bytes of block allocations served from the block pool",
		},
		func() float64 {
			return float64(utils.GetBlockPoolStats().ReuseBytes)
		}))
	reg.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "block_pool_realloc",
			Help: "total fresh block allocations due to block pool misses or asynchronous block allocation",
		},
		func() float64 {
			return float64(utils.GetBlockPoolStats().ReallocCount)
		}))
	reg.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "block_pool_realloc_bytes",
			Help: "total bytes of fresh block allocations due to block pool misses or asynchronous block allocation",
		},
		func() float64 {
			return float64(utils.GetBlockPoolStats().ReallocBytes)
		}))
	reg.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "block_pool_prefault_blocks",
			Help: "total blocks whose pages were prefaulted",
		},
		func() float64 {
			return float64(utils.GetBlockPoolStats().PrefaultBlocks)
		}))
	reg.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "block_pool_prefault_bytes",
			Help: "total bytes whose pages were prefaulted",
		},
		func() float64 {
			return float64(utils.GetBlockPoolStats().PrefaultBytes)
		}))
	reg.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "block_pool_prefault_fail",
			Help: "total failed block pool prefaults",
		},
		func() float64 {
			return float64(utils.GetBlockPoolStats().PrefaultFail)
		}))
	reg.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "block_pool_prefault_skip_blocks",
			Help: "total refill blocks discarded after allocation but before page population",
		},
		func() float64 {
			return float64(utils.GetBlockPoolStats().PrefaultSkipBlocks)
		}))
	reg.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "block_pool_prefault_skip_bytes",
			Help: "total refill bytes discarded after allocation but before page population",
		},
		func() float64 {
			return float64(utils.GetBlockPoolStats().PrefaultSkipBytes)
		}))
	reg.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "block_pool_prefault_discard_blocks",
			Help: "total prefaulted blocks discarded before entering the block pool",
		},
		func() float64 {
			return float64(utils.GetBlockPoolStats().PrefaultDiscardBlocks)
		}))
	reg.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "block_pool_prefault_discard_bytes",
			Help: "total prefaulted bytes discarded before entering the block pool",
		},
		func() float64 {
			return float64(utils.GetBlockPoolStats().PrefaultDiscardBytes)
		}))
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "uptime",
		Help: "Total running time in seconds.",
	}, func() float64 {
		return time.Since(start).Seconds()
	}))
}
