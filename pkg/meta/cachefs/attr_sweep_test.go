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

package cachefsmeta

import (
	"sync"
	"testing"
	"time"

	"github.com/aliyun/cachefs/pkg/meta"
	"github.com/stretchr/testify/assert"
)

func TestAttrSweepInterval(t *testing.T) {
	m := &metaData{config: &MetaConfig{}}
	t.Setenv("CFS_META_SWEEP_INTERVAL", "")
	for _, tt := range []struct {
		timeout int
		want    time.Duration
	}{
		{timeout: 0, want: 5 * time.Second},
		{timeout: 10, want: 5 * time.Second},
		{timeout: 20, want: 10 * time.Second},
	} {
		m.config.ReaddirTimeout = tt.timeout
		assert.Equal(t, tt.want, m.attrSweepInterval())
	}

	m.config.ReaddirTimeout = 20
	t.Setenv("CFS_META_SWEEP_INTERVAL", "1")
	assert.Equal(t, time.Second, m.attrSweepInterval())
	for _, value := range []string{"invalid", "0", "-1", "1s"} {
		t.Setenv("CFS_META_SWEEP_INTERVAL", value)
		assert.Equal(t, 10*time.Second, m.attrSweepInterval())
	}
}

func TestAttrSweepEnabled(t *testing.T) {
	for _, tt := range []struct {
		name   string
		config MetaConfig
		want   bool
	}{
		{name: "source", config: MetaConfig{AttrCleanup: true, ReaddirTimeout: 0}, want: true},
		{name: "cleanup disabled", config: MetaConfig{ReaddirTimeout: 0}},
		{name: "negative timeout", config: MetaConfig{AttrCleanup: true, ReaddirTimeout: -1}},
		{name: "sourceless", config: MetaConfig{AttrCleanup: true, Sourceless: true}},
		{name: "peer meta", config: MetaConfig{AttrCleanup: true, PeerMetaTimeout: 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m := &metaData{config: &tt.config}
			assert.Equal(t, tt.want, m.attrSweepEnabled())
		})
	}
}

func TestStartAttrSweeper(t *testing.T) {
	t.Setenv("CFS_META_SWEEP_INTERVAL", "1")
	stopCh := make(chan struct{})
	m := &metaData{
		config:      &MetaConfig{AttrCleanup: true},
		inodeShards: newInodeShards(),
		stopCh:      stopCh,
	}
	t.Cleanup(func() { close(stopCh) })
	cino := NewInode(1, nil, "file", meta.TypeFile, &meta.Attr{Typ: meta.TypeFile})
	cino.attrCache.Load().loadedAt = time.Now().Add(-time.Second)
	putTestInode(m, cino)
	m.cachedAttrs.Add(1)

	m.startAttrSweeper()
	assert.Eventually(t, func() bool {
		return cino.GetAttr() == nil && m.cachedAttrs.Load() == 0
	}, 3*time.Second, 10*time.Millisecond)
}

func TestSweepAttrsLoop(t *testing.T) {
	m := &metaData{config: &MetaConfig{}}
	stopCh := make(chan struct{})
	close(stopCh)
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.sweepAttrs(stopCh)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Error("sweepAttrs did not stop")
		return
	}
}

func TestSweepAttrs(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		m := &metaData{
			config:      &MetaConfig{AttrCleanup: true, ReaddirTimeout: 10},
			inodeShards: newInodeShards(),
		}
		now := time.Now()
		expired := NewInode(1, nil, "expired", meta.TypeFile, &meta.Attr{Typ: meta.TypeFile})
		expired.attrCache.Load().loadedAt = now.Add(-11 * time.Second)
		fresh := NewInode(2, nil, "fresh", meta.TypeFile, &meta.Attr{Typ: meta.TypeFile})
		fresh.attrCache.Load().loadedAt = now
		empty := NewInode(3, nil, "empty", meta.TypeFile, nil)
		for _, cino := range []*Inode{expired, fresh, empty} {
			putTestInode(m, cino)
		}
		m.cachedAttrs.Store(2)
		assert.Equal(t, int64(2), m.cachedAttrs.Load())

		assert.Equal(t, 1, m.doSweepAttrs())
		assert.Nil(t, expired.GetAttr())
		assert.NotNil(t, fresh.GetAttr())
		assert.Nil(t, empty.GetAttr())
		assert.Equal(t, int64(1), m.cachedAttrs.Load())
		assert.Equal(t, uint64(3), m.inodeCount())
		assert.Zero(t, m.doSweepAttrs())

		fresh.attrCache.Load().loadedAt = now
		m.config.ReaddirTimeout = -1
		assert.Zero(t, m.doSweepAttrs())
		assert.NotNil(t, fresh.GetAttr())
	})

	t.Run("keep new version", func(t *testing.T) {
		m := &metaData{}
		cino := NewInode(1, nil, "file", meta.TypeFile, &meta.Attr{Typ: meta.TypeFile})
		m.cachedAttrs.Add(1)
		old := cino.attrCache.Load()
		m.storeAttr(cino, &meta.Attr{Typ: meta.TypeFile, Length: 100})

		assert.False(t, cino.attrCache.CompareAndSwap(old, nil))
		assert.Equal(t, uint64(100), cino.GetAttr().Length)
		assert.Equal(t, int64(1), m.cachedAttrs.Load())
	})
}

func TestAttrStoreAndSweepConcurrent(t *testing.T) {
	m := &metaData{
		config:      &MetaConfig{AttrCleanup: true, ReaddirTimeout: 0},
		inodeShards: newInodeShards(),
	}
	cino := NewInode(1, nil, "file", meta.TypeFile, &meta.Attr{Typ: meta.TypeFile})
	putTestInode(m, cino)
	m.cachedAttrs.Add(1)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				m.storeAttr(cino, &meta.Attr{Typ: meta.TypeFile})
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				m.doSweepAttrs()
			}
		}()
	}
	wg.Wait()
	m.storeAttr(cino, &meta.Attr{Typ: meta.TypeFile})
	assert.NotNil(t, cino.GetAttr())
	assert.Equal(t, int64(1), m.cachedAttrs.Load())
}
