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

package srcmgr

import (
	"fmt"
	"os"
	"syscall"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestIsInfraError(t *testing.T) {
	assert.False(t, IsInfraError(nil))
	assert.False(t, IsInfraError(os.ErrNotExist))
	assert.False(t, IsInfraError(syscall.EPERM))
	assert.False(t, IsInfraError(syscall.EACCES))
	assert.False(t, IsInfraError(syscall.EINVAL))
	assert.False(t, IsInfraError(syscall.ETIMEDOUT))
	assert.False(t, IsInfraError(fmt.Errorf("wrap: %w", syscall.ENOENT)))

	for _, eno := range []syscall.Errno{syscall.EIO, syscall.ESTALE, syscall.ENOTCONN, syscall.EUCLEAN, syscall.EREMOTEIO} {
		assert.True(t, IsInfraError(eno), eno)
		assert.True(t, IsInfraError(&os.PathError{Op: "open", Path: "/x", Err: eno}), eno)
		assert.True(t, IsInfraError(fmt.Errorf("ctx: %w", eno)), eno)
	}
}

func TestKill(t *testing.T) {
	t.Run("advances and exhausts", func(t *testing.T) {
		m := NewSourceManager([]string{"/a", "/b", "/c"}, nil)
		assert.Equal(t, "/a", m.CurrentAliveDir())

		assert.Equal(t, 1, m.kill(0, syscall.EIO, "test", "/"))
		assert.Equal(t, "/b", m.CurrentAliveDir())

		assert.Equal(t, 2, m.kill(1, syscall.ESTALE, "test", "/"))
		assert.Equal(t, "/c", m.CurrentAliveDir())

		// Killing the last replica leaves no candidate.
		assert.Equal(t, -1, m.kill(2, syscall.EREMOTEIO, "test", "/"))
		// The last replica keeps serving even when failed.
		assert.Equal(t, "/c", m.CurrentAliveDir())
	})

	t.Run("concurrent loser still gets next", func(t *testing.T) {
		m := NewSourceManager([]string{"/a", "/b"}, nil)
		assert.Equal(t, 1, m.kill(0, syscall.EIO, "test", "/"))
		// A second kill of the same replica (CAS loser) must still report the
		// correct next candidate instead of failing the operation.
		assert.Equal(t, 1, m.kill(0, syscall.EIO, "test", "/"))
	})
}

func TestOutageCounterOncePerReplica(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewSourceManager([]string{"/a", "/b"}, reg)
	m.kill(0, syscall.EIO, "test", "/")
	m.kill(0, syscall.ESTALE, "test", "/") // duplicate kill must not double-count
	m.kill(1, syscall.EIO, "test", "/")
	assert.Equal(t, float64(1), testutil.ToFloat64(m.outageTotal.WithLabelValues("/a")))
	assert.Equal(t, float64(1), testutil.ToFloat64(m.outageTotal.WithLabelValues("/b")))
}

func TestDirs(t *testing.T) {
	m := NewSourceManager([]string{"/a", "/b"}, nil)
	assert.Equal(t, []string{"/a", "/b"}, m.Dirs())
	assert.Empty(t, NewSourceManager(nil, nil).Dirs())
}

func TestLen(t *testing.T) {
	assert.Equal(t, 2, NewSourceManager([]string{"/a", "/b"}, nil).Len())
	assert.Equal(t, 0, NewSourceManager(nil, nil).Len())
}

func TestCurrentAliveDir(t *testing.T) {
	t.Run("nil manager returns empty", func(t *testing.T) {
		var m *Manager
		assert.Equal(t, "", m.CurrentAliveDir())
	})

	t.Run("no replicas returns empty", func(t *testing.T) {
		assert.Equal(t, "", NewSourceManager(nil, nil).CurrentAliveDir())
	})

	t.Run("no failure returns first replica", func(t *testing.T) {
		assert.Equal(t, "/a", NewSourceManager([]string{"/a", "/b"}, nil).CurrentAliveDir())
	})

	t.Run("skips failed replicas", func(t *testing.T) {
		m := NewSourceManager([]string{"/a", "/b", "/c"}, nil)
		m.kill(0, syscall.EIO, "test", "/")
		m.kill(1, syscall.EIO, "test", "/")
		// A subsequent operation skips all failed replicas and starts at c.
		assert.Equal(t, "/c", m.CurrentAliveDir())
	})

	t.Run("all failed returns last replica", func(t *testing.T) {
		m := NewSourceManager([]string{"/a", "/b"}, nil)
		m.kill(0, syscall.EIO, "test", "/")
		m.kill(1, syscall.EIO, "test", "/")
		assert.Equal(t, "/b", m.CurrentAliveDir())
	})
}

func TestActiveDirGauge(t *testing.T) {
	active := func(reg *prometheus.Registry) map[string]float64 {
		mfs, err := reg.Gather()
		assert.NoError(t, err)
		out := map[string]float64{}
		for _, mf := range mfs {
			if mf.GetName() != "source_replica_active_source" {
				continue
			}
			for _, gm := range mf.GetMetric() {
				for _, l := range gm.GetLabel() {
					if l.GetName() == "source" {
						out[l.GetValue()] = gm.GetGauge().GetValue()
					}
				}
			}
		}
		return out
	}

	t.Run("tracks current alive through failures", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		m := NewSourceManager([]string{"/a", "/b", "/c"}, reg)
		assert.Equal(t, map[string]float64{"/a": 1, "/b": 0, "/c": 0}, active(reg))
		m.kill(0, syscall.EIO, "test", "/")
		assert.Equal(t, map[string]float64{"/a": 0, "/b": 1, "/c": 0}, active(reg))
		m.kill(1, syscall.EIO, "test", "/")
		assert.Equal(t, map[string]float64{"/a": 0, "/b": 0, "/c": 1}, active(reg))
		m.kill(2, syscall.EIO, "test", "/")
		assert.Equal(t, map[string]float64{"/a": 0, "/b": 0, "/c": 1}, active(reg),
			"the last replica keeps serving when all have failed")
	})

	t.Run("duplicate dirs are dropped at construction", func(t *testing.T) {
		// Replica dirs are replica identity, so duplicates would make one dir
		// address two slots; the constructor keeps the first occurrence.
		reg := prometheus.NewRegistry()
		m := NewSourceManager([]string{"/a", "/a"}, reg)
		assert.Equal(t, []string{"/a"}, m.Dirs())
		assert.Equal(t, 1, m.Len())
		assert.Equal(t, map[string]float64{"/a": 1}, active(reg))
	})
}

func TestRunOp(t *testing.T) {
	t.Run("infra error fails over to next replica", func(t *testing.T) {
		m := NewSourceManager([]string{"/a", "/b"}, nil)
		var calls []string
		err := m.RunOp("walk", "/p", func(dir string) error {
			calls = append(calls, dir)
			if dir == "/a" {
				return syscall.EIO
			}
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, []string{"/a", "/b"}, calls)
	})

	t.Run("semantic error returns as-is without failover", func(t *testing.T) {
		m := NewSourceManager([]string{"/a", "/b"}, nil)
		var calls []string
		err := m.RunOp("walk", "/p", func(dir string) error {
			calls = append(calls, dir)
			return syscall.ENOENT
		})
		assert.ErrorIs(t, err, syscall.ENOENT)
		assert.Equal(t, []string{"/a"}, calls)
	})

	t.Run("exhaustion returns last error", func(t *testing.T) {
		m := NewSourceManager([]string{"/a", "/b"}, nil)
		var calls []string
		err := m.RunOp("walk", "/p", func(dir string) error {
			calls = append(calls, dir)
			return syscall.ESTALE
		})
		assert.ErrorIs(t, err, syscall.ESTALE)
		assert.Equal(t, []string{"/a", "/b"}, calls, "every replica must be tried before giving up")
	})

	t.Run("new op starts at current alive replica", func(t *testing.T) {
		m := NewSourceManager([]string{"/a", "/b", "/c"}, nil)
		m.kill(0, syscall.EIO, "test", "/")
		var calls []string
		err := m.RunOp("walk", "/p", func(dir string) error {
			calls = append(calls, dir)
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, []string{"/b"}, calls)
	})

	t.Run("nil manager degenerates to single attempt", func(t *testing.T) {
		var m *Manager
		var calls []string
		err := m.RunOp("walk", "/p", func(dir string) error {
			calls = append(calls, dir)
			return syscall.EIO
		})
		assert.ErrorIs(t, err, syscall.EIO)
		assert.Equal(t, []string{""}, calls)
	})

	t.Run("empty manager degenerates to single attempt", func(t *testing.T) {
		m := NewSourceManager(nil, nil)
		var calls []string
		err := m.RunOp("walk", "/p", func(dir string) error {
			calls = append(calls, dir)
			return syscall.EIO
		})
		assert.ErrorIs(t, err, syscall.EIO)
		assert.Equal(t, []string{""}, calls)
	})
}
