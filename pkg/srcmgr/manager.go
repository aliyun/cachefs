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

// Package srcmgr tracks the health of the priority-ordered source dir
// replicas of a mount. A replica is permanently marked failed on the first
// infrastructure error it returns (no recovery, no probing); both the data
// path (vfs) and the metadata path (meta) consult the same Manager so a
// replica's failure state has exactly one answer mount-wide.
package srcmgr

import (
	"errors"
	"sync/atomic"
	"syscall"

	"github.com/aliyun/cachefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
)

var logger = utils.GetLogger("cachefs")

// infraErrnos is the whitelist of low-level media/transport errors that fail a
// replica over to the next one. Semantic errors (ENOENT, EPERM, ...),
// consistency anomalies (size mismatch, short read) and anything not listed
// here are returned to the caller as-is, without failing the replica over.
var infraErrnos = []syscall.Errno{
	syscall.EIO,
	syscall.ESTALE,
	syscall.ENOTCONN,
	syscall.EUCLEAN,
	syscall.EREMOTEIO,
}

// IsInfraError reports whether err (possibly wrapped, e.g. in *os.PathError)
// is one of the whitelisted infrastructure errnos.
func IsInfraError(err error) bool {
	for _, eno := range infraErrnos {
		if errors.Is(err, eno) {
			return true
		}
	}
	return false
}

// Manager holds the source dir replicas in priority order and their permanent
// failure flags. It is safe for concurrent use; the dirs slice is immutable
// after construction. It contains atomics and must not be copied after use.
//
// Replica dirs double as replica identity (RunOp callbacks, metrics and
// logs), so they must be unique: duplicates are dropped at construction,
// keeping the first occurrence.
type Manager struct {
	dirs        []string
	dead        []atomic.Bool
	outageTotal *prometheus.CounterVec // nil when reg is nil (tests)
}

// NewSourceManager returns a Manager over the given priority-ordered source dirs.
func NewSourceManager(dirs []string, reg prometheus.Registerer) *Manager {
	seen := make(map[string]struct{}, len(dirs))
	unique := make([]string, 0, len(dirs))
	for _, dir := range dirs {
		if _, ok := seen[dir]; ok {
			logger.Warnf("Duplicate source dir replica ignored: %s", dir)
			continue
		}
		seen[dir] = struct{}{}
		unique = append(unique, dir)
	}
	m := &Manager{dirs: unique, dead: make([]atomic.Bool, len(unique))}
	if reg != nil {
		m.outageTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "source_replica_outage_total",
			Help: "Permanent source dir replica failures, labeled by source dir.",
		}, []string{"source"})
		reg.MustRegister(m.outageTotal)

		for i, dir := range unique {
			idx := i
			reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name:        "source_replica_active_source",
				Help:        "The source dir replica currently serving reads (1 = active, 0 = not)",
				ConstLabels: prometheus.Labels{"source": dir},
			}, func() float64 {
				if m.current() == idx {
					return 1
				}
				return 0
			}))
		}
	}
	logger.Infof("Initializing source manager with dirs: %v", unique)
	return m
}

// current returns the index of the highest-priority replica that has not
// failed. When every replica has failed, it returns the last index: the
// lowest-priority replica keeps serving even when failed, so behavior
// degrades to the single-source-dir status quo. It returns -1 for an empty
// replica list (sourceless consumer mode, which never touches source).
func (m *Manager) current() int {
	for i := range m.dirs {
		if !m.dead[i].Load() {
			return i
		}
	}
	return len(m.dirs) - 1
}

// CurrentAliveDir returns the path of the highest-priority live replica: the
// last replica when every replica has failed, or "" when there are no
// replicas or m is nil (unwired config in tests).
func (m *Manager) CurrentAliveDir() string {
	if m == nil {
		return ""
	}
	idx := m.current()
	if idx < 0 {
		return ""
	}
	return m.dirs[idx]
}

// kill permanently marks replica idx as failed, logging and counting the
// outage exactly once (the compare-and-swap wins under concurrent kills). op
// and target describe the failing operation for the log, e.g. ("read",
// "/f"); only the first killer's are recorded. It returns the next candidate
// index to retry, or -1 when no lower-priority candidate remains and the
// caller should return the current error as-is.
// The retry loop this serves always starts at current and only advances
// via kill, so every replica before idx has already failed and current
// after the kill is > idx exactly when a candidate remains.
func (m *Manager) kill(idx int, err error, op, target string) int {
	if m.dead[idx].CompareAndSwap(false, true) {
		logger.Warnf("Source dir %s is marked failed, %s on %s: %s", m.dirs[idx], op, target, err)
		if m.outageTotal != nil {
			m.outageTotal.WithLabelValues(m.dirs[idx]).Inc()
		}
	}
	if next := m.current(); next > idx {
		return next
	}
	return -1
}

// RunOp runs op against the current-alive replica and retries on
// lower-priority replicas while op fails with an infrastructure error. It
// returns nil on success, or the last error when that is semantic (returned
// as-is) or no lower-priority candidate remains. name and target describe the
// operation for the kill log. A nil or empty manager degenerates to a single
// op(""), matching historic single-dir behavior.
func (m *Manager) RunOp(name, target string, op func(dir string) error) error {
	if m == nil {
		return op("")
	}
	idx := m.current()
	if idx < 0 {
		return op("")
	}

	dir := m.dirs[idx]
	for {
		err := op(dir)
		if !IsInfraError(err) {
			return err
		}
		next := m.kill(idx, err, name, target)
		if next < 0 {
			return err
		}
		idx = next
		dir = m.dirs[idx]
		logger.Infof("Retrying operation %s on target %s with next alive source: %s",
			name, target, dir)
	}
}

// Len returns the number of replicas.
func (m *Manager) Len() int {
	return len(m.dirs)
}

// Dirs returns the replica dirs in priority order. The slice is shared with
// the manager and must not be modified.
func (m *Manager) Dirs() []string {
	return m.dirs
}
