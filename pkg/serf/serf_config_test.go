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

package serf

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGroupMismatchLogFilter(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantPassed bool // true: line should reach the underlying writer
	}{
		{
			name:       "unrelated log line passes through",
			input:      "[INFO] memberlist: Node joined",
			wantPassed: true,
		},
		{
			name:       "push/pull error unrelated to group mismatch passes through",
			input:      "[ERR] memberlist: Failed push/pull merge: connection refused",
			wantPassed: true,
		},
		{
			name:       "group mismatch error is suppressed",
			input:      `[ERR] memberlist: Failed push/pull merge: member not in same group: "testgroup" from=1.2.3.4:7946`,
			wantPassed: false,
		},
		{
			name:       "group mismatch message alone without Failed push/pull merge passes through",
			input:      "[INFO] serf: member not in same group",
			wantPassed: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			f := &groupMismatchLogFilter{w: &buf}
			n, err := f.Write([]byte(tt.input))
			assert.NoError(t, err)
			assert.Equal(t, len(tt.input), n)
			if tt.wantPassed {
				assert.Equal(t, tt.input, buf.String())
			} else {
				assert.Empty(t, buf.String())
			}
		})
	}
}

func TestDefaultSerfConfig(t *testing.T) {
	conf := DefaultSerfConfig()
	defConf := &SerfConfig{
		BindPort:   7946,
		MaxRetry:   10,
		HashRing:   true,
		Reconnect:  20 * time.Second,
		LeaveDelay: 5 * time.Second,
		NodeCount:  100,
	}
	assert.Equal(t, defConf, conf)
}

func TestSetupSerfConfig(t *testing.T) {
	tests := []struct {
		name       string
		conf       *SerfConfig
		expectTags map[string]string
	}{
		{
			name: "cache port",
			conf: &SerfConfig{
				CachePort: 10,
			},
			expectTags: map[string]string{keyCachePort: "10"},
		},
		{
			name: "init node",
			conf: &SerfConfig{
				CachePort: 11,
				Role:      SerfRoleInit,
			},
			expectTags: map[string]string{keyCachePort: "11", keySerfRole: SerfRoleInit},
		},
		{
			name: "serf group",
			conf: &SerfConfig{
				CachePort: 12,
				Group:     "group",
			},
			expectTags: map[string]string{keyCachePort: "12", keySerfGroup: "group"},
		},
		{
			name: "serf group and init node",
			conf: &SerfConfig{
				CachePort: 13,
				Role:      SerfRoleInit,
				Group:     "group2",
			},
			expectTags: map[string]string{keyCachePort: "13", keySerfRole: SerfRoleInit, keySerfGroup: "group2"},
		},
	}

	for _, tc := range tests {
		result := setupSerfConfig(tc.conf, nil, nil, nil)
		assert.Equal(t, tc.expectTags, result.Tags, tc.name)
	}
}
