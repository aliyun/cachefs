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
	"errors"
	"fmt"
	"testing"

	"github.com/eryugey/serf/serf"
	"github.com/stretchr/testify/assert"
)

func TestMergeDelegate(t *testing.T) {
	tests := []struct {
		serfGroup       string
		realGroup       string
		err             bool
		isGroupMismatch bool
	}{
		{"", "", false, false},
		{"group", "", true, true},
		{"", "group", true, true},
		{"group", "group", false, false},
	}
	for _, tt := range tests {
		delegate := newSerfMergeDelegate(tt.serfGroup)
		member := &serf.Member{
			Tags: map[string]string{
				keySerfGroup: tt.realGroup,
			},
		}
		err := delegate.NotifyMerge([]*serf.Member{member})
		msg := fmt.Sprintf("serfGroup \"%s\", realGroup \"%s\"", tt.serfGroup, tt.realGroup)
		if tt.err {
			assert.Error(t, err, msg)
			assert.Equal(t, tt.isGroupMismatch, errors.Is(err, ErrGroupMismatch), msg)
		} else {
			assert.NoError(t, err, msg)
		}
	}
}

func TestIsGroupMismatchError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "unrelated error",
			err:  errors.New("some other error"),
			want: false,
		},
		{
			name: "ErrGroupMismatch directly",
			err:  ErrGroupMismatch,
			want: true,
		},
		{
			name: "wrapped with %w",
			err:  fmt.Errorf("%w: want A got B", ErrGroupMismatch),
			want: true,
		},
		{
			name: "wrapped with %v (memberlist-style, breaks error chain)",
			err:  fmt.Errorf("Failed to join 1.2.3.4:7946: %v", fmt.Errorf("%w: want A got B", ErrGroupMismatch)),
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isGroupMismatchError(tt.err))
		})
	}
}
