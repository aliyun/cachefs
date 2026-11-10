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
	"strings"

	"github.com/eryugey/serf/serf"
)

// error returned when a member's serf group doesn't match.
var ErrGroupMismatch = errors.New("member not in same group")

// Check whether err was caused by a group mismatch.
// errors.Is cannot be used here because memberlist wraps the error without %w,
// which breaks the error chain.
func isGroupMismatchError(err error) bool {
	return err != nil && strings.Contains(err.Error(), ErrGroupMismatch.Error())
}

type serfDelegate struct {
	serfGroup string
}

func newSerfMergeDelegate(group string) serf.MergeDelegate {
	return &serfDelegate{group}
}

// This implements `serf.MergeDelegate` interface, which will be called on member join
// and state merge between serf nodes. And member join & state merge will be canceled
// if `NotifyMerge` returns error. This could be used to determine which nodes are
// allowed to join to this serf cluster based on user provided logic. Here we reject
// all member join from different serf groups.
func (s *serfDelegate) NotifyMerge(members []*serf.Member) error {
	for _, member := range members {
		group, same := isSerfGroup(s.serfGroup, member)
		if !same {
			return fmt.Errorf("%w: want %s got %s", ErrGroupMismatch, s.serfGroup, group)
		}
	}
	return nil
}
