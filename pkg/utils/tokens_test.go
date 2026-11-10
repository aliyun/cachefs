/*
 * Copyright 2024 Alibaba Cloud, Inc. or its affiliates.
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

package utils

import (
	"testing"
	"time"
)

func TestToken(t *testing.T) {
	ch := make(chan struct{})
	token := NewToken(4)
	worker := func(ch chan struct{}) {
		token.Get()
		ch <- struct{}{}
		token.Put()
	}

	// Has available token
	timer := time.NewTimer(time.Millisecond * 100)
	go worker(ch)
	select {
	case <-timer.C:
		t.Error("Should have available token, but timed out")
	case <-ch:
		t.Log("Got token")
	}
	time.Sleep(time.Second)

	// Has 1 available token
	for i := 0; i < 3; i++ {
		token.Get()
	}
	timer = time.NewTimer(time.Millisecond * 100)
	go worker(ch)
	select {
	case <-timer.C:
		t.Error("Should have 1 available token, but timed out")
	case <-ch:
		t.Log("Got token")
	}
	for i := 0; i < 3; i++ {
		token.Put()
	}
	time.Sleep(time.Second)

	// Take all tokens
	for i := 0; i < 4; i++ {
		token.Get()
	}
	timer = time.NewTimer(time.Millisecond * 100)
	go worker(ch)
	select {
	case <-timer.C:
		t.Log("No token, timed out")
	case <-ch:
		t.Error("Should have no available token, but got token")
	}
	for i := 0; i < 4; i++ {
		token.Put()
	}
}
