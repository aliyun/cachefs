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

package utils

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLivenessProbe(t *testing.T) {
	tests := []struct {
		name   string
		addr   string
		rand   bool
		result error
	}{
		{"invalid addr", "invalid:addr:", false, errors.New("liveness format invalid:addr: error: address invalid:addr:: too many colons in address")},
		{"valid addr", "127.0.0.1:7959", false, nil},
		{"addr already used", "127.0.0.1:7959", false, errors.New("failed to listen on liveness addr 127.0.0.1:7959: listen tcp 127.0.0.1:7959: bind: address already in use")},
		{"fallback to random port", "127.0.0.1:7959", true, nil},
	}

	for _, tc := range tests {
		err := LivenessProbe(tc.addr, tc.rand)
		assert.Equal(t, tc.result, err, tc.name)
		if err == nil {
			httpAddr := fmt.Sprintf("http://%s/liveness", tc.addr)
			resp, err := http.Get(httpAddr)
			if err != nil {
				t.Errorf("Get liveness failed for %s: %s", tc.name, err)
				continue
			}

			assert.Equal(t, http.StatusOK, resp.StatusCode, tc.name)

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Errorf("Read response failed for %s: %s", tc.name, err)
				resp.Body.Close()
				continue
			}
			assert.Equal(t, "OK", string(body), tc, tc.name)
			resp.Body.Close()
		}
	}
}
