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
	"fmt"
	"net"
	"net/http"
)

func LivenessProbe(rawAddr string, randPort bool) error {
	var err error
	var ip, port string

	// default set
	ip, port, err = net.SplitHostPort(rawAddr)
	if err != nil {
		return fmt.Errorf("liveness format %s error: %v", rawAddr, err)
	}

	addr := net.JoinHostPort(ip, port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		// return error if random port is not enabled
		if !randPort {
			return fmt.Errorf("failed to listen on liveness addr %s: %v", addr, err)
		}

		// otherwise try again with random port
		logger.Infof("Failed to listen on liveness addr %s: %v, fallback to random port", addr, err)
		ln, err = net.Listen("tcp", net.JoinHostPort(ip, "0"))
		if err != nil {
			return fmt.Errorf("failed to listen on random liveness addr: %w", err)
		}
	}

	mux := http.NewServeMux()
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}
	mux.HandleFunc("/liveness", handler)

	go func() {
		if err := http.Serve(ln, mux); err != nil {
			logger.Fatalf("Serve for liveness: %s", err)
		}
	}()

	logger.Infof("Liveness probe listening on %s", ln.Addr())
	return nil
}
