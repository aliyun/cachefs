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
	"os"
	"strings"
)

// Use var to define THP related paths, useful for unit tests
var (
	thpEnabled      = "/sys/kernel/mm/transparent_hugepage/enabled"
	thpShmemEnabled = "/sys/kernel/mm/transparent_hugepage/shmem_enabled"
)

func getThpCurrentPolicy(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("failed to read %s: %w", path, err)
	}

	// get current value, which is surrounded by []
	content := string(data)
	start := strings.Index(content, "[")
	end := strings.Index(content, "]")

	if start == -1 || end == -1 || start >= end {
		return "", fmt.Errorf("invalid format in %s: %s", path, content)
	}

	currentValue := strings.TrimSpace(content[start+1 : end])
	return currentValue, nil
}

func GetThpShmemPolicy() (string, error) {
	return getThpCurrentPolicy(thpShmemEnabled)
}

func GetThpPolicy() (string, error) {
	return getThpCurrentPolicy(thpEnabled)
}

func setThpPolicy(path, policy string) error {
	file, err := os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open %s for writing: %w", path, err)
	}
	defer file.Close()

	_, err = file.WriteString(policy)
	if err != nil {
		return fmt.Errorf("failed to write '%s' to %s: %w", policy, path, err)
	}

	return nil
}

func SetThpShmemPolicy(policy string) error {
	return setThpPolicy(thpShmemEnabled, policy)
}

func SetThpPolicy(policy string) error {
	return setThpPolicy(thpEnabled, policy)
}

func SetupThp(policy string) {
	cur, err := GetThpPolicy()
	if err != nil {
		logger.Warnf("Get current transparent hugepage policy: %s", err)
		return
	}
	switch policy {
	case "default":
		logger.Infof("THP policy \"default\", current THP \"%s\"", cur)
		return
	case "always":
		if cur != "always" {
			err := SetThpPolicy("always")
			if err != nil {
				logger.Warnf("Set THP policy: %s", err)
				return
			} else {
				logger.Infof("Set THP policy from \"%s\" to \"always\"", cur)
			}
		} else {
			logger.Info("THP policy is already \"always\"")
		}
	case "madvise", "advise":
		if cur == "never" {
			err := SetThpPolicy("madvise")
			if err != nil {
				logger.Warnf("Set THP policy: %s", err)
				return
			} else {
				logger.Infof("Set THP policy from \"%s\" to \"madvise\"", cur)
			}
		} else {
			logger.Infof("THP policy is already \"%s\", skip setting to \"madvise\"", cur)
		}
	default:
		logger.Warnf("Unknown THP policy: %s", policy)
	}
	return
}

func SetupShmThp(policy string) {
	curShm, err := GetThpShmemPolicy()
	if err != nil {
		logger.Warnf("Get current shmem transparent hugepage policy: %s", err)
		return
	}
	switch policy {
	case "default":
		logger.Infof("THP policy \"default\", current shmem THP \"%s\"", curShm)
		return
	case "always":
		if curShm != "always" {
			err := SetThpShmemPolicy("always")
			if err != nil {
				logger.Warnf("Set shmem THP policy: %s", err)
				return
			} else {
				logger.Infof("Set shmem THP policy from \"%s\" to \"always\"", curShm)
			}
		} else {
			logger.Info("Shmem THP policy is already \"always\"")
		}
	case "madvise", "advise":
		if curShm != "always" && curShm != "advise" && curShm != "force" {
			err := SetThpShmemPolicy("advise")
			if err != nil {
				logger.Warnf("Set shmem THP policy: %s", err)
				return
			} else {
				logger.Infof("Set shmem THP policy from \"%s\" to \"advise\"", curShm)
			}
		} else {
			logger.Infof("Shmem THP policy is already \"%s\", skip setting to \"advise\"", curShm)
		}
	default:
		logger.Warnf("Unknown THP policy: %s", policy)
	}
	return
}
