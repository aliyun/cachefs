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

package utils

import (
	"os"
	"strconv"
	"strings"
)

// GetEnvInt reads environment variable `name` as an int.
// It returns `def` silently when the variable is unset or empty. When the value
// is non-numeric or below `min` (the inclusive lower bound), it logs a warning
// and returns `def`. On a valid override it logs an info line and returns the
// value. Use min=1 to require a positive value, or min=0 to also allow zero.
func GetEnvInt(name string, def, min int) int {
	s := os.Getenv(name)
	if s == "" {
		return def
	}
	value, err := strconv.Atoi(s)
	if err != nil {
		logger.Warnf("Convert %s %q failed: %s, use default %d", name, s, err, def)
		return def
	}
	if value < min {
		logger.Warnf("%s %d is less than minimum %d, use default %d", name, value, min, def)
		return def
	}
	logger.Infof("Set %s to %d via env", name, value)
	return value
}

// GetEnvBool reads environment variable `name` as a bool.
// It returns `def` silently when the variable is unset or blank. Everything
// strconv.ParseBool takes is accepted ("1", "0", "t", "f", "true", "false" and
// their upper and title case spellings), plus "yes"/"y"/"on" and "no"/"n"/"off"
// case insensitively. Any other value logs a warning and returns `def`. On a
// valid override it logs an info line and returns the value.
func GetEnvBool(name string, def bool) bool {
	s := strings.TrimSpace(os.Getenv(name))
	if s == "" {
		return def
	}
	value, err := strconv.ParseBool(s)
	if err != nil {
		switch strings.ToLower(s) {
		case "yes", "y", "on":
			value = true
		case "no", "n", "off":
			value = false
		default:
			logger.Warnf("Convert %s %q to bool failed, use default %t", name, s, def)
			return def
		}
	}
	logger.Infof("Set %s to %t via env", name, value)
	return value
}
