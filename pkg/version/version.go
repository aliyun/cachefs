/*
 * JuiceFS, Copyright 2022 Juicedata, Inc.
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

// Reference: https://semver.org; NOT strictly followed.
package version

import (
	"fmt"
	"strconv"
	"strings"
)

var (
	revision     = "$Format:%h$" // value is assigned in Makefile
	revisionDate = "$Format:%as$"
	release      = "$Format:%h$" // value is assigned in Makefile, "" or "dev"
	major        = "0"           // value is assigned in Makefile from build/version
	minor        = "0"           // value is assigned in Makefile from build/version
	patch        = "0"           // value is assigned in Makefile from build/version
	ver          = initVersion()
)

func initVersion() Semver {
	maj, _ := strconv.ParseUint(major, 10, 64)
	min, _ := strconv.ParseUint(minor, 10, 64)
	pat, _ := strconv.ParseUint(patch, 10, 64)
	return Semver{
		major:      maj,
		minor:      min,
		patch:      pat,
		preRelease: fmt.Sprintf("%s", release),
		build:      fmt.Sprintf("%s.%s", revisionDate, revision),
	}
}

type Semver struct {
	major, minor, patch uint64
	preRelease, build   string
}

func Version() string {
	pr := ver.preRelease
	if pr != "" {
		pr = "-" + pr
	}
	if strings.Contains(ver.build, "Format") {
		ver.build = "unknown"
	}
	return fmt.Sprintf("%d.%d.%d%s+%s", ver.major, ver.minor, ver.patch, pr, ver.build)
}

// Cmp compares two Semver values. Returns -1, 0, or 1.
func (s Semver) Cmp(other Semver) int {
	var less bool
	if s.major != other.major {
		less = s.major < other.major
	} else if s.minor != other.minor {
		less = s.minor < other.minor
	} else if s.patch != other.patch {
		less = s.patch < other.patch
	} else if s.preRelease != other.preRelease {
		less = s.preRelease < other.preRelease
		if s.preRelease == "" || other.preRelease == "" {
			less = !less
		}
	} else {
		return 0
	}
	if less {
		return -1
	}
	return 1
}

func Compare(vs string) (int, error) {
	v := Parse(vs)
	if v == nil {
		return 1, fmt.Errorf("invalid version string: %s", vs)
	}
	return ver.Cmp(*v), nil
}

func Parse(vs string) *Semver {
	if p := strings.Index(vs, "+"); p > 0 {
		vs = vs[:p] // ignore build information
	}
	var v Semver
	if p := strings.Index(vs, "-"); p > 0 {
		v.preRelease = vs[p+1:]
		vs = vs[:p]
	}

	ps := strings.Split(vs, ".")
	if len(ps) > 3 {
		return nil
	}
	var err error
	if v.major, err = strconv.ParseUint(ps[0], 10, 64); err != nil {
		return nil
	}
	if len(ps) > 1 {
		if v.minor, err = strconv.ParseUint(ps[1], 10, 64); err != nil {
			return nil
		}
	}
	if len(ps) > 2 {
		if v.patch, err = strconv.ParseUint(ps[2], 10, 64); err != nil {
			return nil
		}
	}
	return &v
}
