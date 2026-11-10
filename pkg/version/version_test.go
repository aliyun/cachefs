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

package version

import "testing"

func TestVersion(t *testing.T) {
	ver = Semver{
		major: 1,
		minor: 0,
		patch: 0,
		build: "2022-02-22.f4692af9",
	}
	if v := Version(); v != "1.0.0+2022-02-22.f4692af9" {
		t.Errorf("Version %s != expected 1.0.0+2022-02-22.f4692af9", v)
		return
	}
	if _, err := Compare(""); err == nil {
		t.Errorf("Expect failed to parse empty string")
		return
	}
	if _, err := Compare("0.1.2.3"); err == nil {
		t.Errorf("Expect failed to parse string \"0.1.2.3\"")
		return
	}

	cases := []struct {
		vs     string
		expect int
	}{
		{"0.9+foo.bar", 1},
		{"0.9.10", 1},
		{"1.0-beta+baz", 1},
		{"1", 0},
		{"1.1", -1},
		{"2.0.0-alpha", -1},
	}
	for _, c := range cases {
		if r, _ := Compare(c.vs); r != c.expect {
			t.Errorf("Failed case: %+v", c)
			return
		}
	}

	ver.preRelease = "beta"
	if v := Version(); v != "1.0.0-beta+2022-02-22.f4692af9" {
		t.Errorf("Version %s != expected 1.0.0-beta+2022-02-22.f4692af9", v)
		return
	}
	cases[2].expect = 0
	cases[3].expect = -1
	for _, c := range cases {
		if r, _ := Compare(c.vs); r != c.expect {
			t.Errorf("Failed case: %+v", c)
			return
		}
	}
}

func TestParse(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  *Semver
	}{
		{"full version", "1.5.2", &Semver{1, 5, 2, "", ""}},
		{"major only", "3", &Semver{3, 0, 0, "", ""}},
		{"major.minor", "1.5", &Semver{1, 5, 0, "", ""}},
		{"with build", "1.5.2+meta", &Semver{1, 5, 2, "", ""}},
		{"with prerelease", "1.5.2-beta", &Semver{1, 5, 2, "beta", ""}},
		{"with both", "1.5.2-rc1+build", &Semver{1, 5, 2, "rc1", ""}},
		{"too many parts", "1.5.2.1", nil},
		{"non-numeric", "a.b.c", nil},
		{"empty", "", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Parse(tt.input)
			if tt.want == nil {
				if got != nil {
					t.Errorf("Parse(%q) = %+v, want nil", tt.input, got)
				}
				return
			}
			if got == nil {
				t.Errorf("Parse(%q) = nil, want %+v", tt.input, tt.want)
				return
			}
			if got.major != tt.want.major || got.minor != tt.want.minor || got.patch != tt.want.patch || got.preRelease != tt.want.preRelease {
				t.Errorf("Parse(%q) = {%d,%d,%d,%q}, want {%d,%d,%d,%q}", tt.input,
					got.major, got.minor, got.patch, got.preRelease,
					tt.want.major, tt.want.minor, tt.want.patch, tt.want.preRelease)
			}
		})
	}
}

func TestSemverCmp(t *testing.T) {
	tests := []struct {
		name string
		a, b string
		want int
	}{
		{"equal", "1.5.2", "1.5.2", 0},
		{"a greater major", "2.0.0", "1.5.2", 1},
		{"a less major", "0.9.8", "1.5.2", -1},
		{"a greater minor", "1.6.0", "1.5.2", 1},
		{"a less minor", "1.4.5", "1.5.2", -1},
		{"a greater patch", "1.5.3", "1.5.2", 1},
		{"a less patch", "1.5.1", "1.5.2", -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := Parse(tt.a)
			b := Parse(tt.b)
			got := a.Cmp(*b)
			if got != tt.want {
				t.Errorf("Cmp(%s, %s) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}
