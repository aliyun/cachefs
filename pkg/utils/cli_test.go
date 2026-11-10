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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/urfave/cli/v2"
)

// testApp builds a minimal cli.App used across all ReorderOptions tests.
//
//	Global flags : --verbose / -v (bool), --output / -o (string)
//	Commands     : "run"  (flag: --jobs / -j string)
//	             : "show" (subcommand: "detail", flag: --full bool)
func testApp() *cli.App {
	return &cli.App{
		Flags: []cli.Flag{
			&cli.BoolFlag{Name: "verbose", Aliases: []string{"v"}},
			&cli.StringFlag{Name: "output", Aliases: []string{"o"}},
		},
		Commands: []*cli.Command{
			{
				Name: "run",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "jobs", Aliases: []string{"j"}},
				},
			},
			{
				Name: "show",
				Subcommands: []*cli.Command{
					{
						Name: "detail",
						Flags: []cli.Flag{
							&cli.BoolFlag{Name: "full"},
						},
					},
				},
			},
		},
	}
}

func TestIsFlag(t *testing.T) {
	flags := []cli.Flag{
		&cli.BoolFlag{Name: "verbose", Aliases: []string{"v"}},
		&cli.StringFlag{Name: "output", Aliases: []string{"o"}},
	}

	tests := []struct {
		name           string
		option         string
		expectIsFlag   bool
		expectHasValue bool
	}{
		{"non-flag positional arg", "file.txt", false, false},
		{"bool flag long form", "--verbose", true, false},
		{"bool flag short alias", "-v", true, false},
		{"bool flag double-dash short", "--v", true, false},
		{"string flag long form needs separate value", "--output", true, true},
		{"string flag short alias needs separate value", "-o", true, true},
		{"string flag with inline value needs no separate value", "--output=file", true, false},
		{"string flag short with inline value", "-o=file", true, false},
		{"unknown flag", "--unknown", false, false},
		{"unknown short flag", "-x", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIsFlag, gotHasValue := isFlag(flags, tt.option)
			assert.Equal(t, tt.expectIsFlag, gotIsFlag)
			assert.Equal(t, tt.expectHasValue, gotHasValue)
		})
	}
}

func TestReorderOptions(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			"global bool flag already before command is unchanged",
			[]string{"app", "--verbose", "run", "file"},
			[]string{"app", "--verbose", "run", "file"},
		},
		{
			"global bool flag after command is moved before it",
			[]string{"app", "run", "file", "--verbose"},
			[]string{"app", "--verbose", "run", "file"},
		},
		{
			"global bool flag short alias after command is moved before it",
			[]string{"app", "run", "--v", "file"},
			[]string{"app", "--v", "run", "file"},
		},
		{
			"global string flag with separate value after command is moved before it",
			[]string{"app", "run", "file", "--output", "out.txt"},
			[]string{"app", "--output", "out.txt", "run", "file"},
		},
		{
			"global string flag with inline value after command is moved before it",
			[]string{"app", "run", "--output=out.txt", "file"},
			[]string{"app", "--output=out.txt", "run", "file"},
		},
		{
			"command flag stays with command",
			[]string{"app", "run", "--jobs", "4", "file"},
			[]string{"app", "run", "--jobs", "4", "file"},
		},
		{
			"command flag with inline value stays with command",
			[]string{"app", "run", "--jobs=4"},
			[]string{"app", "run", "--jobs=4"},
		},
		{
			"mixed: global flag after command, command flag stays",
			[]string{"app", "run", "--jobs", "4", "file", "--verbose"},
			[]string{"app", "--verbose", "run", "--jobs", "4", "file"},
		},
		{
			"help flag -h stays with command",
			[]string{"app", "run", "file", "--h"},
			[]string{"app", "run", "--h", "file"},
		},
		{
			"unrecognized command passes all args through unchanged",
			[]string{"app", "unknown", "--flag", "val"},
			[]string{"app", "unknown", "--flag", "val"},
		},
		{
			"no command - only global flags",
			[]string{"app", "--verbose"},
			[]string{"app", "--verbose"},
		},
		{
			"version flag (added automatically) is treated as global bool",
			[]string{"app", "run", "--version"},
			[]string{"app", "--version", "run"},
		},
		{
			"subcommand: global flag after subcommand is moved before parent",
			[]string{"app", "show", "detail", "--verbose"},
			[]string{"app", "--verbose", "show", "detail"},
		},
		{
			"subcommand: subcommand flag stays with subcommand",
			[]string{"app", "show", "detail", "--full"},
			[]string{"app", "show", "detail", "--full"},
		},
		{
			"positional args are preserved in order after flags",
			[]string{"app", "run", "a", "b", "--jobs", "2", "c"},
			[]string{"app", "run", "--jobs", "2", "a", "b", "c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ReorderOptions(testApp(), tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
