// Copyright 2026 Simone Vellei
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dsl

import (
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// TestExampleFlowsValidate ensures every flow YAML shipped under examples/
// (each example keeps its own flows/ directory) parses and passes validation,
// so the examples always work as documented.
func TestExampleFlowsValidate(t *testing.T) {
	found := 0

	err := filepath.WalkDir("../../examples", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() || (!strings.HasSuffix(path, ".yaml") && !strings.HasSuffix(path, ".yml")) {
			return nil
		}

		found++

		f, pErr := ParseFile(path)
		if pErr != nil {
			t.Errorf("parse %s: %v", path, pErr)
			return nil
		}

		if f.StartNode() == "" {
			t.Errorf("flow %q (%s) has no start node", f.Name, path)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("walk examples: %v", err)
	}

	if found == 0 {
		t.Fatal("no example flow YAMLs found")
	}
}
