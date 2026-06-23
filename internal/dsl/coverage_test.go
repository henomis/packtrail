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
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestInvokerKind(t *testing.T) {
	if got := (&Node{}).InvokerKind(); got != DefaultInvoker {
		t.Errorf("default InvokerKind = %q, want %q", got, DefaultInvoker)
	}

	if got := (&Node{Invoker: "agent"}).InvokerKind(); got != "agent" {
		t.Errorf("explicit InvokerKind = %q, want agent", got)
	}
}

func TestInvokeTarget(t *testing.T) {
	if got := (&Node{Target: "t", Subject: "s"}).InvokeTarget(); got != "t" {
		t.Errorf("InvokeTarget with Target = %q, want t (Target wins)", got)
	}

	if got := (&Node{Subject: "s"}).InvokeTarget(); got != "s" {
		t.Errorf("InvokeTarget with only Subject = %q, want s", got)
	}

	if got := (&Node{}).InvokeTarget(); got != "" {
		t.Errorf("InvokeTarget with neither = %q, want empty", got)
	}
}

func TestJoinKindEdgeCases(t *testing.T) {
	// Unknown policy falls back to JoinAll.
	if k, q := (&Node{JoinPolicy: "majority"}).JoinKind(); k != JoinAll || q != 0 {
		t.Errorf("JoinKind(majority) = (%q,%d), want (all,0)", k, q)
	}
	// Non-numeric quorum count parses to 0.
	if k, q := (&Node{JoinPolicy: "quorum:abc"}).JoinKind(); k != JoinQuorum || q != 0 {
		t.Errorf("JoinKind(quorum:abc) = (%q,%d), want (quorum,0)", k, q)
	}
}

func TestDurationUnmarshal(t *testing.T) {
	// Valid duration on a task node.
	f, err := Parse([]byte(`
name: f
nodes:
  - {id: a, type: task, subject: s, timeout: "30s"}
`))
	if err != nil {
		t.Fatalf("parse valid duration: %v", err)
	}

	if got := f.Node("a").Timeout.D(); got != 30*time.Second {
		t.Errorf("timeout = %v, want 30s", got)
	}

	// Empty string explicitly decodes to zero.
	f, err = Parse([]byte(`
name: f
nodes:
  - {id: a, type: task, subject: s, timeout: ""}
`))
	if err != nil {
		t.Fatalf("parse empty duration: %v", err)
	}

	if got := f.Node("a").Timeout.D(); got != 0 {
		t.Errorf("empty timeout = %v, want 0", got)
	}

	// Invalid duration is a parse error.
	if _, err = Parse([]byte(`
name: f
nodes:
  - {id: a, type: task, subject: s, timeout: "bogus"}
`)); err == nil {
		t.Fatal("invalid duration parsed without error")
	}

	// A non-scalar timeout fails the string decode.
	if _, err = Parse([]byte(`
name: f
nodes:
  - {id: a, type: task, subject: s, timeout: [1, 2]}
`)); err == nil {
		t.Fatal("non-scalar duration parsed without error")
	}
}

func TestParseInvalidYAML(t *testing.T) {
	if _, err := Parse([]byte("\t: not: valid: yaml:")); err == nil {
		t.Fatal("invalid YAML parsed without error")
	}
}

func TestParseFile(t *testing.T) {
	dir := t.TempDir()

	good := filepath.Join(dir, "good.yaml")
	if err := os.WriteFile(good, []byte("name: f\nnodes:\n  - {id: a, type: task, subject: s}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := ParseFile(good); err != nil {
		t.Fatalf("ParseFile(good): %v", err)
	}

	// Missing file: read error.
	if _, err := ParseFile(filepath.Join(dir, "missing.yaml")); err == nil {
		t.Fatal("ParseFile(missing) succeeded, want error")
	}

	// Existing but invalid content: parse error (wrapped with the path).
	bad := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(bad, []byte("name: f\nnodes: []\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := ParseFile(bad); err == nil {
		t.Fatal("ParseFile(bad) succeeded, want error")
	}
}

func TestLoadDir(t *testing.T) {
	dir := t.TempDir()

	// One valid flow, a non-YAML file and a subdirectory that must be skipped.
	if err := os.WriteFile(filepath.Join(dir, "a.yaml"), []byte("name: alpha\nnodes:\n  - {id: a, type: task, subject: s}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("ignore me"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o750); err != nil {
		t.Fatal(err)
	}

	flows, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}

	if len(flows) != 1 || flows["alpha"] == nil {
		t.Fatalf("LoadDir = %v, want only [alpha]", flows)
	}

	// Missing directory: read error.
	if _, err = LoadDir(filepath.Join(dir, "nope")); err == nil {
		t.Fatal("LoadDir(missing) succeeded, want error")
	}
}

func TestLoadDirInvalidFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "broken.yaml"), []byte("name: f\nnodes: []\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := LoadDir(dir); err == nil {
		t.Fatal("LoadDir with an invalid flow file succeeded, want error")
	}
}

func TestLoadDirDuplicateName(t *testing.T) {
	dir := t.TempDir()

	flow := "name: same\nnodes:\n  - {id: a, type: task, subject: s}\n"
	if err := os.WriteFile(filepath.Join(dir, "one.yaml"), []byte(flow), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(dir, "two.yaml"), []byte(flow), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := LoadDir(dir); err == nil {
		t.Fatal("LoadDir with duplicate flow names succeeded, want error")
	}
}

// TestValidateNodeErrors covers the per-type validation error branches not
// already exercised by TestValidateErrors.
func TestValidateNodeErrors(t *testing.T) {
	cases := map[string]string{
		"empty name": `
name: "  "
nodes:
  - {id: a, type: task, subject: s}`,
		"no nodes": `
name: f
nodes: []`,
		"empty node id": `
name: f
nodes:
  - {id: "", type: task, subject: s}`,
		"task negative retry": `
name: f
nodes:
  - {id: a, type: task, subject: s, retry: {max_attempts: -1}}`,
		"fanout missing branches": `
name: f
nodes:
  - {id: a, type: fanout}`,
		"fanout unknown branch": `
name: f
nodes:
  - {id: a, type: fanout, branches: [nope]}`,
		"fanin missing wait_for": `
name: f
nodes:
  - {id: a, type: task, subject: s}
  - {id: j, type: fanin}
edges:
  - {from: a, to: j}`,
		"fanin unknown wait_for": `
name: f
nodes:
  - {id: j, type: fanin, wait_for: [nope]}`,
		"choice non-default missing when": `
name: f
nodes:
  - {id: a, type: task, subject: s}
  - {id: c, type: choice, rules: [{to: a}]}
edges:
  - {from: a, to: c}`,
		"choice rule.to unknown": `
name: f
nodes:
  - {id: c, type: choice, rules: [{default: true, to: nope}]}`,
		"signal missing name": `
name: f
nodes:
  - {id: g, type: signal}`,
		"signal unknown on_timeout": `
name: f
nodes:
  - {id: g, type: signal, signal_name: approval, on_timeout: nope}`,
		"unknown node type": `
name: f
nodes:
  - {id: a, type: bogus}`,
		"two outgoing edges": `
name: f
nodes:
  - {id: a, type: task, subject: s}
  - {id: b, type: task, subject: s}
  - {id: c, type: task, subject: s}
edges:
  - {from: a, to: b}
  - {from: a, to: c}`,
		"no start node": `
name: f
nodes:
  - {id: a, type: task, subject: s}
  - {id: b, type: task, subject: s}
edges:
  - {from: a, to: b}
  - {from: b, to: a}`,
	}
	for name, yml := range cases {
		if _, err := Parse([]byte(yml)); err == nil {
			t.Errorf("%s: expected error, got nil", name)
		}
	}
}
