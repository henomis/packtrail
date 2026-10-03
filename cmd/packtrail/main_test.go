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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
)

const cliFlow = `
name: cli
nodes:
  - {id: w, type: await, signal: go, timeout: 1h, next: t}
  - {id: t, type: task, kind: k}
`

func runCLI(t *testing.T, args ...string) string {
	t.Helper()

	var out bytes.Buffer
	if err := run(args, &out); err != nil {
		t.Fatalf("packtrail %v: %v\n%s", args, err, out.String())
	}

	return out.String()
}

func TestCLI(t *testing.T) {
	s := natstest.Start(t)
	dir := t.TempDir()
	file := filepath.Join(dir, "cli.yaml")

	if err := os.WriteFile(file, []byte(cliFlow), 0o600); err != nil {
		t.Fatal(err)
	}

	if out := runCLI(t, "validate", file); !strings.Contains(out, "ok (cli@") {
		t.Fatalf("validate: %s", out)
	}

	srv := []string{"-server", s.URL(), "-ns", "clitest"}
	cli := func(args ...string) string { return runCLI(t, append(append([]string{}, srv...), args...)...) }

	if out := cli("init", "-partitions", "2"); !strings.Contains(out, `"partitions": 2`) {
		t.Fatalf("init: %s", out)
	}

	cli("register", file)

	if out := cli("flows"); !strings.Contains(out, `"name": "cli"`) {
		t.Fatalf("flows: %s", out)
	}

	// Start returns once an engine created the execution.
	eng, eerr := packtrail.New(s.Connect(t), packtrail.WithNamespace("clitest"), packtrail.WithPartitions(2),
		packtrail.WithoutDispatcher())
	if eerr != nil {
		t.Fatal(eerr)
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		defer close(done)

		_ = eng.Run(ctx)
	}()

	t.Cleanup(func() { cancel(); <-done })

	var started struct {
		ExecID string `json:"exec_id"`
	}
	if err := json.Unmarshal([]byte(cli("start", "cli", "-input", `{"a":1}`, "-id", "e-1")), &started); err != nil ||
		started.ExecID != "e-1" {
		t.Fatalf("start: %+v %v", started, err)
	}

	// The execution can be driven right away; a missing one cannot.
	cli("cancel", "e-1", "-reason", "done")

	var out bytes.Buffer
	if err := run(append(append([]string{}, srv...), "cancel", "nope"), &out); err == nil {
		t.Fatal("cancel of a missing execution succeeded")
	}

	if err := run(append(append([]string{}, srv...), "bogus"), &out); err == nil {
		t.Fatal("unknown command accepted")
	}

	if out := cli("schedules"); strings.TrimSpace(out) != "null" {
		t.Fatalf("schedules: %s", out)
	}
}
