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
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/worker"
)

func TestAPI(t *testing.T) {
	s := natstest.Start(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	eng, err := packtrail.New(s.Connect(t), packtrail.WithPartitions(2), packtrail.WithFlowYAML([]byte(`
name: ui
nodes:
  - {id: w, type: await, signal: go, timeout: 1h, next: t}
  - {id: t, type: task, kind: k}
`)))
	if err != nil {
		t.Fatal(err)
	}

	if err = eng.Init(ctx); err != nil {
		t.Fatal(err)
	}

	go func() { _ = eng.Run(ctx) }()

	w, _ := worker.New(s.Connect(t), "k", func(context.Context, *worker.Job) (*worker.Result, error) {
		return &worker.Result{Output: map[string]any{"ok": true}}, nil
	})

	go func() { _ = w.Run(ctx) }()

	id, err := eng.Client().Start(ctx, "ui", nil)
	if err != nil {
		t.Fatal(err)
	}

	static, _ := fs.Sub(webFS, "web")

	srv := httptest.NewServer(newAPI(s.Connect(t), "packtrail", nil).routes(http.FileServerFS(static)))
	defer srv.Close()

	call := func(method, path, ctype, body string) (int, string) {
		req, _ := http.NewRequestWithContext(ctx, method, srv.URL+path, strings.NewReader(body))
		if ctype != "" {
			req.Header.Set("Content-Type", ctype)
		}

		resp, lerr := http.DefaultClient.Do(req)
		if lerr != nil {
			t.Fatal(lerr)
		}
		defer resp.Body.Close()

		b, _ := io.ReadAll(resp.Body)

		return resp.StatusCode, string(b)
	}

	if code, body := call("GET", "/", "", ""); code != 200 || !strings.Contains(body, "packtrail") {
		t.Fatalf("index %d", code)
	}

	if code, body := call("GET", "/api/namespaces", "", ""); code != 200 ||
		body != `{"default":"packtrail","namespaces":["packtrail"]}`+"\n" {
		t.Fatalf("namespaces %d %s", code, body)
	}

	if code, _ := call("GET", "/api/ns/other/executions", "", ""); code != 404 {
		t.Fatalf("unknown namespace: %d", code)
	}

	if code, _ := call("POST", "/api/ns/packtrail/executions/"+id+"/signal", "text/plain", `{"name":"go"}`); code != 415 {
		t.Fatalf("form-style post accepted: %d", code)
	}

	if code, _ := call("POST", "/api/ns/packtrail/executions/nope/signal", "application/json", `{"name":"go"}`); code != 404 {
		t.Fatalf("missing execution: %d", code)
	}

	if code, _ := call("GET", "/api/ns/packtrail/executions/a.b", "", ""); code != 400 {
		t.Fatalf("invalid id: %d", code)
	}

	if code, body := call("POST", "/api/ns/packtrail/executions/"+id+"/signal", "application/json", `{"name":"go"}`); code != 200 {
		t.Fatalf("signal %d %s", code, body)
	}

	st, err := eng.Client().Wait(ctx, id)
	if err != nil || st.Status != packtrail.StatusCompleted {
		t.Fatalf("wait %v %v", st, err)
	}

	code, body := call("GET", "/api/ns/packtrail/executions/"+id+"/history", "", "")

	var rows []historyRow
	if code != 200 || json.Unmarshal([]byte(body), &rows) != nil || len(rows) < 5 {
		t.Fatalf("history %d %s", code, body)
	}

	if code, body = call("GET", "/api/ns/packtrail/flows/ui", "", ""); code != 200 || !strings.Contains(body, `"start":"w"`) {
		t.Fatalf("flow %d %s", code, body)
	}

	if code, body = call("POST", "/api/ns/packtrail/executions/"+id+"/rerun", "application/json", `{"node":"t"}`); code != 200 ||
		!strings.Contains(body, "exec_id") {
		t.Fatalf("rerun %d %s", code, body)
	}

	for _, p := range []string{"/api/executions", "/api/deadletters", "/api/schedules", "/api/flows"} {
		if code, body = call("GET", strings.Replace(p, "/api/", "/api/ns/packtrail/", 1), "", ""); code != 200 {
			t.Fatalf("%s %d %s", p, code, body)
		}
	}
}

func TestAPIAllowlist(t *testing.T) {
	s := natstest.Start(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, ns := range []string{"a", "b"} {
		eng, err := packtrail.New(s.Connect(t), packtrail.WithNamespace(ns))
		if err != nil {
			t.Fatal(err)
		}

		if err = eng.Init(ctx); err != nil {
			t.Fatal(err)
		}
	}

	nc := s.Connect(t)

	got, err := discover(ctx, nc)
	if err != nil || strings.Join(got, ",") != "a,b" {
		t.Fatalf("discover %v %v", got, err)
	}

	// "c" is allowed but not provisioned: it must fail without being cached.
	a := newAPI(nc, "a", []string{"a", "c"})
	srv := httptest.NewServer(a.routes(http.NotFoundHandler()))

	defer srv.Close()

	for path, want := range map[string]int{
		"/api/ns/a/executions": 200,
		"/api/ns/b/executions": 404,
		"/api/ns/c/executions": 404,
	} {
		req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+path, nil)

		resp, lerr := http.DefaultClient.Do(req)
		if lerr != nil {
			t.Fatal(lerr)
		}

		_ = resp.Body.Close()

		if resp.StatusCode != want {
			t.Fatalf("%s: %d, want %d", path, resp.StatusCode, want)
		}
	}

	if _, ok := a.clients["c"]; ok {
		t.Fatal("unprovisioned namespace cached")
	}
}

func TestParseAllow(t *testing.T) {
	if got, err := parseAllow(""); got != nil || err != nil {
		t.Fatalf("empty: %v %v", got, err)
	}

	if got, err := parseAllow(" b, a ,,b"); err != nil || strings.Join(got, ",") != "a,b" {
		t.Fatalf("list: %v %v", got, err)
	}

	if _, err := parseAllow("a,b.c"); err == nil {
		t.Fatal("invalid namespace accepted")
	}
}
