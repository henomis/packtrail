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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
)

const apiFlow = `
version: "1.0"
name: api-flow
nodes:
  - {id: a, type: task, invoker: custom, target: agent-a}
  - {id: b, type: task, invoker: custom, target: agent-b}
edges:
  - {from: a, to: b}
`

func newTestServer(t *testing.T) *packtrail.Server {
	t.Helper()
	srv := natstest.Start(t)
	custom := packtrail.InvokerFunc(func(_ context.Context, _ packtrail.Request) (packtrail.Result, error) {
		return packtrail.Result{Status: packtrail.StatusOK, Payload: []byte(`{"ok":true}`)}, nil
	})

	s, err := packtrail.New(srv.NC,
		packtrail.WithNamespace("uitest"),
		packtrail.WithFlow([]byte(apiFlow)),
		packtrail.WithInvoker("custom", custom),
		packtrail.WithHistory(time.Hour),
	)
	if err != nil {
		t.Fatalf("packtrail.New: %v", err)
	}

	return s
}

func TestAPIFlowsAndExecutions(t *testing.T) {
	s := newTestServer(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()

	id, err := s.Start(ctx, "api-flow", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	// Wait for completion so the visibility index is populated.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ex, getErr := s.Get(ctx, id)
		if getErr == nil && ex.Status == packtrail.ExecCompleted {
			break
		}

		time.Sleep(20 * time.Millisecond)
	}

	h := newAPI(s).routes()

	// /api/flows
	if body := doGet(t, h, "/api/flows"); !strings.Contains(body, "api-flow") {
		t.Errorf("/api/flows = %s, want api-flow", body)
	}

	// /api/flows/{name}
	var g packtrail.FlowGraph
	mustJSON(t, h, "/api/flows/api-flow", &g)

	if g.Name != "api-flow" || len(g.Nodes) != 2 || len(g.Edges) != 1 {
		t.Errorf("graph = %+v, want 2 nodes / 1 edge", g)
	}

	// /api/executions
	var execs []map[string]any
	mustJSON(t, h, "/api/executions", &execs)

	found := false

	for _, e := range execs {
		if e["id"] == id {
			found = true

			if e["status"] != string(packtrail.ExecCompleted) {
				t.Errorf("exec status = %v, want completed", e["status"])
			}
		}
	}

	if !found {
		t.Errorf("execution %s not in /api/executions: %v", id, execs)
	}

	// /api/executions/{id}
	var ex map[string]any
	mustJSON(t, h, "/api/executions/"+id, &ex)

	if ex["flow"] != "api-flow" {
		t.Errorf("detail flow = %v, want api-flow", ex["flow"])
	}

	// /api/executions/{id}/results — the assembled data-plane context
	var res map[string]any
	mustJSON(t, h, "/api/executions/"+id+"/results", &res)

	if _, ok := res["input"]; !ok {
		t.Errorf("results missing input: %v", res)
	}

	if outs, ok := res["results"].(map[string]any); !ok || outs["a"] == nil || outs["b"] == nil {
		t.Errorf("results.results = %v, want outputs for a and b", res["results"])
	}

	// /api/executions/{id}/history — history is emitted best-effort, so poll
	// briefly for the trace to land.
	var hist []map[string]any

	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		hist = nil
		mustJSON(t, h, "/api/executions/"+id+"/history?limit=50", &hist)

		if len(hist) > 0 && hist[len(hist)-1]["status"] == string(packtrail.ExecCompleted) {
			break
		}

		time.Sleep(20 * time.Millisecond)
	}

	if len(hist) == 0 {
		t.Error("history is empty, want the execution's transition trace")
	} else if last := hist[len(hist)-1]; last["status"] != string(packtrail.ExecCompleted) {
		t.Errorf("last history status = %v, want completed", last["status"])
	}

	// missing flow / execution → 404
	if code := doGetCode(t, h, "/api/flows/nope"); code != http.StatusNotFound {
		t.Errorf("missing flow code = %d, want 404", code)
	}

	if code := doGetCode(t, h, "/api/executions/nope/results"); code != http.StatusNotFound {
		t.Errorf("missing execution results code = %d, want 404", code)
	}
}

func TestServesDashboard(t *testing.T) {
	s := newTestServer(t)
	h := newAPI(s).routes()

	body := doGet(t, h, "/")
	if !strings.Contains(body, "packtrail") || !strings.Contains(body, "app.js") {
		t.Errorf("GET / did not serve the dashboard:\n%s", body)
	}

	if ct := func() string {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", "/app.js", nil))

		return rec.Header().Get("Content-Type")
	}(); !strings.Contains(ct, "javascript") {
		t.Errorf("app.js content-type = %q", ct)
	}
}

func doGet(t *testing.T, h http.Handler, path string) string {
	t.Helper()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s = %d", path, rec.Code)
	}

	return rec.Body.String()
}

func doGetCode(t *testing.T, h http.Handler, path string) int {
	t.Helper()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))

	return rec.Code
}

func mustJSON(t *testing.T, h http.Handler, path string, v any) {
	t.Helper()

	if err := json.Unmarshal([]byte(doGet(t, h, path)), v); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
}
