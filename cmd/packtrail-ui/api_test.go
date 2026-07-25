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
	"errors"
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

	if ex["node_generation"] == nil {
		t.Errorf("detail missing node_generation: %v", ex)
	}

	if versions, ok := ex["output_versions"].(map[string]any); !ok || versions["a"] == nil || versions["b"] == nil {
		t.Errorf("detail output_versions = %v, want committed versions for a and b", ex["output_versions"])
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

	if _, ok := res["branches"]; !ok {
		t.Errorf("results missing branches: %v", res)
	}

	if res["last_node"] != "b" {
		t.Errorf("results.last_node = %v, want b", res["last_node"])
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

// TestHTTPErrorDoesNotLeakInternalErrorText is a regression test:
// packtrail-ui has no authentication, so httpError used to hand err.Error()
// straight to the client — including internal store/NATS error text (bucket
// or subject names) that could help someone probe the deployment. The client
// must get a generic message; the real error still goes to the server log.
func TestHTTPErrorDoesNotLeakInternalErrorText(t *testing.T) {
	rec := httptest.NewRecorder()
	httpError(rec, errors.New("jetstream: key not found in bucket packtrail-secret-internal-bucket"))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("code = %d, want 500", rec.Code)
	}

	body := rec.Body.String()
	if strings.Contains(body, "packtrail-secret-internal-bucket") {
		t.Fatalf("response leaked internal error text: %s", body)
	}

	var decoded map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded["error"] != msgInternalError {
		t.Fatalf("error = %q, want generic %q", decoded["error"], msgInternalError)
	}
}

// TestHTTPErrorReportsDeadlineExceededGenerically checks the timeout/canceled
// branch also stays generic (it already was, but must remain so as this
// function evolves).
func TestHTTPErrorReportsDeadlineExceededGenerically(t *testing.T) {
	rec := httptest.NewRecorder()
	httpError(rec, context.DeadlineExceeded)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d, want 503", rec.Code)
	}

	var decoded map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded["error"] != msgTimeout {
		t.Fatalf("error = %q, want the generic timeout message %q", decoded["error"], msgTimeout)
	}
}

// TestGetResultsErrorDoesNotLeakValidationText exercises the fix end-to-end:
// a malformed id reaching Server.Results (via the /results route) produces a
// non-404 error whose text must not reach the client verbatim either.
func TestGetResultsErrorDoesNotLeakValidationText(t *testing.T) {
	s := newTestServer(t)
	h := newAPI(s).routes()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/api/executions/exec-%2A/results", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("code = %d, want 500", rec.Code)
	}

	body := rec.Body.String()
	if strings.Contains(body, "invalid execution id") {
		t.Fatalf("response leaked validation error text: %s", body)
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

// TestAppJSEscapesStatusField is a regression test: the served JS used to
// interpolate an execution's status field into innerHTML without the esc()
// helper every other field uses, in both the execution-list row and the
// detail header. There's no JS test runner in this repo, so this checks the
// served source directly for the un-escaped patterns rather than executing it.
func TestAppJSEscapesStatusField(t *testing.T) {
	s := newTestServer(t)
	h := newAPI(s).routes()

	body := doGet(t, h, "/app.js")

	for _, unescaped := range []string{
		"badge ${e.status}\">${e.status}",
		"badge ${ex.status}\">${ex.status}",
	} {
		if strings.Contains(body, unescaped) {
			t.Errorf("app.js still interpolates status without esc(): found %q", unescaped)
		}
	}

	for _, escaped := range []string{
		"badge ${esc(e.status)}\">${esc(e.status)}",
		"badge ${esc(ex.status)}\">${esc(ex.status)}",
	} {
		if !strings.Contains(body, escaped) {
			t.Errorf("app.js missing expected escaped status pattern %q", escaped)
		}
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
