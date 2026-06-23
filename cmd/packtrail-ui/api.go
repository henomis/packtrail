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
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/henomis/packtrail"
)

const pingInterval = 25 * time.Second

type api struct {
	srv *packtrail.Server
}

func newAPI(srv *packtrail.Server) *api { return &api{srv: srv} }

func (a *api) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/flows", a.listFlows)
	mux.HandleFunc("GET /api/flows/{name}", a.flowGraph)
	mux.HandleFunc("GET /api/executions", a.listExecutions)
	mux.HandleFunc("GET /api/executions/{id}", a.getExecution)
	mux.HandleFunc("GET /api/events", a.events)
	mux.Handle("/", staticHandler())

	return mux
}

// execSummary is a compact execution row for the list view.
type execSummary struct {
	ID          string    `json:"id"`
	Flow        string    `json:"flow"`
	Status      string    `json:"status"`
	CurrentNode string    `json:"current_node"`
	Error       string    `json:"error,omitempty"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func (a *api) listFlows(w http.ResponseWriter, r *http.Request) {
	flows, err := a.srv.ListFlows(r.Context())
	if err != nil {
		httpError(w, err)
		return
	}

	writeJSON(w, flows)
}

func (a *api) flowGraph(w http.ResponseWriter, r *http.Request) {
	g, err := a.srv.FlowGraph(r.Context(), r.PathValue("name"))
	if errors.Is(err, packtrail.ErrNotFound) {
		http.NotFound(w, r)
		return
	}

	if err != nil {
		httpError(w, err)
		return
	}

	writeJSON(w, g)
}

// listExecutions returns execution summaries, optionally filtered by ?status= or
// ?flow=.
//
// Filtered queries read summaries directly from the visibility index (no
// per-execution round-trips). The unfiltered case ("list all") fetches each
// execution concurrently with a bounded pool because the index has no
// all-executions view that carries full summary data.
func (a *api) listExecutions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	status := r.URL.Query().Get("status")
	flow := r.URL.Query().Get("flow")

	if status != "" || flow != "" {
		var (
			events []packtrail.Event
			err    error
		)

		if status != "" {
			events, err = a.srv.ByStatusEvents(ctx, status)
		} else {
			events, err = a.srv.ByFlowEvents(ctx, flow)
		}

		if err != nil {
			httpError(w, err)
			return
		}

		out := make([]execSummary, len(events))
		for i, ev := range events {
			out[i] = execSummary{
				ID: ev.ExecID, Flow: ev.Flow, Status: ev.Status,
				CurrentNode: ev.Node, Error: ev.Error, UpdatedAt: ev.Time,
			}
		}

		writeJSON(w, out)

		return
	}

	ids, err := a.srv.List(ctx)
	if err != nil {
		httpError(w, err)
		return
	}

	const maxParallel = 32

	out := make([]execSummary, len(ids))
	sem := make(chan struct{}, maxParallel)

	var wg sync.WaitGroup

	for i, id := range ids {
		wg.Add(1)

		go func(i int, id string) {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			ex, getErr := a.srv.Get(ctx, id)
			if getErr == nil {
				out[i] = execSummary{
					ID: ex.ID, Flow: ex.Flow, Status: ex.Status,
					CurrentNode: ex.CurrentNode, Error: ex.Error, UpdatedAt: ex.UpdatedAt,
				}
			}
		}(i, id)
	}

	wg.Wait()

	result := make([]execSummary, 0, len(out))
	for _, e := range out {
		if e.ID != "" {
			result = append(result, e)
		}
	}

	writeJSON(w, result)
}

func (a *api) getExecution(w http.ResponseWriter, r *http.Request) {
	ex, err := a.srv.Get(r.Context(), r.PathValue("id"))
	if errors.Is(err, packtrail.ErrNotFound) {
		http.NotFound(w, r)
		return
	}

	if err != nil {
		httpError(w, err)
		return
	}

	writeJSON(w, ex)
}

// events streams live execution transitions as Server-Sent Events.
func (a *api) events(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		httpError(w, errors.New("streaming unsupported"))
		return
	}

	ctx := r.Context()

	ch, err := a.srv.WatchEvents(ctx)
	if err != nil {
		httpError(w, err)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher.Flush()

	ping := time.NewTicker(pingInterval)
	defer ping.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ping.C:
			_, _ = w.Write([]byte(": ping\n\n"))

			flusher.Flush()
		case ev, open := <-ch:
			if !open {
				return
			}

			data, marshalErr := json.Marshal(ev)
			if marshalErr != nil {
				slog.Error("marshal event", "err", marshalErr)
				continue
			}

			_, _ = w.Write([]byte("data: "))
			_, _ = w.Write(data)
			_, _ = w.Write([]byte("\n\n"))

			flusher.Flush()
		}
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("write json", "err", err)
	}
}

func httpError(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		code = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)

	if encErr := json.NewEncoder(w).Encode(map[string]string{"error": err.Error()}); encErr != nil {
		slog.Error("write error response", "err", encErr)
	}
}
