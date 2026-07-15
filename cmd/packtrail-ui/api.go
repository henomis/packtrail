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
	"strconv"
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
	mux.HandleFunc("GET /api/executions/{id}/results", a.getResults)
	mux.HandleFunc("GET /api/executions/{id}/history", a.getHistory)
	mux.HandleFunc("GET /api/deadletters", a.deadLetters)
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

	result, err := a.fetchSummaries(ctx, ids)
	if err != nil {
		httpError(w, err)
		return
	}

	writeJSON(w, result)
}

// fetchSummaries fetches a summary for each id concurrently. An id archived or
// pruned between List and Get (ErrNotFound) is an expected skip; any other Get
// error is returned (first one wins) so the caller surfaces the fault rather than
// returning a silently-truncated list.
func (a *api) fetchSummaries(ctx context.Context, ids []string) ([]execSummary, error) {
	const maxParallel = 32

	out := make([]execSummary, len(ids))
	sem := make(chan struct{}, maxParallel)

	var (
		wg     sync.WaitGroup
		errMu  sync.Mutex
		getErr error
	)

	for i, id := range ids {
		wg.Add(1)

		go func(i int, id string) {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			ex, gerr := a.srv.Get(ctx, id)
			switch {
			case gerr == nil:
				out[i] = execSummary{
					ID: ex.ID, Flow: ex.Flow, Status: ex.Status,
					CurrentNode: ex.CurrentNode, Error: ex.Error, UpdatedAt: ex.UpdatedAt,
				}
			case errors.Is(gerr, packtrail.ErrNotFound):
				// Expected: gone between List and Get. Skip.
			default:
				errMu.Lock()
				if getErr == nil {
					getErr = gerr
				}
				errMu.Unlock()
			}
		}(i, id)
	}

	wg.Wait()

	if getErr != nil {
		return nil, getErr
	}

	result := make([]execSummary, 0, len(out))
	for _, e := range out {
		if e.ID != "" {
			result = append(result, e)
		}
	}

	return result, nil
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

// getResults returns the execution's assembled
// {input, results, signals, branches, last_node} context document — the
// data-plane view invokers and choice rules see. The control-state snapshot
// (getExecution) does not carry payloads; this is where they live. An archived
// execution's entries may be gone: what remains is returned.
func (a *api) getResults(w http.ResponseWriter, r *http.Request) {
	res, err := a.srv.Results(r.Context(), r.PathValue("id"))
	if errors.Is(err, packtrail.ErrNotFound) {
		http.NotFound(w, r)
		return
	}

	if err != nil {
		httpError(w, err)
		return
	}

	data, err := json.Marshal(res)
	if err != nil {
		httpError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	if _, err = w.Write(data); err != nil {
		slog.Error("write results", "err", err)
	}
}

// getHistory returns the execution's ordered transition trace (oldest first,
// capped by ?limit=). It is empty unless the observed deployment runs with
// WithHistory, so the dashboard treats an empty trace as "feature off".
func (a *api) getHistory(w http.ResponseWriter, r *http.Request) {
	limit := 0

	if s := r.URL.Query().Get("limit"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			limit = n
		}
	}

	evs, err := a.srv.History(r.Context(), r.PathValue("id"), limit)
	if err != nil {
		httpError(w, err)
		return
	}

	if evs == nil {
		evs = []packtrail.Event{}
	}

	writeJSON(w, evs)
}

// deadLetters returns the dead-letter count and the most recent records, so the
// dashboard can surface dropped poison work (a non-zero count warrants attention).
func (a *api) deadLetters(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	count, err := a.srv.DeadLetterCount(ctx)
	if err != nil {
		httpError(w, err)
		return
	}

	const recentCap = 50

	recent, err := a.srv.RecentDeadLetters(ctx, recentCap)
	if err != nil {
		httpError(w, err)
		return
	}

	writeJSON(w, map[string]any{"count": count, "recent": recent})
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
