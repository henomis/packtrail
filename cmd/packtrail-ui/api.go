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
	"fmt"
	"mime"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/names"
)

const (
	keyExecID    = "exec_id"
	keyOK        = "ok"
	defaultLimit = 100
	maxBody      = 4 << 20
	requestTTL   = 30 * time.Second
)

// api serves every namespace of the NATS account (or only an allowlist of
// them), keeping one lazily attached client per namespace.
type api struct {
	nc    *nats.Conn
	def   string
	allow []string // nil: any namespace discovered on the account

	mu      sync.Mutex
	clients map[string]*packtrail.Client
}

// newAPI returns the API. def is the namespace the UI selects first; allow,
// when non-nil, restricts the UI to those namespaces.
func newAPI(nc *nats.Conn, def string, allow []string) *api {
	return &api{nc: nc, def: def, allow: allow, clients: map[string]*packtrail.Client{}}
}

// nsHandler is a handler bound to the client of the {ns} path segment.
type nsHandler func(http.ResponseWriter, *http.Request, *packtrail.Client)

func (a *api) routes(static http.Handler) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/namespaces", a.namespaces)

	for pattern, h := range map[string]nsHandler{
		"GET /executions":                 a.list,
		"GET /executions/{id}":            a.get,
		"GET /executions/{id}/history":    a.history,
		"GET /executions/{id}/watch":      a.watch,
		"POST /executions/{id}/signal":    a.signal,
		"POST /executions/{id}/resume":    a.resume,
		"POST /executions/{id}/cancel":    a.cancel,
		"POST /executions/{id}/fork":      a.fork,
		"POST /executions/{id}/rerun":     a.rerun,
		"GET /flows":                      a.flows,
		"GET /flows/{name}":               a.flow,
		"GET /schedules":                  a.schedules,
		"GET /deadletters":                a.deadLetters,
		"POST /deadletters/{seq}/redrive": a.redrive,
	} {
		method, path, _ := strings.Cut(pattern, " ")
		mux.HandleFunc(method+" /api/ns/{ns}"+path, a.bind(h))
	}

	mux.Handle("GET /", static)

	return securityHeaders(mux)
}

func (a *api) bind(h nsHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := ctxOf(r)
		c, err := a.client(ctx, r.PathValue("ns"))

		cancel()

		if err != nil {
			httpError(w, err)

			return
		}

		h(w, r, c)
	}
}

// client returns the attached client of ns. A client is cached only once it
// has attached: Client keeps its first attach error forever, so a namespace
// that is not provisioned yet (or a transient NATS failure) must not poison
// the cache. Only provisioned (and allowed) namespaces get a client at all.
func (a *api) client(ctx context.Context, ns string) (*packtrail.Client, error) {
	a.mu.Lock()
	c, ok := a.clients[ns]
	a.mu.Unlock()

	if ok {
		return c, nil
	}

	known, err := a.known(ctx)
	if err != nil {
		return nil, err
	}

	if !slices.Contains(known, ns) {
		return nil, fmt.Errorf("%w: namespace %q", packtrail.ErrNotFound, ns)
	}

	c, err = packtrail.NewClient(a.nc, packtrail.WithClientNamespace(ns))
	if err != nil {
		return nil, err
	}

	// Flows attaches the client (a cheap KV key listing).
	if _, err = c.Flows(ctx); err != nil {
		return nil, fmt.Errorf("namespace %q: %w", ns, err)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if prev, cached := a.clients[ns]; cached {
		return prev, nil
	}

	a.clients[ns] = c

	return c, nil
}

// known lists the namespaces the UI may serve: every namespace provisioned on
// the account, narrowed to the allowlist when there is one.
func (a *api) known(ctx context.Context) ([]string, error) {
	found, err := discover(ctx, a.nc)
	if err != nil || a.allow == nil {
		return found, err
	}

	return slices.DeleteFunc(found, func(ns string) bool { return !slices.Contains(a.allow, ns) }), nil
}

// discover finds the provisioned namespaces: a prefix p is one when both the
// p-events and p-cmd streams exist.
func discover(ctx context.Context, nc *nats.Conn) ([]string, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	streams := map[string]bool{}

	lister := js.StreamNames(ctx)
	for name := range lister.Name() {
		streams[name] = true
	}

	if err = lister.Err(); err != nil {
		return nil, fmt.Errorf("list streams: %w", err)
	}

	out := []string{}

	for name := range streams {
		p, ok := strings.CutSuffix(name, "-events")
		if !ok || !names.ValidPrefix(p) {
			continue
		}

		if n := names.New(p); streams[n.StreamCmd] {
			out = append(out, p)
		}
	}

	slices.Sort(out)

	return out, nil
}

func (a *api) namespaces(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := ctxOf(r)
	defer cancel()

	known, err := a.known(ctx)
	if err != nil {
		httpError(w, err)

		return
	}

	writeJSON(w, map[string]any{"namespaces": known, "default": a.def})
}

func securityHeaders(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		h.ServeHTTP(w, r)
	})
}

func ctxOf(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), requestTTL)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// httpError maps the public error sentinels to status codes: caller mistakes
// are 4xx, not 500.
func httpError(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError

	switch {
	case errors.Is(err, packtrail.ErrInvalidArgument):
		code = http.StatusBadRequest
	case errors.Is(err, packtrail.ErrNotFound), errors.Is(err, packtrail.ErrUnknownFlow):
		code = http.StatusNotFound
	case errors.Is(err, packtrail.ErrArchived):
		code = http.StatusConflict
	}

	http.Error(w, err.Error(), code)
}

// decodeAction reads a JSON action body. Requiring application/json blocks
// cross-site form posts (a browser cannot send it without a CORS preflight).
func decodeAction(w http.ResponseWriter, r *http.Request, v any) bool {
	mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mt != "application/json" {
		http.Error(w, "content type must be application/json", http.StatusUnsupportedMediaType)

		return false
	}

	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody)).Decode(v); err != nil {
		http.Error(w, "invalid JSON body: "+err.Error(), http.StatusBadRequest)

		return false
	}

	return true
}

func (a *api) list(w http.ResponseWriter, r *http.Request, c *packtrail.Client) {
	ctx, cancel := ctxOf(r)
	defer cancel()

	q := r.URL.Query()
	f := packtrail.ListFilter{Status: packtrail.Status(q.Get("status")), Flow: q.Get("flow"), Limit: defaultLimit}

	if attr := q.Get("attr"); attr != "" {
		k, v, _ := strings.Cut(attr, "=")
		f.Attr, f.Value = k, v
	}

	if l, err := strconv.Atoi(q.Get("limit")); err == nil && l > 0 {
		f.Limit = l
	}

	out, err := c.List(ctx, f)
	if err != nil {
		httpError(w, err)

		return
	}

	writeJSON(w, out)
}

func (a *api) get(w http.ResponseWriter, r *http.Request, c *packtrail.Client) {
	ctx, cancel := ctxOf(r)
	defer cancel()

	seq, _ := strconv.ParseUint(r.URL.Query().Get("seq"), 10, 64)

	st, err := c.StateAt(ctx, r.PathValue("id"), seq)
	if err != nil {
		httpError(w, err)

		return
	}

	writeJSON(w, st)
}

type historyRow struct {
	Seq         uint64 `json:"seq"`
	DecisionEnd bool   `json:"decision_end"`
	Event       any    `json:"event"`
}

func (a *api) history(w http.ResponseWriter, r *http.Request, c *packtrail.Client) {
	ctx, cancel := ctxOf(r)
	defer cancel()

	evs, err := c.History(ctx, r.PathValue("id"))
	if err != nil {
		httpError(w, err)

		return
	}

	rows := make([]historyRow, len(evs))
	for i, ev := range evs {
		rows[i] = historyRow{Seq: ev.Seq, DecisionEnd: ev.DecisionEnd, Event: ev}
	}

	writeJSON(w, rows)
}

// watch streams new events as server-sent events until the execution ends.
func (a *api) watch(w http.ResponseWriter, r *http.Request, c *packtrail.Client) {
	from, _ := strconv.ParseUint(r.URL.Query().Get("from"), 10, 64)

	ch, err := c.Watch(r.Context(), r.PathValue("id"), from)
	if err != nil {
		httpError(w, err)

		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")

	for ev := range ch {
		b, lerr := json.Marshal(historyRow{Seq: ev.Seq, DecisionEnd: ev.DecisionEnd, Event: ev})
		if lerr != nil {
			return
		}

		if _, lerr = fmt.Fprintf(w, "data: %s\n\n", b); lerr != nil {
			return
		}

		flusher.Flush()
	}
}

func (a *api) signal(w http.ResponseWriter, r *http.Request, c *packtrail.Client) {
	var body struct {
		Name    string          `json:"name"`
		Payload json.RawMessage `json:"payload"`
	}

	if !decodeAction(w, r, &body) {
		return
	}

	a.act(w, r, func(ctx context.Context) (any, error) {
		return nil, c.Signal(ctx, r.PathValue("id"), body.Name, nullable(body.Payload))
	})
}

func (a *api) resume(w http.ResponseWriter, r *http.Request, c *packtrail.Client) {
	var body struct {
		Node  string          `json:"node"`
		Value json.RawMessage `json:"value"`
	}

	if !decodeAction(w, r, &body) {
		return
	}

	a.act(w, r, func(ctx context.Context) (any, error) {
		return nil, c.Resume(ctx, r.PathValue("id"), body.Node, nullable(body.Value))
	})
}

func (a *api) cancel(w http.ResponseWriter, r *http.Request, c *packtrail.Client) {
	var body struct {
		Reason string `json:"reason"`
	}

	if !decodeAction(w, r, &body) {
		return
	}

	a.act(w, r, func(ctx context.Context) (any, error) {
		return nil, c.Cancel(ctx, r.PathValue("id"), body.Reason)
	})
}

func (a *api) fork(w http.ResponseWriter, r *http.Request, c *packtrail.Client) {
	var body struct {
		Seq uint64 `json:"seq"`
	}

	if !decodeAction(w, r, &body) {
		return
	}

	a.act(w, r, func(ctx context.Context) (any, error) {
		id, err := c.Fork(ctx, r.PathValue("id"), body.Seq)

		return map[string]string{keyExecID: id}, err
	})
}

func (a *api) rerun(w http.ResponseWriter, r *http.Request, c *packtrail.Client) {
	var body struct {
		Node string `json:"node"`
	}

	if !decodeAction(w, r, &body) {
		return
	}

	a.act(w, r, func(ctx context.Context) (any, error) {
		id, err := c.Rerun(ctx, r.PathValue("id"), body.Node)

		return map[string]string{keyExecID: id}, err
	})
}

func (a *api) act(w http.ResponseWriter, r *http.Request, fn func(context.Context) (any, error)) {
	ctx, cancel := ctxOf(r)
	defer cancel()

	out, err := fn(ctx)
	if err != nil {
		httpError(w, err)

		return
	}

	if out == nil {
		out = map[string]bool{keyOK: true}
	}

	writeJSON(w, out)
}

func nullable(raw json.RawMessage) any {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}

	return raw
}

func (a *api) flows(w http.ResponseWriter, r *http.Request, c *packtrail.Client) {
	ctx, cancel := ctxOf(r)
	defer cancel()

	out, err := c.Flows(ctx)
	if err != nil {
		httpError(w, err)

		return
	}

	writeJSON(w, out)
}

func (a *api) flow(w http.ResponseWriter, r *http.Request, c *packtrail.Client) {
	ctx, cancel := ctxOf(r)
	defer cancel()

	def, err := c.Flow(ctx, r.PathValue("name"), r.URL.Query().Get("version"))
	if err != nil {
		httpError(w, err)

		return
	}

	writeJSON(w, map[string]any{"definition": def, "start": def.StartNode()})
}

func (a *api) schedules(w http.ResponseWriter, r *http.Request, c *packtrail.Client) {
	ctx, cancel := ctxOf(r)
	defer cancel()

	out, err := c.Schedules(ctx)
	if err != nil {
		httpError(w, err)

		return
	}

	writeJSON(w, out)
}

type deadLetterRow struct {
	packtrail.DeadLetter

	Seq uint64 `json:"seq"`
}

func (a *api) deadLetters(w http.ResponseWriter, r *http.Request, c *packtrail.Client) {
	ctx, cancel := ctxOf(r)
	defer cancel()

	out, err := c.DeadLetters(ctx, defaultLimit)
	if err != nil {
		httpError(w, err)

		return
	}

	rows := make([]deadLetterRow, len(out))
	for i, d := range out {
		rows[i] = deadLetterRow{DeadLetter: d, Seq: d.Seq}
	}

	writeJSON(w, rows)
}

func (a *api) redrive(w http.ResponseWriter, r *http.Request, c *packtrail.Client) {
	seq, err := strconv.ParseUint(r.PathValue("seq"), 10, 64)
	if err != nil {
		http.Error(w, "invalid sequence", http.StatusBadRequest)

		return
	}

	a.act(w, r, func(ctx context.Context) (any, error) { return nil, c.Redrive(ctx, seq) })
}
