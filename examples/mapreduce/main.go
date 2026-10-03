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

// Example mapreduce: a dynamic fan-out over a list computed at run time.
//
// The map node runs one task per element of `input.docs` (at most 4 at a
// time); each instance sees its element as `item`. Outputs are collected in
// order in results.count, and every instance merges its word counts into the
// `words` channel and adds to `total` — the reduce step is just the reducers.
//
//	go run ./examples/mapreduce
package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/examples/internal/devserver"
	"github.com/henomis/packtrail/examples/internal/exutil"
	"github.com/henomis/packtrail/worker"
)

const ns = "example-mapreduce"

const flowYAML = `
name: wordcount
channels:
  total: {reducer: sum}
  longest: {reducer: append}
nodes:
  - {id: count, type: map, kind: counter, over: input.docs, max_parallel: 4}
`

func main() {
	nc, closeAll := devserver.Connect()
	defer closeAll()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	g := exutil.NewGroup(ctx)
	defer g.Stop()

	eng := g.Engine(nc, packtrail.WithNamespace(ns), packtrail.WithFlowYAML([]byte(flowYAML)))

	g.Serve(nc, ns, "counter", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var doc string
		if err := j.Item(&doc); err != nil {
			return nil, worker.Permanent(err)
		}

		words := strings.Fields(doc)
		longest := ""

		for _, w := range words {
			if len(w) > len(longest) {
				longest = w
			}
		}

		return &worker.Result{
			Output: map[string]any{"doc": *j.Context.Index, "words": len(words)},
			Writes: map[string]any{"total": len(words), "longest": longest},
		}, nil
	}, worker.WithConcurrency(4))

	docs := []string{
		"event sourcing keeps every change",
		"the log is the source of truth",
		"state is a fold over the log",
		"forks and time travel come for free",
		"nats jetstream stores the events",
		"workers can be written in any language",
	}

	st := exutil.Run(ctx, eng.Client(), "wordcount", map[string]any{"docs": docs})
	if st.Status != packtrail.StatusCompleted {
		log.Fatalf("%s: %s", st.Status, st.Error)
	}

	var perDoc []struct{ Doc, Words int }

	if err := st.Result("count", &perDoc); err != nil {
		log.Fatal(err)
	}

	var longest []string

	if err := st.Channel("longest", &longest); err != nil {
		log.Fatal(err)
	}

	sort.Strings(longest)

	fmt.Printf("per document: %+v\n", perDoc)
	fmt.Printf("total words: %s\n", st.Channels["total"])
	fmt.Printf("longest words: %v\n", longest)
}
