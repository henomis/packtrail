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

// Command packtrail is the command-line interface: run an engine, register
// and validate flows, start, signal, resume, cancel, inspect, watch, fork and
// rerun executions, manage schedules and dead letters. Output is JSON.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/nats-io/nats.go"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/flow"
)

// keyExecID names the execution id in JSON output.
const keyExecID = "exec_id"

const usage = `usage: packtrail [-server URL] [-ns NAMESPACE] <command> [args]

engine:
  run [-flows DIR] [-partitions N]      run an engine until interrupted
  init [-flows DIR] [-partitions N]     provision the namespace and register flows
  rebuild-index                          rebuild the visibility index from the log
  archive <exec>                         archive a finished execution now

flows:
  validate <file.yaml>...                validate flow files offline
  register <file.yaml>...                register flow versions
  flows                                  list registered versions

executions:
  start <flow> [-input JSON] [-id ID] [-version HASH] [-wait]
  signal <exec> <name> [-payload JSON] [-id ID]
  update <exec> -writes JSON [-id ID]    write channels synchronously; prints the state after the write
  resume <exec> <node> [-value JSON]
  cancel <exec> [-reason TEXT]
  get <exec> [-seq N]                    state now, or right after event N
  history <exec>
  watch <exec>                           stream events until the execution ends
  progress <exec>                        stream workers' progress messages (JSON lines) until it ends
  wait <exec>
  fork <exec> <seq> [-id ID] [-writes JSON]
  rerun <exec> <node> [-id ID]
  list [-status S] [-flow F] [-attr K=V] [-limit N]

schedules and dead letters:
  schedule <name> <flow> <cron> [-input JSON] [-tz ZONE]
  unschedule <name>
  schedules
  dlq [-limit N]
  redrive <seq>
  quarantined                            executions the dispatcher gave up on
  unquarantine <exec>                    replay a quarantined execution's effects and resume it
`

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "packtrail:", err)
		os.Exit(1)
	}
}

type app struct {
	out    io.Writer
	server string
	ns     string
	nc     *nats.Conn
	client *packtrail.Client
}

func run(args []string, out io.Writer) error {
	global := flag.NewFlagSet("packtrail", flag.ContinueOnError)
	a := &app{out: out}

	global.StringVar(&a.server, "server", envOr("NATS_URL", nats.DefaultURL), "NATS server URL")
	global.StringVar(&a.ns, "ns", envOr("PACKTRAIL_NAMESPACE", "packtrail"), "namespace")
	global.Usage = func() { fmt.Fprint(os.Stderr, usage) }

	if err := global.Parse(args); err != nil {
		return err
	}

	rest := global.Args()
	if len(rest) == 0 {
		global.Usage()

		return errors.New("missing command")
	}

	if rest[0] == "validate" {
		return a.validate(rest[1:])
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	nc, err := nats.Connect(a.server, nats.Name("packtrail-cli"))
	if err != nil {
		return err
	}
	defer nc.Close()

	a.nc = nc

	if a.client, err = packtrail.NewClient(nc, packtrail.WithClientNamespace(a.ns)); err != nil {
		return err
	}

	return a.dispatch(ctx, rest[0], rest[1:])
}

func (a *app) dispatch(ctx context.Context, name string, args []string) error {
	handlers := map[string]func(context.Context, []string) error{
		"run": a.runEngine, "init": a.initEngine, "rebuild-index": a.rebuildIndex, "archive": a.archive,
		"register": a.register, "flows": a.flows, "start": a.start, "signal": a.signal, "update": a.update,
		"resume": a.resume,
		"cancel": a.cancel, "get": a.get, "history": a.history, "watch": a.watch, "progress": a.progress,
		"wait": a.wait, "fork": a.fork,
		"rerun": a.rerun, "list": a.list, "schedule": a.schedule, "unschedule": a.unschedule,
		"schedules": a.schedules, "dlq": a.dlq, "redrive": a.redrive, "quarantined": a.quarantined,
		"unquarantine": a.unquarantine,
	}

	h, ok := handlers[name]
	if !ok {
		fmt.Fprint(os.Stderr, usage)

		return fmt.Errorf("unknown command %q", name)
	}

	return h(ctx, args)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}

	return def
}

func (a *app) print(v any) error {
	enc := json.NewEncoder(a.out)
	enc.SetIndent("", "  ")

	return enc.Encode(v)
}

// parse parses flags that may follow positional arguments, and checks the
// number of positional arguments.
func parse(fs *flag.FlagSet, args []string, positional int) ([]string, error) {
	var pos []string

	for len(args) > 0 {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}

		args = fs.Args()
		if len(args) > 0 {
			pos = append(pos, args[0])
			args = args[1:]
		}
	}

	if len(pos) != positional {
		return nil, fmt.Errorf("%s: want %d argument(s), got %d", fs.Name(), positional, len(pos))
	}

	return pos, nil
}

func rawJSON(s string) (json.RawMessage, error) {
	if s == "" {
		return nil, nil
	}

	if !json.Valid([]byte(s)) {
		return nil, fmt.Errorf("invalid JSON: %s", s)
	}

	return json.RawMessage(s), nil
}

// ---------------------------------------------------------------------------
// Engine

func (a *app) engine(fs *flag.FlagSet, args []string) (*packtrail.Engine, error) {
	dir := fs.String("flows", "", "directory of flow definitions")
	parts := fs.Int("partitions", 0, "partition count of a new deployment")

	if _, err := parse(fs, args, 0); err != nil {
		return nil, err
	}

	opts := []packtrail.Option{packtrail.WithNamespace(a.ns)}
	if *dir != "" {
		opts = append(opts, packtrail.WithFlowsDir(*dir))
	}

	if *parts > 0 {
		opts = append(opts, packtrail.WithPartitions(*parts))
	}

	return packtrail.New(a.nc, opts...)
}

func (a *app) runEngine(ctx context.Context, args []string) error {
	eng, err := a.engine(flag.NewFlagSet("run", flag.ContinueOnError), args)
	if err != nil {
		return err
	}

	return eng.Run(ctx)
}

func (a *app) initEngine(ctx context.Context, args []string) error {
	eng, err := a.engine(flag.NewFlagSet("init", flag.ContinueOnError), args)
	if err != nil {
		return err
	}

	if err = eng.Init(ctx); err != nil {
		return err
	}

	return a.print(map[string]any{"namespace": a.ns, "partitions": eng.Partitions()})
}

func (a *app) rebuildIndex(ctx context.Context, _ []string) error {
	eng, err := packtrail.New(a.nc, packtrail.WithNamespace(a.ns))
	if err != nil {
		return err
	}

	n, err := eng.RebuildIndex(ctx)
	if err != nil {
		return err
	}

	return a.print(map[string]int{"indexed": n})
}

func (a *app) archive(ctx context.Context, args []string) error {
	pos, err := parse(flag.NewFlagSet("archive", flag.ContinueOnError), args, 1)
	if err != nil {
		return err
	}

	eng, err := packtrail.New(a.nc, packtrail.WithNamespace(a.ns))
	if err != nil {
		return err
	}

	if err = eng.Init(ctx); err != nil {
		return err
	}

	return eng.Archive(ctx, pos[0])
}

// ---------------------------------------------------------------------------
// Flows

func (a *app) validate(files []string) error {
	if len(files) == 0 {
		return errors.New("validate: no files")
	}

	var errs []error

	for _, f := range files {
		def, err := flow.ParseFile(f)
		if err != nil {
			errs = append(errs, err)

			continue
		}

		h, _ := def.Hash()
		if _, err = fmt.Fprintf(a.out, "%s: ok (%s@%s, start %s)\n", f, def.Name, h, def.StartNode()); err != nil {
			return err
		}
	}

	return errors.Join(errs...)
}

func (a *app) register(ctx context.Context, files []string) error {
	out := map[string]string{}

	for _, f := range files {
		def, err := flow.ParseFile(f)
		if err != nil {
			return err
		}

		h, err := a.client.Register(ctx, def)
		if err != nil {
			return err
		}

		out[def.Name] = h
	}

	return a.print(out)
}

func (a *app) flows(ctx context.Context, _ []string) error {
	vs, err := a.client.Flows(ctx)
	if err != nil {
		return err
	}

	return a.print(vs)
}

// ---------------------------------------------------------------------------
// Executions

func (a *app) start(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	input := fs.String("input", "", "input JSON object")
	id := fs.String("id", "", "execution id")
	version := fs.String("version", "", "flow version hash")
	wait := fs.Bool("wait", false, "wait for the result")

	pos, err := parse(fs, args, 1)
	if err != nil {
		return err
	}

	in, err := rawJSON(*input)
	if err != nil {
		return err
	}

	var opts []packtrail.StartOption
	if *id != "" {
		opts = append(opts, packtrail.WithExecutionID(*id))
	}

	if *version != "" {
		opts = append(opts, packtrail.WithVersion(*version))
	}

	execID, err := a.client.Start(ctx, pos[0], in, opts...)
	if err != nil {
		return err
	}

	if !*wait {
		return a.print(map[string]string{keyExecID: execID})
	}

	st, err := a.client.Wait(ctx, execID)
	if err != nil {
		return err
	}

	return a.print(st)
}

func (a *app) signal(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("signal", flag.ContinueOnError)
	payload := fs.String("payload", "", "payload JSON")
	id := fs.String("id", "", "signal id (idempotency)")

	pos, err := parse(fs, args, 2) //nolint:mnd // exec and name.
	if err != nil {
		return err
	}

	p, err := rawJSON(*payload)
	if err != nil {
		return err
	}

	var opts []packtrail.SignalOption
	if *id != "" {
		opts = append(opts, packtrail.WithSignalID(*id))
	}

	return a.client.Signal(ctx, pos[0], pos[1], p, opts...)
}

func (a *app) update(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("update", flag.ContinueOnError)
	writes := fs.String("writes", "", "channel writes JSON object (required)")
	id := fs.String("id", "", "update id (idempotency)")

	pos, err := parse(fs, args, 1)
	if err != nil {
		return err
	}

	w, err := writesJSON(*writes)
	if err != nil {
		return err
	}

	var opts []packtrail.UpdateOption
	if *id != "" {
		opts = append(opts, packtrail.WithUpdateID(*id))
	}

	st, err := a.client.Update(ctx, pos[0], w, opts...)
	if err != nil {
		return err
	}

	return a.print(st)
}

// writesJSON parses a JSON object of channel writes.
func writesJSON(s string) (map[string]any, error) {
	var w map[string]any
	if err := json.Unmarshal([]byte(s), &w); err != nil || len(w) == 0 {
		return nil, fmt.Errorf("-writes must be a non-empty JSON object: %q", s)
	}

	return w, nil
}

func (a *app) resume(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("resume", flag.ContinueOnError)
	value := fs.String("value", "", "resume value JSON")

	pos, err := parse(fs, args, 2) //nolint:mnd // exec and node.
	if err != nil {
		return err
	}

	v, err := rawJSON(*value)
	if err != nil {
		return err
	}

	return a.client.Resume(ctx, pos[0], pos[1], v)
}

func (a *app) cancel(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("cancel", flag.ContinueOnError)
	reason := fs.String("reason", "", "reason")

	pos, err := parse(fs, args, 1)
	if err != nil {
		return err
	}

	return a.client.Cancel(ctx, pos[0], *reason)
}

func (a *app) get(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	seq := fs.Uint64("seq", 0, "fold up to this event sequence")

	pos, err := parse(fs, args, 1)
	if err != nil {
		return err
	}

	st, err := a.client.StateAt(ctx, pos[0], *seq)
	if err != nil {
		return err
	}

	return a.print(st)
}

func (a *app) history(ctx context.Context, args []string) error {
	pos, err := parse(flag.NewFlagSet("history", flag.ContinueOnError), args, 1)
	if err != nil {
		return err
	}

	evs, err := a.client.History(ctx, pos[0])
	if err != nil {
		return err
	}

	for _, ev := range evs {
		if err = a.printEvent(ev.Seq, ev); err != nil {
			return err
		}
	}

	return nil
}

func (a *app) printEvent(seq uint64, ev any) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(a.out, "%d %s\n", seq, b)

	return err
}

func (a *app) watch(ctx context.Context, args []string) error {
	pos, err := parse(flag.NewFlagSet("watch", flag.ContinueOnError), args, 1)
	if err != nil {
		return err
	}

	ch, err := a.client.Watch(ctx, pos[0], 0)
	if err != nil {
		return err
	}

	for ev := range ch {
		if err = a.printEvent(ev.Seq, ev); err != nil {
			return err
		}
	}

	return nil
}

func (a *app) progress(ctx context.Context, args []string) error {
	pos, err := parse(flag.NewFlagSet("progress", flag.ContinueOnError), args, 1)
	if err != nil {
		return err
	}

	ch, err := a.client.Progress(ctx, pos[0])
	if err != nil {
		return err
	}

	enc := json.NewEncoder(a.out)

	for p := range ch {
		if err = enc.Encode(p); err != nil {
			return err
		}
	}

	return nil
}

func (a *app) wait(ctx context.Context, args []string) error {
	pos, err := parse(flag.NewFlagSet("wait", flag.ContinueOnError), args, 1)
	if err != nil {
		return err
	}

	st, err := a.client.Wait(ctx, pos[0])
	if err != nil {
		return err
	}

	return a.print(st)
}

func (a *app) fork(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("fork", flag.ContinueOnError)
	id := fs.String("id", "", "id of the new execution")
	writes := fs.String("writes", "", "channel writes JSON object, applied to the fork before it resumes")

	pos, err := parse(fs, args, 2) //nolint:mnd // exec and seq.
	if err != nil {
		return err
	}

	seq, err := strconv.ParseUint(pos[1], 10, 64)
	if err != nil {
		return fmt.Errorf("fork: invalid sequence %q", pos[1])
	}

	var opts []packtrail.ForkOption
	if *id != "" {
		opts = append(opts, packtrail.WithForkID(*id))
	}

	if *writes != "" {
		w, werr := writesJSON(*writes)
		if werr != nil {
			return werr
		}

		opts = append(opts, packtrail.WithForkWrites(w))
	}

	newID, err := a.client.Fork(ctx, pos[0], seq, opts...)
	if err != nil {
		return err
	}

	return a.print(map[string]string{keyExecID: newID})
}

func (a *app) rerun(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("rerun", flag.ContinueOnError)
	id := fs.String("id", "", "id of the new execution")

	pos, err := parse(fs, args, 2) //nolint:mnd // exec and node.
	if err != nil {
		return err
	}

	var opts []packtrail.ForkOption
	if *id != "" {
		opts = append(opts, packtrail.WithForkID(*id))
	}

	newID, err := a.client.Rerun(ctx, pos[0], pos[1], opts...)
	if err != nil {
		return err
	}

	return a.print(map[string]string{keyExecID: newID})
}

func (a *app) list(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	status := fs.String("status", "", "status")
	flowName := fs.String("flow", "", "flow name")
	attr := fs.String("attr", "", "search attribute KEY=VALUE")
	limit := fs.Int("limit", 50, "maximum results") //nolint:mnd // default page size.

	if _, err := parse(fs, args, 0); err != nil {
		return err
	}

	f := packtrail.ListFilter{Status: packtrail.Status(*status), Flow: *flowName, Limit: *limit}

	if *attr != "" {
		k, v, ok := strings.Cut(*attr, "=")
		if !ok {
			return errors.New("list: -attr wants KEY=VALUE")
		}

		f.Attr, f.Value = k, v
	}

	out, err := a.client.List(ctx, f)
	if err != nil {
		return err
	}

	return a.print(out)
}

// ---------------------------------------------------------------------------
// Schedules and dead letters

func (a *app) schedule(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("schedule", flag.ContinueOnError)
	input := fs.String("input", "", "input JSON object")
	tz := fs.String("tz", "", "IANA time zone")

	pos, err := parse(fs, args, 3) //nolint:mnd // name, flow and cron.
	if err != nil {
		return err
	}

	in, err := rawJSON(*input)
	if err != nil {
		return err
	}

	var opts []packtrail.ScheduleOption
	if *tz != "" {
		opts = append(opts, packtrail.ScheduleTimeZone(*tz))
	}

	return a.client.Schedule(ctx, pos[0], pos[1], pos[2], in, opts...)
}

func (a *app) unschedule(ctx context.Context, args []string) error {
	pos, err := parse(flag.NewFlagSet("unschedule", flag.ContinueOnError), args, 1)
	if err != nil {
		return err
	}

	return a.client.Unschedule(ctx, pos[0])
}

func (a *app) schedules(ctx context.Context, _ []string) error {
	out, err := a.client.Schedules(ctx)
	if err != nil {
		return err
	}

	return a.print(out)
}

func (a *app) dlq(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("dlq", flag.ContinueOnError)
	limit := fs.Int("limit", 20, "maximum results") //nolint:mnd // default page size.

	if _, err := parse(fs, args, 0); err != nil {
		return err
	}

	out, err := a.client.DeadLetters(ctx, *limit)
	if err != nil {
		return err
	}

	type row struct {
		packtrail.DeadLetter

		Seq uint64 `json:"seq"`
	}

	rows := make([]row, len(out))
	for i, d := range out {
		rows[i] = row{DeadLetter: d, Seq: d.Seq}
	}

	return a.print(rows)
}

func (a *app) redrive(ctx context.Context, args []string) error {
	pos, err := parse(flag.NewFlagSet("redrive", flag.ContinueOnError), args, 1)
	if err != nil {
		return err
	}

	seq, err := strconv.ParseUint(pos[0], 10, 64)
	if err != nil {
		return fmt.Errorf("redrive: invalid sequence %q", pos[0])
	}

	return a.client.Redrive(ctx, seq)
}

func (a *app) quarantined(ctx context.Context, _ []string) error {
	ids, err := a.client.Quarantined(ctx)
	if err != nil {
		return err
	}

	return a.print(ids)
}

func (a *app) unquarantine(ctx context.Context, args []string) error {
	pos, err := parse(flag.NewFlagSet("unquarantine", flag.ContinueOnError), args, 1)
	if err != nil {
		return err
	}

	return a.client.Unquarantine(ctx, pos[0])
}
