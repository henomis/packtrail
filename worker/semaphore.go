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

package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/consume"
	"github.com/henomis/packtrail/internal/sched"
	"github.com/henomis/packtrail/internal/wire"
)

// Per-key concurrency (flow `concurrency: {key, max}`) is a counting
// semaphore in the sem bucket, updated with compare-and-swap. Holders carry
// an expiry so a crashed worker cannot leak a slot forever.

const (
	// Schedules have one-second precision: hand-backs wait 1s, 2s, 4s, 8s.
	busyDelayFirst = time.Second
	busyDelayCap   = 8 * time.Second
	busyMaxShift   = 3
	semCASAttempts = 16
	// semLeaseFactor: a holder's slot expires after this many ack waits
	// without renewal, so a crashed worker blocks its key only briefly.
	semLeaseFactor = 3
)

type semValue struct {
	Holders map[string]time.Time `json:"holders"`
}

func (w *Worker) ackWaitEff() time.Duration {
	if d := time.Duration(w.effAckWait.Load()); d > 0 {
		return d
	}

	return w.ackWait
}

// slot gets the job a slot of its per-key limit. Without a limit it returns
// at once. When the key is busy, one job per key per process waits for it
// (heartbeated, so it costs no delivery attempt); every other job for that key
// is handed back with a delay so it does not occupy a worker slot that jobs
// for other keys could use (F2-02). ok=false means the job was handed back.
func (w *Worker) slot(ctx context.Context, msg jetstream.Msg, wj wire.Job) (release func(), ok bool) {
	if wj.Concurrency == nil || wj.Concurrency.Max <= 0 {
		return func() {}, true
	}

	if rel, got := w.acquire(ctx, wj); got {
		return rel, true
	}

	key := wj.Concurrency.Key

	waiter := w.claimWaiter(key)
	if waiter {
		defer w.releaseWaiter(key)
	} else if w.handBack(ctx, msg, wj) {
		return nil, false
	}

	// The designated waiter — or any job whose hand-back could not be
	// recorded: a nak would then cost a delivery attempt, so it waits too.
	if rel, got := w.waitSlot(ctx, wj); got {
		return rel, true
	}

	if !w.handBack(ctx, msg, wj) { // shutting down and cannot hand back
		_ = msg.Nak()
	}

	return nil, false
}

// waitSlot blocks until the job gets its slot, or until the worker shuts down
// (ok=false): a job that is only waiting is handed back at once rather than
// holding up the drain (F2-06).
func (w *Worker) waitSlot(ctx context.Context, wj wire.Job) (release func(), ok bool) {
	delay := semRetryFirst
	stopping := consume.ShuttingDown(ctx)

	for {
		if rel, got := w.acquire(ctx, wj); got {
			return rel, true
		}

		select {
		case <-ctx.Done():
			return nil, false
		case <-stopping:
			return nil, false
		case <-time.After(delay):
		}

		delay = min(delay*2, semRetryCap) //nolint:mnd // exponential backoff.
	}
}

func (w *Worker) claimWaiter(key string) bool {
	w.waitMu.Lock()
	defer w.waitMu.Unlock()

	if w.waiting == nil {
		w.waiting = map[string]bool{}
	}

	if w.waiting[key] {
		return false
	}

	w.waiting[key] = true

	return true
}

func (w *Worker) releaseWaiter(key string) {
	w.waitMu.Lock()
	defer w.waitMu.Unlock()

	delete(w.waiting, key)
}

// handBack returns a job whose key is busy to the queue without spending a
// delivery attempt: it schedules a copy of the job back onto the work subject
// after a delay (a JetStream message schedule, so no process keeps time) and
// acks the original. The copy starts with a fresh delivery count; how many
// times it was handed back travels in the Pt-Busy header and drives the
// backoff. No counter is kept anywhere, so nothing can be lost or grow
// (F2-02, F3-02, F3-03). A crash between the schedule and the ack leaves two
// copies: at-least-once, and the second completion is stale.
// When the copy cannot be scheduled it returns false and the caller keeps the
// job.
func (w *Worker) handBack(ctx context.Context, msg jetstream.Msg, wj wire.Job) bool {
	n, _ := strconv.Atoi(msg.Headers().Get(HeaderBusy))
	n++

	delay := min(busyDelayFirst<<min(n-1, busyMaxShift), busyDelayCap)

	copyMsg := nats.NewMsg(w.in.Names.WorkDelaySubject(wj.Kind, busyID(wj)))
	copyMsg.Data = msg.Data()

	for k, v := range msg.Headers() {
		if !strings.HasPrefix(k, "Nats-") {
			copyMsg.Header[k] = v
		}
	}

	copyMsg.Header.Set(HeaderBusy, strconv.Itoa(n))
	copyMsg.Header.Set(sched.HeaderSchedule, sched.At(time.Now().Add(delay)))
	copyMsg.Header.Set(sched.HeaderScheduleTarget, w.in.Names.WorkSubject(wj.Kind))

	if _, err := w.in.JS.PublishMsg(ctx, copyMsg); err != nil {
		w.logger.Warn("worker: cannot hand a busy job back, keeping it", "exec", wj.ExecID, "err", err)

		return false
	}

	_ = msg.Ack()

	return true
}

// HeaderBusy counts how many times a job was handed back because its
// concurrency key was busy.
const HeaderBusy = "Pt-Busy"

func busyID(wj wire.Job) string {
	sum := sha256.Sum256([]byte(wire.JobMsgID(wj.ExecID, wj.Key, wj.Generation, wj.Attempt)))

	return hex.EncodeToString(sum[:12])
}

// acquire takes a slot for the job; ok=false means the key is saturated.
// While held, the slot lease is renewed every ack wait (F-05).
func (w *Worker) acquire(ctx context.Context, wj wire.Job) (release func(), ok bool) {
	if wj.Concurrency == nil || wj.Concurrency.Max <= 0 {
		return func() {}, true
	}

	kv, err := w.in.KV(ctx, w.in.Names.BucketSem)
	if err != nil {
		return nil, false
	}

	holder := wire.JobMsgID(wj.ExecID, wj.Key, wj.Generation, wj.Attempt)
	key := wj.Concurrency.Key
	lease := w.ackWaitEff() * semLeaseFactor

	granted := w.casSem(ctx, kv, key, func(v *semValue) bool {
		now := time.Now()
		for h, exp := range v.Holders {
			if now.After(exp) {
				delete(v.Holders, h)
			}
		}

		if _, held := v.Holders[holder]; !held && len(v.Holders) >= wj.Concurrency.Max {
			return false
		}

		v.Holders[holder] = now.Add(lease)

		return true
	})
	if !granted {
		return nil, false
	}

	done := make(chan struct{})

	go w.renewSlot(context.WithoutCancel(ctx), kv, key, holder, lease, done)

	return func() {
		close(done)
		w.casSem(context.WithoutCancel(ctx), kv, key, func(v *semValue) bool {
			delete(v.Holders, holder)

			return true
		})
	}, true
}

func (w *Worker) renewSlot(ctx context.Context, kv jetstream.KeyValue, key, holder string, lease time.Duration,
	done <-chan struct{},
) {
	defer func() {
		if p := recover(); p != nil {
			w.logger.Error("worker: semaphore renewal panic", "panic", p)
		}
	}()

	t := time.NewTicker(max(lease/semLeaseFactor, minAckWait/2)) //nolint:mnd // renew every ack wait.
	defer t.Stop()

	for {
		select {
		case <-done:
			return
		case <-t.C:
			w.casSem(ctx, kv, key, func(v *semValue) bool {
				if _, held := v.Holders[holder]; !held {
					return false
				}

				v.Holders[holder] = time.Now().Add(lease)

				return true
			})
		}
	}
}

// casSem applies fn to the semaphore value under compare-and-swap; it
// returns false when fn refuses or the update keeps conflicting.
func (w *Worker) casSem(ctx context.Context, kv jetstream.KeyValue, key string, fn func(*semValue) bool) bool {
	for range semCASAttempts {
		v := semValue{Holders: map[string]time.Time{}}

		var rev uint64

		e, err := kv.Get(ctx, key)

		switch {
		case err == nil:
			rev = e.Revision()
			_ = json.Unmarshal(e.Value(), &v)

			if v.Holders == nil {
				v.Holders = map[string]time.Time{}
			}
		case !errors.Is(err, jetstream.ErrKeyNotFound):
			return false
		}

		if !fn(&v) {
			return false
		}

		b, err := json.Marshal(v)
		if err != nil {
			return false
		}

		if rev == 0 {
			_, err = kv.Create(ctx, key, b)
		} else {
			_, err = kv.Update(ctx, key, b, rev)
		}

		if err == nil {
			return true
		}
	}

	return false
}
