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

package ready

import (
	"sync"
	"testing"
)

func isReady(s *Signal) bool {
	select {
	case <-s.C():
		return true
	default:
		return false
	}
}

func TestSignalCountsEachPartOnce(t *testing.T) {
	s := New()

	parts, end := s.Begin(2)

	parts[0]()
	parts[0]()

	if isReady(s) {
		t.Fatal("ready with one part of two pulling")
	}

	held := s.C()

	parts[1]()

	if !isReady(s) {
		t.Fatal("not ready with every part pulling")
	}

	select {
	case <-held:
	default:
		t.Fatal("a channel taken before ready did not close")
	}

	end()

	if isReady(s) {
		t.Fatal("ready after the run ended")
	}

	// Late callbacks of an ended run change nothing.
	parts[1]()
	end()

	if isReady(s) {
		t.Fatal("a late callback made an ended run ready")
	}
}

func TestSignalRuns(t *testing.T) {
	s := New()

	first, endFirst := s.Begin(1)
	first[0]()

	// A second run while the first is ready starts not ready, and the first
	// run's callbacks and end do not touch it.
	second, endSecond := s.Begin(1)
	if isReady(s) {
		t.Fatal("a new run starts ready")
	}

	first[0]()
	endFirst()

	if isReady(s) {
		t.Fatal("the old run's callback counted")
	}

	second[0]()

	if !isReady(s) {
		t.Fatal("second run not ready")
	}

	endFirst()

	if !isReady(s) {
		t.Fatal("the old run's end reset the current run")
	}

	endSecond()

	if isReady(s) {
		t.Fatal("ready after the run ended")
	}

	_, endEmpty := s.Begin(0)
	if !isReady(s) {
		t.Fatal("a run with no parts is not ready")
	}

	endEmpty()
}

func TestSignalConcurrentParts(t *testing.T) {
	s := New()

	const n = 64

	parts, end := s.Begin(n)
	defer end()

	var wg sync.WaitGroup

	for _, p := range parts {
		for range 3 {
			wg.Go(p)
		}

		wg.Go(func() { _ = isReady(s) })
	}

	wg.Wait()

	if !isReady(s) {
		t.Fatal("not ready after every part pulled")
	}
}
