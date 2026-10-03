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

// Package ready reports whether a process's consumer loops are all pulling,
// for the run in progress: a readiness probe must not see a run that ended.
package ready

import "sync"

// Signal is the readiness of one process across its runs.
type Signal struct {
	mu   sync.Mutex
	ch   chan struct{}
	up   bool   // ch is closed
	run  uint64 // the current run; callbacks of other runs are ignored
	left int    // parts of the current run not pulling yet
}

// New returns a signal that is not ready.
func New() *Signal { return &Signal{ch: make(chan struct{})} }

// C returns a channel closed while the current run is ready. Once a run ends,
// C returns a new, open channel.
func (s *Signal) C() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.ch
}

// Begin starts a run of n parts. It returns one callback per part, to call
// when that part pulls (calling it again counts once), and end, to call when
// the run returns. With no parts the run is ready at once.
func (s *Signal) Begin(n int) (parts []func(), end func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.reset()
	s.run++
	s.left = n

	run := s.run

	if n == 0 {
		s.open()
	}

	parts = make([]func(), n)

	for i := range parts {
		called := false

		parts[i] = func() {
			s.mu.Lock()
			defer s.mu.Unlock()

			if s.run != run || called {
				return
			}

			called = true
			s.left--

			if s.left == 0 {
				s.open()
			}
		}
	}

	end = func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		if s.run != run {
			return
		}

		s.run++
		s.reset()
	}

	return parts, end
}

// open closes the channel: ready.
func (s *Signal) open() {
	if !s.up {
		s.up = true
		close(s.ch)
	}
}

// reset replaces a closed channel with an open one: not ready.
func (s *Signal) reset() {
	if s.up {
		s.up = false
		s.ch = make(chan struct{})
	}
}
