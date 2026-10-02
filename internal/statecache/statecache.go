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

// Package statecache is a bounded in-memory cache of folded execution states.
package statecache

import (
	"sync"

	"github.com/henomis/packtrail/flow"
	"github.com/henomis/packtrail/internal/fold"
)

// DefaultSize is the default number of cached executions.
const DefaultSize = 10000

// Entry is one cached state with its definition.
type Entry struct {
	State *fold.State
	Def   *flow.Flow
}

// Cache maps execution ids to states. When full, an arbitrary entry is
// evicted: a miss only costs a reload.
type Cache struct {
	mu   sync.Mutex
	size int
	m    map[string]Entry
}

// New returns a cache bounded to size entries.
func New(size int) *Cache {
	if size <= 0 {
		size = DefaultSize
	}

	return &Cache{size: size, m: map[string]Entry{}}
}

// Get returns the cached entry of execID.
func (c *Cache) Get(execID string) (Entry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.m[execID]

	return e, ok
}

// Put stores an entry.
func (c *Cache) Put(execID string, e Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.m[execID]; !ok && len(c.m) >= c.size {
		for k := range c.m {
			delete(c.m, k)

			break
		}
	}

	c.m[execID] = e
}

// Drop removes execID.
func (c *Cache) Drop(execID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.m, execID)
}
