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

package packtrail

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/names"
)

// Namespaces lists the namespaces provisioned on the NATS account that nc
// is connected to, sorted. A prefix p is a namespace when both its
// p-events and p-cmd streams exist.
func Namespaces(ctx context.Context, nc *nats.Conn) ([]string, error) {
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
