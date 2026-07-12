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

package dsl

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that unmarshals from a YAML string like "30s" or
// "24h". A zero/empty value unmarshals to 0.
type Duration time.Duration

// UnmarshalYAML decodes a YAML string like "30s" or "24h" into a Duration.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}

	if s == "" {
		*d = 0
		return nil
	}

	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}

	*d = Duration(parsed)

	return nil
}

// D returns the value as a time.Duration.
func (d *Duration) D() time.Duration { return time.Duration(*d) }

// Parse decodes and validates a Flow Definition from YAML bytes. Decoding is
// strict: an unknown field (a typo like "retires:" or "signal-name:") is an
// error rather than a silently dropped setting, and extra YAML documents after
// the first are rejected rather than silently ignored — one flow per document
// (one WithFlow per flow, or one file per flow in the flows directory).
func Parse(data []byte) (*Flow, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)

	var f Flow
	if err := dec.Decode(&f); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("yaml: empty flow definition")
		}

		return nil, fmt.Errorf("yaml: %w", err)
	}

	// Reject additional non-empty documents (a trailing "---" is fine).
	for {
		var extra any

		err := dec.Decode(&extra)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("yaml: %w", err)
		}

		if extra != nil {
			return nil, errors.New("yaml: multiple flow documents in one input; define one flow per document")
		}
	}

	if err := f.Validate(); err != nil {
		return nil, err
	}

	return &f, nil
}

// ParseFile reads and parses a Flow Definition file.
func ParseFile(path string) (*Flow, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path is caller-supplied by design
	if err != nil {
		return nil, err
	}

	f, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	return f, nil
}

// LoadDir parses every *.yaml / *.yml file in dir, keyed by flow name.
func LoadDir(dir string) (map[string]*Flow, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	flows := make(map[string]*Flow)

	for _, e := range entries {
		if e.IsDir() {
			continue
		}

		ext := filepath.Ext(e.Name())
		if ext != ".yaml" && ext != ".yml" {
			continue
		}

		f, parseErr := ParseFile(filepath.Join(dir, e.Name()))
		if parseErr != nil {
			return nil, parseErr
		}

		if _, dup := flows[f.Name]; dup {
			return nil, fmt.Errorf("duplicate flow name %q", f.Name)
		}

		flows[f.Name] = f
	}

	return flows, nil
}
