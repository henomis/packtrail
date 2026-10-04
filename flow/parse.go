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

package flow

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// Parse decodes and validates a flow definition from YAML. Decoding is strict:
// an unknown field (a typo like "retires:") is an error rather than a silently
// dropped setting, and extra YAML documents are rejected.
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

	f.normalize()

	if err := f.Validate(); err != nil {
		return nil, err
	}

	return &f, nil
}

// ParseJSON decodes and validates a flow definition from its JSON encoding
// (the form stored in the flows bucket). Unknown fields are rejected.
func ParseJSON(data []byte) (*Flow, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	// Keep numbers as written: meta is handed to workers and hashed, and a
	// float64 would round integers above 2^53.
	dec.UseNumber()

	var f Flow
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("json: %w", err)
	}

	f.normalize()

	if err := f.Validate(); err != nil {
		return nil, err
	}

	return &f, nil
}

// normalize converts YAML-decoded values into their JSON shapes so that a flow
// parsed from YAML and the same flow parsed from JSON hash equally.
func (f *Flow) normalize() {
	for i := range f.Nodes {
		if f.Nodes[i].OutputSchema != nil {
			f.Nodes[i].OutputSchema = jsonShape(f.Nodes[i].OutputSchema)
		}
	}

	for k, c := range f.Channels {
		if c.Default != nil {
			c.Default = jsonShape(c.Default)
			f.Channels[k] = c
		}
	}
}

func jsonShape(v any) any {
	b, err := json.Marshal(v)
	if err != nil {
		return v
	}

	var out any
	if err = json.Unmarshal(b, &out); err != nil {
		return v
	}

	return out
}

// ParseFile reads and parses a flow definition file.
func ParseFile(path string) (*Flow, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path is caller-supplied by design.
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
		ext := filepath.Ext(e.Name())
		if e.IsDir() || (ext != ".yaml" && ext != ".yml") {
			continue
		}

		f, lerr := ParseFile(filepath.Join(dir, e.Name()))
		if lerr != nil {
			return nil, lerr
		}

		if _, dup := flows[f.Name]; dup {
			return nil, fmt.Errorf("duplicate flow name %q", f.Name)
		}

		flows[f.Name] = f
	}

	return flows, nil
}
