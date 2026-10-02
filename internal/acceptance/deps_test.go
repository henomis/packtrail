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

package acceptance

import (
	"os"
	"strings"
	"testing"
)

// TestNoForbiddenDependencies keeps the module lean: NATS, expr-lang, yaml
// and a JSON Schema validator — nothing else, and nothing AI-specific.
func TestNoForbiddenDependencies(t *testing.T) {
	data, err := os.ReadFile("../../go.mod")
	if err != nil {
		t.Fatal(err)
	}

	allowed := []string{
		"github.com/nats-io/", "github.com/expr-lang/expr", "gopkg.in/yaml.v3",
		"github.com/santhosh-tekuri/jsonschema", "github.com/klauspost/compress", "github.com/minio/highwayhash",
		"github.com/google/go-tpm", "github.com/antithesishq/", "golang.org/x/", "github.com/nats-io/nuid",
	}

	inRequire := false

	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)

		switch {
		case strings.HasPrefix(line, "require ("):
			inRequire = true

			continue
		case line == ")":
			inRequire = false

			continue
		}

		if !inRequire || line == "" {
			continue
		}

		mod := strings.Fields(line)[0]
		ok := false

		for _, a := range allowed {
			if strings.HasPrefix(mod, a) {
				ok = true
			}
		}

		if !ok {
			t.Errorf("unexpected dependency %s", mod)
		}
	}
}
