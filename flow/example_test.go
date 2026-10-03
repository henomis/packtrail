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

package flow_test

import (
	"fmt"

	"github.com/henomis/packtrail/flow"
)

func ExampleParse() {
	def, err := flow.Parse([]byte(`
name: greet
channels: {log: {reducer: append}}
nodes:
  - {id: hello, type: task, kind: greeter, next: route}
  - id: route
    type: choice
    rules:
      - {when: "results.hello.polite", to: bye}
      - {default: true, to: hello}
  - {id: bye, type: task, kind: greeter}
start: hello
`))
	if err != nil {
		fmt.Println(err)

		return
	}

	fmt.Println(def.Name, def.StartNode(), def.Node("route").Type, def.Channels["log"].ReducerOrDefault())
	// Output: greet hello choice append
}

func ExampleValidationErrors() {
	_, err := flow.Parse([]byte(`
name: greet
nodes:
  - {id: hello, type: task, kind: greeter, retry: {max_attempts: 100}, next: bye}
  - {id: bye, type: task}
`))

	for _, e := range flow.ValidationErrors(err) {
		fmt.Printf("%s [%s] %s\n", e.Node, e.Field, e.Msg)
	}
	// Output:
	// hello [retry.max_attempts] must be between 0 and 64
	// bye [kind] invalid worker kind "": must match [A-Za-z0-9_-]{1,128}
}
