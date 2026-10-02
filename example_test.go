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

package packtrail_test

import (
	"fmt"

	"github.com/henomis/packtrail"
)

func ExampleNew() {
	// New only validates options; Init (or Run) provisions the namespace.
	_, err := packtrail.New(nil)
	fmt.Println(err != nil)

	_, err = packtrail.New(nil, packtrail.WithNamespace("bad.name"))
	fmt.Println(err != nil)
	// Output:
	// true
	// true
}

func ExampleValidateOptions() {
	// Check a configuration without a NATS connection, e.g. in a CI lint step.
	err := packtrail.ValidateOptions(
		packtrail.WithNamespace("tenant-a"),
		packtrail.WithSchedule("nightly", "report", "0 0 3 * * *", nil),
	)
	fmt.Println(err)

	err = packtrail.ValidateOptions(packtrail.WithSchedule("nightly", "report", "0 3 * * *", nil))
	fmt.Println(err != nil)
	// Output:
	// <nil>
	// true
}
