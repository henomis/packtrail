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
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/henomis/packtrail"
)

func TestTimeoutOptionsValidate(t *testing.T) {
	nc := &nats.Conn{}

	bad := []packtrail.Option{
		packtrail.WithAckWait(500 * time.Millisecond), packtrail.WithPullExpiry(0),
		packtrail.WithReadTimeout(0), packtrail.WithBlobTimeout(-time.Second), packtrail.WithReplicas(0),
		packtrail.WithReplicas(7),
	}
	for i, o := range bad {
		if _, err := packtrail.New(nc, o); !errors.Is(err, packtrail.ErrInvalidArgument) {
			t.Errorf("option %d: err = %v", i, err)
		}
	}

	if _, err := packtrail.New(nc, packtrail.WithAckWait(10*time.Second), packtrail.WithPullExpiry(2*time.Second),
		packtrail.WithReadTimeout(time.Minute), packtrail.WithBlobTimeout(10*time.Minute)); err != nil {
		t.Fatal(err)
	}

	if _, err := packtrail.NewClient(nc, packtrail.WithClientReadTimeout(0)); !errors.Is(err,
		packtrail.ErrInvalidArgument) {
		t.Fatalf("client read timeout: %v", err)
	}
}
