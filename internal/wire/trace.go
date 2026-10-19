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

package wire

import "strings"

// Lengths of the hex ids of a traceparent.
const (
	traceIDLen = 32
	spanIDLen  = 16
)

// TraceIDs returns the trace id and the parent (span) id of a W3C
// traceparent ("vv-<32 hex>-<16 hex>-<2 hex>"), for log keys. ok is false for
// an empty or malformed value, or one whose ids are all zero.
func TraceIDs(traceparent string) (traceID, spanID string, ok bool) {
	parts := strings.Split(traceparent, "-")
	//nolint:mnd // version, trace id, parent id, flags.
	if len(parts) < 4 || len(parts[0]) != 2 || parts[0] == "ff" || len(parts[3]) != 2 {
		return "", "", false
	}

	if parts[0] == "00" && len(parts) != 4 {
		return "", "", false
	}

	traceID, spanID = parts[1], parts[2]
	if !lowerHex(parts[0]) || !lowerHex(parts[3]) || !nonZeroHex(traceID, traceIDLen) || !nonZeroHex(spanID, spanIDLen) {
		return "", "", false
	}

	return traceID, spanID, true
}

func lowerHex(s string) bool {
	for i := range len(s) {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}

	return true
}

func nonZeroHex(s string, n int) bool {
	return len(s) == n && lowerHex(s) && strings.Trim(s, "0") != ""
}
