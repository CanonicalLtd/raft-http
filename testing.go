// Copyright 2017 Canonical Ltd.
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

package rafthttp

import (
	"log"
	"testing"

	"github.com/CanonicalLtd/raft-test"
)

// NewTestingHandler creates a Handler for testing purposes. It logs to the
// testing logger and serves requests.
func NewTestingHandler(t *testing.T) *Handler {
	logger := log.New(rafttest.TestingWriter(t), "", 0)
	return NewHandlerWithLogger(logger)
}
