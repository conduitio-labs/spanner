// Copyright © 2025 Meroxa, Inc.
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

package spanner

import (
	"github.com/conduitio-labs/conduit-connector-spanner/common"
)

//go:generate paramgen -output=paramgen_src.go SourceConfig

type SourceConfig struct {
	common.Config
	// Tables represents the spanner tables to read from.
	Tables []string `json:"tables" validate:"required"`
}
