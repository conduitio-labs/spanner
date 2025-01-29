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

package destination

import (
	"errors"
	"fmt"
)

var (
	// ErrNoPayload occurs when there's no payload to insert or update.
	ErrNoPayload = errors.New("no payload")
	// ErrCompositeKeysNotSupported occurs when there are more than one key in a Key map.
	ErrCompositeKeysNotSupported = errors.New("composite keys not yet supported")
	// ErrNoColumnsFound happens when table name or schema is incorrect.
	ErrNoColumnsFound = errors.New("no columns found for table please check the table name and schema")
)

type ColumnNotFoundError struct {
	table, column string
}

func (e ColumnNotFoundError) Error() string {
	return fmt.Sprintf("column %s does not exists in table %s", e.column, e.table)
}

func NewColumnNotFoundError(table, column string) ColumnNotFoundError {
	return ColumnNotFoundError{table, column}
}
