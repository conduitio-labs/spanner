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
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/spanner"
	"github.com/conduitio/conduit-commons/opencdc"
)

type ColumnTypes map[string]string

// Writer implements a writer logic for Spanner destination.
type Writer struct {
	client *spanner.Client
	// Function to dynamically get table name for each record.
	tableNameFunc TableFn
	// Maps table names to their column types.
	columnTypes map[string]ColumnTypes
	// Schema name to write data into.
	schema string
}

// NewWriter creates new instance of the Writer.
func NewWriter(_ context.Context, client *spanner.Client, config Config) (*Writer, error) {
	tableFn, err := config.TableFunction()
	if err != nil {
		return nil, fmt.Errorf("invalid table name or table function: %w", err)
	}

	writer := &Writer{
		client:        client,
		tableNameFunc: tableFn,
		schema:        config.Schema,
		columnTypes:   make(map[string]ColumnTypes),
	}

	return writer, nil
}

// Insert inserts a record.
func (w *Writer) Insert(ctx context.Context, record opencdc.Record) error {
	table, err := w.tableNameFunc(record)
	if err != nil {
		return err
	}

	payload, err := w.structurizeData(record.Payload.After)
	if err != nil {
		return fmt.Errorf("structurize payload: %w", err)
	}
	if len(payload) == 0 {
		return ErrNoPayload
	}

	keys := []string{}
	values := []interface{}{}

	for k, v := range payload {
		keys = append(keys, k)
		values = append(values, v)
	}

	mutation := spanner.Insert(table, keys, values)
	if _, err := w.client.Apply(ctx, []*spanner.Mutation{mutation}); err != nil {
		return fmt.Errorf("error create record: %w", err)
	}
	return nil
}

// Update updates a record.
func (w *Writer) Update(ctx context.Context, record opencdc.Record) error {
	table, err := w.tableNameFunc(record)
	if err != nil {
		return err
	}

	payload, err := w.structurizeData(record.Payload.After)
	if err != nil {
		return fmt.Errorf("structurize payload: %w", err)
	}
	if len(payload) == 0 {
		return ErrNoPayload
	}

	keys := []string{}
	values := []interface{}{}

	for k, v := range payload {
		keys = append(keys, k)
		values = append(values, v)
	}

	mutation := spanner.Update(table, keys, values)
	if _, err := w.client.Apply(ctx, []*spanner.Mutation{mutation}); err != nil {
		return fmt.Errorf("error update record: %w", err)
	}
	return nil
}

// Delete deletes a record.
func (w *Writer) Delete(ctx context.Context, record opencdc.Record) error {
	table, err := w.tableNameFunc(record)
	if err != nil {
		return err
	}

	keyMap, err := w.structurizeData(record.Key)
	if err != nil {
		return fmt.Errorf("structurize key: %w", err)
	}

	if len(keyMap) > 1 {
		return ErrCompositeKeysNotSupported
	}

	columnTypes, err := w.getColumnTypes(ctx, table)
	if err != nil {
		return err
	}

	var key interface{}
	for k, v := range keyMap {
		switch v := v.(type) {
		case float64:
			cType, ok := columnTypes[k]
			if !ok {
				return NewColumnNotFoundError(table, k)
			}
			if cType == "INT64" {
				key = int64(v)
			} else {
				key = v
			}

		default:
			key = v
		}
	}

	mutation := spanner.Delete(table, spanner.Key{key})
	if _, err := w.client.Apply(ctx, []*spanner.Mutation{mutation}); err != nil {
		return fmt.Errorf("error delete record: %w", err)
	}

	return nil
}

// Stop closes database connection.
func (w *Writer) Stop() error {
	if w.client != nil {
		w.client.Close()
	}
	return nil
}

// structurizeData converts opencdc.Data to opencdc.StructuredData.
func (w *Writer) structurizeData(data opencdc.Data) (opencdc.StructuredData, error) {
	if data == nil || len(data.Bytes()) == 0 {
		return opencdc.StructuredData{}, nil
	}

	structuredData := make(opencdc.StructuredData)
	if err := json.Unmarshal(data.Bytes(), &structuredData); err != nil {
		return nil, fmt.Errorf("unmarshal data into structured data: %w", err)
	}

	return structuredData, nil
}

// getColumnTypes returns column types from w.columnTypes, or queries the database if not exists.
func (w *Writer) getColumnTypes(ctx context.Context, table string) (map[string]string, error) {
	if columnTypes, ok := w.columnTypes[table]; ok {
		return columnTypes, nil
	}

	query := fmt.Sprintf(`
		SELECT column_name, spanner_type
		FROM information_schema.columns
		WHERE table_name = '%s' AND table_schema = '%s'`, table, w.schema)

	iter := w.client.Single().Query(ctx, spanner.NewStatement(query))
	defer iter.Stop()

	column := 0
	columnTypes := make(ColumnTypes)

	for {
		row, err := iter.Next()
		if err != nil {
			if err.Error() == "no more items in iterator" {
				if column == 0 {
					return nil, ErrNoColumnsFound
				}
				break
			}
			return nil, fmt.Errorf("failed to fetch column details: %w", err)
		}

		var columnName, columnType string
		if err := row.Columns(&columnName, &columnType); err != nil {
			return nil, fmt.Errorf("failed to read column details: %w", err)
		}

		columnTypes[columnName] = columnType
		column++
	}

	w.columnTypes[table] = columnTypes
	return columnTypes, nil
}
