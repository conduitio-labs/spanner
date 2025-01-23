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
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"cloud.google.com/go/spanner"
	"github.com/conduitio/conduit-commons/opencdc"
)

// ErrNoPayload occurs when there's no payload to insert or update.
var ErrNoPayload = errors.New("no payload")

// Writer implements a writer logic for Spanner destination.
type Writer struct {
	client *spanner.Client
	// Function to dynamically get table name for each record.
	tableNameFunc TableFn
}

// NewWriter creates new instance of the Writer.
func NewWriter(ctx context.Context, client *spanner.Client, config DestinationConfig) (*Writer, error) {
	tableFn, err := config.TableFunction()
	if err != nil {
		return nil, fmt.Errorf("invalid table name or table function: %w", err)
	}

	writer := &Writer{
		client:        client,
		tableNameFunc: tableFn,
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
	if payload == nil {
		return ErrNoPayload
	}

	var keys []string
	var values []interface{}

	for k, v := range payload {
		keys = append(keys, k)
		values = append(values, v)
	}

	mutation := spanner.Insert(table, keys, values)
	if _, err := w.client.Apply(ctx, []*spanner.Mutation{mutation}); err != nil {
		return fmt.Errorf("create: %w", err)
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
	if payload == nil {
		return ErrNoPayload
	}

	var keys []string
	var values []interface{}

	for k, v := range payload {
		keys = append(keys, k)
		values = append(values, v)
	}

	mutation := spanner.Insert(table, keys, values)
	if _, err := w.client.Apply(ctx, []*spanner.Mutation{mutation}); err != nil {
		return fmt.Errorf("create: %w", err)
	}
	return nil
}

// Delete deletes a record.
func (w *Writer) Delete(ctx context.Context, record opencdc.Record) error {
	table, err := w.tableNameFunc(record)
	if err != nil {
		return err
	}

	key, err := w.structurizeData(record.Key)
	if err != nil {
		return fmt.Errorf("structurize key: %w", err)
	}

	mutation := spanner.Delete(table, spanner.Key{key})
	if _, err := w.client.Apply(ctx, []*spanner.Mutation{mutation}); err != nil {
		return fmt.Errorf("delete: %w", err)
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
