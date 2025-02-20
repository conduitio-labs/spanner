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
	"math/big"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/conduitio-labs/conduit-connector-spanner/common"
	"github.com/conduitio/conduit-commons/csync"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"google.golang.org/api/iterator"
	"gopkg.in/tomb.v2"
)

type (
	snapshotIterator struct {
		t            *tomb.Tomb
		acks         csync.WaitGroup
		lastPosition common.SnapshotPosition
		config       snapshotIteratorConfig
		dataC        chan fetchData
	}
	snapshotIteratorConfig struct {
		tableKeys common.TableKeys
		client    *spanner.Client
		position  *common.SnapshotPosition
	}
	fetchData struct {
		payload        opencdc.StructuredData
		table          common.TableName
		primaryKeyName common.PrimaryKeyName
		primaryKeyVal  any
		position       common.TablePosition
	}
)

var _ common.Iterator = new(snapshotIterator)

var (
	ErrSnapshotIteratorDone  = errors.New("snapshot complete")
	ErrPrimaryKeyNotFound    = errors.New("primary key not found")
	ErrUnsupportedColumnType = errors.New("unsupported column type")
)

func newSnapshotIterator(ctx context.Context, config snapshotIteratorConfig) *snapshotIterator {
	lastPosition := common.SnapshotPosition{
		Snapshots: map[common.TableName]common.TablePosition{},
	}
	if config.position != nil {
		lastPosition = *config.position
	}

	t, _ := tomb.WithContext(ctx)
	iterator := &snapshotIterator{
		t:            t,
		acks:         csync.WaitGroup{},
		config:       config,
		dataC:        make(chan fetchData),
		lastPosition: lastPosition,
	}

	for tableName, primaryKey := range config.tableKeys {
		iterator.t.Go(func() error {
			ctx := iterator.t.Context(ctx)
			return iterator.fetchTable(ctx, tableName, primaryKey)
		})
	}

	return iterator
}

func (s *snapshotIterator) Read(ctx context.Context) (opencdc.Record, error) {
	select {
	case <-ctx.Done():
		return opencdc.Record{}, ctx.Err()
	case <-s.t.Dead():
		if err := s.t.Err(); err != nil && !errors.Is(err, ErrSnapshotIteratorDone) {
			return opencdc.Record{}, fmt.Errorf(
				"cannot stop snapshot mode, fetchers exited unexpectedly: %w", err)
		}
		if err := s.acks.Wait(ctx); err != nil {
			return opencdc.Record{}, fmt.Errorf("failed to wait for acks on snapshot iterator done: %w", err)
		}

		return opencdc.Record{}, ErrSnapshotIteratorDone
	case data := <-s.dataC:
		sdk.Logger(ctx).Trace().Msg("received data from fetcher")
		s.acks.Add(1)
		return s.buildRecord(data), nil
	}
}

func (s *snapshotIterator) Ack(context.Context, opencdc.Position) error {
	s.acks.Done()
	return nil
}

func (s *snapshotIterator) Teardown(ctx context.Context) error {
	s.t.Kill(ErrSnapshotIteratorDone)
	if err := s.t.Err(); err != nil && !errors.Is(err, ErrSnapshotIteratorDone) {
		return fmt.Errorf(
			"cannot teardown snapshot mode, fetchers exited unexpectedly: %w", err)
	}

	sdk.Logger(ctx).Info().Msg("killed all workers, waiting for acks")

	if err := s.acks.Wait(ctx); err != nil {
		return fmt.Errorf("failed to wait for snapshot acks: %w", err)
	}

	sdk.Logger(ctx).Info().Msg("all acks received")

	// waiting for the workers to finish will allow us to have an easier time
	// debugging goroutine leaks.
	_ = s.t.Wait()

	sdk.Logger(ctx).Info().Msg("all workers done, teared down snapshot iterator")

	return nil
}

func snapshotQueryIterator(
	ctx context.Context, tx *spanner.ReadOnlyTransaction,
	tableName common.TableName, primaryKey common.PrimaryKeyName,
	start, end int64,
) (*spanner.RowIterator, func()) {
	query := fmt.Sprint(`
		SELECT *
		FROM `, tableName, `
		WHERE `, primaryKey, ` >= @start AND `, primaryKey, ` <= @end
		ORDER BY `, primaryKey)
	stmt := spanner.Statement{
		SQL: query,
		Params: map[string]any{
			"start": start,
			"end":   end,
		},
	}
	iter := tx.Query(ctx, stmt)
	return iter, iter.Stop
}

func (s *snapshotIterator) fetchTable(
	ctx context.Context,
	tableName common.TableName,
	primaryKey common.PrimaryKeyName,
) error {
	tx := s.config.client.ReadOnlyTransaction()
	defer tx.Close()

	start, end, err := s.fetchStartEnd(ctx, tx, tableName)
	if err != nil {
		return fmt.Errorf("failed to fetch start and end of snapshot: %w", err)
	}

	iter, stopIter := snapshotQueryIterator(ctx, tx, tableName, primaryKey, start, end)
	defer stopIter()

	sdk.Logger(ctx).Debug().Msgf("starting fetching rows from table %s", tableName)
	defer sdk.Logger(ctx).Debug().Msgf("finished fetching rows for table %s", tableName)

	for ; ; start++ {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		} else if err != nil {
			return fmt.Errorf("failed to fetch next row: %w", err)
		}

		data, err := buildFetchData(ctx, row, tableName, primaryKey, start, end)
		if err != nil {
			return fmt.Errorf("failed to build fetch data: %w", err)
		}

		select {
		case s.dataC <- data:
		case <-s.t.Dead():
			//nolint:wrapcheck // context error, no need to wrap
			return s.t.Err()
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func buildFetchData(
	ctx context.Context,
	row *spanner.Row,
	tableName common.TableName, primaryKey common.PrimaryKeyName,
	start, end int64,
) (fetchData, error) {
	decodedRow, err := decodeRow(ctx, row)
	if err != nil {
		return fetchData{}, fmt.Errorf("failed to decode row: %w", err)
	}

	primaryKeyVal, ok := decodedRow[string(primaryKey)]
	if !ok {
		sdk.Logger(ctx).Warn().Msgf("primary key %s not found in row %v", primaryKey, decodedRow)
		return fetchData{}, ErrPrimaryKeyNotFound
	}

	return fetchData{
		payload:        decodedRow,
		table:          tableName,
		primaryKeyName: primaryKey,
		primaryKeyVal:  primaryKeyVal,
		position: common.TablePosition{
			LastRead:    start + 1,
			SnapshotEnd: end,
		},
	}, nil
}

func (s *snapshotIterator) fetchStartEnd(
	ctx context.Context,
	tx *spanner.ReadOnlyTransaction,
	tableName common.TableName,
) (start, end int64, err error) {
	var result struct {
		Min int64 `spanner:"min"`
		Max int64 `spanner:"max"`
	}
	col := s.config.tableKeys[tableName]
	stmt := spanner.Statement{SQL: fmt.Sprintf(
		"SELECT MIN(%s) AS min, MAX(%s) AS max FROM %s",
		col, col, tableName,
	)}

	iter := tx.Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to fetch row: %w", err)
	}

	if err := row.ToStruct(&result); err != nil {
		return 0, 0, fmt.Errorf("failed to decode row: %w", err)
	}

	lastRead := s.lastPosition.Snapshots[tableName].LastRead
	if lastRead > result.Min {
		// last read takes preference, as previous records where already fetched
		start = lastRead
	} else {
		start = result.Min
	}

	end = result.Max

	return start, end, nil
}

func (s *snapshotIterator) buildRecord(data fetchData) opencdc.Record {
	s.lastPosition.Snapshots[data.table] = data.position
	position := s.lastPosition.ToSDKPosition()

	metadata := opencdc.Metadata{}
	metadata.SetCollection(string(data.table))

	key := opencdc.StructuredData{
		string(data.primaryKeyName): data.payload[string(data.primaryKeyName)],
	}

	payload := opencdc.StructuredData{}
	for key, val := range data.payload {
		payload[key] = common.FormatValue(val)
	}

	return sdk.Util.Source.NewRecordSnapshot(position, metadata, key, payload)
}

func handleSpannerTypeCode[T any](
	row *spanner.Row,
	index int,
) (any, error) {
	var val T
	if err := row.Column(index, &val); err != nil {
		return nil, fmt.Errorf("failed to decode value: %w", err)
	}

	return val, nil
}

//nolint:funlen // already very concise
func decodeRow(ctx context.Context, row *spanner.Row) (opencdc.StructuredData, error) {
	data := make(opencdc.StructuredData)
	for i, column := range row.ColumnNames() {
		var val any
		var err error
		switch code := row.ColumnType(i).Code; code {
		case spannerpb.TypeCode_BOOL:
			val, err = handleSpannerTypeCode[bool](row, i)
		case spannerpb.TypeCode_INT64:
			val, err = handleSpannerTypeCode[int64](row, i)
		case spannerpb.TypeCode_FLOAT32:
			val, err = handleSpannerTypeCode[float32](row, i)
		case spannerpb.TypeCode_FLOAT64:
			val, err = handleSpannerTypeCode[float64](row, i)
		case spannerpb.TypeCode_TIMESTAMP:
			val, err = handleSpannerTypeCode[time.Time](row, i)
		case spannerpb.TypeCode_DATE:
			val, err = handleSpannerTypeCode[civil.Date](row, i)
		case spannerpb.TypeCode_STRING, spannerpb.TypeCode_ENUM:
			val, err = handleSpannerTypeCode[string](row, i)
		case spannerpb.TypeCode_BYTES, spannerpb.TypeCode_PROTO:
			val, err = handleSpannerTypeCode[[]byte](row, i)
		case spannerpb.TypeCode_ARRAY:
			val, err = handleSpannerTypeCode[any](row, i)
		case spannerpb.TypeCode_STRUCT:
			val, err = handleSpannerTypeCode[[]spanner.GenericColumnValue](row, i)
		case spannerpb.TypeCode_NUMERIC:
			val, err = handleSpannerTypeCode[*big.Rat](row, i)
		case spannerpb.TypeCode_JSON:
			val, err = handleSpannerTypeCode[json.RawMessage](row, i)
		case spannerpb.TypeCode_INTERVAL:
			val, err = handleSpannerTypeCode[string](row, i)
		case spannerpb.TypeCode_TYPE_CODE_UNSPECIFIED:
			val, err = handleSpannerTypeCode[any](row, i)
			sdk.Logger(ctx).Warn().Msgf("column %s has unspecified type %v for value %v", column, code, val)
		default:
			sdk.Logger(ctx).Warn().Msgf("unidentified type %v for column %v", code, column)
			return nil, ErrUnsupportedColumnType
		}
		if err != nil {
			return nil, fmt.Errorf("failed to decode column %s: %w", column, err)
		}
		data[column] = val
	}
	return data, nil
}
