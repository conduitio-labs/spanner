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

var ErrSnapshotIteratorDone = errors.New("snapshot complete")

func newSnapshotIterator(ctx context.Context, config snapshotIteratorConfig) *snapshotIterator {
	t, _ := tomb.WithContext(ctx)
	iterator := &snapshotIterator{
		t:      t,
		acks:   csync.WaitGroup{},
		config: config,
		dataC:  make(chan fetchData),
		lastPosition: common.SnapshotPosition{
			Snapshots: map[common.TableName]common.TablePosition{},
		},
	}

	for tableName, primaryKey := range config.tableKeys {
		iterator.t.Go(func() error {
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

	if err := s.acks.Wait(ctx); err != nil {
		return fmt.Errorf("failed to wait for snapshot acks: %w", err)
	}

	// waiting for the workers to finish will allow us to have an easier time
	// debugging goroutine leaks.
	_ = s.t.Wait()

	sdk.Logger(ctx).Info().Msg("all workers done, teared down snapshot iterator")

	return nil
}

func (s *snapshotIterator) fetchTable(
	ctx context.Context,
	tableName common.TableName,
	primaryKey common.PrimaryKeyName,
) error {
	ro := s.config.client.ReadOnlyTransaction()
	defer ro.Close()

	start, end, err := s.fetchStartEnd(ctx, ro, tableName)
	if err != nil {
		return fmt.Errorf("failed to fetch start and end of snapshot: %w", err)
	}

	query := fmt.Sprint(`
		SELECT *
		FROM `, tableName, `
		WHERE `, primaryKey, ` > @start AND `, primaryKey, ` <= @end
		ORDER BY `, primaryKey)
	stmt := spanner.Statement{
		SQL: query,
		Params: map[string]any{
			"start": start,
			"end":   end,
		},
	}
	iter := ro.Query(ctx, stmt)
	defer iter.Stop()

	for ; ; start++ {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to fetch next row: %w", err)
		}

		data, err := decodeRow(row)
		if err != nil {
			return fmt.Errorf("failed to decode row: %w", err)
		}

		primaryKeyVal, ok := data[string(primaryKey)]
		if !ok {
			return fmt.Errorf("primary key %s not found in row %v", primaryKey, data)
		}

		s.dataC <- fetchData{
			payload:        data,
			table:          tableName,
			primaryKeyName: primaryKey,
			primaryKeyVal:  primaryKeyVal,

			position: common.TablePosition{
				LastRead:    start,
				SnapshotEnd: end,
			},
		}
	}

	return nil
}

func (s *snapshotIterator) fetchStartEnd(
	ctx context.Context,
	ro *spanner.ReadOnlyTransaction,
	tableName common.TableName,
) (start, end int64, err error) {
	query := fmt.Sprint(`
		SELECT count(*) as count
		FROM `, tableName,
	)
	stmt := spanner.Statement{
		SQL:    query,
		Params: nil,
	}
	iter := ro.Query(ctx, stmt)
	defer iter.Stop()

	var result struct {
		Count int64 `spanner:"count"`
	}

	row, err := iter.Next()
	if err != nil {
		return start, end, err
	}

	if err := row.ToStruct(&result); err != nil {
		return start, end, err
	}

	return 0, result.Count, nil
}

func (s *snapshotIterator) buildRecord(data fetchData) opencdc.Record {
	s.lastPosition.Snapshots[data.table] = data.position
	position := s.lastPosition.ToSDKPosition()

	metadata := opencdc.Metadata{}
	metadata.SetCollection(string(data.table))

	key := opencdc.StructuredData{
		string(data.primaryKeyName): data.payload[string(data.primaryKeyName)],
	}

	return sdk.Util.Source.NewRecordSnapshot(
		position,
		metadata,
		key,
		data.payload,
	)
}

func decodeRow(row *spanner.Row) (opencdc.StructuredData, error) {
	data := make(opencdc.StructuredData)
	for i, column := range row.ColumnNames() {
		var val interface{}
		var err error
		switch code := row.ColumnType(i).Code; code {
		case spannerpb.TypeCode_BOOL:
			var v bool
			err = row.Column(i, &v)
			val = v
		case spannerpb.TypeCode_INT64:
			var v int64
			err = row.Column(i, &v)
			val = v
		case spannerpb.TypeCode_FLOAT64:
			var v float64
			err = row.Column(i, &v)
			val = v
		case spannerpb.TypeCode_FLOAT32:
			var v float32
			err = row.Column(i, &v)
			val = v
		case spannerpb.TypeCode_TIMESTAMP:
			var v time.Time
			err = row.Column(i, &v)
			val = v
		case spannerpb.TypeCode_DATE:
			var v civil.Date
			err = row.Column(i, &v)
			val = v
		case spannerpb.TypeCode_STRING:
			var v string
			err = row.Column(i, &v)
			val = v
		case spannerpb.TypeCode_BYTES:
			var v []byte
			err = row.Column(i, &v)
			val = v
		case spannerpb.TypeCode_ARRAY:
			var v interface{}
			err = row.Column(i, &v)
			val = v
		case spannerpb.TypeCode_STRUCT:
			var v []spanner.GenericColumnValue
			err = row.Column(i, &v)
			val = v
		case spannerpb.TypeCode_NUMERIC:
			var v *big.Rat
			err = row.Column(i, &v)
			val = v
		case spannerpb.TypeCode_JSON:
			var v json.RawMessage
			err = row.Column(i, &v)
			val = v
		case spannerpb.TypeCode_PROTO:
			var v []byte
			err = row.Column(i, &v)
			val = v
		case spannerpb.TypeCode_ENUM:
			var v string
			err = row.Column(i, &v)
			val = v
		case spannerpb.TypeCode_TYPE_CODE_UNSPECIFIED:
			return nil, fmt.Errorf("unsupported column type %s for %s", code.String(), column)
		default:
			return nil, fmt.Errorf("unsupported column type %s for %s", code.String(), column)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to decode column %s: %w", column, err)
		}
		data[column] = val
	}
	return data, nil
}
