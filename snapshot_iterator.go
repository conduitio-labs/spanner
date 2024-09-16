package spanner

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/spanner"
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
		payload    opencdc.StructuredData
		table      common.TableName
		primaryKey common.PrimaryKeyName
		position   common.TablePosition
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

func (s *snapshotIterator) Read(ctx context.Context) (rec opencdc.Record, err error) {
	select {
	case <-ctx.Done():
		//nolint:wrapcheck // no need to wrap canceled error
		return rec, ctx.Err()
	case <-s.t.Dead():
		if err := s.t.Err(); err != nil && !errors.Is(err, ErrSnapshotIteratorDone) {
			return rec, fmt.Errorf(
				"cannot stop snapshot mode, fetchers exited unexpectedly: %w", err)
		}
		if err := s.acks.Wait(ctx); err != nil {
			return rec, fmt.Errorf("failed to wait for acks on snapshot iterator done: %w", err)
		}

		return rec, ErrSnapshotIteratorDone
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
		return err
	}

	query := fmt.Sprint(`
		SELECT *
		FROM `, tableName, `
		WHERE `, primaryKey, ` > @start AND `, primaryKey, ` <= @end
		ORDER BY `, primaryKey, ` LIMIT ?
	`)
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
			return err
		}

		data := opencdc.StructuredData{}
		for i, column := range row.ColumnNames() {
			var val interface{}
			if err := row.Column(i, &val); err != nil {
				return err
			}
			data[column] = val
		}

		s.dataC <- fetchData{
			payload:    data,
			table:      tableName,
			primaryKey: primaryKey,
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
) (start, end uint64, err error) {
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
		Count uint64 `spanner:"count"`
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

	metadata := opencdc.Metadata{
		"table": string(data.table),
	}

	key := opencdc.StructuredData{
		string(data.primaryKey): data.payload[string(data.primaryKey)],
	}

	return sdk.Util.Source.NewRecordSnapshot(
		position,
		metadata,
		key,
		data.payload,
	)
}
