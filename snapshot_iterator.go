package spanner

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	"github.com/conduitio-labs/conduit-connector-spanner/common"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"google.golang.org/api/iterator"
	"gopkg.in/tomb.v2"
)

type (
	snapshotIterator struct {
		t       *tomb.Tomb
		config  snapshotIteratorConfig
		recordC chan opencdc.Record
	}
	snapshotIteratorConfig struct {
		tableKeys common.TableKeys
		client    *spanner.Client
	}
)

var _ common.Iterator = new(snapshotIterator)

func newSnapshotIterator(ctx context.Context, config snapshotIteratorConfig) *snapshotIterator {
	t, _ := tomb.WithContext(ctx)
	iterator := &snapshotIterator{
		t:      t,
		config: config,
	}

	for tableName, primaryKey := range config.tableKeys {
		iterator.t.Go(func() error {
			return iterator.fetchTable(ctx, tableName, primaryKey)
		})
	}

	return iterator
}

func (s *snapshotIterator) Read(ctx context.Context) (rec opencdc.Record, err error) {
	return rec, nil
}

func (s *snapshotIterator) Ack(ctx context.Context, pos opencdc.Position) error {
	return nil
}

func (s *snapshotIterator) Teardown(ctx context.Context) error {
	return nil
}

func (s *snapshotIterator) fetchTable(
	ctx context.Context,
	tableName common.TableName,
	primaryKey common.PrimaryKeyName,
) error {
	ro := s.config.client.ReadOnlyTransaction()
	defer ro.Close()

	// fetch start end
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

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		var data opencdc.StructuredData
		for i, column := range row.ColumnNames() {
			var val interface{}
			if err := row.Column(i, &val); err != nil {
				return err
			}
			data[column] = val
		}

		var position opencdc.Position

		metadata := opencdc.Metadata{
			"table": string(tableName),
		}

		var key opencdc.Data

		rec := sdk.Util.Source.NewRecordSnapshot(position, metadata, key, data)

		s.recordC <- rec
	}

	return nil
}

func (s *snapshotIterator) fetchStartEnd(
	ctx context.Context,
	ro *spanner.ReadOnlyTransaction,
	tableName common.TableName,
) (start, end int, err error) {
	query := fmt.Sprint(`
		SELECT count(*) as count
		FROM `, tableName,
	)
	stmt := spanner.Statement{SQL: query}
	iter := ro.Query(ctx, stmt)
	defer iter.Stop()

	var result struct {
		Count int `spanner:"count"`
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
