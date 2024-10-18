package spanner

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/spanner"
	"github.com/conduitio-labs/conduit-connector-spanner/common"
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"google.golang.org/api/iterator"
)

type Source struct {
	sdk.UnimplementedSource

	client   *spanner.Client
	iterator *snapshotIterator
	config   SourceConfig
}

func NewSource() sdk.Source {
	enabled := false
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware(
		sdk.SourceWithSchemaExtractionConfig{
			PayloadEnabled: &enabled,
			KeyEnabled:     &enabled,
		},
	)...)
}

func (s *Source) Parameters() config.Parameters {
	return s.config.Parameters()
}

func (s *Source) Configure(ctx context.Context, cfg config.Config) error {
	err := sdk.Util.ParseConfig(ctx, cfg, &s.config, s.config.Parameters())
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	sdk.Logger(ctx).Info().Msg("configured source")

	return nil
}

func (s *Source) Open(ctx context.Context, pos opencdc.Position) (err error) {
	s.client, err = common.NewClient(ctx, common.NewClientConfig{
		DatabaseName: s.config.Database,
		Endpoint:     s.config.Endpoint,
	})
	if err != nil {
		return fmt.Errorf("failed to create spanner client: %w", err)
	}
	sdk.Logger(ctx).Info().Msg("created spanner client")

	tableKeys, err := getTableKeys(ctx, s.client, s.config.Tables)
	if err != nil {
		return fmt.Errorf("failed to get table primary keys: %w", err)
	}

	sdk.Logger(ctx).Info().Msg("got primary keys from tables")

	if pos != nil {
		parsed, err := common.ParseSDKPosition(pos)
		if err != nil {
			return fmt.Errorf("failed to parse position when opening connector: %w", err)
		}

		if parsed.Kind == common.PositionTypeSnapshot {
			s.iterator = newSnapshotIterator(ctx, snapshotIteratorConfig{
				tableKeys: tableKeys,
				client:    s.client,
				position:  parsed.SnapshotPosition,
			})
		} else {
			//nolint:err113 // will be supported soon
			return fmt.Errorf("unsupported cdc mode")
		}
	} else {
		s.iterator = newSnapshotIterator(ctx, snapshotIteratorConfig{
			tableKeys: tableKeys,
			client:    s.client,
		})
	}

	sdk.Logger(ctx).Info().Msg("opened source")

	return nil
}

func (s *Source) Read(ctx context.Context) (rec opencdc.Record, err error) {
	return s.iterator.Read(ctx)
}

func (s *Source) Ack(ctx context.Context, pos opencdc.Position) error {
	return s.iterator.Ack(ctx, pos)
}

func (s *Source) Teardown(ctx context.Context) error {
	defer func() {
		if s.client != nil {
			s.client.Close()
		}
	}()
	if s.iterator != nil {
		if err := s.iterator.Teardown(ctx); err != nil {
			return fmt.Errorf("failed to teardown iterator: %w", err)
		}
	}

	return nil
}

func getPrimaryKey(
	ctx context.Context,
	client *spanner.Client,
	table string,
) (common.PrimaryKeyName, error) {
	stmt := spanner.Statement{
		SQL: `
			SELECT COLUMN_NAME
			FROM INFORMATION_SCHEMA.INDEX_COLUMNS
			WHERE TABLE_NAME = @table
			AND INDEX_NAME = 'PRIMARY_KEY'
			ORDER BY ORDINAL_POSITION LIMIT 1
		`,
		Params: map[string]interface{}{
			"table": table,
		},
	}

	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if errors.Is(err, iterator.Done) {
		return "", ErrPrimaryKeyNotFound
	}
	if err != nil {
		return "", fmt.Errorf("failed to get primary key from table %s: %w", table, err)
	}

	var columnName common.PrimaryKeyName
	if err := row.Columns(&columnName); err != nil {
		return "", fmt.Errorf("failed to scan primary key from table %s: %w", table, err)
	}

	return columnName, nil
}

func getTableKeys(
	ctx context.Context,
	client *spanner.Client,
	tables []string,
) (common.TableKeys, error) {
	tableKeys := make(common.TableKeys)
	for _, table := range tables {
		primaryKey, err := getPrimaryKey(ctx, client, table)
		if err != nil {
			return nil, fmt.Errorf("failed to get primary key for table %q: %w", table, err)
		}
		tableKeys[common.TableName(table)] = primaryKey
	}
	return tableKeys, nil
}
