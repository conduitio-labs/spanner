package spanner

//go:generate paramgen -output=paramgen_src.go SourceConfig

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

	config           SourceConfig
	lastPositionRead opencdc.Position //nolint:unused // this is just an example
}

type SourceConfig struct {
	// Config includes parameters that are the same in the source and destination.
	Config
	// SourceConfigParam is named foo and must be provided by the user.
	SourceConfigParam string `json:"foo" validate:"required"`
}

func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(new(Source), sdk.DefaultSourceMiddleware()...)
}

func (s *Source) Parameters() config.Parameters {
	// Parameters is a map of named Parameters that describe how to configure
	// the Source. Parameters can be generated from SourceConfig with paramgen.
	return s.config.Parameters()
}

func (s *Source) Configure(ctx context.Context, cfg config.Config) error {
	sdk.Logger(ctx).Info().Msg("Configuring Source...")
	err := sdk.Util.ParseConfig(ctx, cfg, &s.config, NewSource().Parameters())
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	return nil
}

func (s *Source) Open(_ context.Context, _ opencdc.Position) (err error) {
	return nil
}

func (s *Source) Read(_ context.Context) (rec opencdc.Record, err error) {
	return rec, nil
}

func (s *Source) Ack(_ context.Context, _ opencdc.Position) error {
	return nil
}

func (s *Source) Teardown(_ context.Context) error {
	// Teardown signals to the plugin that there will be no more calls to any
	// other function. After Teardown returns, the plugin should be ready for a
	// graceful shutdown.
	return nil
}

//nolint:unused // we'll use this later on when integrating the iterators with the source connector
func getPrimaryKey(
	ctx context.Context,
	client *spanner.Client,
	table string,
) (common.PrimaryKeyName, error) {
	stmt := spanner.Statement{
		SQL: `
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_NAME = @table
            AND CONSTRAINT_NAME = 'PRIMARY_KEY'
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
		return "", fmt.Errorf("no primary key found for table %s", table)
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

//nolint:unused // we'll use this later on when integrating the iterators with the source connector
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
