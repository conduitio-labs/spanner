package spanner

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/cloudspannerecosystem/spanner-change-streams-tail/changestreams"
	"github.com/conduitio-labs/conduit-connector-spanner/common"
	"github.com/conduitio/conduit-commons/opencdc"
)

type (
	cdcIteratorConfig struct {
		tableName   string
		projectID   string
		instanceID  string
		databaseID  string
		position    *common.CDCPosition
		client      *spanner.Client
		adminClient *database.DatabaseAdminClient
		endpoint    string
	}
	cdcIterator struct {
		reader *changestreams.Reader
		recsC  chan opencdc.Record
		errC   chan error
		config *cdcIteratorConfig
	}
)

var _ common.Iterator = new(cdcIterator)

func newCdcIterator(ctx context.Context, config *cdcIteratorConfig) (*cdcIterator, error) {
	databaseName := fmt.Sprintf(
		"projects/%s/instances/%s/databases/%s",
		config.projectID, config.instanceID, config.databaseID,
	)

	var streamID string
	if config.position != nil {
		streamID = config.position.StreamID
	} else {
		streamID = fmt.Sprintf("%sChangeStream", config.tableName)
	}

	streamExists, err := checkIfStreamExists(ctx, config.client, streamID)
	if err != nil {
		return nil, fmt.Errorf("failed to check if change stream %s exists: %w", streamID, err)
	}

	if !streamExists {
		stmt := fmt.Sprint("CREATE CHANGE STREAM ", streamID, " FOR ", config.tableName)
		op, err := config.adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
			Database:   databaseName,
			Statements: []string{stmt},
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create change stream %s: %w", streamID, err)
		}
		if err := op.Wait(ctx); err != nil {
			return nil, fmt.Errorf("failed to wait for change stream %s creation: %w", streamID, err)
		}
	}

	changestreamsConfig := changestreams.Config{
		SpannerClientOptions: common.ClientOptions(config.endpoint),
		SpannerClientConfig: spanner.ClientConfig{
			SessionPoolConfig: spanner.DefaultSessionPoolConfig,
		},
	}
	if config.position != nil {
		changestreamsConfig.StartTimestamp = config.position.Start
	}

	reader, err := changestreams.NewReaderWithConfig(ctx,
		config.projectID, config.instanceID,
		config.databaseID, streamID,
		changestreamsConfig,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader for change stream %s: %w", streamID, err)
	}

	iterator := &cdcIterator{
		recsC:  make(chan opencdc.Record),
		errC:   make(chan error),
		reader: reader,
		config: config,
	}

	go iterator.startReader(ctx, streamID)

	return iterator, nil
}

func checkIfStreamExists(ctx context.Context, client *spanner.Client, streamID string) (bool, error) {
	stmt := spanner.NewStatement(`
		SELECT COUNT(*) 
		FROM INFORMATION_SCHEMA.CHANGE_STREAMS 
		WHERE CHANGE_STREAM_NAME = @streamID
	`)
	stmt.Params["streamID"] = streamID

	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var count int64
	row, err := iter.Next()
	if err != nil {
		return false, err
	}
	if err := row.Columns(&count); err != nil {
		return false, err
	}

	return count > 0, nil
}

func (c *cdcIterator) startReader(ctx context.Context, streamID string) {
	c.errC <- c.reader.Read(ctx, func(result *changestreams.ReadResult) error {
		for _, changeRec := range result.ChangeRecords {
			for _, dataChangeRec := range changeRec.DataChangeRecords {
				for _, mod := range dataChangeRec.Mods {
					// add a nanosecond so that the position doesn't include the record itself
					start := dataChangeRec.CommitTimestamp.Add(time.Nanosecond)
					position := common.CDCPosition{
						Start:    start,
						StreamID: streamID,
					}

					before := parseValues(mod.OldValues, mod.Keys)
					after := parseValues(mod.NewValues, mod.Keys)

					var operation opencdc.Operation

					if len(before) == 0 && len(after) > 0 {
						operation = opencdc.OperationCreate
					} else if len(before) > 0 && len(after) == 0 {
						operation = opencdc.OperationDelete
					} else if len(before) > 0 && len(after) > 0 {
						operation = opencdc.OperationUpdate
					} else {
						return fmt.Errorf("data change record modification maps are empty, something unexpected happened")
					}

					metadata := opencdc.Metadata{}
					metadata.SetCollection(c.config.tableName)

					key := opencdc.StructuredData{}
					if mod.Keys.Valid {
						if keys, ok := mod.Keys.Value.(map[string]interface{}); ok {
							for k, v := range keys {
								key[k] = common.FormatValue(v)
							}
						}
					}

					c.recsC <- opencdc.Record{
						Position:  position.ToSDKPosition(),
						Operation: operation,
						Metadata:  metadata,
						Key:       key,
						Payload: opencdc.Change{
							Before: before,
							After:  after,
						},
					}
				}
			}
		}
		return nil
	})
}

func (c *cdcIterator) Read(ctx context.Context) (rec opencdc.Record, err error) {
	select {
	case rec := <-c.recsC:
		return rec, nil
	case err := <-c.errC:
		return rec, err
	case <-ctx.Done():
		return rec, ctx.Err()
	}
}

func (c *cdcIterator) Ack(context.Context, opencdc.Position) error {
	return nil
}

func (c *cdcIterator) Teardown(context.Context) error {
	c.reader.Close()
	return nil
}

func parseValues(values spanner.NullJSON, keys spanner.NullJSON) opencdc.StructuredData {
	if values.IsNull() {
		return nil
	}
	valuesMap, ok := values.Value.(map[string]any)
	if !ok {
		return nil
	} else if len(valuesMap) == 0 {
		return nil
	}

	data := make(opencdc.StructuredData)
	for key, val := range valuesMap {
		data[key] = common.FormatValue(val)
	}

	if keys.Valid {
		keysMap, ok := keys.Value.(map[string]any)
		if ok {
			for key, val := range keysMap {
				data[key] = common.FormatValue(val)
			}
		}
	}

	return data
}
