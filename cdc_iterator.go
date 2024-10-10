package spanner

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/cloudspannerecosystem/spanner-change-streams-tail/changestreams"
	"github.com/conduitio-labs/conduit-connector-spanner/common"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type (
	cdcIteratorConfig struct {
		tableName  string
		projectID  string
		instanceID string
		databaseID string
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
	// create the stream
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return nil, err
	}
	defer adminClient.Close()

	databaseName := fmt.Sprintf(
		"projects/%s/instances/%s/databases/%s",
		config.projectID, config.instanceID, config.databaseID,
	)
	streamID := fmt.Sprintf("%sChangeStream", config.tableName)
	stmt := fmt.Sprint("CREATE CHANGE STREAM ", streamID, " FOR ", config.tableName)
	op, err := adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   databaseName,
		Statements: []string{stmt},
	})
	if err != nil {
		return nil, err
	}
	if err := op.Wait(ctx); err != nil {
		return nil, err
	}

	reader, err := changestreams.NewReaderWithConfig(ctx,
		config.projectID, config.instanceID,
		config.databaseID, streamID,
		changestreams.Config{
			SpannerClientConfig: spanner.ClientConfig{
				SessionPoolConfig: spanner.DefaultSessionPoolConfig,
			},
		},
	)
	if err != nil {
		return nil, err
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

func (c *cdcIterator) startReader(ctx context.Context, streamID string) {
	c.errC <- c.reader.Read(ctx, func(result *changestreams.ReadResult) error {
		for _, changeRec := range result.ChangeRecords {
			for _, dataChangeRec := range changeRec.DataChangeRecords {
				for _, mod := range dataChangeRec.Mods {
					if mod.OldValues.IsNull() {
						return fmt.Errorf("old values are null")
					} else if mod.NewValues.IsNull() {
						return fmt.Errorf("new values are null")
					}

					position := common.CDCPosition{
						Start:    dataChangeRec.CommitTimestamp,
						StreamID: streamID,
					}

					operation := opencdc.OperationUpdate

					before, beforeOk := mod.OldValues.Value.(map[string]interface{})
					after, afterOk := mod.NewValues.Value.(map[string]interface{})
					if !beforeOk || !afterOk {
						return fmt.Errorf("old or new values are not maps")
					}

					if len(before) == 0 && len(after) > 0 {
						operation = opencdc.OperationCreate
					} else if len(before) > 0 && len(after) == 0 {
						operation = opencdc.OperationDelete
					} else if len(before) > 0 && len(after) > 0 {
						operation = opencdc.OperationUpdate
					} else {
						sdk.Logger(ctx).Warn().Msgf("both mod maps values are empty")
					}

					metadata := opencdc.Metadata{}
					metadata.SetCollection(c.config.tableName)

					key := opencdc.StructuredData{}
					if keys, ok := mod.Keys.Value.(map[string]interface{}); ok && mod.Keys.Valid {
						for k, v := range keys {
							key[k] = v
						}
					}

					c.recsC <- opencdc.Record{
						Position:  position.ToSDKPosition(),
						Operation: operation,
						Metadata:  metadata,
						Key:       key,
						Payload: opencdc.Change{
							Before: opencdc.StructuredData(before),
							After:  opencdc.StructuredData(after),
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
