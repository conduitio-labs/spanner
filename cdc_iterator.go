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
)

type (
	cdcIteratorConfig struct {
		tableName  string
		projectID  string
		instanceID string
		databaseID string
	}
	cdcIterator struct {
		recsC  chan opencdc.Record
		reader *changestreams.Reader
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

	recsC := make(chan opencdc.Record)

	return &cdcIterator{
		recsC:  recsC,
		reader: reader,
	}, nil
}

func (c *cdcIterator) Ack(context.Context, opencdc.Position) error {
	return nil
}

func (c *cdcIterator) Read(ctx context.Context) (rec opencdc.Record, err error) {
	err = c.reader.Read(ctx, func(result *changestreams.ReadResult) error {
		for _, changeRec := range result.ChangeRecords {
			for _, childPartRec := range changeRec.ChildPartitionsRecords {
				for _, partition := range childPartRec.ChildPartitions {
					fmt.Println("partition token", partition.Token)
				}

			}

			for _, dataChangeRec := range changeRec.DataChangeRecords {
				for _, mod := range dataChangeRec.Mods {
					fmt.Println(mod.NewValues)
				}
			}
		}
		return nil
	})

	return rec, err
}

func (c *cdcIterator) Teardown(context.Context) error {
	c.reader.Close()
	return nil
}
