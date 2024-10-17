package testutils

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/conduitio-labs/conduit-connector-spanner/common"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestContext(t *testing.T) context.Context {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	return logger.WithContext(context.Background())
}

var (
	projectID    = "test-project"
	instanceID   = "test-instance"
	instanceName = fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID)
	databaseID   = "test-database"
	databaseName = fmt.Sprint("projects/", projectID, "/instances/", instanceID, "/databases/", databaseID)
)

var TableKeys = common.TableKeys{
	"singers": "SingerID",
}

var emulatorHost = "localhost:9010"

func NewClient(ctx context.Context, is *is.I) *spanner.Client {
	is.Helper()

	client, err := spanner.NewClient(ctx, databaseName,
		option.WithEndpoint(emulatorHost),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication())
	is.NoErr(err)

	return client
}

func NewInstanceAdminClient(ctx context.Context, is *is.I) *instance.InstanceAdminClient {
	is.Helper()

	client, err := instance.NewInstanceAdminClient(ctx,
		option.WithEndpoint(emulatorHost),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication())
	is.NoErr(err)

	return client
}

func CreateInstance(ctx context.Context, is *is.I) {
	client := NewInstanceAdminClient(ctx, is)
	defer client.Close()

	err := client.DeleteInstance(ctx, &instancepb.DeleteInstanceRequest{
		Name: `projects/` + projectID + `/instances/` + instanceID,
	})
	is.NoErr(err)

	op, err := client.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", projectID),
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Name:        instanceName,
			DisplayName: "Test Instance",
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/emulator-config", projectID),
			NodeCount:   1,
		},
	})
	is.NoErr(err)

	_, err = op.Wait(ctx)
	is.NoErr(err)
}

func NewDatabaseAdminClient(ctx context.Context, is *is.I) *database.DatabaseAdminClient {
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx,
		option.WithEndpoint(emulatorHost),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication())
	is.NoErr(err)

	return databaseAdminClient
}

func SetupDatabase(ctx context.Context, is *is.I) {
	client := NewDatabaseAdminClient(ctx, is)
	defer client.Close()

	err := client.DropDatabase(ctx, &databasepb.DropDatabaseRequest{
		Database: databaseName,
	})
	is.NoErr(err)

	dbOp, err := client.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
		CreateStatement: "CREATE DATABASE `" + databaseID + "`",
	})
	is.NoErr(err)

	_, err = dbOp.Wait(ctx)
	is.NoErr(err)

	opUpdate, err := client.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database: databaseName,
		Statements: []string{
			`CREATE TABLE Singers (
				SingerID   INT64 NOT NULL,
				Name       STRING(1024),
				CreatedAt  TIMESTAMP DEFAULT (CURRENT_TIMESTAMP())
			) PRIMARY KEY (SingerID DESC)`,
		},
	})
	is.NoErr(err)

	err = opUpdate.Wait(ctx)
	is.NoErr(err)
}

type Singer struct {
	SingerID  int64     `spanner:"SingerID"`
	Name      string    `spanner:"Name"`
	CreatedAt time.Time `spanner:"CreatedAt"`
}

func (s Singer) ToStructuredData() opencdc.StructuredData {
	return opencdc.StructuredData{
		"SingerID":  s.SingerID,
		"Name":      s.Name,
		"CreatedAt": s.CreatedAt,
	}
}

func InsertSinger(ctx context.Context, is *is.I, singerID int, singerName string) Singer {
	client := NewClient(ctx, is)
	defer client.Close()

	var insertedSinger Singer

	tx := func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		stmt := spanner.Statement{
			SQL: `INSERT INTO Singers (SingerID, Name)
				  VALUES (@singerID, @name)`,
			Params: map[string]interface{}{
				"singerID": singerID,
				"name":     singerName,
			},
		}
		if _, err := txn.Update(ctx, stmt); err != nil {
			return fmt.Errorf("failed to insert singer: %w", err)
		}

		return txn.Query(ctx, spanner.Statement{
			SQL: "SELECT * FROM Singers WHERE Name = @name",
			Params: map[string]interface{}{
				"name": singerName,
			},
		}).Do(func(r *spanner.Row) error {
			return r.ToStruct(&insertedSinger)
		})
	}

	_, err := client.ReadWriteTransaction(ctx, tx)
	is.NoErr(err)

	return insertedSinger
}

func ReadAndAssertSnapshot(
	ctx context.Context, is *is.I,
	iterator common.Iterator, singer Singer,
) opencdc.Record {
	rec, err := iterator.Read(ctx)
	is.NoErr(err)
	is.NoErr(iterator.Ack(ctx, rec.Position))

	is.Equal(rec.Operation, opencdc.OperationSnapshot)

	assertMetadata(is, rec.Metadata)

	isDataEqual(is, rec.Key, opencdc.StructuredData{"SingerID": singer.SingerID})
	isDataEqual(is, rec.Payload.After, singer.ToStructuredData())

	return rec
}

func isDataEqual(is *is.I, a, b opencdc.Data) {
	is.Helper()
	is.Equal("", cmp.Diff(a, b))
}

func assertMetadata(is *is.I, metadata opencdc.Metadata) {
	col, err := metadata.GetCollection()
	is.NoErr(err)
	is.Equal(col, "singers")
}
