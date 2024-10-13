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
	ProjectID    = "test-project"
	InstanceID   = "test-instance"
	instanceName = fmt.Sprintf("projects/%s/instances/%s", ProjectID, InstanceID)
	DatabaseID   = "test-database"
	DatabaseName = fmt.Sprint(
		"projects/", ProjectID,
		"/instances/", InstanceID,
		"/databases/", DatabaseID,
	)
)

var TableKeys = common.TableKeys{
	"singers": "SingerID",
}

var EmulatorHost = "localhost:9010"

func NewClient(ctx context.Context, is *is.I) *spanner.Client {
	is.Helper()

	client, err := spanner.NewClient(ctx, DatabaseName,
		option.WithEndpoint(EmulatorHost),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication())
	is.NoErr(err)

	return client
}

func newInstanceAdminClient(ctx context.Context, is *is.I) *instance.InstanceAdminClient {
	is.Helper()

	client, err := instance.NewInstanceAdminClient(ctx,
		option.WithEndpoint(EmulatorHost),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication())
	is.NoErr(err)

	return client
}

func createInstance(ctx context.Context, is *is.I) {
	client := newInstanceAdminClient(ctx, is)
	defer client.Close()

	err := client.DeleteInstance(ctx, &instancepb.DeleteInstanceRequest{
		Name: `projects/` + ProjectID + `/instances/` + InstanceID,
	})
	is.NoErr(err)

	op, err := client.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", ProjectID),
		InstanceId: InstanceID,
		Instance: &instancepb.Instance{
			Name:        instanceName,
			DisplayName: "Test Instance",
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/emulator-config", ProjectID),
			NodeCount:   1,
		},
	})
	is.NoErr(err)

	_, err = op.Wait(ctx)
	is.NoErr(err)
}

func NewDatabaseAdminClient(ctx context.Context, is *is.I) *database.DatabaseAdminClient {
	client, err := common.NewDatabaseAdminClientWithEndpoint(ctx, EmulatorHost)
	is.NoErr(err)

	return client
}

func SetupDatabase(ctx context.Context, is *is.I) {
	createInstance(ctx, is)

	client := NewDatabaseAdminClient(ctx, is)
	defer client.Close()

	// cleanup previous database setup, if any
	err := client.DropDatabase(ctx, &databasepb.DropDatabaseRequest{
		Database: DatabaseName,
	})
	is.NoErr(err)

	dbOp, err := client.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", ProjectID, InstanceID),
		CreateStatement: "CREATE DATABASE `" + DatabaseID + "`",
	})
	is.NoErr(err)

	_, err = dbOp.Wait(ctx)
	is.NoErr(err)

	opUpdate, err := client.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database: DatabaseName,
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

func (s Singer) Update() Singer {
	s.Name = fmt.Sprintf("%v-updated", s.Name)
	return s
}

func (s Singer) ToStructuredData() opencdc.StructuredData {
	return opencdc.StructuredData{
		"SingerID":  s.SingerID,
		"Name":      s.Name,
		"CreatedAt": s.CreatedAt.Format(time.RFC3339),
	}
}

func InsertSinger(ctx context.Context, is *is.I, singerID int, singerName string) Singer {
	client := NewClient(ctx, is)
	defer client.Close()

	var insertedSinger Singer

	tx := func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		name := fmt.Sprint("singer ", singerID)
		stmt := spanner.Statement{
			SQL: `INSERT INTO Singers (SingerID, Name)
				  VALUES (@singerID, @name)`,
			Params: map[string]interface{}{
				"singerID": singerID,
				"name":     name,
			},
		}
		if _, err := txn.Update(ctx, stmt); err != nil {
			return fmt.Errorf("failed to insert singer: %w", err)
		}

		return txn.Query(ctx, spanner.Statement{
			SQL: "SELECT * FROM Singers WHERE Name = @name",
			Params: map[string]interface{}{
				"name": name,
			},
		}).Do(func(r *spanner.Row) error {
			return r.ToStruct(&insertedSinger)
		})
	}

	_, err := client.ReadWriteTransaction(ctx, tx)
	is.NoErr(err)

	return insertedSinger
}

func DeleteSinger(ctx context.Context, is *is.I, singer Singer) {
	client := NewClient(ctx, is)
	defer client.Close()

	_, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Delete("Singers", spanner.Key{singer.SingerID}),
	})
	is.NoErr(err)
}

func ReadAndAssertSnapshot(
	ctx context.Context, is *is.I,
	iterator common.Iterator, singer Singer,
) opencdc.Record {
	is.Helper()

	rec, err := iterator.Read(ctx)
	is.NoErr(err)
	is.NoErr(iterator.Ack(ctx, rec.Position))

	is.Equal(rec.Operation, opencdc.OperationSnapshot)

	assertMetadata(is, rec.Metadata)

	assertKey(is, rec, singer)
	isDataEqual(is, rec.Payload.After, singer.ToStructuredData())

	return rec
}

func ReadAndAssertInsert(
	ctx context.Context, is *is.I,
	iterator common.Iterator, singer Singer,
) opencdc.Record {
	is.Helper()
	rec, err := iterator.Read(ctx)
	is.NoErr(err)
	is.NoErr(iterator.Ack(ctx, rec.Position))

	is.Equal(rec.Operation, opencdc.OperationCreate)

	assertMetadata(is, rec.Metadata)

	assertKey(is, rec, singer)
	isDataEqual(is, rec.Payload.After, singer.ToStructuredData())

	return rec
}

func ReadAndAssertUpdate(
	ctx context.Context, is *is.I,
	iterator common.Iterator, prev, next Singer,
) {
	is.Helper()
	rec, err := iterator.Read(ctx)
	is.NoErr(err)
	is.NoErr(iterator.Ack(ctx, rec.Position))

	is.Equal(rec.Operation, opencdc.OperationUpdate)

	assertMetadata(is, rec.Metadata)

	assertKey(is, rec, prev)
	assertKey(is, rec, next)

	isDataEqual(is, rec.Payload.Before, prev.ToStructuredData())
	isDataEqual(is, rec.Payload.After, next.ToStructuredData())
}

func ReadAndAssertDelete(
	ctx context.Context, is *is.I,
	iterator common.Iterator, singer Singer,
) {
	is.Helper()

	rec, err := iterator.Read(ctx)
	is.NoErr(err)
	is.NoErr(iterator.Ack(ctx, rec.Position))

	is.Equal(rec.Operation, opencdc.OperationDelete)

	assertMetadata(is, rec.Metadata)
	assertKey(is, rec, singer)
}

func isDataEqual(is *is.I, a, b opencdc.Data) {
	is.Helper()
	is.Equal("", cmp.Diff(a, b))
}

func assertMetadata(is *is.I, metadata opencdc.Metadata) {
	is.Helper()
	col, err := metadata.GetCollection()
	is.NoErr(err)
	is.Equal(col, "Singers")
}

func assertKey(is *is.I, rec opencdc.Record, singer Singer) {
	is.Helper()
	isDataEqual(is, rec.Key, opencdc.StructuredData{"SingerID": singer.SingerID})
}
