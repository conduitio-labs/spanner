package testutils

import (
	"context"
	"fmt"
	"log"
	"os"
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
	databaseID   = "test-database"
	databaseName = fmt.Sprint("projects/", projectID, "/instances/", instanceID, "/databases/", databaseID)
)

var TableKeys = common.TableKeys{
	"singers": "SingerID",
}

func NewClient(ctx context.Context, is *is.I) *spanner.Client {
	is.Helper()

	emulatorHost := os.Getenv("SPANNER_EMULATOR_HOST")
	if emulatorHost == "" {
		log.Fatal("SPANNER_EMULATOR_HOST environment variable is not set")
	}

	client, err := spanner.NewClient(ctx, databaseName,
		option.WithEndpoint(emulatorHost),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication())
	is.NoErr(err)

	return client
}

func NewInstanceAdminClient(ctx context.Context, is *is.I) *instance.InstanceAdminClient {
	is.Helper()

	emulatorHost := os.Getenv("SPANNER_EMULATOR_HOST")
	if emulatorHost == "" {
		log.Fatal("SPANNER_EMULATOR_HOST environment variable is not set")
	}

	client, err := instance.NewInstanceAdminClient(ctx,
		option.WithEndpoint(emulatorHost),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication())
	is.NoErr(err)

	return client
}

func CreateInstance(ctx context.Context, t *testing.T, is *is.I) {
	t.Helper()

	client := NewInstanceAdminClient(ctx, is)

	instanceConfig := new(instancepb.Instance)
	instanceConfig.Name = databaseName
	instanceConfig.Config = fmt.Sprintf("projects/%s/instanceConfigs/emulator-config", projectID)
	instanceConfig.DisplayName = "Test Instance"
	instanceConfig.NodeCount = 1

	op, err := client.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", projectID),
		InstanceId: instanceID,
		Instance:   instanceConfig,
	})
	is.NoErr(err)

	_, err = op.Wait(ctx)
	is.NoErr(err)

	t.Cleanup(func() {
		err := client.DeleteInstance(ctx, &instancepb.DeleteInstanceRequest{
			Name: `projects/` + projectID + `/instances/` + instanceID,
		})
		is.NoErr(err)
	})
}

func NewDatabaseAdminClient(ctx context.Context, is *is.I) *database.DatabaseAdminClient {
	is.Helper()

	emulatorHost := os.Getenv("SPANNER_EMULATOR_HOST")
	if emulatorHost == "" {
		log.Fatal("SPANNER_EMULATOR_HOST environment variable is not set")
	}

	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx,
		option.WithEndpoint(emulatorHost),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication())
	is.NoErr(err)

	return databaseAdminClient
}

func SetupDatabase(ctx context.Context, t *testing.T, is *is.I) {
	t.Helper()

	client := NewDatabaseAdminClient(ctx, is)

	//exhaustruct:ignore
	dbOp, err := client.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
		CreateStatement: "CREATE DATABASE `" + databaseID + "`",
	})
	is.NoErr(err)

	_, err = dbOp.Wait(ctx)
	is.NoErr(err)

	//exhaustruct:ignore
	opUpdate, err := client.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database: databaseName,
		Statements: []string{
			`CREATE TABLE Singers (
				SingerId   bigserial PRIMARY KEY,
				Name       varchar(1024),
				CreatedAt  timestamp with time zone DEFAULT CURRENT_TIMESTAMP
			)`,
		},
	})
	is.NoErr(err)

	err = opUpdate.Wait(ctx)
	is.NoErr(err)

	t.Cleanup(func() {
		err := client.DropDatabase(ctx, &databasepb.DropDatabaseRequest{
			Database: databaseName,
		})
		is.NoErr(err)
	})
}

type Singer struct {
	SingerID  int64     `spanner:"SingerId"`
	Name      string    `spanner:"Name"`
	CreatedAt time.Time `spanner:"CreatedAt"`
}

func (s Singer) ToStructuredData() opencdc.StructuredData {
	return opencdc.StructuredData{
		"SingerId":  s.SingerID,
		"Name":      s.Name,
		"CreatedAt": s.CreatedAt,
	}
}

type SingersTable struct{}

func (SingersTable) Insert(ctx context.Context, is *is.I, singerName string) Singer {
	client := NewClient(ctx, is)
	defer client.Close()

	var insertedSinger Singer

	tx := func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		stmt := spanner.Statement{
			SQL: `INSERT INTO Singers (Name)
				  VALUES ($1)`,
			Params: map[string]interface{}{
				"p1": singerName,
			},
		}
		if _, err := txn.Update(ctx, stmt); err != nil {
			return err
		}

		return txn.Query(ctx, spanner.Statement{
			SQL: "SELECT * FROM Singers WHERE Name = $1",
			Params: map[string]interface{}{
				"p1": singerName,
			},
		}).Do(func(r *spanner.Row) error {
			return r.ToStruct(&insertedSinger)
		})
	}

	_, err := client.ReadWriteTransaction(ctx, tx)
	is.NoErr(err)

	return insertedSinger
}

func (SingersTable) Update(ctx context.Context, is *is.I, singer Singer) {
	client := NewClient(ctx, is)
	defer client.Close()

	tx := func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		stmt := spanner.Statement{
			SQL: `UPDATE Singers
				SET Name = $2, CreatedAt = $3
				WHERE SingerId = $1`,
			Params: map[string]interface{}{
				"p1": singer.SingerID,
				"p2": singer.Name,
				"p3": singer.CreatedAt,
			},
		}
		_, err := txn.Update(ctx, stmt)
		return err
	}

	_, err := client.ReadWriteTransaction(ctx, tx)
	is.NoErr(err)
}

func (SingersTable) Delete(ctx context.Context, is *is.I, singer Singer) {
	client := NewClient(ctx, is)
	defer client.Close()

	tx := func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		stmt := spanner.Statement{
			SQL: `DELETE FROM Singers
				  WHERE SingerId = $1`,
			Params: map[string]interface{}{
				"p1": singer.SingerID,
			},
		}
		_, err := txn.Update(ctx, stmt)
		return err
	}

	_, err := client.ReadWriteTransaction(ctx, tx)
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

	IsDataEqual(is, rec.Key, opencdc.StructuredData{"id": singer.SingerID})
	IsDataEqual(is, rec.Payload.After, singer.ToStructuredData())

	return rec
}

func IsDataEqual(is *is.I, a, b opencdc.Data) {
	is.Helper()
	is.Equal("", cmp.Diff(a, b))
}

func assertMetadata(is *is.I, metadata opencdc.Metadata) {
	col, err := metadata.GetCollection()
	is.NoErr(err)
	is.Equal(col, "singers")
}
