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
	is.Helper()

	client := NewInstanceAdminClient(ctx, is)

	op, err := client.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", projectID),
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/emulator-config", projectID),
			DisplayName: "Test Instance",
			NodeCount:   1,
		},
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
	is.Helper()

	client := NewDatabaseAdminClient(ctx, is)
	dbOp, err := client.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
		CreateStatement: "CREATE DATABASE `" + databaseID + "`",
	})
	is.NoErr(err)

	_, err = dbOp.Wait(ctx)
	is.NoErr(err)

	updateReq := &databasepb.UpdateDatabaseDdlRequest{
		Database: databaseName,
		Statements: []string{
			`CREATE TABLE Singers (
				SingerId   bigint NOT NULL PRIMARY KEY,
				Name       varchar(1024),
				CreatedAt  timestamp with time zone DEFAULT CURRENT_TIMESTAMP
			)`,
		},
	}
	opUpdate, err := client.UpdateDatabaseDdl(ctx, updateReq)
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

var SingersTable singersTable

type singersTable struct{}

func (singersTable) Insert(ctx context.Context, is *is.I, singer Singer) {
	client := NewClient(ctx, is)
	defer client.Close()

	tx := func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		stmt := spanner.Statement{
			SQL: `INSERT INTO Singers (SingerId, Name, CreatedAt)
				  VALUES ($1, $2, $3)`,
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

func (singersTable) Update(ctx context.Context, is *is.I, singer Singer) {
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

func (singersTable) Delete(ctx context.Context, is *is.I, singer Singer) {
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
