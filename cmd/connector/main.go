package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	instancepb "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx := context.Background()

	emulatorHost := os.Getenv("SPANNER_EMULATOR_HOST")
	if emulatorHost == "" {
		log.Fatal("SPANNER_EMULATOR_HOST environment variable is not set")
	}

	projectID := "test-project"
	instanceID := "test-instance"
	databaseID := "test-database"

	// Create instance admin client
	instanceAdminClient, err := instance.NewInstanceAdminClient(ctx,
		option.WithEndpoint(emulatorHost),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication())
	if err != nil {
		log.Fatalf("Failed to create instance admin client: %v", err)
	}
	defer instanceAdminClient.Close()

	// Create instance
	op, err := instanceAdminClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", projectID),
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/emulator-config", projectID),
			DisplayName: "Test Instance",
			NodeCount:   1,
		},
	})
	if err != nil {
		log.Fatalf("Failed to create instance: %v", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		log.Fatalf("Failed to wait for instance creation: %v", err)
	}

	// Create database admin client
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx,
		option.WithEndpoint(emulatorHost),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication())
	if err != nil {
		log.Fatalf("Failed to create database admin client: %v", err)
	}
	defer databaseAdminClient.Close()

	// Create database
	dbOp, err := databaseAdminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
		CreateStatement: "CREATE DATABASE `" + databaseID + "`",
	})
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	if _, err := dbOp.Wait(ctx); err != nil {
		log.Fatalf("Failed to wait for database creation: %v", err)
	}

	// Create Spanner client
	databaseName := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)
	client, err := spanner.NewClient(ctx, databaseName,
		option.WithEndpoint(emulatorHost),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication())
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	fmt.Println("Successfully connected to Spanner emulator")

	// Perform a simple query
	stmt := spanner.Statement{SQL: "SELECT 1 as test_column"}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}

	var value int64
	if err := row.Columns(&value); err != nil {
		log.Fatalf("Failed to parse row: %v", err)
	}

	fmt.Printf("Query result: %d\n", value)
}
