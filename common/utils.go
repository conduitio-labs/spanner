package common

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type NewClientConfig struct {
	DatabaseName string
	Endpoint     string
}

func NewClient(ctx context.Context, config NewClientConfig) (*spanner.Client, error) {
	options := []option.ClientOption{
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	}
	if config.Endpoint != "" {
		options = append(options, option.WithEndpoint(config.Endpoint))
	}

	client, err := spanner.NewClient(ctx, config.DatabaseName, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create spanner client: %w", err)
	}

	return client, nil
}

func NewDatabaseAdminClientWithEndpoint(
	ctx context.Context, endpoint string,
) (*database.DatabaseAdminClient, error) {
	options := []option.ClientOption{
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	}
	if endpoint != "" {
		options = append(options, option.WithEndpoint(endpoint))
	}

	client, err := database.NewDatabaseAdminClient(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create database admin client: %w", err)
	}

	return client, nil
}

func ClientOptions(endpoint string) []option.ClientOption {
	options := []option.ClientOption{
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	}
	if endpoint != "" {
		options = append(options, option.WithEndpoint(endpoint))
	}

	return options
}

func FormatValue(val any) any {
	switch v := val.(type) {
	case *time.Time:
		return v.Format(time.RFC3339)
	case time.Time:
		return v.Format(time.RFC3339)
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return v
		}

		return i
	default:
		return v
	}
}
