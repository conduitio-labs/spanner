package common

import (
	"context"

	"cloud.google.com/go/spanner"
	sdk "github.com/conduitio/conduit-connector-sdk"
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
		sdk.Logger(ctx).Info().Msg("using custom endpoint")
	}

	return spanner.NewClient(ctx, config.DatabaseName, options...)
}
