package spanner

//go:generate paramgen -output=paramgen_src.go SourceConfig

import (
	"context"
	"fmt"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Source struct {
	sdk.UnimplementedSource

	config           SourceConfig
	lastPositionRead opencdc.Position //nolint:unused // this is just an example
}

type SourceConfig struct {
	// Config includes parameters that are the same in the source and destination.
	Config
	// SourceConfigParam is named foo and must be provided by the user.
	SourceConfigParam string `json:"foo" validate:"required"`
}

func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(new(Source), sdk.DefaultSourceMiddleware()...)
}

func (s *Source) Parameters() config.Parameters {
	// Parameters is a map of named Parameters that describe how to configure
	// the Source. Parameters can be generated from SourceConfig with paramgen.
	return s.config.Parameters()
}

func (s *Source) Configure(ctx context.Context, cfg config.Config) error {
	sdk.Logger(ctx).Info().Msg("Configuring Source...")
	err := sdk.Util.ParseConfig(ctx, cfg, &s.config, NewSource().Parameters())
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	return nil
}

func (s *Source) Open(_ context.Context, _ opencdc.Position) (err error) {
	return nil
}

func (s *Source) Read(_ context.Context) (rec opencdc.Record, err error) {
	return rec, nil
}

func (s *Source) Ack(_ context.Context, _ opencdc.Position) error {
	return nil
}

func (s *Source) Teardown(_ context.Context) error {
	// Teardown signals to the plugin that there will be no more calls to any
	// other function. After Teardown returns, the plugin should be ready for a
	// graceful shutdown.
	return nil
}
