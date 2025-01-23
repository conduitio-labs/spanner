// Copyright © 2025 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spanner

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	"github.com/conduitio-labs/conduit-connector-spanner/common"
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Destination struct {
	sdk.UnimplementedDestination
	writer *Writer
	client *spanner.Client

	config DestinationConfig
}

func NewDestination() sdk.Destination {
	// Create Destination and wrap it in the default middleware.
	return sdk.DestinationWithMiddleware(new(Destination), sdk.DefaultDestinationMiddleware()...)
}

func (d *Destination) Parameters() config.Parameters {
	// Parameters is a map of named Parameters that describe how to configure
	// the Destination. Parameters can be generated from DestinationConfig with
	// paramgen.
	return d.config.Parameters()
}

func (d *Destination) Configure(ctx context.Context, cfg config.Config) error {
	sdk.Logger(ctx).Info().Msg("Configuring Destination...")
	err := sdk.Util.ParseConfig(ctx, cfg, &d.config, d.config.Parameters())
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	d.config.Init()
	sdk.Logger(ctx).Info().Msg("configured destination")
	return nil
}

func (d *Destination) Open(ctx context.Context) (err error) {
	d.client, err = common.NewClient(ctx, common.NewClientConfig{
		DatabaseName: d.config.Database,
		Endpoint:     d.config.Endpoint,
	})
	if err != nil {
		return fmt.Errorf("failed to create spanner client: %w", err)
	}
	sdk.Logger(ctx).Info().Msg("opened destination")

	d.writer, err = NewWriter(ctx, d.client, d.config)
	if err != nil {
		return fmt.Errorf("new writer: %w", err)
	}

	return nil
}

func (d *Destination) Write(ctx context.Context, records []opencdc.Record) (int, error) {
	for i, record := range records {
		err := sdk.Util.Destination.Route(ctx, record,
			d.writer.Insert,
			d.writer.Update,
			d.writer.Delete,
			d.writer.Insert,
		)
		if err != nil {
			if record.Key != nil {
				return i, fmt.Errorf("key %s: %w", string(record.Key.Bytes()), err)
			}
			return i, fmt.Errorf("record with no key: %w", err)
		}
	}

	return len(records), nil
}

func (d *Destination) Teardown(_ context.Context) error {
	if d.client != nil {
		d.client.Close()
	}
	if d.writer != nil {
		if err := d.writer.Stop(); err != nil {
			return fmt.Errorf("stop writer: %w", err)
		}
	}
	return nil
}
