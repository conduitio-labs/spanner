package spanner

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// version is set during the build process with ldflags (see Makefile).
// Default version matches default from runtime/debug.
var version = "(devel)"

// Specification returns the connector's specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:              "spanner",
		Summary:           "A Conduit Connector for Spanner",
		Description:       "A source and destination connector for Spanner",
		Version:           version,
		Author:            "Meroxa, Inc.",
		SourceParams:      new(SourceConfig).Parameters(),
		DestinationParams: new(DestinationConfig).Parameters(),
	}
}
