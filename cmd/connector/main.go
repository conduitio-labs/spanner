package main

import (
	spanner "github.com/conduitio-labs/conduit-connector-spanner"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func main() {
	sdk.Serve(spanner.Connector)
}
