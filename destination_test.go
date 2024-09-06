package spanner_test

import (
	"context"
	"testing"

	spanner "github.com/conduitio-labs/conduit-connector-spanner"
	"github.com/matryer/is"
)

func TestTeardown_NoOpen(t *testing.T) {
	is := is.New(t)
	con := spanner.NewDestination()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}
