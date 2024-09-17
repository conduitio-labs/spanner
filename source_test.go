package spanner

import (
	"context"
	"testing"

	testutils "github.com/conduitio-labs/conduit-connector-spanner/test"
	"github.com/matryer/is"
)

func TestTeardownSource_NoOpen(t *testing.T) {
	is := is.New(t)
	con := NewSource()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}

var singersTable testutils.SingersTable
