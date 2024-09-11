package spanner_test

import (
	"context"
	"testing"

	spanner "github.com/conduitio-labs/conduit-connector-spanner"
	testutils "github.com/conduitio-labs/conduit-connector-spanner/test"
	"github.com/matryer/is"
)

func TestTeardownSource_NoOpen(t *testing.T) {
	is := is.New(t)
	con := spanner.NewSource()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}

var singersTable testutils.SingersTable

func TestSnapshot(t *testing.T) {
	is := is.New(t)
	ctx := testutils.TestContext(t)

	testutils.CreateInstance(ctx, t, is)
	testutils.SetupDatabase(ctx, t, is)

	singer1 := singersTable.Insert(ctx, is, "singer1")
	singer2 := singersTable.Insert(ctx, is, "singer2")
	singer3 := singersTable.Insert(ctx, is, "singer3")

	// start snapshot iterator

	// assert read singers from records correspond
}
