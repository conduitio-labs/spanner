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

func TestSnapshot(t *testing.T) {
	is := is.New(t)
	ctx := testutils.TestContext(t)

	testutils.CreateInstance(ctx, t, is)
	testutils.SetupDatabase(ctx, t, is)

	var singers []testutils.Singer

	singer1 := singersTable.Insert(ctx, is, "singer1")
	singers = append(singers, singer1)

	singer2 := singersTable.Insert(ctx, is, "singer2")
	singers = append(singers, singer2)

	singer3 := singersTable.Insert(ctx, is, "singer3")
	singers = append(singers, singer3)

	iterator := newSnapshotIterator()
	defer func() { is.NoErr(iterator.Teardown(ctx)) }()

	for _, singer := range singers {
		testutils.ReadAndAssertSnapshot(ctx, is, iterator, singer)
	}
}
