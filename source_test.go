package spanner

import (
	"context"
	"testing"

	testutils "github.com/conduitio-labs/conduit-connector-spanner/test"
	"github.com/conduitio/conduit-commons/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func testSource(ctx context.Context, is *is.I) (sdk.Source, func()) {
	source := NewSource()
	is.NoErr(source.Configure(ctx, config.Config{
		SourceConfigDatabase: testutils.DatabaseName,
		SourceConfigEndpoint: testutils.EmulatorHost,
		SourceConfigTables:   "Singers",
	}))
	is.NoErr(source.Open(ctx, nil))

	return source, func() {
		is.NoErr(source.Teardown(ctx))
	}
}

func TestTeardownSource_NoOpen(t *testing.T) {
	is := is.New(t)
	con := NewSource()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}
func TestSourceSnapshot(t *testing.T) {
	is := is.New(t)
	ctx := testutils.TestContext(t)

	testutils.CreateInstance(ctx, is)
	testutils.SetupDatabase(ctx, is)

	var singers []testutils.Singer

	singer1 := testutils.InsertSinger(ctx, is, 1, "singer1")
	singers = append(singers, singer1)

	singer2 := testutils.InsertSinger(ctx, is, 2, "singer2")
	singers = append(singers, singer2)

	singer3 := testutils.InsertSinger(ctx, is, 3, "singer3")
	singers = append(singers, singer3)

	source, stopSource := testSource(ctx, is)
	defer stopSource()

	for _, singer := range singers {
		testutils.ReadAndAssertSnapshot(ctx, is, source, singer)
	}
}
