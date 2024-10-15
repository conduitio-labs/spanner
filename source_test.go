package spanner

import (
	"context"
	"testing"

	testutils "github.com/conduitio-labs/conduit-connector-spanner/test"
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
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

func testSourceAtPosition(ctx context.Context, is *is.I, pos opencdc.Position) (sdk.Source, func()) {
	source := NewSource()
	is.NoErr(source.Configure(ctx, config.Config{
		SourceConfigDatabase: testutils.DatabaseName,
		SourceConfigEndpoint: testutils.EmulatorHost,
		SourceConfigTables:   "Singers",
	}))
	is.NoErr(source.Open(ctx, pos))

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
func TestSource_SimpleSnapshot(t *testing.T) {
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

func TestSource_RestartSnapshotAtPosition(t *testing.T) {
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

	firstChunk := singers[:2]
	secondChunk := singers[2:]

	// read the first chunk and store the last position.
	var latestPosition opencdc.Position
	{
		source, stopSource := testSource(ctx, is)

		var latestRecordRead opencdc.Record
		for _, singer := range firstChunk {
			latestRecordRead = testutils.ReadAndAssertSnapshot(ctx, is, source, singer)
		}
		latestPosition = latestRecordRead.Position

		stopSource()
	}

	// read the second chunk, starting from the last position read.
	{
		source, stopSource := testSourceAtPosition(ctx, is, latestPosition)
		for _, singer := range secondChunk {
			testutils.ReadAndAssertSnapshot(ctx, is, source, singer)
		}
		stopSource()
	}
}
