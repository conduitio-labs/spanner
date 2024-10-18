package spanner

import (
	"context"
	"testing"

	testutils "github.com/conduitio-labs/conduit-connector-spanner/test"
	"github.com/matryer/is"
)

func testSnapshotIterator(ctx context.Context, is *is.I) (*snapshotIterator, func()) {
	is.Helper()

	client := testutils.NewClient(ctx, is)

	iterator := newSnapshotIterator(ctx, snapshotIteratorConfig{
		tableKeys: testutils.TableKeys,
		client:    client,
	})

	return iterator, func() { is.NoErr(iterator.Teardown(ctx)) }
}

func TestSnapshot(t *testing.T) {
	is := is.New(t)
	ctx := testutils.TestContext(t)

	testutils.SetupDatabase(ctx, is)

	var singers []testutils.Singer

	singer1 := testutils.InsertSinger(ctx, is, 1)
	singers = append(singers, singer1)

	singer2 := testutils.InsertSinger(ctx, is, 2)
	singers = append(singers, singer2)

	singer3 := testutils.InsertSinger(ctx, is, 3)
	singers = append(singers, singer3)

	iterator, stopIterator := testSnapshotIterator(ctx, is)
	defer stopIterator()

	for _, singer := range singers {
		testutils.ReadAndAssertSnapshot(ctx, is, iterator, singer)
	}
}
