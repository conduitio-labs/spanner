package spanner

import (
	"context"
	"testing"

	"github.com/conduitio-labs/conduit-connector-spanner/common"
	testutils "github.com/conduitio-labs/conduit-connector-spanner/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func testCdcIterator(ctx context.Context, is *is.I) (common.Iterator, func()) {
	client := testutils.NewClient(ctx, is)

	iterator, err := newCdcIterator(ctx, &cdcIteratorConfig{
		tableName:  "Singers",
		projectID:  testutils.ProjectID,
		instanceID: testutils.InstanceID,
		databaseID: testutils.DatabaseID,
		client:     client,
	})
	is.NoErr(err)

	return iterator, func() { is.NoErr(iterator.Teardown(ctx)) }
}

func testCdcIteratorAtPosition(
	ctx context.Context, is *is.I,
	sdkPos opencdc.Position,
) (common.Iterator, func()) {
	client := testutils.NewClient(ctx, is)

	pos, err := common.ParseSDKPosition(sdkPos)
	is.NoErr(err)

	is.Equal(pos.Kind, common.PositionType("cdc"))
	is.True(pos.CDCPosition != nil)
	is.True(pos.SnapshotPosition == nil)

	iterator, err := newCdcIterator(ctx, &cdcIteratorConfig{
		tableName:  "Singers",
		projectID:  testutils.ProjectID,
		instanceID: testutils.InstanceID,
		databaseID: testutils.DatabaseID,
		client:     client,
		position:   pos.CDCPosition,
	})
	is.NoErr(err)

	return iterator, func() { is.NoErr(iterator.Teardown(ctx)) }
}

func TestCDCIterator_InsertAction(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	testutils.SetupDatabase(ctx, is)

	iterator, teardown := testCdcIterator(ctx, is)
	defer teardown()

	user1 := singersTable.Insert(ctx, is, 1)
	user2 := singersTable.Insert(ctx, is, 2)
	user3 := singersTable.Insert(ctx, is, 3)

	testutils.ReadAndAssertInsert(ctx, is, iterator, user1)
	testutils.ReadAndAssertInsert(ctx, is, iterator, user2)
	testutils.ReadAndAssertInsert(ctx, is, iterator, user3)
}

func TestCDCIterator_DeleteAction(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	testutils.SetupDatabase(ctx, is)

	user1 := singersTable.Insert(ctx, is, 1)
	user2 := singersTable.Insert(ctx, is, 2)
	user3 := singersTable.Insert(ctx, is, 3)

	iterator, teardown := testCdcIterator(ctx, is)
	defer teardown()

	singersTable.Delete(ctx, is, user1)
	singersTable.Delete(ctx, is, user2)
	singersTable.Delete(ctx, is, user3)

	testutils.ReadAndAssertDelete(ctx, is, iterator, user1)
	testutils.ReadAndAssertDelete(ctx, is, iterator, user2)
	testutils.ReadAndAssertDelete(ctx, is, iterator, user3)
}

func TestCDCIterator_UpdateAction(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	testutils.SetupDatabase(ctx, is)

	user1 := singersTable.Insert(ctx, is, 1)
	user2 := singersTable.Insert(ctx, is, 2)
	user3 := singersTable.Insert(ctx, is, 3)

	iterator, teardown := testCdcIterator(ctx, is)
	defer teardown()

	user1Updated := singersTable.Update(ctx, is, user1.Update())
	user2Updated := singersTable.Update(ctx, is, user2.Update())
	user3Updated := singersTable.Update(ctx, is, user3.Update())

	testutils.ReadAndAssertUpdate(ctx, is, iterator, user1, user1Updated)
	testutils.ReadAndAssertUpdate(ctx, is, iterator, user2, user2Updated)
	testutils.ReadAndAssertUpdate(ctx, is, iterator, user3, user3Updated)
}

func TestCDCIterator_RestartOnPosition(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	testutils.SetupDatabase(ctx, is)

	// start the iterator at the beginning

	iterator, teardown := testCdcIterator(ctx, is)

	// and trigger some insert actions

	user1 := singersTable.Insert(ctx, is, 1)
	user2 := singersTable.Insert(ctx, is, 2)
	user3 := singersTable.Insert(ctx, is, 3)
	user4 := singersTable.Insert(ctx, is, 4)

	var latestPosition opencdc.Position

	// read and ack 2 records
	testutils.ReadAndAssertInsert(ctx, is, iterator, user1)
	rec := testutils.ReadAndAssertInsert(ctx, is, iterator, user2)
	teardown()

	latestPosition = rec.Position

	// then, try to read from the second record

	iterator, teardown = testCdcIteratorAtPosition(ctx, is, latestPosition)
	defer teardown()

	user5 := singersTable.Insert(ctx, is, 5)

	testutils.ReadAndAssertInsert(ctx, is, iterator, user3)
	testutils.ReadAndAssertInsert(ctx, is, iterator, user4)
	testutils.ReadAndAssertInsert(ctx, is, iterator, user5)
}
