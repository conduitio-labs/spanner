package spanner

import (
	"math/rand/v2"
	"testing"
	"time"

	testutils "github.com/conduitio-labs/conduit-connector-spanner/test"
	"github.com/matryer/is"
)

func TestCDC(t *testing.T) {
	is := is.New(t)
	ctx := testutils.TestContext(t)

	testutils.CreateInstance(ctx, is)
	testutils.SetupDatabase(ctx, is)

	iterator, err := newCdcIterator(ctx, &cdcIteratorConfig{
		tableName:  "Singers",
		projectID:  testutils.ProjectID,
		instanceID: testutils.InstanceID,
		databaseID: testutils.DatabaseID,
	})
	is.NoErr(err)

	defer func() { is.NoErr(iterator.Teardown(ctx)) }()

	<-time.After(1 * time.Second)

	var id int64

	id = rand.Int64()
	singer := singersTable.Insert(ctx, is, int(id))
	singersTable.Delete(ctx, is, singer)

	iterator.Read(ctx)
}

func TestInsertSomeData(t *testing.T) {
	is := is.New(t)
	ctx := testutils.TestContext(t)

	{
		id := rand.Int64()
		singersTable.Insert(ctx, is, int(id))
	}
	{
		id := rand.Int64()
		singersTable.Insert(ctx, is, int(id))
	}
	{
		id := rand.Int64()
		singersTable.Insert(ctx, is, int(id))
	}
}
