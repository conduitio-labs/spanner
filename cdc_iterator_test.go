package spanner

import (
	"fmt"
	"math/rand/v2"
	"testing"

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

	var id int64

	id = rand.Int64()
	singersTable.Insert(ctx, is, int(id), fmt.Sprintf("singer 1 %v", id))
	id = rand.Int64()
	singersTable.Insert(ctx, is, int(id), fmt.Sprintf("singer 2 %v", id))
	id = rand.Int64()
	singersTable.Insert(ctx, is, int(id), fmt.Sprintf("singer 3 %v", id))
	id = rand.Int64()
	singersTable.Insert(ctx, is, int(id), fmt.Sprintf("singer 4 %v", id))

	iterator.Read(ctx)
	fmt.Println("Read")
}

func TestInsertSomeData(t *testing.T) {
	is := is.New(t)
	ctx := testutils.TestContext(t)

	{
		id := rand.Int64()
		singersTable.Insert(ctx, is, int(id), fmt.Sprintf("singer%v", id))
	}
	{
		id := rand.Int64()
		singersTable.Insert(ctx, is, int(id), fmt.Sprintf("singer%v", id))
	}
	{
		id := rand.Int64()
		singersTable.Insert(ctx, is, int(id), fmt.Sprintf("singer%v", id))
	}
}
