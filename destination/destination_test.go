// Copyright © 2025 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package destination

import (
	"context"
	"fmt"
	"testing"
	"time"

	testutils "github.com/conduitio-labs/conduit-connector-spanner/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func TestDestination_Configure(t *testing.T) {
	t.Parallel()

	t.Run("destination configure success", func(t *testing.T) {
		is := is.New(t)

		d := NewDestination()

		err := d.Configure(context.Background(), map[string]string{
			ConfigDatabase: "projects/project_id/instances/instance_id/databases/database_id",
			ConfigEndpoint: "localhost:9010",
		})
		is.NoErr(err)
	})

	t.Run("destination configure failure", func(t *testing.T) {
		is := is.New(t)

		d := NewDestination()

		err := d.Configure(context.Background(), map[string]string{
			ConfigEndpoint: "localhost:9010",
		})
		is.True(err != nil)
		is.Equal(err.Error(), `invalid config: config invalid: error validating "database": required parameter is not provided`)
	})
}

func TestDestination_Open(t *testing.T) {
	t.Parallel()

	t.Run("destination open success", func(t *testing.T) {
		is := is.New(t)

		d := NewDestination()

		ctx := context.Background()

		err := d.Configure(ctx, map[string]string{
			ConfigDatabase: "projects/project_id/instances/instance_id/databases/database_id",
			ConfigEndpoint: "localhost:9010",
		})
		is.NoErr(err)

		err = d.Open(ctx)
		is.NoErr(err)

		d.Teardown(ctx)
	})

	t.Run("destination open failure", func(t *testing.T) {
		is := is.New(t)

		d := NewDestination()

		ctx := context.Background()

		err := d.Configure(ctx, map[string]string{
			ConfigDatabase: "projects/project_id/instances/instance_id/databases",
			ConfigEndpoint: "localhost:9010",
			ConfigTable:    `{{ index .Metadata }}`,
		})
		is.NoErr(err)

		err = d.Open(ctx)
		is.True(err != nil)
		is.Equal(err.Error(), `failed to create spanner client: failed to create spanner client: database name "projects/project_id/instances/instance_id/databases" should conform to pattern "^projects/(?P<project>[^/]+)/instances/(?P<instance>[^/]+)/databases/(?P<database>[^/]+)$"`)

		d.Teardown(ctx)
	})
}

func TestDestination_Write(t *testing.T) {
	t.Parallel()

	t.Run("destination write insert success", func(t *testing.T) {
		is := is.New(t)
		ctx := context.Background()
		testutils.SetupDatabase(ctx, is)

		d := NewDestination()

		err := d.Configure(ctx, map[string]string{
			ConfigDatabase: fmt.Sprintf("projects/%s/instances/%s/databases/%s", testutils.ProjectID, testutils.InstanceID, testutils.DatabaseID),
			ConfigEndpoint: "localhost:9010",
		})
		is.NoErr(err)

		err = d.Open(ctx)
		is.NoErr(err)

		records := []opencdc.Record{
			{
				Operation: opencdc.OperationCreate,
				Metadata:  opencdc.Metadata{"opencdc.collection": "Singers"},
				Key:       opencdc.StructuredData{"SingerID": 1},
				Payload: opencdc.Change{
					After: opencdc.StructuredData{
						"SingerID":  "1",
						"Name":      "jon",
						"CreatedAt": time.Now().UTC(),
					},
				},
			},
		}

		n, err := d.Write(ctx, records)
		is.NoErr(err)
		is.Equal(n, len(records))

		d.Teardown(ctx)
	})

	t.Run("destination write update success", func(t *testing.T) {
		is := is.New(t)
		ctx := context.Background()
		d := NewDestination()

		err := d.Configure(ctx, map[string]string{
			ConfigDatabase: fmt.Sprintf("projects/%s/instances/%s/databases/%s", testutils.ProjectID, testutils.InstanceID, testutils.DatabaseID),
			ConfigEndpoint: "localhost:9010",
		})
		is.NoErr(err)

		err = d.Open(ctx)
		is.NoErr(err)

		records := []opencdc.Record{
			{
				Operation: opencdc.OperationUpdate,
				Metadata:  opencdc.Metadata{"opencdc.collection": "Singers"},
				Key:       opencdc.StructuredData{"SingerID": 1},
				Payload: opencdc.Change{
					After: opencdc.StructuredData{
						"SingerID":  "1",
						"Name":      "jon",
						"CreatedAt": time.Now().UTC(),
					},
				},
			},
		}

		n, err := d.Write(ctx, records)
		is.NoErr(err)
		is.Equal(n, len(records))

		d.Teardown(ctx)
	})

	t.Run("destination write insert failure no payload", func(t *testing.T) {
		is := is.New(t)
		ctx := context.Background()
		d := NewDestination()

		err := d.Configure(ctx, map[string]string{
			ConfigDatabase: fmt.Sprintf("projects/%s/instances/%s/databases/%s", testutils.ProjectID, testutils.InstanceID, testutils.DatabaseID),
			ConfigEndpoint: "localhost:9010",
		})
		is.NoErr(err)

		err = d.Open(ctx)
		is.NoErr(err)

		_, err = d.Write(ctx, []opencdc.Record{
			{
				Operation: opencdc.OperationCreate,
				Metadata:  opencdc.Metadata{"opencdc.collection": "Singers"},
				Key:       opencdc.StructuredData{"SingerID": 1},
			},
		})
		is.Equal(err.Error(), `key {"SingerID":1}: no payload`)

		d.Teardown(ctx)
	})

	t.Run("destination write insert failure record with no key", func(t *testing.T) {
		is := is.New(t)
		ctx := context.Background()
		d := NewDestination()

		err := d.Configure(ctx, map[string]string{
			ConfigDatabase: fmt.Sprintf("projects/%s/instances/%s/databases/%s", testutils.ProjectID, testutils.InstanceID, testutils.DatabaseID),
			ConfigEndpoint: "localhost:9010",
		})
		is.NoErr(err)

		err = d.Open(ctx)
		is.NoErr(err)

		_, err = d.Write(ctx, []opencdc.Record{
			{
				Operation: opencdc.OperationCreate,
				Metadata:  opencdc.Metadata{"opencdc.collection": "Singers"},
			},
		})
		is.Equal(err.Error(), `record with no key: no payload`)

		d.Teardown(ctx)
	})

	t.Run("destination write delete success", func(t *testing.T) {
		is := is.New(t)
		ctx := context.Background()
		d := NewDestination()

		err := d.Configure(ctx, map[string]string{
			ConfigDatabase: fmt.Sprintf("projects/%s/instances/%s/databases/%s", testutils.ProjectID, testutils.InstanceID, testutils.DatabaseID),
			ConfigEndpoint: "localhost:9010",
		})
		is.NoErr(err)

		err = d.Open(ctx)
		is.NoErr(err)

		records := []opencdc.Record{
			{
				Operation: opencdc.OperationDelete,
				Metadata:  opencdc.Metadata{"opencdc.collection": "Singers"},
				Key:       opencdc.StructuredData{"SingerID": 1},
			},
		}

		n, err := d.Write(ctx, records)
		is.NoErr(err)
		is.Equal(n, len(records))

		d.Teardown(ctx)
	})
}
