# Conduit Connector for MySQL

[Conduit](https://conduit.io) connector for Google Spanner.

## How to build?

Run `make build` to build the connector.

## Testing

Run `make test` to run all tests.

The Docker compose file at `test/docker-compose.yml` can be used to run the
required resource locally. While the `gcloud` cli util is not needed to run the tests, it might be useful to debug the emulator state. Here's an example command to run custom SQL:

```bash
gcloud spanner databases execute-sql test-database \
    --instance=test-instance \
    --project=test-project \
    --sql="
        SELECT *
        FROM singers
    "
```

## Source

A source connector pulls data from an external resource and pushes it to
downstream resources via Conduit.

### Snapshot mode

Snapshot mode is the first stage of the source sync process. It reads all rows
from the configured tables as record snapshots.

In snapshot mode, the record payload consists of
[opencdc.StructuredData](https://pkg.go.dev/github.com/conduitio/conduit-connector-sdk@v0.9.1#StructuredData),
with each key being a column and each value being that column's value.

### Change Data Capture mode

(planned)

### Configuration

| name       | description                                                                                                                         | required | default value |
| ---------- | ----------------------------------------------------------------------------------------------------------------------------------- | -------- | ------------- |
| `database` | The name of the database to use. A valid database name has the form projects/PROJECT_ID/instances/INSTANCE_ID/databases/DATABASE_ID | true     |               |
| `tables`   | The list of tables to pull data from                                                                                                | true     |               |
| `endpoint` | The URL for endpoint override - testing/dry-run only                                                                                | false    |               |

## Destination

A destination connector reads data from a conduit source connector and writes it to
spanner tables.

### Configuration

| name       | description                                                                                                                         | required | default value |
| ---------- | ----------------------------------------------------------------------------------------------------------------------------------- | -------- | ------------- |
| `database` | The name of the database to use. A valid database name has the form projects/PROJECT_ID/instances/INSTANCE_ID/databases/DATABASE_ID | true     |               |
| `table`   | The name of the table to write data to.                                                                                                | true     |               |
| `schema`   | Schema name to write data to.                                                                                                | true     |               |
| `endpoint` | The URL for endpoint override - testing/dry-run only                                                                                | false    |               |


![scarf pixel](https://static.scarf.sh/a.png?x-pxid=0b3dbe6d-12de-4051-b4b9-38275009f5eb)
