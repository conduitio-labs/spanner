package spanner

//go:generate paramgen -output=paramgen_src.go SourceConfig

type SourceConfig struct {
	// Database is the name of the database to use. A valid database name has the
	// form projects/PROJECT_ID/instances/INSTANCE_ID/databases/DATABASE_ID
	Database string `json:"database" validate:"required"`

	// Tables represents the spanner tables to read from.
	Tables []string `json:"tables" validate:"required"`

	// Endpoint is the URL for endpoint override - testing/dry-run only
	Endpoint string `json:"endpoint"`
}
