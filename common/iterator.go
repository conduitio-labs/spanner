package common

import (
	"context"

	"github.com/conduitio/conduit-commons/opencdc"
)

// Iterator is an object that can iterate over a queue of records.
type Iterator interface {
	// Read takes and returns the next record from the queue. Read is allowed to
	// block until either a record is available or the context gets canceled.
	Read(context.Context) (opencdc.Record, error)
	// Ack signals that a record at a specific position was successfully
	// processed.
	Ack(context.Context, opencdc.Position) error
	// Teardown attempts to gracefully teardown the iterator.
	Teardown(context.Context) error
}
