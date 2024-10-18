package common

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
)

type (
	PrimaryKeyName string
	TableName      string
	TableKeys      map[TableName]PrimaryKeyName
)

type PositionType string

const (
	PositionTypeSnapshot PositionType = "snapshot"
	PositionTypeCDC      PositionType = "cdc"
)

type Position struct {
	Kind             PositionType      `json:"kind"`
	SnapshotPosition *SnapshotPosition `json:"snapshot_position,omitempty"`
	CDCPosition      *CDCPosition      `json:"cdc_position,omitempty"`
}

func ParseSDKPosition(p opencdc.Position) (Position, error) {
	var pos Position
	if err := json.Unmarshal(p, &pos); err != nil {
		return pos, fmt.Errorf("failed to parse position: %w", err)
	}
	return pos, nil
}

type SnapshotPosition struct {
	Snapshots SnapshotPositions `json:"snapshots,omitempty"`
}

func (p SnapshotPosition) ToSDKPosition() opencdc.Position {
	v, err := json.Marshal(Position{
		Kind:             PositionTypeSnapshot,
		SnapshotPosition: &p,
	})
	if err != nil {
		// This should never happen, all Position structs should be valid.
		panic(err)
	}
	return v
}

type SnapshotPositions map[TableName]TablePosition

type TablePosition struct {
	LastRead    int64 `json:"last_read"`
	SnapshotEnd int64 `json:"snapshot_end"`
}

type CDCPosition struct {
	Start    time.Time `json:"start"`
	StreamID string    `json:"stream_id"`
}

func (p CDCPosition) ToSDKPosition() opencdc.Position {
	v, err := json.Marshal(Position{
		Kind:        PositionTypeCDC,
		CDCPosition: &p,
	})
	if err != nil {
		// This should never happen, all Position structs should be valid.
		panic(err)
	}
	return v
}
